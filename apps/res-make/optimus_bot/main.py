import os
import re
import res
import time
import flask
import traceback
import pandas as pd
import numpy as np
import logging
import traceback as tb
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import lru_cache
from matplotlib import pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.pyplot import cm
from langchain.agents import create_pandas_dataframe_agent
from langchain.llms import OpenAI
from flask import Flask, Response, make_response
from slack_sdk import WebClient
from slack_bolt import App, Say
from slack_bolt.adapter.flask import SlackRequestHandler
from res.airtable.print import ASSETS
from res.utils.secrets import secrets
from res.flows.make.print.rebalance_print_queues import reassign_roll
from res.flows.make.nest.clear_nest_roll_assignments import clear_nest_roll_assignments
from res.flows.meta.ONE.meta_one import MetaOne
from res.learn.optimization.schedule.ppu import (
    schedule_ppus,
    get_roll_names_for_ppu,
    assign_ppus_to_printers,
    PRINTERS,
)
from res.media.images.providers.dxf import DxfFile

try:
    secrets.get_secret("SLACK_OPTIMUS_BOT_SIGNING_SECRET")
    secrets.get_secret("SLACK_OPTIMUS_BOT_TOKEN")
    secrets.get_secret("OPENAI_API_KEY")
except:
    print("cannot load secrets in this context....")

app = Flask(__name__)
PROCESS_NAME = "optimus-bot"
logging.getLogger("werkzeug").setLevel(logging.WARN)

PROCESS_FAILURE_CONTRACT_VARIABLE = "reccyLMIhbma0MfBn"

try:
    client = WebClient(token=os.environ.get("SLACK_OPTIMUS_BOT_TOKEN"))
    bolt_app = App(
        token=os.environ.get("SLACK_OPTIMUS_BOT_TOKEN"),
        signing_secret=os.environ.get("SLACK_OPTIMUS_BOT_SIGNING_SECRET"),
    )
    handler = SlackRequestHandler(bolt_app)
    res.utils.logger.info(f"Bot running")
except Exception as ex:
    res.utils.logger.warn(f"Failed to set up the bot {traceback.format_exc()}")


def cache_ts(cache_timeout_s=60):
    return int(time.time() // cache_timeout_s)


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return Response("ok", status=200)


def _parse_command_args(arg_str):
    arg_str = arg_str.upper()

    parts = OrderedDict()
    parts["rolls"] = "R[0-9_E]+(: [A-Z]+[0-9]*)?"
    parts["ppus"] = "PPU-[0-9\\-]+"
    parts["printers"] = "(JP7 ?)?(LUNA|NOVA|FLASH)"
    parts["materials"] = "[A-Z]+[0-9]*"

    out = {}
    out["named_args"] = {}

    while True:
        match = max(
            re.finditer("[A-Z]+=[A-Z0-9]+", arg_str),
            key=lambda m: len(m.group(0)),
            default=None,
        )
        if match:
            k, v = match.group(0).split("=")
            out["named_args"][k] = v
            arg_str = arg_str.replace(match.group(0), "")
        else:
            break

    for part, part_re in parts.items():
        while True:
            match = max(
                re.finditer(part_re, arg_str),
                key=lambda m: len(m.group(0)),
                default=None,
            )
            if match:
                if part not in out:
                    out[part] = []
                out[part].append(match.group(0))
                arg_str = arg_str.replace(match.group(0), "")
            else:
                break

    arg_str = re.sub("[ ,]", "", arg_str)
    if len(arg_str) > 0:
        raise ValueError(
            f"Failed to parse args - unused string {arg_str} and args {out}"
        )
    return out


@bolt_app.command("/optimus_flush")
def optimus_flush(ack, say, command):
    ack()
    try:
        args = _parse_command_args(command["text"])
        materials = args["materials"]
        rolls = args.get("rolls", [])
        job = f"""construct-rolls-adhoc-{int(time.time())}"""
        res.connectors.load("argo").handle_event(
            {
                "apiVersion": "resmagic.io/v1",
                "args": {
                    "materials": materials,
                    "fullness_override": 0,
                    "slack_channel": say.channel,
                    **({"roll_names": rolls} if len(rolls) > 0 else {}),
                },
                "assets": [],
                "kind": "resFlow",
                "metadata": {
                    "name": "make.nest.progressive.construct_rolls",
                    "version": "primary",
                },
                "task": {
                    "key": job,
                },
            },
            context={},
            template="res-flow-node",
            unique_job_name=job,
        )
        say(
            f"""Flushing all assets of materials: {materials} to {"rolls " + ",".join(rolls) if len(rolls) > 0 else "any available rolls"} -- Argo job name {job}"""
        )
    except Exception as ex:
        say(f"Failed to flush optimus: {repr(ex)}")


@bolt_app.command("/optimus_unassign")
def optimus_unassign(ack, say, command):
    ack()
    try:
        args = _parse_command_args(command["text"])
        dry_run = os.environ.get("RES_ENV") != "production"
        for roll_name in args["rolls"]:
            if ":" not in roll_name:
                say(f"Note that unassignment needs full roll names, got {roll_name}")
            else:
                say(
                    f"""Clearing assignments on {roll_name} {"dry run" if dry_run else "for real"}"""
                )
                total_assets, total_nests, total_pfs = clear_nest_roll_assignments(
                    ["all"],
                    [roll_name],
                    dry_run,
                    notes="Roll unassigned through optimus bot",
                )
                say(
                    f"""{"Would have unassigned" if dry_run else "Unassigned"}: {total_assets} assets, {total_nests} nests and {total_pfs} print files from {roll_name}"""
                )
    except Exception as ex:
        say(f"Failed to unassign roll {roll_name}: {repr(ex)}")


@bolt_app.command("/optimus_process_fail")
def optimus_process_fail(ack, say, command):
    ack()
    try:
        args = _parse_command_args(command["text"])
        for roll_name in args["rolls"]:
            if ":" not in roll_name:
                say(f"Note that unassignment needs full roll names, got {roll_name}")
            else:
                say(
                    f"""Clearing assignments on {roll_name} and marking assets with process failure"""
                )
                total_assets, total_nests, total_pfs = clear_nest_roll_assignments(
                    ["all"],
                    [roll_name],
                    os.environ.get("RES_ENV") != "production",
                    notes="Roll unassigned through optimus bot",
                    asset_contract_variables=[PROCESS_FAILURE_CONTRACT_VARIABLE],
                )
                say(
                    f"""Unassigned: {total_assets} assets, {total_nests} nests and {total_pfs} print files from {roll_name} and marked with process failure"""
                )
    except Exception as ex:
        say(f"Failed to unassign roll {roll_name}: {repr(ex)}")


@bolt_app.command("/optimus_reassign")
def optimus_reassign(ack, say, command):
    ack()
    try:
        args = _parse_command_args(command["text"])
        printers = [p if "JP7" in p else "JP7 " + p for p in args["printers"]]
        rolls = args.get("rolls", []) + [
            r for p in args.get("ppus", []) for r in get_roll_names_for_ppu(p)
        ]
        if len(printers) != 1:
            raise say(f"Need to specify exactly one printer, got {printers}")
        else:
            printer = printers[0]
            for roll in rolls:
                if ":" not in roll:
                    say(f"Reassignment needs full roll names, got {roll}")
                else:
                    reassign_roll(roll, printer)
                    say(f"Reassigned {roll} to {printer}")
    except Exception as ex:
        say(f"Failed to reassign rolls: {repr(ex)}")


@bolt_app.command("/optimus_renest")
def optimus_renest(ack, say, command):
    ack()
    try:
        args = _parse_command_args(command["text"])
        rolls = args["rolls"]
        if len(rolls) != 1:
            raise say(f"Need to specify exactly one roll, got {rolls}")
        else:
            roll = rolls[0]
            material = roll.split(":")[1].strip()
            width_override = args["named_args"].get("WIDTH")
            say(
                f"Renesting roll {roll} {f'with width override {width_override}' if width_override is not None else ''}"
            )
            say(f"Clearing existing assignment on {roll}")
            total_assets, total_nests, total_pfs = clear_nest_roll_assignments(
                ["all"],
                [roll],
                False,
                notes="Roll unassigned in order to re-nest",
            )
            say(
                f"""Unassigned: {total_assets} assets, {total_nests} nests and {total_pfs} print files from {roll}"""
            )
            job = f"""construct-rolls-adhoc-{int(time.time())}"""
            res.connectors.load("argo").handle_event(
                {
                    "apiVersion": "resmagic.io/v1",
                    "args": {
                        "materials": [material],
                        "fullness_override": 0,
                        "slack_channel": say.channel,
                        "roll_names": [roll],
                        **(
                            {"width_override": width_override}
                            if width_override is not None
                            else {}
                        ),
                    },
                    "assets": [],
                    "kind": "resFlow",
                    "metadata": {
                        "name": "make.nest.progressive.construct_rolls",
                        "version": "primary",
                    },
                    "task": {
                        "key": job,
                    },
                },
                context={},
                template="res-flow-node",
                unique_job_name=job,
            )
            say(
                f"""Flushing all assets of material: {material} to {roll} {f'with width override {width_override}' if width_override is not None else ''} -- Argo job name {job}"""
            )
    except Exception as ex:
        say(f"Failed to renest roll: {repr(ex)}")


@bolt_app.command("/optimus_ppu")
def optimus_ppu(ack, say, command):
    ack()
    try:
        args = _parse_command_args(command["text"])
        printers = [p.replace("JP7 ", "").lower() for p in args.get("printers", [])]
        if len(printers) == 0:
            printers = PRINTERS
        say(
            f"Computing a PPU schedule for unassigned rolls assuming {', '.join(printers)} - will take ~10 minutes."
        )
        scheduled, summary = schedule_ppus(
            plot_filename="ppu_sched.png",
            text_filename="ppu_sched.txt",
            active_printers=printers,
        )
        if scheduled:
            client.files_upload(
                channels=say.channel,
                title="PPU Schedule",
                file="ppu_sched.png",
            )
            client.files_upload(
                channels=say.channel,
                title="PPU Schedule",
                file="ppu_sched.txt",
            )
        say(summary)
    except Exception as ex:
        say(f"Failed to schedule PPUs: {repr(ex)}")
        tb.print_exc(limit=5)


def nestable_assets_report(channel):
    nestable_assets = ASSETS.all(
        fields=[
            "Material Code",
            "Days in Print Station",
            "Asset Age Days",
        ],
        formula="{Nesting ONE Ready}=1",
    )
    df = pd.DataFrame([r["fields"] for r in nestable_assets])
    for delta in [2, 5]:
        df[f"Order > {delta}d"] = df["Days in Print Station"].apply(
            lambda d: 1 if d > delta else 0
        )
        df[f"Asset > {delta}d"] = df["Asset Age Days"].apply(
            lambda d: 1 if d > delta else 0
        )
    g = (
        df.groupby("Material Code")
        .agg(
            {
                "Material Code": "count",
                **{f"Order > {delta}d": "sum" for delta in [2, 5]},
                **{f"Asset > {delta}d": "sum" for delta in [2, 5]},
            }
        )
        .rename(columns={"Material Code": "Total Assets"})
        .reset_index()
        .sort_values("Material Code")
    )
    with open("nestable_assets.txt", "w") as f:
        f.write("Materials out of contract:\n")
        f.write(g[g["Asset > 2d"] > 0].to_markdown(tablefmt="grid", index=False))
        f.write("\n\nMaterials within contract:\n")
        f.write(g[g["Asset > 2d"] == 0].to_markdown(tablefmt="grid", index=False))
    client.files_upload(
        channels=channel,
        title="Nestable Assets Report",
        file="nestable_assets.txt",
    )
    # now make a plot
    plot_days = 20
    start_date = datetime.now() - timedelta(days=plot_days)
    df = res.connectors.load("snowflake").execute(
        f"""
        select
            "_created_ts",
            "__request_print_printed_ts",
            "Rank",
            "Material Code",
            "__EXISTS_IN_AIRTABLE__"
        from
            IAMCURIOUS_PRODUCTION.AIRTABLE__RES_MAGIC_PRINT__PRINT_ASSETS_FLOW
        where
            "_created_ts" > '{start_date.strftime("%Y-%m-%d")}'
            and "Print Queue" in ('TO DO', 'PRINTED')
    """
    )
    df["enter"] = df["_created_ts"].apply(
        lambda d: (start_date - timedelta(days=1))
        if d is None
        else datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")
    )
    df["exit"] = df["__request_print_printed_ts"].apply(
        lambda d: (datetime.now() + timedelta(days=1))
        if pd.isnull(d)
        else datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")
    )
    df["enter_ts"] = df["enter"].apply(lambda d: d.timestamp())
    df["exit_ts"] = df["exit"].apply(lambda d: d.timestamp())
    df = df[
        df.apply(
            lambda r: r["__EXISTS_IN_AIRTABLE__"]
            or pd.notnull(r["__request_print_printed_ts"]),
            axis=1,
        )
    ].reset_index(drop=True)
    fig, ax = plt.subplots(figsize=(20, 20))
    materials = [k for k, v in dict(df["Material Code"].value_counts()[0:7]).items()]
    colors = cm.rainbow(np.linspace(1, 0, len(materials) + 1))
    mat_color = {m: p for m, p in zip(materials, colors)}
    gdf = df.sort_values("enter_ts").reset_index(drop=True)
    for i, r in gdf.iterrows():
        ax.barh(
            i,
            left=r["enter_ts"],
            width=r["exit_ts"] - r["enter_ts"],
            height=2,
            color=mat_color.get(r["Material Code"], colors[-1]),
        )
    day0 = datetime(start_date.year, start_date.month, start_date.day)
    days = [day0 + timedelta(days=i) for i in range(plot_days + 1)]
    for d in days:
        if d.weekday() == 5:
            ax.add_patch(
                mpatches.Rectangle(
                    (d.timestamp(), 0), 48 * 3600, gdf.shape[0], alpha=0.1
                )
            )
    ax.set_xticks([d.timestamp() for d in days])
    ax.set_xticklabels([d.strftime("%Y-%m-%d") for d in days], fontsize=20)
    ax.tick_params(axis="x", labelrotation=-90)
    ax.set_xlim(days[0].timestamp() - 1000, days[-1].timestamp() + 1000)
    ax.set_ylim(0, gdf.shape[0])
    ax.legend(
        handles=[mpatches.Patch(color=c, label=p) for p, c in mat_color.items()]
        + [mpatches.Patch(color=colors[-1], label="OTHER")],
        loc=2,
        fontsize=18,
    )
    plt.savefig("flow_report.png")
    client.files_upload(
        channels=channel,
        title="Print Flow Report",
        file="flow_report.png",
    )


@bolt_app.command("/dxf")
def dxf(ack, say, command):
    ack()
    sku = command["text"]
    say(f"Generating dxf for sku: {sku}")
    m1 = MetaOne(sku)
    mat_array = m1._data.garment_piece_material_code.unique()
    mat_props = res.connectors.load("airtable").get_airtable_table_for_schema_by_name(
        "make.material_properties",
        f"""FIND({{Material Code}}, '{",".join(mat_array)}')""",
    )
    mat_dict = {r["key"]: r for r in mat_props.to_dict("records")}
    for m in mat_dict:
        try:
            filename = f"""{sku.replace(" ", "_")}_({m}).dxf"""
            m1.save_nested_cutlines(
                filename,
                mat_dict[m]["cuttable_width"],
                mat_dict[m]["paper_marker_compensation_width"],
                mat_dict[m]["paper_marker_compensation_length"],
                material=m,
            )
            # DxfFile(filename).plot()
            # plt.savefig(filename.replace(".dxf", ".png"))
            client.files_upload(
                channels=say.channel,
                title=filename,
                file=filename,
            )
        except Exception as ex:
            say(f"Failed to generate dxf for sku: {sku}: {repr(ex)}")


@app.route(f"/{PROCESS_NAME}/slack/events", methods=["POST"])
def slack_events():
    """Declaring the route where slack will post a request and dispatch method of App"""
    try:
        request_json = flask.request.get_json(silent=True, force=True)
        if request_json.get("challenge") is not None:
            r = make_response(request_json.get("challenge"), 200)
            r.mimetype = "text/plain"
            return r
    except:
        pass

    try:
        res.utils.logger.info(flask.request)
        return handler.handle(flask.request)
    except:
        res.utils.logger.warn(
            f"Failed to relay slack message: {traceback.format_exc()}"
        )
        return {"statusCode": 500, "body": {"message": "failed"}}


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    if os.environ["RES_ENV"] == "production":
        scheduler.add_job(
            nestable_assets_report,
            CronTrigger(start_date="2022-1-1", hour=13),  # 9am
            args=["one-platform-x"],
        )
        scheduler.add_job(
            assign_ppus_to_printers,
            CronTrigger(start_date="2022-1-1", hour=21, day_of_week="mon-fri"),  # 5pm
        )
    scheduler.start()
    app.run(host="0.0.0.0")
    scheduler.shutdown(wait=False)
