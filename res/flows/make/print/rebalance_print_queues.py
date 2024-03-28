import res
import pandas as pd
import random
import math
import arrow
import os
import datetime
from res.airtable.print import (
    ROLLS,
    PRINTFILES,
    MACHINE_INFO,
    NESTS,
)
from res.airtable.misc import MATERIAL_PROP
from res.flows import flow_node_attributes, FlowContext
from res.flows.make.nest.progressive.utils import ping_rob
from res.utils import logger

SECONDS_BETWEEN_ROLLS = 15 * 60
SECONDS_BETWEEN_FILES = 60


def normalize_printer_name(p):
    return p.replace("JP7 ", "").replace("JP5 ", "").lower()


def load_machine_info():
    machines_info = MACHINE_INFO.all(
        formula=(
            "AND({Machine Type}='Printer', {Machine.Model}='MS JP7')"
            if os.environ["RES_ENV"] != "production"
            else "AND({Machine Type}='Printer', {Machine.Model}='MS JP7', {Enable Roll Assignment}=TRUE())"
        ),
        fields=[
            "Name",
            "Material Capabilities",
            "Status",
            "Active Rows At Printer",
            "Processing Roll Name",
        ],
    )
    if os.environ["RES_ENV"] == "production":
        machines_info = [
            r
            for r in machines_info
            if r["fields"]["Status"]
            not in ["OFF", "FAILURE", "DEVELOPMENT", "UNPLANNED MAINTENANCE"]
        ]
    machine_props = {
        normalize_printer_name(r["fields"]["Name"]): r["fields"] for r in machines_info
    }
    current_processing = set(
        r["fields"]["Processing Roll Name"]
        for r in machines_info
        if "Processing Roll Name" in r["fields"]
    )
    return (machine_props, current_processing)


def load_roll_info():
    rolls_info = ROLLS.all(
        fields=[
            "Name",
            "Prioritization Master",
            "Material Code",
            "Assigned Printer Name v3",
            "Carts",
            "Carts: Last Modified Time",
        ],
        formula="""
            AND(
                {Flag for Review}!=TRUE(),
                {Print Assets (flow) 3}!='',
                {Ready for Print}=TRUE(),
                {Current Substation}='Print',
                {Roll Status}!='5. Done',
                {Roll Status}!='4. Error',
                {Pretreated Length}>=5
            )
        """,
    )
    roll_names = set(r["fields"]["Name"] for r in rolls_info)
    current_assignment = {
        r["fields"]["Name"]: normalize_printer_name(
            r["fields"]["Assigned Printer Name v3"]
        )
        for r in rolls_info
        if "Assigned Printer Name v3" in r["fields"]
    }
    cart_info = {}
    rolls_in_carts = [r for r in rolls_info if "Carts" in r["fields"]]
    i = 0
    last_cart = None
    for r in sorted(
        sorted(
            rolls_in_carts,
            key=lambda r: r["fields"]["Carts: Last Modified Time"],
            reverse=True,
        ),
        key=lambda r: r["fields"]["Carts"],
    ):
        if last_cart != r["fields"]["Carts"]:
            i = 0
            last_cart = r["fields"]["Carts"]
        cart_info[r["fields"]["Name"]] = (r["fields"]["Carts"][0], i)
        i += 1
    return (rolls_info, roll_names, current_assignment, cart_info)


def load_printfile_info(roll_names):
    printfile_info = PRINTFILES.all(
        fields=[
            "Key",
            "Roll Name",
            "Height mm",
            "Asset Path",
            "Flag For Review",
            "Material Code",
            "Print Queue",
            "Printed_ts",
        ],
        formula="OR({Print Queue}='TO DO', {Print Queue}='PROCESSING')",
    )
    # itertools groupby is fucking useless
    rolls_printfiles = {}
    for r in printfile_info:
        roll_name = r["fields"]["Roll Name"]
        if roll_name not in rolls_printfiles:
            rolls_printfiles[roll_name] = []
        rolls_printfiles[roll_name].append(r)
    rolls_printfiles = {
        k: g
        for k, g in rolls_printfiles.items()
        if k in roll_names
        and all(r["fields"].get("Asset Path", "") != "" for r in g)
        and all(r["fields"].get("Flag For Review", False) == False for r in g)
    }
    return (printfile_info, rolls_printfiles)


def material_print_modes():
    material_info = MATERIAL_PROP.all(
        fields=["Material Code", "Print Mode Resolution"],
        formula="{Development Status}='Approved'",
    )
    return {
        r["fields"]["Material Code"]: (
            (lambda r: r if "HQ" not in r else r.replace("HQ", "") + " HQ")(
                r["fields"]["Print Mode Resolution"].replace("HighSpeed", "HiSpeed")
            )
        )
        for r in material_info
        if "Print Mode Resolution" in r["fields"]
    }


def estimate_print_time(
    material_code,
    printfiles,
    length,
    elapsed,
    material_print_mode,
    machine_props,
    speed_info,
    current_assignment,
):
    print_mode = material_print_mode[material_code]
    est_times = {}
    for machine, props in machine_props.items():
        if (
            material_code in props["Material Capabilities"]
            or machine == current_assignment
        ):
            speed_mm_s = (
                speed_info["base_rates"][print_mode]
                * speed_info["printer_multipliers"][machine]
                * props["Active Rows At Printer"]
            )
            est_times[machine] = (
                SECONDS_BETWEEN_FILES * printfiles + length / speed_mm_s
            )
            if est_times[machine] < elapsed:
                est_times[machine] = 0
            else:
                est_times[machine] -= elapsed
    return est_times


def cart_to_row(cart_gdf):
    est_times = list(cart_gdf.est_time_s.values)
    est_times_combined = est_times[0]
    for i in range(1, cart_gdf.shape[0]):
        c = {}
        for m, t in est_times[i].items():
            if m in est_times_combined:
                c[m] = t + est_times_combined[m] + 5 * 60
        est_times_combined = c
    return pd.DataFrame.from_records(
        [
            {
                "orig_ind": cart_gdf.index.values,
                "est_time_s": est_times_combined,
                "Processing": any(cart_gdf["Processing"]),
                "Current Assignment": cart_gdf["Current Assignment"].iloc[0],
            }
        ]
    )


def explode_sched(sched, cart_df):
    return {
        m: [pid for cid in cids for pid in cart_df.iloc[cid].orig_ind]
        for m, cids in sched.items()
    }


def score(df, sched):
    max_t = 0
    reassigns = 0
    for p, s in sched.items():
        t = 0
        mat = None
        for i in s:
            r = df.iloc[i]
            t += SECONDS_BETWEEN_ROLLS + r.est_time_s[p]
            if r.get("Current Assignment", "") != p:
                reassigns += 1
        if t > max_t:
            max_t = t
    return max_t + 300 * reassigns


def anneal(df, machine_names, max_iters=10000):
    n = df.shape[0]
    sched = current_sched(df, machine_names)
    curr = score(df, sched)
    fixed_i = set(i for i, r in df.iterrows() if r["Processing"])
    logger.info(f"Fixed rolls: {fixed_i}")
    logger.info(f"Initial Schedule: {curr} {sched}")
    for iteration in range(max_iters):
        i = random.randint(0, n - 1)
        if not i in fixed_i:
            sched_new = {k: [j for j in v if j != i] for k, v in sched.items()}
            m = random.choice(list(df.iloc[i].est_time_s.keys()))
            p = random.randint(
                1 if len(sched_new[m]) > 0 and sched_new[m][0] in fixed_i else 0,
                len(sched_new[m]),
            )
            sched_new[m].insert(p, i)
            score_new = score(df, sched_new)
            delta = score_new - curr
            if delta < 0 or random.random() > (
                2 / (1 + math.exp(-delta * 100 * iteration / max_iters)) - 1
            ):
                curr = score_new
                sched = sched_new
    logger.info(f"Final Schedule: {curr} {sched}")
    return sched


def current_sched(df, machine_names):
    sched = {p: [] for p in machine_names}
    for i, r in df.iterrows():
        current_assign = r["Current Assignment"]
        if current_assign is not None:
            if r["Processing"]:
                sched[current_assign].insert(0, i)
            else:
                sched[current_assign].append(i)
        else:
            sched[random.choice(list(r.est_time_s.keys()))].append(i)
    return sched


def update_schedule_times(sched, df, material_print_mode, machine_props, speed_info):
    upd = []
    for m, ind in sched.items():
        ts = datetime.datetime.utcnow()
        for i in ind:
            r = df.iloc[i]
            for pf in r["Printfiles"]:
                start_time = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                if pf["fields"]["Print Queue"] == "PROCESSING":
                    elapsed = (
                        datetime.datetime.utcnow()
                        - datetime.datetime.strptime(
                            pf["fields"]["Printed_ts"], "%Y-%m-%dT%H:%M:%S.%fZ"
                        )
                    ).total_seconds()
                else:
                    elapsed = 0
                dt = estimate_print_time(
                    r["Material Code"],
                    0,
                    pf["fields"]["Height mm"],
                    elapsed,
                    material_print_mode,
                    machine_props,
                    speed_info,
                    m,
                )[m]
                ts += datetime.timedelta(seconds=dt)
                end_time = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                ts += datetime.timedelta(seconds=SECONDS_BETWEEN_FILES)
                upd.append(
                    {
                        "id": pf["id"],
                        "fields": {
                            "Scheduled End Time": end_time,
                            **(
                                {
                                    "Scheduled Start Time": start_time,
                                }
                                if pf["fields"]["Print Queue"] != "PROCESSING"
                                else {}
                            ),
                        },
                    }
                )
            ts += datetime.timedelta(seconds=SECONDS_BETWEEN_ROLLS)
    PRINTFILES.batch_update(upd)


def reassign_roll(
    roll_name,
    machine_name,
):
    logger.info(f"Reassigning {roll_name} to {machine_name}")
    roll_record = ROLLS.all(formula=f"{{Name}}='{roll_name}'")
    if len(roll_record) != 1:
        raise ValueError(
            f"Failed to find roll {roll_name}: airtable returned {len(roll_record)} records"
        )
    if roll_record["fields"].get("Assigned Printer Name v3"):
        logger.info(f"{roll_name} is already assigned to {machine_name}")
        return
    roll_record = roll_record[0]
    if not roll_record["fields"]["Ready for Print"]:
        raise ValueError(f"{roll_name} is not ready for print")
    NESTS.batch_update(
        [
            {
                "id": r,
                "fields": {
                    "Assigned Printer": machine_name,
                },
            }
            for r in roll_record["fields"]["Nested Asset Sets"]
        ],
        typecast=True,
    )
    PRINTFILES.batch_update(
        [
            {
                "id": r,
                "fields": {
                    "Assigned Printer": machine_name,
                    "Asset File Status": None,
                    "Print Asset Downloaded?": False,
                },
            }
            for r in roll_record["fields"]["Print Files"]
        ],
        typecast=True,
    )
    ROLLS.update(
        roll_record["id"],
        {
            "Assigned Printer Name v3": machine_name,
            "Print Flow": "Locked",
            "Locked Roll At": arrow.utcnow().isoformat(),
        },
        typecast=True,
    )


def assign_roll_to_machine(
    machine_name,
    roll_name,
    rolls_info,
    machine_props,
    printfile_info,
    previous_machine_name=None,
):
    logger.info(
        f"Reassigning {roll_name} to {machine_name} from {previous_machine_name}"
    )
    if os.getenv("RES_ENV") != "production":
        logger.info("Not reassigning due to env.")
        return
    roll_record = next(r for r in rolls_info if r["fields"]["Name"] == roll_name)
    ROLLS.update(
        roll_record["id"],
        {
            "Assigned Printer Name v3": machine_props[machine_name]["Name"],
            "Print Flow": "Locked",
            "Locked Roll At": arrow.utcnow().isoformat(),
        },
        typecast=True,
    )
    PRINTFILES.batch_update(
        [
            {
                "id": r["id"],
                "fields": {
                    "Assigned Printer": machine_props[machine_name]["Name"],
                    **(
                        {
                            "Asset File Status": None,
                            "Print Asset Downloaded?": False,
                        }
                        if previous_machine_name
                        else {}
                    ),
                },
            }
            for r in printfile_info
            if r["fields"]["Roll Name"] == roll_name
        ],
        typecast=True,
    )
    NESTS.batch_update(
        [
            {
                "id": r["id"],
                "fields": {
                    "Assigned Printer": machine_props[machine_name]["Name"],
                },
            }
            for r in NESTS.all(fields=["Key"], formula=f"{{Roll Name}}='{roll_name}'")
        ],
        typecast=True,
    )


@flow_node_attributes(
    "rebalance_print_queues.handler",
    mapped=False,
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        try:
            rolls_info, roll_names, current_assignment, cart_info = load_roll_info()
            if len(rolls_info) == 0:
                return
            speed_info = res.connectors.load("s3").read(
                "s3://res-data-development/experimental/printer_speed/data.json"
            )
            machine_props, current_processing = load_machine_info()
            machine_names = list(machine_props.keys())
            printfile_info, rolls_printfiles = load_printfile_info(roll_names)
            material_print_mode = material_print_modes()
            rolls_df = pd.DataFrame.from_records(
                [
                    {
                        "Roll Name": k,
                        "Current Assignment": current_assignment.get(k),
                        "Processing": k in current_processing,
                        "Cart": cart_info.get(k, (k, 0))[0],
                        "Cart Idx": cart_info.get(k, (k, 0))[1],
                        "Material Code": g[0]["fields"]["Material Code"][0],
                        "Printfile Count": len(g),
                        "Total Printfile Length": sum(
                            r["fields"]["Height mm"] for r in g
                        ),
                        "Elapsed": max(
                            [
                                (
                                    datetime.datetime.utcnow()
                                    - datetime.datetime.strptime(
                                        r["fields"]["Printed_ts"],
                                        "%Y-%m-%dT%H:%M:%S.%fZ",
                                    )
                                ).total_seconds()
                                for r in g
                                if r["fields"]["Print Queue"] == "PROCESSING"
                            ],
                            default=0,
                        ),
                        "Printfiles": sorted(g, key=lambda r: r["fields"]["Key"]),
                    }
                    for k, g in rolls_printfiles.items()
                ]
            )
            rolls_df["est_time_s"] = rolls_df.apply(
                lambda r: estimate_print_time(
                    r["Material Code"],
                    r["Printfile Count"],
                    r["Total Printfile Length"],
                    r["Elapsed"],
                    material_print_mode,
                    machine_props,
                    speed_info,
                    r["Current Assignment"],
                ),
                axis=1,
            )
            rolls_df = rolls_df[
                rolls_df.est_time_s.apply(lambda m: len(m) > 0)
            ].reset_index(drop=True)
            cart_df = rolls_df.groupby("Cart").apply(cart_to_row).reset_index(drop=True)
            sched = anneal(cart_df, machine_names)
            sched = explode_sched(sched, cart_df)
            sched_named = {
                machine: [rolls_df.iloc[i]["Roll Name"] for i in machine_sched]
                for machine, machine_sched in sched.items()
            }
            logger.info(f"Updated schedule: {sched_named}")
            reassignment = {r: p for p, l in sched_named.items() for r in l}
            rolls_df["Reassignment"] = rolls_df["Roll Name"].apply(
                lambda n: reassignment[n]
            )
            rolls_df[rolls_df["Reassignment"] != rolls_df["Current Assignment"]]
            rolls_df["Current Expected Time"] = rolls_df.apply(
                lambda r: r.est_time_s[r["Current Assignment"]] / 3600.0, axis=1
            )
            rolls_df["Reassignment Expected Time"] = rolls_df.apply(
                lambda r: r.est_time_s[r["Reassignment"]] / 3600.0, axis=1
            )
            current_times = rolls_df.groupby("Current Assignment")[
                "Current Expected Time"
            ].sum()
            reassign_times = rolls_df.groupby("Reassignment")[
                "Reassignment Expected Time"
            ].sum()
            reassignment = rolls_df[
                rolls_df["Reassignment"] != rolls_df["Current Assignment"]
            ]
            print_queue_message = f"""Print Queue Hours: {", ".join(f"{p}: {h:.2f}" for p, h in dict(current_times).items())}"""
            if reassignment.shape[0] > 0:
                if os.getenv("RES_ENV") == "production":
                    rebalance_message = f"""Rebalancing: {", ".join(reassignment.apply(lambda r: r["Roll Name"] + ": " + r["Current Assignment"] + " -> " + r["Reassignment"], axis = 1).values)}
                        Rebalanced Print Hours: {", ".join(f"{p}: {h:.2f}" for p, h in dict(reassign_times).items())}
                        """
                else:
                    rebalance_message = f"""Would rebalance: {", ".join(reassignment.apply(lambda r: r["Roll Name"] + ": " + r["Current Assignment"] + " -> " + r["Reassignment"], axis = 1).values)}
                        Rebalanced Print Hours: {", ".join(f"{p}: {h:.2f}" for p, h in dict(reassign_times).items())}
                        """
                ping_rob(
                    "\n".join(
                        r.strip()
                        for r in f"""{print_queue_message}\n{rebalance_message}""".split(
                            "\n"
                        )
                        if r != ""
                    )
                )
            if os.getenv("RES_ENV") == "production":
                update_schedule_times(
                    sched, rolls_df, material_print_mode, machine_props, speed_info
                )
                for _, r in reassignment.iterrows():
                    assign_roll_to_machine(
                        r["Reassignment"],
                        r["Roll Name"],
                        rolls_info,
                        machine_props,
                        printfile_info,
                        previous_machine_name=r["Current Assignment"],
                    )
        except Exception as ex:
            pass
