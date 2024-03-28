# a low-bloat way to schedule a bunch of stop-gap processes to sync data into our primary database
import json
from collections.abc import Iterable

import numpy as np
import pandas as pd

import res
from res.flows.dxa.setup_files import net_dxf_location_cache
from res.flows.dxa.styles import helpers, queries
from res.utils.env import RES_DATA_BUCKET, RES_ENV

##### a bunch of temporary jobs that we dont need yo add anywhere else


def make_temporary_marker(
    sku,
    body_version,
    color_pieces,
    color_pieces_combo=None,
    print_type=None,
    cache=None,
    **kwargs,
):
    """
    this is a patch
    when we cannot create a style in the database properly with all the goodies, we can at least take the DXA marker color pieces which must exist
    and replace this marker with the intelligent one marker - the intelligence is added by the new meta one anyway so we dont need the IoM at all

    with this we can aspire to create fully intelligent pieces and fallback to dumb piecs when we cannot. also this is closer to what we do today so good for roll out testing
    """

    s3 = res.connectors.load("s3")

    def make_piece_row(sku, body_version, filename):
        res.utils.logger.debug(f"Processing {sku} {body_version}, {filename}")
        im = s3.read(filename)
        outline = res.media.images.outlines.get_piece_outline(im)
        notches = None
        try:
            notches = res.media.images.geometry.detect_castle_notches_from_outline(
                outline
            )
            if notches.is_empty:
                notches = None
        except:
            pass

        return {
            "sku": sku,
            "body_version": body_version,
            "print_type": print_type,
            "outline": str(outline),
            "notches": str(notches),
            "uri": filename,
            "key": filename.split("/")[-1].split(".")[0],
        }

    def part(i):
        return sku.split(" ")[i].strip()

    body, material, color, size = part(0), part(1), part(2), part(3)
    root = f"s3://res-data-platform/temporary-markers/styles/{body}/{material}/{color}/{size}/v{body_version}"
    res.utils.logger.debug(f"processing files for {root}")
    color_pieces = [f"s3://meta-one-assets-prod/{f}" for f in color_pieces]
    data = [make_piece_row(sku, body_version, p) for p in color_pieces]
    data = pd.DataFrame(data)
    res.utils.logger.debug(f"writing self to {root}")
    s3.write(f"{root}/self-marker.feather", data)

    if isinstance(color_pieces_combo, Iterable):
        color_pieces = [f"s3://meta-one-assets-prod/{f}" for f in color_pieces_combo]
        data = [make_piece_row(sku, body_version, p) for p in color_pieces]
        data = pd.DataFrame(data)
        res.utils.logger.debug(f"writing combo to {root}")
        s3.write(f"{root}/combo-marker.feather", data)

    return data


def process_outstanding_temporary_markers(event, context=None, max_records=10):
    """
    the thing that creates a temporary marker wrapped in a workflow
    we look at an already cached list of DXA records and see if we have the marker by style/body version that we want
    we must check if have both ranks or we re-process
    """
    s3 = res.connectors.load("s3")

    def path_from_sku_version(sku, body_version):
        def part(i):
            return sku.split(" ")[i].strip()

        body, material, color, size = part(0), part(1), part(2), part(3)
        return f"s3://res-data-platform/temporary-markers/styles/{body}/{material}/{color}/{size}/v{body_version}"

    cache = s3.read("s3://res-data-platform/temporary-markers/dxa.feather")
    # we should have removed duplicates
    # inputs['modified_at'] = pd.to_datetime(inputs['modified_at'], utc=True)
    counter = 0
    for record in cache.to_dict("records"):
        sku = record["sku"]
        body_version = int(record["body_version"])
        expected = 1
        combo_pieces = record.get("color_pieces_combo", [])
        if isinstance(combo_pieces, Iterable) and len(combo_pieces) > 0:
            expected = 2
        path = path_from_sku_version(sku, body_version)
        count_at_site = len(list(s3.ls(path)))
        # lazy should check if
        res.utils.logger.debug(f"{count_at_site} files at {path}")
        if count_at_site < expected:
            try:
                make_temporary_marker(**record)
                counter += 1
            except Exception as ex:
                res.utils.logger.warn(f"Failing to make the temporary marker {ex}")
        else:
            res.utils.logger.info(f"{path} is done")
        if counter >= max_records:
            break

    return {}


def load_and_check_style_valid(event, context={}, max_requests=50, plan=False):
    """
    simple system heal on any env
    we keep a cached list of styles we want. if the bodies are ready, we request them in kafka
    we need a way to know we have already tried and failed

    in this case we use s3 as a poor mans log of what we requested so we pin it to an environment bucket
    """

    def try_load(s):
        try:
            return json.loads(s)
        except:
            return None

    REQUEST_TOPIC = "res_meta.dxa.style_pieces_update_requests"

    s3 = res.connectors.load("s3")
    kafka = res.connectors.load("kafka")
    chk = s3.read(f"s3://{RES_DATA_BUCKET}/cache/styles_we_want.feather")
    chk["payload"] = chk["payload"].map(try_load)

    cc = net_dxf_location_cache()
    bds = cc._db.read()

    test = pd.merge(chk, bds, left_on="body_key", right_on="key")

    counter = 0
    active_styles = queries.list_active_styles()
    # check if we have an active style size for anything we have not requested(recently) and is valid from a body perspective
    for r in test[(test["is_valid"] == True) & (test["requested"].isnull())].to_dict(
        "records"
    ):
        payload = r["payload"]

        # one query would be better here or retry and skip
        if r["sku"] not in active_styles and payload is not None:
            kafka[REQUEST_TOPIC].publish(payload, use_kgateway=True)
            counter += 1
            # update requested at on the file and we can save it again
            chk.loc[
                chk["index"] == r["index"], "requested"
            ] = res.utils.dates.utc_now_iso_string()
        else:
            res.utils.logger.info(f"WE HAVE THE {r['sku']}")
        if counter >= max_requests:
            break

    chk["payload"] = chk["payload"].map(
        lambda x: None if pd.isnull(x) else json.dumps(x)
    )

    # this is a way to say we already requested for this environment and we can skip next time until investigation
    s3.write(f"s3://{RES_DATA_BUCKET}/cache/styles_we_want.feather", chk)

    return chk


def purge_airtable_queues(event, context={}):
    from res.flows import FlowEventProcessor
    from res.flows.api.core.workflow import process_evictions_sql

    payload = FlowEventProcessor().make_sample_flow_payload_for_function(
        "res.flows.api.core.workflow.process_evictions_sql"
    )

    # if you want to hard code to and from dates ..nudge
    # payload["args"]["window_from"] = datetime(2023, 7, 1)
    # payload["args"]["window_to"] = datetime(2023, 7, 3)

    process_evictions_sql(payload, {})
    return {}


def ingest_missing_valid_bodies(event, context={}, watermark_days=2):
    """
    We have a proper flow for ingesting 3d bodies - this is a background task to matain 2d stuff only
    """
    res.utils.logger.info("ingesting missing valid bodies")
    with res.flows.FlowContext(event, context) as fc:
        cc = net_dxf_location_cache()
        # load everything from cache of 2d NET DXF fo;es
        stuff = cc.load()
        # get anything we have validated and found to be good
        stuff = stuff[stuff["validated_at"].notnull()]
        stuff["validated_at"] = pd.to_datetime(stuff["validated_at"], utc=True)
        # for this job only take recently validated things
        recently_validated = stuff[
            stuff["validated_at"] > res.utils.dates.utc_days_ago(watermark_days)
        ]

        try:
            # slack notify bodies failing
            report = recently_validated[recently_validated["is_valid"] == False]
            if len(report):
                if fc.args.get("send_slack", True):
                    r = (
                        report.reset_index()
                        .groupby(["body_code", "body_version"])[["_id"]]
                        .count()
                        .reset_index()
                        .rename(columns={"_id": "failing_sizes"})
                    )

                    owner = "Followers <@U01JDKSB196>"
                    s = f"Bodies failing NET DXF Validation {owner}\n"
                    slack = res.connectors.load("slack")
                    for row in r.to_dict("records"):
                        s += f"- {row['body_code']} V{row['body_version']} ({row['failing_sizes']} sizes failing)\n"

                    p = {
                        "slack_channels": ["autobots"],
                        "message": s,
                        "attachments": [],
                    }

                    if RES_ENV == "development":
                        slack(p)
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed in sending slack notification for failing body validation report {ex}"
            )

    newly_valid = recently_validated[recently_validated["is_valid"] == True]
    if len(newly_valid):
        hasura = res.connectors.load("hasura")
        res.utils.logger.info(
            f"Adding {len(newly_valid)} records from newly validated bodies"
        )
        # run the bootstrapper - this skips anything already added in 3d and uses the 2d NET DXF
        helpers._bootstrap_bodies(newly_valid, hasura=hasura)

        updated_bodies = list(newly_valid["body_code"].unique())
        res.utils.logger.info(
            f"Requesting updates to the styles for the updated {len(updated_bodies)} bodies"
        )
        for body in updated_bodies:
            _invoke_ingest_for_body(body)
    return {}


def exec_it(action, event):
    # some boiler plate here
    return action(event)


@res.flows.flow_node_attributes(memory="20Gi")
def six_hourly(event, context={}):
    """
    hourly we want to move things from other queues as temporary measure
    we keep some things eventually consistent with airtable
    """
    import traceback

    from res.flows.make.analysis import handler as healing_analysis
    from res.flows.make.production.utils import bootstrap_bridge_ones
    from res.flows.meta.ONE.queue_update import (  # MetaOneNode._reimport_style_rrm()
        populate_dxa_queues,
    )
    from res.flows.meta.ONE.style_node import MetaOneNode

    # from res.flows.sell.orders.process import reload_rrm
    from res.learn.agents.data.loaders import load_orders, reload_all

    try:
        s = bootstrap_bridge_ones(in_make_only=True)

    except Exception as ex:
        res.utils.logger.warn(f"Failed to repair the one bridge")

    try:
        res.utils.logger.info(
            f"importing res.flows.make.payment.order_cost_price_resolver ..."
        )
        from res.flows.finance.order_cost_price_resolver import OrderCostPriceResolver

        res.utils.logger.info(
            f"running OrderCostPriceResolver.check_and_fix_orders_for_recent_days ..."
        )
        resolver = OrderCostPriceResolver()
        resolver.check_and_fix_orders_for_recent_days()

    except Exception as ex:
        res.utils.logger.warn(
            f"check_and_fix_orders_for_recent_days failed {traceback.format_exc()}"
        )

    # create an rrm for this later
    healing_analysis({})

    return {}


@res.flows.flow_node_attributes(memory="50Gi")
def hourly(event, context={}):
    """
    hourly we want to move things from other queues as temporary measure
    we keep some things eventually consistent with airtable ....
    """
    import traceback

    from res.flows.make.production.queue_update import ingest_queue_data
    from res.flows.meta.ONE.queue_update import populate_dxa_queues
    from res.learn.agents.data.loaders import load_orders

    res.utils.logger.info("starting hourly maintenance...")

    """
    we hit the different bases here every hour - we are reprocessing a 24 hour window but its ok
    """

    try:
        from res.flows.meta.ONE.body_node import process_missing_plt_files

        res.utils.logger.info(f"running process_missing_plt_files ...")
        process_missing_plt_files()
    except:
        res.utils.logger.warn(
            f"Failed to update the plt stuff {traceback.format_exc()}"
        )

    failed = []
    try:
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "MakeNodeHourly", "ENTERED"
        )
        res.utils.logger.info(f"running ingest_queue_data ...")
        ingest_queue_data(
            since_date=res.utils.dates.utc_hours_ago(24), write_bridge_table=True
        )
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "MakeNodeHourly", "EXITED"
        )

    except:
        res.utils.logger.warn(
            f"failed to sync mone queue data {traceback.format_exc()}"
        )
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "MakeNodeHourly", "FAILED"
        )
        failed.append("Make Hourly Failed")

    try:
        res.utils.logger.info(f"importing brand_airtable_to_pg ...")
        from res.flows.finance.brands import brand_airtable_to_pg

        res.utils.logger.info(f"running brand_airtable_to_pg to sync brands ...")
        brand_airtable_to_pg.update_existing_brands_from_airtable_to_postgres()

    except Exception as ex:
        res.utils.logger.warn(f"brand_airtable_to_pg failed {traceback.format_exc()}")

    try:
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "DxaNode", "ENTERED"
        )
        res.utils.logger.info(f"running populate_dxa_queues ...")
        populate_dxa_queues(days_back=1)
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "DxaNode", "EXITED"
        )
    except:
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "DxaNode", "FAILED"
        )
        res.utils.logger.warn(f"failed to sync dxa data {traceback.format_exc()}")
        failed.append("Dxa Hourly Failed")
    # .
    try:
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "SellNode", "ENTERED"
        )
        res.utils.logger.info(f"running load_orders ...")
        load_orders(days_back=1, route="queue")
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "SellNode", "EXITED"
        )
    except:
        res.utils.logger.metric_node_state_transition_incr(
            "Observability", "SellNode", "FAILED"
        )
        res.utils.logger.warn(f"failed to sync dxa data {traceback.format_exc()}")
        failed.append("Sell Hourly Failed")

    try:
        res.utils.logger.info(f"running sync_meta_objects ...")
        sync_meta_objects({})
    except Exception as ex:
        res.utils.logger.warn(f"failed to sync meta objects {traceback.format_exc()}")
    # LATER Airtable and other dead letters

    try:
        from res.flows.meta.construction.quality import dump_quality

        res.utils.logger.info(f"running dump_quality ...")
        dump_quality()
    except Exception as ex:
        res.utils.logger.warn(f"failed to sync meta objects {traceback.format_exc()}")
    # LATER Airtable and other dead letters

    if len(failed):
        slack = res.connectors.load("slack")
        slack(
            {
                "slack_channels": ["flow-api"],
                "message": f"""One of the hourly observability jobs failed {failed}""",
            }
        )

    return {}


def sync_airtable_last_modified(event, context={}):
    """
    these are to support genome. its usually slowly change tables that are used in bodies. every two hours we can check for row changes
    """

    from res.flows.meta.construction import load_symbols
    from res.flows.meta.pieces import load_dictionary

    res.utils.logger.info(f"reloading piece dictionary")
    load_dictionary(reload=True)

    res.utils.logger.info(f"loading symbols dictionary")
    load_symbols(reload=True)


def try_ppp_flush():
    """
    this uses a stuck in ppp view to flush the queue of things that we failed to flush before.
    sometimes it requires a re-run or sometimes they might be really stuck
    """
    try:
        from res.flows.dxa.prep_ordered_pieces import try_flush_stuck

        try_flush_stuck()
    except Exception as ex:
        print(ex, "failing in try flush ppp job")


def refresh_assets_on_rolls_cache(hours_ago=4, reload_cache=False):
    """
    lookup of assets on rolls primarily to load pretreatment data
    bootstrap with
            udata = airtable["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"].to_dataframe(
                        fields=[
                            "rolls_ids",
                            "__order_number",
                            "Material Code",
                            "Nested Printfile Name",
                        ]
                    )
            udata = udata.rename(
                columns={
                    "__order_number": "one_number",
                    "record_id_y": "asset_count",
                    "rolls_ids": "roll_id",
                    "record_id_x": "record_id",
                    "Material Code": "material_code",
                    "Nested Printfile Name": "printfile_name",
                })
            s3.write(
                        "s3://res-data-platform/cache/assets_on_rolls.feather",
                        udata.reset_index(),
                    )
            udata
    """
    import re

    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")
    pdata = s3.read("s3://res-data-platform/cache/assets_on_rolls.feather")
    if reload_cache:
        udata = airtable["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"].updated_rows(
            fields=[
                "rolls_ids",
                "__order_number",
                "Material Code",
                "Nested Printfile Name",
            ],
            hours_ago=hours_ago,
            last_modified_field="Last Updated At",
        )
        udata = udata.rename(
            columns={
                "__order_number": "one_number",
                "rolls_ids": "roll_id",
                "Material Code": "material_code",
                "Nested Printfile Name": "printfile_name",
            }
        ).dropna()
        cols = list(udata.columns)
        all_data = (
            pd.concat([udata, pdata])
            .sort_values("__timestamp__")
            .drop_duplicates(subset=["record_id"])
        )

        new_cols = [
            "record_id",
            "__timestamp__",
            "one_number_count",
            "roll_id",
            "one_number",
            "material_code",
            "printfile_name",
        ]
        all_data = pd.merge(
            all_data[cols],
            all_data[cols].groupby("roll_id").count().reset_index(),
            on="roll_id",
            suffixes=["", "_count"],
        )[new_cols]

        all_data["printfile_name"] = all_data["printfile_name"].map(
            lambda x: x if pd.isnull(x) else re.sub("[^A-Za-z0-9_]+", "_", x)
        )

        s3.write(
            "s3://res-data-platform/cache/assets_on_rolls.feather",
            all_data[cols + ["one_number_count"]].reset_index(),
        )

        return all_data
    else:
        return s3.read("s3://res-data-platform/cache/assets_on_rolls.feather")


def reload_pretreatment_data(update_cache=False):
    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")

    if update_cache:
        res.utils.logger.info("reloading airtable data")
        data = airtable.get_table_data("apprcULXTWu33KFsh/tblIH1BBcRFsnYPEr")
        roll_pretreatement = data[
            [
                "Name",
                "1. Water (kg)",
                "2. Albaflow (ml)",
                "3. Prepajet PA-B (kg)",
                "4. Urea (kg)",
                "5. COTTON Sodium Carbonate (kg)",
                "5. SILK Sodium Bicarbonate (kg)",
                "6. Lyoprint Rg (kg)",
                "Rolls",
            ]
        ].rename(
            columns={
                "1. Water (kg)": "WATER_KG",
                "3. Prepajet PA-B (kg)": "PREPAJET_KG",
                "5. SILK Sodium Bicarbonate (kg)": "SILK_SODIUM_BICARBONATE_KG",
                "5. COTTON Sodium Carbonate (kg)": "COTTON_SODIUM_BICARBONATE_KG",
                "4. Urea (kg)": "UREA_KG",
                "6. Lyoprint Rg (kg)": "LYOPRINT_KG",
                "2. Albaflow (ml)": "ALBAFLOW_L",
            }
        )[
            [
                "Rolls",
                "WATER_KG",
                "PREPAJET_KG",
                "SILK_SODIUM_BICARBONATE_KG",
                "COTTON_SODIUM_BICARBONATE_KG",
                "UREA_KG",
                "LYOPRINT_KG",
                "ALBAFLOW_L",
            ]
        ]

        s3.write(
            "s3://res-data-platform/cache/pretreatment.feather",
            roll_pretreatement.reset_index(),
        )

    else:
        res.utils.logger.info("read from s3 cache")
        roll_pretreatement = s3.read(
            "s3://res-data-platform/cache/pretreatment.feather"
        )
    return roll_pretreatement


def sync_meta_objects(event={}, context={}):
    from res.connectors.airtable import AirtableConnector
    from res.flows.dxa.styles import queries
    from res.flows.meta.pieces import UPSERT_PIECE_COMPS, UPSERT_PIECE_NAMES

    hasura = res.connectors.load("hasura")
    with res.flows.FlowContext(event, context) as fc:
        watermark = fc.args.get("watermark_since", 2)
        airtable = res.connectors.load("airtable")
        #####
        ##      PIECE COMPONENTS
        #########
        res.utils.logger.info("Syncing piece components")
        data = airtable["appa7Sw0ML47cA8D1"]["tbllT2w5JpN8JxqkZ"].updated_rows(
            hours_ago=watermark, last_modified_field="platform_fields_modified"
        )
        data = res.utils.dataframes.rename_and_whitelist(
            data,
            columns={
                "Tag Abbreviation": "key",
                "Tag Category": "type",
                "Commercial Acceptability Zone": "commercial_acceptability_zone",
                "sew_symbols": "sew_symbol",
            },
        ).drop_duplicates(subset=["key"])

        if len(data) > 0:
            res.utils.logger.info(f"Saving {len(data)} records")
            # make an id from the key AND the type which makes it unique
            data["id"] = data.apply(
                lambda k: res.utils.uuid_str_from_dict(
                    {"key": k["key"], "type": k["type"]}, axis=1
                )
            )
            df = df.astype(object).replace(np.nan, None)

            # upsert to hasura
            hasura.execute_with_kwargs(
                UPSERT_PIECE_COMPS, piece_components=data.to_dict("records")
            )

        #####
        ##      PIECE NAMES
        #########
        res.utils.logger.info("Syncing piece names")
        data = airtable["appa7Sw0ML47cA8D1"]["tbl4V0x9Muo2puF8M"].updated_rows(
            hours_ago=watermark,
            last_modified_field="platform_fields_modified",
            fields=["Name", "Generated Piece Code"],
        )
        if len(data) > 0:
            res.utils.logger.info(f"Saving {len(data)} records")
            # make an id from the key
            data["id"] = data["key"].map(
                lambda k: res.utils.uuid_str_from_dict({"key": k})
            )
            # a little cleaning
            data = data[~data["key"].isin(["-", "-X"])].drop_duplicates(subset=["key"])
            # upsert to hasura
            data = res.utils.dataframes.replace_nan_with_none(data)
            hasura.execute_with_kwargs(
                UPSERT_PIECE_NAMES, piece_names=data.to_dict("records")
            )

        ######
        ###   SIZES
        ######

        res.utils.logger.info("Syncing sizes")
        data = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"].updated_rows(
            hours_ago=watermark,
            last_modified_field="platform_fields_modified",
            fields=["Size Chart", "Size Normalized", "_accountingsku"],
        )
        if len(data) > 0:
            res.utils.logger.info(f"Saving {len(data)} records")
            data = res.utils.dataframes.rename_and_whitelist(
                data,
                columns={
                    "Size Chart": "size_chart",
                    "_accountingsku": "key",
                    "Size Normalized": "size_normalized",
                },
            ).drop_duplicates(subset=["key"])
            data["id"] = data["key"].map(
                lambda k: res.utils.uuid_str_from_dict({"key": k})
            )
            data = res.utils.dataframes.replace_nan_with_none(data)
            hasura.execute_with_kwargs(
                queries.UPSERT_SIZES, sizes=data.to_dict("records")
            )

        #######
        ##   MATERIALS
        ######

        # res.utils.logger.info(f"Pulling the materials (not delta yet)")
        # data = AirtableConnector.get_airtable_table_for_schema_by_name(
        #     "make.material_properties"
        # ).rename(
        #     columns={
        #         "cuttable_width": "cuttable_width_inches",
        #         "offset_size": "offset_size_inches",
        #     }
        # )
        # data["id"] = data["key"].map(lambda k: res.utils.uuid_str_from_dict({"key": k}))
        # data = data.where(pd.notnull(data), None)
        # # data = data.astype(object).replace(np.nan, None)
        # data = data.drop(["record_id", "__timestamp__", "order_key"], 1)
        # data = res.utils.dataframes.replace_nan_with_none(data)
        # hasura.execute_with_kwargs(
        #     queries.UPSERT_MATERIALS, materials=data.to_dict("records")
        # )

        # res.utils.logger.info("Meta objects synced")


@res.flows.flow_node_attributes(memory="10Gi")
def daily(event, context={}):
    """
    daily tasks to generally keep things up to date
    - style valid is a way to store pieces for any styles that are valid in the database - we keep trying
    - ingesting missing bodies is the same for bodies, we do it before the style
    - purging airtable queues is a way to keep record limits in check
    """
    from warnings import filterwarnings

    try:
        from res.flows.infra.platform.observability import send_dxa_status_messages

        send_dxa_status_messages()
    except Exception as ex:
        res.utils.logger.warn(f"Failed sending dxa reports {ex}")

    try:
        from res.connectors.slack.SlackConnector import ingest_slack_channels

        ingest_slack_channels(relative_back=2)
    except Exception as ex:
        res.utils.logger.warn(f"Failed slack data sync {ex}")

    try:
        from res.flows.infra.platform.observability import the_slack_news

        the_slack_news({})
    except Exception as ex:
        res.utils.logger.warn(f"Failed slack news sync {ex}")

    try:
        from res.flows.finance.utils import sync_all_rates

        sync_all_rates({})
    except Exception as ex:
        res.utils.logger.warn(f"Failed rates data sync {ex}")

    try:
        reload_pretreatment_data(update_cache=True)
    except Exception as ex:
        res.utils.logger.warn(f"Failed pretreatment data sync {ex}")

    try:
        # every day make sure nothing stuck in the queue
        from res.flows.meta.ONE.style_node import retry_generate_meta_one

        retry_generate_meta_one({})
    except:
        pass
    filterwarnings("ignore")

    res.utils.logger.info("starting daily maintenance...")
    #  bodies - save all cached valid bodies to the database if they are not there already
    # read valid latest bodies from the cache and for each one check if it exists and is not in 3d and write it
    # we also need to recheck the list of bodies and update the cache to say its valid. may want to move the validation ti the place where we update the body later
    # strategy is to use this to trigger N jobs for each body that is newly validated and request the styles if they dont exist
    # exec_it(ingest_missing_valid_bodies, event)

    # this one pops a few of the list of the cached master list we make off line and submits payloads when things are missing
    # it may be we tried and failed and these need to be looked at separately. check the kafka response failures for this
    # exec_it(load_and_check_style_valid, event)

    res.utils.logger.info("flushing ppp...")
    try_ppp_flush()

    # purge airtable qs - evictions
    exec_it(purge_airtable_queues, event)

    return {}


@res.flows.flow_node_attributes(memory="50Gi")
def handle_batch_style_ingest(event, context={}):
    """
    this is actually a 2d flow use case to create 2dm1s while we move things to 3d
    it would be triggered on addition of valid bodies
    note we base flow='2d' below because the default is for 3d
    """
    from warnings import filterwarnings

    filterwarnings("ignore")

    from res.flows.meta.ONE.meta_one import MetaOne

    with res.flows.FlowContext(event, context) as fc:
        body = fc.args["body_code"]
        res.utils.logger.info(
            f"Building styles for body - check first its valid {body}"
        )
        s3 = res.connectors.load("s3")
        candidates = s3.read("s3://res-data-platform/cache/candidate_styles.feather")
        candidates = candidates[candidates["body_code"] == body]
        existing_styles = queries.list_active_styles()
        batch = []
        for record in candidates.to_dict("records"):
            for size in record["sizes"]:
                sku = f"{record['sku']} {size}"
                if sku not in existing_styles:
                    try:
                        res.utils.logger.info(f"Building {sku} for 2d flow")
                        MetaOne.build(sku, flow="2d")
                    except Exception as ex:
                        res.utils.logger.info(f"Failed {ex}")
                        batch.append({"record": sku, "error": str(ex)})

        res.utils.logger.info("Done - writing status")
        if len(batch) > 0:
            res.utils.logger.info(f"There were {len(batch)} build failures")
            s3.write(
                f"s3://res-data-platform/cache/candidate_styles_failed_{body}.csv",
                pd.DataFrame(batch),
            )

    return {}


def _invoke_ingest_for_body(body, test=False):
    """
    if we onboard a body we can invoke this flow to bulk load all styles for the body if they dont exist
    """
    from warnings import filterwarnings

    filterwarnings("ignore")
    try:
        event = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
            "infra.platform.main_jobs.handle_batch_style_ingest"
        )
        event["args"]["body_code"] = body

        if test:
            handle_batch_style_ingest(event)
        else:
            argo = res.connectors.load("argo")
            argo.handle_event(
                event, unique_job_name=f"ingest-body-{body}".lower().replace("_", "-")
            )
    except Exception as ex:
        res.utils.logger.warning(f"Failed invoke {ex}")


def _bodies_to_prod(bodies):
    for body in bodies:
        # create a template for the event
        event = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
            "infra.platform.main_jobs.handle_batch_style_ingest"
        )
        # pick a body
        event["args"]["body_code"] = body
        url = "https://data.resmagic.io/res-connect/flows/res-flow-node"
        res.utils.logger.info(event)
        # submit it on prod
        res.utils.safe_http.request_post(url, json=event)


@res.flows.flow_node_attributes(
    memory="16Gi",
)
def save_thumbnails(event, context={}):
    Q = """query MyQuery {
      meta_pieces {
        base_image_uri
        body_piece {
          key
        }
      }
    }
    """
    from PIL import ImageOps

    from res.media.images import make_square_thumbnail

    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")
    res.utils.logger.info("Loading piece uris from hasura...")
    data = hasura.execute_with_kwargs(Q)

    df = pd.DataFrame(data["meta_pieces"]).dropna()
    res.utils.logger.info(f"Loaded {len(df)} records")
    df["key"] = df["body_piece"].map(lambda x: x["key"])

    current = pd.DataFrame(
        s3.ls("s3://res-data-platform/images/pieces/color-thumbnails-sku")
    )
    current = list(current[0])

    # filter by the ones that are named how we like??
    # df = df[df['base_image_uri'].map(lambda x : 'V' in x.split('/')[-1])]
    # df.to_pickle('/Users/sirsh/Downloads/image_urls.pkl')

    for record in df.to_dict("records"):
        key = record["key"]
        uri = record["base_image_uri"]
        fkey = uri.split("/")[-3]
        body = "-".join(key.split("-")[:2])
        # save the SKU with size which we could ignore and then the piece name - we want to know the color really
        path_color = (
            f"s3://res-data-platform/images/pieces/color-thumbnails/{fkey}/{key}.png"
        )

        if path_color in current:
            # print("skip", path_color)
            continue

        try:
            im = make_square_thumbnail(uri)

            gim = ImageOps.invert(im.convert("L"))
            path_gray = (
                f"s3://res-data-platform/images/pieces/gray-thumbnails/{body}/{key}.png"
            )
            res.utils.logger.info(path_color)
            s3.write(path_color, im)
            res.utils.logger.info(path_gray)
            s3.write(path_gray, gim)
        except Exception as ex:
            res.utils.logger.warn(f"Failed: {res.utils.ex_repr(ex)}")
