from warnings import filterwarnings

filterwarnings("ignore")

import os
import pandas as pd
import numpy as np
import base64
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from datetime import datetime, timedelta
import re
import pytz
import io
import res
from res.flows import FlowEventProcessor
from res.utils.secrets import secrets
import requests
import json
from enum import Enum
from res.utils import logger

RECENTLY_ATTEMPTED_QUERY = """
    query get_latest_errors($started_from: timestamptz!) {
        dxa_flow_node_run(
            where: {
                # details: {_contains: $details}, 
                # preset_key: {_eq: "body_bundle_v0"}
                started_at: {_gte: $started_from},
                # status: {_nin: [COMPLETED_SUCCESS, NEW]}
            }, 
            order_by: {updated_at: desc}, 
        ) {
            id
            status
            preset_key
            details
            status_details
            created_at
            updated_at
            started_at
            ended_at
            auto_retries
        }
    }
"""

RETRY_QUERY = """
    mutation retry_job($id: uuid!, $status_details: String!) {
        update_dxa_flow_node_run_by_pk(
            pk_columns: {id: $id}, 
            _set: {status: NEW, status_details: $status_details}
            _inc: {auto_retries: 1}
        ) {
            id
        }
    }
"""

ADD_3D_JOB = """
	mutation AddNew3dModel($details: jsonb) {
	  insert_dxa_flow_node_run(objects: [{
	    type: DXA_EXPORT_ASSET_BUNDLE, 
	    preset_key: "3d_model_v0", 
	    details: $details,
        priority: -1,
	  }]) {
	    returning {
	      id
	      created_at
	    }
	  }
    }
"""

EXCEEDED_RETRIES_MUTATION = """
    mutation exceeded_retries($id: uuid!, $status_details: String!) {
        update_dxa_flow_node_run_by_pk(
            pk_columns: {id: $id},
            _set: {status: COMPLETED_ERRORS, status_details: $status_details}
        ) {
            id
        }
    }
"""

GET_TODAYS_JOBS = """
    query get_todays_jobs($from_date: timestamptz!, $to_date: timestamptz!) {
        dxa_flow_node_run(where: {
            _or: [
                {created_at: {_gte: $from_date, _lte: $to_date}},
                {started_at: {_gte: $from_date, _lte: $to_date}}
            ]
        }) {
            status
            status_details
            preset_key
            id
            created_at
            started_at
            ended_at
            details
        }
    }
"""


def should_retry(row):
    status = row["status"]
    started_at = row["started_at"]
    if status == "COMPLETED_SUCCESS" or status == "NEW":
        return False

    # make it utc
    threshold = datetime.utcnow() - timedelta(hours=1)
    threshold = pytz.UTC.localize(threshold)
    # fromisoformat only accepts 3 or 6 digits after the decimal
    started_at = re.sub(r"\.[0-9]*\+", ".000+", started_at)
    old = datetime.fromisoformat(started_at) < threshold

    if status == "IN_PROGRESS":
        return "hung" if old else False

    status_details = row["status_details"]
    if status == "COMPLETED_ERRORS" and not status_details:
        return "no reason given"

    # if "wider than" in status_details:
    #     return "wider algo changed"

    if "timed out after " in status_details:
        return "timeout"

    if "Failed to start" in status_details:
        return "failed to start"

    if "gateway" in status_details.lower():
        return "gateway timeout"

    if (
        "VStitcher did not complete successfully, maybe killed?".lower()
        in status_details.lower()
    ):
        return "vstitcher killed without error"

    if "VStitcher 2021.2 - " in status_details:
        return "got window text not popup"

    # if (
    #     "grading" in status_details
    #     or "close" in status_details
    #     or "sharp" in status_details
    # ):
    #     return "grading/close/sharp"

    return False


def set_exceeded_retries(row):
    logger.info(f"Exceeded retries for {row['id']}")
    status_details = row["status_details"] or ""
    if "exceeded auto retry limit" not in status_details:
        status_details = f"exceeded auto retry limit -- {status_details}"
    hasura = res.connectors.load("hasura")
    hasura.execute_with_kwargs(
        EXCEEDED_RETRIES_MUTATION, id=row["id"], status_details=status_details
    )


def retry_depending_on_circumstances(row):
    reason = should_retry(row)
    if reason:
        logger.info(f"Retrying {row['id']} because {reason}")
        new_status_details = f"retry: {reason} -- {row['status_details']}"
        hasura = res.connectors.load("hasura")
        hasura.execute_with_kwargs(
            RETRY_QUERY, id=row["id"], status_details=new_status_details
        )
        return reason
    return None


def retry_df(df, preset_key, testing=False):
    a = df.apply(lambda x: should_retry(x) != False, axis=1)
    retry_df = df[a]
    if len(retry_df) > 0:
        logger.info(f"Retrying {len(retry_df)} {preset_key} jobs")

        retry_df["why"] = retry_df.apply(should_retry, axis=1)
        logger.info(retry_df[["id", "why", "status_details", "auto_retries"]])

        if not testing:
            retry_df["retrying_because"] = df.apply(
                retry_depending_on_circumstances, axis=1
            )
        else:
            logger.warn("---Not retrying because testing=True---")
    else:
        logger.info(f"No {preset_key} retries needed")


def retry_latest_failures_for_by(
    all_df, preset_key, group_by, testing=False, auto_retry_limit=3
):
    df = all_df[all_df["preset_key"] == preset_key]

    df = (
        df.sort_values(by="updated_at", ascending=False)
        .groupby(group_by)
        .first()
        .reset_index()
    )
    df = df[df["status"] != "COMPLETED_SUCCESS"]

    if auto_retry_limit > 0:
        # log all the ones we're not retrying
        exceeded = df[df["auto_retries"] >= auto_retry_limit]
        if len(exceeded) > 0:
            logger.info("Limiting auto retries")
            logger.info(exceeded[["id", "auto_retries"]])
            if not testing:
                exceeded.apply(set_exceeded_retries, axis=1)
            else:
                logger.warn("---Not setting retries exceeded because testing=True---")

        df = df[df["auto_retries"] < auto_retry_limit]

    retry_df(df, preset_key, testing)


def bertha_retry_non_user_failed_jobs(
    event, context=None, from_date=None, testing=False, auto_retry_limit=3
):
    if testing:
        logger.warn("*** Won't retrying because testing=True ***")

    start = str(
        pd.Timestamp(from_date)
        if from_date
        else pd.Timestamp.now() - pd.Timedelta(hours=6)
    )
    logger.info(f"Getting jobs from {start}")
    hasura = res.connectors.load("hasura")
    jobs = hasura.execute_with_kwargs(RECENTLY_ATTEMPTED_QUERY, started_from=start)
    if not jobs.get("dxa_flow_node_run"):
        logger.info("No jobs found")
        return
    logger.info(f"Found {len(jobs['dxa_flow_node_run'])} jobs")

    df = pd.DataFrame.from_records(jobs["dxa_flow_node_run"])

    def extract_detail(key):
        f = lambda row: row["details"].get(key)
        return f

    df["style_id"] = df.apply(extract_detail(key="style_id"), axis=1)
    df["body_id"] = df.apply(extract_detail(key="body_id"), axis=1)
    df["body_code"] = df.apply(extract_detail(key="body_code"), axis=1)
    df["apply_color_request_id"] = df.apply(
        extract_detail(key="at_color_queue_record_id"), axis=1
    )

    args = {"testing": testing, "auto_retry_limit": auto_retry_limit}

    # retry bodies first
    retry_latest_failures_for_by(df, "body_bundle_v0", "body_id", **args)

    # same for styles
    retry_latest_failures_for_by(df, "techpack_v0", "style_id", **args)

    # finally, same for 3d_model_v0
    retry_latest_failures_for_by(df, "3d_model_v0", "style_id", **args)


class ChartGenerationType(Enum):
    PLOT = "plot"
    BASE64 = "base64"


def plot_status_chart(from_str=None, hours=24):
    return generate_status_chart(from_str, hours, ChartGenerationType.PLOT)


def get_base64_status_chart(from_str=None, hours=24):
    matplotlib.pyplot.switch_backend("Agg")
    return generate_status_chart(from_str, hours, ChartGenerationType.BASE64)


def generate_status_chart(
    from_str=None, hours=24, mode: ChartGenerationType = ChartGenerationType.PLOT
):
    if from_str is None:
        # get the start of the day in UTC
        from_str = pd.Timestamp.now(tz="utc").floor("d").isoformat()
    from_date = pd.to_datetime(from_str, utc=True)
    to_date = from_date + pd.Timedelta(hours=hours)

    res.utils.logger.info(f"Getting jobs from {from_date} to {to_date}")

    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        GET_TODAYS_JOBS, from_date=str(from_date), to_date=str(to_date)
    )
    df = pd.DataFrame(result["dxa_flow_node_run"])

    if len(df) == 0:
        print("no jobs found")
        return None

    done = len(df[df["ended_at"].notna()])
    remaining = len(df[df["ended_at"].isna()])

    # convert to datetime
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["started_at"] = pd.to_datetime(df["started_at"], utc=True)
    df["ended_at"] = pd.to_datetime(df["ended_at"], utc=True)

    created_to_started = (
        pd.to_datetime(df["started_at"], utc=True)
        - pd.to_datetime(df["created_at"], utc=True)
    ).mean()
    started_to_ended = (
        pd.to_datetime(df["ended_at"], utc=True)
        - pd.to_datetime(df["started_at"], utc=True)
    ).mean()

    # if it hasn't started yet, set the start date to the to_date
    df.loc[df["started_at"].isna(), "started_at"] = pd.Timestamp.now(tz="utc")
    df.loc[df["ended_at"].isna(), "ended_at"] = pd.Timestamp.now(tz="utc")

    # if any started after the to_date, set to to_date
    df.loc[df["started_at"] > to_date, "started_at"] = to_date
    df.loc[
        (df["ended_at"] < df["started_at"]) & (df["status"] == "IN_PROGRESS"),
        "ended_at",
    ] = pd.Timestamp.now(tz="utc").isoformat()
    df.loc[df["ended_at"] > to_date, "ended_at"] = to_date

    # if it was created before the from_date, set to from_date
    df.loc[df["created_at"] < from_date, "created_at"] = from_date

    # get these as seconds since from_date
    def secs_from_date(col, default_date):
        # if it's null set to the default date
        col = col.fillna(default_date)
        return (
            (pd.to_datetime(col, utc=True) - from_date).dt.total_seconds().astype(int)
        )

    df["created_at_secs"] = secs_from_date(df["created_at"], to_date)
    df["started_at_secs"] = secs_from_date(df["started_at"], to_date)
    df["ended_at_secs"] = secs_from_date(df["ended_at"], df["started_at"])
    # if any ended_at_secs are negative, set to from_date
    df.loc[df["ended_at_secs"] < 0, "ended_at_secs"] = df["started_at_secs"]

    # # if the diff between start and end is < 60s set to 60s
    # min_secs = 180
    # df.loc[df["ended_at_secs"] - df["started_at_secs"] < min_secs, "ended_at_secs"] = (
    #     df["started_at_secs"] + min_secs
    # )

    # just get time
    df["time"] = pd.to_datetime(df["created_at"]).dt.floor("S")

    # sort by created_at and reindex
    df = df.sort_values("created_at")
    df = df.reset_index(drop=True)
    # df.head()

    fig, ax = plt.subplots(figsize=(10, 8))
    left = df["created_at_secs"]

    # the left side of the bar tells us the time it took to start
    def status_to_wait_color(row):
        status = row["status"]
        if status == "NEW":
            return "#ADD8E6"
        if status == "IN_PROGRESS":
            return "#fed8b1"
        if status == "COMPLETED_ERRORS":
            return "#ffcccb"
        if status == "COMPLETED_SUCCESS":
            return "#90ee90"
        return "lightblue"

    colors = df.apply(status_to_wait_color, axis=1)
    # b1 = ax.barh(df.index, df['started_at_secs']- df['created_at_secs'], left=left, label='started', color='lightblue')
    b1 = ax.barh(
        df.index,
        df["started_at_secs"] - df["created_at_secs"],
        left=left,
        label=df["id"],
        color=colors,
    )

    # the right side of the bar tells us the time it took to complete
    def status_to_processing_color(row):
        status = row["status"]
        any_probs = row["status_details"]
        if status == "COMPLETED_SUCCESS":
            return "green" if any_probs is None else "#005a00"
        elif status == "COMPLETED_ERRORS":
            return "red"
        elif status == "IN_PROGRESS":
            return "orange"
        else:
            return "blue"

    colors = df.apply(status_to_processing_color, axis=1)
    b2 = ax.barh(
        df.index,
        df["ended_at_secs"] - df["started_at_secs"],
        left=df["started_at_secs"],
        label="ended",
        color=colors,
    )

    # for each row add text
    df["body_code"] = df["details"].apply(
        lambda x: x["body_code"] if x is not None else None
    )
    df["style_code"] = df["details"].apply(
        lambda x: x.get("style_code") if x is not None else None
    )

    for i, row in df.iterrows():
        t = ax.text(
            row["ended_at_secs"] + 5,
            i,
            row["style_code"] or row["body_code"],
            ha="left",
            va="center",
        )
        (
            t.set_fontweight("bold")
            if row["preset_key"] == "body_bundle_v0"
            else t.set_fontweight("normal")
        )
        t.set_color(status_to_processing_color(row))
        t.set_fontsize(6)

    # change the x axis to show time instead, half hour intervals
    ax.set_xticks(np.arange(0, hours * 60 * 60, 30 * 60))
    x_ticks = ax.get_xticks()
    x_labels = [
        f"{(from_date + pd.Timedelta(seconds=label)).time()}"
        for i, label in enumerate(x_ticks)
    ]
    ax.set_xticklabels(x_labels)
    plt.xticks(rotation=90)

    ax_top = ax.twiny()
    ax_top.set_xlim(ax.get_xlim())
    ax_top.set_xticks(x_ticks)
    ax_top.set_xticklabels(x_labels)

    # make x ticks vertical
    plt.xticks(rotation=90)

    plt.xlabel("seconds since " + str(from_date))
    plt.ylabel("job index")
    plt.title("job duration")

    # legend
    custom_lines = {
        "new": Line2D([0], [0], color="#ADD8E6", lw=4),
        "in progress": Line2D([0], [0], color="orange", lw=4),
        "success": Line2D([0], [0], color="green", lw=4),
        "error": Line2D([0], [0], color="red", lw=4),
    }

    ax.legend(custom_lines.values(), custom_lines.keys(), loc="lower right")

    if mode == ChartGenerationType.PLOT:
        plt.show()

        print(f"Average time to start: {created_to_started}")
        print(f"Average time to process: {started_to_ended}")
        print(f"Total requests: {len(df)}")
        print(f"Done: {done}")
        print(f"Remaining: {remaining}")
    elif mode == ChartGenerationType.BASE64:
        bytes = io.BytesIO()
        plt.savefig(bytes, format="png")
        bytes.seek(0)
        # base64_image = base64.b64encode(bytes.read()).decode()

        return bytes


def post_apply_dynamic_color(skus, test=False):
    """
    Given a list of skus, post a request to apply dynamic color to each sku
    It will get the latest apply color request and use that to generate the event
    """
    base = os.environ.get("RES_CONNECT_URL", "https://data.resmagic.io/res-connect")
    res_connect_endpoint = f"{base}/flows/res-flow-node"
    # if skus is a string separate by newline, else assume array of skus
    if isinstance(skus, str):
        skus = list(set([s.strip() for s in skus.split("\n") if s.strip()]))

    airtable = res.connectors.load("airtable")
    # s3 = res.connectors.load("s3")

    apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]

    responses = {}
    TOKEN = secrets.get_secret("RES_CONNECT_TOKEN")
    for sku in skus:
        try:
            sizes = ["all"]
            sku_parts = sku.split(" ")
            sku_to_search_for = None
            rec_to_search_for = None
            sizeless_sku = None

            # redo a apply color request record id or style id
            if sku.startswith("rec"):
                rec_to_search_for = sku_parts[0]
                # hacky I know
                if len(sku_parts) == 2:
                    sizes = sku_parts[1].split(",")
            # redo a specific apply color request
            elif len(sku_parts) == 5 and sku_parts[-1].startswith("(#"):
                sku_to_search_for = sku
            # assume it's a sku with a size
            elif len(sku_parts) == 4:
                sizeless_sku = " ".join(sku_parts[:3])
                sizes = [sku_parts[3]]
            elif len(sku_parts) == 3:  # assume it's a sizeless sku
                sizeless_sku = " ".join(sku_parts[:3])
                sku_to_search_for = sizeless_sku
            else:
                raise Exception(f"Could not parse sku {sku}")

            sku_filters = f"""OR(SEARCH("{sku_to_search_for}",{{Request Key}}))"""
            rec_filters = f"""OR(SEARCH("{rec_to_search_for}",{{_record_id}}),SEARCH("{rec_to_search_for}",{{Style ID}}))"""

            # default
            filters = rec_filters if rec_to_search_for else sku_filters

            fields = [
                "_record_id",
                "Body Version",
                "Request Auto Number",
                "Request Key",
                "Color Type",
            ]
            x = apply_color_requests.to_dataframe(filters=filters, fields=fields)

            if len(x) == 0:
                responses[sku] = f"Could not find apply color request for {sku}"
                logger.error(f"Could not find apply color request for {sku}")
                continue

            color_type = x["Color Type"].iloc[0]
            if color_type != "Default":
                responses[sku] = f"Color type is not default for {sku}"
                logger.error(f"Color type is not default for {sku}")
                continue

            # get the latest one
            x = x.sort_values("Request Auto Number", ascending=False)
            sizeless_sku = sizeless_sku or " ".join(
                x["Request Key"].iloc[0].split(" ")[:3]
            )

            id = sizeless_sku.replace(" ", "-").replace("_", "-")
            key = f"apply-dynamic-color-{id}-{res.utils.res_hash()}".lower()
            event = FlowEventProcessor().make_sample_flow_payload_for_function(
                "dxa.apply_dynamic_color", key=key
            )
            event["assets"] = []
            event["args"] = {
                "body_version": int(x["Body Version"].iloc[0]),
                "body_code": sizeless_sku.split(" ")[0],
                "sizes": sizes,
                # "sizes": ["base"], # quicker for testing
                "sizeless_sku": sizeless_sku,
                "apply_color_request_record_id": x["_record_id"].iloc[0],
                "publish_kafka": True,
                # "testing": True,
                "use_default_artwork": True,
            }

            if test:
                event_json = json.dumps(event, indent=2)
                responses[sku] = event_json
                logger.info(f"Test {sku} {event_json}")
                continue
            else:
                data = requests.post(
                    res_connect_endpoint,
                    json=event,
                    headers={"Authorization": f"Bearer {TOKEN}"},
                )
                logger.info(f"Posted {sku} HTTP:{data.status_code}")
                responses[sku] = data
        except Exception as e:
            logger.error(f"Error on {sku} {e}")
            responses[sku] = e
            pass

    return responses


def _create_swatch_pieces(
    body_code, body_version, size_codes, piece_key_no_size, graph_body_id
):
    from shapely.geometry import Polygon
    from res.flows.dxa.styles.helpers import _body_piece_id_from_body_id_key_size
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
    from res.flows.meta.body.unpack_asset_bundle.unpack_asset_bundle import (
        ADD_POM_FILE_3D,
    )
    import res.flows.meta.body.import_poms_from_rulers.import_poms_from_rulers as import_poms_from_rulers

    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")
    gql = ResGraphQLClient()

    GET_META_BODY = """
        query getMetaBody($code: String!, $version: numeric!) {
            meta_bodies(where: {body_code: {_eq: $code}, version: {_eq: $version}}) {
                id
                body_pieces {
                    id
                    size_code
                }
            }
        }
    """
    result = hasura.execute_with_kwargs(
        GET_META_BODY, code=body_code, version=body_version
    )
    if len(result["meta_bodies"]) == 0:
        raise Exception(f"Could not find meta body for {body_code} {body_version}")

    meta_body = result["meta_bodies"][0]
    hasura_body_id = meta_body["id"]

    existing_sizes = [p["size_code"] for p in meta_body["body_pieces"]]
    sizes_to_create = list(set(size_codes) - set(existing_sizes))
    all_sizes = list(set(size_codes + existing_sizes))

    logger.info(f"Existing sizes: {existing_sizes}")
    logger.info(f"Sizes needed: {size_codes}")
    logger.info(f"Sizes to create: {sizes_to_create}")

    sizes_table = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    sizes_df = sizes_table.to_dataframe()

    for size in sizes_to_create:
        logger.info(f"Creating size {size}")
        # get the size dimensions from airtable
        size_record = sizes_df[sizes_df["_accountingsku"] == size].iloc[0]

        DPI = 300
        (width, height) = (
            DPI * size_record["_inches_wide"],
            DPI * size_record["_inches_high"],
        )
        shape = Polygon([(0, 0), (width, 0), (width, height), (0, height)])

        # add it to meta_bodies
        shape_geojson = shape.boundary.__geo_interface__
        shape_geojson = {"type": "Feature", "geometry": shape_geojson}
        # json.dumps(shape_geojson)

        # piece_key_no_size = f"{body_code}-V1-MTRFXPNL-S"
        key = f"{piece_key_no_size}_{size}"
        piece_id = _body_piece_id_from_body_id_key_size(
            hasura_body_id, piece_key_no_size, size
        )

        # UPDATE_BODY_PIECE = """
        #     mutation addBodyPiece(
        #         $id: uuid!,
        #         $geojson: jsonb!,
        #     ) {
        #         update_meta_body_pieces_by_pk (pk_columns: {id: $id}, _set: {
        #             inner_geojson: $geojson,
        #             outer_geojson: $geojson,
        #         }) {
        #             id
        #         }
        #     }
        # """

        # hasura.execute_with_kwargs(
        #     UPDATE_BODY_PIECE,
        #     id=piece_id,
        #     geojson=shape_geojson,
        # )
        INSERT_BODY_PIECE = """
            mutation addBodyPiece(
                $id: uuid!,
                $body_id: uuid!,
                $key: String!,
                $piece_key: String!,
                $geojson: jsonb!,
                $size: String!,
            ) {
                insert_meta_body_pieces_one (object: {
                    id: $id,
                    type: "self",
                    body_id: $body_id,
                    key: $key,
                    size_code: $size,
                    vs_size_code: $size,
                    piece_key: $piece_key,
                    inner_geojson: $geojson,
                    outer_geojson: $geojson,
                }) {
                    id
                }
            }
        """

        hasura.execute_with_kwargs(
            INSERT_BODY_PIECE,
            id=piece_id,
            body_id=hasura_body_id,
            key=key,
            piece_key=piece_key_no_size,
            geojson=shape_geojson,
            size=size,
        )

    ##############################################################
    # create rulers
    ##############################################################
    data = {
        "index": [
            "Units: Centimeters",
            "Size",
            "Length of the Body",
            "Width of the Body",
        ]
    }
    x_if_nan = lambda x, v: x if not np.isnan(x) else v
    for size in all_sizes:
        row = sizes_df[sizes_df["_accountingsku"] == size].iloc[0]
        width_cms = x_if_nan(row.get("_inches_wide", 50), 50) * 2.54
        length_cms = x_if_nan(row.get("_inches_high", 36 * 15), 36 * 15) * 2.54
        data[size] = ["", row["Size Chart"], f"{length_cms:.1f}", f"{width_cms:.1f}"]

    rulers_df = pd.DataFrame(data).set_index("index")

    # write to csv but skip the first line and wrap everything in quotes
    s3_body_version = f"{body_code.lower().replace('-', '_')}/v{body_version}"
    s3_body_root = "s3://meta-one-assets-prod/bodies/3d_body_files"
    s3_dest_path = f"{s3_body_root}/{s3_body_version}/extracted"
    s3.write(f"{s3_dest_path}/rulers/rulers.csv", rulers_df, header=False, quoting=1)

    # register the poms
    rulers = [f for f in s3.ls(s3_dest_path) if "rulers/" in f]
    res.utils.logger.info(f"Registering POMs {rulers}")
    for ruler in rulers:
        logger.info(
            gql.query(
                ADD_POM_FILE_3D,
                {
                    "id": graph_body_id,
                    "input": {
                        "uri": ruler,
                        "bodyVersion": int(body_version),
                    },
                },
            )
        )

    # _, pom_errors =
    import_poms_from_rulers.handler(body_code, body_version)

    return all_sizes


def upsert_style(
    sku,
    brand_code,
    body_code,
    material_code,
    color_id,
    all_sizes=None,
    start_via_airtable=False,
):
    # Q: how come there's no body_version?
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
    import res.flows.meta.style.graphql_queries as style_graphql_queries
    import schemas.pydantic.style as style_schemas
    import res.flows.meta.apply_color_request.graphql_queries as graphql_queries

    airtable = res.connectors.load("airtable")
    gql = ResGraphQLClient()

    if all_sizes is None:
        GET_BODY_SIZES = """
            query getBody($number: String) {
                body(number: $number) {
                    availableSizes{
                        code
                    }
                }
            }
        """

        # get the body pieces and sizes
        result = gql.query(GET_BODY_SIZES, variables={"number": body_code})
        all_sizes = list(
            set([s["code"] for s in result["data"]["body"]["availableSizes"]])
        )

    GET_STYLE = """
        query GetStyle($code: String!) {
            style(code: $code) {
                id
                code
            }
        }
    """
    logger.info(f"Getting style for {sku}")
    style = gql.query(GET_STYLE, variables={"code": sku})
    if style["data"]["style"]:
        logger.info(f"Style {sku} already exists")
        style_id = style["data"]["style"]["id"]
    else:
        logger.info(f"Creating new style: {sku}")
        style = gql.query(
            style_graphql_queries.CREATE_STYLE,
            variables={"input": {"brandCode": brand_code}},
        )["data"]["createStyle"]["style"]
        style_id = style["id"]

        # Assign BMC (body, material color)
        style_update = gql.query(
            style_graphql_queries.UPDATE_STYLE,
            variables={
                "id": style_id,
                "input": {
                    "printType": style_schemas.PrintType.DIRECTIONAL.value,
                    "bodyCode": body_code,
                    "materialCode": material_code,
                    "colorId": color_id,
                },
            },
        )["data"]["updateStyle"]["style"]
        logger.info(f"Created new style: {style_update['code']}")

    ##############################################################
    # create an apply color request
    ##############################################################
    logger.info(f"Creating apply color request...")
    acr = gql.query(
        graphql_queries.CREATE_APPLY_COLOR_REQUEST,
        variables={
            "input": {
                "styleId": style_id,
                "styleVersion": 1,
                "assigneeEmail": "techpirates@resonance.nyc",
                "hasOpenOnes": False,
                "requestsIds": [],
                "requestType": "Brand Style Onboarding",
                "originalRequestPlacedAt": None,
                "priority": 0,
                "requesterEmail": "techpirates@resonance.nyc",
                "designDirection": None,
                "requestReasonsContractVariablesIds": [],
            }
        },
    )
    acr_id = acr["data"]["createApplyColorRequest"]["applyColorRequest"]["id"]

    logger.info(f"    Updating style record id... {style_id}")
    styles = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"]
    styles.update_record(
        {
            "record_id": style_id,
            "Style Editor Type": "3D Style Editor",
            "Print Type": "Directional",
            "Apply Color Flow Type": "Apply Dynamic Color Workflow",
            "Override ONE Ready": True,
        }
    )
    logger.info(f"    Updating style record id...done")

    logger.info(f"    Updating apply color request record id {acr_id}...")
    acr_status = "To Do" if start_via_airtable else "In Progress"
    apply_color_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    apply_color_requests.update_record(
        {
            "record_id": acr_id,
            "Apply Dynamic Color Status": acr_status,
            "Color Application Mode": "Automatically",
            "Apply Color Flow Type": "Apply Dynamic Color Workflow",
            "Color Type": "Default",
            "Meta ONE Sizes Required": all_sizes,
            # these can't be set
            # "Requires Turntable Brand Feedback": False,
            # "Apply Color Flow Status": "Done",
        }
    )
    logger.info(f"    Updating apply color request record id...done")

    return {"acr_id": acr_id, "style_id": style_id}


def add_swatch_style(
    body_code_and_version="CC-9050-V1",
    material_codes="COMCT",
    color_codes="NATUNM",
    size_codes=["0ZZOS"],
):
    """
    Adds a swatch with the dimensions in the size airtable, then generate
    a style for each color and material

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    This is unfinished and highly experimental, use at your own risk
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    """
    import res.flows.meta.style.graphql_queries as style_graphql_queries
    import schemas.pydantic.style as style_schemas
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
    import res.flows.meta.apply_color_request.graphql_queries as graphql_queries

    import res

    to_arr = lambda x: x if isinstance(x, list) else [x]
    color_codes = to_arr(color_codes)
    material_codes = to_arr(material_codes)
    size_codes = to_arr(size_codes)

    logger.info(
        f"*** add_swatch_style for {body_code_and_version} {material_codes} {color_codes} ***"
    )

    hasura = res.connectors.load("hasura")
    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")
    gql = ResGraphQLClient()

    GET_SIZE_ID = """query ($code: String!) { size(code: $code) { id } }"""
    size_ids = [
        gql.query(GET_SIZE_ID, variables={"code": size_code})["data"]["size"]["id"]
        for size_code in size_codes
    ]
    logger.info(f"Size ids: {size_ids}")

    parts = body_code_and_version.split("-")  # e.g. TT-9014-V1
    brand_code = parts[0]
    body_code = "-".join(parts[:2])
    body_version = int(parts[-1].split("V")[-1])
    piece_key_no_size = f"{body_code}-V{body_version}-MTRFXPNL-S"

    ##############################################################
    # update the body on the graph for the extra sizes
    ##############################################################
    GET_BODY = """
        query getBody($code: String!) {
            body(number: $code) {
                id
                code
                availableSizes {
                    id
                    code
                }
            }
        }
    """
    graph_body = gql.query(GET_BODY, variables={"code": body_code})
    availableSizeIds = [s["id"] for s in graph_body["data"]["body"]["availableSizes"]]

    UPDATE_BODY = """
        mutation updateBody($id: ID!, $input: UpdateBodyInput!){
            updateBody(id:$id, input:$input){
                body {
                    id
                }
            }
        }
    """

    graph_body_id = graph_body["data"]["body"]["id"]

    gql.query(
        UPDATE_BODY,
        variables={
            "id": graph_body_id,
            "input": {
                "availableSizeIds": list(set(size_ids + availableSizeIds)),
                "bodyPiecesIds": ["recXysGgoFqwHfmKw"],
            },
        },
    )

    ##############################################################
    # we may need to create extra pieces in meta.body_pieces
    ##############################################################
    all_sizes = _create_swatch_pieces(
        body_code, body_version, size_codes, piece_key_no_size, graph_body_id
    )

    ##############################################################
    # create the styles for each color/material
    ##############################################################
    s3_body_version = f"{body_code.lower().replace('-', '_')}/v{body_version}"

    for color_code in color_codes:
        for material_code in material_codes:
            sku = f"{body_code} {material_code} {color_code}"
            logger.info(f"Processing {sku}")

            source_root = (
                "s3://meta-one-assets-prod/bodies/3d_body_files/tt_9011/v1/extracted"
            )
            dest_root = f"s3://meta-one-assets-prod/color_on_shape/{s3_body_version}/{color_code.lower()}"

            s3.copy(f"{source_root}/3d.glb", f"{dest_root}/3d.glb")
            s3.copy(f"{source_root}/point_cloud.json", f"{dest_root}/point_cloud.json")

            ##############################################################
            # get the color id
            ##############################################################
            color_result = gql.query(
                """
                query getColor($code: String!) {
                    color(code: $code) {
                        id
                        code
                        activeArtworkFile {
                            file {
                                s3 {
                                    bucket
                                    key
                                }
                            }
                        }
                    }
                }
            """,
                variables={"code": color_code},
            )
            color = color_result["data"]["color"]
            color_id = color["id"]
            color_bucket = color["activeArtworkFile"]["file"]["s3"]["bucket"]
            color_key = color["activeArtworkFile"]["file"]["s3"]["key"]
            color_uri = f"s3://{color_bucket}/{color_key}"
            logger.info(f"Color uri: {color_uri}")

            ##############################################################
            # create a style & apply color request if it doesn't exist
            ##############################################################
            style_result = upsert_style(
                sku, brand_code, body_code, material_code, color_id, all_sizes
            )

            ##############################################################
            # get the meta artwork for this style
            ##############################################################
            GET_ARTWORK_BY_ORIGINAL_URI = """
                query GetArtworkBySourceId($uri: String) {
                    meta_artworks(where: {
                        original_uri: {_eq: $uri}
                    }, limit: 1) {
                        id
                        name
                    }
                }
            """

            artwork_result = hasura.execute_with_kwargs(
                GET_ARTWORK_BY_ORIGINAL_URI, uri=color_uri
            )["meta_artworks"][0]

            artwork_id = artwork_result["id"]
            logger.info(f"Artwork id: {artwork_id} ({artwork_result['name']})")

            ##############################################################
            # fire off a apply dynamic color job
            ##############################################################
            logger.info(f"Creating apply dynamic color job...")
            event = {
                "apiVersion": "v0",
                "kind": "resFlow",
                "metadata": {"name": "dxa.apply_dynamic_color", "version": "dev"},
                "args": {
                    "sizeless_sku": sku,
                    "sizes": size_codes,
                    "body_code": body_code,
                    "body_version": body_version,
                    "publish_kafka": True,
                    "use_default_artwork": False,
                    "apply_color_request_record_id": style_result["acr_id"],
                },
                "assets": [
                    {
                        "piece_key": piece_key_no_size,
                        "artworks": [
                            {
                                "artworkId": artwork_id,
                                "tiled": "true",
                                "verticalFraction": 0.5,
                                "horizontalFraction": 0.5,
                                "scale": 1,
                                "radians": 0,
                            }
                        ],
                    }
                ],
            }

            logger.info(f"Event: {event}")
            argo = res.connectors.load("argo")
            argo.handle_event(event)


def create_swatch(
    brand_code="TT",
    material_codes=["COMCT"],
    color_codes=["NATUNM"],
    size_codes=["0Y005"],
    base_size_code_index=0,
    name="Swatches",
    graph_categoryId="rec2RoG9C712ZcGMX",  # non wearable accessory
    graph_coverImageFileIds=["5fa1e8e564a5a21f9db86808"],
    create_style=False,
):
    """
    Creates a swatch body with the given brand, size, dimensions, color, and material

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    This is unfinished and highly experimental, use at your own risk
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    """
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
    from res.flows.dxa.styles.helpers import (
        _body_id_from_body_code_and_version,
    )
    import res.flows.meta.body_one_ready_request.body_one_ready_request as BodyOneReadyRequest
    from schemas.pydantic.meta import BodyMetaOneRequest, BodyMetaOneResponse
    from res.flows.meta.ONE.body_node import BodyMetaOneNode, UpsertBodyResponse
    from schemas.pydantic.body_one_ready_request import (
        CreateBodyOneReadyRequestInput,
        BodyOneReadyRequestType,
        RequestorLayer,
    )

    import res

    logger.info(f"*** creating swatch body for {brand_code} ***")

    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")
    gql = ResGraphQLClient()

    size_codes = [size_codes] if isinstance(size_codes, str) else size_codes

    GET_SIZE_ID = """query ($code: String!) { size(code: $code) { id } }"""
    size_id = gql.query(
        GET_SIZE_ID, variables={"code": size_codes[base_size_code_index]}
    )["data"]["size"]["id"]
    logger.info(f"Using {size_id} ({size_codes[base_size_code_index]}) for base size")

    size_ids = [
        gql.query(GET_SIZE_ID, variables={"code": size_code})["data"]["size"]["id"]
        for size_code in size_codes
    ]

    ##############################################################
    # create a new body via graph
    ##############################################################
    CREATE_BODY = """
        mutation createBody($input: CreateBodyInput!) {
            createBody(input: $input) {
                body {
                    id
                    name
                    code
                    brand{
                        id
                        code
                    }
                }
            }
        }
    """

    graph_body = gql.query(
        CREATE_BODY,
        variables={
            "input": {
                "name": name,
                "brandCode": brand_code,
                "categoryId": graph_categoryId,
                "coverImagesFilesIds": graph_coverImageFileIds,
                "campaignsIds": [],
                "onboardingMaterialsIds": [],
                "createdByEmail": "techpirates@resonance.nyc",
            }
        },
    )

    UPDATE_BODY = """
        mutation updateBody($id: ID!, $input: UpdateBodyInput!){
            updateBody(id:$id, input:$input){
                body {
                    id
                }
            }
        }
    """

    body_code = graph_body["data"]["createBody"]["body"]["code"]
    graph_body_id = graph_body["data"]["createBody"]["body"]["id"]

    gql.query(
        UPDATE_BODY,
        variables={
            "id": graph_body_id,
            "input": {
                "basePatternSizeId": size_id,
                "availableSizeIds": size_ids,
                "bodyPiecesIds": ["recXysGgoFqwHfmKw"],
            },
        },
    )

    body_version = 1

    ##############################################################
    # copy over the 3d files from the tt_9011 body
    ##############################################################
    s3_body_version = f"{body_code.lower().replace('-', '_')}/v{body_version}"
    s3_body_root = "s3://meta-one-assets-prod/bodies/3d_body_files"
    s3_dest_path = f"{s3_body_root}/{s3_body_version}/extracted"
    s3_source_path = f"{s3_body_root}/tt_9011/v1/extracted"

    s3.copy(f"{s3_source_path}/3d.glb", f"{s3_dest_path}/3d.glb")
    s3.copy(f"{s3_source_path}/point_cloud.json", f"{s3_dest_path}/point_cloud.json")

    ##############################################################
    # create a new body via hasura
    ##############################################################
    hasura_body_id = _body_id_from_body_code_and_version(body_code, body_version)
    INSERT_BODY = f"""
        mutation addBody(
            $id: uuid!,
            $body_code: String!,
            $brand_code: String!,
            $body_version: numeric,
            $metadata: jsonb,
        ) {{
            insert_meta_bodies_one (object: {{
                id: $id,
                body_code: $body_code,
                brand_code: $brand_code,
                version: $body_version,
                profile: "default",
                metadata: $metadata,
                model_3d_uri: "{s3_dest_path}/3d.glb",
                point_cloud_uri: "{s3_dest_path}/point_cloud.json",
            }}) {{
                id
            }}
        }}
    """

    hasura.execute_with_kwargs(
        INSERT_BODY,
        id=hasura_body_id,
        body_code=body_code,
        brand_code=brand_code,
        body_version=body_version,
        metadata={"autogenerated": True, "flow": "3d"},
    )

    _create_swatch_pieces(
        body_code,
        body_version,
        size_codes,
        f"{body_code}-V{body_version}-MTRFXPNL-S",
        graph_body_id,
    )

    ##############################################################
    # create a body update request & response immediately (i.e. not using Kafka to wait)
    ##############################################################
    borr = BodyOneReadyRequest.create_body_one_ready_request(
        CreateBodyOneReadyRequestInput(
            **{
                "body_id": graph_body_id,
                "requested_by_email": "techpirates@resonance.nyc",
                "body_one_ready_request_type": BodyOneReadyRequestType.BODY_ONBOARDING.value,
                "requestor_layers": [RequestorLayer.PLATFORM.value],
                "body_design_notes": "*** automatically created ***",
            }
        )
    )["bodyOneReadyRequest"]

    # BodyMetaOneNode.move_body_request_to_node_for_contracts(
    #     borr["id"],
    #     body_meta_one_contracts_failed,
    #     current_status=borr["status"],
    # )
    from datetime import datetime

    iso_now = datetime.utcnow().isoformat()

    # request = BodyMetaOneRequest(
    #     body_code=body_code,
    #     body_version=body_version,
    #     body_one_ready_request=borr["id"],
    #     contract_failure_context={},
    #     contracts_failed=[],
    #     created_at=iso_now,
    #     flow_id=None,
    #     id=None,  # ??? optional so setting to none
    #     metadata={},
    #     mode=None,
    #     pieces=[{"key": f"{body_code}-V{body_version}-MTRFXPNL-S"}],
    #     size_code=size_codes[base_size_code_index],
    #     sample_size=size_codes[base_size_code_index],
    #     sizes=size_codes,
    # )
    # node = BodyMetaOneNode(
    #     request,
    #     response_type=BodyMetaOneResponse,
    #     queue_context=res.flows.FlowContext({}),
    # )
    # node._save(request)

    data = {
        "body_code": body_code,
        "body_version": body_version,
        "user_email": "sirsh@a.b.io",
        "process_id": "auto_bertha",
        "body_file_uploaded_at": iso_now,
    }

    data = BodyMetaOneNode.upsert_response(UpsertBodyResponse(**data))

    # now the body is created, add a piece for each size, and a style for each color/material combo
    if create_style:
        add_swatch_style(
            body_code_and_version=f"{body_code}-V{body_version}",
            material_codes=material_codes,
            color_codes=color_codes,
            size_codes=size_codes,
        )
