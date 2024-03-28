from datetime import datetime
import pandas as pd
import urllib
import res
from res.flows import FlowContext
from res.utils.dataframes import _to_snake_case
from res.connectors.airtable import AirtableConnector
from tenacity import retry, wait_fixed, stop_after_attempt
from res.utils.dataframes import rename_and_whitelist

# legacy
PRINTER_DATA_TOPIC = "printJobData"
TOPIC = "res_premises.printer_data_collector.print_job_data"
USE_KGATEWAY = True
RESPONSE_TOPIC = "res_make.printer.printfile_events"
VALID_STATUSES = ["1"]


def millimeters_to_yards(m):
    if m:
        return (int(m) * 1.09361) / 1000


def inches_to_yards(inches):
    if inches:
        return inches * 0.0277778


def unpack(df, completed=True, **kwargs):
    """
    unpacks the printer topic data into something useful
    """

    def roll_part(k):
        parts = k.split("_")
        if len(parts) > 2:
            return f"{parts[1]}_{parts[2]}"

    df = pd.DataFrame([r for r in df["MSjobinfo"].dropna()]).drop_duplicates(
        subset=["Name"]
    )

    if len(df) == 0:
        return df

    res.utils.logger.info(f"Unpacking {len(df)} nest records.")

    df["is_roll_format"] = df["Name"].map(
        lambda x: "_R" in x and "_RGB" not in x and "JOB" in x
    )
    df = df[df["is_roll_format"]].reset_index(drop=True)
    df["key"] = df["Name"].map(urllib.parse.unquote)
    df["start_ts"] = pd.to_datetime(df["StartTime"], unit="s")
    df["end_ts"] = pd.to_datetime(df["EndTime"], unit="s")
    df["roll_part"] = df["key"].map(roll_part)
    df = df[df["roll_part"].notnull()].reset_index(drop=True)

    if len(df) == 0:
        return df

    res.utils.logger.info(
        f"Unpacking {len(df)} nest records that confirm to the roll name format."
    )

    df["job"] = df["key"].map(lambda s: s[s.index("JOB") :].replace("_", " #"))

    df["material_code"] = df["Media"]

    df["nest_key"] = df.apply(
        lambda row: f"{row['job']}{row['roll_part']}: {row['material_code']}", axis=1
    )

    def remove_job(s):
        return s[: s.index("_J")]

    df["roll"] = df["roll_part"].map(lambda x: x.split(":")[0].split("_")[0])

    df = df[
        [
            "Jobid",
            "nest_key",
            "roll",
            "roll_part",
            "key",
            "end_ts",
            "start_ts",
            "Media",
            "job",
            "RequestedLength",
            "PrintedLength",
        ]
    ]
    if completed:
        df = df.dropna().reset_index().drop("index", 1)

    return df


def merge_nest_data(data, fc, return_printed_nests=False):
    """
    given printer data we load the nest data and all the stats that we have already saved for it
    we merge this and save them to an entity and then snowflake
    later roll level aggregates can be computed

    This depends on the nest statistics at style level AND on the printer event joining the nest
    We need to be able to lookup the stat quickly by job id (e.g. JOB# 123456 or job key from nest argo job
    Once we have the nest key we need to lookup the stats data

    Params:
        Return printed nests (without stats)

    """

    from res.flows.dxa.printfile import ResNestV1

    def add_nest_stats(data, stats_lookup):
        def try_get(k):
            try:
                p = stats_lookup.get(k)

                # mode where we lookup existing
                return s3.read(p)

                # node where we generate fresh but need a schema re-alignment
                return ResNestV1(p).get_nest_evaluation()
            except:
                return {}

        stats = data["nest_job_key"].map(try_get)
        stats = pd.DataFrame([d for d in stats]).dropna()
        res.utils.logger.info(
            f"merging in {len(stats)} stats on job key; some keys are {list(stats['job_key'])[:10]}\n we want to join them to keys like {list(data['nest_job_key'])[:5]}"
        )
        data = pd.merge(data, stats, left_on="nest_job_key", right_on="job_key")

        return data

    airtable = fc.connectors["airtable"]
    s3 = fc.connectors["s3"]

    match_nest = airtable.get_airtable_table_for_schema_by_name("make.nest_assets")
    # we can load on demand with the nest object; or we can preprocess every nest
    known_stats = {
        f.split("/")[-2]: f
        for f in list(
            s3.ls(
                "s3://res-data-production/flows/v1/dxa-printfile/expv2/style_nest_evaluation",  # style_nest_evaluation, perform_nest_compute
                # TODO: water marker can be changed - this should be temporary
                modified_after=res.utils.dates.relative_to_now(30),
            )
        )
    }

    res.utils.logger.info(
        f"Read {len(known_stats)} stats files and {len(match_nest)} nest records; these will be joined to {len(data)} printer data records"
    )

    printed_nest_data = match_nest[
        ["key", "asset_path", "compensation_x", "compensation_y", "ripped_filename"]
    ].reset_index()

    printed_nest_data["job"] = printed_nest_data["key"].map(lambda x: x.split("_")[0])
    printed_nest_data["nest_job_key"] = printed_nest_data["asset_path"].map(
        lambda x: x.split("/")[-1] if pd.notnull(x) else None
    )

    printed_nest_data = pd.merge(
        data, printed_nest_data, left_on="key", right_on="ripped_filename"
    )

    res.utils.logger.info(
        f"merged printed nest data of length {len(printed_nest_data)}"
    )

    printed_nest_data["stats"] = printed_nest_data["nest_job_key"].map(
        lambda x: known_stats.get(x)
    )

    printed_nest_data = printed_nest_data[
        [
            "roll",
            "start_ts",
            "end_ts",
            "nest_job_key",
            "RequestedLength",
            "PrintedLength",
            "Media",
            "stats",
            "key_y",
            "key_x",
            "compensation_x",
            "compensation_y",
        ]
    ]

    # convert the start and end time to non unix and convert the yards from meters
    for c in ["start_ts", "end_ts"]:
        printed_nest_data[c] = pd.to_datetime(printed_nest_data[c], unit="s")

    for c in ["RequestedLength", "PrintedLength"]:
        printed_nest_data[c] = printed_nest_data[c].map(millimeters_to_yards)

    printed_nest_data = printed_nest_data.rename(
        columns={
            "start_ts": "print_started_at",
            "end_ts": "print_ended_at",
            "RequestedLength": "requested_length",
            "PrintedLength": "printed_length",
            "key_y": "print_nest_key",
            "key_x": "printer_data_key",
            "Media": "printed_material_code",
        }
    )

    if return_printed_nests:
        return printed_nest_data

    # here we are looking up the nest data without worrying about if its been pre-processed
    # in future the ETL should be enriched to validate and prc up front
    # this is all nests with printer data and the geom statistics on the nest
    return add_nest_stats(printed_nest_data, known_stats)

    printed_nest_data = (
        printed_nest_data[printed_nest_data["stats"].notnull()]
        .reset_index()
        .drop("index", 1)
    )

    def read_with_file_key(f):
        df = s3.read(f)
        df["file_key"] = f
        return df

    def read_all_with_file_key(files):
        return (
            pd.concat([read_with_file_key(f) for f in files])
            .reset_index()
            .drop("index", 1)
        )

    if not len(printed_nest_data):
        return printed_nest_data

    stats_data = read_all_with_file_key(printed_nest_data["stats"].unique())

    data = pd.merge(
        printed_nest_data,
        stats_data,
        left_on="stats",
        right_on="file_key",
        suffixes=["_header", ""],
    )

    return data


def process_roll_nest_statistics(event, context=None):
    def filter_printer_rows_by_date(row):
        try:
            ts = row.get("MSjobinfo", {}).get("StartTime")
            return datetime.utcfromtimestamp(ts) > res.utils.dates.relative_to_now(14)
        except:
            return False

    with FlowContext(event, context) as fc:
        poll_timeout = fc.args.get("consumer_poll_timeout", 100)
        # can try setting this from args too
        topic_config = {"auto.offset.reset": "latest"}
        kafka = res.connectors.load(
            "kafka", group_id="resflow_print_node", topic_config=topic_config
        )
        s3 = fc.connectors["s3"]
        snowflake = fc.connectors["snowflake"]
        consumer = kafka[TOPIC]
        watermarker_filter = {"created_at": res.utils.dates.relative_to_now(14)}

        data = consumer.consume(
            # filter=filter_printer_rows_by_date,
            timeout_poll_avro=poll_timeout,
        )
        # printer data
        data = unpack(data)

        if len(data):
            # add on nest data and stats
            data = merge_nest_data(data, fc)

            entity_name = "roll_nest_statistics"
            res.utils.logger.info(f"Ingesting entity {entity_name} to snowflake")

            s3.write_date_partition(
                data,
                "print_started_at",
                entity=entity_name,
                dedup_key="nest_job_key",
            )

            stage_root = f"s3://res-data-platform/entities/{entity_name}/ALL_GROUPS/"
            snowflake.ingest_from_s3_path(stage_root=stage_root, name=entity_name)


def make_key(row):
    return f"{row['Printer']}_{row['StartTime']}_{row['EndTime']}"


def relay_print_job_events(df, fc, ignore_root="MSjobinfo"):
    """
    Example:
     df = s3.read('s3://res-kafka-out-production/topics/printJobData/partition=0/printJobData+0+0000109407.avro')
     relay_print_job_events(df, FlowContext({},{}),ignore_root='MSjobinfo')
    """

    if ignore_root:
        df = pd.DataFrame([r[ignore_root] for r in df.to_dict("records")])

    # back compat on bad data
    if "Jobid" not in df:
        df["Jobid"] = "MISSING"

    kc = fc.connectors["kafka"]

    df["Key"] = df.apply(make_key, axis=1)
    df["StartTime"] = pd.to_datetime(df["StartTime"], unit="s").astype(str)
    df["EndTime"] = pd.to_datetime(df["EndTime"], unit="s").astype(str)

    material_usage = df[
        [
            "Key",
            "Printer",
            "Jobid",
            "Name",
            "StartTime",
            "EndTime",
            "Media",
            "HeadGap",
            "Width",
            "RequestedLength",
            "PrintedLength",
            "FoldDetected",
        ]
    ]

    df["InkUsage"] = df["MSinkusage"].apply(lambda x: x["Ink"])
    ink_usage = df.explode("InkUsage")[
        [
            "Key",
            "StartTime",
            "EndTime",
            "Printer",
            "Jobid",
            "Name",
            "RequestedLength",
            "PrintedLength",
            "InkUsage",
        ]
    ]
    ink_usage["Color"] = ink_usage["InkUsage"].map(lambda x: x["Color"])
    ink_usage["ToPrint"] = ink_usage["InkUsage"].map(lambda x: x["Usage"]["ToPrint"])
    ink_usage["ToClean"] = ink_usage["InkUsage"].map(lambda x: x["Usage"]["ToClean"])
    ink_usage = ink_usage.drop("InkUsage", 1)

    ink_usage["Key"] = ink_usage.apply(
        lambda row: f"{row['Key']}_{row['Color']}", axis=1
    )

    res.utils.logger.info(
        f"Publishing material usage records of length {len(material_usage)}"
    )
    kc["printerMaterialUsage"].publish(
        material_usage, dedup_on_key="Key", use_kgateway=False, coerce=True
    )

    res.utils.logger.info(
        f"Publishing ink usage records of length {len(material_usage)}"
    )
    kc["printerInkUsage"].publish(
        ink_usage, dedup_on_key="Key", use_kgateway=False, coerce=True
    )


def handler(event, context):
    with FlowContext(event, context) as fc:
        kc = fc.connectors["kafka"]
        data = kc[PRINTER_DATA_TOPIC].consume()

        data = data[data["MSjobinfo"].notnull()]

        relay_print_job_events(data, fc, ignore_root="MSjobinfo")


"""
preceding is some legacy stuff we may or may not used
"""


@retry(wait=wait_fixed(3), stop=stop_after_attempt(4))
def lookup_print_file_asset(name):
    """
    name: rip file name
    e.g. CFTGR_R11572_1_004_optimus_printer_2022_04_01T04_24_02_577376Z
    """
    airtable = res.connectors.load("airtable")
    tab = airtable["apprcULXTWu33KFsh"]["tblAQcPuKUDVfU7Fx"]
    res.utils.logger.info(f"Looking up asset path for print file {name}")
    d = tab.to_dataframe(
        filters=AirtableConnector.make_key_lookup_predicate([name], "RIP File Name")
    )
    name = d.iloc[0]["Asset Path"]
    res.utils.logger.info(f"found {name}")
    return name


def get_printed_assets(
    data, printed_length, timestamp, printer_status_code="1", **kwargs
):
    """
    for example:
        from res.utils import res_hash
        from res.flows.make.printer_events_relay import *
        s3 =res.connectors.load('s3')
        data = s3.read('s3://res-data-development/flows/v1/make-nest-stitch_printfiles/primary/concat/stitch-r23207--ly115-1-1657651038/manifest.feather')
        data['piece_id'] = data.apply(lambda row : res_hash(f"{row['asset_id']}{row['piece_name']}".encode()),axis=1)
        data.set_index('piece_id')

        example = get_printed_assets(data, 100, res.utils.dates.utc_now_iso_string())
        example

    """

    # data = data[data["max_y_inches"] < printed_length].reset_index()

    data["node"] = "printer"
    data["asset_type"] = "make_pieces"
    # do we trust the print or not
    data["status"] = "TERMINATED" if printer_status_code != "1" else "EXIT"
    # if in dont check some function (we need more than this)
    data.loc[
        data["max_y_inches"].map(inches_to_yards) < printed_length, "status"
    ] = "EXIT"
    data["event_datetime"] = timestamp
    data["observation_confidence"] = 1.0  # or a function of distance from the edge

    data["metadata"] = data.apply(
        lambda x: {
            "maxy_yards": inches_to_yards(x["max_y_inches"]),
            "printed_length": printed_length,
            "printer_status_code": printer_status_code,
        },
        axis=1,
    )

    data = rename_and_whitelist(
        data,
        {
            "asset_key": "one_number",
            "asset_id": "asset_request_id",
            "piece_id": "id",
            "piece_name": "name",
            "asset_type": "asset_type",
            "status": "status",
            "node": "node",
            "event_datetime": "event_datetime",
            "observation_confidence": "observation_confidence",
            "metadata": "metadata",
        },
    ).to_dict("records")

    return data


def get_print_assets_from_print_event(event):
    s3 = res.connectors.load("s3")
    printed_length = event.get("printed_length")
    # we set this first in processing input
    asset_path = event.get("stitched_file_path")
    if not asset_path:
        return None
    manifest_path = f"{asset_path}/manifest.feather"
    data = s3.read(manifest_path)
    return get_printed_assets(
        data,
        printed_length,
        printer_status_code=event.get("status", "1"),
        timestamp=event["end_time"],
    )


def relay_printed_assets(response_event):
    try:
        kafka = res.connectors.load("kafka")
        assets = get_print_assets_from_print_event(response_event)
        if assets is not None:
            kafka["res_make.piece_tracking.make_asset_status"].publish(
                assets, use_kgateway=USE_KGATEWAY, coerce=True
            )
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to create print assets - check the stitched print file exists "
        )
        raise ex


def handle_new_event(event, fc):
    """
    handle events from print events to write to new flat events
    we use this process to track printed pieces (experimental)
    """
    try:
        kafka = res.connectors.load("kafka")
        MSjobinfo = event.get("MSjobinfo")
        if MSjobinfo:
            data = {
                _to_snake_case(k): v
                for k, v in MSjobinfo.items()
                if k not in ["MSinkusage"]
            }

            for field in ["printed_length", "requested_length", "width"]:
                data[field] = millimeters_to_yards(data[field])
            for field in ["start_time", "end_time"]:
                if data.get(field):
                    data[field] = datetime.utcfromtimestamp(
                        int(float(data[field]))
                    ).isoformat()
            name_parts = data["name"].split("_") if data.get("name") else []
            data["roll_name"] = name_parts[1] if len(name_parts) > 1 else None
            try:
                data["stitched_file_path"] = lookup_print_file_asset(data["name"])
                data["stitching_job_key"] = data.get("stitched_file_path", "").split(
                    "/"
                )[-1]
            except Exception as ex:
                res.utils.logger.warn(
                    f"fail resolve stitched files {res.utils.ex_repr(ex)}"
                )
                data["stitched_file_path"] = None
                data["stitching_job_key"] = None

            data["roll_key"] = (
                "_".join(name_parts[1:3]) if len(name_parts) > 2 else None
            )

            data["metadata"] = {}

            kafka[RESPONSE_TOPIC].publish(data, use_kgateway=USE_KGATEWAY, coerce=True)

            if data.get("end_time") and data["status"] in VALID_STATUSES:
                relay_printed_assets(data)
            else:
                res.utils.logger.info(
                    f"We had a message but the status or times were not valid so we have nothing to relay on the piece asset status"
                )

        else:
            res.utils.logger.warn("Empty payload child")

        return data
    except Exception as ex:
        res.utils.logger.warn(f"error handling {data}: {res.utils.ex_repr(ex)}")
        raise ex
        fc.queue_exception_metric_inc()


EXAMPLE_INPUT = {
    "MSjobinfo": {
        "Printer": "nova",
        "Jobid": "47512",
        "Name": "LY115_R23207_0_001_optimus_printer_2022_07_12T18_12_26",
        "Status": "1",
        "PrintMode": "D2: 4passes 9levels StdSpeed HQ",
        "Direction": "Bidi",
        "Media": "CC254",
        "HeadGap": "2.000",
        "Width": "1526",
        "RequestedLength": "1176",
        "StartTime": "1657673530",
        "EndTime": "1657673703",
        "PrintedLength": "1271",
        "FoldDetected": "0",
        "MSinkusage": {
            "Ink": [
                {
                    "Color": "Cyan",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.260", "ToClean": "0.000"},
                },
                {
                    "Color": "Magenta",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.109", "ToClean": "0.000"},
                },
                {
                    "Color": "Yellow",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.154", "ToClean": "0.000"},
                },
                {
                    "Color": "Black",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "1.523", "ToClean": "0.000"},
                },
                {
                    "Color": "Red",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.058", "ToClean": "0.000"},
                },
                {
                    "Color": "Orange",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.058", "ToClean": "0.000"},
                },
                {
                    "Color": "Blue",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.144", "ToClean": "0.000"},
                },
                {
                    "Color": "Transparent",
                    "Type": "[R] Kiian Bellagio",
                    "Cleanings": {"Soft": "0", "Normal": "0", "Strong": "0"},
                    "Usage": {"ToPrint": "0.963", "ToClean": "0.000"},
                },
            ]
        },
    }
}


EXAMPLE_RESPONSE = {
    "printer": "nova",
    "jobid": "47512",
    "name": "CFTGR_R11555_1_001_optimus_printer_2022_07_08T20_22_55",
    "status": "1",
    "print_mode": "D2: 4passes 9levels StdSpeed HQ",
    "direction": "Bidi",
    "media": "CC254",
    "head_gap": "2.000",
    "width": 1.66884886,
    "requested_length": 1.28608536,
    "start_time": "2022-07-13T00:52:10",
    "end_time": "2022-07-13T00:55:03",
    "printed_length": 1.38997831,
    "fold_detected": "0",
    "roll_name": "R23222",
    "roll_key": "R23222_3",
}
