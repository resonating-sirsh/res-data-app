from res.connectors import s3
from numpy.core.records import record
from numpy.lib.arraysetops import isin
import pandas as pd
from . import AIRTABLE_API_ROOT, AIRTABLE_LATEST_S3_BUCKET, get_airtable_request_header
from . import safe_http, logger

import re
import dateparser

WEBHOOK_RESPONSE_RENAME = {}


def flatten_response(json_data, cursor=None):
    if "cursor" not in json_data:
        return [], cursor, True

    def safe_iter(d, k):
        if k in d:
            for k, v in d[k].items():
                yield k, v

    cursor = json_data["cursor"]
    might_have_more = json_data["mightHaveMore"]

    def _iter_payload(payloads):
        for p in payloads:
            user = (
                p["user"]["id"]
                if "user" in p and p["user"] is not None and "id" in p["user"]
                else None
            )
            timestamp = p["timestamp"]
            for table_id, table_changes in safe_iter(p, "changedTablesById"):
                # TODO: add record change type
                for rec_id, rec_changes in safe_iter(
                    table_changes, "changedRecordsById"
                ):
                    transition_type = rec_changes["transitionType"]
                    # TODO: we intentionally ignore removal because data is not really removed and we should handle differently???
                    # like yield and set the deleted timestamp that gets written to the database?
                    if transition_type == "remove":
                        continue
                    for cell_change in rec_changes["cellValues"]:
                        field_data = cell_change
                        field_data.update(
                            {
                                "user": user,
                                "record_id": rec_id,
                                "timestamp": timestamp,
                                "table_id": table_id,
                                "cursor": cursor,
                            }
                        )

                        yield field_data

    data = (
        pd.DataFrame(_iter_payload(json_data["payloads"]))
        if "payloads" in json_data
        else None
    ).rename(
        columns={
            "fieldName": "column_name",
            "value": "cell_value",
            "fieldId": "field_id",
        }
    )

    if data is not None and len(data) > 0:
        # TODO: need to double check that we do get UTC times and thus mapping to conventional ignore is correct
        data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_localize(None)

    return data, cursor, might_have_more


def stream_cell_changes(base_id, webhook_id, cursor=0):

    root = f"{AIRTABLE_API_ROOT}/bases/{base_id}"

    def _url(cursor):
        url = f"{root}/webhooks/{webhook_id}/payloads"
        if cursor is not None:
            url = f"{url}?cursor={cursor}"
        return url

    might_have_more = True
    while might_have_more:
        url = _url(cursor)

        json_data = safe_http.request_get(
            url, headers=get_airtable_request_header()
        ).json()
        data, cursor, might_have_more = flatten_response(json_data, cursor=cursor)
        if len(data) > 0:
            yield data, cursor


###
#  legacy
###


def _get_legacy_archive_locations():
    from res.connectors.s3 import S3Connector

    s3 = S3Connector()
    l = s3.ls("s3://iamcurious/data_sources/Airtable/")
    backups = pd.DataFrame(l, columns=["url"])
    backups.columns = ["url"]
    metadata = backups["url"].map(_get_part).dropna()
    metadata = pd.DataFrame([d for d in metadata])
    return backups.join(metadata, rsuffix="temp")


def _get_part(x):
    try:
        parts = x.split("/")
        base_id = parts[parts.index("bases") + 1]
        table_id = parts[parts.index("tables") + 1] if "tables" in x else None
        RES_LOC = (
            parts.index("RESONANCE") if "RESONANCE" in parts else parts.index("RS")
        )
        parts = parts[RES_LOC + 1].split("__")
        part = parts[1].split("_")[-1]
        date_parts = (
            parts[0].replace("_", "-") + " " + ":".join(parts[1].split("_")[:-1])
        )
        date_of_file = dateparser.parse(date_parts)
        return {
            "table_id": table_id,
            "base_id": base_id,
            "ts": date_of_file,
            "part": part,
            "location": x.replace("/records.json", ""),
            "is_record": "records.json" in x,
        }
    except:
        logger.warn(
            f"Failed when parsing paths in legacy archive migration for location {x}"
        )
        # raise


def unstack_data(data, location):
    parts = _get_part(location)

    location = parts["location"]

    metadata = pd.read_json(f"{location}/table.json", lines=True)
    table_id = metadata.iloc[0]["id"]
    fields = pd.DataFrame(metadata["fields"][0])

    unstacked = (
        pd.DataFrame([d for d in data["fields"]])
        .unstack()
        .reset_index()
        .set_index("level_1")
    )
    unstacked = unstacked.join(data[["id", "createdTime"]]).rename(
        columns={
            "level_0": "column_name",
            0: "cell_value",
            "id": "record_id",
            "createdTime": "timestamp",
        }
    )
    unstacked["table_id"] = table_id
    unstacked = pd.merge(
        unstacked,
        fields[["id", "name"]].rename(
            columns={"name": "column_name", "id": "field_id"}
        ),
        on="column_name",
    )

    unstacked["__file_date__"] = parts["ts"]
    unstacked["__file_part_suffix__"] = parts["part"]
    unstacked["base_id"] = parts["base_id"]

    # everything must be a string
    unstacked["cell_value"] = unstacked["cell_value"].map(str)

    return unstacked


def read_dataframe_from_location(location):
    """
    original format has two json files for records and table metadata
    We read these into an airtable snapshot in pandas dataframe
    This dataframe is merged into the set of all dataframes
    """

    data = pd.read_json(f"{location}/records.json", lines=True)

    if len(data) == 0:
        raise Exception(f"There are 0 records in {location}/records.json")

    return unstack_data(data, location)


def merge_table_sets(a, b, sort_on="timestamp"):
    """
    Merge cells into the latest and only keeping the last by timestamp
    We could maybe use a watermark to ignore very old data but then if we do we may as well as well split latest into parts e.g. quarterly airtables
    """

    # first we drop any duplicate values keeping first
    union = (
        pd.concat([a, b])
        .sort_values(sort_on)
        .drop_duplicates(subset=["record_id", "field_id", "cell_value"], keep="first")
    )
    # then we drop any duplicate cells keeping last value
    return (
        union.drop_duplicates(subset=["record_id", "field_id"], keep="last")
        .reset_index()
        .drop("index", 1)
    )


def combine_stages(path, use_initial=False):
    import res

    LATEST_LOCATION = f"{path}/.latest"

    union = pd.read_parquet(LATEST_LOCATION) if use_initial else None

    s3 = res.connectors.load("s3")
    for f in s3.ls(f"{path}/stages"):

        logger.info(f"reading chunk {f}")
        chunk = pd.read_parquet(f)
        union = chunk if union is None else merge_table_sets(union, chunk)

    logger.info(f"Saving combined {LATEST_LOCATION}")

    union.to_parquet(LATEST_LOCATION)


def count_distinct_archive_records(metadata, table_id):
    records = metadata[
        (metadata["table_id"].isin([table_id])) & (metadata["is_record"] == True)
    ].sort_values("ts")

    # this only works for one table as it returns not yields the result
    for table_id, records in records.groupby("table_id"):
        base_id = records.iloc[0]["base_id"]
        logger.info(
            f"{len(records)} archive files to merge for table {base_id}/{table_id}"
        )

        union = None
        for _, row in records.iterrows():
            try:
                logger.info(f"reading {row['location']}")
                chunk = read_dataframe_from_location(row["location"])
            except:
                logger.warn(f"""Could not read data from location {row["location"]}""")
                continue

            union = (
                chunk
                if union is None
                else pd.concat([union, chunk[["record_id", "timestamp"]]])
            )

            union = union.drop_duplicates(subset=["record_id"])
            logger.info(f"Merged into a set of size {len(union)}")

        return union


def bootstrap_table_history(
    table_ids, metadata, show_progress=False, union=None, save=True, stage=None
):

    """
    Go through a collection of paths matching the table id and read all data into a single distinct list of records.
    This takes a special format of legacy archive path info  using `_get_legacy_archive_locations`

    this can be saved in the latest location e.g.
    from res.connectors.airtable import AIRTABLE_LATEST_S3_BUCKET
    LATEST_LOCATION = f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{base_d}/{table_id}/.latest"
    union.to_parquet(LATEST_LOCATION)

    @metadata is built from the legacy archive file lists
    @table_id
    @show_progress
    @to bootsrap from the existing pass as a param otherwise it is built from scracth

    """

    if metadata is None:
        logger.info("Loading legacy locations - this takes a long time")
        metadata = _get_legacy_archive_locations()
    if not isinstance(table_ids, list):
        table_ids = [table_ids]

    records = metadata[
        (metadata["table_id"].isin(table_ids)) & (metadata["is_record"] == True)
    ].sort_values("ts")

    # if using stage we can filter
    if stage is not None and "stage" in records.columns:
        records = records[records["stage"] == stage]
        logger.info(f"filtered stage to {len(records)} records")

    gen = records.iterrows()
    if show_progress:
        from tqdm import tqdm

        gen = tqdm(list(gen))

    last_check_sum = 0
    for table_id, records in records.groupby("table_id"):
        base_id = records.iloc[0]["base_id"]
        logger.info(
            f"{len(records)} archive files to merge for table {base_id}/{table_id}"
        )

        for _, row in gen:
            try:
                chunk = read_dataframe_from_location(row["location"])
            except:
                logger.warn(f"""Could not read data from location {row["location"]}""")
                continue
            # this is an optimization if there are snapshots that are not leading to any changes
            chk_sum = pd.util.hash_pandas_object(chunk).sum()
            if chk_sum == last_check_sum:
                continue
            last_check_sum = chk_sum
            # if there are changes, merge in the data
            union = chunk if union is None else merge_table_sets(union, chunk)

            # logger.info(f"union on the latest. length is now {len(union)}")

        LATEST_LOCATION = (
            f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{base_id}/{table_id}/.latest"
        )

        # do it in stages where the stage is assumed a monthly partition
        if stage is not None:
            LATEST_LOCATION = f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{base_id}/{table_id}/stages/{stage}/.latest"

        logger.info(
            f"saving to {LATEST_LOCATION}. Union has {len(union)} rows and {len(union['record_id'].unique())} distinct records"
        )
        if save:
            union.to_parquet(LATEST_LOCATION)
