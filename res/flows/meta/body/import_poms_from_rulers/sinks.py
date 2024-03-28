import res
import pandas as pd


def transform_json_and_write(
    pom_json,
    body_code,
    body_version,
    source=None,
    write_file=False,
    refresh_stage=False,
):
    s3 = res.connectors.load("s3")

    dfs = []
    for size, data in pom_json.items():
        df = pd.DataFrame(data).T.reset_index()
        df["size"] = size
        df["body_code"] = body_code
        df["body_version"] = body_version
        dfs.append(df)
    dfs = (
        pd.concat(dfs)
        .reset_index(drop=True)
        .rename(
            columns={
                "index": "key",
                "patternMeasurement": "value",
                "measurementId": "measurement_ordinal",
            }
        )
    )

    dfs["measurement_ordinal"] = dfs["measurement_ordinal"].fillna(0).map(int)
    dfs["value"] = dfs["value"].round(5)

    H = dfs[
        ["key", "value", "measurement_ordinal", "size", "body_code", "body_version"]
    ]

    def row_hash(row):
        # its not clear to me if the measurement ordinal should be there
        return res.utils.uuid_str_from_dict(
            {
                "key": row["key"],
                "value": row["value"],
                "size": row["size"],
                "body_code": row["body_code"],
                "body_version": row["body_version"],
                "measurement_ordinal": row["measurement_ordinal"],
            }
        )

    rhash = res.utils.dataframes.hash_dataframe_short(H)
    dfs["source"] = source
    dfs["timestamp"] = res.utils.dates.utc_now()
    dfs["filehash"] = rhash
    dfs["id"] = dfs.apply(row_hash, axis=1)

    write_file_root = f"s3://res-data-development/data/staging/row_hashed_rulers/{body_code}/v{body_version}/rulers.parquet"

    if s3.exists(write_file_root):
        res.utils.logger.info(f"Merging existing file...")
        existing = s3.read(write_file_root)
        # merge unique keys and drop any new duplicate
        dfs = (
            pd.concat([dfs, existing])
            .sort_values("timestamp")
            .drop_duplicates("id", keep="first")
        )
    if write_file_root:
        s3.write(f"{write_file_root}", dfs)
        res.utils.logger.info(f"Writing {write_file_root}")

    if refresh_stage:
        # check the timestamps make sense for this - we could maybe pass specific files in future too
        migrate_pom_files_snowflake(
            partitions_modified_since=res.utils.dates.utc_hours_ago(1)
        )
    return dfs


def batch_migrate(on_error=None):
    """
    for bootstrapping
    """
    from res.flows.meta.body.import_poms_from_rulers.import_poms_from_rulers import (
        get_poms_from_rulers,
    )

    s3 = res.connectors.load("s3")
    try_failed = []
    for pom_file_uri in s3.ls("s3://meta-one-assets-prod/bodies/3d_body_files/"):
        if "rulers.csv" not in pom_file_uri:
            continue

        body_code = pom_file_uri.split("/")[-5].upper().replace("_", "-")
        body_version = int(float(pom_file_uri.split("/")[-4].replace("v", "")))
        pom_json, errors = get_poms_from_rulers(body_code, body_version, pom_file_uri)

        if not errors:
            try:
                transform_json_and_write(
                    pom_json,
                    body_code,
                    body_version,
                    write_file=True,
                    source=pom_file_uri,
                )
            except:
                if on_error == "raise":
                    raise
                try_failed.append(pom_file_uri)


def migrate_pom_files_snowflake(partitions_modified_since):
    # try the stage refresh approach
    # this could be more elegant for our small file sizes this is a good enough way to make the file visible in the external table (i think
    # #the stage was created with the snowflake connector

    snowflake = res.connectors.load("snowflake", schema="IAMCURIOUS_PRODUCTION_STAGING")
    # S3__BODY_POINTS_OF_MEASURE_EXT_HASHED -> s3://res-data-development/data/staging/hashed_rulers/
    # TABLE NAME S3__BODY_POINTS_OF_MEASURE_EXT_HASHED
    snowflake.refresh_stage("S3__BODY_POINTS_OF_MEASURE_EXT_HASHED")

    # # the return value for this gives status - poll it - r.get_history()
    # r = snowflake.ingest_partitions(
    #     table_name="S3__BODY_POINTS_OF_MEASURE",
    #     # the / currently at the end is required -> we stage once per name be careful
    #     stage_root="s3://res-data-platform/data/staging/rulers/",
    #     # use whatever schema
    #     snow_schema="IAMCURIOUS_PRODUCTION_STAGING",
    #     # only load files after a certain date
    #     partitions_modified_since=partitions_modified_since,
    # )
    # return r
    return
