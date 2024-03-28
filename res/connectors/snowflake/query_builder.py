from logging import StringTemplateStyle
import os
from datetime import datetime
import re
import res

# we tend to store varchars with max length
# we tend to store dates as TIMEZONE_LTZ i.e. the local time for presentation but UTC for storage

# TODO: broken abstraction this is in two places

DEFAULT_SCHEMA = "IAMCURIOUS_DEVELOPMENT"

map_pandas_types_to_snowflake = {
    "str": "VARCHAR(16777216)",
    "datetime": "TIMESTAMP_LTZ(9)",  # or it could be DATE
    "boolean": "BOOLEAN",
    "object": "VARIANT",
    "float": "NUMBER(18,2)",  # not sure about this one,
    "int": "NUMBER(38,0)",
}

# for now make them 1:1
map_snowflake_types_to_pandas = {v: k for k, v in map_pandas_types_to_snowflake.items()}


def alter_table(table, column_types):
    column_types = dict(column_types)
    query = f"""alter TABLE "{table}" add"""
    for k, v in column_types.items():
        v = map_pandas_types_to_snowflake.get(v, "VARCHAR(16777216)")
        query += f"""\n"{k}" {v},"""
    query = query.rstrip(",") + ")"
    return query


def create_table(table, column_types):
    column_types = dict(column_types)
    query = f"""create or replace TABLE "{table}" ("""
    for k, v in column_types.items():
        v = map_pandas_types_to_snowflake.get(v, "VARCHAR(16777216)")
        query += f"""\n"{k}" {v},"""
    # add control field for when we added the partition to staging
    query += f"""\n"__partition_added__" timestamp default current_timestamp(),"""
    query = query.rstrip(",") + ")"
    return query


def delete_partitions(table_name, partition_keys):
    # TODO - this is just a way to get snowpipe to re-ingest partitions without deleting things
    PART_KEY_COL = ""
    FORMAL_LIST = ""

    query = f"""DELETE {table_name} where PART_KEY_COL in {FORMAL_LIST}"""

    return query


# in reality we are going to map the columns while migrating from parquet straight into the table we want
# https://docs.snowflake.com/en/user-guide/data-load-transform.html
def create_pipe(table_name, column_types, schema, namespace="airtable"):
    """
    We create a pipe that reads from a parquet file with known schmea
    Into a staging table with known schema.

    TODO: i think we need a ensure that whenever a partition changes it is using the col superset
    Snowflake uses md5 hash to decide if it should load (and we trigger)
    This query will make sense if the files always have all queries columns (See below) when they are triggered
    Making sure the target able has those columns is easy but we should make sure not to drop columns only add-alter
    We may want to transform things on load: https://docs.snowflake.com/en/user-guide/data-load-transform.html

    ```sql

        --create or replace stage test_stage
        -- url='s3://some_namespace/some_schema/TestData/partitions/'
        -- credentials=(aws_key_id='******' aws_secret_key='*****')
        -- file_format = (type = 'PARQUET');

        create or replace pipe mypipe_s3
        auto_ingest = false
        as
        copy into DEFAULT_SCHEMA."TestData"
        from
            (
            select
                --list out columns that we need (all of them)
                --copy from the parquet file into our table
                $1:"a",
                $1:"b",
                $1:"c"
            from @test_stage
            )
        file_format = (type = 'PARQUET');
    ```
    """
    table_ref = clean_table_name(table_name)
    # TODO: clean this up - naff that we reference this from here
    stage = (
        f"{namespace}_{table_ref}_envstage" if namespace else f"{table_ref}_envstage"
    )
    stage = stage.upper()

    THE_TABLE = f"""{schema}."{table_ref}" """

    column_types = dict(column_types)
    timestamp_inserted = datetime.utcnow().isoformat()

    def cast_it(k, v):
        # print(k, v)
        # warning - since this is experimental code for specific use cases we are assuming objects are strings but we can do better but typing is a whole thing
        # if str(v) in ["str", "object"]:
        #     return "::varchar"
        return ""

    # pass in a dict with cols and types
    # we dont use types right now but its consist with other methods and in future we might
    columns = ",".join([f'$1:"{c}"{cast_it(c,v)}' for c, v in column_types.items()])
    pipe_name = (
        f"{namespace}_{table_ref.replace('__','_')}_pipe"
        if namespace is not None
        else f"{table_ref}_pipe"
    ).upper()

    # qualify this in case we are executing from another schema
    # pipe_name = f"{schema}.{pipe_name}"

    res.utils.logger.info(f"Table {THE_TABLE} for pipe: {pipe_name} to stage {stage}")

    create_pipe_query = f"""
    create or replace pipe {pipe_name}
      auto_ingest = false
      as
       copy into {THE_TABLE} FROM
        (
         SELECT {columns}, '{timestamp_inserted}' from @{stage}
        )
        file_format = (type = 'PARQUET');
    """

    return create_pipe_query, pipe_name


def merge_staging_to_prod(table, metadata, watermark_days_back=7, commit=False):
    # TODO: ill work on this when i confirm we are happy with schema and values upstream
    # BEGIN TRANSACTION
    # use the watermark in the where clause for records inserted or modified in a window

    # END TRANSACTION OR ROLLBACK
    return ""


def clean_table_name(table_name):
    table_name = table_name.replace('"', "").replace(" ", "_")
    if re.match(r"^[\w ]+$", table_name) is None:
        table_name = re.sub(r"[^\w]+", "_", table_name)

    # use lower case for tables
    return table_name.rstrip("_")  # .replace("__", "_")


def clean_field_name(name):
    """
    i have tried to match how it seems we map to Snowflake but im not sure about this - logic seemed weird
    """
    name = name.replace('"', "")
    if re.match(r"^[\w ]+$", name) is None:
        name = re.sub(r"[^\w]+", "_", name)

    return name.rstrip("_")


def list_stages(namespace, table_ref):
    stage = (
        f"@s3_{namespace}_{table_ref}_stage" if namespace else f"@s3_{table_ref}_stage"
    )
    return f"""list {stage}"""
