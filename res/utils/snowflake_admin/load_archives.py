import os, datetime, boto3, json, random, string
from res.utils import logger, secrets_client
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from s3_helpers import *

RES_ENV = os.getenv("RES_ENV", "development")

# Snowflake Queries
SNOWFLAKE_GET_UNIQUE_DATES = "SELECT distinct FILE_DATE FROM {variant_table};"
SNOWFLAKE_CREATE_TEMP = """CREATE OR REPLACE TABLE {temp_table} (COL1 variant);"""
SNOWFLAKE_CREATE_VARIANT = """CREATE TABLE IF NOT EXISTS {variant_table} (VARIANT_DATA variant, FILE_DATE varchar);"""
SNOWFLAKE_LOAD_TEMP = """COPY INTO {temp_table}
                         FROM @iamcurious/data_sources/Airtable/RS_{env}/{date}/{base}/tables/{table}/records.json
                         FILE_FORMAT = (type = 'JSON' strip_outer_array = true);"""
SNOWFLAKE_CLEAR_VARIANT = "DELETE FROM {variant_table} WHERE FILE_DATE = '{date}';"
SNOWFLAKE_LOAD_VARIANT = (
    """INSERT INTO {variant_table} SELECT COL1, '{date}' FROM {temp_table};"""
)
SNOWFLAKE_DROP_TEMP = "DROP TABLE {temp_table};"

if __name__ == "__main__":
    # Collect user overrides for settings
    base_id = os.getenv("BASE_ID", "all")
    table_id = os.getenv("TABLE_ID", "all")
    start_date = os.getenv("START_DATE", "latest")
    end_date = os.getenv("END_DATE", "latest")
    start_date = "2020-10-01"
    end_date = "2021-10-12"

    snow_staging_client = ResSnowflakeClient(schema=f"IAMCURIOUS_{RES_ENV}_STAGING")

    # Get list of bases/tables to load
    if base_id == "all":
        bases = get_bases()
    elif table_id == "all":
        bases = {base_id: get_tables(base_id)}
    else:
        bases = {base_id: [table_id]}

    # For each base / table / file / record:
    # 1. create snowflake temp table
    # 2. load into snowflake temp table (Variant)
    # 3. copy into permanent variant table with column for file
    for base in bases:
        for table in bases[base]:
            files = get_files(base, table, start_date, end_date)
            variant_table = f"{base}_{table}"
            existing_dates = [
                i[0]
                for i in snow_staging_client.execute(
                    SNOWFLAKE_GET_UNIQUE_DATES.format(variant_table=variant_table)
                )
            ]
            for file in files:
                logger.info(f"Processing {file}")
                this_date = file.split("/")[-5]
                if this_date not in existing_dates:
                    random_str = "".join(
                        random.choices(string.ascii_uppercase + string.digits, k=8)
                    )
                    variant_table_old = f"{base}_{table}_{this_date}"
                    temp_table = f"{variant_table}_{random_str}"
                    snow_staging_client.execute(
                        SNOWFLAKE_CREATE_TEMP.format(temp_table=temp_table)
                    )
                    snow_staging_client.execute(
                        SNOWFLAKE_LOAD_TEMP.format(
                            env=RES_ENV,
                            temp_table=temp_table,
                            date=this_date,
                            base=base,
                            table=table,
                        )
                    )
                    snow_staging_client.execute(
                        SNOWFLAKE_CREATE_VARIANT.format(variant_table=variant_table)
                    )
                    snow_staging_client.execute(
                        SNOWFLAKE_CLEAR_VARIANT.format(
                            variant_table=variant_table, date=this_date
                        )
                    )
                    snow_staging_client.execute(
                        SNOWFLAKE_LOAD_VARIANT.format(
                            variant_table=variant_table,
                            date=this_date,
                            temp_table=temp_table,
                        )
                    )
                    snow_staging_client.execute(
                        SNOWFLAKE_DROP_TEMP.format(temp_table=temp_table)
                    )
