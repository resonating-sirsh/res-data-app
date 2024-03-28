import os, datetime, boto3, json, random, string, sys
from res.utils import logger, secrets_client
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient

# This script loads data into a temp table in Snowflake, then clears out the "variant" table
# for any data from the current date (e.g. in case of a failed load), and replaces with the new data

RES_ENV = os.getenv("RES_ENV", "development")

SNOWFLAKE_GET_UNIQUE_DATES = "SELECT distinct FILE_DATE FROM {variant_table};"
SNOWFLAKE_CREATE_TEMP = """CREATE OR REPLACE TABLE {temp_table} (COL1 variant);"""
SNOWFLAKE_CREATE_VARIANT = """CREATE TABLE IF NOT EXISTS {variant_table} (VARIANT_DATA variant, FILE_DATE varchar);"""
SNOWFLAKE_LOAD_TEMP = """COPY INTO {temp_table}
                         FROM @res_data_{env}/airtable_archives/{date}/{base}/tables/{table}/records.json
                         FILE_FORMAT = (type = 'JSON' strip_outer_array = true);"""
SNOWFLAKE_CLEAR_VARIANT = "DELETE FROM {variant_table} WHERE FILE_DATE = '{date}';"
SNOWFLAKE_LOAD_VARIANT = (
    """INSERT INTO {variant_table} SELECT COL1, '{date}' FROM {temp_table};"""
)
SNOWFLAKE_DROP_TEMP = "DROP TABLE {temp_table};"

if __name__ == "__main__":
    client = ResSnowflakeClient(schema=f"IAMCURIOUS_{RES_ENV}_STAGING")
    event = json.loads(sys.argv[1])
    # event = {"base": "appfaTObyfrmPHvHc", "tables": ["tblUcI0VyLs7070yI"], "dates": ["2021_10_22__05_00_08"]}
    base = event["base"]
    tables = event["tables"]
    dates = event["dates"]
    overwrite = event.get("overwrite", False)

    for table in tables:
        try:
            logger.info(f"Loading table {table}...")
            variant_table = f"{base}_{table}"

            # Pull down snowflake data if table exists
            table_results = client.execute(
                f"show tables like 'abc{variant_table.upper()}'"
            )
            existing_dates = (
                [
                    row[0]
                    for row in client.execute(
                        f"SELECT DISTINCT FILE_DATE FROM {variant_table}"
                    )
                ]
                if len(table_results) > 0
                else []
            )
            for date in dates:
                if overwrite or date not in existing_dates:
                    random_str = "".join(
                        random.choices(string.ascii_uppercase + string.digits, k=8)
                    )
                    temp_table = f"{variant_table}_{random_str}"

                    logger.info(f"Loading temp table {date}...")
                    client.execute(SNOWFLAKE_CREATE_TEMP.format(temp_table=temp_table))
                    client.execute(
                        SNOWFLAKE_LOAD_TEMP.format(
                            env=RES_ENV,
                            temp_table=temp_table,
                            date=date,
                            base=base,
                            table=table,
                        )
                    )
                    logger.info(f"Loading variant table {date}...")
                    client.execute(
                        SNOWFLAKE_CREATE_VARIANT.format(variant_table=variant_table)
                    )
                    client.execute(
                        SNOWFLAKE_CLEAR_VARIANT.format(
                            variant_table=variant_table, date=date
                        )
                    )
                    client.execute(
                        SNOWFLAKE_LOAD_VARIANT.format(
                            variant_table=variant_table,
                            date=date,
                            temp_table=temp_table,
                        )
                    )
                    client.execute(SNOWFLAKE_DROP_TEMP.format(temp_table=temp_table))

        except Exception as e:
            event = event if event in locals() else '["bad event payload"]'
            logger.error(
                "Error loading Airtable data from S3 to Snowflake!{} {}".format(
                    json.dumps({**event, **{"tables": [table]}}), str(e)
                )
            )
            continue
    logger.info("Done!")
