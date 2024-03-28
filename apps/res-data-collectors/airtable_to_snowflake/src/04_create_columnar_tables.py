import os
import json
import random
import string
import re
import sys

import boto3

from res.utils import logger, secrets_client
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.connectors.airtable.AirtableClient import ResAirtableClient

# This cleans up base/table/field names and types, creates a new "columnar" table if one doesn't exist,
# then splits the variant data into columns and copies into the new table.
# It only copies the latest version of each record in a table.

RES_ENV = os.getenv("RES_ENV", "development")
TABLE_CREATE_QUERY = """CREATE OR REPLACE TABLE IAMCURIOUS_{env}.{columnar_table}
        AS SELECT 
            VARIANT_DATA:id::varchar AS OBJECT_ID,
            FILE_DATE as __FILE_DATE__,
            FILE_DATE='{date}' AS __exists_in_airtable__,
            {input_columns}
        FROM (
            SELECT *, row_number() over (
                partition by VARIANT_DATA:id
                order by FILE_DATE desc) as rn
            FROM {variant_table}
        )
        WHERE rn = 1;"""

# These columns have data types that have changed over time -- they don't properly cast to their current types
# They need to be varchars to accomodate all the broken data
CORRUPT_COLUMNS = {
    "appjmzNPXOuynj6xP_tblmszDBvO1MvJrlJ": [
        "Flash Yield",
        "Flash Trim Cost",
        "Flash Labor Cost",
    ],
    "appa7Sw0ML47cA8D1_tblrq1XfkbuElPh0l": ["t10.8 (Fit & Balance Review Completed)"],
    "appfaTObyfrmPHvHc_tblhtedTR2AFCpd8A": [
        "__order_fulfillment_status_last_cheked_ts"
    ],
    "apprcULXTWu33KFsh_tblSYU8a71UgbQve4": ["Inv. Count Audit"],
}

# Mapping of airtable data types to corresponding snowflake types
AIRTABLE_TO_SNOWFLAKE = {
    "singleLineText": "varchar",
    "formula": "varchar",
    "multipleAttachments": "array[0]::string",
    "checkbox": "varchar",
    "singleSelect": "varchar",
    "url": "varchar",
    "createdTime": "varchar",
    "button": "varchar",
    "singleCollaborator": "varchar",
    "multipleRecordLinks": "array[0]::string",
    "lastModifiedTime": "varchar",
    "rollup": "array[0]::string",
    "dateTime": "datetime",
    "autoNumber": "int",
    "number": "number(38, 0)",
    "date": "date",
    "multilineText": "varchar",
    "currency": "varchar",
    "count": "int",
    "multipleSelects": "array[0]::string",
    "email": "varchar",
    "richText": "varchar",
    "lookup": "array[0]::string",
    "multipleCollaborators": "array[0]::string",
    "barcode": "varchar",
    "lastModifiedBy": "varchar",
    "percent": "varchar",
    "createdBy": "varchar",
    "phoneNumber": "varchar",
    "rating": "varchar",
    "duration": "varchar",
}


def legacy_column_cleanup(raw_name):
    # This is a specific name cleanup, designed to match the legacy names. This is so we don't have to recode a bunch of things in Looker
    regex_replaced = re.sub("[^A-Za-z0-9 _]+", "", raw_name)
    if raw_name == regex_replaced:
        return regex_replaced
    return regex_replaced.replace("  ", "_").replace(" ", "_")


if __name__ == "__main__":
    boto_client = boto3.client("s3")
    snow_client = ResSnowflakeClient(schema=f"IAMCURIOUS_{RES_ENV}_STAGING")
    snow_client_lg = ResSnowflakeClient(
        schema=f"IAMCURIOUS_{RES_ENV}_STAGING", warehouse="AIRTABLE_SNAPSHOT"
    )
    # Set up airtable client. Replace default personal access token with data
    # team specific one. This helps to prevent rate limiting
    DATA_TEAM_AIRTABLE_READER_PAT = secrets_client.get_secret(
        "DATA_TEAM_AIRTABLE_READER_PAT"
    )
    airtable_client = ResAirtableClient()
    airtable_client.api_key = DATA_TEAM_AIRTABLE_READER_PAT
    airtable_client.bearer_header = {
        "Authorization": f"Bearer {DATA_TEAM_AIRTABLE_READER_PAT}"
    }
    event = json.loads(sys.argv[1])
    # event = {"base": "appjmzNPXOuynj6xP", "tables": ["tblmszDBvO1MvJrlJ"], "dates": ["2021_10_26__05_00_07"]}
    base = event["base"]
    tables = event["tables"]
    date = event["dates"][0]

    # Open base metadata
    base_key = f"airtable_archives/{date}/{base}/base.json"
    data = boto_client.get_object(Bucket=f"res-data-{RES_ENV}", Key=base_key)
    base_metadata = json.loads(data["Body"].read())
    exception_count = 0

    for table in tables:
        try:
            logger.info(f"Creating columnar for table {table}")
            # Find metadata/fields spec for this table
            table_metadata = {}
            for temp_metadata in base_metadata["tables"]:
                if temp_metadata["id"] == table:
                    table_metadata = temp_metadata

            # Clean up names and build table names
            clean_table_name = airtable_client.clean_name(
                table_metadata["name"]
            ).lstrip("_")
            for f in base_metadata:
                print(f)
            clean_base_name = airtable_client.clean_name(base_metadata["name"]).lstrip(
                "_"
            )
            columnar_table_name = f"AIRTABLE__{clean_base_name}__{clean_table_name}"
            variant_table_name = f"{base}_{table}"

            # Convert and clean up columns
            columns = ""
            unique_clean_columns = [
                "__exists_in_airtable__",
                "OBJECT_ID",
                "__FILE_DATE__",
            ]
            for field in table_metadata["fields"]:
                raw_field_name = field["name"]
                clean_field_name = legacy_column_cleanup(raw_field_name)
                if clean_field_name in unique_clean_columns:
                    logger.warn(f"!!! Duplicate column found: {clean_field_name}")
                    random_str = "".join(
                        random.choices(string.ascii_uppercase + string.digits, k=8)
                    )
                    clean_field_name = f"{clean_field_name}_{random_str}"
                # Check if this is a bad column, otherwise get proper type
                if (
                    variant_table_name in CORRUPT_COLUMNS
                    and raw_field_name in CORRUPT_COLUMNS[variant_table_name]
                ):
                    snowflake_type = "varchar"
                else:
                    # Special exception for numeric decimal fields, use precision provided by airtable
                    if (
                        field.get("config", {})
                        .get("options", {})
                        .get("precision", None)
                    ):
                        precision = field["config"]["options"]["precision"]
                        snowflake_type = f"number(38,{precision})"
                    else:
                        snowflake_type = (
                            AIRTABLE_TO_SNOWFLAKE[field["config"]["type"]]
                            if field.get("config", {}).get("type")
                            in AIRTABLE_TO_SNOWFLAKE
                            else "varchar"
                        )
                # Need to escape any double quotes
                raw_field_name_escaped = raw_field_name.replace('"', '""')
                columns += f'\nVARIANT_DATA:fields:"{raw_field_name_escaped}"::{snowflake_type} AS "{clean_field_name}",'
                unique_clean_columns.append(clean_field_name)

            # Execute
            logger.debug(
                TABLE_CREATE_QUERY.format(
                    env=RES_ENV,
                    date=date,
                    columnar_table=columnar_table_name,
                    input_columns=columns[:-1],  # strip out final comma
                    variant_table=variant_table_name,
                )
            )

            try:
                snow_client.execute(
                    TABLE_CREATE_QUERY.format(
                        env=RES_ENV,
                        date=date,
                        columnar_table=columnar_table_name,
                        input_columns=columns[:-1],  # strip out final comma
                        variant_table=variant_table_name,
                    )
                )
            except:
                logger.info(
                    f"Upsert query Snowflake faied for table {table}, trying larger warehouse!"
                )
                snow_client_lg.execute(
                    TABLE_CREATE_QUERY.format(
                        env=RES_ENV,
                        date=date,
                        columnar_table=columnar_table_name,
                        input_columns=columns[:-1],  # strip out final comma
                        variant_table=variant_table_name,
                    )
                )

        except Exception as e:
            event = event if "event" in locals() else '["bad event payload"]'
            logger.error(
                "Error pulling creating the columnar table in Snowflake!{} {}".format(
                    json.dumps({**event, **{"tables": [table]}}), str(e)
                )
            )
            exception_count += 1
            continue

    if exception_count > 0:
        raise Exception(
            f"Error pulling Airtable data down to S3 for {exception_count} {'table' if exception_count==1 else 'tables'}; see logs for details"
        )

    else:
        logger.info("Done!")
