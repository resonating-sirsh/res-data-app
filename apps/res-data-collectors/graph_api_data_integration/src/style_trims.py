import gc
import hashlib
import os
import sys
from datetime import datetime

from graphql_objects import GraphQLJSONResponse
from helper_functions import get_latest_snowflake_timestamp, to_snowflake
from queries import GET_STYLE_TRIMS

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger


def flatten_data(json_data: GraphQLJSONResponse):
    flat_data = []
    logger.info("Parsing graphql response")

    json_data = json_data["data"]["styles"]["styles"]

    for record in json_data:
        style_id = record.get("id")
        style_code = record.get("code")
        style_name = record.get("name")

        # Get trim info from the style bill of materials. Not all items are trim
        # items
        try:
            for i in record["styleBillOfMaterials"]:
                bom = i.get("billOfMaterial")
                if bom:
                    # Grain of data added to Snowflake; an individual bill of
                    # material record attached to a style
                    row = {
                        "meta_one_bill_of_material_record_id": bom.get("id"),
                        "meta_one_trim_id": (bom.get("trim") or {}).get("id"),
                        "meta_one_trim_taxonomy_record_id": (
                            bom.get("trimTaxonomyId") or [None]
                        )[0],
                        "style_id": style_id,
                        "bill_of_material_friendly_name": bom.get(
                            "styleBomFriendlyName"
                        ),
                        "bill_of_material_name": bom.get("name"),
                        "bill_of_material_type": bom.get("type"),
                        "bill_of_material_unit": bom.get("unit"),
                        "style_code": style_code,
                        "style_name": style_name,
                        "trim_length": bom.get("trimLength"),
                        "trim_quantity": bom.get("trimQuantity"),
                    }

                    # Create a unique key using the values of the recorded state.
                    # Add it and sync time to the row
                    record_field_hash = hashlib.md5(
                        str(row.values()).encode("utf-8")
                    ).hexdigest()
                    row = {
                        "primary_key": record_field_hash,
                        **row,
                        "synced_at": SYNC_TIME,
                    }

                    # Add to flat_data list
                    flat_data.append(row)

        except Exception as e:
            logger.error(
                f"Exception while flattening trims for style ID {style_id}: {e}"
            )

            raise

    return flat_data


# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
SYNC_TYPE = (sys.argv[1] or "").lower()
DEV_OVERRIDE = (sys.argv[2] or "").lower() == "true"
IS_TEST = (sys.argv[3] or "").lower() == "true"
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[4] or sys.argv[4].lower() == "all"

# Constants
DATABASE = "raw" if RES_ENV.lower() == "production" and not IS_TEST else "raw_dev"
SCHEMA = "resmagic_api"
TABLE = "style_trims"

if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):
    # Set up query variables based on sync type
    variables = {
        "after": "0",
    }

    if SYNC_TYPE.lower() == "incremental":
        ts = get_latest_snowflake_timestamp(
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE,
            timestamp_field="synced_at",
            timestamp_format_str="%Y-%m-%dT%H:%M:%SZ",
        )

        if ts is None:
            logger.warning(
                "Couldn't retrieve timestamp for incremental sync. Switching to full sync"
            )

        else:
            variables["where"] = {"updatedAt": {"isGreaterThanOrEqualTo": ts}}

    else:
        logger.info("Full sync")

    # Execute functions
    graphql_client = ResGraphQLClient()
    SYNC_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f %Z")
    json_data = graphql_client.query(
        query=GET_STYLE_TRIMS,
        variables=variables,
        paginate=False if IS_TEST else True,
    )
    flat_data = flatten_data(json_data)

    # Clear non-flat data from memory
    del json_data
    gc.collect()

    # Send to Snowflake
    to_snowflake(
        data=flat_data,
        database=DATABASE,
        schema=SCHEMA,
        table=TABLE,
    )
    logger.info("Style trims sync complete")
