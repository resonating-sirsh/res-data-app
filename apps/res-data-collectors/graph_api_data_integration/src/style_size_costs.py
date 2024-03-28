import gc
import hashlib
import os
import sys
from datetime import datetime

from graphql_objects import GraphQLJSONResponse
from helper_functions import get_latest_snowflake_timestamp, to_snowflake
from queries import GET_STYLE_COSTS

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient, GraphQLException
from res.utils import logger


def flatten_data(json_data: GraphQLJSONResponse):
    flat_data = []
    logger.info("Parsing data")

    json_data = json_data["data"]["styles"]["styles"]

    for record in json_data:
        style_id = record.get("id")
        style_code = record.get("code")

        # SKUs are made up of the style and size. If either of these are missing
        # skip the records
        if style_id is None:
            continue

        for size in record.get("onePrices") or []:
            size_id = size.get("size").get("id")
            size_code = size.get("size").get("code")

            # SKUs are made up of the style and size. If either of these are
            # missing skip the records
            if size_id is None or size_code is None:
                continue

            for component in size.get("priceBreakdown") or []:
                # This is the smallest grain and forms the rows of data
                row = {
                    "sku": style_code + " " + size_code,
                    "style_id": style_id,
                    "style_code": style_code,
                    "style_name": record.get("name"),
                    "size_id": size_id,
                    "size_code": size_code,
                    "size_name": size.get("size").get("name"),
                    "style_size_cost": size.get("cost"),
                    "style_size_margin": size.get("margin"),
                    "style_size_price": size.get("price"),
                    "price_item": component.get("item"),
                    "price_item_category": component.get("category"),
                    "price_item_rate": component.get("rate"),
                    "price_item_quantity": component.get("quantity"),
                    "price_item_unit": component.get("unit"),
                    "price_item_cost": component.get("cost"),
                }

                # Create a unique key using the values of the recorded state
                record_field_hash = hashlib.md5(
                    str(row.values()).encode("utf-8")
                ).hexdigest()
                row["primary_key"] = record_field_hash

                # Add sync time. This isn't part of the primary key because that
                # can create multiple records for observations of the same
                # record state
                row = {
                    "primary_key": record_field_hash,
                    **row,
                    "retrieved_at": SYNC_TIME,
                }

                # Add to flat_data list
                flat_data.append(row)

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
TABLE = "sku_costs"

if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):
    # Set up query variables based on sync type
    variables = {
        "after": "0",
        "where": {
            "isStyle3dOnboarded": {"is": True},
        },
    }

    if SYNC_TYPE.lower() == "incremental":
        ts = get_latest_snowflake_timestamp(
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE,
            timestamp_field="retrieved_at",
            timestamp_format_str="%Y-%m-%dT%H:%M:%SZ",
        )

        if ts is None:
            logger.warning(
                "Couldn't retrieve timestamp for incremental sync. Switching to full sync"
            )

        else:
            variables["where"]["updatedAt"] = {"isGreaterThanOrEqualTo": ts}
    else:
        logger.info("Full sync")

    # Execute functions
    graphql_client = ResGraphQLClient()
    SYNC_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f %Z")

    try:
        # Create empty json_data object in order to check it in except
        json_data = None
        json_data = graphql_client.query(
            query=GET_STYLE_COSTS,
            variables=variables,
            paginate=False if IS_TEST else True,
        )

    except GraphQLException as e:
        if json_data is not None:
            # There is a relatively common error with these queries having
            # to do with the one_prices of some styles -- for now I log and
            # ignore it as it merely means that a record is missing pricing
            # data, something its mostly empty row illustrates downstream. The
            # GraphQL client raises this error after query execution is done.
            error_list = e.message

            for error in error_list:
                if (
                    "Cannot read property 'defaultProductTags'" not in error["message"]
                    and error["message"]
                    != "Expected Iterable, but did not find one for field Style.onePrices."
                ):
                    error_list.remove(error)

                # If any errors remain after exclusion raise the Exception
                if len(error_list) > 0:
                    raise

                else:
                    pass
        else:
            raise

    flat_data = flatten_data(json_data)

    del json_data
    gc.collect()

    # Send to Snowflake
    to_snowflake(
        data=flat_data,
        database=DATABASE,
        schema=SCHEMA,
        table=TABLE,
    )
    logger.info("SKU costs sync complete")
