import asyncio
import gc
import os
import sys
from datetime import datetime, timedelta
from helper_functions import get_bulk_query_responses
from res.utils import logger, secrets_client
from res.connectors.snowflake import SnowflakeConnector


# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
SYNC_TYPE = (sys.argv[1] or "").lower()
DEV_OVERRIDE = (sys.argv[2] or "").lower() == "true"
SINGLE_SHOP_DOMAIN_NAME = (sys.argv[3] or "").lower()
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[4] or sys.argv[4].lower() == "all"

# Constants
DATABASE = "raw" if RES_ENV == "production" and not SYNC_TYPE == "test" else "raw_dev"
SCHEMA = "shopify"
TIMESTAMP_FORMAT_STR = "%Y-%m-%dT%H:%M:%SZ"


def flatten_records(data: list[dict]):

    output = []

    for record in data:

        # Extract IDs
        order_id = record.get("id").replace("gid://shopify/Order/", "")
        order_id = (
            order_id.replace("gid://shopify/Order/", "") if order_id else order_id
        )
        app_id = (record.get("app") or {}).get("id")
        app_id = app_id.replace("gid://shopify/App/", "") if app_id else app_id
        publication_id = (record.get("publication") or {}).get("id")
        publication_id = (
            publication_id.replace("gid://shopify/Publication/", "")
            if publication_id
            else publication_id
        )
        catalog_id = ((record.get("publication") or {}).get("catalog") or {}).get("id")
        catalog_id = (
            catalog_id.replace("gid://shopify/Catalog/", "")
            if catalog_id
            else catalog_id
        )

        output.append(
            {
                "order_id": record.get("id").replace("gid://shopify/Order/", ""),
                "shop_id": record.get("shop_id"),
                "shop_name": record.get("shop_name"),
                "app_id": app_id,
                "app_name": (record.get("app") or {}).get("name"),
                "publication_id": publication_id,
                "publication_name": (record.get("publication") or {}).get("name"),
                "catalog_id": catalog_id,
                "catalog_title": (
                    (record.get("publication") or {}).get("catalog") or {}
                ).get("title"),
                "synced_at": record.get("synced_at"),
            }
        )

    return output


if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):

    # Create Snowflake connector
    snowflake_connector = SnowflakeConnector(profile="data_team", warehouse="loader_wh")

    # Retrieve Shopify credentials and check parameter for override; the
    # override directs the app to only run to requested sync for a single shop
    shopify_keys = secrets_client.get_secret("SHOPIFY_DATA_TEAM_APP_API_KEYS")

    # Set default since timestamp
    since_ts = None

    if SINGLE_SHOP_DOMAIN_NAME != "none":

        shopify_keys = [
            key_dict
            for key_dict in shopify_keys
            if key_dict.get("shop_domain_name").lower() == SINGLE_SHOP_DOMAIN_NAME
        ]

        # Throw an exception if there wasn't a match
        if len(shopify_keys) == 0:

            e_str = "Provided single shop domain name does not match any domain names in retrieved AWS secret value"
            logger.error(e_str)

            raise Exception(e_str)

    if SYNC_TYPE == "incremental":

        # Retrieve Snowflake timestamp before async invocation
        since_ts = snowflake_connector.get_latest_snowflake_timestamp(
            database=DATABASE,
            schema="shopify",
            table="orders",
            timestamp_field="updated_at",
        )
        since_ts_str = datetime.strftime(since_ts, "%Y-%m-%dT%H:%M:%S.%fZ")

    elif SYNC_TYPE == "test":

        since_ts = datetime.now() - timedelta(days=5)
        since_ts_str = datetime.strftime(since_ts, "%Y-%m-%dT%H:%M:%S.%fZ")

    else:

        if since_ts is None:

            logger.warning(f"No since timestamp; Defaulting to full sync")

        elif SYNC_TYPE == "full":

            logger.info(f"Full sync")

        else:

            raise ValueError("Invalid sync type provided to function")

        # Retrieve since July 2015
        since_ts_str = "2015-07-01T00:00:01Z"
        since_ts = datetime.strptime(since_ts_str, TIMESTAMP_FORMAT_STR)

    # Raise exception if since_date is in the future because that shouldn't ever
    # happen
    current_ts = datetime.now()
    if since_ts > current_ts:

        raise Exception("Error: since_ts is in the future")

    # Set until_date_object to an arbitrary date to begin while loop
    until_ts = since_ts

    while until_ts < current_ts:

        # Sync will iterate through the time period from a target date through
        # present day in 180 day increments. This time period keeps the query cost
        # sufficiently low. Full syncs begin from 2015. Set until to present day
        # if it is a future date
        until_ts = min(current_ts, since_ts + timedelta(days=180))
        until_ts_str = datetime.strftime(until_ts, TIMESTAMP_FORMAT_STR)

        logger.info(f"Syncing data from {since_ts_str} to {until_ts_str}")

        body = """
            mutation {
            bulkOperationRunQuery(
                query: \"\"\"
                {
                    orders(query: "updated_at:>=%s AND updated_at:<=%s") {
                        edges {
                            node {
                                id
                                app {
                                    id
                                    name
                                }
                                publication {
                                    id
                                    name
                                    catalog {
                                        id
                                        title
                                    }
                                }
                            }
                        }
                    }
                }
                \"\"\"
            ) {
                bulkOperation {
                    id
                    status
                }
                userErrors {
                    field
                    message
                }
            }
            }
        """ % (
            since_ts_str,
            until_ts_str,
        )

        # Execute async API calls
        data = asyncio.run(
            get_bulk_query_responses(
                shopify_keys,
                body,
                since_ts,
                until_ts,
            )
        )

        # Flatten data
        flat_records = flatten_records(data)

        if len(flat_records) > 0:

            logger.info("Syncing order app and publication information to Snowflake")
            snowflake_connector.to_snowflake(
                data=flat_records,
                database=DATABASE,
                schema=SCHEMA,
                table="order_app_publications",
                add_primary_key=True,
            )

        # Clean up
        del flat_records
        gc.collect()

        # Iterate
        since_ts = until_ts
        since_ts_str = until_ts_str

    logger.info("Sync complete")
