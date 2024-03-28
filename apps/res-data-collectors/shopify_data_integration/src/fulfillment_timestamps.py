import aiohttp
import asyncio
import gc
import hashlib
import json
import os
import snowflake.connector
import sys
from datetime import datetime, timedelta
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
from helper_functions import (
    get_secret,
    get_latest_snowflake_timestamp,
    dict_to_sql,
)
from res.utils import logger


# Create synchronous functions to create tables in Snowflake and upsert data
def create_snowflake_tables():

    try:

        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="loader_wh",
            database=database,
        )

        logger.info("Connected to Snowflake")

    except:

        logger.error("Failed to authenticate to snowflake")

    # Create schema and tables
    logger.info("Creating schema and tables if they do not exist")

    base_connection.cursor().execute("create schema if not exists shopify")
    base_connection.cursor().execute("use schema shopify")
    base_connection.cursor().execute(
        """
            create table if not exists fulfillment_timestamps (        
                primary_key varchar,
                fulfillment_id varchar,
                shop_id integer,
                shop_name varchar,
                in_transit_at timestamp_tz,
                estimated_delivery_at timestamp_tz,
                delivered_at timestamp_tz,
                updated_at timestamp_tz,
                synced_at timestamp_tz
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_data(flat_records):

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    dtypes = {
        "primary_key": VARCHAR,
        "fulfillment_id": INTEGER,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "in_transit_at": TIMESTAMP(timezone=True),
        "estimated_delivery_at": TIMESTAMP(timezone=True),
        "delivered_at": TIMESTAMP(timezone=True),
        "updated_at": TIMESTAMP(timezone=True),
        "synced_at": TIMESTAMP(timezone=True),
    }

    dict_to_sql(flat_records, "fulfillment_timestamps_stage", engine, dtype=dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    if len(flat_records) > 0:

        logger.info("Merging data from stage table to target table")

        # Create stage and target tables
        stage_table = meta.tables["fulfillment_timestamps_stage"]
        target_table = meta.tables["fulfillment_timestamps"]

        # Structure merge
        merge = MergeInto(
            target=target_table,
            source=stage_table,
            on=target_table.c.primary_key == stage_table.c.primary_key,
        )

        # Create column metadata dict using column names from stage table
        cols = {}

        for column in stage_table.columns._all_columns:

            cols[column.name] = getattr(stage_table.c, column.name)

        # Insert new records and update existing ones
        merge.when_not_matched_then_insert().values(**cols)
        merge.when_matched_then_update().values(**cols)
        alchemy_connection.execute(merge)
        logger.info("Merge executed for fulfillment timestamps")

    # Close connection
    alchemy_connection.close()
    engine.dispose()
    logger.info("sqlalchemy connection closed; sqlalchemy engine disposed of")


async def get_bulk_query_response(
    shopify_access_token: str,
    shop_domain: str,
    client_session,
    since_date: str,
    until_date: str,
):

    # Set constant request components
    headers = {"X-Shopify-Access-Token": f"{shopify_access_token}"}

    while True:

        # Shop name and id API request
        async with client_session.get(
            url=f"https://{shop_domain}.myshopify.com/admin/api/2023-04/shop.json?fields=id,name",
            headers=headers,
        ) as response:

            try:

                response.raise_for_status()
                shop_config_data = (await response.json(encoding="UTF-8")).get("shop")
                shop_id = shop_config_data.get("id")
                shop_name = shop_config_data.get("name")

                break

            except:

                logger.warning(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

                if response.status == 429:

                    wait_time = int(response.headers.get("Retry-After"))
                    logger.info(
                        f"Rate limit reached; retrying after {wait_time} seconds"
                    )
                    await asyncio.sleep(wait_time)

                    continue

                else:

                    # End function for shop if API call fails after wait with non
                    # too many requests response code
                    return

    # Set up url, parameters, and JSON
    headers = {"X-Shopify-Access-Token": f"{shopify_access_token}"}
    url = f"https://{shop_domain}.myshopify.com/admin/api/2023-04/graphql.json"
    body = """
    mutation {
    bulkOperationRunQuery(
        query: \"\"\"
        {
        orders(query: "updated_at:>=%s AND updated_at:<=%s") {
            edges {
            node {
                fulfillments {
                    id
                    inTransitAt
                    deliveredAt
                    estimatedDeliveryAt
                    updatedAt
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
        since_date,
        until_date,
    )

    # Send POST request with query
    async with client_session.post(
        url=url, headers=headers, json={"query": body}
    ) as response:

        try:

            response.raise_for_status()
            data = await response.json(encoding="UTF-8")

            # The GraphQL API can return a 200 OK response code in cases
            # that would typically produce 4xx or 5xx errors in REST
            errors = (
                data.get("data", {}).get("bulkOperationRunQuery", {}).get("userErrors")
            ) or []

            if response.status == 200 and len(errors) > 0:

                logger.error(f"GraphQL Errors: {errors}")

        except:

            raise Exception(
                f"{response.status} {response.reason} for Shop domain {shop_domain}"
            )

    # Shopify allows for bulk operations when querying connection field that's
    # defined by the GraphQL Admin API schema. Shopify handles the query
    # execution and provides a URL where the data can be downloaded after
    # execution is complete. The app must check the link for query status. The
    # alternative is subscribing to a webhook but per Shopify those webhooks
    # sometimes trigger before execution is complete. Results are delivered in
    # a JSONL file.
    poll_query = """
        query {
            currentBulkOperation {
                id
                status
                errorCode
                createdAt
                completedAt
                objectCount
                fileSize
                url
                partialDataUrl
            }
        }    
    """

    # Wait 15 seconds before polling
    await asyncio.sleep(15)

    while True:

        async with client_session.post(
            url=url, headers=headers, json={"query": poll_query}
        ) as poll_response:

            try:

                poll_response.raise_for_status()
                poll_data = await poll_response.json(encoding="UTF-8")
                operation_status = (
                    poll_data.get("data", {})
                    .get("currentBulkOperation", {})
                    .get("status", "")
                ).lower()

                if operation_status in ["running", "created"]:

                    logger.info(
                        f"Awaiting bulk operation completion for domain {shop_domain}"
                    )
                    await asyncio.sleep(15)

                    continue

                if operation_status == "completed":

                    operation_created_at = poll_data["data"]["currentBulkOperation"][
                        "createdAt"
                    ]
                    output_url = poll_data["data"]["currentBulkOperation"]["url"]

                    break

                if operation_status == "failed":

                    error_code = poll_data.get("currentBulkOperation", {}).get(
                        "errorCode", []
                    )

                    raise Exception(
                        f"Error during bulk query execution; Code {error_code}"
                    )

                else:

                    raise Exception(
                        f"Error during bulk operation; bulk operation {operation_status}"
                    )

            except:

                raise Exception(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

    if output_url is None:

        logger.info(f"No output for {shop_name} from {since_date} to {until_date}")

    else:

        logger.info(
            f"Retrieving data for {shop_name} from {since_date} to {until_date}"
        )

        async with client_session.get(url=output_url) as response:

            response.raise_for_status()

            async for row in response.content:

                json_row = json.loads(row)

                for fulfillment in json_row.get("fulfillments", []):

                    # Only keep rows that have at least one of the additional
                    # timestamp fields
                    if (
                        fulfillment.get("inTransitAt") is not None
                        or fulfillment.get("deliveredAt") is not None
                        or fulfillment.get("estimatedDeliveryAt") is not None
                    ):

                        fulfillment_id = fulfillment.get("id").replace(
                            "gid://shopify/Fulfillment/", ""
                        )
                        flat_row = {
                            "fulfillment_id": fulfillment_id,
                            "shop_id": shop_id,
                            "shop_name": shop_name,
                            "in_transit_at": fulfillment.get("inTransitAt"),
                            "estimated_delivery_at": fulfillment.get(
                                "estimatedDeliveryAt"
                            ),
                            "delivered_at": fulfillment.get("deliveredAt"),
                            "updated_at": fulfillment.get("updatedAt"),
                        }
                        field_hash = hashlib.md5(
                            str(flat_row.values()).encode("utf-8")
                        ).hexdigest()
                        flat_row["primary_key"] = field_hash
                        # Sync time isn't included in the primary key because
                        # that would cause a record update every sync even if
                        # data has not changed
                        flat_row["synced_at"] = operation_created_at

                        flat_records.append(flat_row)


# Async query wrapper
async def fetch_data(
    shopify_keys,
    since_date: str,
    until_date: str,
):

    timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=timeout) as client_session:

        futures = [
            get_bulk_query_response(
                shopify_access_token=shop["shop_app_api_key"],
                shop_domain=shop["shop_domain_name"],
                client_session=client_session,
                since_date=since_date,
                until_date=until_date,
            )
            for shop in shopify_keys
        ]

        await asyncio.gather(*futures)


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[2]

# The app can be set to only run certain steps
steps_to_run = sys.argv[4]

if steps_to_run == "all":

    is_included_step = True

else:

    is_included_step = "fulfillment_timestamps" in steps_to_run

if (
    __name__ == "__main__"
    and is_included_step
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
):

    # Retrieve Shopify credentials and check parameter for override; the
    # override directs the app to only run to requested sync for a single shop
    shopify_keys = get_secret("SHOPIFY_DATA_TEAM_APP_API_KEYS")
    single_shop_domain_name = sys.argv[3]

    if single_shop_domain_name != "none":

        shopify_keys = [
            key_dict
            for key_dict in shopify_keys
            if key_dict.get("shop_domain_name").lower()
            == single_shop_domain_name.lower()
        ]

        # Throw an exception if there wasn't a match
        if len(shopify_keys) == 0:

            e_str = "Provided single shop domain name does not match any domain names in retrieved AWS secret value"
            logger.error(e_str)

            raise Exception(e_str)

    # Retrieve sync type from parameter
    sync_type = sys.argv[1]

    # Set target database according to environment
    if os.getenv("RES_ENV") == "production":

        database = "raw"

    else:

        database = "raw_dev"

    # Retrieve Snowflake credentials from secrets manager
    snowflake_cred = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    snowflake_user = snowflake_cred["user"]
    snowflake_password = snowflake_cred["password"]
    snowflake_account = snowflake_cred["account"]

    create_snowflake_tables()

    # Create empty flat_records list
    flat_records = []

    if sync_type == "incremental":

        # Retrieve Snowflake date before async invocation
        since_date = get_latest_snowflake_timestamp(
            schema="SHOPIFY",
            table="fulfillment_timestamps",
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_account=snowflake_account,
            timestamp_field="synced_at",
            is_date=True,
        )

    elif sync_type == "test":

        since_date = (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d")

    elif sync_type == "full":

        logger.info(f"Full sync")

        # Start in July 2015
        since_date = "2015-07-01"

    # Incremental sync on an empty table returns a since_date of None
    if since_date is None:

        logger.warning(f"No since date; Defaulting to full sync")

        # Start in July 2015
        since_date = "2015-07-01"

    since_date_object = datetime.strptime(since_date, "%Y-%m-%d").date()

    # Raise exception if since_date is in the future because that shouldn't ever
    # happen
    if since_date_object > datetime.today().date():

        raise Exception("Error: since_date is in the future")

    # Set until_date_object to an arbitrary date to begin while loop
    until_date_object = since_date_object

    while until_date_object < datetime.today().date():

        # Sync will iterate through the time period from a target date through
        # present day in 180 day increments. This time period keeps the query cost
        # sufficiently low. Full syncs begin from 2015. Set until to present day
        # if it is a future date
        until_date_object = min(
            datetime.today().date(), since_date_object + timedelta(days=180)
        )
        until_date = datetime.strftime(until_date_object, "%Y-%m-%d")
        logger.info(f"Syncing data from {since_date} to {until_date}")

        # Execute async API calls
        asyncio.run(
            fetch_data(
                shopify_keys,
                since_date=since_date,
                until_date=until_date,
            )
        )

        if len(flat_records) > 0:

            upsert_data(flat_records)

        # Clean up
        del flat_records
        gc.collect()
        flat_records = []

        # Iterate
        since_date = until_date
        since_date_object = until_date_object

    logger.info("Shopify fulfillment timestamps sync complete")
