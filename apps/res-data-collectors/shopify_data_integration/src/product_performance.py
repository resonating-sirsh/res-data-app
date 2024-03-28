import aiohttp
import asyncio
import gc
import hashlib
import math
import os
import snowflake.connector
import sys
import time
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
            create table if not exists product_performance (
                historical_product_performance_id varchar,
                product_id integer,
                shop_id integer,
                shop_name varchar,
                shop_timezone varchar,                                
                shop_currency varchar,                                
                cart_sessions integer,
                checkout_sessions integer,
                checkout_to_purchase_rate number(16, 5),
                gross_sales number(16, 2),
                net_sales number(16, 2),
                purchase_sessions integer,
                quantity_added_to_cart integer,
                quantity_purchased integer,
                view_cart_checkout_to_purchase_rate number(16, 5),
                view_sessions integer,
                view_to_cart_rate number(16, 5),
                view_to_purchase_rate number(16, 5),
                date_day date
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
        "historical_product_performance_id": VARCHAR,
        "product_id": INTEGER,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "shop_timezone": VARCHAR,
        "shop_currency": VARCHAR,
        "cart_sessions": INTEGER,
        "checkout_sessions": INTEGER,
        "checkout_to_purchase_rate": NUMERIC(16, 5),
        "gross_sales": NUMERIC(16, 2),
        "net_sales": NUMERIC(16, 2),
        "purchase_sessions": INTEGER,
        "quantity_added_to_cart": INTEGER,
        "quantity_purchased": INTEGER,
        "view_cart_checkout_to_purchase_rate": NUMERIC(16, 5),
        "view_sessions": INTEGER,
        "view_to_cart_rate": NUMERIC(16, 5),
        "view_to_purchase_rate": NUMERIC(16, 5),
        "date_day": DATE,
    }

    dict_to_sql(flat_records, "product_performance_stage", engine, dtype=dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    if len(flat_records) > 0:

        logger.info("Merging data from stage table to target table")

        # Create stage and target tables
        stage_table = meta.tables["product_performance_stage"]
        target_table = meta.tables["product_performance"]

        # Structure merge; add rows when any carrier service value changes
        product_performance_merge = MergeInto(
            target=target_table,
            source=stage_table,
            on=target_table.c.historical_product_performance_id
            == stage_table.c.historical_product_performance_id,
        )

        product_performance_cols = {
            "historical_product_performance_id": stage_table.c.historical_product_performance_id,
            "product_id": stage_table.c.product_id,
            "shop_id": stage_table.c.shop_id,
            "shop_name": stage_table.c.shop_name,
            "shop_timezone": stage_table.c.shop_timezone,
            "shop_currency": stage_table.c.shop_currency,
            "cart_sessions": stage_table.c.cart_sessions,
            "checkout_sessions": stage_table.c.checkout_sessions,
            "checkout_to_purchase_rate": stage_table.c.checkout_to_purchase_rate,
            "gross_sales": stage_table.c.gross_sales,
            "net_sales": stage_table.c.net_sales,
            "purchase_sessions": stage_table.c.purchase_sessions,
            "quantity_added_to_cart": stage_table.c.quantity_added_to_cart,
            "quantity_purchased": stage_table.c.quantity_purchased,
            "view_cart_checkout_to_purchase_rate": stage_table.c.view_cart_checkout_to_purchase_rate,
            "view_sessions": stage_table.c.view_sessions,
            "view_to_cart_rate": stage_table.c.view_to_cart_rate,
            "view_to_purchase_rate": stage_table.c.view_to_purchase_rate,
            "date_day": stage_table.c.date_day,
        }

        product_performance_merge.when_not_matched_then_insert().values(
            **product_performance_cols
        )

        product_performance_merge.when_matched_then_update().values(
            **product_performance_cols
        )

        # Execute merge
        alchemy_connection.execute(product_performance_merge)

    # Close connection
    alchemy_connection.close()
    logger.info("SQLAlchemy connection closed")


async def get_query_response(
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
            url=f"https://{shop_domain}.myshopify.com/admin/api/2022-10/shop.json?fields=id,name,iana_timezone,currency",
            headers=headers,
        ) as response:

            try:

                response.raise_for_status()
                shop_config_data = (await response.json(encoding="UTF-8")).get("shop")
                shop_id = shop_config_data.get("id")
                shop_name = shop_config_data.get("name")
                shop_timezone = shop_config_data.get("iana_timezone")
                shop_currency = shop_config_data.get("currency")

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
                    time.sleep(wait_time)

                    continue

                else:

                    # End function for shop if API call fails after wait with non
                    # too many requests response code
                    return

    # Set up url, parameters, and JSON
    url = f"https://{shop_domain}.myshopify.com/admin/api/2023-01/graphql.json"
    body = """
            query {
            shopifyqlQuery(query: "
                FROM products 
                SHOW 
                    sum(view_sessions), 
                    sum(cart_sessions), 
                    sum(checkout_sessions), 
                    sum(purchase_sessions),
                    view_to_cart_rate,
                    view_cart_checkout_to_purchase_rate,
                    checkout_to_purchase_rate,
                    view_to_purchase_rate,
                    sum(quantity_added_to_cart), 
                    sum(quantity_purchased), 
                    sum(gross_sales), 
                    sum(net_sales) 
                GROUP BY day, product_id 
                SINCE %s
                UNTIL %s
                ORDER BY day") {
                __typename
                ... on TableResponse {
                tableData {
                    unformattedData
                    rowData
                    columns {
                    name
                    dataType
                    displayName
                    }
                }
                }
                parseErrors {
                code
                message
                range {
                    start {
                    line
                    character
                    }
                    end {
                    line
                    character
                    }
                }
                }
            }
            }
        """ % (
        since_date,
        until_date,
    )

    # Send POST request with query
    while True:

        async with client_session.post(
            url=url, headers=headers, json={"query": body}
        ) as response:

            try:

                response.raise_for_status()
                data = await response.json(encoding="UTF-8")
                unformatted_data = (
                    data.get("data", {})
                    .get("shopifyqlQuery", {})
                    .get("tableData", {})
                    .get("unformattedData", [])
                )
                column_info = (
                    data.get("data", {})
                    .get("shopifyqlQuery", {})
                    .get("tableData", {})
                    .get("columns", {})
                )

                # The GraphQL API can return a 200 OK response code in cases
                # that would typically produce 4xx or 5xx errors in REST
                errors = (
                    data.get("data", {}).get("shopifyqlQuery", {}).get("parseErrors")
                )

                if response.status == 200 and errors is not None:

                    # Loop through errors. If the only error is a throttle then
                    # wait and retry. Otherwise print errors
                    if len(errors) == 1 and errors[0]["code"] == "THROTTLED":

                        # Retrieve query cost
                        query_cost = (
                            data.get("extensions", {})
                            .get("cost", {})
                            .get("actualQueryCost")
                        )
                        restore_rate = (
                            data.get("extensions", {})
                            .get("cost", {})
                            .get("throttleStatus", {})
                            .get("restoreRate")
                        )

                        # Wait time is required cost divided by the restore rate
                        wait_time = math.ceil(query_cost / restore_rate)

                        logger.info(
                            f"Rate limit reached; sleeping for {wait_time} seconds"
                        )
                        time.sleep(wait_time)

                        continue

                    else:

                        logger.warning(f"GraphQL Errors: {errors}")

            except:

                logger.warning(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

                return

            break

    if len(unformatted_data) > 0:

        column_names = []

        # Renaming in the query makes it slower
        for column in column_info:

            column["name"] = column["name"].replace("sum_", "")
            column_names.append(column["name"])

        column_names[0] = "date_day"

        for row in unformatted_data:

            row_dict = {}

            for col_number, col_value in enumerate(row, 0):

                row_dict[column_names[col_number]] = row[col_number]

            # Add shop config info
            row_dict["shop_id"] = shop_id
            row_dict["shop_name"] = shop_name
            row_dict["shop_timezone"] = shop_timezone
            row_dict["shop_currency"] = shop_currency

            # Create unique key and add to row
            p_key_concat = str(row_dict["product_id"]) + str(row_dict["date_day"])
            p_key = hashlib.md5(p_key_concat.encode("utf-8")).hexdigest()
            row_dict["historical_product_performance_id"] = p_key

            flat_records.append(row_dict)


# Async query wrapper
async def fetch_data(
    shopify_keys,
    since_date: str,
    until_date: str,
):

    async with aiohttp.ClientSession() as client_session:

        futures = [
            get_query_response(
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

    is_included_step = "product_performance" in steps_to_run

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
            table="PRODUCT_PERFORMANCE",
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_account=snowflake_account,
            timestamp_field="date_day",
            is_date=True,
        )

    elif sync_type == "test":

        since_date = (datetime.utcnow() - timedelta(days=5)).strftime("%Y-%m-%d")

    elif sync_type == "full":

        # Resonance was founded in July 2015
        since_date = "2015-07-01"

    # Incremental sync on an empty table returns a since_date of None
    if since_date is None:

        logger.warning(
            f"No since date; Defaulting to full sync for Shopify product performance"
        )

        sync_type = "full"

        # Resonance was founded in July 2015
        since_date = "2015-07-01"

    logger.info(f"Starting {sync_type} sync for Shopify product performance")
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

    logger.info("Shopify product performance sync complete")
