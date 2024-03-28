from datetime import datetime
from helper_functions import *
from res.utils import logger
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
import asyncio
import hashlib
import os
import snowflake.connector
import sys


# Create synchronous function to flatten info
def flatten_records(output_list):

    logger.info(f"Proceeding to flatten records")

    # Record time of invocation (string)
    current_timestamp_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    for fulfillment_service in output_list:

        shop_id = fulfillment_service["shop_id"]
        shop_name = fulfillment_service["shop_name"]
        fulfillment_service_id = fulfillment_service["id"]
        shop_fulfillment_service_concat = str(shop_id) + str(fulfillment_service_id)
        shop_fulfillment_service_id = hashlib.md5(
            shop_fulfillment_service_concat.encode("utf-8")
        ).hexdigest()
        historical_fulfillment_service_concat = (
            shop_fulfillment_service_concat + current_timestamp_utc
        )
        historical_fulfillment_service_id = hashlib.md5(
            historical_fulfillment_service_concat.encode("utf-8")
        ).hexdigest()

        flat_services.append(
            {
                "historical_fulfillment_service_id": historical_fulfillment_service_id,
                "shop_fulfillment_service_id": shop_fulfillment_service_id,
                "shop_id": shop_id,
                "shop_name": shop_name,
                "fulfillment_service_id": fulfillment_service_id,
                "name": fulfillment_service.get("name"),
                "email": fulfillment_service.get("email"),
                "service_name": fulfillment_service.get("service_name"),
                "handle": fulfillment_service.get("handle"),
                "is_fulfillment_orders_opt_in": fulfillment_service.get(
                    "fulfillment_orders_opt_in"
                ),
                "is_includes_pending_stock": fulfillment_service.get(
                    "include_pending_stock"
                ),
                "provider_id": fulfillment_service.get("provider_id"),
                "location_id": fulfillment_service.get("location_id"),
                "callback_url": fulfillment_service.get("callback_url"),
                "has_tracking_support": fulfillment_service.get("tracking_support"),
                "has_inventory_management": fulfillment_service.get(
                    "inventory_management"
                ),
                "admin_graphql_api_id": fulfillment_service.get("admin_graphql_api_id"),
                "synced_at": current_timestamp_utc,
            }
        )


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
            create table if not exists fulfillment_services (
                historical_fulfillment_service_id varchar,
                shop_fulfillment_service_id varchar,
                shop_id integer,
                shop_name varchar,
                fulfillment_service_id integer,
                name varchar,
                email varchar,
                service_name varchar,
                handle varchar,
                is_fulfillment_orders_opt_in boolean,
                is_includes_pending_stock boolean,
                provider_id integer,
                location_id integer,
                callback_url varchar,
                has_tracking_support boolean,
                has_inventory_management boolean,
                admin_graphql_api_id varchar,
                synced_at timestamp_ntz
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_fulfillment_services_data(flat_services):

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    fulfillment_services_dtypes = {
        "historical_fulfillment_service_id": VARCHAR,
        "shop_fulfillment_service_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "fulfillment_service_id": INTEGER,
        "name": VARCHAR,
        "email": VARCHAR,
        "service_name": VARCHAR,
        "handle": VARCHAR,
        "is_fulfillment_orders_opt_in": BOOLEAN,
        "is_includes_pending_stock": BOOLEAN,
        "provider_id": INTEGER,
        "location_id": INTEGER,
        "callback_url": VARCHAR,
        "has_tracking_support": BOOLEAN,
        "has_inventory_management": BOOLEAN,
        "admin_graphql_api_id": VARCHAR,
        "synced_at": TIMESTAMP(timezone=False),
    }

    dict_to_sql(
        flat_services,
        "fulfillment_services_stage",
        engine,
        dtype=fulfillment_services_dtypes,
    )

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stage table to target table")

    if len(flat_services) > 0:

        # Create stage and target tables
        fulfillment_services_stage_table = meta.tables["fulfillment_services_stage"]
        fulfillment_services_target_table = meta.tables["fulfillment_services"]

        # Structure merge; add rows when any fulfillment service value changes
        fulfillment_services_merge = MergeInto(
            target=fulfillment_services_target_table,
            source=fulfillment_services_stage_table,
            on=fulfillment_services_target_table.c.shop_fulfillment_service_id
            == fulfillment_services_stage_table.c.shop_fulfillment_service_id
            and fulfillment_services_target_table.c.shop_id
            == fulfillment_services_stage_table.c.shop_id
            and fulfillment_services_target_table.c.shop_name
            == fulfillment_services_stage_table.c.shop_name
            and fulfillment_services_target_table.c.fulfillment_service_id
            == fulfillment_services_stage_table.c.fulfillment_service_id
            and fulfillment_services_target_table.c.name
            == fulfillment_services_stage_table.c.name
            and fulfillment_services_target_table.c.email
            == fulfillment_services_stage_table.c.email
            and fulfillment_services_target_table.c.service_name
            == fulfillment_services_stage_table.c.service_name
            and fulfillment_services_target_table.c.handle
            == fulfillment_services_stage_table.c.handle
            and fulfillment_services_target_table.c.is_fulfillment_orders_opt_in
            == fulfillment_services_stage_table.c.is_fulfillment_orders_opt_in
            and fulfillment_services_target_table.c.is_includes_pending_stock
            == fulfillment_services_stage_table.c.is_includes_pending_stock
            and fulfillment_services_target_table.c.provider_id
            == fulfillment_services_stage_table.c.provider_id
            and fulfillment_services_target_table.c.location_id
            == fulfillment_services_stage_table.c.location_id
            and fulfillment_services_target_table.c.callback_url
            == fulfillment_services_stage_table.c.callback_url
            and fulfillment_services_target_table.c.has_tracking_support
            == fulfillment_services_stage_table.c.has_tracking_support
            and fulfillment_services_target_table.c.has_inventory_management
            == fulfillment_services_stage_table.c.has_inventory_management
            and fulfillment_services_target_table.c.admin_graphql_api_id
            == fulfillment_services_stage_table.c.admin_graphql_api_id,
        )

        fulfillment_services_merge.when_not_matched_then_insert().values(
            historical_fulfillment_service_id=fulfillment_services_stage_table.c.historical_fulfillment_service_id,
            shop_fulfillment_service_id=fulfillment_services_stage_table.c.shop_fulfillment_service_id,
            shop_id=fulfillment_services_stage_table.c.shop_id,
            shop_name=fulfillment_services_stage_table.c.shop_name,
            fulfillment_service_id=fulfillment_services_stage_table.c.fulfillment_service_id,
            name=fulfillment_services_stage_table.c.name,
            email=fulfillment_services_stage_table.c.email,
            service_name=fulfillment_services_stage_table.c.service_name,
            handle=fulfillment_services_stage_table.c.handle,
            is_fulfillment_orders_opt_in=fulfillment_services_stage_table.c.is_fulfillment_orders_opt_in,
            is_includes_pending_stock=fulfillment_services_stage_table.c.is_includes_pending_stock,
            provider_id=fulfillment_services_stage_table.c.provider_id,
            location_id=fulfillment_services_stage_table.c.location_id,
            callback_url=fulfillment_services_stage_table.c.callback_url,
            has_tracking_support=fulfillment_services_stage_table.c.has_tracking_support,
            has_inventory_management=fulfillment_services_stage_table.c.has_inventory_management,
            admin_graphql_api_id=fulfillment_services_stage_table.c.admin_graphql_api_id,
            synced_at=fulfillment_services_stage_table.c.synced_at,
        )

        # Execute merge
        alchemy_connection.execute(fulfillment_services_merge)

    # Close connection
    alchemy_connection.close()
    logger.info("SQLAlchemy connection closed")


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[2]

# The app can be set to only run certain steps
steps_to_run = sys.argv[4]

if steps_to_run == "all":

    is_included_step = True

else:

    is_included_step = "fulfillment_services" in steps_to_run

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

    # This step always performs full syncs. Updates are identified by rows not
    # matching any existing row

    # ETL setup
    # Create list to extend for response data
    output_list = []

    # Create empty lists to append to
    flat_services = []

    # Execute async API calls
    asyncio.run(
        fetch_all_data(
            api_endpoint="events",
            output_list=output_list,
            shopify_keys=shopify_keys,
            sync_type="full",
            pagination_type="none",
        )
    )

    # Flatten data from all API calls
    flatten_records(output_list)

    # Add to Snowflake
    create_snowflake_tables()
    upsert_fulfillment_services_data(flat_services=flat_services)

    logger.info("Shopify fulfillment services sync complete")
