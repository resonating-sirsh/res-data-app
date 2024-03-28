import gc
import hashlib
import os
import sys
import asyncio
import snowflake.connector
from datetime import datetime, timedelta
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
from helper_functions import (
    get_secret,
    get_latest_snowflake_timestamp,
    fetch_all_data,
    dict_to_sql,
    safe_strptime,
)
from res.utils import logger


# Create synchronous function to flatten info
def flatten_records(output_list):

    logger.info(f"Proceeding to flatten records")

    for events_record in output_list:

        shop_id = events_record["shop_id"]
        shop_name = events_record["shop_name"]
        event_id = events_record["id"]
        created_at = events_record["created_at"]
        shop_event_concat = str(shop_id) + str(event_id)
        shop_event_id = hashlib.md5(shop_event_concat.encode("utf-8")).hexdigest()
        historical_shop_event_concat = str(shop_id) + str(event_id) + str(created_at)
        historical_shop_event_id = hashlib.md5(
            historical_shop_event_concat.encode("utf-8")
        ).hexdigest()

        flat_events.append(
            {
                "historical_shop_event_id": f"{historical_shop_event_id}",
                "shop_event_id": f"{shop_event_id}",
                "shop_id": shop_id,
                "shop_name": f"{shop_name}",
                "event_id": f"{event_id}",
                "event_subject_id": events_record["subject_id"],
                "created_at": safe_strptime(events_record["created_at"]),
                "event_subject_type": events_record["subject_type"],
                "event_type": events_record["verb"],
                "event_resources": str(events_record["arguments"]),
                "event_body": events_record["body"],
                "event_message": events_record["message"],
                "event_author": events_record["author"],
                "event_description": events_record["description"],
                "event_path": events_record["path"],
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
            create table if not exists shop_content_events (
                historical_shop_event_id varchar,
                shop_event_id varchar,
                shop_id integer,
                shop_name varchar,
                event_id integer,
                event_subject_id integer,
                created_at timestamp_tz,
                event_subject_type varchar,
                event_type varchar,
                event_resources varchar,
                event_body varchar,
                event_message varchar,
                event_author varchar,
                event_description varchar,
                event_path varchar
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_events_data(flat_events):

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    dtypes = {
        "historical_shop_event_id": VARCHAR,
        "shop_event_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "event_id": INTEGER,
        "event_subject_id": INTEGER,
        "created_at": TIMESTAMP(timezone=True),
        "event_subject_type": VARCHAR,
        "event_type": VARCHAR,
        "event_resources": VARCHAR,
        "event_body": VARCHAR,
        "event_message": VARCHAR,
        "event_author": VARCHAR,
        "event_description": VARCHAR,
        "event_path": VARCHAR,
    }

    dict_to_sql(flat_events, "shop_content_events_stage", engine, dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stage table to target table")

    if len(flat_events) > 0:

        # Create stage and target tables
        events_stage_table = meta.tables["shop_content_events_stage"]
        events_target_table = meta.tables["shop_content_events"]

        # Structure merge
        events_merge = MergeInto(
            target=events_target_table,
            source=events_stage_table,
            on=events_target_table.c.historical_shop_event_id
            == events_stage_table.c.historical_shop_event_id,
        )

        events_cols = {
            "historical_shop_event_id": events_stage_table.c.historical_shop_event_id,
            "shop_event_id": events_stage_table.c.shop_event_id,
            "shop_id": events_stage_table.c.shop_id,
            "shop_name": events_stage_table.c.shop_name,
            "event_id": events_stage_table.c.event_id,
            "event_subject_id": events_stage_table.c.event_subject_id,
            "created_at": events_stage_table.c.created_at,
            "event_subject_type": events_stage_table.c.event_subject_type,
            "event_type": events_stage_table.c.event_type,
            "event_resources": events_stage_table.c.event_resources,
            "event_body": events_stage_table.c.event_body,
            "event_message": events_stage_table.c.event_message,
            "event_author": events_stage_table.c.event_author,
            "event_description": events_stage_table.c.event_description,
            "event_path": events_stage_table.c.event_path,
        }

        events_merge.when_not_matched_then_insert().values(**events_cols)

        events_merge.when_matched_then_update().values(**events_cols)

        # Execute merge
        alchemy_connection.execute(events_merge)

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

    is_included_step = "shop_content_events" in steps_to_run

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

    if sync_type == "incremental":

        # Retrieve Snowflake timestamp before async invocation
        since_ts = get_latest_snowflake_timestamp(
            schema="SHOPIFY",
            table="SHOP_CONTENT_EVENTs",
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_account=snowflake_account,
            timestamp_field="created_at",
        )

    elif sync_type == "test":

        since_ts = (datetime.utcnow() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

    else:

        since_ts = None

    # ETL setup
    # Create list to extend for response data
    output_list = []

    # Create empty lists to append to
    flat_events = []

    # Execute async API calls
    asyncio.run(
        fetch_all_data(
            api_endpoint="events",
            output_list=output_list,
            shopify_keys=shopify_keys,
            sync_type=sync_type,
            pagination_type="since_id",
            since_target_field="created_at",
            since_ts=since_ts,
        )
    )

    # Flatten data from all API calls
    flatten_records(output_list)

    # Clean up
    del output_list
    gc.collect()

    # Add to Snowflake
    create_snowflake_tables()
    upsert_events_data(flat_events=flat_events)

    logger.info("Shopify shop content events sync complete")
