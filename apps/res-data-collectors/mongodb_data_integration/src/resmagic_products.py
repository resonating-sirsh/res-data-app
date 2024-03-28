import hashlib
import os
import sys
import urllib.parse
from datetime import datetime, timedelta

from bson import ObjectId
from helper_functions import (
    camel_to_snake,
    get_latest_snowflake_timestamp,
    get_secret,
    to_snowflake,
)
from pymongo import MongoClient

from res.utils import logger

# Retrieve args; set null params
SYNC_TYPE = sys.argv[1].lower()
DEV_OVERRIDE = (sys.argv[2]).lower() == "true"
# Checks if the python file name (retrieved programmatically given known length
# of the flow_name input relative path) is in steps_to_run input or if
# steps_to_run = all
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[3] or sys.argv[3].lower() == "all"
RES_ENV = os.getenv("RES_ENV", "development").lower()


def get_mongo_products(client: MongoClient, ts: datetime):
    """
    Retrieves products from MongoDB -- the resmagic database and products
    collection

    Inputs:

        - client: a MongoDB Client
        - ts: a timestamp used as the minimum updatedAt timestamp for
            retrieved records

    Outputs:

        - output_data: a list containing MongoDB products data with
        datetimes, dicts, lists, and ObjectIds stringified
    """

    try:
        # Log timestamp if not none
        log_str = (
            f"Retrieving products from MongoDB since {ts.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}"
            if ts
            else "Retrieving all products from MongoDB"
        )
        logger.info(log_str)
        sync_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f %Z")

        if ts:
            cursor = client.resmagic.products.find({"updatedAt": {"$gt": ts}})

        else:
            cursor = client.resmagic.products.find()

    except Exception as e:
        logger.error(e)

        raise

    logger.info("Retrieved data. Parsing")
    output_data = []

    # Returned cursor is an iterable
    for document in cursor:
        # Check values within doc. Convert Object IDs, datetimes, dicts, and
        # lists to strings
        row = {}

        for key, value in document.items():
            # Skip undefined columns
            if key.lower() == "undefined":
                continue

            if isinstance(value, ObjectId):
                converted_value = value.__str__()

                # Add ObjectId creation timestamp as a field
                row["object_created_at"] = value.generation_time.strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )

            elif isinstance(value, (list, dict)):
                converted_value = str(value)

            elif isinstance(value, datetime):
                converted_value = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

            else:
                converted_value = value

            row[camel_to_snake(key)] = converted_value

        # Create a unique key using the values of the recorded state
        record_field_hash = hashlib.md5(str(row.values()).encode("utf-8")).hexdigest()
        row["primary_key"] = record_field_hash

        # Add sync time. This isn't part of the primary key because that can
        # create multiple records for observations of the same record state
        row["synced_at"] = sync_time

        output_data.append(row)

    return output_data


if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):
    database = (
        "raw" if not (RES_ENV == "development" or SYNC_TYPE == "test") else "raw_dev"
    )

    # Retrieve Snowflake credentials from secrets manager
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

    if SYNC_TYPE == "incremental":
        ts = get_latest_snowflake_timestamp(
            database,
            "mongodb",
            "resmagic_products",
            snowflake_creds["user"],
            snowflake_creds["password"],
            snowflake_creds["account"],
            "synced_at",
        )

        if ts is None:
            logger.warning(
                "Could not retrieve latest timestamp; defaulting to full sync"
            )

        else:
            logger.info("Beginning incremental sync")

    elif SYNC_TYPE == "test":
        ts = datetime.utcnow() - timedelta(days=5)
        logger.info("Beginning test sync")

    # Otherwise defaults to a full sync
    else:
        ts = None

        if SYNC_TYPE == "full":
            logger.info("Beginning full sync")

        else:
            logger.warning("Invalid sync type provided; beginning full sync")

    # Retrieve MongoDB Credentials
    mongodb_host = os.getenv("MONGODB_HOST", "resmagic.fahv4.mongodb.net")
    mongodb_username = urllib.parse.quote_plus(get_secret("MONGODB_USER")[RES_ENV])
    mongodb_password = urllib.parse.quote_plus(get_secret("MONGODB_PASSWORD")[RES_ENV])
    client = MongoClient(
        "mongodb+srv://%s:%s@%s/?retryWrites=true&w=majority"
        % (mongodb_username, mongodb_password, mongodb_host)
    )

    data = get_mongo_products(client, ts)

    if len(data) > 0:
        to_snowflake(data, snowflake_creds, database, "mongodb", "resmagic_products")
        logger.info("Sync complete")

    else:
        logger.info("No data to sync")
