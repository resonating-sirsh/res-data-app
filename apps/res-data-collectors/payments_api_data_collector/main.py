import os
import gc
import json
from datetime import timedelta, date, datetime
import requests
import hashlib
from helper_functions import (
    get_secret,
    get_latest_snowflake_timestamp,
    to_snowflake,
)

from res.utils import logger

# Retrieve args; set null params
SYNC_TYPE = os.getenv("sync_type").lower()
DEV_OVERRIDE = os.getenv("dev_override").lower() == "true"
RES_ENV = os.getenv("RES_ENV", "development").lower()
CURRENT_DATE = date.today()
CURRENT_TS = datetime.utcnow()

if __name__ == "__main__" and (RES_ENV == "production" or DEV_OVERRIDE):
    # Retrieve API key
    api_key = get_secret("RES_PAYMENTS_API_KEY")["RES_PAYMENTS_API_KEY"]

    # Retrieve Snowflake credentials and set Snowflake database
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    ts = None
    database = (
        "raw" if not (RES_ENV == "development" or SYNC_TYPE == "test") else "raw_dev"
    )

    if SYNC_TYPE == "test":
        # Always use the dev snowflake database for this sync type -- even in
        # production environments. Retrieve five days of data.
        database = "raw_dev"
        start_date = CURRENT_DATE - timedelta(days=5)
        logger.info("Beginning test sync")

    elif SYNC_TYPE == "incremental":
        start_date = get_latest_snowflake_timestamp(
            database,
            "resmagic_api",
            "payments_api_transactions",
            snowflake_creds["user"],
            snowflake_creds["password"],
            snowflake_creds["account"],
            "updated_at::date",
        )

        if start_date is None:
            logger.warning("Could not retrieve latest timestamp")

        else:
            logger.info("Beginning incremental sync")

    elif SYNC_TYPE == "full":
        # The step will always split syncs containing more than a month of data
        # into multiple syncs. Full syncs use a start date of December 7th 2022,
        # the date of the earliest payment record
        start_date = date(2022, 12, 7)
        logger.info("Beginning full sync")

    else:
        e_str = "Null or invalid sync type provided; sync type must be either \
            full, incremental, or test"
        logger.error(e_str)
        raise ValueError(e_str)

    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    while start_date < CURRENT_DATE:
        # Sync 180 days (~ 6 months) of data at a time
        end_date = min(start_date + timedelta(days=180), CURRENT_DATE)
        params = {"start_date": start_date, "end_date": end_date}

        logger.info(f"Retrieving from {start_date} to {end_date}")

        try:
            response = requests.get(
                url="https://data.resmagic.io/payments-api/GetTransactions/",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            data = json.loads(response.text)

            for rec in data:
                # Create a unique key using the values of the recorded state
                record_field_hash = hashlib.md5(
                    str(rec.values()).encode("utf-8")
                ).hexdigest()
                rec["primary_key"] = record_field_hash

                # Add sync time. This isn't part of the primary key because
                # that can create multiple records for observations of the
                # same record state
                rec["synced_at"] = CURRENT_TS

        except Exception as e:
            logger.error(e)

            raise

        if len(data) > 0:
            logger.info(f"Sending data to Snowflake")
            to_snowflake(
                data,
                snowflake_creds,
                database,
                "resmagic_api",
                "payments_api_transactions",
            )
            logger.info(f"Data sent")

        else:
            logger.info(f"No data to sync from {start_date} to {end_date}")

        # Remove data from memory
        del data
        gc.collect()

        # Increment start ts
        start_date = end_date
