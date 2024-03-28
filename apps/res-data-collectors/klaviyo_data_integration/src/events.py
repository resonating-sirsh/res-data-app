import asyncio
import gc
import os
import sys
from datetime import datetime, timedelta, timezone

from helper_functions import (
    get_secret,
    get_latest_snowflake_timestamp,
    get_all_api_data,
    to_snowflake,
)

from res.utils import logger

# Retrieve args; set null params
SYNC_TYPE = sys.argv[1].lower()
DEV_OVERRIDE = (sys.argv[2]).lower() == "true"
# Checks if the python file name (retrieved programmatically given known length
# of the flow_name input relative path) is in steps_to_run input or if
# steps_to_run = all
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[3] or sys.argv[3].lower() == "all"
# The app can be run for a single Klaviyo account (usually intended to allow for
# a full sync of a newly added account without triggering a full sync for all
# accounts)
SINGLE_ACCOUNT = sys.argv[4].lower()
RES_ENV = os.getenv("RES_ENV", "development").lower()
CURRENT_TS = datetime.now(timezone.utc)

if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):
    # Retrieve API keys
    klaviyo_api_keys_dict = get_secret("KLAVIYO_DATA_TEAM_API_KEYS")

    if SINGLE_ACCOUNT != "none":
        logger.info(f"Syncing solely for account {SINGLE_ACCOUNT}")
        klaviyo_api_keys_dict = [
            key_dict
            for key_dict in klaviyo_api_keys_dict
            if key_dict.get("account") == SINGLE_ACCOUNT
        ]

    klaviyo_api_keys = [account["api_key"] for account in klaviyo_api_keys_dict]

    # Retrieve Snowflake credentials and set Snowflake database
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    database = (
        "raw" if not (RES_ENV == "development" or SYNC_TYPE == "test") else "raw_dev"
    )

    if SYNC_TYPE == "test":
        # Always use the dev snowflake database for this sync type -- even in
        # production environments. Retrieve data updated in the last 2 days.
        database = "raw_dev"
        start_ts = CURRENT_TS - timedelta(days=2)
        logger.info("Beginning test sync")

    elif SYNC_TYPE == "incremental":
        start_ts = get_latest_snowflake_timestamp(
            database,
            "klaviyo",
            "events",
            snowflake_creds["user"],
            snowflake_creds["password"],
            snowflake_creds["account"],
            "datetime::timestamp_tz",
        )

        if start_ts is None:
            e_str = "Could not retrieve latest timestamp"
            logger.error(e_str)

            raise Exception(e_str)

        else:
            logger.info("Beginning incremental sync")

    elif SYNC_TYPE == "full":
        # The step will always split syncs containing more than a month of data
        # into multiple syncs. Full syncs use a start date of the March 1st 2023
        # for the purposes of this calculation
        start_ts = datetime(2023, 1, 1, 0, 0, 0, 0, timezone.utc)
        logger.info("Beginning full sync")

    else:
        e_str = "Null or invalid sync type provided; sync type must be either \
            full, incremental, or test"
        logger.error(e_str)
        raise ValueError(e_str)

    while start_ts < CURRENT_TS:
        # Set end timestamp and string values for it and start. End timestamps
        # in the future are allowed within the Klaviyo API. Sync 5 days of data
        # at a time
        end_ts = min(start_ts + timedelta(days=5), CURRENT_TS)
        start_ts_str = start_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_ts_str = end_ts.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Set params according to campaign channel and sync type
        params = {
            "filter": (
                f"greater-or-equal(datetime,{start_ts_str})"
                + (f",less-than(datetime,{end_ts_str})")
            )
        }

        # Execute program
        logger.info(
            f"Retrieving Klaviyo events data from {start_ts_str} to {end_ts_str}"
        )
        output_data = asyncio.run(get_all_api_data(klaviyo_api_keys, "events", params))[
            "events"
        ]

        if len(output_data) > 0:
            logger.info(f"Sending events data to Snowflake")
            to_snowflake(output_data, snowflake_creds, database, "klaviyo", "events")
            logger.info(f"Events sent")

        else:
            logger.info(f"No data to sync from {start_ts_str} to {end_ts_str}")

        # Remove data from memory
        del output_data
        gc.collect()

        # Increment start ts
        start_ts = end_ts
