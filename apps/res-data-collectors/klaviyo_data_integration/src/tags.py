import asyncio
import os
import sys

from helper_functions import (
    get_secret,
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

    # The tags endpoint does not allow filtering by datetime, therefore all
    # syncs are full syncs
    logger.info("Retrieving tags")
    params = {
        "fields[tag-group]": "name",
        "include": "tag-group",
    }

    # Execute program
    output_data = asyncio.run(get_all_api_data(klaviyo_api_keys, "tags", params))
    output_data["tag_groups"] = asyncio.run(
        get_all_api_data(klaviyo_api_keys, "tag-groups")
    )["tag_groups"]

    for table, content in output_data.items():
        if len(content) > 0:
            logger.info(f"Syncing for {table} table")
            to_snowflake(content, snowflake_creds, database, "klaviyo", table)
            logger.info(f"Sync complete for {table} table")

        else:
            logger.info(f"No data to sync for {table}")
