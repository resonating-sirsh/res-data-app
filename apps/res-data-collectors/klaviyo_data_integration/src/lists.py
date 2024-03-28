import asyncio
import os
import sys
from datetime import datetime, timedelta

from helper_functions import (
    get_secret,
    get_latest_snowflake_timestamp,
    get_all_api_data,
    get_additional_records,
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
    ts = None
    database = (
        "raw" if not (RES_ENV == "development" or SYNC_TYPE == "test") else "raw_dev"
    )

    if SYNC_TYPE == "test":
        # Always use the dev snowflake database for this sync type -- even in
        # production environments. Retrieve data updated in the last 5 days.
        database = "raw_dev"
        ts = datetime.utcnow().date() - timedelta(days=5)
        logger.info("Beginning test sync")

    elif os.getenv("RES_ENV") == "production":
        database = "raw"

    else:
        database = "raw_dev"

    if SYNC_TYPE == "incremental":
        ts = get_latest_snowflake_timestamp(
            database,
            "klaviyo",
            "lists",
            snowflake_creds["user"],
            snowflake_creds["password"],
            snowflake_creds["account"],
            "updated::timestamp_tz",
        )

        if ts is None:
            logger.warning(
                "Could not retrieve latest timestamp; defaulting to full sync"
            )

        else:
            logger.info("Beginning incremental sync")

    if ts is None:
        logger.info("Beginning full sync")

    # Retrieved data has "included" extra records related to retrieved data.
    # The fields those use need to be explicitly defined. Otherwise the server
    # may opt to only send a subset of fields
    tag_fields = ["name"]

    # Set params according to campaign channel and sync type. This endpoint only
    # allows greater than and not greater than or equal to. This shouldn't cause
    # problems but any utilized timestamp will have an hour subtracted from it
    # just in case
    if ts is not None:
        ts = ts - timedelta(hours=1)
        ts = ts.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "filter": f"greater-than(updated,{ts})" if ts else "",
        "fields[tag]": ",".join(tag_fields),
        "include": "tags",
    }

    # Execute program
    logger.info("Retrieving lists")
    output_data = asyncio.run(get_all_api_data(klaviyo_api_keys, "lists", params))

    # Construct a dictionary where each key is an account ID and each value is a
    # list of that account's list IDs
    list_ids = {}

    for rec in output_data["lists"]:
        account_id = rec["account_id"]
        list_id = rec["id"]

        # Add the account ID as a key if it is not there yet
        if account_id not in list_ids:
            list_ids[account_id] = []

        # Add the record's list ID to its account's list
        list_ids[account_id].append(list_id)

    # Set new params for list profiles query. Profiles data is retrieved for
    # all profiles in another step so this table of list membership doesn't
    # need full details for each profile beyond IDs and join timestamps. Klaviyo
    # might provide it but we won't ask for it
    profile_fields = ["joined_group_at"]
    params = {
        "fields[profile]": ",".join(profile_fields),
        "page[size]": "100",
    }
    logger.info("Retrieving list profiles")
    additional_output_data = asyncio.run(
        get_additional_records(
            klaviyo_api_keys,
            "lists/id/profiles/",
            list_ids,
            params,
            "list_profiles",
        )
    )

    # Merge the additional data into the original output data
    output_data.update(additional_output_data)

    for table, content in output_data.items():
        if len(content) > 0:
            logger.info(f"Syncing for {table} table")
            to_snowflake(content, snowflake_creds, database, "klaviyo", table)
            logger.info(f"Sync complete for {table} table")

        else:
            logger.info(f"No data to sync for {table}")
