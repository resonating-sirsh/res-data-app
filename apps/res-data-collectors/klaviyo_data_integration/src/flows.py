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
        # production environments. Retrieve data updated in the last 50 days.
        database = "raw_dev"
        ts = datetime.utcnow().date() - timedelta(days=50)
        logger.info("Beginning test sync")

    elif os.getenv("RES_ENV") == "production":
        database = "raw"

    else:
        database = "raw_dev"

    if SYNC_TYPE == "incremental":
        ts = get_latest_snowflake_timestamp(
            database,
            "klaviyo",
            "flows",
            snowflake_creds["user"],
            snowflake_creds["password"],
            snowflake_creds["account"],
            "updated::timestamp_tz",
            "%Y-%m-%dT%H:%M:%SZ",
        )

        if ts is None:
            logger.warning(
                "Could not retrieve latest timestamp; defaulting to full sync"
            )

        else:
            logger.info("Beginning incremental sync")

    if ts is None:
        logger.info("Beginning full sync")

    # Retrieved flows have "included" extra records related to those retrieved
    # flows. The fields those use need to be explicitly defined.
    # Otherwise the server may opt to only send a subset of fields
    tag_fields = ["name"]

    # We only need a single flow action field to ensure its IDs are returned in
    # the API response. Additional calls are made to retrieve each flow action's
    # data (in order to get its flow messages) so there is no need to retrieve
    # all flow action fields now
    flow_action_fields = ["action_type"]

    # Set params
    params = {
        "filter": f"greater-or-equal(updated,{ts})" if ts else "",
        "fields[flow-action]": ",".join(flow_action_fields),
        "fields[tag]": ",".join(tag_fields),
        "include": "flow-actions,tags",
        "sort": "-updated",
    }

    # Execute program
    output_data = asyncio.run(get_all_api_data(klaviyo_api_keys, "flows", params))

    # Remove list of flow action records from the output and use their IDs to
    # retrieve additional data
    flow_actions = output_data.pop("flow_actions")

    # Construct a dictionary where each key is an account ID and each value is a
    # list of that account's flow action IDs
    flow_action_ids = {}

    for rec in flow_actions:
        account_id = rec["account_id"]
        flow_action_id = rec["id"]

        # Add the account ID as a key if it is not there yet
        if account_id not in flow_action_ids:
            flow_action_ids[account_id] = []

        # Add the record's flow action ID to its account's list
        flow_action_ids[account_id].append(flow_action_id)

    # Set new params for flow actions
    flow_fields = ["name"]
    flow_message_fields = [
        "name",
        "channel",
        "content",
        "created",
        "updated",
    ]
    params = {
        "fields[flow]": ",".join(flow_fields),
        "fields[flow-message]": ",".join(flow_message_fields),
        "include": "flow,flow-messages",
    }

    additional_output_data = asyncio.run(
        get_additional_records(
            klaviyo_api_keys, "flow-actions", flow_action_ids, params
        )
    )

    # Flows were an included record in params to make sure the flow actions had
    # their flow as a field. But, the full data for flows is already in the
    # original dict
    del additional_output_data["flows"]

    # Merge the additional data into the original output data. The data entity
    # with limited fields that was popped earlier is replaced by data here
    output_data.update(additional_output_data)

    for table, content in output_data.items():
        if len(content) > 0:
            logger.info(f"Syncing for {table}")
            to_snowflake(content, snowflake_creds, database, "klaviyo", table)
            logger.info(f"Sync complete for {table}")

        else:
            logger.info(f"No data to sync for {table}")
