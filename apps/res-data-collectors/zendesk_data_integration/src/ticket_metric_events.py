from datetime import timedelta, datetime
from res.utils import logger
from helper_functions import (
    get_secret,
    get_latest_snowflake_epoch,
    get_all_api_data,
    to_snowflake,
)
import os
import sys
import asyncio


# Retrieve args; set null params
sync_type = sys.argv[1].lower()
dev_override = (sys.argv[2]).lower() == "true"
# Checks if the python file name (retrieved programmatically given known length
# of the flow_name input relative path) is in steps_to_run input or if
# steps_to_run = all
is_included_step = sys.argv[0][9:-3] in sys.argv[3] or sys.argv[3].lower() == "all"
params = None
ts = None

if (
    __name__ == "__main__"
    and is_included_step
    and (os.getenv("RES_ENV") == "production" or dev_override)
):

    # Retrieve Snowflake credentials from secrets manager
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

    # Set database type
    if sync_type == "test":

        # Always use the dev snowflake database for this sync type -- even in
        # production environments. Retrieve one day of data.
        database = "raw_dev"
        ts = int((datetime.utcnow() - timedelta(days=1)).timestamp())
        logger.info("Beginning test sync")

    elif os.getenv("RES_ENV") == "production":

        database = "raw"

    else:

        database = "raw_dev"

    if sync_type == "incremental":

        ts = get_latest_snowflake_epoch(
            database,
            "zendesk",
            "ticket_events",
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

    if ts is None:

        # Resonance was founded in July 2017; use July 1st 2017 as start
        ts = 1498867200
        logger.info("Beginning full sync")

    # ETL setup
    output_list = []
    params = {"start_time": ts}
    asyncio.run(
        get_all_api_data(
            get_secret("DATA-TEAM-ZENDESK-API-ACCESS"),
            "ticket_metric_events",
            "next_page",
            "/api/v2/incremental/ticket_metric_events",
            output_list,
            params=params,
        )
    )

    if len(output_list) > 0:

        to_snowflake(
            output_list, snowflake_creds, database, "zendesk", "ticket_metric_events"
        )
        logger.info("Sync complete")

    else:

        logger.info("No data to sync")
