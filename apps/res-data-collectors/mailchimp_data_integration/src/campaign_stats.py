from helper_functions import (
    get_secret,
    to_snowflake,
    get_all_data,
)
from res.utils import logger
import asyncio
import os
import sys


def get_campaign_stats(mailchimp_client, query_parameters):

    return mailchimp_client.reports.get_all_campaign_reports(**query_parameters)


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[1]

# A parameter of steps to skip can be passed. Used to run or test specific steps
is_excluded_step = "campaign_stats" in sys.argv[5]

if (
    __name__ == "__main__"
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
    and not is_excluded_step
):

    # Set target database according to environment
    if os.getenv("RES_ENV") == "production":

        database = "raw"

    else:

        database = "raw_dev"

    # Retrieve Mailchimp credentials or override credentials
    api_key_override = sys.argv[2]
    server_override = sys.argv[3]
    is_test = sys.argv[4]

    if api_key_override != "none" and server_override != "none":

        api_key_dicts = [
            {
                "api_key": api_key_override,
                "server": server_override,
            }
        ]

    else:

        api_key_dicts = get_secret("DATA_TEAM_MAILCHIMP_API_KEYS")

    # Retrieve Snowflake credentials from secrets manager
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

    # Create list to extend for response data
    output_list = []

    asyncio.run(
        get_all_data(
            api_function=get_campaign_stats,
            api_key_dicts=api_key_dicts,
            is_test=is_test,
            output_list=output_list,
        )
    )

    logger.info(f"Beginning Snowflake upsert Mailchimp campaign stats data")

    to_snowflake(database, output_list, snowflake_creds, "campaign_stats")

    logger.info("Mailchimp campaign stats sync complete")
