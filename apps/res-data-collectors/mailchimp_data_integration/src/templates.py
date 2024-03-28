from helper_functions import (
    get_all_data,
    get_secret,
    to_s3_and_snowflake,
)
from res.utils import logger
import asyncio
import os
import sys


def get_templates(mailchimp_client, query_parameters):

    return mailchimp_client.templates.list(**query_parameters)


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[1]

# A parameter of steps to skip can be passed. Used to run or test specific steps
is_excluded_step = "templates" in sys.argv[5]

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
            api_function=get_templates,
            api_key_dicts=api_key_dicts,
            is_test=is_test,
            output_list=output_list,
        )
    )

    logger.info(f"Sending data to Snowflake and image files to S3")

    asyncio.run(
        to_s3_and_snowflake(
            bucket_name="iamcurious",
            content_type="template",
            database=database,
            output_list=output_list,
            snowflake_creds=snowflake_creds,
            table="templates",
            target_path="res_data_collectors_output/mailchimp_data_integration/templates/",
            url_key="thumbnail",
        )
    )

    logger.info("Mailchimp templates data sync complete")
