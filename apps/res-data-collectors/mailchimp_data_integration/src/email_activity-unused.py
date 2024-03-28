from helper_functions import get_secret, get_from_ids_for_all_accounts, to_snowflake
from res.utils import logger
import snowflake.connector
import os
import sys
import asyncio


def get_campaign_activity(mailchimp_client, campaign_id, query_parameters):

    return mailchimp_client.reports.get_email_activity_for_campaign(
        campaign_id, **query_parameters
    )


# Collect arguments
dev_override = sys.argv[1]
api_key_override = sys.argv[2]
server_override = sys.argv[3]
is_test = sys.argv[4]
is_excluded_step = "email_activity" in sys.argv[5]

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

    # Create an object with each account's campaigns in separate lists
    campaigns_object = {key_dict["account_name"]: [] for key_dict in api_key_dicts}

    # Get campaigns with that have sent at least one email (email activity for
    # campaigns that never sent emails would just be an empty dataset for every
    # possible recipient)
    conn = snowflake.connector.connect(
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        account=snowflake_creds["account"],
        warehouse="loader_wh",
        database=database,
    )
    cur = conn.cursor()

    try:

        cur.execute(
            f"""
                select distinct 
                    account_name, 
                    id 
                from {database}.mailchimp.campaigns 
                where emails_sent > 0 
                    and id is not null 
                    and account_name is not null
            """
        )

        for (account_name, id) in cur:

            campaigns_object.get(account_name).append(id)

    except Exception as e:

        logger.error(e)

        raise

    finally:

        cur.close()

    # For test syncs limit the number of records for each account to 2
    if is_test.lower() == "true":

        for account, campaigns in campaigns_object.items():

            campaigns_object[account] = campaigns[:2]

    # Set up logger string which says how many campaigns are in each account
    sync_str = "test sync" if is_test == "true" else "full sync"
    record_totals_str = "; ".join(
        f"{account}: {len(campaigns)}"
        for account, campaigns in campaigns_object.items()
    )
    logger.info(
        f"Beginning {sync_str} for {len(campaigns_object)} accounts. Number of campaigns for each account: {record_totals_str}"
    )

    # Iterate through 10-campaign sized slices for each account
    iteration = 1

    while sum(len(v) for k, v in campaigns_object.items()) > 0:

        output_list = []
        campaigns_object_slice = {}

        for account, ids in campaigns_object.items():

            if len(ids) > 0:

                campaigns_object_slice[account] = ids[:10]

                del ids[:10]

        logger.info(f"Retrieving API data for iteration {iteration}")
        asyncio.run(
            get_from_ids_for_all_accounts(
                api_key_dicts,
                get_campaign_activity,
                campaigns_object_slice,
                output_list,
            )
        )

        if len(output_list) > 0:

            # Remove records with no email address
            output_list = [
                rec for rec in output_list if rec.get("email_address", "") != ""
            ]

            logger.info(f"Sending API data to Snowflake for iteration {iteration}")
            to_snowflake(database, output_list, snowflake_creds, "email_activity")

        iteration += 1

    logger.info("Mailchimp campaign email activity sync complete")
