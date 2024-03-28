from datetime import datetime
from mailchimp_marketing import Client
from mailchimp_marketing.api_client import ApiClientError
from helper_functions import get_secret, to_snowflake
from res.utils import logger
import hashlib
import snowflake.connector
import os
import sys
import asyncio


async def campaign_content(
    campaign_id: str,
    mailchimp_client: Client,
    output_list: list,
    semaphore: asyncio.Semaphore,
):

    """
    Retrieves the  HTML and plain-text content for a given Mailchimp account
    and email campaign (campaign_id)

    Parameters
    ----------
    - campaign_id:
        ID of the target campaign
    mailchimp_client:
        Client object with configs for api_key and server for a given
        Mailchimp account
    - output_list:
        List to add processed API records to from the output of the function
    - semaphore:
        Asyncio Semaphore class; used to limit the number of simultaneous
        actions. Tasks making use of the same Semaphore must wait for space
        to open up based on the integer value of the Semaphore
    """
    async with semaphore:

        while True:

            try:

                data = mailchimp_client.campaigns.get_content(campaign_id)

                # Add account info and primary key to data
                data["account_name"] = account_name
                data["campaign_id"] = campaign_id

                # Create a unique key using the values of the recorded state
                record_field_hash = hashlib.md5(
                    str(data.values()).encode("utf-8")
                ).hexdigest()
                data["primary_key"] = record_field_hash

                # Add sync time. This isn't included in the primary key because that would
                # cause a record update every sync even if data has not changed
                data["synced_at"] = datetime.utcnow().strftime(
                    "%Y-%m-%d %H:%M:%S.%f %Z"
                )

                output_list.append(data)

                break

            except ApiClientError as e:

                if e.status_code == 429:

                    logger.warning("Throttled; napping for a minute")

                    await asyncio.sleep(60)

                else:

                    logger.error(f"Error: {e}")

                    raise

            except Exception as e:

                logger.error(e)

                raise


async def account_campaign_content(
    api_key_dict: dict,
    campaign_ids: list,
    output_list: list,
):
    """
    Wrapper for the API call tasks of a given account. This function
    contains the Mailchimp Client class instance that each account will use.
    Additionally it makes use of the asyncio Semaphore class to create a
    concurrency limit for the tasks that it awaits. This limits the number
    of active connections for a given account to 10, which is the limit
    enforced by Mailchimp before they begin to throttle.

    Parameters
    ----------
    - api_key_dict:
        Dictionary containing the account api key, server, and name
    campaign_ids:
        List of campaign IDs to retrieve campaign content for within the
        account
    - output_list:
        List to add processed API records to from the output of the function
    """

    api_key = api_key_dict["api_key"]
    server = api_key_dict["server"]

    # Create Mailchimp client
    mailchimp_client = Client()
    mailchimp_client.set_config({"api_key": api_key, "server": server})

    # Create a limit on concurrent calls; each account is allowed 10 connections
    semaphore = asyncio.Semaphore(10)

    tasks = [
        asyncio.create_task(
            campaign_content(
                campaign_id,
                mailchimp_client,
                output_list,
                semaphore,
            )
        )
        for campaign_id in campaign_ids
    ]

    await asyncio.wait(tasks, return_when="FIRST_EXCEPTION")


async def all_account_campaign_content(
    api_key_dicts,
    campaigns_object,
    output_list,
):

    """
    Wrapper for executing async API calls for all available Mailchimp
    accounts
    """

    futures = [
        account_campaign_content(
            [i for i in api_key_dicts if i["account_name"] == account_name][0],
            campaign_ids,
            output_list,
        )
        for account_name, campaign_ids in campaigns_object.items()
    ]

    await asyncio.gather(*futures)


# Collect arguments
dev_override = sys.argv[1]
api_key_override = sys.argv[2]
server_override = sys.argv[3]
is_test = sys.argv[4]
is_excluded_step = "campaign_content" in sys.argv[5]

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
                where id is not null and account_name is not null
            """
        )

        for (account_name, id) in cur:

            campaigns_object.get(account_name).append(id)

    except Exception as e:

        logger.error(e)

        raise

    finally:

        cur.close()

    # For test syncs limit the number of records for each account to 5
    if is_test.lower() == "true":

        for account, campaigns in campaigns_object.items():

            campaigns_object[account] = campaigns[:5]

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
            all_account_campaign_content(
                api_key_dicts, campaigns_object_slice, output_list
            )
        )
        logger.info(f"Sending API data to Snowflake for iteration {iteration}")

        if len(output_list) > 0:

            to_snowflake(database, output_list, snowflake_creds, "campaign_content")

        else:

            logger.info(f"No output for iteration {iteration}")

        iteration += 1

    logger.info("Mailchimp campaign content sync complete")
