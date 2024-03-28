from helper_functions import get_secret, to_snowflake
from res.utils import logger
import snowflake.connector
import os
import sys
import asyncio
import aiohttp
from datetime import datetime
import json
import hashlib


async def get_campaign_email_activity(
    aiohttp_session, api_key_dict, campaign_id, output_list, semaphore
):
    async with semaphore:

        # Get mailchimp account info from dict
        account_name = api_key_dict["account_name"]
        api_key = api_key_dict["api_key"]
        server = api_key_dict["server"]

        # Prepare request parameters
        url = (
            f"https://{server}.api.mailchimp.com/export/1.0/campaignSubscriberActivity/"
        )
        params = {"apikey": api_key, "id": campaign_id}
        data = []

        while True:

            async with aiohttp_session.get(
                url=url,
                params=params,
            ) as response:

                try:

                    # Avoid aiohttp raising ValueError('Line is too long')
                    response.content._high_water *= 128

                    async for line in response.content:

                        # Ignore any keep-alive lines
                        if line:

                            # Skip 1-long byte strings
                            if len(line) == 1:

                                continue

                            line_data = json.loads(line)

                            # Check line to see if it's an error message
                            if "error" in line_data:

                                if line_data["code"] == 429 or line_data["code"] == -50:

                                    logger.warning(
                                        "Throttled or max connections; napping for a minute"
                                    )

                                    await asyncio.sleep(60)

                                    continue

                                else:

                                    raise Exception(
                                        f"Code {line_data['code']}: {line_data['error']}"
                                    )

                            else:

                                data.append(line_data)

                except Exception as e:

                    logger.error(e)

                    raise

            break

        # Add account info and primary key to data
        for record in data:

            # The response is a dict with a key of the subscriber email address
            # and a value of their activity. Adjust the keys to ensure
            # consistency across records
            email_address = next(iter(record))
            record["activity"] = record.pop(email_address)
            record["campaign_id"] = campaign_id
            record["email_address"] = email_address
            record["email_id"] = hashlib.md5(
                email_address.lower().encode("utf-8")
            ).hexdigest()
            record["account_name"] = account_name

            # Create a unique key using the values of the recorded state
            record_field_hash = hashlib.md5(
                str(record.values()).encode("utf-8")
            ).hexdigest()
            record["primary_key"] = record_field_hash

            # Add sync time. This isn't included in the primary key because that would
            # cause a record update every sync even if data has not changed
            record["synced_at"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f %Z")

        output_list.extend(data)


async def get_account_email_activity(campaign_ids, api_key_dict, output_list):

    timeout = aiohttp.ClientTimeout(total=None)

    # Create a limit on concurrent calls; each account is allowed 10 connections
    semaphore = asyncio.Semaphore(10)

    aiohttp_session = aiohttp.ClientSession(
        trust_env=(os.environ.get("AM_I_IN_A_DOCKER_CONTAINER") == "true"),
        timeout=timeout,
    )

    tasks = [
        asyncio.create_task(
            get_campaign_email_activity(
                aiohttp_session,
                api_key_dict,
                campaign_id,
                output_list,
                semaphore,
            )
        )
        for campaign_id in campaign_ids
    ]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    for task in pending:

        task.cancel()

    for task in done:

        e = task.exception()

        if isinstance(e, Exception):

            logger.error(e)
            raise e

    aiohttp_session.close()


async def get_all_email_activity(
    campaigns_object_slice,
    api_key_dicts,
    output_list,
):

    """
    Wrapper for executing async API calls for all available Mailchimp
    accounts
    """

    futures = [
        get_account_email_activity(
            campaign_ids,
            [i for i in api_key_dicts if i["account_name"] == account_name][0],
            output_list,
        )
        for account_name, campaign_ids in campaigns_object_slice.items()
    ]

    await asyncio.gather(*futures)


# Collect arguments
dev_override = sys.argv[1]
api_key_override = sys.argv[2]
server_override = sys.argv[3]
is_test = sys.argv[4]
is_excluded_step = "subscriber_activity" in sys.argv[5]

if (
    __name__ == "__main__"
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
    and not is_excluded_step
):

    # Set target database according to environment
    if os.getenv("RES_ENV") == "production" and is_test.lower() != "true":

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

    # Get campaigns with that have sent at least one email (subscriber activity for
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

    # Iterate through 100-campaign sized slices for each account
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
            get_all_email_activity(
                campaigns_object_slice,
                api_key_dicts,
                output_list,
            )
        )

        if len(output_list) > 0:

            logger.info(f"Sending API data to Snowflake for iteration {iteration}")
            to_snowflake(database, output_list, snowflake_creds, "subscriber_activity")

        iteration += 1

    logger.info("Mailchimp campaign subscriber activity sync complete")
