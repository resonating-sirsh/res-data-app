from helper_functions import get_data, get_secret, to_snowflake, get_all_data
from res.utils import logger
import asyncio
import gc
import os
import sys


def get_lists(mailchimp_client, query_parameters):

    return mailchimp_client.lists.get_all_lists(**query_parameters)


async def get_account_list_members(
    api_key_dict,
    is_test,
    lists_object,
    output_list,
):

    """
    Additional function for retrieving data about the members of Mailchimp
    lists. Each list's members must be retrieved via its own API call; there
    is no single call in the sdk to retrieve all of the members of all of
    the lists on an account.
    """

    # Loop through each list for each account. Each list will have its own
    # function that is passed to the helper functions. This is done
    # synchronously and not asynchronously. This is because each account has a
    # maximum number of 10 connections and teh function uses those connections
    # to maximize the retrieval speed of each list.
    lists = lists_object.get(api_key_dict["account_name"])

    for list_id in lists:

        def get_list_members(mailchimp_client, query_parameters):

            return mailchimp_client.lists.get_list_members_info(
                list_id, **query_parameters
            )

        # Run a single instance of the async function
        await get_data(
            api_function=get_list_members,
            api_key_dict=api_key_dict,
            is_test=is_test,
            output_list=output_list,
        )


async def get_all_account_list_members(
    api_key_dicts,
    is_test,
    lists_object,
    output_list,
):

    """
    Wrapper to retrieve members of account lists for all accounts
    asynchronously. This is possible and practical because the limit on
    concurrent connections (10) is applied on a per-account basis. Retrieval
    of an individual account's lists must be done synchronously however to
    stay mindful of that limit.
    """

    futures = [
        get_account_list_members(
            api_key_dict=api_key_dict,
            is_test=is_test,
            lists_object=lists_object,
            output_list=output_list,
        )
        for api_key_dict in api_key_dicts
    ]

    await asyncio.gather(*futures)


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[1]

# A parameter of steps to skip can be passed. Used to run or test specific steps
is_excluded_step = "lists" in sys.argv[5]

if (
    __name__ == "__main__"
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
    and not is_excluded_step
):

    """
    In order to account for a possible future in which we need to run
    this application for existing / old Mailchimp accounts of newly
    on-boarded brands, this application both allows for input overrides to
    the standard API key and account prefix.
    """

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

    # Sync and retrieve lists for all accounts
    asyncio.run(
        get_all_data(
            api_function=get_lists,
            api_key_dicts=api_key_dicts,
            is_test=is_test,
            output_list=output_list,
        )
    )

    logger.info(f"Beginning Snowflake upsert for Mailchimp list data")

    to_snowflake(database, output_list, snowflake_creds, "lists")

    logger.info("Mailchimp list sync complete")

    # Sync and retrieve all account's list members. Retrieve target lists from
    # output_list and reset output_list
    lists_object = {key_dict["account_name"]: [] for key_dict in api_key_dicts}

    for account in lists_object:

        lists_object[account] = [
            i["id"] for i in output_list if i["account_name"] == account
        ]

    # Reset output list; clean up
    del output_list
    gc.collect()
    output_list = []

    # Retrieve list membership data
    asyncio.run(
        get_all_account_list_members(
            api_key_dicts=api_key_dicts,
            is_test=is_test,
            lists_object=lists_object,
            output_list=output_list,
        )
    )

    logger.info(f"Beginning Snowflake upsert for Mailchimp list membership data")

    to_snowflake(database, output_list, snowflake_creds, "list_members")

    logger.info("Mailchimp list membership sync complete")
