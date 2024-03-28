from helper_functions import get_secret, get_from_ids_for_all_accounts, to_snowflake
from res.utils import logger
import snowflake.connector
import os
import sys
import asyncio


def get_segment_members(mailchimp_client, segment_dict, query_parameters):

    list_id = segment_dict["list_id"]
    segment_id = segment_dict["segment_id"]

    return mailchimp_client.lists.get_segment_members_list(
        list_id, segment_id, **query_parameters
    )


# Collect arguments
dev_override = sys.argv[1]
api_key_override = sys.argv[2]
server_override = sys.argv[3]
is_test = sys.argv[4]
is_excluded_step = "segment_members" in sys.argv[5]

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
    segment_members_object = {
        key_dict["account_name"]: [] for key_dict in api_key_dicts
    }

    # Get all list segments
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
                    list_id,
                    id 
                from {database}.mailchimp.list_segments 
                where id is not null 
                    and list_id is not null
                    and account_name is not null
            """
        )

        for (account_name, list_id, id) in cur:

            segment_dict = {"list_id": list_id, "segment_id": id}
            segment_members_object.get(account_name).append(segment_dict)

    except Exception as e:

        logger.error(e)

        raise

    finally:

        cur.close()

    # For test syncs limit the number of records for each account to 2
    if is_test.lower() == "true":

        for account, segments in segment_members_object.items():

            segment_members_object[account] = segments[:2]

    # Set up logger string which says how many campaigns are in each account
    sync_str = "test sync" if is_test == "true" else "full sync"
    record_totals_str = "; ".join(
        f"{account}: {len(segments)}"
        for account, segments in segment_members_object.items()
    )
    logger.info(
        f"Beginning {sync_str} for {len(segment_members_object)} accounts. Number of segments for each account: {record_totals_str}"
    )

    # Iterate through 10-segment sized slices for each account
    iteration = 1

    while sum(len(v) for k, v in segment_members_object.items()) > 0:

        output_list = []
        segments_object_slice = {}

        for account, ids in segment_members_object.items():

            if len(ids) > 0:

                segments_object_slice[account] = ids[:10]

                del ids[:10]

        logger.info(f"Retrieving API data for iteration {iteration}")
        asyncio.run(
            get_from_ids_for_all_accounts(
                api_key_dicts, get_segment_members, segments_object_slice, output_list
            )
        )

        if len(output_list) > 0:

            logger.info(f"Sending API data to Snowflake for iteration {iteration}")
            to_snowflake(database, output_list, snowflake_creds, "segment_members")

        iteration += 1

    logger.info("Mailchimp segment members sync complete")
