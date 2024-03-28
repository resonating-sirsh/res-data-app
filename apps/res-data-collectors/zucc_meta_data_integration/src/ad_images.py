from facebook_business.adobjects.adaccount import AdAccount
from res.utils import logger, secrets_client
from helper_functions import (
    to_snowflake,
    get_all_data,
)
import os
import sys
import asyncio


def api_call_function(id, api, field_info, params):

    return AdAccount(fbid=id, api=api).get_ad_images(
        fields=list(field_info),
        params=params,
    )


# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
SYNC_TYPE = (sys.argv[1] or "").lower()
DEV_OVERRIDE = (sys.argv[2] or "").lower() == "true"
SINGLE_ACCOUNT_ID = sys.argv[3] or ""
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[4] or sys.argv[4].lower() == "all"

# Constants
DATABASE = (
    "raw" if (RES_ENV.lower() == "production" and SYNC_TYPE != "test") else "raw_dev"
)
SCHEMA = "zucc_meta"
TABLE = "ad_accounts"

if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):

    # Retrieve Meta ad manager credentials for each ad account
    meta_secret = secrets_client.get_secret("META-DATA-TEAM-API-ACCESS-APP")
    cred_dicts = meta_secret.get("system_user_account_tokens")
    app_id = meta_secret.get("app_id")
    app_secret = meta_secret.get("app_secret")

    # If the app is directed to sync only for a single ad account it will remove
    # non-matching accounts from the dictionary of account access keys
    single_account_str = None

    if SYNC_TYPE == "single_account" and SINGLE_ACCOUNT_ID != "none":

        cred_dicts = [
            next(
                (
                    account
                    for account in cred_dicts
                    if account["ad_account_id"] == SINGLE_ACCOUNT_ID
                )
            )
        ]
        single_account_name = cred_dicts[0]["account_name"]
        single_account_str = f"; single account = {single_account_name}"

    # Retrieve Snowflake credentials from secrets manager
    snowflake_creds = secrets_client.get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

    # ETL setup
    output_list = []

    fields = {
        "id",
        "account_id",
        "created_time",
        "creatives",
        "hash",
        "height",
        "is_associated_creatives_in_adgroups",
        "name",
        "original_height",
        "original_width",
        "permalink_url",
        "status",
        "updated_time",
        "width",
    }

    # This endpoint has no parameters. So all syncs are full syncs for a given
    # account. Indicate this for provided sync types besides test (such as
    # incremental, which is the default)
    logger.info(
        f"Meta full sync for ad images{single_account_str if single_account_str else ''}"
    )

    # Execute async API calls. This API call retrieves one record per account
    # and is always a full sync
    asyncio.run(
        get_all_data(
            ad_account_cred_dicts=cred_dicts,
            api_call_function=api_call_function,
            app_id=app_id,
            app_secret=app_secret,
            endpoint_value_type="multiple",
            fields=fields,
            output_list=output_list,
            params={"limit": 100},
        )
    )

    if len(output_list) > 0:

        to_snowflake(
            data=output_list,
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE,
            add_primary_key_and_ts=True,
        )

    else:

        logger.warning("No output from API call")

    logger.info("Meta ad images sync complete")
