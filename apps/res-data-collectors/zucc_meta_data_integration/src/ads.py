from datetime import date, timedelta
from facebook_business.adobjects.adaccount import AdAccount
from res.utils import logger, secrets_client
from helper_functions import (
    get_all_data,
    get_latest_snowflake_timestamp,
    to_snowflake,
)
import os
import sys
import time
import asyncio


def api_call_function(id, api, fields, params):

    return AdAccount(fbid=id, api=api).get_ads(
        fields=list(fields),
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
TABLE = "ads"

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

    # ETL setup
    output_list = []

    fields = {
        "id",
        "account_id",
        "adset_id",
        "bid_amount",
        "campaign_id",
        "conversion_domain",
        "created_time",
        "creative",
        "effective_status",
        "name",
        "recommendations",
        "source_ad_id",
        "status",
        "updated_time",
    }

    params = {"limit": 500}

    if SYNC_TYPE == "incremental":

        # Retrieve last sync time
        since_ts = get_latest_snowflake_timestamp(
            database=DATABASE,
            schema=SCHEMA,
            table=TABLE,
            timestamp_field="last_synced_at",
            is_date=False,
            is_string=False,
        )

        if since_ts is None:

            logger.warning(
                "Error during latest date retrieval; defaulting to Meta full sync for ads"
            )

            # Only retrieve ads updated since July 2017. It's unlikely ads not
            # updated since then matter for Resonance data purposes
            params["updated_since"] = int(time.mktime(date(2017, 7, 1).timetuple()))

        else:

            params["updated_since"] = int(time.mktime(since_ts.timetuple()))

    else:

        if SYNC_TYPE == "full":

            # Only retrieve ads updated since July 2017. It's unlikely ads not
            # updated since then matter for Resonance data purposes
            params["updated_since"] = int(time.mktime(date(2017, 7, 1).timetuple()))

        elif SYNC_TYPE == "test":

            # Test sync using updated_since the last 10 days
            params["updated_since"] = int(
                time.mktime((date.today() - timedelta(days=10)).timetuple())
            )

        else:

            logger.warning(
                "No or invalid sync type provided; defaulting to Meta full sync for ads"
            )

            params["updated_since"] = int(time.mktime(date(2017, 7, 1).timetuple()))

    # Execute async API calls
    logger.info(
        f"Meta {SYNC_TYPE} sync for ads{single_account_str if single_account_str else ''}"
    )

    asyncio.run(
        get_all_data(
            ad_account_cred_dicts=cred_dicts,
            api_call_function=api_call_function,
            app_id=app_id,
            app_secret=app_secret,
            endpoint_value_type="multiple",
            fields=fields,
            output_list=output_list,
            params=params,
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

    logger.info("Meta ads sync complete")
