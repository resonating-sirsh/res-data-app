from datetime import datetime, date, timedelta
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.api import FacebookAdsApi, FacebookSession, FacebookRequestError
from helper_functions import (
    to_snowflake,
    get_latest_snowflake_timestamp,
)
from res.utils import logger, secrets_client
import json
import os
import sys
import asyncio


async def get_insights(
    ad_account_cred_dict,
    app_id,
    app_secret,
    fields,
    output_list,
    params,
):

    id = "act_" + ad_account_cred_dict["ad_account_id"]
    access_token = ad_account_cred_dict["access_token"]
    account_name = ad_account_cred_dict["account_name"]

    try:

        # Working with multiple APIs and users requires creating multiple sessions
        # instead of one default one
        session = FacebookSession(
            app_id=app_id, app_secret=app_secret, access_token=access_token
        )

        api = FacebookAdsApi(session)

        logger.info(f"FacebookSession created for {account_name}")

    except Exception as e:

        logger.info(f"FacebookSession creation failed for {account_name}: {e}")

        raise

    try:

        # Create an async query job and poll every 15 seconds for status
        async_job = AdAccount(fbid=id, api=api).get_insights(
            fields=fields, params=params, is_async=True
        )

    # If the request encounters a rate limit, pause execution of the coroutine.
    # Coroutines are broken out by Ad Account and each have their own System
    # User access token with its own individual rate limit. The RequestError for
    # rate-limit errors (response code 400) contains an estimate (in minutes) of
    # how much time needs to pass before requests will no longer be throttled
    except FacebookRequestError as e:

        # Handle error 400 (rate limit) by sleeping for time in header
        if e.http_status() == 400:

            usage_dict = json.loads(dict(e.http_headers())["X-Business-Use-Case"])
            wait_time = max(
                300,
                30
                + (
                    60
                    * int(
                        usage_dict[next(iter(usage_dict))][0][
                            "estimated_time_to_regain_access"
                        ]
                    )
                ),
            )

            # Rate info is the value of a dict using the account ID as the key
            logger.info(
                f"Rate limit reached for {account_name}. Sleeping for {wait_time} seconds"
            )

            await asyncio.sleep(wait_time)

            try:

                async_job = AdAccount(fbid=id, api=api).get_insights(
                    fields=fields, params=params, is_async=True
                )

            except FacebookRequestError as e:

                # If it's still erroring out after failing once raise the Exception
                logger.error(dict(e.body())["error"]["message"])

                raise

        else:

            # Otherwise raise the exception for other request status
            logger.error(dict(e.body())["error"]["message"])

            raise

    except Exception as e:

        logger.error(f"Error creating async job for {account_name}; {e}")

        raise

    # Use a while loop to periodically poll the job for status
    while True:

        try:

            job = async_job.api_get()

        except Exception as e:

            logger.error(f"Error polling async job status for {account_name}; {e}")

            raise

        # Job status fields are located within the AdReportRun class returned by
        # the async job get_insights method of an AdAccount
        percent_done = str(job[AdReportRun.Field.async_percent_completion])
        job_status = str(job[AdReportRun.Field.async_status])

        if job_status == "Job Completed":

            logger.info(f"{account_name} {job_status}; {percent_done}% Complete")

            break

        elif job_status == "Job Failed":

            logger.error(f"{account_name} {job_status}")

            raise Exception(f"{job_status}")

        else:

            logger.info(f"{account_name} {job_status}; {percent_done}% Complete")

            await asyncio.sleep(15)

            continue

    try:

        data = list(async_job.get_result())

    except FacebookRequestError as e:

        # Handle error 400 (rate limit) by sleeping for time in header
        if e.http_status() == 400:

            usage_dict = json.loads(dict(e.http_headers())["X-Business-Use-Case"])
            wait_time = max(
                300,
                30
                + (
                    60
                    * int(
                        usage_dict[next(iter(usage_dict))][0][
                            "estimated_time_to_regain_access"
                        ]
                    )
                ),
            )

            # Rate info is the value of a dict using the account ID as the key
            logger.info(
                f"Rate limit reached for {account_name}. Sleeping for {wait_time} seconds"
            )

            await asyncio.sleep(wait_time)

            try:

                data = list(async_job.get_result())

            except FacebookRequestError as e:

                # If it's still erroring out after failing once raise the Exception
                logger.error(dict(e.body())["error"]["message"])

                raise

        else:

            # Otherwise raise the exception for other request status
            logger.error(dict(e.body())["error"]["message"])

            raise

    except Exception as e:

        logger.error(f"Error creating async job for {account_name}; {e}")

        raise

    for object in data:

        # Fields that return dicts or lists need to have their values
        # coerced into strings to be properly added to Snowflake
        dict_object = dict(object)

        for key in dict_object:

            if type(dict_object[key]) is dict or type(dict_object[key]) is list:

                str_obj = str(dict_object[key])

                dict_object[key] = str_obj

        output_list.append(dict_object)

    logger.info(f"API data retrieved for {account_name}")


async def get_all_insights(
    ad_account_cred_dicts,
    app_id,
    app_secret,
    fields,
    output_list,
    params,
):

    futures = [
        get_insights(
            ad_account_cred_dict=ad_account_cred_dict,
            app_id=app_id,
            app_secret=app_secret,
            fields=fields,
            output_list=output_list,
            params=params,
        )
        for ad_account_cred_dict in ad_account_cred_dicts
    ]

    await asyncio.gather(*futures)


# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
SYNC_TYPE = (sys.argv[1] or "").lower()
DEV_OVERRIDE = (sys.argv[2] or "").lower() == "true"
SINGLE_ACCOUNT_ID = sys.argv[3] or ""
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[4] or sys.argv[4].lower() == "all"

# Constants
DATABASE = "raw" if RES_ENV.lower() == "production" else "raw_dev"
SCHEMA = "zucc_meta"

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

    # This is constructed as dicts within a dict so that if we ever want to add
    # additional reports to this script it'll be just a matter of adding their
    # info here
    all_report_fields = {
        "ad_insights": [
            "account_id",
            "account_name",
            "ad_id",
            "adset_id",
            "adset_name",
            "campaign_id",
            "campaign_name",
            "clicks",
            "date_start",
            "date_stop",
            "impressions",
            "outbound_clicks",
            "reach",
            "spend",
        ]
    }

    params = {
        "action_attribution_windows": ["7d_click", "1d_view"],
        "action_report_time": "conversion",
        "breakdowns": ["device_platform"],
        "level": "ad",
        "time_increment": 1,
    }

    for target_table in all_report_fields:

        fields = all_report_fields[target_table]

        if SYNC_TYPE == "incremental":

            try:
                # Retrieve Snowflake date before async invocation
                since_date_str = get_latest_snowflake_timestamp(
                    database=DATABASE,
                    schema=SCHEMA,
                    table=target_table,
                    timestamp_field="date_start",
                    is_date=True,
                    is_string=True,
                )

                # Data must be from no more than 36 months ago and the date must
                # be valid
                if since_date_str is None:

                    raise Exception

            except:

                logger.warning(
                    f"Error during timestamp retrieval; defaulting to full sync for {target_table}"
                )

                # Resonance was founded in July 2017. The API returns at most the
                # last 37 months of data
                since_date_str = "2017-07-01"

        elif SYNC_TYPE == "test":

            # For test syncs retrieve the last 15 days of data
            since_date_str = (date.today() - timedelta(days=15)).strftime("%Y-%m-%d")

        else:

            # Resonance was founded in July 2017. The API returns at most the
            # last 37 months of data
            since_date_str = "2017-07-01"

            if SYNC_TYPE != "full":

                logger.warning(
                    f"No sync type provided; defaulting to Meta full sync for insights"
                )

        start_date = datetime.strptime(since_date_str, "%Y-%m-%d").date()
        logger.info(
            f"Meta {SYNC_TYPE} sync for {target_table}{single_account_str if single_account_str else ''}"
        )

        while start_date < date.today():

            # Queries cannot be for data older than 37 months. Facebook defines
            # this window by a month offset, not actual days. This also needs to
            # account for leap days and the offset from those

            # Set the start date equal to the greater of the start date and 3
            # years (36 months) ago. For leap days (Feb 29) treat the compared
            # start date as March 1st. This ensures that date is always captured
            # within the request time range
            if start_date.month == 2 and start_date.day == 29:

                start_date = max(
                    datetime(year=date.today().year - 3, month=3, day=1).date(),
                    datetime(year=start_date.year, month=start_date.month, day=28).date(),
                )

            else:

                start_date = max(
                    datetime(
                        year=date.today().year - 3,
                        month=date.today().month,
                        day=date.today().day,
                    ).date(),
                    start_date,
                )

            # Increment by 15 days or max at today, whichever is smaller
            next_date = min((start_date + timedelta(days=15)), date.today())
            params.update(
                {"time_range": {"since": str(start_date), "until": str(next_date)}}
            )
            logger.info(f"Syncing data from {start_date} to {next_date}")

            # Execute async API calls. This API call retrieves one record per account
            # and is always a full sync
            asyncio.run(
                get_all_insights(
                    ad_account_cred_dicts=cred_dicts,
                    app_id=app_id,
                    app_secret=app_secret,
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
                    table=target_table,
                    add_primary_key_and_ts=True,
                )
                logger.info(
                    f"Synced {target_table} data from {str(start_date)} to {str(next_date)}"
                )

            else:

                logger.warning("No output from API call")

            # Reset output and increment start date
            output_list = []
            start_date = next_date

        logger.info(f"Sync for {target_table} complete")

    logger.info("Meta insights sync complete")
