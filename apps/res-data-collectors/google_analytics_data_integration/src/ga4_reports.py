from datetime import datetime, timedelta
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.oauth2 import service_account
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    BatchRunReportsRequest,
)
from sqlalchemy.types import *
from res.utils import logger
from helper_functions import (
    camel_to_snake,
    get_latest_snowflake_timestamp,
    get_secret,
    to_snowflake,
)
import gc
import hashlib
import os
import sys
import asyncio


async def get_reports(
    google_credentials,
    property,
    start_date_str: str,
    end_date_str: str,
    response_output_list: list,
):

    """
    Queries the Google Analytics Data API v1.

    Args:

        google_credentials: Oath google credentials object used to authenticate
        query POST requests

        property_name: Dict containing information about the property to be
        queried and its account label

        start_date_str: Start date for the report queries

        end_date_str: End date for the report queries

        response_output_list: list to append query responses to

    Output:

        Appends the Data API v1 response to the supplied response
        output list.
    """

    client = BetaAnalyticsDataClient(credentials=google_credentials)

    property_name = property["name"]
    account_label = property["account_label"]

    # Create report requests objects to be used in batch request
    channel_performance = RunReportRequest(
        date_ranges=[
            DateRange(start_date=f"{start_date_str}", end_date=f"{end_date_str}")
        ],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="sessionCampaignId"),
            Dimension(name="sessionCampaignName"),
            Dimension(name="sessionManualAdContent"),
            Dimension(name="sessionManualTerm"),
            Dimension(name="sessionMedium"),
            Dimension(name="sessionSource"),
        ],
        metrics=[
            Metric(name="averageSessionDuration"),
            Metric(name="engagedSessions"),
            Metric(name="newUsers"),
            Metric(name="screenPageViews"),
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="userEngagementDuration"),
        ],
        limit="100000",
    )

    page_traffic = RunReportRequest(
        date_ranges=[
            DateRange(start_date=f"{start_date_str}", end_date=f"{end_date_str}")
        ],
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pageTitle"),
        ],
        metrics=[
            Metric(name="averageSessionDuration"),
            Metric(name="engagedSessions"),
            Metric(name="newUsers"),
            Metric(name="screenPageViews"),
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="userEngagementDuration"),
            Metric(name="scrolledUsers"),
        ],
        limit="100000",
    )

    platform_traffic = RunReportRequest(
        date_ranges=[
            DateRange(start_date=f"{start_date_str}", end_date=f"{end_date_str}")
        ],
        dimensions=[
            Dimension(name="browser"),
            Dimension(name="date"),
            Dimension(name="deviceCategory"),
            Dimension(name="operatingSystem"),
        ],
        metrics=[
            Metric(name="averageSessionDuration"),
            Metric(name="engagedSessions"),
            Metric(name="newUsers"),
            Metric(name="screenPageViews"),
            Metric(name="sessions"),
            Metric(name="totalUsers"),
            Metric(name="userEngagementDuration"),
        ],
        limit="100000",
    )

    try:

        requests = BatchRunReportsRequest(
            property=f"{property_name}",
            requests=[
                channel_performance,
                page_traffic,
                platform_traffic,
            ],
        )

        response = client.batch_run_reports(requests)

    except Exception as e:

        logger.error(
            f"""
                An error occurred while retrieving reports for {account_label}: {e}
            """
        )

        raise

    response_dict = {"account_label": account_label, "response": response}

    response_output_list.append(response_dict)


async def get_all_reports(
    google_credentials,
    properties,
    start_date_str: str,
    end_date_str: str,
    response_output_list: list,
):

    futures = [
        get_reports(
            google_credentials,
            property,
            start_date_str,
            end_date_str,
            response_output_list,
        )
        for property in properties
    ]

    await asyncio.gather(*futures)


def parse_responses(response_list: list, sync_time: str):

    """
    Parses the Analytics Reporting API V4 response and separates it into a list
    of three flat lists of dicts.

    Args:

        response_list: An Analytics Reporting API V4 response
        sync_time: String timestamp

    Returns:

        output: a dict of list of dicts for platform traffic, channel
            performance, and page traffic
    """

    # Create output object
    output = {"channel_performance": [], "page_traffic": [], "platform_traffic": []}

    for response_dict in response_list:

        response = response_dict["response"]
        account_label = response_dict["account_label"]

        for report in response.reports:

            for row in report.rows:

                row_dict = {"account_label": account_label}

                for i, dimension in enumerate(row.dimension_values):

                    dimension_name_raw = report.dimension_headers[i].name
                    dimension_name = camel_to_snake(dimension_name_raw)
                    dimension_value = dimension.value

                    # Check if dimension value includes the "other" (overflow) row and throw an error
                    # if it does
                    if dimension_value == "(other)":

                        logger.error(
                            """
                                Report request cardinality exceeds system limits; 
                                (other) row is present
                            """
                        )

                        raise Exception(
                            """
                                Report request cardinality exceeds system limits; 
                                (other) row is present
                            """
                        )

                    if dimension_name == "date":

                        dimension_value = (
                            dimension_value[:4]
                            + "-"
                            + dimension_value[4:6]
                            + "-"
                            + dimension_value[6:]
                        )

                    row_dict[f"{dimension_name}"] = dimension_value

                for i, metric in enumerate(row.metric_values):
                    metric_name_raw = report.metric_headers[i].name
                    metric_name = camel_to_snake(metric_name_raw)
                    row_dict[f"{metric_name}"] = float(metric.value)

                record_field_hash = hashlib.md5(
                    str(row_dict.values()).encode("utf-8")
                ).hexdigest()
                row_dict["primary_key"] = record_field_hash

                # Add sync time. This isn't part of the primary key because that
                # can create multiple records for observations of the same
                # record state
                row_dict["last_synced_at"] = sync_time

                # Check which report the row belongs to. This is done by
                # checking whether a dimension that is only present in one of
                # the reports exists in the row

                if row_dict.get("page_title") is not None:

                    output["page_traffic"].append(row_dict)

                elif row_dict.get("device_category") is not None:

                    output["platform_traffic"].append(row_dict)

                elif row_dict.get("session_source") is not None:

                    output["channel_performance"].append(row_dict)

    return output


# Parse environment variables and set constants. App will only run in
# production; running in dev environments requires an override
RES_ENV = os.getenv("RES_ENV", "development").lower()
SYNC_TYPE = (sys.argv[1] or "").lower()
IS_DEV_OVERRIDE = (sys.argv[2] or "").lower() == "true"
IS_TEST = SYNC_TYPE == "test"
SINGLE_PROPERTY_ID = (
    None if (sys.argv[3] == "none" or sys.argv[3] == "") else sys.argv[3]
)
DATABASE = "raw" if RES_ENV.lower() == "production" and not IS_TEST else "raw_dev"
CURRENT_DATE = datetime.today().date()

if __name__ == "__main__" and (RES_ENV == "production" or IS_DEV_OVERRIDE):

    # Retrieve credentials and GA4 properties
    google_credentials = service_account.Credentials.from_service_account_info(
        get_secret("DATA_TEAM_GOOGLE_SERVICE_KEY")
    )
    snowflake_creds = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    properties = get_secret("GOOGLE_GA4_PROPERTIES")

    # If a single property is provided sync only for that property
    if SINGLE_PROPERTY_ID is not None:

        properties[:] = [
            item
            for item in properties
            if item.get("name", "").__contains__(f"{SINGLE_PROPERTY_ID}")
        ]

        if len(properties) == 0:

            logger.error("Provided property ID does not match any target property ID")

            raise Exception(
                "Provided property ID does not match any target property ID"
            )

        logger.info(f"Sync for single GA4 property {SINGLE_PROPERTY_ID}")

    # Other syncs (or undeclared sync types) will sync for all target properties
    if SYNC_TYPE == "full":

        logger.info("Full sync for Google Analytics - GA4")

        # GA4 properties do not backfill. Therefore no data will exist
        # before the service was introduced in October 2020. Additionally,
        # GA4 reports do not sample -- instead
        # they bucket data after 100K rows into an "(other)" row. This
        # allows for larger time buckets compared to UA.
        start_date = datetime.strptime("2020-10-01", "%Y-%m-%d").date()

    elif SYNC_TYPE == "test":

        # Sync for the past 5 days only
        logger.info("Test sync for Google Analytics - GA4")
        start_date = datetime.today().date() - timedelta(days=5)

    else:

        # All other syncs are either explicitly incremental or coerced to
        # incremental
        if SYNC_TYPE == "incremental":

            logger.info("Incremental sync for Google Analytics - GA4")

        else:

            logger.info(
                """
                    Empty or invalid sync type; Defaulting to incremental 
                    sync for Google Analytics - GA4
                """
            )

        # Retrieve Snowflake date
        start_date_str = get_latest_snowflake_timestamp(
            schema="google_analytics",
            table="ga4_channel_performance",
            snowflake_user=snowflake_creds["user"],
            snowflake_password=snowflake_creds["password"],
            snowflake_account=snowflake_creds["account"],
            timestamp_field="date",
            is_date=True,
        )

        if start_date_str is None:

            logger.error("Error retrieving latest date")

            raise Exception("Error retrieving latest date")

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

    # Set the end date arbitrarily in the past to begin the while loop
    end_date = CURRENT_DATE - timedelta(days=1)

    while end_date < CURRENT_DATE:

        # Set the end date to the lesser of 30 days after start date and
        # the current date
        end_date = min(CURRENT_DATE, start_date + timedelta(days=30))
        start_date_str = datetime.strftime(start_date, "%Y-%m-%d")
        end_date_str = datetime.strftime(end_date, "%Y-%m-%d")

        # Log sync bounds and store time
        logger.info(f"Syncing data from {start_date_str} to {end_date_str}")
        response_output_list = []
        sync_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")

        # Execute async API calls
        asyncio.run(
            get_all_reports(
                google_credentials,
                properties,
                start_date_str,
                end_date_str,
                response_output_list,
            )
        )

        if len(response_output_list) > 0:

            table_data = parse_responses(response_output_list, sync_time)

            # Clean up
            del response_output_list
            gc.collect()

            for table, data in table_data.items():

                if len(data) > 0:

                    logger.info(f"Syncing {table}")
                    table = "ga4_" + table
                    to_snowflake(
                        data, snowflake_creds, DATABASE, "google_analytics", table
                    )

        # Clean up (and free from memory); reset lists
        del table_data
        gc.collect()

        # Iterate. Date ranges in queries are inclusive.
        start_date = end_date + timedelta(days=1)

    logger.info("GA4 sync complete")
