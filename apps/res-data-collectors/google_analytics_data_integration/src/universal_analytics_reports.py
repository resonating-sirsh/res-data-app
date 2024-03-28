from datetime import datetime, timedelta
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
from helper_functions import (
    initialize_analytics_reporting,
    get_secret,
    get_latest_snowflake_timestamp,
    camel_to_snake,
    dict_to_sql,
)
from res.utils import logger
import gc
import hashlib
import os
import sys
import asyncio
import snowflake.connector


async def get_report(
    analytics_service_object,
    view_item,
    start_date_str: str,
    end_date_str: str,
    response_output_list: list,
):

    """
    Queries the Analytics Reporting API V4.

    Args:

        analytics_service_object: An authorized Analytics Reporting API V4
        service object. Used to authorize API requests

        view_item: Dict containing information about the view to be queried,
        it's account label, and its website

        start_date_str: Start date for the report queries

        end_date_str: End date for the report queries

        response_output_list: list to append query responses to

    Output:

        Appends the Analytics Reporting API V4 response to the supplied response
        output list.
    """

    view_id = view_item["id"]
    website_url = view_item["websiteUrl"]
    account_label = view_item["account_label"]

    try:

        response = (
            analytics_service_object.reports()
            .batchGet(
                body={
                    "reportRequests": [
                        # Channel Performance
                        {
                            "viewId": f"{view_id}",
                            "dateRanges": [
                                {
                                    "startDate": f"{start_date_str}",
                                    "endDate": f"{end_date_str}",
                                }
                            ],
                            "metrics": [
                                {"expression": "ga:avgSessionDuration"},
                                {"expression": "ga:bounceRate"},
                                {"expression": "ga:bounces"},
                                {"expression": "ga:organicSearches"},
                                {"expression": "ga:sessions"},
                                {"expression": "ga:pageviewsPerSession"},
                                {"expression": "ga:percentNewSessions"},
                            ],
                            "dimensions": [
                                {"name": "ga:campaign"},
                                {"name": "ga:date"},
                                {"name": "ga:fullReferrer"},
                                {"name": "ga:keyword"},
                                {"name": "ga:medium"},
                                {"name": "ga:adContent"},
                                {"name": "ga:source"},
                            ],
                            "samplingLevel": "LARGE",
                            "pageSize": 10000,
                        },
                        # Ecommerce Performance -- ad content is not allowed
                        # with transaction ID so landing page path is used instead
                        {
                            "viewId": f"{view_id}",
                            "dateRanges": [
                                {
                                    "startDate": f"{start_date_str}",
                                    "endDate": f"{end_date_str}",
                                }
                            ],
                            "metrics": [{"expression": "ga:transactions"}],
                            "dimensions": [
                                {"name": "ga:campaign"},
                                {"name": "ga:date"},
                                {"name": "ga:fullReferrer"},
                                {"name": "ga:keyword"},
                                {"name": "ga:medium"},
                                {"name": "ga:landingPagePath"},
                                {"name": "ga:source"},
                                {"name": "ga:transactionId"},
                            ],
                            "samplingLevel": "LARGE",
                            "pageSize": 10000,
                        },
                        # Page Traffic
                        {
                            "viewId": f"{view_id}",
                            "dateRanges": [
                                {
                                    "startDate": f"{start_date_str}",
                                    "endDate": f"{end_date_str}",
                                }
                            ],
                            "metrics": [
                                {"expression": "ga:avgSessionDuration"},
                                {"expression": "ga:avgTimeOnPage"},
                                {"expression": "ga:bounceRate"},
                                {"expression": "ga:bounces"},
                                {"expression": "ga:exitRate"},
                                {"expression": "ga:pageValue"},
                                {"expression": "ga:pageviews"},
                                {"expression": "ga:percentNewSessions"},
                                {"expression": "ga:uniquePageviews"},
                                {"expression": "ga:users"},
                            ],
                            "dimensions": [
                                {"name": "ga:date"},
                                {"name": "ga:pageTitle"},
                            ],
                            "samplingLevel": "LARGE",
                            "pageSize": 10000,
                        },
                        # Platform Traffic
                        {
                            "viewId": f"{view_id}",
                            "dateRanges": [
                                {
                                    "startDate": f"{start_date_str}",
                                    "endDate": f"{end_date_str}",
                                }
                            ],
                            "metrics": [
                                {"expression": "ga:avgSessionDuration"},
                                {"expression": "ga:bounceRate"},
                                {"expression": "ga:bounces"},
                                {"expression": "ga:newUsers"},
                                {"expression": "ga:pageviews"},
                                {"expression": "ga:pageviewsPerSession"},
                                {"expression": "ga:percentNewSessions"},
                                {"expression": "ga:sessions"},
                                {"expression": "ga:sessionsPerUser"},
                                {"expression": "ga:users"},
                            ],
                            "dimensions": [
                                {"name": "ga:date"},
                                {"name": "ga:operatingSystem"},
                                {"name": "ga:browser"},
                                {"name": "ga:deviceCategory"},
                            ],
                            "samplingLevel": "LARGE",
                            "pageSize": 10000,
                        },
                    ]
                }
            )
            .execute()
        )

    except Exception as e:

        logger.error(
            f"""
                An error occurred while retrieving reports for {account_label} {website_url}: {e}
            """
        )

        raise

    response["website_url"] = website_url
    response["account_label"] = account_label

    response_output_list.append(response)


async def get_all_reports(
    view_items,
    analytics_service_object,
    start_date_str: str,
    end_date_str: str,
    response_output_list: list,
):

    futures = [
        get_report(
            analytics_service_object,
            view_item,
            start_date_str,
            end_date_str,
            response_output_list,
        )
        for view_item in view_items
    ]

    await asyncio.gather(*futures)


def parse_responses(
    response_list,
    platform_traffic_output: list,
    channel_performance_output: list,
    page_traffic_output: list,
    ecommerce_performance_output: list,
):

    """
    Parses the Analytics Reporting API V4 response and separates it into three
    flat lists of dicts.

    Args:

        response: An Analytics Reporting API V4 response.

        platform_traffic_output: A list to append platform_traffic query
        response rows to

        channel_performance_output: A list to append channel_performance query
        response rows to

        page_traffic_output: A list to append page_traffic query response rows
        to

        ecommerce_performance_output: A list to append ecommerce_performance
        query response rows to
    """

    for response in response_list:

        website_url = response["website_url"]
        account_label = response["account_label"]

        for report in response.get("reports", []):

            # Google Analytics samples data if the amount of sessions in the request
            # time frame exceeds certain amounts (usually ~ 100,000). Sampled data
            # returns two key that indicate the relative size of the sample. If either
            # of these keys are present return an error regarding the time frame
            # settings of the app

            if report.get("data", {}).get("samplesReadCounts") or report.get(
                "data", {}
            ).get("samplingSpaceSizes"):

                logger.error("An exception occurred; response data is sampled")

                raise Exception("An exception occurred; response data is sampled")

            # Google Analytics allows for pagination. This app is constructed to use
            # time windows that, in theory, should never return more than 10,000 records
            # (the maximum) for a single report. Therefore pagination should never be
            # necessary. Check if a pagination token (next token) is present in the
            # response and raise an Exception if so noting the need for pagination in
            # the app

            if report.get("nextPageToken"):

                logger.error("An exception occurred; response requires pagination")

                raise Exception("An exception occurred; response requires pagination")

            # Determine which report is being looped through. The API does not
            # necessarily maintain the order from the request in the response object

            if report.get("columnHeader", {}).get("dimensions") == [
                "ga:date",
                "ga:operatingSystem",
                "ga:browser",
                "ga:deviceCategory",
            ]:

                report_name = "platform_traffic"

            elif report.get("columnHeader", {}).get("dimensions") == [
                "ga:campaign",
                "ga:date",
                "ga:fullReferrer",
                "ga:keyword",
                "ga:medium",
                "ga:adContent",
                "ga:source",
            ]:

                report_name = "channel_performance"

            elif report.get("columnHeader", {}).get("dimensions") == [
                "ga:date",
                "ga:pageTitle",
            ]:

                report_name = "page_traffic"

            elif report.get("columnHeader", {}).get("dimensions") == [
                "ga:campaign",
                "ga:date",
                "ga:fullReferrer",
                "ga:keyword",
                "ga:medium",
                "ga:landingPagePath",
                "ga:source",
                "ga:transactionId",
            ]:

                report_name = "ecommerce_performance"

            column_headers = report.get("columnHeader", {})
            dimension_names = column_headers.get("dimensions", [])
            metric_names = column_headers.get("metricHeader", {}).get(
                "metricHeaderEntries", []
            )

            for row in report.get("data", {}).get("rows", []):

                dimension_values = row.get("dimensions", [])
                metric_values = row.get("metrics", [])[0].get("values", [])

                # Construct base dict for the eventual database row with the
                # website url. Add to the dict by looping through the dimension
                # and metric values
                row_dict_base = {
                    "website_url": website_url,
                    "account_label": account_label,
                    "last_synced_at": datetime.utcnow().strftime(
                        "%Y-%m-%d %H:%M:%S.%f"
                    ),
                }

                # Concatenate website URL and account label. Add dimensions to
                # that concatenation. This will be used as the primary key to
                # identify when to insert or update records. Sync time isn't
                # included in the concatenation because the app is set up to
                # update existing keys; adding the sync time to the key means
                # new rows will always be added even if the data has not changed
                dimension_concat = website_url + account_label

                for i, value in enumerate(dimension_values):

                    key_raw = (dimension_names[i]).replace("ga:", "")
                    key = camel_to_snake(key_raw)

                    # Add hyphens to dates
                    if key == "date":

                        value = value[:4] + "-" + value[4:6] + "-" + value[6:]

                    dimension_concat += value
                    row_dict_base[key] = value

                for i, value in enumerate(metric_values):

                    # Metrics of type percent need to be divided by 100 to be
                    # expressed properly as decimal values. All values need to
                    # be converted from strings to numbers prior to division
                    value = float(value)

                    if metric_names[i]["type"] == "PERCENT":

                        value /= 100

                    key_raw = (metric_names[i]["name"]).replace("ga:", "")
                    key = camel_to_snake(key_raw)
                    row_dict_base[key] = value

                # Construct primary key dict and add earlier dict to it. This is
                # just so that the primary key is the first record in the
                # downstream Snowflake table
                row_dict = {
                    "primary_key": hashlib.md5(
                        dimension_concat.encode("utf-8")
                    ).hexdigest()
                }
                row_dict.update(row_dict_base)

                # Append to the proper flat report list
                if report_name == "platform_traffic":

                    platform_traffic_output.append(row_dict)

                elif report_name == "channel_performance":

                    channel_performance_output.append(row_dict)

                elif report_name == "page_traffic":

                    page_traffic_output.append(row_dict)

                elif report_name == "ecommerce_performance":

                    ecommerce_performance_output.append(row_dict)


# Create synchronous functions to create tables in Snowflake and upsert data
def create_snowflake_tables(
    snowflake_user, snowflake_password, snowflake_account, database
):

    try:

        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="loader_wh",
        )

        logger.info("Connected to Snowflake")

    except Exception as e:

        logger.error(f"Failed to authenticate to snowflake; {e}")

        raise

    # Create schema and tables
    logger.info("Creating schema and tables if they do not exist")

    base_connection.cursor().execute(f"use database {database}")
    base_connection.cursor().execute("create schema if not exists google_analytics")
    base_connection.cursor().execute("use schema google_analytics")
    base_connection.cursor().execute(
        """
            create table if not exists ua_channel_performance (
                primary_key varchar,
                website_url varchar,
                account_label varchar,
                campaign varchar,
                full_referrer varchar,
                keyword varchar,
                medium varchar,
                ad_content varchar,
                source varchar,
                avg_session_duration number(32, 10),
                bounce_rate number(32, 10),
                bounces integer,
                organic_searches integer,
                sessions integer,
                pageviews_per_session number(32, 10),
                percent_new_sessions number(32, 10),
                date date,
                last_synced_at timestamp_ntz                
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists ua_ecommerce_performance (
                primary_key varchar, 
                website_url varchar,
                account_label varchar,
                transaction_id varchar,
                campaign varchar,  
                full_referrer varchar, 
                keyword varchar, 
                medium varchar, 
                landing_page_path varchar, 
                source varchar,
                date date,
                last_synced_at timestamp_ntz                                      
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists ua_page_traffic (
                primary_key varchar,
                website_url varchar,
                account_label varchar,                
                page_title varchar,
                avg_session_duration number(32, 10),
                avg_time_on_page number(32, 10),
                bounce_rate number(32, 10),
                bounces integer,
                exit_rate number(32, 10),
                page_value number(32, 10),
                pageviews integer,
                percent_new_sessions number(32, 10),
                unique_pageviews integer,
                users integer,
                date date,
                last_synced_at timestamp_ntz                              
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists ua_platform_traffic (
                primary_key varchar,
                website_url varchar,
                account_label varchar,                
                operating_system varchar,
                browser varchar,
                device_category varchar,
                avg_session_duration number(32, 10),
                bounce_rate number(32, 10),
                bounces integer,
                new_users integer,
                pageviews integer,
                pageviews_per_session number(32, 10),
                percent_new_sessions number(32, 10),
                sessions integer,
                sessions_per_user number(32, 10),
                users integer,   
                date date,
                last_synced_at timestamp_ntz         
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_data(snowflake_user, snowflake_password, snowflake_account, database):
    try:

        # Create a SQLAlchemy connection to the Snowflake database
        logger.info("Creating SQLAlchemy Snowflake session")
        engine = create_engine(
            f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/google_analytics?warehouse=loader_wh"
        )
        session = sessionmaker(bind=engine)()
        alchemy_connection = engine.connect()
        logger.info("SQLAlchemy Snowflake session creation successful")

    except Exception as e:

        logger.error(f"SQLAlchemy Snowflake session creation failed: {e}")

        raise

    # Create explicit datatype dicts
    channel_performance_dtypes = {
        "primary_key": VARCHAR,
        "website_url": VARCHAR,
        "account_label": VARCHAR,
        "campaign": VARCHAR,
        "full_referrer": VARCHAR,
        "keyword": VARCHAR,
        "medium": VARCHAR,
        "ad_content": VARCHAR,
        "source": VARCHAR,
        "avg_session_duration": NUMERIC(32, 10),
        "bounce_rate": NUMERIC(32, 10),
        "bounces": INTEGER,
        "organic_searches": INTEGER,
        "sessions": INTEGER,
        "pageviews_per_session": NUMERIC(32, 10),
        "percent_new_sessions": NUMERIC(32, 10),
        "date": DATE,
        "last_synced_at": TIMESTAMP(timezone=False),
    }
    ecommerce_performance_dtypes = {
        "primary_key": VARCHAR,
        "website_url": VARCHAR,
        "account_label": VARCHAR,
        "transaction_id": VARCHAR,
        "campaign": VARCHAR,
        "full_referrer": VARCHAR,
        "keyword": VARCHAR,
        "medium": VARCHAR,
        "landing_page_path": VARCHAR,
        "source": VARCHAR,
        "date": DATE,
        "last_synced_at": TIMESTAMP(timezone=False),
    }
    page_traffic_dtypes = {
        "primary_key": VARCHAR,
        "website_url": VARCHAR,
        "account_label": VARCHAR,
        "page_title": VARCHAR,
        "avg_session_duration": NUMERIC(32, 10),
        "avg_time_on_page": NUMERIC(32, 10),
        "bounce_rate": NUMERIC(32, 10),
        "bounces": INTEGER,
        "exit_rate": NUMERIC(32, 10),
        "page_value": NUMERIC(32, 10),
        "pageviews": INTEGER,
        "percent_new_sessions": NUMERIC(32, 10),
        "unique_pageviews": INTEGER,
        "users": INTEGER,
        "date": DATE,
        "last_synced_at": TIMESTAMP(timezone=False),
    }
    platform_traffic_dtypes = {
        "primary_key": VARCHAR,
        "website_url": VARCHAR,
        "account_label": VARCHAR,
        "operating_system": VARCHAR,
        "browser": VARCHAR,
        "device_category": VARCHAR,
        "avg_session_duration": NUMERIC(32, 10),
        "bounce_rate": NUMERIC(32, 10),
        "bounces": INTEGER,
        "new_users": INTEGER,
        "pageviews": INTEGER,
        "pageviews_per_session": NUMERIC(32, 10),
        "percent_new_sessions": NUMERIC(32, 10),
        "sessions": INTEGER,
        "sessions_per_user": NUMERIC(32, 10),
        "users": INTEGER,
        "date": DATE,
        "last_synced_at": TIMESTAMP(timezone=False),
    }

    # Use flat tables and defined datatypes to create stage tables in Snowflake
    logger.info("Creating stage tables")

    if len(flat_channel_performance) > 0:

        dict_to_sql(
            flat_channel_performance,
            "ua_channel_performance_stage",
            engine,
            dtype=channel_performance_dtypes,
        )

    if len(flat_ecommerce_performance) > 0:

        dict_to_sql(
            flat_ecommerce_performance,
            "ua_ecommerce_performance_stage",
            engine,
            dtype=ecommerce_performance_dtypes,
        )

    if len(flat_page_traffic) > 0:

        dict_to_sql(
            flat_page_traffic,
            "ua_page_traffic_stage",
            engine,
            dtype=page_traffic_dtypes,
        )

    if len(flat_platform_traffic) > 0:

        dict_to_sql(
            flat_platform_traffic,
            "ua_platform_traffic_stage",
            engine,
            dtype=platform_traffic_dtypes,
        )

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stage tables to corresponding target tables")

    if len(flat_channel_performance) > 0:

        # Create stage and target tables
        channel_performance_stage_table = meta.tables["ua_channel_performance_stage"]
        channel_performance_target_table = meta.tables["ua_channel_performance"]

        # Structure merge
        channel_performance_merge = MergeInto(
            target=channel_performance_target_table,
            source=channel_performance_stage_table,
            on=channel_performance_target_table.c.primary_key
            == channel_performance_stage_table.c.primary_key,
        )

        channel_performance_cols = {
            "primary_key": channel_performance_stage_table.c.primary_key,
            "website_url": channel_performance_stage_table.c.website_url,
            "account_label": channel_performance_stage_table.c.account_label,
            "campaign": channel_performance_stage_table.c.campaign,
            "full_referrer": channel_performance_stage_table.c.full_referrer,
            "keyword": channel_performance_stage_table.c.keyword,
            "medium": channel_performance_stage_table.c.medium,
            "ad_content": channel_performance_stage_table.c.ad_content,
            "source": channel_performance_stage_table.c.source,
            "avg_session_duration": channel_performance_stage_table.c.avg_session_duration,
            "bounce_rate": channel_performance_stage_table.c.bounce_rate,
            "bounces": channel_performance_stage_table.c.bounces,
            "organic_searches": channel_performance_stage_table.c.organic_searches,
            "sessions": channel_performance_stage_table.c.sessions,
            "pageviews_per_session": channel_performance_stage_table.c.pageviews_per_session,
            "percent_new_sessions": channel_performance_stage_table.c.percent_new_sessions,
            "date": channel_performance_stage_table.c.date,
            "last_synced_at": channel_performance_stage_table.c.last_synced_at,
        }

        channel_performance_merge.when_not_matched_then_insert().values(
            **channel_performance_cols
        )

        channel_performance_merge.when_matched_then_update().values(
            **channel_performance_cols
        )

        # Execute merge
        alchemy_connection.execute(channel_performance_merge)

    if len(flat_ecommerce_performance) > 0:

        # Create stage and target tables
        ecommerce_performance_stage_table = meta.tables[
            "ua_ecommerce_performance_stage"
        ]
        ecommerce_performance_target_table = meta.tables["ua_ecommerce_performance"]

        # Structure merge
        ecommerce_performance_merge = MergeInto(
            target=ecommerce_performance_target_table,
            source=ecommerce_performance_stage_table,
            on=ecommerce_performance_target_table.c.primary_key
            == ecommerce_performance_stage_table.c.primary_key,
        )

        ecommerce_performance_cols = {
            "primary_key": ecommerce_performance_stage_table.c.primary_key,
            "website_url": ecommerce_performance_stage_table.c.website_url,
            "account_label": ecommerce_performance_stage_table.c.account_label,
            "transaction_id": ecommerce_performance_stage_table.c.transaction_id,
            "campaign": ecommerce_performance_stage_table.c.campaign,
            "full_referrer": ecommerce_performance_stage_table.c.full_referrer,
            "keyword": ecommerce_performance_stage_table.c.keyword,
            "medium": ecommerce_performance_stage_table.c.medium,
            "landing_page_path": ecommerce_performance_stage_table.c.landing_page_path,
            "source": ecommerce_performance_stage_table.c.source,
            "date": ecommerce_performance_stage_table.c.date,
            "last_synced_at": ecommerce_performance_stage_table.c.last_synced_at,
        }

        ecommerce_performance_merge.when_not_matched_then_insert().values(
            **ecommerce_performance_cols
        )

        ecommerce_performance_merge.when_matched_then_update().values(
            **ecommerce_performance_cols
        )

        # Execute merge
        alchemy_connection.execute(ecommerce_performance_merge)

    if len(flat_page_traffic) > 0:

        # Create stage and target tables
        page_traffic_stage_table = meta.tables["ua_page_traffic_stage"]
        page_traffic_target_table = meta.tables["ua_page_traffic"]

        # Structure merge
        page_traffic_merge = MergeInto(
            target=page_traffic_target_table,
            source=page_traffic_stage_table,
            on=page_traffic_target_table.c.primary_key
            == page_traffic_stage_table.c.primary_key,
        )

        page_traffic_cols = {
            "primary_key": page_traffic_stage_table.c.primary_key,
            "website_url": page_traffic_stage_table.c.website_url,
            "account_label": page_traffic_stage_table.c.account_label,
            "page_title": page_traffic_stage_table.c.page_title,
            "avg_session_duration": page_traffic_stage_table.c.avg_session_duration,
            "avg_time_on_page": page_traffic_stage_table.c.avg_time_on_page,
            "bounce_rate": page_traffic_stage_table.c.bounce_rate,
            "bounces": page_traffic_stage_table.c.bounces,
            "exit_rate": page_traffic_stage_table.c.exit_rate,
            "page_value": page_traffic_stage_table.c.page_value,
            "pageviews": page_traffic_stage_table.c.pageviews,
            "percent_new_sessions": page_traffic_stage_table.c.percent_new_sessions,
            "unique_pageviews": page_traffic_stage_table.c.unique_pageviews,
            "users": page_traffic_stage_table.c.users,
            "date": page_traffic_stage_table.c.date,
            "last_synced_at": page_traffic_stage_table.c.last_synced_at,
        }

        page_traffic_merge.when_not_matched_then_insert().values(**page_traffic_cols)

        page_traffic_merge.when_matched_then_update().values(**page_traffic_cols)

        # Execute merge
        alchemy_connection.execute(page_traffic_merge)

    if len(flat_platform_traffic) > 0:

        # Create stage and target tables
        platform_traffic_stage_table = meta.tables["ua_platform_traffic_stage"]
        platform_traffic_target_table = meta.tables["ua_platform_traffic"]

        # Structure merge
        platform_traffic_merge = MergeInto(
            target=platform_traffic_target_table,
            source=platform_traffic_stage_table,
            on=platform_traffic_target_table.c.primary_key
            == platform_traffic_stage_table.c.primary_key,
        )

        platform_traffic_cols = {
            "primary_key": platform_traffic_stage_table.c.primary_key,
            "website_url": platform_traffic_stage_table.c.website_url,
            "account_label": platform_traffic_stage_table.c.account_label,
            "operating_system": platform_traffic_stage_table.c.operating_system,
            "browser": platform_traffic_stage_table.c.browser,
            "device_category": platform_traffic_stage_table.c.device_category,
            "avg_session_duration": platform_traffic_stage_table.c.avg_session_duration,
            "bounce_rate": platform_traffic_stage_table.c.bounce_rate,
            "bounces": platform_traffic_stage_table.c.bounces,
            "new_users": platform_traffic_stage_table.c.new_users,
            "pageviews": platform_traffic_stage_table.c.pageviews,
            "pageviews_per_session": platform_traffic_stage_table.c.pageviews_per_session,
            "percent_new_sessions": platform_traffic_stage_table.c.percent_new_sessions,
            "sessions": platform_traffic_stage_table.c.sessions,
            "sessions_per_user": platform_traffic_stage_table.c.sessions_per_user,
            "users": platform_traffic_stage_table.c.users,
            "date": platform_traffic_stage_table.c.date,
            "last_synced_at": platform_traffic_stage_table.c.last_synced_at,
        }

        platform_traffic_merge.when_not_matched_then_insert().values(
            **platform_traffic_cols
        )

        platform_traffic_merge.when_matched_then_update().values(
            **platform_traffic_cols
        )

        # Execute merge
        alchemy_connection.execute(platform_traffic_merge)

    # Close connection
    alchemy_connection.close()
    logger.info("SQLAlchemy connection closed")


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[2]

# Universal Analytics was sunset as a service on July 1, 2023 and data will
# no longer be available six months after that. This step will only run if an
# explicit override is passed to do so
ua_override = sys.argv[3]

if (
    __name__ == "__main__"
    and ua_override == "true"
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
):

    current_date = datetime.today().date()

    # Retrieve sync type from parameter
    sync_type = sys.argv[1]

    # Retrieve optional single URL target from parameter
    single_url = sys.argv[3]

    # Set database type
    if sync_type == "test":

        # Always use the dev snowflake database for this sync type -- even in
        # production environments
        database = "raw_dev"

    elif os.getenv("RES_ENV") == "production":

        database = "raw"

    else:

        database = "raw_dev"

    # Retrieve Google service account credentials
    google_creds = get_secret("DATA_TEAM_GOOGLE_SERVICE_KEY")

    # Retrieve list of target universal analytics views
    target_views = get_secret("GOOGLE_UNIVERSAL_ANALYTICS_PROFILES")
    view_items = target_views["items"]

    # Retrieve Snowflake credentials from secrets manager
    snowflake_cred = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")

    # Create tables
    create_snowflake_tables(
        snowflake_cred["user"],
        snowflake_cred["password"],
        snowflake_cred["account"],
        database,
    )

    # Create empty output lists
    flat_channel_performance = []
    flat_ecommerce_performance = []
    flat_page_traffic = []
    flat_platform_traffic = []

    # Create service account object
    analytics_service_object = initialize_analytics_reporting(
        google_credentials=google_creds,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        api="analyticsreporting",
        version_string="v4",
    )

    # For single_url syncs, reduce the property items list to the single
    # property and then perform the appropriate sync type. This is done by
    # adjusting the sync type (if it is a single_property type) or, if the sync
    # type was provided as normal, using that in conjuction with the provided
    # single property target. This functionality is provided in order to trigger
    # syncs in prod for newly added domains without having to run the sync on existing ones
    if single_url != "none":

        view_items[:] = [
            item
            for item in view_items
            if item.get("websiteUrl", "").__contains__(f"{single_url}")
        ]

        if len(view_items) == 0:

            logger.error("Provided URL does not match any target view URL")

            raise Exception("Provided URL does not match any target view URL")

        logger.info(f"Sync for single Universal Analytics URL {single_url}")

        if sync_type == "single_url_full":

            sync_type == "full"

        elif sync_type == "single_url_incremental":

            sync_type == "incremental"

    # Other syncs (or undeclared sync types) will sync for all websites
    if sync_type == "full":

        logger.info("Full sync for Google Analytics - Universal Analytics")

        # Resonance was founded in July 2015. Perform syncs for fifteen day
        # periods up until present day. This time length reduces the risk of
        # Google deciding to return sampled data.
        start_date = datetime.strptime("2015-07-15", "%Y-%m-%d").date()

    elif sync_type == "test":

        # Used in development to test full sync looping functionality over a
        # small subset of time.
        logger.info("Test sync for Google Analytics - Universal Analytics")

        start_date = datetime.strptime("2022-01-01", "%Y-%m-%d").date()
        current_date = datetime.strptime("2022-01-31", "%Y-%m-%d").date()

    else:

        # All other syncs are either explicitly incremental or coerced to
        # incremental
        if sync_type == "incremental":

            logger.info(
                """
                    Incremental sync for Google Analytics - Universal Analytics
                """
            )

        else:

            logger.info(
                """
                    Empty or invalid sync type; Defaulting to incremental 
                    sync for Google Analytics - Universal Analytics
                """
            )

        # Retrieve Snowflake date
        start_date_str = get_latest_snowflake_timestamp(
            schema="GOOGLE_ANALYTICS",
            table="UA_CHANNEL_PERFORMANCE",
            snowflake_user=snowflake_cred["user"],
            snowflake_password=snowflake_cred["password"],
            snowflake_account=snowflake_cred["account"],
            timestamp_field="date",
            is_date=True,
        )

        if start_date_str is None:

            logger.error("Error retrieving latest date")

            raise Exception("Error retrieving latest date")

        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()

        # On July 1, 2023, standard Universal Analytics properties will stop
        # processing new hits. From that point forward only GA4 properties
        # will process data. After July 1, 2023, previously processed data
        # in a Universal Analytics property will be available for at least
        # six months. This step will not do anything if the latest date is
        # July 1, 2023 and the last sync time is at least one day after that
        # (i.e a sync with that days full data has occurred)

        if start_date_str == "2023-07-01":

            last_sync_date_str = get_latest_snowflake_timestamp(
                schema="GOOGLE_ANALYTICS",
                table="UA_CHANNEL_PERFORMANCE",
                snowflake_user=snowflake_cred["user"],
                snowflake_password=snowflake_cred["password"],
                snowflake_account=snowflake_cred["account"],
                timestamp_field="last_synced_at",
                is_date=True,
            )

            # Exit the step if the last sync date is at least two days after
            # the Universal Analytics sunset date
            last_sync_date = datetime.strptime(last_sync_date_str, "%Y-%m-%d").date()
            sunset_date = start_date

            if last_sync_date - sunset_date > 1:

                os._exit()

    # Set the end date arbitrarily in the past to begin the while loop
    end_date = current_date - timedelta(days=1)

    while end_date < current_date:

        # Set the end date to the lesser of 15 days after start date and
        # the current date
        end_date = min(current_date, start_date + timedelta(days=15))
        start_date_str = datetime.strftime(start_date, "%Y-%m-%d")
        end_date_str = datetime.strftime(end_date, "%Y-%m-%d")

        logger.info(f"Syncing data from {start_date_str} to {end_date_str}")

        response_output_list = []

        # Execute async API calls
        asyncio.run(
            get_all_reports(
                view_items,
                analytics_service_object,
                start_date_str,
                end_date_str,
                response_output_list,
            )
        )

        if len(response_output_list) > 0:

            parse_responses(
                response_list=response_output_list,
                channel_performance_output=flat_channel_performance,
                ecommerce_performance_output=flat_ecommerce_performance,
                page_traffic_output=flat_page_traffic,
                platform_traffic_output=flat_platform_traffic,
            )

            # Clean up
            del response_output_list
            gc.collect()

            upsert_data(
                snowflake_user=snowflake_cred["user"],
                snowflake_password=snowflake_cred["password"],
                snowflake_account=snowflake_cred["account"],
                database=database,
            )

        # Clean up; reset lists
        del flat_channel_performance
        del flat_ecommerce_performance
        del flat_page_traffic
        del flat_platform_traffic

        gc.collect()

        flat_channel_performance = []
        flat_ecommerce_performance = []
        flat_page_traffic = []
        flat_platform_traffic = []

        # Iterate. Date ranges in queries are inclusive.
        start_date = end_date + timedelta(days=1)

    logger.info("Google Analytics Universal Analytics sync complete")
