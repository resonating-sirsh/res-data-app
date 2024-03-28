from looker_sdk.sdk.api40.models import DashboardBase
from res.connectors.looker.ResLookerClient import ResLookerClient
from res.connectors.email.resmailconnector import ResEmailClient
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.flows import FlowContext
import res
from res.utils import logger
import requests
import os
import json
import sys
import traceback
from datetime import datetime

ENV = os.getenv("RES_ENV")
RES_META_BASE = "appc7VJXjoGsSOjmw"
BRANDS_TABLE = "tblMnwRUuEvot969I"
SUBSCRIBER_TABLE = "tblJeGr97r8Ogtdrl"
USERS_TABLE = "tblY3a0W4urnjt7a6"
DASHBOARD_IDS = [
    403,
    861,
]  # Currently the daily performance report + the Make ONE Summary Report
EMAILS_TO_INCLUDE = [
    "aahmad@resonance.nyc",
    "ajbutler@resonance.nyc",
    "emagro@resonance.nyc",
    "india@resonance.nyc",
    "ksteib@resonance.nyc",
    "lawrence@resonance.nyc",
    "managedservices@resonance.nyc",
    "nshaikh@resonance.nyc",
    "techpirates@resonance.nyc",
]  # All reports will go to these emails
SNOWFLAKE_QUERY = """select brand_code
from IAMCURIOUS_PRODUCTION.shopify_orders
where created_at >= DATEADD(Day ,-7, current_date)
group by 1 order by 1"""


def generate_reports(record, fc, brands_with_orders, users, event_data):
    airtable_client = ResAirtableClient()

    # Set event settings
    brand_filter = event_data.get("brand_code", None)
    dashboard_id_filter = event_data.get("dashboard_id", None)
    if "send_email" in event_data:
        send_email = True if event_data.get("send_email").lower() == "true" else False
    else:
        send_email = True

    brand_code = record.get("Code", None)
    if brand_filter and brand_code != brand_filter:
        logger.info(f"Skipping brand due to event settings: {brand_code}")
        return 0
    if (
        not record.get("Active")
        or type(record.get("Primary Users")) != list
        or len(record.get("Primary Users")) == 0
    ):
        logger.info(
            f"Brand not set up for emails (either not active or no users): {brand_code}"
        )
        return 0
    if brand_code not in brands_with_orders:
        logger.info(f"Brand had no orders in the past week: {brand_code}")
        return 0

    logger.info(f"Processing {brand_code}")

    for dashboard_id in DASHBOARD_IDS:
        if dashboard_id_filter and str(dashboard_id) != str(dashboard_id_filter):
            logger.info(f"Skipping dashboard due to event settings: {dashboard_id}")
            continue
        logger.info(f"\tGenerating dashboard {dashboard_id}")
        result = fc.connectors["looker"].generate_pdf_from_dashboard(
            dashboard_id,
            width=1900,
            filters={"Brand Code": brand_code},
        )
        filename = "dashboard-" + str(datetime.now().date()) + ".pdf"
        file = open(filename, "wb")
        file.write(result)
        file.close()

        # for now adding dashboards under res-data but we could promote a separate bucket
        # the path of this is fixed as we do upsert the file id in mongo so the same id should always work
        path = f"s3://resmagic/saved-dashboard/{dashboard_id}/{brand_code}/{filename}"

        logger.info(f"\tSaving report to {path}")
        fc.connectors["s3"].write(path, result)

        # for testing adding this here so that we can create the reports without updating anything
        file_id = fc.connectors["mongo"].legacy_add_resmagic_file(
            path,
            size=sys.getsizeof(result),
            file_type="application/pdf",
        )
        logger.info(f"\tCreated a linked file in mongo/files with _id {file_id}")

        # Update the record in Airtable, so create.ONE points to the new report PDF file
        airtable_rows = airtable_client.get_records(
            RES_META_BASE,
            SUBSCRIBER_TABLE,
            filter_by_formula='AND({Brand Code}="'
            + brand_code
            + '",{Dashboard ID}='
            + str(dashboard_id)
            + ")",
        )
        record_id = next(airtable_rows, {}).get("id", None)
        if not record_id:
            logger.error(
                f"Couldn't find record_id for brand in Airtable subscriber base! Brand={brand_code} and Dashboard_ID={dashboard_id}"
            )
        elif (
            ENV == "production"
            or event_data.get("update_create_one", "false").lower() == "true"
        ):
            airtable_client.set_record(
                RES_META_BASE,
                SUBSCRIBER_TABLE,
                record_id,
                "Last Report File Id",
                file_id,
            )
        else:
            logger.info("Skipping upload of PDF to create.one due to event settings")

        user_emails = []
        for user in record["Primary Users"]:
            # Lookup user email based on record_id
            user_emails.append(users.get(user, None))
        email_list = user_emails + EMAILS_TO_INCLUDE
        logger.info(f"Production email addresses: {email_list}")

        message = record["Name"] + " Daily Performance Dashboard"
        if ENV != "production":
            # Override email list for non-prod environments
            email_list = ["techpirates@resonance.nyc", "bgriffin@resonance.nyc"]
            message = "TESTING: IGNORE - " + message
        logger.info(f"\tSending email to {email_list}")
        if send_email:
            ResEmailClient().send_email(
                email_list,
                message,
                "Please find your Daily Performance report attached. If you have any questions or comments, reach out to support@resonance.nyc",
                filename,
            )
            logger.info(f"\tEmail Sent!")
        else:
            logger.info("Skipping email send due to event settings")


def handler(event, context):
    """
    Loops through all active brands and generates daily reports
    Uploads the PDF version to S3 + makes available in create.ONE
    Emails the PDF to the brands
    """
    snowflake_client = ResSnowflakeClient()
    results = snowflake_client.execute(SNOWFLAKE_QUERY)
    brands_with_orders = [result[0] for result in results]

    with FlowContext(event, context) as fc:
        # Get list of active brands with their email list
        ac = fc.connectors["airtable"]
        brands_table = ac[RES_META_BASE][BRANDS_TABLE].to_dataframe()
        users_table = ac[RES_META_BASE][USERS_TABLE].to_dataframe()
        users = {}
        for user in users_table.to_dict("records"):
            users[user["record_id"]] = user["Email"]

        for record in brands_table.to_dict("records"):
            try:
                generate_reports(record, fc, brands_with_orders, users, event)
            except Exception as e:
                logger.error(str(e) + str(traceback.format_exc()))
                continue
