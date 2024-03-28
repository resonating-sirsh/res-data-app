import asyncio
import os
import sys
from io import StringIO, BytesIO
import csv
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from snowflake.connector import DictCursor
from quickbooks import helpers
from quickbooks.objects import (
    Customer,
    CustomerMemo,
    DescriptionLineDetail,
    DescriptionOnlyLine,
    Invoice,
    SalesItemLine,
    SalesItemLineDetail,
    CustomField,
    Item,
)
from boxsdk.exception import BoxAPIException

from helper_functions import (
    get_enabled_sales_form_custom_fields,
    get_invoice_by_doc_number,
    validate_brand_folder_structure,
    get_last_complete_week_dates,
    add_and_alert_uninvoiced_line_items,
)
from objects import SALES_CHANNEL_ITEM_IDS, JOURNAL_ENTRY_BRANDS
from queries import SNOWFLAKE_BRANDS, SNOWFLAKE_RES_COMPANIES_ORDER_LINE_ITEMS

from res.connectors.quickbooks.QuickbooksClient import QuickBooksClient
from res.connectors.box import BoxConnector
from res.utils import logger, secrets_client, ping_slack

# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
DEV_OVERRIDE = (sys.argv[1] or "").lower() == "true"
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[5] or sys.argv[5].lower() == "all"

# Constants
BOX_BRANDS_FOLDER_ID = "143847228389"


async def generate_invoice(
    brand_dict: dict,
    all_line_items: list[dict],
    qbo_client: QuickBooksClient,
    box_connector: BoxConnector,
    start_date: str,
    end_date: str,
    errors: list,
    box_pdf_folder_id: str,
    box_csv_folder_id: str,
):
    """
    Creates an invoice asynchronously for a single brand's line items
    """

    # Create invoice object
    invoice = Invoice()

    # Retrieve brand info
    brand_name = brand_dict.get("brand_name")
    brand_code = brand_dict.get("brand_code")
    brand_qbo_customer_id = brand_dict.get("quickbooks_companies_id")

    # Designate customer. In prod this uses the brand's Quickbooks ID from the
    # brands table to retrieve a customer and then create a customer ref. In
    # dev, or if the brand does not have a Quickbooks ID, a generic customer ref
    # is used
    def generic_customer_ref():

        if RES_ENV == "production":

            customer_ref = Customer.get(53, qb=qbo_client.client).to_ref()

        elif RES_ENV == "development":

            customer_ref = Customer.get(58, qb=qbo_client.client).to_ref()

        return customer_ref

    # Add customer attributes to the invoice.
    if RES_ENV == "production":

        try:

            customer_ref = Customer.get(
                int(brand_qbo_customer_id), qb=qbo_client.client
            ).to_ref()

        except Exception as e:

            logger.warning(
                f"Could not retrieve a customer record for brand {brand_name}"
                f" with Quickbooks customer ID {brand_qbo_customer_id}."
                " Using a generic customer"
            )
            customer_ref = generic_customer_ref()

    elif RES_ENV == "development":

        logger.info("Dev environment; using a generic customer for sandbox env")
        customer_ref = generic_customer_ref()

    # Add customer, memo, and dates
    invoice.CustomerRef = customer_ref
    invoice.CustomerMemo = CustomerMemo()
    invoice.CustomerMemo.value = (
        f"Invoice for {brand_name} ({brand_code}) orders fulfilled from"
        f" {start_date} to {end_date}\n"
        "\n"
        "REMIT TO:\n"
        "\n"
        "Resonance Inc.\n"
        "Bank: Silicon Valley Bank\n"
        "3003 Tasman Drive, Santa Clara, CA 95054\n"
        "Account Name: Resonance Companies, Inc.\n"
        "Routing No: 121140399\n"
        "Credit Account: 3302363347\n"
    )
    invoice.TxnDate = end_date
    invoice.DueDate = helpers.qb_date_format(
        datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=30)
    )

    # Check to see if an invoice with a matching document number already exists.
    # If it does, add a timestamp to the document number. For user provided date
    # ranges change the standard format
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    if sys.argv[3].lower() == "last_complete_week":

        doc_number = f"{brand_code} WK {end_date}"

    else:

        doc_number = f"{brand_code} {current_datetime}"

    if get_invoice_by_doc_number(doc_number, qbo_client) is not None:

        modified_doc_number = f"{brand_code} {current_datetime}"
        logger.warning(
            f"An invoice with document number {doc_number} already exists;"
            f" assigning invoice document number {modified_doc_number}"
        )
        invoice.DocNumber = modified_doc_number

    else:

        invoice.DocNumber = doc_number

    # Winnow line items to only those needed for the brand
    brand_line_items = [i for i in all_line_items if i["brand_code"] == brand_code]

    # Invoices have a Line attribute which is a list for SalesItemLine objects.
    # SalesItemLine objects contain SalesItemLineDetail objects which can
    # reference Item objects.
    invoice_lines_dict = {}

    for order_line_item in brand_line_items:

        order_line_item_ids = order_line_item["order_line_item_ids"]
        order_id = order_line_item["order_id"]
        order_number = order_line_item["order_number"]
        channel = order_line_item["channel"]
        is_ecom_or_wholesale = channel.lower() in ["ecom", "e-commerce", "wholesale"]
        sku = order_line_item["sku"]
        style_id = order_line_item["style_id"]
        count = order_line_item["count"]
        revenue_share = order_line_item["revenue_share"]
        price = order_line_item["line_item_price"]
        total_cost = order_line_item["total_cost"]
        is_in_airtable = order_line_item["is_in_airtable"]

        # Check if the order line item is missing either the style ID, price,
        # or revenue share. Log these instances and continue. Missing price and
        # revenue share is only important for e-commerce and wholesale orders
        # since other channels only charge BCOO. Style ID is always needed
        # for BCOO
        error = {
            "app_name": "res_companies_weekly_brand_invoices",
            "order_line_item_ids": order_line_item_ids,
            "order_id": order_id,
            "order_number": order_number,
            "sku": sku,
            "is_in_airtable": is_in_airtable,
        }

        if not style_id:

            error["error"] = "Missing style ID"
            errors.append(error)

        if (price or 0) == 0 and is_ecom_or_wholesale:

            error["error"] = f"Missing {channel} price for style {style_id}"
            errors.append(error)

        if total_cost == 0:

            error["error"] = f"Missing style {style_id} costs"
            errors.append(error)

        if all(
            [
                # Only report this for revenue share amounts that are null due to
                # missing brand revenue share, not missing prices
                price is not None,
                revenue_share is None,
                is_ecom_or_wholesale,
            ]
        ):

            error["error"] = f"Missing {channel} revenue share for brand {brand_name}"
            errors.append(error)

        if any(
            [
                not style_id,
                (price or 0) == 0 and is_ecom_or_wholesale,
                total_cost == 0,
                price is not None and revenue_share is None and is_ecom_or_wholesale,
            ]
        ):

            continue

        # Check if the order is already present in the invoice_lines_dict and
        # add it if it is not. Data is sorted in the Snowflake query so line
        # items in the same order should be sequential. Lines are grouped by
        # order (1) to add subtotal rows and (2) to charge shipping once per
        # order
        if order_id not in invoice_lines_dict:

            invoice_lines_dict[order_id] = []

        # Retrieve the item corresponding to the sales channel
        item_id = SALES_CHANNEL_ITEM_IDS["resonance_companies"][
            channel.lower() or "null"
        ][RES_ENV]
        item_ref = Item.get(item_id, qb=qbo_client.client).to_ref()

        # Create the detail object for BCOO
        bcoo_line_detail = SalesItemLineDetail()
        bcoo_line_detail.ItemRef = item_ref
        bcoo_line_detail.ServiceDate = helpers.qb_date_format(
            order_line_item["created_date"]
        )
        bcoo_line_detail.Qty = count
        bcoo_line_detail.UnitPrice = total_cost

        # Create the line object for BCOO
        bcoo_line = SalesItemLine()
        bcoo_line.SalesItemLineDetail = bcoo_line_detail
        bcoo_line.Amount = total_cost * count
        bcoo_line.Description = f"Order: {order_number or ''} - {sku} - BCOO"

        # Append lines to dict list
        invoice_lines_dict[order_id].append(bcoo_line)

        # Create line for revenue share for ecom and wholesale orders
        if is_ecom_or_wholesale and revenue_share > 0:

            # Create the detail object for revenue share
            rev_share_line_detail = SalesItemLineDetail()
            rev_share_line_detail.ItemRef = item_ref
            rev_share_line_detail.ServiceDate = helpers.qb_date_format(
                order_line_item["created_date"]
            )
            rev_share_line_detail.Qty = count
            rev_share_line_detail.UnitPrice = revenue_share

            # Create the line object for revenue share
            rev_share_line = SalesItemLine()
            rev_share_line.SalesItemLineDetail = rev_share_line_detail
            rev_share_line.Amount = revenue_share * count
            rev_share_line.Description = (
                f"Order: {order_number or ''} - {sku} - Revenue Share"
            )

            # Append lines to dict list
            invoice_lines_dict[order_id].append(rev_share_line)

    # Add shipping to each order and add the order to the invoice. Maintain a
    # count of orders and order line items by counting order_id and COGs lines
    total_orders = 0
    total_items = 0

    for order_id, order_line_item_invoice_lines in invoice_lines_dict.items():

        total_orders += 1
        total_items += len(
            [i for i in order_line_item_invoice_lines if "BCOO" in i.Description]
        )
        invoice.Line.extend(order_line_item_invoice_lines)

        # Identify the order's fulfillment date. Even though there is one charge
        # per order it is possible for orders to ship in multiple shipments and
        # thus be fulfilled at different times. Use the latest fulfillment date
        fulfillment_date = max(
            [
                i["fulfillment_completed_date"]
                for i in brand_line_items
                if i["fulfillment_completed_date"] and i["order_id"] == order_id
            ]
        )
        order_number = [
            i["order_number"] for i in brand_line_items if i["order_id"] == order_id
        ][0]

        # Add order shipping. Currently fixed at $20 per order

        # Line detail
        channel = [i["channel"] for i in brand_line_items if i["order_id"] == order_id][
            0
        ]
        # Retrieve the item corresponding to the sales channel
        item_id = SALES_CHANNEL_ITEM_IDS["resonance_companies"][
            channel.lower() or "null"
        ][RES_ENV]
        item_ref = Item.get(item_id, qb=qbo_client.client).to_ref()

        # Add details
        shipping_line_detail = SalesItemLineDetail()
        shipping_line_detail.ItemRef = item_ref
        shipping_line_detail.ServiceDate = helpers.qb_date_format(fulfillment_date)
        shipping_line_detail.Qty = 1
        shipping_line_detail.UnitPrice = 20

        # Line
        shipping_line = SalesItemLine()
        shipping_line.SalesItemLineDetail = shipping_line_detail
        shipping_line.Description = f"Order: {order_number or ''} - Shipping Charge"
        shipping_line.Amount = 20

        # Append to the Line list
        invoice.Line.append(shipping_line)

        # Add a subtotal line for the order. This is done by adding a
        # DescriptionLine with a description attribute of "Subtotal:"
        subtotal_line = DescriptionOnlyLine()
        subtotal_line.DescriptionLineDetail = DescriptionLineDetail()
        subtotal_line.DescriptionLineDetail.ServiceDate = helpers.qb_date_format(
            fulfillment_date
        )
        subtotal_line.Description = "Subtotal:"

        # Append to the Line list
        invoice.Line.append(subtotal_line)

    # Add custom field values
    custom_fields = get_enabled_sales_form_custom_fields(qbo_client)

    for k, v in custom_fields.items():

        if v == "Total Orders":

            custom_field = CustomField()
            custom_field.Type = "StringType"
            custom_field.DefinitionId = str(k)
            custom_field.StringValue = str(total_orders)
            invoice.CustomField.append(custom_field)

        elif v == "Total Items":

            custom_field = CustomField()
            custom_field.Type = "StringType"
            custom_field.DefinitionId = str(k)
            custom_field.StringValue = str(total_items)
            invoice.CustomField.append(custom_field)

        elif v == "Billing Period":

            custom_field = CustomField()
            custom_field.Type = "StringType"
            custom_field.DefinitionId = str(k)
            custom_field.StringValue = f"FUL {start_date} to {end_date}"
            invoice.CustomField.append(custom_field)

    # Save invoice to Quickbooks
    if len(invoice.Line) > 0:

        invoice.save(qb=qbo_client.client)
        logger.info(f"Invoice saved to Quickbooks for brand {brand_name}")

        # Save PDF and csv of invoice to Box. Start by finding the PDF and csv
        # folders for the current year and month
        logger.info(f"Generating PDF of invoice {invoice.DocNumber}")
        file_name = f"{brand_code}_fulfilled_{start_date}-{end_date}"

        # Retrieve PDF of invoice. Upload PDF and csv of line items to Box
        invoice_json = get_invoice_by_doc_number(invoice.DocNumber, qbo_client)

        # Create a PDF in memory then send the stream to Box
        invoice_pdf = qbo_client.client.download_pdf(
            qbbo="invoice", item_id=invoice_json["Id"]
        )
        stream = BytesIO(invoice_pdf)

        try:

            box_connector.upload_from_stream(
                box_pdf_folder_id, stream, file_name + ".pdf"
            )
            logger.info(f"Uploaded PDF of invoice {invoice.DocNumber} to Box")

        # BoxAPIExceptions with a message of "file already exists" mean that the
        # file needs to be updated with a newer version instead of created
        except BoxAPIException as e:

            if e.message == "Item with the same name already exists":

                logger.warning(
                    f'Item with name {file_name+".pdf"} already exists; updating'
                    " existing item with a new version"
                )

                # Find the file id for the file with the same name
                file_id = [
                    i.id
                    for i in box_connector.get_folder_items(box_pdf_folder_id)
                    if i.name == (file_name + ".pdf")
                ][0]
                box_connector.upload_new_version_from_stream(file_id, stream)
                logger.info(
                    f"Uploaded new pdf version of invoice {invoice.DocNumber} to Box"
                )

            else:

                raise

        except Exception as e:

            raise

        # Create a csv in memory then send the stream to Box
        logger.info(f"Creating csv of invoice {invoice.DocNumber}")
        csv_rows = []
        for line in invoice_json["Line"]:

            # Ignore non sales lines (subtotals)
            if line["DetailType"] == "SalesItemLineDetail":

                csv_rows.append(
                    {
                        "doc_number": invoice_json["DocNumber"],
                        "transaction_date": invoice_json["TxnDate"],
                        "currency": invoice_json["CurrencyRef"]["value"],
                        "item": line["SalesItemLineDetail"]["ItemRef"]["name"],
                        "description": line["Description"],
                        "service_date": line["SalesItemLineDetail"]["ServiceDate"],
                        "unit_price": line["SalesItemLineDetail"]["UnitPrice"],
                        "quantity": line["SalesItemLineDetail"]["Qty"],
                        "amount": line["Amount"],
                    }
                )

        # Create header row using a row dict's keys
        fieldnames = csv_rows[0].keys()

        # Write to in-memory csv file
        in_memory_csv = StringIO()
        csv_writer = csv.DictWriter(in_memory_csv, fieldnames=fieldnames)
        csv_writer.writeheader()
        csv_writer.writerows(csv_rows)

        # Return to beginning of file
        in_memory_csv.seek(0)

        # Upload to Box
        try:

            box_connector.upload_from_stream(
                box_csv_folder_id, in_memory_csv, file_name + ".csv"
            )
            logger.info(f"Uploaded csv of invoice {invoice.DocNumber} to Box")

        # BoxAPIExceptions with a message of "file already exists" mean that the
        # file needs to be updated with a newer version instead of created
        except BoxAPIException as e:

            if e.message == "Item with the same name already exists":

                logger.warning(
                    f'Item with name {file_name+".csv"} already exists; updating'
                    " existing item with a new version"
                )

                # Find the file id for the file with the same name
                file_id = [
                    i.id
                    for i in box_connector.get_folder_items(box_csv_folder_id)
                    if i.name == (file_name + ".csv")
                ][0]
                box_connector.upload_new_version_from_stream(file_id, in_memory_csv)
                logger.info(
                    f"Uploaded new csv version of invoice {invoice.DocNumber} to Box"
                )

            else:

                raise

        except Exception as e:

            raise

    else:

        logger.warning(
            f"No order line items to invoice for brand {brand_name}. Check"
            "logged errors for potential missing required brand data"
        )


async def generate_invoices(
    brand_dicts: list[str],
    all_line_items: list[dict],
    qbo_client: QuickBooksClient,
    box_connector: BoxConnector,
    start_date: str,
    end_date: str,
    box_pdf_folder_id: str,
    box_csv_folder_id: str,
):

    # It is possible for a record to be missing a style ID, a price, or revenue
    # share. These instances are compiled and logged as well as reported to a
    # Slack channel
    errors = []

    futures = [
        generate_invoice(
            brand_dict,
            all_line_items,
            qbo_client,
            box_connector,
            start_date,
            end_date,
            errors,
            box_pdf_folder_id,
            box_csv_folder_id,
        )
        for brand_dict in brand_dicts
        # Partner brands are booked via journal entries instead of invoices
        if brand_dict["brand_id"] not in JOURNAL_ENTRY_BRANDS
    ]

    await asyncio.gather(*futures)

    # Drop duplicates from list
    errors = pd.Series(errors).drop_duplicates().tolist()

    # Log and report errors
    if len(errors) > 0:

        add_and_alert_uninvoiced_line_items(box_connector, errors)


if (
    __name__ == "__main__"
    and (RES_ENV == "production" or DEV_OVERRIDE)
    and IS_INCLUDED_STEP
):

    # Determine what dates the report will cover
    start_date = sys.argv[3]
    end_date = sys.argv[4]

    # Validate dates
    if (start_date == "last_complete_week" and end_date != "last_complete_week") or (
        start_date != "last_complete_week" and end_date == "last_complete_week"
    ):

        raise TypeError("Cannot combine 'last_complete_week' with a date")

    elif start_date != "last_complete_week" and end_date != "last_complete_week":

        try:

            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")

        except Exception as e:

            logger.error("Improper start or end date format")
            raise

    if start_date == "last_complete_week" or end_date == "last_complete_week":

        start_date, end_date = get_last_complete_week_dates()

    # Connect to Snowflake in order to retrieve data. Use a simple function to
    # make return dict keys lowercase since Snowflake returns them as uppercase
    def low_key(input: list[dict]):

        output = []
        for i in input:

            output.append({k.lower(): v for k, v in i.items()})

        return output

    try:

        snowflake_creds = secrets_client.get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
        conn = snowflake.connector.connect(
            user=snowflake_creds["user"],
            password=snowflake_creds["password"],
            account=snowflake_creds["account"],
            warehouse="loader_wh",
            database="iamcurious_db",
            schema="iamcurious_production_models",
        )
        cur = conn.cursor(DictCursor)

        # Get brands and brand address info from brands table. Exclude brands
        # that were created after the time period. If brand codes were provided
        # to the function use those only
        brand_dicts = cur.execute(SNOWFLAKE_BRANDS.format(end_date=end_date)).fetchall()
        brand_dicts = low_key(brand_dicts)
        target_brands = sys.argv[2] or "all"

        if target_brands != "all":

            target_brands = target_brands.split(",")
            brand_dicts = [
                brand for brand in brand_dicts if brand["brand_code"] in target_brands
            ]

        if len(brand_dicts) == 0:

            raise ValueError(
                "Provided target brand codes do not match any brands in the res.Meta brands table"
            )

        # Retrieve order line items for the time period
        order_line_items = cur.execute(
            SNOWFLAKE_RES_COMPANIES_ORDER_LINE_ITEMS.format(
                start_date=start_date, end_date=end_date
            )
        ).fetchall()
        order_line_items = low_key(order_line_items)

        # Remove brands whose codes aren't found in the order line items data
        order_brand_codes = [i["brand_code"] for i in order_line_items]
        order_brand_codes = pd.Series(order_brand_codes).drop_duplicates().tolist()
        brand_dicts = [i for i in brand_dicts if i["brand_code"] in order_brand_codes]

    except Exception as e:

        logger.error(e)

        raise

    finally:

        cur.close()

    # Create Quickbooks client
    qbo_client = QuickBooksClient("companies")
    qbo_client.refresh()
    logger.info("Refreshed Quickbooks token")

    # Create Box connector
    box_connector = BoxConnector()

    # Validate the target box folder format
    TARGET_FOLDERS = validate_brand_folder_structure(box_connector, str(start_date))
    box_pdf_folder_id = TARGET_FOLDERS["Resonance Companies"]["pdf"]
    box_csv_folder_id = TARGET_FOLDERS["Resonance Companies"]["csv"]

    # Generate invoices asynchronously
    logger.info("Generating invoices")
    asyncio.run(
        generate_invoices(
            brand_dicts,
            order_line_items,
            qbo_client,
            box_connector,
            start_date,
            end_date,
            box_pdf_folder_id,
            box_csv_folder_id,
        )
    )
    logger.info("Invoice generation complete")
