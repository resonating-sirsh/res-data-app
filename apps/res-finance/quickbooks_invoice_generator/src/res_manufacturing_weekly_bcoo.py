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
    low_key,
    validate_brand_folder_structure,
    get_last_complete_week_dates,
    add_and_alert_uninvoiced_line_items,
)
from objects import SALES_CHANNEL_ITEM_IDS
from queries import SNOWFLAKE_BRANDS, SNOWFLAKE_RES_MANUFACTURING_ORDER_LINE_ITEMS

from res.connectors.quickbooks.QuickbooksClient import QuickBooksClient
from res.connectors.box import BoxConnector
from res.utils import logger, secrets_client, ping_slack

# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
DEV_OVERRIDE = (sys.argv[1] or "").lower() == "true"
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[5] or sys.argv[5].lower() == "all"


# Constants
BOX_BRANDS_FOLDER_ID = "143847228389"
BOX_MANUFACTURING_FOLDER_ID = "153097979670"
QBO_SHIPPING_ACCRUAL_CUSTOMER_ID = "378"


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

    # Add customer attributes to the invoice. For Resonance Manufacturing the
    # customer is always Resonance Companies
    if RES_ENV == "production":

        res_companies_qb_customer_id = 1398

    elif RES_ENV == "development":

        res_companies_qb_customer_id = 59

    # Add customer, memo, and dates
    invoice.CustomerRef = Customer.get(
        res_companies_qb_customer_id, qb=qbo_client.client
    ).to_ref()
    invoice.CustomerMemo = CustomerMemo()
    invoice.CustomerMemo.value = (
        f"Invoice for {brand_name} ({brand_code}) orders fulfilled from"
        f" {start_date} to {end_date}\n"
        "\n"
        "REMIT TO:\n"
        "\n"
        "Resonance Manufacturing\n"
        "Bank: Banco Multiple BHD\n"
        "Av. 27 De Febrero, Esq. A.V. Winston Churchill\n"
        "Santo Domingo de Guzman, DO-01 10214\n"
        "Account No.: 28430180020\n"
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
    # reference Item objects. We can omit the Item for this exercise since all
    # of these are services.
    invoice_lines_dict = {}

    for order_line_item in brand_line_items:

        order_line_item_ids = order_line_item["order_line_item_ids"]
        order_id = order_line_item["order_id"]
        order_number = order_line_item["order_number"]
        channel = order_line_item["channel"]
        sku = order_line_item["sku"]
        style_id = order_line_item["style_id"]
        count = order_line_item["count"]
        brand_cost_of_one = order_line_item["total_cost"]
        fulfillment_shipping_cost = order_line_item["fulfillment_shipping_cost"]
        is_in_airtable = order_line_item["is_in_airtable"]
        is_fulfilled_from_nyc = order_line_item["is_fulfilled_from_nyc"]
        has_tracking_number = order_line_item["has_tracking_number"]

        # Check if the order line item is missing either the style ID, BCOO,
        # or shipping cost. Log these instances and continue. Snowflake will
        # add null values as the dict value, so using keys will still return
        # None
        error = {
            "app_name": "res_manufacturing_weekly_bcoo",
            "order_line_item_ids": order_line_item_ids,
            "order_id": order_id,
            "order_number": order_number,
            "sku": sku,
            "is_in_airtable": is_in_airtable,
        }

        if not style_id:

            error["error"] = "Missing style ID"
            errors.append(error)

        if brand_cost_of_one == 0:

            error["error"] = f"Missing style {style_id} costs"
            errors.append(error)

        # Shipping charges are only considered for items  with tracking numbers
        # fulfilled from somewhere other than the NYC warehouse
        if (
            not fulfillment_shipping_cost
            and not is_fulfilled_from_nyc
            and has_tracking_number
        ):

            error["error"] = "Missing fulfillment shipping cost"
            errors.append(error)

        if any(
            [
                not style_id,
                brand_cost_of_one == 0,
                not fulfillment_shipping_cost
                and not is_fulfilled_from_nyc
                and has_tracking_number,
            ]
        ):

            continue

        # Check if the order is already present in the invoice_lines_dict and
        # add it if it is not. Data is sorted in the Snowflake query so line
        # items in the same order should be sequential. Lines are grouped by
        # order to add subtotal rows
        if order_id not in invoice_lines_dict:

            invoice_lines_dict[order_id] = []

        # Retrieve the item corresponding to the sales channel
        item_id = SALES_CHANNEL_ITEM_IDS["resonance_manufacturing"][
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
        bcoo_line_detail.UnitPrice = brand_cost_of_one

        # Create the line object for BCOO
        bcoo_line = SalesItemLine()
        bcoo_line.SalesItemLineDetail = bcoo_line_detail
        bcoo_line.Amount = brand_cost_of_one * count
        bcoo_line.Description = f"Order: {order_number or ''} - {sku} - BCOO"

        # Append lines to dict list
        invoice_lines_dict[order_id].append(bcoo_line)

        if not is_fulfilled_from_nyc and has_tracking_number:

            # Retrieve the item for shipping
            item_id = SALES_CHANNEL_ITEM_IDS["resonance_manufacturing"]["shipping"][
                RES_ENV
            ]
            item_ref = Item.get(item_id, qb=qbo_client.client).to_ref()

            # Create the detail object for fulfillment shipping cost
            shipping_line_detail = SalesItemLineDetail()
            shipping_line_detail.ItemRef = item_ref
            shipping_line_detail.ServiceDate = helpers.qb_date_format(
                order_line_item["fulfillment_completed_date"]
            )
            shipping_line_detail.Qty = count
            shipping_line_detail.UnitPrice = fulfillment_shipping_cost

            # Create the line object for fulfillment shipping cost
            shipping_line = SalesItemLine()
            shipping_line.SalesItemLineDetail = shipping_line_detail
            shipping_line.Amount = fulfillment_shipping_cost * count
            shipping_line.Description = (
                f"Order: {order_number or ''} - {sku} - Shipping Cost"
            )

            # Append lines to dict list
            invoice_lines_dict[order_id].append(shipping_line)

    # Add lines to invoice with a subtotal line for each order. Maintain a count
    # of orders and order line items by counting order_id and BCOO lines
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
            " logged errors for potential missing required brand data"
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
            SNOWFLAKE_RES_MANUFACTURING_ORDER_LINE_ITEMS.format(
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
    qbo_client = QuickBooksClient("manufacturing")
    qbo_client.refresh()
    logger.info("Refreshed Quickbooks token")

    # Create Box connector
    box_connector = BoxConnector()

    # Validate the target box folder format
    TARGET_FOLDERS = validate_brand_folder_structure(box_connector, str(start_date))
    box_pdf_folder_id = TARGET_FOLDERS["Resonance Manufacturing"]["pdf"]
    box_csv_folder_id = TARGET_FOLDERS["Resonance Manufacturing"]["csv"]

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
