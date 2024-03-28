import asyncio
import os
import sys
import pandas as pd
import snowflake.connector
from datetime import datetime
from snowflake.connector import DictCursor
from quickbooks import helpers
from quickbooks.objects import (
    Account,
    Customer,
    Entity,
    JournalEntry,
    JournalEntryLine,
    JournalEntryLineDetail,
    Vendor,
)

from helper_functions import (
    get_last_complete_week_dates,
    add_and_alert_uninvoiced_line_items,
)
from objects import JOURNAL_ENTRY_BRANDS
from queries import SNOWFLAKE_BRANDS, SNOWFLAKE_RES_COMPANIES_ORDER_LINE_ITEMS

from res.connectors.quickbooks.QuickbooksClient import QuickBooksClient
from res.connectors.box import BoxConnector
from res.utils import logger, secrets_client, ping_slack

# Environment variables and arguments
RES_ENV = os.getenv("RES_ENV", "development").lower()
DEV_OVERRIDE = (sys.argv[1] or "").lower() == "true"
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[5] or sys.argv[5].lower() == "all"

# Constants
RES_COMPANIES_BCOO_ACCOUNT_ID = (
    "27" if RES_ENV == "production" else "1150000007"
)  # CR: 40-4200 Sales of Product Income
RES_COMPANIES_REV_SHARE_ACCOUNT_ID = (
    "394" if RES_ENV == "production" else "1150000005"
)  # CR: 40-4250 Revenue Share
RES_COMPANIES_SHIPPING_ACCOUNT_ID = (
    "178" if RES_ENV == "production" else "1150000006"
)  # CR: 40-4000 Shipping revenue
RES_COMPANIES_INTERCOMPANY_RECEIVABLES_ACCOUNT_ID = (
    "396" if RES_ENV == "production" else "1150000004"
)  # DR: 10-2020 Inter-Company Receivables (Mfg Costs)


async def generate_journal_entry(
    brand_dict: dict,
    all_line_items: list[dict],
    res_companies_qbo_client: QuickBooksClient,
    start_date: str,
    end_date: str,
    errors: list,
):
    """ """

    # Retrieve brand info
    brand_id = brand_dict.get("brand_id")
    brand_name = brand_dict.get("brand_name")
    brand_code = brand_dict.get("brand_code")
    brand_line_items = [i for i in all_line_items if i["brand_code"] == brand_code]
    brand_customer_id = brand_dict.get("quickbooks_companies_id")

    if RES_ENV == "development":

        brand_bcoo_account_id = "91"
        brand_rev_share_account_id = "1150000004"
        brand_shipping_account_id = "1150000005"
        brand_intercompany_res_account_id = "93"
        brand_resonance_vendor_id = "58"

    else:

        brand_bcoo_account_id = JOURNAL_ENTRY_BRANDS[brand_id]["brand_bcoo_account_id"]
        brand_rev_share_account_id = JOURNAL_ENTRY_BRANDS[brand_id][
            "brand_rev_share_account_id"
        ]
        brand_shipping_account_id = JOURNAL_ENTRY_BRANDS[brand_id][
            "brand_shipping_account_id"
        ]
        brand_intercompany_res_account_id = JOURNAL_ENTRY_BRANDS[brand_id][
            "brand_intercompany_res_account_id"
        ]
        brand_resonance_vendor_id = JOURNAL_ENTRY_BRANDS[brand_id][
            "resonance_vendor_id"
        ]

    # Create a QBO client for the brand's account
    logger.info(f"Creating QBO clients and objects for brand {brand_name}")
    brand_qbo_client_name = JOURNAL_ENTRY_BRANDS[brand_id]["name"]
    brand_qbo_client = QuickBooksClient(brand_qbo_client_name)

    # Retrieve all needed account refs
    brand_bcoo_account_ref = Account.get(
        brand_bcoo_account_id, qb=brand_qbo_client.client
    ).to_ref()
    brand_intercompany_account_ref = Account.get(
        brand_intercompany_res_account_id, qb=brand_qbo_client.client
    ).to_ref()
    brand_rev_share_account_ref = Account.get(
        brand_rev_share_account_id, qb=brand_qbo_client.client
    ).to_ref()
    brand_shipping_account_ref = Account.get(
        brand_shipping_account_id, qb=brand_qbo_client.client
    ).to_ref()
    res_bcoo_account_ref = Account.get(
        RES_COMPANIES_BCOO_ACCOUNT_ID, qb=res_companies_qbo_client.client
    ).to_ref()
    res_intercompany_account_ref = Account.get(
        RES_COMPANIES_INTERCOMPANY_RECEIVABLES_ACCOUNT_ID,
        qb=res_companies_qbo_client.client,
    ).to_ref()
    res_rev_share_account_ref = Account.get(
        RES_COMPANIES_REV_SHARE_ACCOUNT_ID, qb=res_companies_qbo_client.client
    ).to_ref()
    res_shipping_account_ref = Account.get(
        RES_COMPANIES_SHIPPING_ACCOUNT_ID, qb=res_companies_qbo_client.client
    ).to_ref()

    # Retrieve needed customer refs. When an account uses Accounts Receivable
    # a customer ref is needed for the EntityRef of a journal entry's line
    # detail. When an account uses Accounts Payable a vendor ref is needed for
    # the EntityRef of a journal entry's line detail
    brand_resonance_vendor_ref = Vendor.get(
        brand_resonance_vendor_id, qb=brand_qbo_client.client
    ).to_ref()

    if brand_customer_id and RES_ENV == "production":

        res_brand_customer_ref = Customer.get(
            brand_customer_id, qb=res_companies_qbo_client.client
        ).to_ref()

    else:

        # If there is not a customer ID for the brand in Airtable or if the
        # code is run in a development environment use a generic customer ref
        generic_customer_id = 53 if RES_ENV == "production" else 58
        res_brand_customer_ref = Customer.get(
            generic_customer_id, qb=res_companies_qbo_client.client
        ).to_ref()

    # Create vendor entity ref
    brand_line_detail_entity = Entity()
    brand_line_detail_entity.Type = "Vendor"
    brand_line_detail_entity.EntityRef = brand_resonance_vendor_ref

    # Create customer entity ref
    res_line_detail_entity = Entity()
    res_line_detail_entity.Type = "Customer"
    res_line_detail_entity.EntityRef = res_brand_customer_ref

    # Order IDs that have been seen are stored in a dict of lists. This is in
    # order to only create a journal entry for an order's shipping charge once
    res_journal_lines_dict = {}
    brand_journal_lines_dict = {}

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
            "app_name": "res_companies_weekly_partner_brand_journal_entries",
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
        if order_id not in res_journal_lines_dict:

            res_journal_lines_dict[order_id] = []

        if order_id not in brand_journal_lines_dict:

            brand_journal_lines_dict[order_id] = []

        # Create the BCOO journal lines for Resonance Companies

        # Debit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Debit"
        line_detail.AccountRef = res_intercompany_account_ref
        line_detail.Entity = res_line_detail_entity

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = total_cost * count
        line.Description = f"{brand_name} Order: {order_number or ''} - {sku} - BCOO"

        # Append to dict list
        res_journal_lines_dict[order_id].append(line)

        # Credit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Credit"
        line_detail.AccountRef = res_bcoo_account_ref

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = total_cost * count
        line.Description = f"{brand_name} Order: {order_number or ''} - {sku} - BCOO"

        # Append to dict list
        res_journal_lines_dict[order_id].append(line)

        # Create the BCOO journal lines for the brand

        # Debit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Debit"
        line_detail.AccountRef = brand_bcoo_account_ref

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = total_cost * count
        line.Description = f"{brand_name} Order: {order_number or ''} - {sku} - BCOO"

        # Append to dict list
        brand_journal_lines_dict[order_id].append(line)

        # Credit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Credit"
        line_detail.AccountRef = brand_intercompany_account_ref
        line_detail.Entity = brand_line_detail_entity

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = total_cost * count
        line.Description = f"{brand_name} Order: {order_number or ''} - {sku} - BCOO"

        # Append to dict list
        brand_journal_lines_dict[order_id].append(line)

        # Create revenue share journal entries for ecom and wholesale orders for
        # brands that have a non-zero revenue share
        if is_ecom_or_wholesale and revenue_share > 0:

            # Create the revenue share journal lines for Resonance Companies

            # Debit

            # Create journal entry line detail
            line_detail = JournalEntryLineDetail()
            line_detail.PostingType = "Debit"
            line_detail.AccountRef = res_intercompany_account_ref
            line_detail.Entity = res_line_detail_entity

            # Create journal entry line
            line = JournalEntryLine()
            line.JournalEntryLineDetail = line_detail
            line.Amount = revenue_share * count
            line.Description = (
                f"{brand_name} Order: {order_number or ''} - {sku} - Rev Share"
            )

            # Append to dict list
            res_journal_lines_dict[order_id].append(line)

            # Credit

            # Create journal entry line detail
            line_detail = JournalEntryLineDetail()
            line_detail.PostingType = "Credit"
            line_detail.AccountRef = res_rev_share_account_ref

            # Create journal entry line
            line = JournalEntryLine()
            line.JournalEntryLineDetail = line_detail
            line.Amount = revenue_share * count
            line.Description = (
                f"{brand_name} Order: {order_number or ''} - {sku} - Rev Share"
            )

            # Append to dict list
            res_journal_lines_dict[order_id].append(line)

            # Create the revenue share journal lines for the brand

            # Debit

            # Create journal entry line detail
            line_detail = JournalEntryLineDetail()
            line_detail.PostingType = "Debit"
            line_detail.AccountRef = brand_rev_share_account_ref

            # Create journal entry line
            line = JournalEntryLine()
            line.JournalEntryLineDetail = line_detail
            line.Amount = revenue_share * count
            line.Description = (
                f"{brand_name} Order: {order_number or ''} - {sku} - Rev Share"
            )

            # Append to dict list
            brand_journal_lines_dict[order_id].append(line)

            # Credit

            # Create journal entry line detail
            line_detail = JournalEntryLineDetail()
            line_detail.PostingType = "Credit"
            line_detail.AccountRef = brand_intercompany_account_ref
            line_detail.Entity = brand_line_detail_entity

            # Create journal entry line
            line = JournalEntryLine()
            line.JournalEntryLineDetail = line_detail
            line.Amount = revenue_share * count
            line.Description = (
                f"{brand_name} Order: {order_number or ''} - {sku} - Rev Share"
            )

            # Append to dict list
            brand_journal_lines_dict[order_id].append(line)

    if len(res_journal_lines_dict) == 0:

        logger.warning(f"No line items to generate for brand {brand_name}")

    for order_id, res_journal_lines in res_journal_lines_dict.items():

        lines = res_journal_lines

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

        # Create the shipping journal lines for Resonance Companies

        # Debit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Debit"
        line_detail.AccountRef = res_intercompany_account_ref
        line_detail.Entity = res_line_detail_entity

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = 20
        line.Description = f"{brand_name} Order: {order_number or ''} - Shipping"

        # Append to journal entry
        lines.append(line)

        # Credit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Credit"
        line_detail.AccountRef = res_shipping_account_ref

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = 20
        line.Description = f"{brand_name} Order: {order_number or ''} - Shipping"

        # Append to journal entry
        lines.append(line)

        # Create the journal entry to contain the order's journal entry lines
        journal_entry = JournalEntry()
        journal_entry.DocNumber = order_id
        journal_entry.TxnDate = helpers.qb_date_format(fulfillment_date)
        journal_entry.Line.extend(lines)

        # Save journal entry for Resonance Companies
        journal_entry.save(qb=res_companies_qbo_client.client)

    if len(res_journal_lines_dict) != 0:

        logger.info(
            f"Journal entries generated in Resonance Companies QBO for brand {brand_name}"
        )

    for order_id, brand_journal_lines in brand_journal_lines_dict.items():

        lines = brand_journal_lines

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

        # Create the shipping journal lines for Resonance Companies

        # Debit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Debit"
        line_detail.AccountRef = brand_shipping_account_ref

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = 20
        line.Description = f"{brand_name} Order: {order_number or ''} - Shipping"

        # Append to journal entry
        lines.append(line)

        # Credit

        # Create journal entry line detail
        line_detail = JournalEntryLineDetail()
        line_detail.PostingType = "Credit"
        line_detail.AccountRef = brand_intercompany_account_ref
        line_detail.Entity = brand_line_detail_entity

        # Create journal entry line
        line = JournalEntryLine()
        line.JournalEntryLineDetail = line_detail
        line.Amount = 20
        line.Description = f"{brand_name} Order: {order_number or ''} - Shipping"

        # Append to journal entry
        lines.append(line)

        # Create the journal entry to contain the order's journal entry lines
        journal_entry = JournalEntry()
        journal_entry.DocNumber = order_id
        journal_entry.TxnDate = helpers.qb_date_format(fulfillment_date)
        journal_entry.Line.extend(lines)

        # Save journal entry
        journal_entry.save(qb=brand_qbo_client.client)

    if len(brand_journal_lines_dict) != 0:

        logger.info(
            f"Journal entries generated in partner brand QBO for brand {brand_name}"
        )


async def generate_journal_entries(
    brand_dicts: list[str],
    all_line_items: list[dict],
    res_companies_qbo_client: QuickBooksClient,
    box_connector: BoxConnector,
    start_date: str,
    end_date: str,
):
    """ """

    # It is possible for a record to be missing a style ID, a price, or revenue
    # share. These instances are compiled and logged as well as reported to a
    # Slack channel
    errors = []

    futures = [
        generate_journal_entry(
            brand_dict,
            all_line_items,
            res_companies_qbo_client,
            start_date,
            end_date,
            errors,
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

        # This step only makes use of partner brands with Quickbooks accounts
        brand_dicts = [i for i in brand_dicts if i["brand_id"] in JOURNAL_ENTRY_BRANDS]

        if len(brand_dicts) == 0:

            raise ValueError(
                "Provided target brand codes do not match any brands in the res.Meta brands table that are booked via journal entries"
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
    res_companies_qbo_client = QuickBooksClient("companies")
    res_companies_qbo_client.refresh()
    logger.info("Refreshed Quickbooks tokens")

    # Create Box connector
    box_connector = BoxConnector()

    # Generate invoices asynchronously
    logger.info("Generating journal entries")
    asyncio.run(
        generate_journal_entries(
            brand_dicts,
            order_line_items,
            res_companies_qbo_client,
            box_connector,
            start_date,
            end_date,
        )
    )
    logger.info("Journal entry generation complete")
