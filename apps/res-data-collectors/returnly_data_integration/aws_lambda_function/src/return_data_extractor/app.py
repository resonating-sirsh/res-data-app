from datetime import datetime
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
import hashlib
import json
import os
import pandas as pd
import requests
import snowflake.connector

# Load and access environmental variables
snowflake_user = os.getenv("SNOWFLAKE_USER")
snowflake_password = os.getenv("SNOWFLAKE_PASSWORD")
snowflake_account = os.getenv("SNOWFLAKE_ACCOUNT")
returnly_api_token = os.getenv("RETURNLY_API_TOKEN")


def lambda_handler(event, context):

    print(f"request: {event}")

    # Record time of invocation (string)
    current_timestamp_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    returnly_auth_headers = {
        "X-Api-Token": returnly_api_token,
        "X-Api-Version": "2020-08",
        "Content-Type": "application/json",
    }

    # Connect to Snowflake
    base_connection = snowflake.connector.connect(
        user=snowflake_user,
        password=snowflake_password,
        account=snowflake_account,
        warehouse="loader_wh",
        database="raw",
        schema="returnly",
    )

    # Fetch latest update date from returns table
    latest_update_ts = (
        base_connection.cursor()
        .execute('select max(updated_at) from "RAW"."RETURNLY"."RETURNS"')
        .fetchone()[0]
    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # Eventbridge schedules are used to invoke this Lambda function. Lambda will
    # note the type of invocation within a JSON payload accessible from within the
    # Lambda function. Incremental syncs are syncs that request records newer than
    # those currently exiting within Snowflake. Full refresh syncs request all
    # available data.

    base_url = "https://api.returnly.com/returns.json?include=return_line_items,shipping_labels,instant_refund_voucher,refunds,repurchases,invoice_items&page=1"

    if event["invocation_type"] == "incremental":

        try:

            print("Fetching latest return record timestamp")

            request_url = f"{base_url}&min_updated_at={latest_update_ts}"

            print(
                f"Returnly incremental sync for return records updated on or after {latest_update_ts}"
            )

        except:

            print("Fetch failed; defaulting to full refresh")

            request_url = base_url

    elif event["invocation_type"] == "full_refresh":

        request_url = base_url

        print("Returnly full refresh sync")

    # Create empty list for return records
    flat_returns = []

    # Create empty list for return record relationships
    flat_return_foreign_keys = []
    flat_return_line_items = []
    flat_shipping_labels = []
    flat_instant_refund_vouchers = []
    flat_refunds = []
    flat_repurchases = []
    flat_invoice_items = []

    # Create API call and parse response
    try:

        return_response = requests.get(url=request_url, headers=returnly_auth_headers)

        return_response.raise_for_status()
        return_data = json.loads(return_response.text)

    except Exception as e:

        print(f"{e.return_response.status_code} {e.return_response.reason}")

    print("Proceeding to flatten records")

    # While loop will gather data from API calls until final page
    while True:

        # Flatten return attributes into returns list
        for return_record in return_data["data"]:

            return_id = return_record["id"]
            return_updated_at = return_record["attributes"]["updated_at"]

            return_id_time_concat = str(return_id) + str(return_updated_at)

            historical_return_id = hashlib.md5(
                f"{return_id_time_concat}".encode("utf-8")
            ).hexdigest()

            flat_returns.append(
                {
                    "historical_return_id": f"{historical_return_id}",
                    "return_id": f"{return_id}",
                    "rma_id": (return_record["attributes"]).get("rma"),
                    "ext_store_id": (return_record["attributes"]).get("ext_store_id"),
                    "shopper_email": (return_record["attributes"]).get("shopper_email"),
                    "currency": (return_record["attributes"]).get("currency"),
                    "status": (return_record["attributes"]).get("status"),
                    "order_shipping_refund": (return_record["attributes"]).get(
                        "order_shipping_refund"
                    ),
                    "return_shipping_quote_usd": (return_record["attributes"]).get(
                        "return_shipping_quote_amount"
                    ),
                    "return_shipping_refund_usd": (return_record["attributes"]).get(
                        "return_shipping_refund_amount"
                    ),
                    "return_shipping_paid_usd": (return_record["attributes"]).get(
                        "return_shipping_paid_amount"
                    ),
                    "subtotal_usd": (return_record["attributes"]).get(
                        "subtotal_amount"
                    ),
                    "estimated_refund_usd": (return_record["attributes"]).get(
                        "estimated_refund_amount"
                    ),
                    "refund_usd": (return_record["attributes"]).get("refund_amount"),
                    "restocking_fee_usd": (return_record["attributes"]).get(
                        "restocking_fee_amount"
                    ),
                    "tax_usd": (return_record["attributes"]).get("tax_amount"),
                    "discount_usd": (return_record["attributes"]).get(
                        "discount_amount"
                    ),
                    "shopper_message": (return_record["attributes"]).get(
                        "shopper_message"
                    ),
                    "merchant_notes": (return_record["attributes"]).get(
                        "merchant_notes"
                    ),
                    "is_exchange": (return_record["attributes"]).get("is_exchange"),
                    "is_gift": (return_record["attributes"]).get("is_gift"),
                    "is_exempt_from_shipping": (return_record["attributes"]).get(
                        "is_exempt_from_shipping"
                    ),
                    "return_label_usd": (return_record["attributes"]).get(
                        "return_label_amount"
                    ),
                    "csr_user_id": (return_record["attributes"]).get("csr_user_id"),
                    "shopper_address_name": (
                        return_record["attributes"]["shopper_address"]
                    ).get("name"),
                    "shopper_address_line1": (
                        return_record["attributes"]["shopper_address"]
                    ).get("line1"),
                    "shopper_address_line2": (
                        return_record["attributes"]["shopper_address"]
                    ).get("line2"),
                    "shopper_address_city": (
                        return_record["attributes"]["shopper_address"]
                    ).get("city"),
                    "shopper_address_state": (
                        return_record["attributes"]["shopper_address"]
                    ).get("state"),
                    "shopper_address_postal_code": (
                        return_record["attributes"]["shopper_address"]
                    ).get("postal_code"),
                    "shopper_address_country_code": (
                        return_record["attributes"]["shopper_address"]
                    ).get("country_code"),
                    "created_at": (return_record["attributes"]).get("created_at"),
                    "updated_at": (return_record["attributes"]).get("updated_at"),
                    "refunded_at": (return_record["attributes"]).get("refunded_at"),
                    "shipping_labels": str(
                        (
                            (return_record["relationships"]).get("shipping_labels", {})
                        ).get("data")
                    ),
                    "return_line_items": str(
                        (
                            (return_record["relationships"]).get(
                                "return_line_items", {}
                            )
                        ).get("data")
                    ),
                    "refunds": str(
                        ((return_record["relationships"]).get("refunds", {})).get(
                            "data"
                        )
                    ),
                }
            )

            # Loop through return relationships and append foreign keys to empty list

            relationships_list = [
                "return_line_items",
                "refunds",
                "instant_refund_voucher",
                "repurchases",
                "invoice_items",
            ]

            for relationship in relationships_list:

                # Skip the relationship if the return record does not have it
                if f"{relationship}" not in return_record.get("relationships", {}):

                    continue

                # Skip the relationship if the relationship dict data is an empty list
                elif (
                    len(return_record["relationships"][f"{relationship}"]["data"]) == 0
                ):

                    continue

                # When relationship dict data is a list, iterate over items
                if (
                    type(return_record["relationships"][f"{relationship}"]["data"])
                    is list
                ):

                    for relationship_item in return_record["relationships"][
                        f"{relationship}"
                    ]["data"]:

                        flat_return_foreign_keys.append(
                            {
                                "historical_return_id": f"{historical_return_id}",
                                "return_id": f"{return_id}",
                                "key_type": relationship_item["type"],
                                "key_value": relationship_item["id"],
                            }
                        )

                # When relationship dict data is a dict, retrieve key info
                if (
                    type(return_record["relationships"][f"{relationship}"]["data"])
                    is dict
                ):

                    flat_return_foreign_keys.append(
                        {
                            "historical_return_id": f"{historical_return_id}",
                            "return_id": f"{return_id}",
                            "key_type": return_record["relationships"][
                                f"{relationship}"
                            ]["data"]["type"],
                            "key_value": return_record["relationships"][
                                f"{relationship}"
                            ]["data"]["id"],
                        }
                    )

        # Flatten included attributes
        for included_record in return_data["included"]:

            included_record_id = str(included_record["id"])
            included_record_historical_id_concat = (
                included_record_id + current_timestamp_utc
            )
            included_record_historical_id = hashlib.md5(
                f"{included_record_historical_id_concat}".encode("utf-8")
            ).hexdigest()

            if included_record["type"] == "return_line_items":

                flat_return_line_items.append(
                    {
                        "historical_return_line_item_id": f"{included_record_historical_id}",
                        "return_line_item_id": included_record["id"],
                        "ext_order_line_item_id": (included_record["attributes"]).get(
                            "ext_order_line_item_id"
                        ),
                        "ext_order_id": (included_record["attributes"]).get(
                            "ext_order_id"
                        ),
                        "ext_order_number": (included_record["attributes"]).get(
                            "ext_order_number"
                        ),
                        "sku": (included_record["attributes"]).get("sku"),
                        "product_upc": (included_record["attributes"]).get(
                            "product_upc"
                        ),
                        "product_name": (included_record["attributes"]).get(
                            "product_name"
                        ),
                        "product_id": (included_record["attributes"]).get("product_id"),
                        "variant_id": (included_record["attributes"]).get("variant_id"),
                        "shipping_label_id": (included_record["attributes"]).get(
                            "shipping_label_id"
                        ),
                        "return_reason": (included_record["attributes"]).get(
                            "return_cause"
                        ),
                        "original_price_usd": (included_record["attributes"]).get(
                            "original_amount"
                        ),
                        "discount_price_usd": (included_record["attributes"]).get(
                            "discount_amount"
                        ),
                        "estimated_refund_usd": (included_record["attributes"]).get(
                            "estimated_refund_amount"
                        ),
                        "restocking_fee_usd": (included_record["attributes"]).get(
                            "restocking_fee_amount"
                        ),
                        "order_shipping_refund_usd": (
                            included_record["attributes"]
                        ).get("order_shipping_refund_amount"),
                        "return_label_cost_usd": (included_record["attributes"]).get(
                            "return_label_cost_amount"
                        ),
                        "return_shipping_paid_usd": (included_record["attributes"]).get(
                            "return_shipping_paid_amount"
                        ),
                        "total_refund_usd": (included_record["attributes"]).get(
                            "total_refund_amount"
                        ),
                        "tax_usd": (included_record["attributes"]).get("tax_amount"),
                        "units": (included_record["attributes"]).get("units"),
                        "return_type": (included_record["attributes"]).get(
                            "return_type"
                        ),
                        "return_location": (included_record["attributes"]).get(
                            "return_location"
                        ),
                        "return_warehouse_tag": (included_record["attributes"]).get(
                            "return_warehouse_tag"
                        ),
                        "return_sub_cause": (included_record["attributes"]).get(
                            "return_sub_cause"
                        ),
                        "is_ror_or_roe": (included_record["attributes"]).get(
                            "is_ror_or_roe"
                        ),
                        "original_return_merchandise_auths": str(
                            (included_record["attributes"]).get("original_rmas")
                        ),
                        "last_retrieved_at": f"{current_timestamp_utc}",
                    }
                )

            elif included_record["type"] == "shipping_labels":

                flat_shipping_labels.append(
                    {
                        "historical_shipping_label_id": f"{included_record_historical_id}",
                        "shipping_label_id": included_record["id"],
                        "carrier": (included_record["attributes"]).get("carrier"),
                        "weight_grams": (included_record["attributes"]).get(
                            "weight_grams"
                        ),
                        "tracking_number": (included_record["attributes"]).get(
                            "tracking_number"
                        ),
                        "pdf_url": (included_record["attributes"]).get("pdf_url"),
                        "last_retrieved_at": f"{current_timestamp_utc}",
                    }
                )

            elif included_record["type"] == "instant_refund_voucher":

                flat_instant_refund_vouchers.append(
                    {
                        "historical_refund_voucher_id": f"{included_record_historical_id}",
                        "instant_refund_voucher_id": included_record["id"],
                        "status": (included_record["attributes"]).get("status"),
                        "credit_issued_usd": (included_record["attributes"]).get(
                            "credit_issued_amount"
                        ),
                        "credit_spent_usd": (included_record["attributes"]).get(
                            "credit_spent_amount"
                        ),
                        "ext_voucher_id": (included_record["attributes"]).get(
                            "ext_voucher_id"
                        ),
                        "refund_to_shopper_usd": (included_record["attributes"]).get(
                            "refund_to_shopper_amount"
                        ),
                        "merchant_invoice_amount_usd": (
                            included_record["attributes"]
                        ).get("merchant_invoice_amount"),
                        "shopper_invoice_amount_usd": (
                            included_record["attributes"]
                        ).get("shopper_invoice_amount"),
                        "last_retrieved_at": f"{current_timestamp_utc}",
                    }
                )

            elif included_record["type"] == "refunds":

                flat_refunds.append(
                    {
                        "historical_refund_id": f"{included_record_historical_id}",
                        "refund_id": included_record["id"],
                        "ext_order_id": (included_record["attributes"]).get(
                            "ext_order_id"
                        ),
                        "ext_order_number": (included_record["attributes"]).get(
                            "ext_order_number"
                        ),
                        "ext_refund_id": (included_record["attributes"]).get(
                            "ext_refund_id"
                        ),
                        "refund_amount": (included_record["attributes"]).get(
                            "refund_amount"
                        ),
                        "created_at": (included_record["attributes"]).get("created_at"),
                        "updated_at": (included_record["attributes"]).get("updated_at"),
                        "refund_target": (included_record["attributes"]).get(
                            "refund_target"
                        ),
                        "ext_voucher_id": (included_record["attributes"]).get(
                            "ext_voucher_id"
                        ),
                    }
                )

            elif included_record["type"] == "repurchases":

                for voucher_repurchase_dict in included_record["attributes"][
                    "voucher_repurchase_details"
                ]:

                    # Repurchases contain multiple vouchers with their own ID. These
                    # must be added to the primary key to keep it unique

                    historical_repurchase_concat = (
                        included_record_historical_id_concat
                        + voucher_repurchase_dict.get("ext_voucher_id", "")
                    )
                    historical_repurchase_id = hashlib.md5(
                        f"{historical_repurchase_concat}".encode("utf-8")
                    ).hexdigest()

                    flat_repurchases.append(
                        {
                            "historical_repurchase_id": f"{historical_repurchase_id}",
                            "repurchase_id": included_record["id"],
                            "repurchase_type": (included_record["attributes"]).get(
                                "repurchase_type"
                            ),
                            "order_id": (included_record["attributes"]).get("order_id"),
                            "order_number": (included_record["attributes"]).get(
                                "order_number"
                            ),
                            "total_credit_spent_usd": (
                                included_record["attributes"]
                            ).get("total_credit_spent_amount"),
                            "total_order_value_usd": (
                                included_record["attributes"]
                            ).get("total_order_amount"),
                            "total_shopper_uplift_charge_usd": (
                                included_record["attributes"]
                            ).get("total_shopper_uplift_amount"),
                            "shopper_uplift_charge_id": (
                                included_record["attributes"]
                            ).get("shopper_uplift_charge_id"),
                            "total_returnly_fee_usd": (
                                included_record["attributes"]
                            ).get("total_returnly_fee_amount"),
                            "processed_at": (included_record["attributes"]).get(
                                "processed_at"
                            ),
                            "commission_adjustment_invoice_id": (
                                included_record["attributes"]
                            ).get("commission_adjustment_invoice_id"),
                            "returnly_payment_id": (included_record["attributes"]).get(
                                "returnly_payment_id"
                            ),
                            "merchant_payment_id": (included_record["attributes"]).get(
                                "merchant_payment_id"
                            ),
                            "ext_voucher_id": voucher_repurchase_dict.get(
                                "ext_voucher_id"
                            ),
                            "credit_spent_usd": voucher_repurchase_dict.get(
                                "credit_spent_amount"
                            ),
                            "remaining_credit_usd": voucher_repurchase_dict.get(
                                "remaining_credit_amount"
                            ),
                            "fee_amount_usd": voucher_repurchase_dict.get("fee_amount"),
                            "gift_card_last_four": voucher_repurchase_dict.get(
                                "gift_card_last4"
                            ),
                            "processed_at": voucher_repurchase_dict.get("processed_at"),
                            "invoice_item_id": voucher_repurchase_dict.get(
                                "invoice_item_id"
                            ),
                            "last_retrieved_at": f"{current_timestamp_utc}",
                        }
                    )

            elif included_record["type"] == "invoice_items":

                flat_invoice_items.append(
                    {
                        "historical_invoice_item_id": f"{included_record_historical_id}",
                        "invoice_item_id": included_record["id"],
                        "invoice_type": (included_record["attributes"]).get(
                            "invoice_type"
                        ),
                        "invoice_amount_usd": (included_record["attributes"]).get(
                            "invoice_amount"
                        ),
                        "invoiced_at": (included_record["attributes"]).get(
                            "invoiced_at"
                        ),
                        "paid_at": (included_record["attributes"]).get("paid_at"),
                        "created_at": (included_record["attributes"]).get("created_at"),
                        "last_retrieved_at": f"{current_timestamp_utc}",
                    }
                )

        # Break look if final page, i.e. next page is null
        if return_data["links"]["next"] == None:

            break

        # Otherwise perform next API call
        return_response = requests.get(
            url=return_data["links"]["next"], headers=returnly_auth_headers
        )

        return_response.raise_for_status()
        return_data = json.loads(return_response.text)

    print("Creating tables in Snowflake if they do not already exist")

    # Create returns table if it doesn't already exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS RETURNS (
                HISTORICAL_RETURN_ID VARCHAR(16777216),
                RETURN_ID INTEGER,
                RMA_ID INTEGER,
                EXT_STORE_ID INTEGER,
                SHOPPER_EMAIL VARCHAR(16777216),
                CURRENCY VARCHAR(16777216),
                STATUS VARCHAR(16777216),
                ORDER_SHIPPING_REFUND VARCHAR(16777216),
                RETURN_SHIPPING_QUOTE_USD NUMBER(38, 2),
                RETURN_SHIPPING_REFUND_USD NUMBER(38, 2),
                RETURN_SHIPPING_PAID_USD NUMBER(38, 2),
                SUBTOTAL_USD NUMBER(38, 2),
                ESTIMATED_REFUND_USD NUMBER(38, 2),
                REFUND_USD NUMBER(38, 2),
                RESTOCKING_FEE_USD NUMBER(38, 2),
                TAX_USD NUMBER(38, 2),
                DISCOUNT_USD NUMBER(38, 2),
                SHOPPER_MESSAGE VARCHAR(16777216),
                MERCHANT_NOTES VARCHAR(16777216),
                IS_EXCHANGE BOOLEAN,
                IS_GIFT BOOLEAN,
                IS_EXEMPT_FROM_SHIPPING BOOLEAN,
                RETURN_LABEL_USD NUMBER(38, 2),
                CSR_USER_ID INTEGER,
                SHOPPER_ADDRESS_NAME VARCHAR(16777216),
                SHOPPER_ADDRESS_LINE1 VARCHAR(16777216),
                SHOPPER_ADDRESS_LINE2 VARCHAR(16777216),
                SHOPPER_ADDRESS_CITY VARCHAR(16777216),
                SHOPPER_ADDRESS_STATE VARCHAR(16777216),
                SHOPPER_ADDRESS_POSTAL_CODE VARCHAR(16777216),
                SHOPPER_ADDRESS_COUNTRY_CODE VARCHAR(16777216),
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ,
                REFUNDED_AT TIMESTAMP_NTZ,
                SHIPPING_LABELS VARCHAR(16777216),
                RETURN_LINE_ITEMS VARCHAR(16777216),
                REFUNDS VARCHAR(16777216)
            );
        """
    )

    # Create return foreign key table if it doesn't already exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS RETURN_FOREIGN_KEYS (
                HISTORICAL_RETURN_ID VARCHAR(16777216),
                RETURN_ID INTEGER,
                KEY_TYPE VARCHAR(256),
                KEY_VALUE VARCHAR(256)
            );
        """
    )

    # Create return line items table if it doesn't already exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS RETURN_LINE_ITEMS (
                HISTORICAL_RETURN_LINE_ITEM_ID VARCHAR(16777216),
                RETURN_LINE_ITEM_ID INTEGER,
                EXT_ORDER_LINE_ITEM_ID INTEGER,
                EXT_ORDER_ID INTEGER,
                EXT_ORDER_NUMBER VARCHAR(16777216),
                SKU VARCHAR(16777216),
                PRODUCT_UPC VARCHAR(16777216),
                PRODUCT_NAME VARCHAR(16777216),
                PRODUCT_ID INTEGER,
                VARIANT_ID INTEGER,
                SHIPPING_LABEL_ID INTEGER,
                RETURN_REASON VARCHAR(16777216),
                ORIGINAL_PRICE_USD NUMBER(38, 2),
                DISCOUNT_PRICE_USD NUMBER(38, 2),
                ESTIMATED_REFUND_USD NUMBER(38, 2),
                RESTOCKING_FEE_USD NUMBER(38, 2),
                ORDER_SHIPPING_REFUND_USD NUMBER(38, 2),
                RETURN_LABEL_COST_USD NUMBER(38, 2),
                RETURN_SHIPPING_PAID_USD NUMBER(38, 2),
                TOTAL_REFUND_USD NUMBER(38, 2),
                TAX_USD NUMBER(38, 2),
                UNITS INTEGER,
                RETURN_TYPE VARCHAR(16777216),
                RETURN_LOCATION VARCHAR(16777216),
                RETURN_WAREHOUSE_TAG VARCHAR(16777216),
                RETURN_SUB_CAUSE VARCHAR(16777216),
                IS_ROR_OR_ROE BOOLEAN,
                ORIGINAL_RETURN_MERCHANDISE_AUTHS VARCHAR(16777216),
                LAST_RETRIEVED_AT TIMESTAMP_NTZ
            );    
        """
    )

    # Create shipping labels table if it doesn't already exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS SHIPPING_LABELS (
                HISTORICAL_SHIPPING_LABEL_ID VARCHAR(16777216),
                SHIPPING_LABEL_ID INTEGER,
                CARRIER VARCHAR(16777216),
                WEIGHT_GRAMS NUMBER(38, 4),
                TRACKING_NUMBER VARCHAR(16777216),
                PDF_URL VARCHAR(16777216),
                LAST_RETRIEVED_AT TIMESTAMP_NTZ
            );    
        """
    )

    # Create instant refund vouchers table if it does not exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS INSTANT_REFUND_VOUCHERS (
                HISTORICAL_REFUND_VOUCHER_ID VARCHAR(16777216),            
                INSTANT_REFUND_VOUCHER_ID VARCHAR(16777216),
                STATUS VARCHAR(16777216),
                CREDIT_ISSUED_USD NUMBER(38, 2),
                CREDIT_SPENT_USD NUMBER(38, 2),
                EXT_VOUCHER_ID VARCHAR(16777216),
                REFUND_TO_SHOPPER_USD NUMBER(38, 2),
                MERCHANT_INVOICE_AMOUNT_USD NUMBER(38, 2),
                SHOPPER_INVOICE_AMOUNT_USD NUMBER(38, 2),
                LAST_RETRIEVED_AT TIMESTAMP_NTZ
            );    
        """
    )

    # Create refunds table if it does not exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS REFUNDS (
                HISTORICAL_REFUND_ID VARCHAR(16777216),
                REFUND_ID INTEGER,
                EXT_ORDER_ID VARCHAR(16777216),
                EXT_ORDER_NUMBER VARCHAR(16777216),
                EXT_REFUND_ID INTEGER,
                REFUND_AMOUNT NUMBER(38, 2),
                CREATED_AT TIMESTAMP_NTZ,
                UPDATED_AT TIMESTAMP_NTZ,
                REFUND_TARGET VARCHAR(16777216),
                EXT_VOUCHER_ID INTEGER
            );    
        """
    )

    # Create repurchases table if it does not exist
    base_connection.cursor().execute(
        """
            CREATE TABLE IF NOT EXISTS REPURCHASES (
                HISTORICAL_REPURCHASE_ID VARCHAR(16777216),
                REPURCHASE_ID INTEGER,
                REPURCHASE_TYPE VARCHAR(16777216),
                ORDER_ID INTEGER,
                ORDER_NUMBER VARCHAR(16777216),
                TOTAL_CREDIT_SPENT_USD NUMBER(38, 2),
                TOTAL_ORDER_VALUE_USD NUMBER(38, 2),
                TOTAL_SHOPPER_UPLIFT_CHARGE_USD NUMBER(38, 2),
                SHOPPER_UPLIFT_CHARGE_ID VARCHAR(16777216),
                TOTAL_RETURNLY_FEE_USD NUMBER(38, 2),
                PROCESSED_AT TIMESTAMP_NTZ,
                COMMISSION_ADJUSTMENT_INVOICE_ID VARCHAR(16777216),
                RETURNLY_PAYMENT_ID VARCHAR(16777216),
                MERCHANT_PAYMENT_ID VARCHAR(16777216),
                EXT_VOUCHER_ID INTEGER,
                CREDIT_SPENT_USD NUMBER(38, 2),
                REMAINING_CREDIT_USD NUMBER(38, 2),
                FEE_AMOUNT_USD NUMBER(38, 2),
                GIFT_CARD_LAST_FOUR VARCHAR(16777216),
                INVOICE_ITEM_ID VARCHAR(16777216),
                LAST_RETRIEVED_AT TIMESTAMP_NTZ
            );    
        """
    )

    # Create invoice items table if it does not exist. Empty at time of function
    # creation; skip if flattened list is empty
    if len(flat_invoice_items) > 0:

        base_connection.cursor().execute(
            """    
                CREATE TABLE IF NOT EXISTS INVOICE_ITEMS (
                    HISTORICAL_INVOICE_ITEM_ID VARCHAR(16777216),
                    INVOICE_ITEM_ID VARCHAR(16777216),                
                    INVOICE_TYPE VARCHAR(16777216),
                    INVOICE_AMOUNT_USD NUMBER(38, 2),
                    INVOICED_AT TIMESTAMP_NTZ,
                    PAID_AT TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ,
                    LAST_RETRIEVED_AT TIMESTAMP_NTZ
                );        
            """
        )

    # Close base connection
    base_connection.close()
    print("Snowflake for python connection closed")

    # Create a SQLAlchemy connection to the Snowflake database
    print("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/raw/returnly?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create function for creating dataframes and inserting into Snowflake
    def dict_to_sql(input_dict_list, stage_table_name):

        if len(input_dict_list) > 0:

            df = pd.DataFrame.from_dict(input_dict_list)
            df.to_sql(
                stage_table_name,
                engine,
                index=False,
                chunksize=5000,
                if_exists="replace",
            )

        else:
            pass

    dict_to_sql(flat_returns, "returns_stage")
    dict_to_sql(flat_return_foreign_keys, "return_foreign_keys_stage")
    dict_to_sql(flat_return_line_items, "return_line_items_stage")
    dict_to_sql(flat_shipping_labels, "shipping_labels_stage")
    dict_to_sql(flat_instant_refund_vouchers, "instant_refund_vouchers_stage")
    dict_to_sql(flat_refunds, "refunds_stage")
    dict_to_sql(flat_repurchases, "repurchases_stage")
    dict_to_sql(flat_invoice_items, "invoice_items_stage")

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    print("Merging data from each stage table to corresponding target table")

    # Create stage and target tables
    if len(flat_returns) > 0:
        returns_stage_table = meta.tables["returns_stage"]
        returns_target_table = meta.tables["returns"]

    if len(flat_return_foreign_keys) > 0:
        return_foreign_keys_stage_table = meta.tables["return_foreign_keys_stage"]
        return_foreign_keys_target_table = meta.tables["return_foreign_keys"]

    if len(flat_return_line_items) > 0:
        return_line_items_stage_table = meta.tables["return_line_items_stage"]
        return_line_items_target_table = meta.tables["return_line_items"]

    if len(flat_shipping_labels) > 0:
        shipping_labels_stage_table = meta.tables["shipping_labels_stage"]
        shipping_labels_target_table = meta.tables["shipping_labels"]

    if len(flat_instant_refund_vouchers) > 0:
        instant_refund_vouchers_stage_table = meta.tables[
            "instant_refund_vouchers_stage"
        ]
        instant_refund_vouchers_target_table = meta.tables["instant_refund_vouchers"]

    if len(flat_refunds) > 0:
        refunds_stage_table = meta.tables["refunds_stage"]
        refunds_target_table = meta.tables["refunds"]

    if len(flat_repurchases) > 0:
        repurchases_stage_table = meta.tables["repurchases_stage"]
        repurchases_target_table = meta.tables["repurchases"]

    if len(flat_invoice_items) > 0:
        invoice_items_stage_table = meta.tables["invoice_items_stage"]
        invoice_items_target_table = meta.tables["invoice_items"]

    # Construct merges and execute

    # Returns
    if len(flat_returns) > 0:

        returns_merge = MergeInto(
            target=returns_target_table,
            source=returns_stage_table,
            on=returns_target_table.c.historical_return_id
            == returns_stage_table.c.historical_return_id,
        )

        returns_merge.when_not_matched_then_insert().values(
            historical_return_id=returns_stage_table.c.historical_return_id,
            return_id=returns_stage_table.c.return_id,
            rma_id=returns_stage_table.c.rma_id,
            ext_store_id=returns_stage_table.c.ext_store_id,
            shopper_email=returns_stage_table.c.shopper_email,
            currency=returns_stage_table.c.currency,
            status=returns_stage_table.c.status,
            order_shipping_refund=returns_stage_table.c.order_shipping_refund,
            return_shipping_quote_usd=returns_stage_table.c.return_shipping_quote_usd,
            return_shipping_refund_usd=returns_stage_table.c.return_shipping_refund_usd,
            return_shipping_paid_usd=returns_stage_table.c.return_shipping_paid_usd,
            subtotal_usd=returns_stage_table.c.subtotal_usd,
            estimated_refund_usd=returns_stage_table.c.estimated_refund_usd,
            refund_usd=returns_stage_table.c.refund_usd,
            restocking_fee_usd=returns_stage_table.c.restocking_fee_usd,
            tax_usd=returns_stage_table.c.tax_usd,
            discount_usd=returns_stage_table.c.discount_usd,
            shopper_message=returns_stage_table.c.shopper_message,
            merchant_notes=returns_stage_table.c.merchant_notes,
            is_exchange=returns_stage_table.c.is_exchange,
            is_gift=returns_stage_table.c.is_gift,
            is_exempt_from_shipping=returns_stage_table.c.is_exempt_from_shipping,
            return_label_usd=returns_stage_table.c.return_label_usd,
            csr_user_id=returns_stage_table.c.csr_user_id,
            shopper_address_name=returns_stage_table.c.shopper_address_name,
            shopper_address_line1=returns_stage_table.c.shopper_address_line1,
            shopper_address_line2=returns_stage_table.c.shopper_address_line2,
            shopper_address_city=returns_stage_table.c.shopper_address_city,
            shopper_address_state=returns_stage_table.c.shopper_address_state,
            shopper_address_postal_code=returns_stage_table.c.shopper_address_postal_code,
            shopper_address_country_code=returns_stage_table.c.shopper_address_country_code,
            created_at=returns_stage_table.c.created_at,
            updated_at=returns_stage_table.c.updated_at,
            refunded_at=returns_stage_table.c.refunded_at,
            shipping_labels=returns_stage_table.c.shipping_labels,
            return_line_items=returns_stage_table.c.return_line_items,
            refunds=returns_stage_table.c.refunds,
        )

        alchemy_connection.execute(returns_merge)

    # Return Foreign Keys
    if len(flat_return_foreign_keys) > 0:

        return_foreign_keys_merge = MergeInto(
            target=return_foreign_keys_target_table,
            source=return_foreign_keys_stage_table,
            on=return_foreign_keys_target_table.c.historical_return_id
            == return_foreign_keys_stage_table.c.historical_return_id
            and return_foreign_keys_target_table.c.return_id
            == return_foreign_keys_stage_table.c.return_id
            and return_foreign_keys_target_table.c.key_type
            == return_foreign_keys_stage_table.c.key_type
            and return_foreign_keys_target_table.c.key_value
            == return_foreign_keys_stage_table.c.key_value,
        )

        return_foreign_keys_merge.when_not_matched_then_insert().values(
            historical_return_id=return_foreign_keys_stage_table.c.historical_return_id,
            return_id=return_foreign_keys_stage_table.c.return_id,
            key_type=return_foreign_keys_stage_table.c.key_type,
            key_value=return_foreign_keys_stage_table.c.key_value,
        )

        alchemy_connection.execute(return_foreign_keys_merge)

    # Return Line Items
    if len(flat_return_line_items) > 0:

        return_line_items_merge = MergeInto(
            target=return_line_items_target_table,
            source=return_line_items_stage_table,
            on=return_line_items_target_table.c.historical_return_line_item_id
            == return_line_items_stage_table.c.historical_return_line_item_id,
        )

        return_line_items_merge.when_not_matched_then_insert().values(
            historical_return_line_item_id=return_line_items_stage_table.c.historical_return_line_item_id,
            return_line_item_id=return_line_items_stage_table.c.return_line_item_id,
            ext_order_line_item_id=return_line_items_stage_table.c.ext_order_line_item_id,
            ext_order_id=return_line_items_stage_table.c.ext_order_id,
            ext_order_number=return_line_items_stage_table.c.ext_order_number,
            sku=return_line_items_stage_table.c.sku,
            product_upc=return_line_items_stage_table.c.product_upc,
            product_name=return_line_items_stage_table.c.product_name,
            product_id=return_line_items_stage_table.c.product_id,
            variant_id=return_line_items_stage_table.c.variant_id,
            shipping_label_id=return_line_items_stage_table.c.shipping_label_id,
            return_reason=return_line_items_stage_table.c.return_reason,
            original_price_usd=return_line_items_stage_table.c.original_price_usd,
            discount_price_usd=return_line_items_stage_table.c.discount_price_usd,
            estimated_refund_usd=return_line_items_stage_table.c.estimated_refund_usd,
            restocking_fee_usd=return_line_items_stage_table.c.restocking_fee_usd,
            order_shipping_refund_usd=return_line_items_stage_table.c.order_shipping_refund_usd,
            return_label_cost_usd=return_line_items_stage_table.c.return_label_cost_usd,
            return_shipping_paid_usd=return_line_items_stage_table.c.return_shipping_paid_usd,
            total_refund_usd=return_line_items_stage_table.c.total_refund_usd,
            tax_usd=return_line_items_stage_table.c.tax_usd,
            units=return_line_items_stage_table.c.units,
            return_type=return_line_items_stage_table.c.return_type,
            return_location=return_line_items_stage_table.c.return_location,
            return_warehouse_tag=return_line_items_stage_table.c.return_warehouse_tag,
            return_sub_cause=return_line_items_stage_table.c.return_sub_cause,
            is_ror_or_roe=return_line_items_stage_table.c.is_ror_or_roe,
            original_return_merchandise_auths=return_line_items_stage_table.c.original_return_merchandise_auths,
            last_retrieved_at=return_line_items_stage_table.c.last_retrieved_at,
        )

        alchemy_connection.execute(return_line_items_merge)

    # Shipping Labels
    if len(flat_shipping_labels) > 0:

        shipping_labels_merge = MergeInto(
            target=shipping_labels_target_table,
            source=shipping_labels_stage_table,
            on=shipping_labels_target_table.c.historical_shipping_label_id
            == shipping_labels_stage_table.c.historical_shipping_label_id,
        )

        shipping_labels_merge.when_not_matched_then_insert().values(
            historical_shipping_label_id=shipping_labels_stage_table.c.historical_shipping_label_id,
            shipping_label_id=shipping_labels_stage_table.c.shipping_label_id,
            carrier=shipping_labels_stage_table.c.carrier,
            weight_grams=shipping_labels_stage_table.c.weight_grams,
            tracking_number=shipping_labels_stage_table.c.tracking_number,
            pdf_url=shipping_labels_stage_table.c.pdf_url,
            last_retrieved_at=shipping_labels_stage_table.c.last_retrieved_at,
        )

        alchemy_connection.execute(shipping_labels_merge)

    # Instant Refund Vouchers
    if len(flat_instant_refund_vouchers) > 0:

        instant_refund_vouchers_merge = MergeInto(
            target=instant_refund_vouchers_target_table,
            source=instant_refund_vouchers_stage_table,
            on=instant_refund_vouchers_target_table.c.historical_refund_voucher_id
            == instant_refund_vouchers_stage_table.c.historical_refund_voucher_id,
        )

        instant_refund_vouchers_merge.when_not_matched_then_insert().values(
            historical_refund_voucher_id=instant_refund_vouchers_stage_table.c.historical_refund_voucher_id,
            instant_refund_voucher_id=instant_refund_vouchers_stage_table.c.instant_refund_voucher_id,
            status=instant_refund_vouchers_stage_table.c.status,
            credit_issued_usd=instant_refund_vouchers_stage_table.c.credit_issued_usd,
            credit_spent_usd=instant_refund_vouchers_stage_table.c.credit_spent_usd,
            ext_voucher_id=instant_refund_vouchers_stage_table.c.ext_voucher_id,
            refund_to_shopper_usd=instant_refund_vouchers_stage_table.c.refund_to_shopper_usd,
            merchant_invoice_amount_usd=instant_refund_vouchers_stage_table.c.merchant_invoice_amount_usd,
            shopper_invoice_amount_usd=instant_refund_vouchers_stage_table.c.shopper_invoice_amount_usd,
            last_retrieved_at=instant_refund_vouchers_stage_table.c.last_retrieved_at,
        )

        alchemy_connection.execute(instant_refund_vouchers_merge)

    # Refunds
    if len(flat_refunds) > 0:

        refunds_merge = MergeInto(
            target=refunds_target_table,
            source=refunds_stage_table,
            on=refunds_target_table.c.historical_refund_id
            == refunds_stage_table.c.historical_refund_id,
        )

        refunds_merge.when_not_matched_then_insert().values(
            historical_refund_id=refunds_stage_table.c.historical_refund_id,
            refund_id=refunds_stage_table.c.refund_id,
            ext_order_id=refunds_stage_table.c.ext_order_id,
            ext_order_number=refunds_stage_table.c.ext_order_number,
            ext_refund_id=refunds_stage_table.c.ext_refund_id,
            refund_amount=refunds_stage_table.c.refund_amount,
            created_at=refunds_stage_table.c.created_at,
            updated_at=refunds_stage_table.c.updated_at,
            refund_target=refunds_stage_table.c.refund_target,
            ext_voucher_id=refunds_stage_table.c.ext_voucher_id,
        )

        alchemy_connection.execute(refunds_merge)

    # Repurchases
    if len(flat_repurchases) > 0:

        repurchases_merge = MergeInto(
            target=repurchases_target_table,
            source=repurchases_stage_table,
            on=repurchases_target_table.c.historical_repurchase_id
            == repurchases_stage_table.c.historical_repurchase_id,
        )

        repurchases_merge.when_not_matched_then_insert().values(
            historical_repurchase_id=repurchases_stage_table.c.historical_repurchase_id,
            repurchase_id=repurchases_stage_table.c.repurchase_id,
            repurchase_type=repurchases_stage_table.c.repurchase_type,
            order_id=repurchases_stage_table.c.order_id,
            order_number=repurchases_stage_table.c.order_number,
            total_credit_spent_usd=repurchases_stage_table.c.total_credit_spent_usd,
            total_order_value_usd=repurchases_stage_table.c.total_order_value_usd,
            total_shopper_uplift_charge_usd=repurchases_stage_table.c.total_shopper_uplift_charge_usd,
            shopper_uplift_charge_id=repurchases_stage_table.c.shopper_uplift_charge_id,
            total_returnly_fee_usd=repurchases_stage_table.c.total_returnly_fee_usd,
            processed_at=repurchases_stage_table.c.processed_at,
            commission_adjustment_invoice_id=repurchases_stage_table.c.commission_adjustment_invoice_id,
            returnly_payment_id=repurchases_stage_table.c.returnly_payment_id,
            merchant_payment_id=repurchases_stage_table.c.merchant_payment_id,
            ext_voucher_id=repurchases_stage_table.c.ext_voucher_id,
            credit_spent_usd=repurchases_stage_table.c.credit_spent_usd,
            remaining_credit_usd=repurchases_stage_table.c.remaining_credit_usd,
            fee_amount_usd=repurchases_stage_table.c.fee_amount_usd,
            gift_card_last_four=repurchases_stage_table.c.gift_card_last_four,
            invoice_item_id=repurchases_stage_table.c.invoice_item_id,
            last_retrieved_at=repurchases_stage_table.c.last_retrieved_at,
        )

        alchemy_connection.execute(repurchases_merge)

    # Invoice Items
    if len(flat_invoice_items) > 0:

        invoice_items_merge = MergeInto(
            target=invoice_items_target_table,
            source=invoice_items_stage_table,
            on=invoice_items_target_table.c.historical_invoice_item_id
            == invoice_items_stage_table.c.historical_invoice_item_id,
        )

        invoice_items_merge.when_not_matched_then_insert().values(
            historical_invoice_item_id=invoice_items_stage_table.c.historical_invoice_item_id,
            invoice_item_id=invoice_items_stage_table.c.invoice_item_id,
            invoice_type=invoice_items_stage_table.c.invoice_type,
            invoice_amount_usd=invoice_items_stage_table.c.invoice_amount_usd,
            invoiced_at=invoice_items_stage_table.c.invoiced_at,
            paid_at=invoice_items_stage_table.c.paid_at,
            created_at=invoice_items_stage_table.c.created_at,
            last_retrieved_at=invoice_items_stage_table.c.last_retrieved_at,
        )

        alchemy_connection.execute(invoice_items_merge)

    # Close connection
    alchemy_connection.close()
    print("SQLAlchemy connection closed")

    response = "Function complete"

    return response
