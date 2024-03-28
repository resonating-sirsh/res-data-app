import datetime
import gc
import hashlib
import os
import sys
import asyncio
from datetime import datetime, timedelta
from helper_functions import (
    fetch_all_data,
    traverse_nested_dict,
    safe_strptime,
)
from res.utils import logger, secrets_client
from res.connectors.snowflake.SnowflakeConnector import SnowflakeConnector


# Create synchronous function to flatten info
def flatten_records(output_list):

    logger.info(f"Proceeding to flatten records")

    for order in output_list:

        # Using non-safe get; important to raise Exceptions here
        shop_id = order["shop_id"]
        shop_name = order["shop_name"]
        order_id = order["id"]
        shop_order_concat = str(shop_id) + str(order_id)
        shop_order_id = hashlib.md5(shop_order_concat.encode("utf-8")).hexdigest()
        historical_shop_order_concat = (
            str(shop_id) + str(order_id) + str(order.get("updated_at"))
        )
        historical_shop_order_id = hashlib.md5(
            historical_shop_order_concat.encode("utf-8")
        ).hexdigest()

        # Nested dicts
        client_details = order.get("client_details") or {}
        billing_address = order.get("billing_address") or {}
        shipping_address = order.get("shipping_address") or {}
        current_total_duties = traverse_nested_dict(
            order, ["current_total_duties_set", "shop_money", "amount"]
        )
        total_shipping_price = traverse_nested_dict(
            order, ["total_shipping_price_set", "shop_money", "amount"]
        )

        flat_orders.append(
            {
                "historical_shop_order_id": f"{historical_shop_order_id}",
                "shop_order_id": f"{shop_order_id}",
                "shop_id": shop_id,
                "shop_name": f"{shop_name}",
                "order_id": f"{order_id}",
                "admin_graphql_api_id": order.get("admin_graphql_api_id"),
                "app_id": order.get("app_id"),
                "browser_ip": order.get("browser_ip"),
                "is_buyer_accepts_marketing": order.get("buyer_accepts_marketing"),
                "cancel_reason": order.get("cancel_reason"),
                "cancelled_at": safe_strptime(order.get("cancelled_at")),
                "cart_token": order.get("cart_token"),
                "checkout_id": order.get("checkout_id"),
                "checkout_token": order.get("checkout_token"),
                "client_accepted_language": client_details.get("accept_language"),
                "client_browser_height": client_details.get("browser_height"),
                "client_browser_ip": client_details.get("browser_ip"),
                "client_browser_width": client_details.get("browser_width"),
                "client_session_hash": client_details.get("session_hash"),
                "client_user_agent": client_details.get("user_agent"),
                "closed_at": safe_strptime(order.get("closed_at")),
                "is_confirmed": order.get("confirmed"),
                "contact_email": order.get("contact_email"),
                "created_at": safe_strptime(order.get("created_at")),
                "shop_currency": order.get("currency"),
                "current_subtotal_price": order.get("current_subtotal_price"),
                "current_total_discounts": order.get("current_total_discounts"),
                "current_total_duties": current_total_duties,
                "current_total_price": order.get("current_total_price"),
                "current_total_tax": order.get("current_total_tax"),
                "customer_locale": order.get("customer_locale"),
                "device_id": order.get("device_id"),
                "discount_applications": str(order.get("discount_applications")),
                "discount_codes": str(order.get("discount_codes")),
                "email": order.get("email"),
                "has_estimated_taxes": order.get("estimated_taxes"),
                "financial_status": order.get("financial_status"),
                "fulfillment_status": order.get("fulfillment_status"),
                "gateway": order.get("gateway"),
                "landing_site": order.get("landing_site"),
                "landing_site_ref": order.get("landing_site_ref"),
                "location_id": order.get("location_id"),
                "merchant_of_record_app_id": order.get("merchant_of_record_app_id"),
                "name": order.get("name"),
                "note": str(order.get("note")),
                "note_attributes": str(order.get("note_attributes")),
                "shop_order_sequence_number": order.get("number"),
                "order_number": order.get("order_number"),
                "order_status_url": order.get("order_status_url"),
                "original_total_duties_set": order.get("original_total_duties_set"),
                "payment_terms": str(order.get("payment_terms")),
                "phone": order.get("phone"),
                "presentment_currency": order.get("presentment_currency"),
                "processed_at": safe_strptime(order.get("processed_at")),
                "processing_method": order.get("processing_method"),
                "reference": order.get("reference"),
                "referring_site": order.get("referring_site"),
                "source_identifier": order.get("source_identifier"),
                "source_name": order.get("source_name"),
                "source_url": order.get("source_url"),
                "subtotal_price": order.get("subtotal_price"),
                "tags": order.get("tags"),
                "tax_lines": str(order.get("tax_lines")),
                "is_taxes_included": order.get("taxes_included"),
                "is_test": order.get("test"),
                "token": order.get("token"),
                "total_discounts": order.get("total_discounts"),
                "total_line_items_price": order.get("total_line_items_price"),
                "total_outstanding": order.get("total_outstanding"),
                "total_price": order.get("total_price"),
                "total_shipping_price": total_shipping_price,
                "total_tax": order.get("total_tax"),
                "total_tip_received": order.get("total_tip_received"),
                "total_weight_grams": order.get("total_weight"),
                "updated_at": safe_strptime(order.get("updated_at")),
                "user_id": order.get("user_id"),
                "billing_address_one": billing_address.get("address1"),
                "billing_phone": billing_address.get("phone"),
                "billing_city": billing_address.get("city"),
                "billing_zip": billing_address.get("zip"),
                "billing_province": billing_address.get("province"),
                "billing_country": billing_address.get("country"),
                "billing_last_name": billing_address.get("last_name"),
                "billing_address_two": billing_address.get("address2"),
                "billing_company": billing_address.get("company"),
                "billing_latitude": billing_address.get("latitude"),
                "billing_longitude": billing_address.get("longitude"),
                "billing_name": billing_address.get("name"),
                "billing_country_code": billing_address.get("country_code"),
                "billing_province_code": billing_address.get("province_code"),
                "customer_id": (order.get("customer") or {}).get("id"),
                "discount_applications": str(order.get("discount_applications")),
                "shipping_address_one": shipping_address.get("address1"),
                "shipping_phone": shipping_address.get("phone"),
                "shipping_city": shipping_address.get("city"),
                "shipping_zip": shipping_address.get("zip"),
                "shipping_province": shipping_address.get("province"),
                "shipping_country": shipping_address.get("country"),
                "shipping_last_name": shipping_address.get("last_name"),
                "shipping_address_two": shipping_address.get("address2"),
                "shipping_company": shipping_address.get("company"),
                "shipping_latitude": shipping_address.get("latitude"),
                "shipping_longitude": shipping_address.get("longitude"),
                "shipping_name": shipping_address.get("name"),
                "shipping_country_code": shipping_address.get("country_code"),
                "shipping_province_code": shipping_address.get("province_code"),
            }
        )

        if len(order.get("line_items", [])) > 0:

            for line in order["line_items"]:

                order_line_item_id = line.get("id")
                shop_order_line_item_concat = str(shop_id) + str(order_line_item_id)
                shop_order_line_item_id = hashlib.md5(
                    shop_order_line_item_concat.encode("utf-8")
                ).hexdigest()
                historical_shop_order_line_item_concat = str(
                    historical_shop_order_concat
                ) + str(order_line_item_id)
                historical_shop_order_line_item_id = hashlib.md5(
                    historical_shop_order_line_item_concat.encode("utf-8")
                ).hexdigest()

                flat_order_line_items.append(
                    {
                        "historical_shop_order_line_item_id": historical_shop_order_line_item_id,
                        "shop_order_line_item_id": shop_order_line_item_id,
                        "historical_shop_order_id": historical_shop_order_id,
                        "shop_order_id": shop_order_id,
                        "shop_id": shop_id,
                        "shop_name": shop_name,
                        "order_id": order_id,
                        "order_line_item_id": order_line_item_id,
                        "admin_graphql_api_id": line.get("admin_graphql_api_id"),
                        "fulfillable_quantity": line.get("fulfillable_quantity"),
                        "fulfillment_service": line.get("fulfillment_service"),
                        "fulfillment_status": line.get("fulfillment_status"),
                        "is_gift_card": line.get("gift_card"),
                        "weight_grams": line.get("grams"),
                        "name": line.get("name"),
                        "presentment_currency": order.get("presentment_currency"),
                        "shop_currency": order.get("currency"),
                        "price": line.get("price"),
                        "is_existing_product": line.get("product_exists"),
                        "product_id": line.get("product_id"),
                        "properties": str(line.get("properties")),
                        "quantity": line.get("quantity"),
                        "is_requires_shipping": line.get("requires_shipping"),
                        "sku": line.get("sku"),
                        "is_taxable": line.get("taxable"),
                        "title": line.get("title"),
                        "total_discount": line.get("total_discount"),
                        "variant_id": line.get("variant_id"),
                        "variant_inventory_management": line.get(
                            "variant_inventory_management"
                        ),
                        "variant_title": line.get("variant_title"),
                        "vendor": line.get("vendor"),
                        "tax_lines": str(line.get("tax_lines")),
                        "duties": str(line.get("duties")),
                        "discount_allocations": str(line.get("discount_allocations")),
                        "order_created_at": safe_strptime(order.get("created_at")),
                        "order_updated_at": safe_strptime(order.get("updated_at")),
                    }
                )

        if len(order.get("shipping_lines", [])) > 0:

            for shipping_line in order["shipping_lines"]:

                order_shipping_line_id = shipping_line.get("id")
                shop_order_shipping_line_concat = str(shop_id) + str(
                    order_shipping_line_id
                )
                shop_order_shipping_line_id = hashlib.md5(
                    shop_order_shipping_line_concat.encode("utf-8")
                ).hexdigest()
                historical_shop_order_shipping_line_concat = str(
                    historical_shop_order_concat
                ) + str(order_shipping_line_id)
                historical_shop_order_shipping_line_id = hashlib.md5(
                    historical_shop_order_shipping_line_concat.encode("utf-8")
                ).hexdigest()

                flat_order_shipping_lines.append(
                    {
                        "historical_shop_order_shipping_line_id": historical_shop_order_shipping_line_id,
                        "shop_order_shipping_line_id": shop_order_shipping_line_id,
                        "historical_shop_order_id": historical_shop_order_id,
                        "shop_order_id": shop_order_id,
                        "shop_id": shop_id,
                        "shop_name": shop_name,
                        "order_id": order_id,
                        "order_shipping_line_id": order_shipping_line_id,
                        "carrier_identifier": shipping_line.get("carrier_identifier"),
                        "code": shipping_line.get("code"),
                        "presentment_currency": order.get("presentment_currency"),
                        "shop_currency": order.get("currency"),
                        "discounted_price": shipping_line.get("discounted_price"),
                        "phone": shipping_line.get("phone"),
                        "price": shipping_line.get("price"),
                        "requested_fulfillment_service_id": shipping_line.get(
                            "requested_fulfillment_service_id"
                        ),
                        "source": shipping_line.get("source"),
                        "title": shipping_line.get("title"),
                        "tax_lines": str(shipping_line.get("tax_lines")),
                        "discount_allocations": str(
                            shipping_line.get("discount_allocations")
                        ),
                        "order_created_at": safe_strptime(order.get("created_at")),
                        "order_updated_at": safe_strptime(order.get("updated_at")),
                    }
                )

        if len(order.get("fulfillments", [])) > 0:

            for fulfillment in order["fulfillments"]:

                fulfillment_id = fulfillment.get("id")
                shop_fulfillment_concat = str(shop_id) + str(fulfillment_id)
                shop_fulfillment_id = hashlib.md5(
                    shop_fulfillment_concat.encode("utf-8")
                ).hexdigest()
                historical_shop_fulfillment_concat = str(shop_fulfillment_concat) + str(
                    fulfillment.get("updated_at")
                )
                historical_shop_fulfillment_id = hashlib.md5(
                    historical_shop_fulfillment_concat.encode("utf-8")
                ).hexdigest()

                origin_address = traverse_nested_dict(
                    fulfillment, ["origin_address"], True
                )
                receipt = traverse_nested_dict(fulfillment, ["receipt"], True)

                flat_fulfillments.append(
                    {
                        "historical_shop_fulfillment_id": historical_shop_fulfillment_id,
                        "shop_order_id": shop_order_id,
                        "shop_id": shop_id,
                        "shop_name": shop_name,
                        "shop_fulfillment_id": shop_fulfillment_id,
                        "fulfillment_id": fulfillment_id,
                        "created_at": safe_strptime(fulfillment.get("created_at")),
                        "admin_graphql_api_id": fulfillment.get("admin_graphql_api_id"),
                        "location_id": fulfillment.get("location_id"),
                        "fulfillment_name": fulfillment.get("name"),
                        "order_id": fulfillment.get("order_id"),
                        "origin_address_one": origin_address.get("address1"),
                        "origin_address_two": origin_address.get("address2"),
                        "origin_city": origin_address.get("city"),
                        "origin_country_code": origin_address.get("country_code"),
                        "origin_province_code": origin_address.get("province_code"),
                        "origin_zip": origin_address.get("zip"),
                        "is_receipt_testcase": receipt.get("testcase"),
                        "receipt_authorization": receipt.get("authorization"),
                        "service": fulfillment.get("service"),
                        "shipment_status": fulfillment.get("shipment_status"),
                        "status": fulfillment.get("status"),
                        "tracking_company": fulfillment.get("tracking_company"),
                        "tracking_numbers": str(fulfillment.get("tracking_numbers")),
                        "tracking_urls": str(fulfillment.get("tracking_urls")),
                        "updated_at": safe_strptime(fulfillment.get("updated_at")),
                        "variant_inventory_management": fulfillment.get(
                            "variant_inventory_management"
                        ),
                    }
                )

                if len(fulfillment.get("line_items", [])) > 0:

                    for fulfillment_line_item in fulfillment["line_items"]:

                        fulfillment_line_item_id = fulfillment_line_item.get("id")
                        shop_fulfillment_line_item_concat = (
                            shop_fulfillment_concat + str(fulfillment_line_item_id)
                        )
                        shop_fulfillment_line_item_id = hashlib.md5(
                            shop_fulfillment_line_item_concat.encode("utf-8")
                        ).hexdigest()
                        historical_shop_fulfillment_line_item_concat = (
                            historical_shop_fulfillment_concat
                            + str(fulfillment_line_item_id)
                        )
                        historical_shop_fulfillment_line_item_id = hashlib.md5(
                            historical_shop_fulfillment_line_item_concat.encode("utf-8")
                        ).hexdigest()

                        flat_fulfillment_line_items.append(
                            {
                                "historical_shop_fulfillment_line_item_id": historical_shop_fulfillment_line_item_id,
                                "historical_shop_fulfillment_id": historical_shop_fulfillment_id,
                                "shop_fulfillment_line_item_id": shop_fulfillment_line_item_id,
                                "shop_order_id": shop_order_id,
                                "shop_id": shop_id,
                                "shop_name": shop_name,
                                "shop_fulfillment_id": shop_fulfillment_id,
                                "fulfillment_id": fulfillment_id,
                                "fulfillment_line_item_id": fulfillment_line_item_id,
                                "variant_id": fulfillment_line_item.get("variant_id"),
                                "title": fulfillment_line_item.get("title"),
                                "quantity": fulfillment_line_item.get("quantity"),
                                "presentment_currency": order.get(
                                    "presentment_currency"
                                ),
                                "shop_currency": order.get("currency"),
                                "price": fulfillment_line_item.get("price"),
                                "weight_grams": fulfillment_line_item.get("grams"),
                                "sku": fulfillment_line_item.get("sku"),
                                "variant_title": fulfillment_line_item.get(
                                    "variant_title"
                                ),
                                "vendor": fulfillment_line_item.get("vendor"),
                                "fulfillment_service": fulfillment_line_item.get(
                                    "fulfillment_service"
                                ),
                                "product_id": fulfillment_line_item.get("product_id"),
                                "is_requires_shipping": fulfillment_line_item.get(
                                    "requires_shipping"
                                ),
                                "is_taxable": fulfillment_line_item.get("taxable"),
                                "is_gift_card": fulfillment_line_item.get("gift_card"),
                                "name": fulfillment_line_item.get("name"),
                                "variant_inventory_management": fulfillment_line_item.get(
                                    "variant_inventory_management"
                                ),
                                "properties": str(
                                    fulfillment_line_item.get("properties")
                                ),
                                "is_existing_product": fulfillment_line_item.get(
                                    "product_exists"
                                ),
                                "fulfillable_quantity": fulfillment_line_item.get(
                                    "fulfillable_quantity"
                                ),
                                "total_discount": fulfillment_line_item.get(
                                    "total_discount"
                                ),
                                "fulfillment_status": fulfillment_line_item.get(
                                    "fulfillment_status"
                                ),
                                "fulfillment_line_item_quantity_id": fulfillment_line_item.get(
                                    "fulfillment_line_item_id"
                                ),
                                "tax_lines": str(
                                    fulfillment_line_item.get("tax_lines")
                                ),
                                "duties": str(fulfillment_line_item.get("duties")),
                                "fulfillment_created_at": safe_strptime(
                                    fulfillment.get("created_at")
                                ),
                                "fulfillment_updated_at": safe_strptime(
                                    fulfillment.get("updated_at")
                                ),
                            }
                        )


def upsert_orders_data():

    if len(flat_orders) > 0:

        logger.info("Syncing orders")
        snowflake_connector.to_snowflake(
            data=flat_orders,
            database=database,
            schema="shopify",
            table="orders",
            primary_key_fieldname="historical_shop_order_id",
        )

    if len(flat_order_line_items) > 0:

        logger.info("Syncing order line items")
        snowflake_connector.to_snowflake(
            data=flat_order_line_items,
            database=database,
            schema="shopify",
            table="order_line_items",
            primary_key_fieldname="historical_shop_order_line_item_id",
        )

    if len(flat_order_shipping_lines) > 0:

        logger.info("Syncing order shipping lines")
        snowflake_connector.to_snowflake(
            data=flat_order_shipping_lines,
            database=database,
            schema="shopify",
            table="order_shipping_lines",
            primary_key_fieldname="historical_shop_order_shipping_line_id",
        )
    if len(flat_fulfillments) > 0:

        logger.info("Syncing fulfillments")
        snowflake_connector.to_snowflake(
            data=flat_fulfillments,
            database=database,
            schema="shopify",
            table="fulfillments",
            primary_key_fieldname="historical_shop_fulfillment_id",
        )

    if len(flat_fulfillment_line_items) > 0:

        logger.info("Syncing fulfillment line items")
        snowflake_connector.to_snowflake(
            data=flat_fulfillment_line_items,
            database=database,
            schema="shopify",
            table="fulfillment_line_items",
            primary_key_fieldname="historical_shop_fulfillment_line_item_id",
        )


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[2]

# The app can be set to only run certain steps
steps_to_run = sys.argv[4]

if steps_to_run == "all":

    is_included_step = True

else:

    is_included_step = "orders" in steps_to_run

if (
    __name__ == "__main__"
    and is_included_step
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
):

    # Retrieve Shopify credentials and check parameter for override; the
    # override directs the app to only run to requested sync for a single shop
    shopify_keys = secrets_client.get_secret("SHOPIFY_DATA_TEAM_APP_API_KEYS")
    single_shop_domain_name = sys.argv[3]

    if single_shop_domain_name != "none":

        shopify_keys = [
            key_dict
            for key_dict in shopify_keys
            if key_dict.get("shop_domain_name").lower()
            == single_shop_domain_name.lower()
        ]

        # Throw an exception if there wasn't a match
        if len(shopify_keys) == 0:

            e_str = "Provided single shop domain name does not match any domain names in retrieved AWS secret value"
            logger.error(e_str)

            raise Exception(e_str)

    # Retrieve sync type from parameter
    sync_type = sys.argv[1]

    # Set target database according to environment
    if os.getenv("RES_ENV") == "production":

        database = "raw"

    else:

        database = "raw_dev"

    # Create Snowflake connector
    snowflake_connector = SnowflakeConnector(profile="data_team", warehouse="loader_wh")

    if sync_type == "incremental":

        # Retrieve Snowflake timestamp before async invocation
        since_ts = snowflake_connector.get_latest_snowflake_timestamp(
            database=database,
            schema="shopify",
            table="orders",
            timestamp_field="updated_at",
            timestamp_format_str="%Y-%m-%dT%H:%M:%S.%fZ",
        )

    elif sync_type == "test":

        since_ts = (datetime.now() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

    else:

        since_ts = None

    # ETL setup
    # Create list to extend for response data
    output_list = []

    # Create empty lists to append to
    flat_orders = []
    flat_order_line_items = []
    flat_order_shipping_lines = []
    flat_fulfillments = []
    flat_fulfillment_line_items = []

    # There are too many orders to retrieve at once without running into memory
    # leak issues. API calls are broken down into one-year chunks for full syncs
    if sync_type == "full" or since_ts is None:

        # Start the API calls at 2015
        current_year = datetime.date.today().year
        start_year = 2015

        for year in range(start_year, current_year + 1):

            # Get one year at a time
            additional_params = {
                "status": "any",
                "created_at_min": f"{year}-01-01",
                "created_at_max": f"{year+1}-01-01",
            }

            logger.info(f"Syncing data for year {year}")

            # Execute async API calls in chunks
            asyncio.run(
                fetch_all_data(
                    api_endpoint="orders",
                    output_list=output_list,
                    shopify_keys=shopify_keys,
                    sync_type=sync_type,
                    additional_params=additional_params,
                    pagination_type="next_link",
                    since_ts=since_ts,
                )
            )

            # Flatten data from all API calls and upsert data
            flatten_records(output_list)
            upsert_orders_data()

            # Delete tables from memory; clean up
            del output_list
            del flat_orders
            del flat_order_line_items
            del flat_order_shipping_lines
            del flat_fulfillments
            del flat_fulfillment_line_items
            gc.collect()

            # Reset lists
            output_list = []
            flat_orders = []
            flat_order_line_items = []
            flat_order_shipping_lines = []
            flat_fulfillments = []
            flat_fulfillment_line_items = []

    else:

        # Execute async API calls
        asyncio.run(
            fetch_all_data(
                api_endpoint="orders",
                output_list=output_list,
                shopify_keys=shopify_keys,
                sync_type=sync_type,
                additional_params={"status": "any"},
                pagination_type="next_link",
                since_target_field="updated_at",
                since_ts=since_ts,
            )
        )

        # Flatten data from all API calls
        flatten_records(output_list)

        # Clean up
        del output_list
        gc.collect()

        # Add to Snowflake
        upsert_orders_data()

    logger.info("Shopify orders sync complete")
