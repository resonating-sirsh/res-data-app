import gc
import hashlib
import os
import sys
import asyncio
import snowflake.connector
from datetime import datetime, timedelta
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
from helper_functions import (
    get_secret,
    get_latest_snowflake_timestamp,
    fetch_all_data,
    dict_to_sql,
    safe_strptime,
)
from res.utils import logger


# Create synchronous function to flatten info
def flatten_records(output_list):

    logger.info(f"Proceeding to flatten records")

    for checkout_record in output_list:

        shop_id = checkout_record["shop_id"]
        shop_name = checkout_record["shop_name"]
        abandoned_checkout_id = checkout_record["id"]
        shop_abandoned_checkout_concat = str(shop_id) + str(abandoned_checkout_id)
        shop_abandoned_checkout_id = hashlib.md5(
            shop_abandoned_checkout_concat.encode("utf-8")
        ).hexdigest()
        historical_shop_abandoned_checkout_concat = (
            str(shop_id)
            + str(abandoned_checkout_id)
            + str(checkout_record["updated_at"])
        )
        historical_shop_abandoned_checkout_id = hashlib.md5(
            historical_shop_abandoned_checkout_concat.encode("utf-8")
        ).hexdigest()

        billing_object = checkout_record.get("billing_address") or {}
        shipping_object = checkout_record.get("shipping_address") or {}

        flat_abandoned_checkouts.append(
            {
                "historical_shop_abandoned_checkout_id": f"{historical_shop_abandoned_checkout_id}",
                "shop_abandoned_checkout_id": f"{shop_abandoned_checkout_id}",
                "shop_id": shop_id,
                "shop_name": f"{shop_name}",
                "abandoned_checkout_id": f"{abandoned_checkout_id}",
                "checkout_id": checkout_record.get("token"),
                "cart_id": checkout_record.get("cart_token"),
                "email": checkout_record.get("email"),
                "gateway": checkout_record.get("gateway"),
                "is_buyer_accepts_marketing": checkout_record.get(
                    "buyer_accepts_marketing"
                ),
                "created_at": safe_strptime(checkout_record.get("created_at")),
                "updated_at": safe_strptime(checkout_record.get("updated_at")),
                "landing_site": checkout_record.get("landing_site"),
                "checkout_note": checkout_record.get("note"),
                "checkout_note_attributes": str(checkout_record.get("note_attributes")),
                "referring_site": checkout_record.get("referring_site"),
                "is_taxes_included": checkout_record.get("taxes_included"),
                "total_weight": checkout_record.get("total_weight"),
                "shop_currency": checkout_record.get("currency"),
                "completed_at": safe_strptime(checkout_record.get("completed_at")),
                "closed_at": safe_strptime(checkout_record.get("closed_at")),
                "creating_user_id": checkout_record.get("user_id"),
                "location_id": checkout_record.get("location_id"),
                "source_identifier": checkout_record.get("source_identifier"),
                "source_url": checkout_record.get("source_url"),
                "device_id": checkout_record.get("device_id"),
                "phone": checkout_record.get("phone"),
                "customer_locale": checkout_record.get("customer_locale"),
                "name": checkout_record.get("name"),
                "source": checkout_record.get("source"),
                "abandoned_checkout_url": checkout_record.get("abandoned_checkout_url"),
                "discount_codes": str(checkout_record.get("discount_codes")),
                "tax_lines": str(checkout_record.get("tax_lines")),
                "source_name": checkout_record.get("source_name"),
                "presentment_currency": checkout_record.get("presentment_currency"),
                "is_buyer_accepts_sms_marketing": checkout_record.get(
                    "buyer_accepts_sms_marketing"
                ),
                "sms_marketing_phone": checkout_record.get("sms_marketing_phone"),
                "total_discounts": checkout_record.get("total_discounts"),
                "total_line_items_price": checkout_record.get("total_line_items_price"),
                "total_price": checkout_record.get("total_price"),
                "total_tax": checkout_record.get("total_tax"),
                "subtotal_price": checkout_record.get("subtotal_price"),
                "total_duties": checkout_record.get("total_duties"),
                "billing_first_name": billing_object.get("first_name"),
                "billing_address_line_one": billing_object.get("address1"),
                "billing_phone": billing_object.get("phone"),
                "billing_city": billing_object.get("city"),
                "billing_zip": billing_object.get("zip"),
                "billing_province": billing_object.get("province"),
                "billing_country": billing_object.get("country"),
                "billing_last_name": billing_object.get("last_name"),
                "billing_address_line_two": billing_object.get("address2"),
                "billing_company": billing_object.get("company"),
                "billing_latitude": billing_object.get("latitude"),
                "billing_longitude": billing_object.get("longitude"),
                "billing_name": billing_object.get("name"),
                "billing_country_code": billing_object.get("country_code"),
                "billing_province_code": billing_object.get("province_code"),
                "shipping_first_name": shipping_object.get("first_name"),
                "shipping_address_line_one": shipping_object.get("address1"),
                "shipping_phone": shipping_object.get("phone"),
                "shipping_city": shipping_object.get("city"),
                "shipping_zip": shipping_object.get("zip"),
                "shipping_province": shipping_object.get("province"),
                "shipping_country": shipping_object.get("country"),
                "shipping_last_name": shipping_object.get("last_name"),
                "shipping_address_line_two": shipping_object.get("address2"),
                "shipping_company": shipping_object.get("company"),
                "shipping_latitude": shipping_object.get("latitude"),
                "shipping_longitude": shipping_object.get("longitude"),
                "shipping_name": shipping_object.get("name"),
                "shipping_country_code": shipping_object.get("country_code"),
                "shipping_province_code": shipping_object.get("province_code"),
            }
        )

        if len(checkout_record["shipping_lines"]) > 0:

            for line_number, shipping_line in enumerate(
                checkout_record["shipping_lines"], 1
            ):

                # If the shipping line ID missing it will be replaced by its
                # position in the shipping line list and the abandoned checkout id

                if shipping_line.get("id"):

                    abandoned_checkout_ship_line_id = shipping_line["id"]

                else:

                    abandoned_checkout_ship_line_id = (
                        f"{abandoned_checkout_id}_shipping_line_{line_number}"
                    )

                shop_abandoned_checkout_ship_line_concat = str(shop_id) + str(
                    abandoned_checkout_ship_line_id
                )
                shop_abandoned_checkout_ship_line_id = hashlib.md5(
                    shop_abandoned_checkout_ship_line_concat.encode("utf-8")
                ).hexdigest()
                historical_shop_abandoned_checkout_ship_line_concat = (
                    historical_shop_abandoned_checkout_concat
                    + str(abandoned_checkout_ship_line_id)
                )
                historical_shop_abandoned_checkout_ship_line_id = hashlib.md5(
                    historical_shop_abandoned_checkout_ship_line_concat.encode("utf-8")
                ).hexdigest()

                # Some records have a dict with delivery option fields. These
                # fields are used when their non-dict forms are not present

                delivery_option_group_object = (
                    shipping_line.get("delivery_option_group") or {}
                )
                surrogate_delivery_option_group_type = delivery_option_group_object.get(
                    "type"
                )

                flat_abandoned_checkout_shipping_lines.append(
                    {
                        "historical_shop_abandoned_checkout_ship_line_id": f"{historical_shop_abandoned_checkout_ship_line_id}",
                        "shop_abandoned_checkout_ship_line_id": f"{shop_abandoned_checkout_ship_line_id}",
                        "historical_shop_abandoned_checkout_id": f"{historical_shop_abandoned_checkout_id}",
                        "shop_abandoned_checkout_id": f"{shop_abandoned_checkout_id}",
                        "abandoned_checkout_id": f"{abandoned_checkout_id}",
                        "abandoned_checkout_ship_line_id": f"{abandoned_checkout_ship_line_id}",
                        "shop_id": shop_id,
                        "shop_name": f"{shop_name}",
                        "code": shipping_line.get("code"),
                        "shop_currency": checkout_record.get("currency"),
                        "presentment_currency": checkout_record.get(
                            "presentment_currency"
                        ),
                        "price": shipping_line.get("price"),
                        "original_shop_price": shipping_line.get("original_shop_price"),
                        "original_rate_price": shipping_line.get("original_rate_price"),
                        "original_shop_markup": shipping_line.get(
                            "original_shop_markup"
                        ),
                        "source": shipping_line.get("source"),
                        "title": shipping_line.get("title"),
                        "presentment_title": shipping_line.get("presentment_title"),
                        "phone": shipping_line.get("phone"),
                        "tax_lines": str(shipping_line.get("tax_lines")),
                        "custom_tax_lines": shipping_line.get("custom_tax_lines"),
                        "validation_context": shipping_line.get("validation_context"),
                        "markup": shipping_line.get("markup"),
                        "delivery_category": shipping_line.get("delivery_category"),
                        "carrier_identifier": shipping_line.get("carrier_identifier"),
                        "carrier_service_id": shipping_line.get("carrier_service_id"),
                        "api_client_id": shipping_line.get("api_client_id"),
                        "requested_fulfillment_service_id": shipping_line.get(
                            "requested_fulfillment_service_id"
                        ),
                        "applied_discounts": str(
                            shipping_line.get("applied_discounts")
                        ),
                        "delivery_option_group_id": delivery_option_group_object.get(
                            "token"
                        ),
                        "delivery_option_group_type": shipping_line.get(
                            "delivery_option_group_type",
                            surrogate_delivery_option_group_type,
                        ),
                        "delivery_expectation_range": str(
                            shipping_line.get("delivery_expectation_range")
                        ),
                        "delivery_expectation_type": shipping_line.get(
                            "delivery_expectation_type"
                        ),
                        "estimated_delivery_time_range": str(
                            shipping_line.get("estimated_delivery_time_range")
                        ),
                        "checkout_created_at": safe_strptime(
                            checkout_record.get("created_at")
                        ),
                        "checkout_updated_at": safe_strptime(
                            checkout_record.get("updated_at")
                        ),
                    }
                )

        if len(checkout_record["line_items"]) > 0:

            for line_number, line_item in enumerate(checkout_record["line_items"], 1):

                # If the shipping line ID missing it will be replaced by its
                # position in the shipping line list and the abandoned checkout id

                if line_item.get("id"):

                    abandoned_checkout_line_item_id = line_item["id"]

                else:

                    abandoned_checkout_line_item_id = (
                        f"{abandoned_checkout_id}_line_item_{line_number}"
                    )

                shop_abandoned_checkout_line_item_concat = str(shop_id) + str(
                    abandoned_checkout_line_item_id
                )
                shop_abandoned_checkout_line_item_id = hashlib.md5(
                    shop_abandoned_checkout_line_item_concat.encode("utf-8")
                ).hexdigest()
                historical_shop_abandoned_checkout_line_item_concat = (
                    historical_shop_abandoned_checkout_concat
                    + str(abandoned_checkout_line_item_id)
                )
                historical_shop_abandoned_checkout_line_item_id = hashlib.md5(
                    historical_shop_abandoned_checkout_line_item_concat.encode("utf-8")
                ).hexdigest()
                shop_variant_concat = str(shop_id) + str(line_item["variant_id"])
                shop_variant_id = hashlib.md5(
                    shop_variant_concat.encode("utf-8")
                ).hexdigest()
                shop_product_concat = str(shop_id) + str(line_item["product_id"])
                shop_product_id = hashlib.md5(
                    shop_product_concat.encode("utf-8")
                ).hexdigest()
                unit_price_measurement_base = line_item.get(
                    "unit_price_measurement", {}
                )

                # Explicitly replace None with {} to prevent any get issues
                unit_price_measurement = (
                    {}
                    if unit_price_measurement_base is None
                    else unit_price_measurement_base
                )

                flat_abandoned_checkout_line_items.append(
                    {
                        "historical_shop_abandoned_checkout_line_item_id": f"{historical_shop_abandoned_checkout_line_item_id}",
                        "shop_abandoned_checkout_line_item_id": f"{shop_abandoned_checkout_line_item_id}",
                        "historical_shop_abandoned_checkout_id": f"{historical_shop_abandoned_checkout_id}",
                        "shop_abandoned_checkout_id": f"{shop_abandoned_checkout_id}",
                        "abandoned_checkout_id": f"{abandoned_checkout_id}",
                        "abandoned_checkout_line_item_id": f"{abandoned_checkout_line_item_id}",
                        "shop_id": shop_id,
                        "shop_variant_id": shop_variant_id,
                        "shop_product_id": shop_product_id,
                        "shop_name": f"{shop_name}",
                        "applied_discounts": str(line_item.get("applied_discounts")),
                        "discount_allocations": str(
                            line_item.get("discount_allocations")
                        ),
                        "key": line_item.get("key"),
                        "destination_location_id": line_item.get(
                            "destination_location_id"
                        ),
                        "fulfillment_service": line_item.get("fulfillment_service"),
                        "is_gift_card": line_item.get("gift_card"),
                        "weight_grams": line_item.get("grams"),
                        "origin_location_id": line_item.get("origin_location_id"),
                        "presentment_title": line_item.get("presentment_title"),
                        "presentment_variant_title": line_item.get(
                            "presentment_variant_title"
                        ),
                        "product_id": line_item.get("product_id"),
                        "properties": str(line_item.get("properties")),
                        "quantity": line_item.get("quantity"),
                        "is_requires_shipping": line_item.get("requires_shipping"),
                        "sku": line_item.get("sku"),
                        "tax_lines": str(line_item.get("tax_lines")),
                        "is_taxable": line_item.get("taxable"),
                        "title": line_item.get("title"),
                        "variant_id": line_item.get("variant_id"),
                        "variant_title": line_item.get("variant_title"),
                        "variant_price": line_item.get("variant_price"),
                        "vendor": line_item.get("vendor"),
                        "user_id": line_item.get("user_id"),
                        "shop_currency": checkout_record.get("currency"),
                        "presentment_currency": checkout_record.get(
                            "presentment_currency"
                        ),
                        "price_meas_measured_type": unit_price_measurement.get(
                            "measured_type"
                        ),
                        "price_meas_quantity_value": unit_price_measurement.get(
                            "quantity_value"
                        ),
                        "price_meas_quantity_unit": unit_price_measurement.get(
                            "quantity_unit"
                        ),
                        "price_meas_reference_value": unit_price_measurement.get(
                            "reference_value"
                        ),
                        "price_meas_reference_unit": unit_price_measurement.get(
                            "reference_unit"
                        ),
                        "rank": line_item.get("rank"),
                        "compare_at_price": line_item.get("compare_at_price"),
                        "line_price": line_item.get("line_price"),
                        "price": line_item.get("price"),
                        "checkout_created_at": safe_strptime(
                            checkout_record.get("created_at")
                        ),
                        "checkout_updated_at": safe_strptime(
                            checkout_record.get("updated_at")
                        ),
                    }
                )


# Create synchronous functions to create tables in Snowflake and upsert data
def create_snowflake_tables():

    try:

        # Connect to Snowflake
        base_connection = snowflake.connector.connect(
            user=snowflake_user,
            password=snowflake_password,
            account=snowflake_account,
            warehouse="loader_wh",
            database=database,
        )

        logger.info("Connected to Snowflake")

    except:

        logger.error("Failed to authenticate to snowflake")

    # Create schema and tables
    logger.info("Creating schema and tables if they do not exist")
    base_connection.cursor().execute("create schema if not exists shopify")
    base_connection.cursor().execute("use schema shopify")
    base_connection.cursor().execute(
        """
            create table if not exists abandoned_checkouts (
                historical_shop_abandoned_checkout_id varchar,
                shop_abandoned_checkout_id varchar,
                shop_id integer,
                shop_name varchar,
                abandoned_checkout_id integer,
                checkout_id varchar,
                cart_id varchar,
                email varchar,
                gateway varchar,
                is_buyer_accepts_marketing boolean,
                created_at timestamp_tz,
                updated_at timestamp_tz,
                landing_site varchar,
                checkout_note varchar,
                checkout_note_attributes varchar,
                referring_site varchar,
                is_taxes_included boolean,
                total_weight number(32, 16),
                shop_currency varchar,
                completed_at timestamp_tz,
                closed_at timestamp_tz,
                creating_user_id integer,
                location_id integer,
                source_identifier varchar,
                source_url varchar,
                device_id integer,
                phone varchar,
                customer_locale varchar,
                name varchar,
                source varchar,
                abandoned_checkout_url varchar,
                discount_codes varchar,
                tax_lines varchar,
                source_name varchar,
                presentment_currency varchar,
                is_buyer_accepts_sms_marketing boolean,
                sms_marketing_phone varchar,
                total_discounts number(32, 2),
                total_line_items_price number(32, 2),
                total_price number(32, 2),
                total_tax number(32, 2),
                subtotal_price number(32, 2),
                total_duties number(32, 2),
                billing_first_name varchar,
                billing_address_line_one varchar,
                billing_phone varchar,
                billing_city varchar,
                billing_zip varchar,
                billing_province varchar,
                billing_country varchar,
                billing_last_name varchar,
                billing_address_line_two varchar,
                billing_company varchar,
                billing_latitude varchar,
                billing_longitude varchar,
                billing_name varchar,
                billing_country_code varchar,
                billing_province_code varchar,
                shipping_first_name varchar,
                shipping_address_line_one varchar,
                shipping_phone varchar,
                shipping_city varchar,
                shipping_zip varchar,
                shipping_province varchar,
                shipping_country varchar,
                shipping_last_name varchar,
                shipping_address_line_two varchar,
                shipping_company varchar,
                shipping_latitude number(32, 16),
                shipping_longitude number(32, 16),
                shipping_name varchar,
                shipping_country_code varchar,
                shipping_province_code varchar
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists abandoned_checkout_shipping_lines (
                historical_shop_abandoned_checkout_ship_line_id varchar,
                shop_abandoned_checkout_ship_line_id varchar,
                historical_shop_abandoned_checkout_id varchar,
                shop_abandoned_checkout_id varchar,
                abandoned_checkout_id integer,
                abandoned_checkout_ship_line_id varchar,
                shop_id integer,
                shop_name varchar,
                code varchar,
                shop_currency varchar,
                presentment_currency varchar,
                price number(32, 2),
                original_shop_price number(32, 2),
                original_rate_price number(32, 2),
                original_shop_markup number(32, 2),
                source varchar,
                title varchar,
                presentment_title varchar,
                phone varchar,
                tax_lines varchar,
                custom_tax_lines varchar,
                validation_context varchar,
                markup number(32, 2),
                delivery_category varchar,
                carrier_identifier varchar,
                carrier_service_id varchar,
                api_client_id varchar,
                requested_fulfillment_service_id varchar,
                applied_discounts varchar,
                delivery_option_group_id varchar,
                delivery_option_group_type varchar,
                delivery_expectation_range varchar,
                delivery_expectation_type varchar,
                estimated_delivery_time_range varchar,
                checkout_created_at timestamp_tz,
                checkout_updated_at timestamp_tz                               
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists abandoned_checkout_line_items (
                historical_shop_abandoned_checkout_line_item_id varchar,
                shop_abandoned_checkout_line_item_id varchar,
                historical_shop_abandoned_checkout_id varchar,
                shop_abandoned_checkout_id varchar,
                abandoned_checkout_id varchar,
                abandoned_checkout_line_item_id varchar,
                shop_id integer,
                shop_variant_id varchar,
                shop_product_id varchar,
                shop_name varchar,
                applied_discounts varchar,
                discount_allocations varchar,
                key varchar,
                destination_location_id integer,
                fulfillment_service varchar,
                is_gift_card boolean,
                weight_grams number(32, 16),
                origin_location_id integer,
                presentment_title varchar,
                presentment_variant_title varchar,
                product_id integer,
                properties varchar,
                quantity integer,
                is_requires_shipping boolean,
                sku varchar,
                tax_lines varchar,
                is_taxable boolean,
                title varchar,
                variant_id integer,
                variant_title varchar,
                variant_price number(32, 2),
                vendor varchar,
                user_id integer,
                shop_currency varchar,
                presentment_currency varchar,                
                price_meas_measured_type varchar,
                price_meas_quantity_value number(32, 16),
                price_meas_quantity_unit varchar,
                price_meas_reference_value number(32, 16),
                price_meas_reference_unit varchar,
                rank varchar,
                compare_at_price number(32, 2),
                line_price number(32, 2),
                price number(32, 2),
                checkout_created_at timestamp_tz,
                checkout_updated_at timestamp_tz  
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_abandoned_checkouts_data():

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    abandoned_checkouts_dtypes = {
        "historical_shop_abandoned_checkout_id": VARCHAR,
        "shop_abandoned_checkout_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "abandoned_checkout_id": INTEGER,
        "checkout_id": VARCHAR,
        "cart_id": VARCHAR,
        "email": VARCHAR,
        "gateway": VARCHAR,
        "is_buyer_accepts_marketing": BOOLEAN,
        "created_at": TIMESTAMP(timezone=True),
        "updated_at": TIMESTAMP(timezone=True),
        "landing_site": VARCHAR,
        "checkout_note": VARCHAR,
        "checkout_note_attributes": VARCHAR,
        "referring_site": VARCHAR,
        "is_taxes_included": BOOLEAN,
        "total_weight": NUMERIC(32, 16),
        "shop_currency": VARCHAR,
        "completed_at": TIMESTAMP(timezone=True),
        "closed_at": TIMESTAMP(timezone=True),
        "creating_user_id": INTEGER,
        "location_id": INTEGER,
        "source_identifier": VARCHAR,
        "source_url": VARCHAR,
        "device_id": INTEGER,
        "phone": VARCHAR,
        "customer_locale": VARCHAR,
        "name": VARCHAR,
        "source": VARCHAR,
        "abandoned_checkout_url": VARCHAR,
        "discount_codes": VARCHAR,
        "tax_lines": VARCHAR,
        "source_name": VARCHAR,
        "presentment_currency": VARCHAR,
        "is_buyer_accepts_sms_marketing": BOOLEAN,
        "sms_marketing_phone": VARCHAR,
        "total_discounts": NUMERIC(32, 2),
        "total_line_items_price": NUMERIC(32, 2),
        "total_price": NUMERIC(32, 2),
        "total_tax": NUMERIC(32, 2),
        "subtotal_price": NUMERIC(32, 2),
        "total_duties": NUMERIC(32, 2),
        "billing_first_name": VARCHAR,
        "billing_address_line_one": VARCHAR,
        "billing_phone": VARCHAR,
        "billing_city": VARCHAR,
        "billing_zip": VARCHAR,
        "billing_province": VARCHAR,
        "billing_country": VARCHAR,
        "billing_last_name": VARCHAR,
        "billing_address_line_two": VARCHAR,
        "billing_company": VARCHAR,
        "billing_latitude": VARCHAR,
        "billing_longitude": VARCHAR,
        "billing_name": VARCHAR,
        "billing_country_code": VARCHAR,
        "billing_province_code": VARCHAR,
        "shipping_first_name": VARCHAR,
        "shipping_address_line_one": VARCHAR,
        "shipping_phone": VARCHAR,
        "shipping_city": VARCHAR,
        "shipping_zip": VARCHAR,
        "shipping_province": VARCHAR,
        "shipping_country": VARCHAR,
        "shipping_last_name": VARCHAR,
        "shipping_address_line_two": VARCHAR,
        "shipping_company": VARCHAR,
        "shipping_latitude": NUMERIC(32, 16),
        "shipping_longitude": NUMERIC(32, 16),
        "shipping_name": VARCHAR,
        "shipping_country_code": VARCHAR,
        "shipping_province_code": VARCHAR,
    }
    abandoned_checkout_shipping_lines_dtypes = {
        "historical_shop_abandoned_checkout_ship_line_id": VARCHAR,
        "shop_abandoned_checkout_ship_line_id": VARCHAR,
        "historical_shop_abandoned_checkout_id": VARCHAR,
        "shop_abandoned_checkout_id": VARCHAR,
        "abandoned_checkout_id": INTEGER,
        "abandoned_checkout_ship_line_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "code": VARCHAR,
        "shop_currency": VARCHAR,
        "presentment_currency": VARCHAR,
        "price": NUMERIC(32, 2),
        "original_shop_price": NUMERIC(32, 2),
        "original_rate_price": NUMERIC(32, 2),
        "original_shop_markup": NUMERIC(32, 2),
        "source": VARCHAR,
        "title": VARCHAR,
        "presentment_title": VARCHAR,
        "phone": VARCHAR,
        "tax_lines": VARCHAR,
        "custom_tax_lines": VARCHAR,
        "validation_context": VARCHAR,
        "markup": NUMERIC(32, 2),
        "delivery_category": VARCHAR,
        "carrier_identifier": VARCHAR,
        "carrier_service_id": VARCHAR,
        "api_client_id": VARCHAR,
        "requested_fulfillment_service_id": VARCHAR,
        "applied_discounts": VARCHAR,
        "delivery_option_group_id": VARCHAR,
        "delivery_option_group_type": VARCHAR,
        "delivery_expectation_range": VARCHAR,
        "delivery_expectation_type": VARCHAR,
        "estimated_delivery_time_range": VARCHAR,
        "checkout_created_at": TIMESTAMP(timezone=True),
        "checkout_updated_at": TIMESTAMP(timezone=True),
    }
    abandoned_checkout_line_items_dtypes = {
        "historical_shop_abandoned_checkout_line_item_id": VARCHAR,
        "shop_abandoned_checkout_line_item_id": VARCHAR,
        "historical_shop_abandoned_checkout_id": VARCHAR,
        "shop_abandoned_checkout_id": VARCHAR,
        "abandoned_checkout_id": VARCHAR,
        "abandoned_checkout_line_item_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_variant_id": VARCHAR,
        "shop_product_id": VARCHAR,
        "shop_name": VARCHAR,
        "applied_discounts": VARCHAR,
        "discount_allocations": VARCHAR,
        "key": VARCHAR,
        "destination_location_id": INTEGER,
        "fulfillment_service": VARCHAR,
        "is_gift_card": BOOLEAN,
        "weight_grams": NUMERIC(32, 16),
        "origin_location_id": INTEGER,
        "presentment_title": VARCHAR,
        "presentment_variant_title": VARCHAR,
        "product_id": INTEGER,
        "properties": VARCHAR,
        "quantity": INTEGER,
        "is_requires_shipping": BOOLEAN,
        "sku": VARCHAR,
        "tax_lines": VARCHAR,
        "is_taxable": BOOLEAN,
        "title": VARCHAR,
        "variant_id": INTEGER,
        "variant_title": VARCHAR,
        "variant_price": NUMERIC(32, 2),
        "vendor": VARCHAR,
        "user_id": INTEGER,
        "shop_currency": VARCHAR,
        "presentment_currency": VARCHAR,
        "price_meas_measured_type": VARCHAR,
        "price_meas_quantity_value": NUMERIC(32, 16),
        "price_meas_quantity_unit": VARCHAR,
        "price_meas_reference_value": NUMERIC(32, 16),
        "price_meas_reference_unit": VARCHAR,
        "rank": VARCHAR,
        "compare_at_price": NUMERIC(32, 2),
        "line_price": NUMERIC(32, 2),
        "price": NUMERIC(32, 2),
        "checkout_created_at": TIMESTAMP(timezone=True),
        "checkout_updated_at": TIMESTAMP(timezone=True),
    }

    dict_to_sql(
        flat_abandoned_checkouts,
        "abandoned_checkouts_stage",
        engine,
        dtype=abandoned_checkouts_dtypes,
    )
    dict_to_sql(
        flat_abandoned_checkout_shipping_lines,
        "abandoned_checkout_shipping_lines_stage",
        engine,
        dtype=abandoned_checkout_shipping_lines_dtypes,
    )
    dict_to_sql(
        flat_abandoned_checkout_line_items,
        "abandoned_checkout_line_items_stage",
        engine,
        dtype=abandoned_checkout_line_items_dtypes,
    )

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stages table to corresponding target tables")

    if len(flat_abandoned_checkouts) > 0:

        # Create stage and target tables
        abandoned_checkouts_stage_table = meta.tables["abandoned_checkouts_stage"]
        abandoned_checkouts_target_table = meta.tables["abandoned_checkouts"]

        # Structure merge
        abandoned_checkouts_merge = MergeInto(
            target=abandoned_checkouts_target_table,
            source=abandoned_checkouts_stage_table,
            on=abandoned_checkouts_target_table.c.historical_shop_abandoned_checkout_id
            == abandoned_checkouts_stage_table.c.historical_shop_abandoned_checkout_id,
        )

        abandoned_checkouts_cols = {
            "historical_shop_abandoned_checkout_id": abandoned_checkouts_stage_table.c.historical_shop_abandoned_checkout_id,
            "shop_abandoned_checkout_id": abandoned_checkouts_stage_table.c.shop_abandoned_checkout_id,
            "shop_id": abandoned_checkouts_stage_table.c.shop_id,
            "shop_name": abandoned_checkouts_stage_table.c.shop_name,
            "abandoned_checkout_id": abandoned_checkouts_stage_table.c.abandoned_checkout_id,
            "checkout_id": abandoned_checkouts_stage_table.c.checkout_id,
            "cart_id": abandoned_checkouts_stage_table.c.cart_id,
            "email": abandoned_checkouts_stage_table.c.email,
            "gateway": abandoned_checkouts_stage_table.c.gateway,
            "is_buyer_accepts_marketing": abandoned_checkouts_stage_table.c.is_buyer_accepts_marketing,
            "created_at": abandoned_checkouts_stage_table.c.created_at,
            "updated_at": abandoned_checkouts_stage_table.c.updated_at,
            "landing_site": abandoned_checkouts_stage_table.c.landing_site,
            "checkout_note": abandoned_checkouts_stage_table.c.checkout_note,
            "checkout_note_attributes": abandoned_checkouts_stage_table.c.checkout_note_attributes,
            "referring_site": abandoned_checkouts_stage_table.c.referring_site,
            "is_taxes_included": abandoned_checkouts_stage_table.c.is_taxes_included,
            "total_weight": abandoned_checkouts_stage_table.c.total_weight,
            "shop_currency": abandoned_checkouts_stage_table.c.shop_currency,
            "completed_at": abandoned_checkouts_stage_table.c.completed_at,
            "closed_at": abandoned_checkouts_stage_table.c.closed_at,
            "creating_user_id": abandoned_checkouts_stage_table.c.creating_user_id,
            "location_id": abandoned_checkouts_stage_table.c.location_id,
            "source_identifier": abandoned_checkouts_stage_table.c.source_identifier,
            "source_url": abandoned_checkouts_stage_table.c.source_url,
            "device_id": abandoned_checkouts_stage_table.c.device_id,
            "phone": abandoned_checkouts_stage_table.c.phone,
            "customer_locale": abandoned_checkouts_stage_table.c.customer_locale,
            "name": abandoned_checkouts_stage_table.c.name,
            "source": abandoned_checkouts_stage_table.c.source,
            "abandoned_checkout_url": abandoned_checkouts_stage_table.c.abandoned_checkout_url,
            "discount_codes": abandoned_checkouts_stage_table.c.discount_codes,
            "tax_lines": abandoned_checkouts_stage_table.c.tax_lines,
            "source_name": abandoned_checkouts_stage_table.c.source_name,
            "presentment_currency": abandoned_checkouts_stage_table.c.presentment_currency,
            "is_buyer_accepts_sms_marketing": abandoned_checkouts_stage_table.c.is_buyer_accepts_sms_marketing,
            "sms_marketing_phone": abandoned_checkouts_stage_table.c.sms_marketing_phone,
            "total_discounts": abandoned_checkouts_stage_table.c.total_discounts,
            "total_line_items_price": abandoned_checkouts_stage_table.c.total_line_items_price,
            "total_price": abandoned_checkouts_stage_table.c.total_price,
            "total_tax": abandoned_checkouts_stage_table.c.total_tax,
            "subtotal_price": abandoned_checkouts_stage_table.c.subtotal_price,
            "total_duties": abandoned_checkouts_stage_table.c.total_duties,
            "billing_first_name": abandoned_checkouts_stage_table.c.billing_first_name,
            "billing_address_line_one": abandoned_checkouts_stage_table.c.billing_address_line_one,
            "billing_phone": abandoned_checkouts_stage_table.c.billing_phone,
            "billing_city": abandoned_checkouts_stage_table.c.billing_city,
            "billing_zip": abandoned_checkouts_stage_table.c.billing_zip,
            "billing_province": abandoned_checkouts_stage_table.c.billing_province,
            "billing_country": abandoned_checkouts_stage_table.c.billing_country,
            "billing_last_name": abandoned_checkouts_stage_table.c.billing_last_name,
            "billing_address_line_two": abandoned_checkouts_stage_table.c.billing_address_line_two,
            "billing_company": abandoned_checkouts_stage_table.c.billing_company,
            "billing_latitude": abandoned_checkouts_stage_table.c.billing_latitude,
            "billing_longitude": abandoned_checkouts_stage_table.c.billing_longitude,
            "billing_name": abandoned_checkouts_stage_table.c.billing_name,
            "billing_country_code": abandoned_checkouts_stage_table.c.billing_country_code,
            "billing_province_code": abandoned_checkouts_stage_table.c.billing_province_code,
            "shipping_first_name": abandoned_checkouts_stage_table.c.shipping_first_name,
            "shipping_address_line_one": abandoned_checkouts_stage_table.c.shipping_address_line_one,
            "shipping_phone": abandoned_checkouts_stage_table.c.shipping_phone,
            "shipping_city": abandoned_checkouts_stage_table.c.shipping_city,
            "shipping_zip": abandoned_checkouts_stage_table.c.shipping_zip,
            "shipping_province": abandoned_checkouts_stage_table.c.shipping_province,
            "shipping_country": abandoned_checkouts_stage_table.c.shipping_country,
            "shipping_last_name": abandoned_checkouts_stage_table.c.shipping_last_name,
            "shipping_address_line_two": abandoned_checkouts_stage_table.c.shipping_address_line_two,
            "shipping_company": abandoned_checkouts_stage_table.c.shipping_company,
            "shipping_latitude": abandoned_checkouts_stage_table.c.shipping_latitude,
            "shipping_longitude": abandoned_checkouts_stage_table.c.shipping_longitude,
            "shipping_name": abandoned_checkouts_stage_table.c.shipping_name,
            "shipping_country_code": abandoned_checkouts_stage_table.c.shipping_country_code,
            "shipping_province_code": abandoned_checkouts_stage_table.c.shipping_province_code,
        }

        abandoned_checkouts_merge.when_not_matched_then_insert().values(
            **abandoned_checkouts_cols
        )

        abandoned_checkouts_merge.when_matched_then_update().values(
            **abandoned_checkouts_cols
        )

        # Execute merge
        alchemy_connection.execute(abandoned_checkouts_merge)

    if len(flat_abandoned_checkout_shipping_lines) > 0:

        # Create stage and target tables
        abandoned_checkout_shipping_lines_stage_table = meta.tables[
            "abandoned_checkout_shipping_lines_stage"
        ]
        abandoned_checkout_shipping_lines_target_table = meta.tables[
            "abandoned_checkout_shipping_lines"
        ]

        # Structure merge
        abandoned_checkout_shipping_lines_merge = MergeInto(
            target=abandoned_checkout_shipping_lines_target_table,
            source=abandoned_checkout_shipping_lines_stage_table,
            on=abandoned_checkout_shipping_lines_target_table.c.historical_shop_abandoned_checkout_ship_line_id
            == abandoned_checkout_shipping_lines_stage_table.c.historical_shop_abandoned_checkout_ship_line_id,
        )

        abandoned_checkout_shipping_lines_cols = {
            "historical_shop_abandoned_checkout_ship_line_id": abandoned_checkout_shipping_lines_stage_table.c.historical_shop_abandoned_checkout_ship_line_id,
            "shop_abandoned_checkout_ship_line_id": abandoned_checkout_shipping_lines_stage_table.c.shop_abandoned_checkout_ship_line_id,
            "historical_shop_abandoned_checkout_id": abandoned_checkout_shipping_lines_stage_table.c.historical_shop_abandoned_checkout_id,
            "shop_abandoned_checkout_id": abandoned_checkout_shipping_lines_stage_table.c.shop_abandoned_checkout_id,
            "abandoned_checkout_id": abandoned_checkout_shipping_lines_stage_table.c.abandoned_checkout_id,
            "abandoned_checkout_ship_line_id": abandoned_checkout_shipping_lines_stage_table.c.abandoned_checkout_ship_line_id,
            "shop_id": abandoned_checkout_shipping_lines_stage_table.c.shop_id,
            "shop_name": abandoned_checkout_shipping_lines_stage_table.c.shop_name,
            "code": abandoned_checkout_shipping_lines_stage_table.c.code,
            "shop_currency": abandoned_checkout_shipping_lines_stage_table.c.shop_currency,
            "presentment_currency": abandoned_checkout_shipping_lines_stage_table.c.presentment_currency,
            "price": abandoned_checkout_shipping_lines_stage_table.c.price,
            "original_shop_price": abandoned_checkout_shipping_lines_stage_table.c.original_shop_price,
            "original_rate_price": abandoned_checkout_shipping_lines_stage_table.c.original_rate_price,
            "original_shop_markup": abandoned_checkout_shipping_lines_stage_table.c.original_shop_markup,
            "source": abandoned_checkout_shipping_lines_stage_table.c.source,
            "title": abandoned_checkout_shipping_lines_stage_table.c.title,
            "presentment_title": abandoned_checkout_shipping_lines_stage_table.c.presentment_title,
            "phone": abandoned_checkout_shipping_lines_stage_table.c.phone,
            "tax_lines": abandoned_checkout_shipping_lines_stage_table.c.tax_lines,
            "custom_tax_lines": abandoned_checkout_shipping_lines_stage_table.c.custom_tax_lines,
            "validation_context": abandoned_checkout_shipping_lines_stage_table.c.validation_context,
            "markup": abandoned_checkout_shipping_lines_stage_table.c.markup,
            "delivery_category": abandoned_checkout_shipping_lines_stage_table.c.delivery_category,
            "carrier_identifier": abandoned_checkout_shipping_lines_stage_table.c.carrier_identifier,
            "carrier_service_id": abandoned_checkout_shipping_lines_stage_table.c.carrier_service_id,
            "api_client_id": abandoned_checkout_shipping_lines_stage_table.c.api_client_id,
            "requested_fulfillment_service_id": abandoned_checkout_shipping_lines_stage_table.c.requested_fulfillment_service_id,
            "applied_discounts": abandoned_checkout_shipping_lines_stage_table.c.applied_discounts,
            "delivery_option_group_id": abandoned_checkout_shipping_lines_stage_table.c.delivery_option_group_id,
            "delivery_option_group_type": abandoned_checkout_shipping_lines_stage_table.c.delivery_option_group_type,
            "delivery_expectation_range": abandoned_checkout_shipping_lines_stage_table.c.delivery_expectation_range,
            "delivery_expectation_type": abandoned_checkout_shipping_lines_stage_table.c.delivery_expectation_type,
            "estimated_delivery_time_range": abandoned_checkout_shipping_lines_stage_table.c.estimated_delivery_time_range,
            "checkout_created_at": abandoned_checkout_shipping_lines_stage_table.c.checkout_created_at,
            "checkout_updated_at": abandoned_checkout_shipping_lines_stage_table.c.checkout_updated_at,
        }

        abandoned_checkout_shipping_lines_merge.when_not_matched_then_insert().values(
            **abandoned_checkout_shipping_lines_cols
        )

        abandoned_checkout_shipping_lines_merge.when_matched_then_update().values(
            **abandoned_checkout_shipping_lines_cols
        )

        # Execute merge
        alchemy_connection.execute(abandoned_checkout_shipping_lines_merge)

    if len(flat_abandoned_checkout_line_items) > 0:

        # Create stage and target tables
        abandoned_checkout_line_items_stage_table = meta.tables[
            "abandoned_checkout_line_items_stage"
        ]
        abandoned_checkout_line_items_target_table = meta.tables[
            "abandoned_checkout_line_items"
        ]

        # Structure merge
        abandoned_checkout_line_items_merge = MergeInto(
            target=abandoned_checkout_line_items_target_table,
            source=abandoned_checkout_line_items_stage_table,
            on=abandoned_checkout_line_items_target_table.c.historical_shop_abandoned_checkout_line_item_id
            == abandoned_checkout_line_items_stage_table.c.historical_shop_abandoned_checkout_line_item_id,
        )

        abandoned_checkout_line_items_cols = {
            "historical_shop_abandoned_checkout_line_item_id": abandoned_checkout_line_items_stage_table.c.historical_shop_abandoned_checkout_line_item_id,
            "shop_abandoned_checkout_line_item_id": abandoned_checkout_line_items_stage_table.c.shop_abandoned_checkout_line_item_id,
            "historical_shop_abandoned_checkout_id": abandoned_checkout_line_items_stage_table.c.historical_shop_abandoned_checkout_id,
            "shop_abandoned_checkout_id": abandoned_checkout_line_items_stage_table.c.shop_abandoned_checkout_id,
            "abandoned_checkout_id": abandoned_checkout_line_items_stage_table.c.abandoned_checkout_id,
            "abandoned_checkout_line_item_id": abandoned_checkout_line_items_stage_table.c.abandoned_checkout_line_item_id,
            "shop_id": abandoned_checkout_line_items_stage_table.c.shop_id,
            "shop_variant_id": abandoned_checkout_line_items_stage_table.c.shop_variant_id,
            "shop_product_id": abandoned_checkout_line_items_stage_table.c.shop_product_id,
            "shop_name": abandoned_checkout_line_items_stage_table.c.shop_name,
            "applied_discounts": abandoned_checkout_line_items_stage_table.c.applied_discounts,
            "discount_allocations": abandoned_checkout_line_items_stage_table.c.discount_allocations,
            "key": abandoned_checkout_line_items_stage_table.c.key,
            "destination_location_id": abandoned_checkout_line_items_stage_table.c.destination_location_id,
            "fulfillment_service": abandoned_checkout_line_items_stage_table.c.fulfillment_service,
            "is_gift_card": abandoned_checkout_line_items_stage_table.c.is_gift_card,
            "weight_grams": abandoned_checkout_line_items_stage_table.c.weight_grams,
            "origin_location_id": abandoned_checkout_line_items_stage_table.c.origin_location_id,
            "presentment_title": abandoned_checkout_line_items_stage_table.c.presentment_title,
            "presentment_variant_title": abandoned_checkout_line_items_stage_table.c.presentment_variant_title,
            "product_id": abandoned_checkout_line_items_stage_table.c.product_id,
            "properties": abandoned_checkout_line_items_stage_table.c.properties,
            "quantity": abandoned_checkout_line_items_stage_table.c.quantity,
            "is_requires_shipping": abandoned_checkout_line_items_stage_table.c.is_requires_shipping,
            "sku": abandoned_checkout_line_items_stage_table.c.sku,
            "tax_lines": abandoned_checkout_line_items_stage_table.c.tax_lines,
            "is_taxable": abandoned_checkout_line_items_stage_table.c.is_taxable,
            "title": abandoned_checkout_line_items_stage_table.c.title,
            "variant_id": abandoned_checkout_line_items_stage_table.c.variant_id,
            "variant_title": abandoned_checkout_line_items_stage_table.c.variant_title,
            "variant_price": abandoned_checkout_line_items_stage_table.c.variant_price,
            "vendor": abandoned_checkout_line_items_stage_table.c.vendor,
            "user_id": abandoned_checkout_line_items_stage_table.c.user_id,
            "shop_currency": abandoned_checkout_line_items_stage_table.c.shop_currency,
            "presentment_currency": abandoned_checkout_line_items_stage_table.c.presentment_currency,
            "price_meas_measured_type": abandoned_checkout_line_items_stage_table.c.price_meas_measured_type,
            "price_meas_quantity_value": abandoned_checkout_line_items_stage_table.c.price_meas_quantity_value,
            "price_meas_quantity_unit": abandoned_checkout_line_items_stage_table.c.price_meas_quantity_unit,
            "price_meas_reference_value": abandoned_checkout_line_items_stage_table.c.price_meas_reference_value,
            "price_meas_reference_unit": abandoned_checkout_line_items_stage_table.c.price_meas_reference_unit,
            "rank": abandoned_checkout_line_items_stage_table.c.rank,
            "compare_at_price": abandoned_checkout_line_items_stage_table.c.compare_at_price,
            "line_price": abandoned_checkout_line_items_stage_table.c.line_price,
            "price": abandoned_checkout_line_items_stage_table.c.price,
            "checkout_created_at": abandoned_checkout_line_items_stage_table.c.checkout_created_at,
            "checkout_updated_at": abandoned_checkout_line_items_stage_table.c.checkout_updated_at,
        }

        abandoned_checkout_line_items_merge.when_not_matched_then_insert().values(
            **abandoned_checkout_line_items_cols
        )

        abandoned_checkout_line_items_merge.when_matched_then_update().values(
            **abandoned_checkout_line_items_cols
        )

        # Execute merge
        alchemy_connection.execute(abandoned_checkout_line_items_merge)

    # Close connection
    alchemy_connection.close()
    logger.info("SQLAlchemy connection closed")


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[2]

# The app can be set to only run certain steps
steps_to_run = sys.argv[4]

if steps_to_run == "all":

    is_included_step = True

else:

    is_included_step = "abandoned_checkouts" in steps_to_run

if (
    __name__ == "__main__"
    and is_included_step
    and (os.getenv("RES_ENV") == "production" or dev_override == "true")
):

    # Retrieve Shopify credentials and check parameter for override; the
    # override directs the app to only run to requested sync for a single shop
    shopify_keys = get_secret("SHOPIFY_DATA_TEAM_APP_API_KEYS")
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

    # Retrieve Snowflake credentials from secrets manager
    snowflake_cred = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    snowflake_user = snowflake_cred["user"]
    snowflake_password = snowflake_cred["password"]
    snowflake_account = snowflake_cred["account"]

    if sync_type == "incremental":

        # Retrieve Snowflake timestamp before async invocation
        since_ts = get_latest_snowflake_timestamp(
            schema="SHOPIFY",
            table="ABANDONED_CHECKOUTS",
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_account=snowflake_account,
            timestamp_field="created_at",
        )

    elif sync_type == "test":

        since_ts = (datetime.utcnow() - timedelta(days=5)).strftime(
            "%Y-%m-%dT%H:%M:%S.%fZ"
        )

    else:

        since_ts = None

    # ETL setup
    # Create list to extend for response data
    output_list = []

    # Create empty lists to append to
    flat_abandoned_checkouts = []
    flat_abandoned_checkout_shipping_lines = []
    flat_abandoned_checkout_line_items = []

    # Execute async API calls
    asyncio.run(
        fetch_all_data(
            api_endpoint="checkouts",
            output_list=output_list,
            shopify_keys=shopify_keys,
            sync_type=sync_type,
            pagination_type="since_id",
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
    create_snowflake_tables()
    upsert_abandoned_checkouts_data()

    logger.info("Shopify abandoned checkouts sync complete")
