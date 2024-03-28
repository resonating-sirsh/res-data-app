import gc
import hashlib
import os
import sys
import asyncio
import snowflake.connector
from datetime import date, datetime, timedelta
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

    for customer_record in output_list:

        shop_id = customer_record["shop_id"]
        shop_name = customer_record["shop_name"]
        customer_id = customer_record["id"]
        updated_at = customer_record["updated_at"]
        shop_customer_concat = str(shop_id) + str(customer_id)
        shop_customer_id = hashlib.md5(shop_customer_concat.encode("utf-8")).hexdigest()
        historical_shop_customer_concat = (
            str(shop_id) + str(customer_id) + str(updated_at)
        )
        historical_shop_customer_id = hashlib.md5(
            historical_shop_customer_concat.encode("utf-8")
        ).hexdigest()
        email_marketing_object = customer_record.get("email_marketing_consent") or {}
        sms_marketing_object = customer_record.get("sms_marketing_consent") or {}

        flat_customers.append(
            {
                "historical_shop_customer_id": f"{historical_shop_customer_id}",
                "shop_customer_id": f"{shop_customer_id}",
                "shop_id": shop_id,
                "shop_name": f"{shop_name}",
                "customer_id": f"{customer_id}",
                "email": customer_record["email"],
                "created_at": safe_strptime(customer_record["created_at"]),
                "updated_at": safe_strptime(customer_record["updated_at"]),
                "first_name": customer_record["first_name"],
                "last_name": customer_record["last_name"],
                "orders_count": customer_record["orders_count"],
                "account_status": customer_record["state"],
                "total_spent": customer_record["total_spent"],
                "last_order_id": customer_record["last_order_id"],
                "note": customer_record["note"],
                "is_verified_email": customer_record["verified_email"],
                "is_tax_exempt": customer_record["tax_exempt"],
                "tags": customer_record["tags"],
                "last_order_name": customer_record["last_order_name"],
                "currency_iso": customer_record["currency"],
                "phone_number": customer_record["phone"],
                "tax_exemptions": str(customer_record["tax_exemptions"]),
                "email_marketing_consent_status": email_marketing_object.get("state"),
                "email_marketing_consent_opt_in_level": email_marketing_object.get(
                    "opt_in_level"
                ),
                "email_marketing_consent_updated_at": safe_strptime(
                    email_marketing_object.get("consent_updated_at")
                ),
                "sms_marketing_consent_status": sms_marketing_object.get("state"),
                "sms_marketing_consent_opt_in_level": sms_marketing_object.get(
                    "opt_in_level"
                ),
                "sms_marketing_consent_updated_at": safe_strptime(
                    sms_marketing_object.get("consent_updated_at")
                ),
                "sms_marketing_consent_collected_from": sms_marketing_object.get(
                    "consent_collected_from"
                ),
                "admin_graphql_api_id": customer_record["admin_graphql_api_id"],
            }
        )

        if len(customer_record["addresses"]) > 0:

            for address_record in customer_record["addresses"]:

                customer_address_id = address_record["id"]
                shop_customer_address_concat = str(shop_id) + str(customer_address_id)
                shop_customer_address_id = hashlib.md5(
                    shop_customer_address_concat.encode("utf-8")
                ).hexdigest()
                historical_shop_customer_address_concat = (
                    historical_shop_customer_concat + str(customer_address_id)
                )
                historical_shop_customer_address_id = hashlib.md5(
                    historical_shop_customer_address_concat.encode("utf-8")
                ).hexdigest()

                flat_customer_addresses.append(
                    {
                        "historical_shop_customer_address_id": f"{historical_shop_customer_address_id}",
                        "historical_shop_customer_id": f"{historical_shop_customer_id}",
                        "shop_customer_address_id": f"{shop_customer_address_id}",
                        "shop_id": shop_id,
                        "shop_name": f"{shop_name}",
                        "shop_customer_id": f"{shop_customer_id}",
                        "customer_address_id": f"{customer_address_id}",
                        "customer_id": address_record["customer_id"],
                        "first_name": address_record["first_name"],
                        "last_name": address_record["last_name"],
                        "company": address_record["company"],
                        "address_line_one": address_record["address1"],
                        "address_line_two": address_record["address2"],
                        "city": address_record["city"],
                        "province": address_record["province"],
                        "country": address_record["country"],
                        "zip": address_record["zip"],
                        "phone": address_record["phone"],
                        "name": address_record["name"],
                        "province_code": address_record["province_code"],
                        "country_code": address_record["country_code"],
                        "country_name": address_record["country_name"],
                        "is_default_address": address_record["default"],
                        "customer_created_at": safe_strptime(
                            customer_record["created_at"]
                        ),
                        "customer_updated_at": safe_strptime(
                            customer_record["updated_at"]
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
            create table if not exists customers (
                historical_shop_customer_id varchar,
                shop_customer_id varchar,
                shop_id integer,
                shop_name varchar,
                customer_id integer,
                email varchar,
                created_at timestamp_tz,
                updated_at timestamp_tz,                
                first_name varchar,
                last_name varchar,
                orders_count integer,
                account_status varchar,
                total_spent number(38, 2),
                last_order_id integer,
                note varchar,
                is_verified_email boolean,
                is_tax_exempt boolean,
                tags varchar,
                last_order_name varchar,
                currency_iso varchar,
                phone_number varchar, 
                tax_exemptions varchar,
                email_marketing_consent_status varchar,
                email_marketing_consent_opt_in_level varchar,
                email_marketing_consent_updated_at timestamp_tz,
                sms_marketing_consent_status varchar,
                sms_marketing_consent_opt_in_level varchar,
                sms_marketing_consent_updated_at timestamp_tz,
                sms_marketing_consent_collected_from varchar,
                admin_graphql_api_id varchar
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists customer_addresses (
                historical_shop_customer_address_id varchar,
                historical_shop_customer_id varchar,
                shop_customer_address_id varchar,
                shop_id integer,
                shop_name varchar,                
                shop_customer_id varchar,                
                customer_address_id integer,
                customer_id integer,
                first_name varchar,
                last_name varchar,                
                company varchar,                
                address_line_one varchar,                
                address_line_two varchar,                
                city varchar,                
                province varchar,                
                country varchar,                
                zip varchar,                
                phone varchar,                
                name varchar,                
                province_code varchar,                
                country_code varchar,                
                country_name varchar,                
                is_default_address boolean,
                customer_created_at timestamp_tz,
                customer_updated_at timestamp_tz                 
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_customers_data(flat_customers, flat_customer_addresses):

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    customers_dtypes = {
        "historical_shop_customer_id": VARCHAR,
        "shop_customer_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "customer_id": INTEGER,
        "email": VARCHAR,
        "created_at": TIMESTAMP(timezone=True),
        "updated_at": TIMESTAMP(timezone=True),
        "first_name": VARCHAR,
        "last_name": VARCHAR,
        "orders_count": INTEGER,
        "account_status": VARCHAR,
        "total_spent": NUMERIC(38, 2),
        "last_order_id": INTEGER,
        "note": VARCHAR,
        "is_verified_email": BOOLEAN,
        "is_tax_exempt": BOOLEAN,
        "tags": VARCHAR,
        "last_order_name": VARCHAR,
        "currency_iso": VARCHAR,
        "phone_number": VARCHAR,
        "tax_exemptions": VARCHAR,
        "email_marketing_consent_status": VARCHAR,
        "email_marketing_consent_opt_in_level": VARCHAR,
        "email_marketing_consent_updated_at": TIMESTAMP(timezone=True),
        "sms_marketing_consent_status": VARCHAR,
        "sms_marketing_consent_opt_in_level": VARCHAR,
        "sms_marketing_consent_updated_at": TIMESTAMP(timezone=True),
        "sms_marketing_consent_collected_from": VARCHAR,
        "admin_graphql_api_id": VARCHAR,
    }
    customer_addresses_dtypes = {
        "historical_shop_customer_address_id": VARCHAR,
        "historical_shop_customer_id": VARCHAR,
        "shop_customer_address_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "shop_customer_id": VARCHAR,
        "customer_address_id": INTEGER,
        "customer_id": INTEGER,
        "first_name": VARCHAR,
        "last_name": VARCHAR,
        "company": VARCHAR,
        "address_line_one": VARCHAR,
        "address_line_two": VARCHAR,
        "city": VARCHAR,
        "province": VARCHAR,
        "country": VARCHAR,
        "zip": VARCHAR,
        "phone": VARCHAR,
        "name": VARCHAR,
        "province_code": VARCHAR,
        "country_code": VARCHAR,
        "country_name": VARCHAR,
        "is_default_address": BOOLEAN,
        "customer_created_at": TIMESTAMP(timezone=True),
        "customer_updated_at": TIMESTAMP(timezone=True),
    }

    dict_to_sql(flat_customers, "customers_stage", engine, dtype=customers_dtypes)
    dict_to_sql(
        flat_customer_addresses,
        "customer_addresses_stage",
        engine,
        dtype=customer_addresses_dtypes,
    )

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from each stage table to corresponding target table")

    if len(flat_customers) > 0:

        # Create stage and target tables
        customers_stage_table = meta.tables["customers_stage"]
        customers_target_table = meta.tables["customers"]

        # Structure merge
        customers_merge = MergeInto(
            target=customers_target_table,
            source=customers_stage_table,
            on=customers_target_table.c.historical_shop_customer_id
            == customers_stage_table.c.historical_shop_customer_id,
        )

        customers_cols = {
            "historical_shop_customer_id": customers_stage_table.c.historical_shop_customer_id,
            "shop_customer_id": customers_stage_table.c.shop_customer_id,
            "customer_id": customers_stage_table.c.customer_id,
            "shop_id": customers_stage_table.c.shop_id,
            "shop_name": customers_stage_table.c.shop_name,
            "email": customers_stage_table.c.email,
            "created_at": customers_stage_table.c.created_at,
            "updated_at": customers_stage_table.c.updated_at,
            "first_name": customers_stage_table.c.first_name,
            "last_name": customers_stage_table.c.last_name,
            "orders_count": customers_stage_table.c.orders_count,
            "account_status": customers_stage_table.c.account_status,
            "total_spent": customers_stage_table.c.total_spent,
            "last_order_id": customers_stage_table.c.last_order_id,
            "note": customers_stage_table.c.note,
            "is_verified_email": customers_stage_table.c.is_verified_email,
            "is_tax_exempt": customers_stage_table.c.is_tax_exempt,
            "tags": customers_stage_table.c.tags,
            "last_order_name": customers_stage_table.c.last_order_name,
            "currency_iso": customers_stage_table.c.currency_iso,
            "phone_number": customers_stage_table.c.phone_number,
            "tax_exemptions": customers_stage_table.c.tax_exemptions,
            "email_marketing_consent_status": customers_stage_table.c.email_marketing_consent_status,
            "email_marketing_consent_opt_in_level": customers_stage_table.c.email_marketing_consent_opt_in_level,
            "email_marketing_consent_updated_at": customers_stage_table.c.email_marketing_consent_updated_at,
            "sms_marketing_consent_status": customers_stage_table.c.sms_marketing_consent_status,
            "sms_marketing_consent_opt_in_level": customers_stage_table.c.sms_marketing_consent_opt_in_level,
            "sms_marketing_consent_updated_at": customers_stage_table.c.sms_marketing_consent_updated_at,
            "sms_marketing_consent_collected_from": customers_stage_table.c.sms_marketing_consent_collected_from,
            "admin_graphql_api_id": customers_stage_table.c.admin_graphql_api_id,
        }

        customers_merge.when_not_matched_then_insert().values(**customers_cols)

        customers_merge.when_matched_then_update().values(**customers_cols)

        # Execute merge
        alchemy_connection.execute(customers_merge)

    if len(flat_customer_addresses) > 0:

        # Create stage and target tables
        customer_addresses_stage_table = meta.tables["customer_addresses_stage"]
        customer_addresses_target_table = meta.tables["customer_addresses"]

        # Structure merge
        customer_addresses_merge = MergeInto(
            target=customer_addresses_target_table,
            source=customer_addresses_stage_table,
            on=customer_addresses_target_table.c.historical_shop_customer_address_id
            == customer_addresses_stage_table.c.historical_shop_customer_address_id,
        )

        customer_addresses_cols = {
            "historical_shop_customer_address_id": customer_addresses_stage_table.c.historical_shop_customer_address_id,
            "historical_shop_customer_id": customer_addresses_stage_table.c.historical_shop_customer_id,
            "shop_customer_address_id": customer_addresses_stage_table.c.shop_customer_address_id,
            "customer_address_id": customer_addresses_stage_table.c.customer_address_id,
            "shop_id": customer_addresses_stage_table.c.shop_id,
            "shop_name": customer_addresses_stage_table.c.shop_name,
            "customer_id": customer_addresses_stage_table.c.customer_id,
            "first_name": customer_addresses_stage_table.c.first_name,
            "last_name": customer_addresses_stage_table.c.last_name,
            "company": customer_addresses_stage_table.c.company,
            "address_line_one": customer_addresses_stage_table.c.address_line_one,
            "address_line_two": customer_addresses_stage_table.c.address_line_two,
            "city": customer_addresses_stage_table.c.city,
            "province": customer_addresses_stage_table.c.province,
            "country": customer_addresses_stage_table.c.country,
            "zip": customer_addresses_stage_table.c.zip,
            "phone": customer_addresses_stage_table.c.phone,
            "name": customer_addresses_stage_table.c.name,
            "province_code": customer_addresses_stage_table.c.province_code,
            "country_code": customer_addresses_stage_table.c.country_code,
            "country_name": customer_addresses_stage_table.c.country_name,
            "is_default_address": customer_addresses_stage_table.c.is_default_address,
            "customer_created_at": customer_addresses_stage_table.c.customer_created_at,
            "customer_updated_at": customer_addresses_stage_table.c.customer_updated_at,
        }

        customer_addresses_merge.when_not_matched_then_insert().values(
            **customer_addresses_cols
        )

        customer_addresses_merge.when_matched_then_update().values(
            **customer_addresses_cols
        )

        # Execute merge
        alchemy_connection.execute(customer_addresses_merge)

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

    is_included_step = "customers" in steps_to_run

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
            table="CUSTOMERS",
            snowflake_user=snowflake_user,
            snowflake_password=snowflake_password,
            snowflake_account=snowflake_account,
            timestamp_field="updated_at",
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
    flat_customers = []
    flat_customer_addresses = []

    # There are too many customers at times to retrieve at once without running
    # into memory leak issues. API calls are broken down into four week chunks
    # for full syncs
    if sync_type == "full" or since_ts is None:

        # Date parameters for the Orders endpoint are inclusive
        today = date.today()
        start_date = date(2015, 1, 1)

        # Domain rebecca-minkoff has a very large amount of customers that were
        # created in the last quarter of 2017 (presumably from a large import or
        # initial launch of some kind). If that domain is part
        # of the sync then the time increments in September 2017 need to be 1
        # day and increments from October through December 2017 need to be 5 days.
        # Otherwise they can be 30 days.
        end_date = start_date + timedelta(days=30)

        while start_date < today:

            additional_params = {
                "created_at_min": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "created_at_max": end_date.strftime("%Y-%m-%dT%H:%M:%S"),
            }
            logger.info(
                f"Syncing data from {start_date.strftime('%Y-%m-%dT%H:%M:%S')} to {end_date.strftime('%Y-%m-%dT%H:%M:%S')}"
            )

            # Execute async API calls in chunks
            asyncio.run(
                fetch_all_data(
                    api_endpoint="customers",
                    output_list=output_list,
                    shopify_keys=shopify_keys,
                    sync_type=sync_type,
                    additional_params=additional_params,
                    pagination_type="next_link",
                )
            )

            if len(output_list) > 0:

                # Flatten data from all API calls
                flatten_records(output_list)

                # Add to Snowflake
                create_snowflake_tables()
                upsert_customers_data(flat_customers, flat_customer_addresses)

            else:

                logger.info(
                    f"No data from {start_date.strftime('%Y-%m-%dT%H:%M:%S')} to {end_date.strftime('%Y-%m-%dT%H:%M:%S')}"
                )

            # Delete tables from memory; clean up
            del output_list
            del flat_customers
            del flat_customer_addresses
            gc.collect()

            # Reset lists
            output_list = []
            flat_customers = []
            flat_customer_addresses = []

            # Increment times
            start_date = end_date

            if (
                (start_date + timedelta(days=30)).year == 2017
                and (start_date + timedelta(days=30)).month >= 9
                and start_date.month < 9
                and start_date != date(2017, 8, 31)
            ):

                end_date = date(2017, 8, 31)

            elif (
                any(
                    "rebecca-minkoff" == key_dict["shop_domain_name"]
                    for key_dict in shopify_keys
                )
                and start_date.year == 2017
                and start_date.month >= 9
            ) or start_date == date(2017, 8, 31):

                if start_date.month <= 9:

                    end_date = start_date + timedelta(days=1)

                elif start_date.month > 9:

                    end_date = start_date + timedelta(days=5)

            else:

                end_date = min(start_date + timedelta(days=30), today)

    else:

        # Execute async API calls
        asyncio.run(
            fetch_all_data(
                api_endpoint="customers",
                output_list=output_list,
                shopify_keys=shopify_keys,
                sync_type=sync_type,
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
        create_snowflake_tables()
        upsert_customers_data(flat_customers, flat_customer_addresses)

    logger.info("Shopify customers sync complete")
