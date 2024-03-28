import gc
import hashlib
import os
import sys
import aiohttp
import asyncio
import snowflake.connector
from snowflake.sqlalchemy import MergeInto
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import *
from helper_functions import (
    get_secret,
    dict_to_sql,
    safe_strptime,
)
from res.utils import logger


# Create synchronous function to flatten info
def flatten_records(output_list):

    logger.info(f"Proceeding to flatten records")

    for shops_record in output_list:

        shop_id = shops_record["id"]
        updated_at = shops_record["updated_at"]
        historical_shop_concat = str(shop_id) + str(updated_at)
        historical_shop_id = hashlib.md5(
            historical_shop_concat.encode("utf-8")
        ).hexdigest()

        flat_shops.append(
            {
                "historical_shop_id": historical_shop_id,
                "shop_id": shop_id,
                "address_one": shops_record.get("address1"),
                "address_two": shops_record.get("address2"),
                "is_auto_configured_tax_inclusivity": shops_record.get(
                    "auto_configure_tax_inclusivity"
                ),
                "is_checkout_api_supported": shops_record.get("checkout_api_supported"),
                "city": shops_record.get("city"),
                "cookie_consent_level": shops_record.get("cookie_consent_level"),
                "country_code": shops_record.get("country_code"),
                "country_name": shops_record.get("country_name"),
                "country": shops_record.get("country"),
                "has_per_county_taxes": shops_record.get("county_taxes"),
                "created_at": safe_strptime(shops_record.get("created_at")),
                "default_currency": shops_record.get("currency"),
                "customer_comm_email": shops_record.get("customer_email"),
                "domain": shops_record.get("domain"),
                "is_eligible_for_card_reader_giveaway": shops_record.get(
                    "eligible_for_card_reader_giveaway"
                ),
                "is_eligible_for_payments": shops_record.get("eligible_for_payments"),
                "email": shops_record.get("email"),
                "enabled_presentment_currencies": str(
                    shops_record.get("enabled_presentment_currencies")
                ),
                "google_apps_domain": shops_record.get("google_apps_domain"),
                "is_google_apps_login_enabled": shops_record.get(
                    "google_apps_login_enabled"
                ),
                "has_discounts": shops_record.get("has_discounts"),
                "has_gift_cards": shops_record.get("has_gift_cards"),
                "has_storefront": shops_record.get("has_storefront"),
                "iana_timezone": shops_record.get("iana_timezone"),
                "latitude": shops_record.get("latitude"),
                "longitude": shops_record.get("longitude"),
                "is_marketing_sms_consent_enabled_at_checkout": shops_record.get(
                    "marketing_sms_consent_enabled_at_checkout"
                ),
                "money_format": shops_record.get("money_format"),
                "money_in_emails_format": shops_record.get("money_in_emails_format"),
                "money_with_currency_format": shops_record.get(
                    "money_with_currency_format"
                ),
                "money_with_currency_in_emails_format": shops_record.get(
                    "money_with_currency_in_emails_format"
                ),
                "myshopify_domain": shops_record.get("myshopify_domain"),
                "name": shops_record.get("name"),
                "is_password_enabled": shops_record.get("password_enabled"),
                "phone": shops_record.get("phone"),
                "plan_display_name": shops_record.get("plan_display_name"),
                "plan_name": shops_record.get("plan_name"),
                "is_pre_launch_enabled": shops_record.get("pre_launch_enabled"),
                "primary_locale": shops_record.get("primary_locale"),
                "province_code": shops_record.get("province_code"),
                "province": shops_record.get("province"),
                "is_requires_extra_payments_agreement": shops_record.get(
                    "requires_extra_payments_agreement"
                ),
                "has_outstanding_setup_steps": shops_record.get("setup_required"),
                "shop_owner": shops_record.get("shop_owner"),
                "referral_account": shops_record.get("source"),
                "is_tax_shipping": shops_record.get("tax_shipping"),
                "is_taxes_included": shops_record.get("taxes_included"),
                "timezone": shops_record.get("timezone"),
                "is_transactional_sms_disabled": shops_record.get(
                    "transactional_sms_disabled"
                ),
                "updated_at": safe_strptime(updated_at),
                "visitor_tracking_consent_preference": shops_record.get(
                    "visitor_tracking_consent_preference"
                ),
                "weight_unit": shops_record.get("weight_unit"),
                "zip": shops_record.get("zip"),
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
            create table if not exists shops (
                historical_shop_id varchar,
                shop_id integer,
                address_one varchar,
                address_two varchar,
                is_auto_configured_tax_inclusivity boolean,
                is_checkout_api_supported boolean,
                city varchar,
                cookie_consent_level varchar,
                country_code varchar,
                country_name varchar,
                country varchar,
                has_per_county_taxes boolean,
                created_at timestamp_tz,
                default_currency varchar,
                customer_comm_email varchar,
                domain varchar,
                is_eligible_for_card_reader_giveaway boolean,
                is_eligible_for_payments boolean,
                email varchar,
                enabled_presentment_currencies varchar,
                google_apps_domain varchar,
                is_google_apps_login_enabled boolean,
                has_discounts boolean,
                has_gift_cards boolean,
                has_storefront boolean,
                iana_timezone varchar,
                latitude number(16, 6),
                longitude number(16, 6),
                is_marketing_sms_consent_enabled_at_checkout boolean,
                money_format varchar,
                money_in_emails_format varchar,
                money_with_currency_format varchar,
                money_with_currency_in_emails_format varchar,
                myshopify_domain varchar,
                name varchar,
                is_password_enabled boolean,
                phone varchar,
                plan_display_name varchar,
                plan_name varchar,
                is_pre_launch_enabled boolean,
                primary_locale varchar,
                province_code varchar,
                province varchar,
                is_requires_extra_payments_agreement boolean,
                has_outstanding_setup_steps boolean,
                shop_owner varchar,
                referral_account varchar,
                is_tax_shipping boolean,
                is_taxes_included boolean,
                timezone varchar,
                is_transactional_sms_disabled boolean,
                updated_at timestamp_tz,
                visitor_tracking_consent_preference varchar,
                weight_unit varchar,
                zip varchar
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_shops_data(flat_shops):

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    dtypes = {
        "historical_shop_id": VARCHAR,
        "shop_id": INTEGER,
        "address_one": VARCHAR,
        "address_two": VARCHAR,
        "is_auto_configured_tax_inclusivity": BOOLEAN,
        "is_checkout_api_supported": BOOLEAN,
        "city": VARCHAR,
        "cookie_consent_level": VARCHAR,
        "country_code": VARCHAR,
        "country_name": VARCHAR,
        "country": VARCHAR,
        "has_per_county_taxes": BOOLEAN,
        "created_at": TIMESTAMP(timezone=True),
        "default_currency": VARCHAR,
        "customer_comm_email": VARCHAR,
        "domain": VARCHAR,
        "is_eligible_for_card_reader_giveaway": BOOLEAN,
        "is_eligible_for_payments": BOOLEAN,
        "email": VARCHAR,
        "enabled_presentment_currencies": VARCHAR,
        "google_apps_domain": VARCHAR,
        "is_google_apps_login_enabled": BOOLEAN,
        "has_discounts": BOOLEAN,
        "has_gift_cards": BOOLEAN,
        "has_storefront": BOOLEAN,
        "iana_timezone": VARCHAR,
        "latitude": NUMERIC(16, 6),
        "longitude": NUMERIC(16, 6),
        "is_marketing_sms_consent_enabled_at_checkout": BOOLEAN,
        "money_format": VARCHAR,
        "money_in_emails_format": VARCHAR,
        "money_with_currency_format": VARCHAR,
        "money_with_currency_in_emails_format": VARCHAR,
        "myshopify_domain": VARCHAR,
        "name": VARCHAR,
        "is_password_enabled": BOOLEAN,
        "phone": VARCHAR,
        "plan_display_name": VARCHAR,
        "plan_name": VARCHAR,
        "is_pre_launch_enabled": BOOLEAN,
        "primary_locale": VARCHAR,
        "province_code": VARCHAR,
        "province": VARCHAR,
        "is_requires_extra_payments_agreement": BOOLEAN,
        "has_outstanding_setup_steps": BOOLEAN,
        "shop_owner": VARCHAR,
        "referral_account": VARCHAR,
        "is_tax_shipping": BOOLEAN,
        "is_taxes_included": BOOLEAN,
        "timezone": VARCHAR,
        "is_transactional_sms_disabled": BOOLEAN,
        "updated_at": TIMESTAMP(timezone=True),
        "visitor_tracking_consent_preference": VARCHAR,
        "weight_unit": VARCHAR,
        "zip": VARCHAR,
    }

    dict_to_sql(flat_shops, "shops_stage", engine, dtypes)

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stage table to target table")

    if len(flat_shops) > 0:

        # Create stage and target tables
        stage_table = meta.tables["shops_stage"]
        target_table = meta.tables["shops"]

        # Structure merge
        shops_merge = MergeInto(
            target=target_table,
            source=stage_table,
            on=target_table.c.historical_shop_id == stage_table.c.historical_shop_id,
        )

        shops_cols = {
            "historical_shop_id": stage_table.c.historical_shop_id,
            "shop_id": stage_table.c.shop_id,
            "address_one": stage_table.c.address_one,
            "address_two": stage_table.c.address_two,
            "is_auto_configured_tax_inclusivity": stage_table.c.is_auto_configured_tax_inclusivity,
            "is_checkout_api_supported": stage_table.c.is_checkout_api_supported,
            "city": stage_table.c.city,
            "cookie_consent_level": stage_table.c.cookie_consent_level,
            "country_code": stage_table.c.country_code,
            "country_name": stage_table.c.country_name,
            "country": stage_table.c.country,
            "has_per_county_taxes": stage_table.c.has_per_county_taxes,
            "created_at": stage_table.c.created_at,
            "default_currency": stage_table.c.default_currency,
            "customer_comm_email": stage_table.c.customer_comm_email,
            "domain": stage_table.c.domain,
            "is_eligible_for_card_reader_giveaway": stage_table.c.is_eligible_for_card_reader_giveaway,
            "is_eligible_for_payments": stage_table.c.is_eligible_for_payments,
            "email": stage_table.c.email,
            "enabled_presentment_currencies": stage_table.c.enabled_presentment_currencies,
            "google_apps_domain": stage_table.c.google_apps_domain,
            "is_google_apps_login_enabled": stage_table.c.is_google_apps_login_enabled,
            "has_discounts": stage_table.c.has_discounts,
            "has_gift_cards": stage_table.c.has_gift_cards,
            "has_storefront": stage_table.c.has_storefront,
            "iana_timezone": stage_table.c.iana_timezone,
            "latitude": stage_table.c.latitude,
            "longitude": stage_table.c.longitude,
            "is_marketing_sms_consent_enabled_at_checkout": stage_table.c.is_marketing_sms_consent_enabled_at_checkout,
            "money_format": stage_table.c.money_format,
            "money_in_emails_format": stage_table.c.money_in_emails_format,
            "money_with_currency_format": stage_table.c.money_with_currency_format,
            "money_with_currency_in_emails_format": stage_table.c.money_with_currency_in_emails_format,
            "myshopify_domain": stage_table.c.myshopify_domain,
            "name": stage_table.c.name,
            "is_password_enabled": stage_table.c.is_password_enabled,
            "phone": stage_table.c.phone,
            "plan_display_name": stage_table.c.plan_display_name,
            "plan_name": stage_table.c.plan_name,
            "is_pre_launch_enabled": stage_table.c.is_pre_launch_enabled,
            "primary_locale": stage_table.c.primary_locale,
            "province_code": stage_table.c.province_code,
            "province": stage_table.c.province,
            "is_requires_extra_payments_agreement": stage_table.c.is_requires_extra_payments_agreement,
            "has_outstanding_setup_steps": stage_table.c.has_outstanding_setup_steps,
            "shop_owner": stage_table.c.shop_owner,
            "referral_account": stage_table.c.referral_account,
            "is_tax_shipping": stage_table.c.is_tax_shipping,
            "is_taxes_included": stage_table.c.is_taxes_included,
            "timezone": stage_table.c.timezone,
            "is_transactional_sms_disabled": stage_table.c.is_transactional_sms_disabled,
            "updated_at": stage_table.c.updated_at,
            "visitor_tracking_consent_preference": stage_table.c.visitor_tracking_consent_preference,
            "weight_unit": stage_table.c.weight_unit,
            "zip": stage_table.c.zip,
        }

        shops_merge.when_not_matched_then_insert().values(**shops_cols)

        shops_merge.when_matched_then_update().values(**shops_cols)

        # Execute merge
        alchemy_connection.execute(shops_merge)

    # Close connection
    alchemy_connection.close()
    logger.info("SQLAlchemy connection closed")


# This endpoint cannot use the get_shopify_data function from helper_functions
# because the endpoint only returns one result and not a list of records
async def get_shop_data(
    shopify_access_token: str,
    shop_domain: str,
    client_session,
    output_list,
):

    logger.info(f"Beginning shop config API call for shop domain {shop_domain}")

    # Set constant request components
    headers = {"X-Shopify-Access-Token": f"{shopify_access_token}"}

    while True:

        # Shop name and id API request
        async with client_session.get(
            url=f"https://{shop_domain}.myshopify.com/admin/api/2022-10/shop.json",
            headers=headers,
        ) as response:

            try:

                response.raise_for_status()
                data = (await response.json(encoding="UTF-8")).get("shop")

                break

            except:

                logger.warning(
                    f"{response.status} {response.reason} for Shop domain {shop_domain}"
                )

                if response.status == 429:

                    wait_time = int(response.headers.get("Retry-After"))
                    logger.info(
                        f"Rate limit reached; retrying after {wait_time} seconds"
                    )
                    asyncio.sleep(wait_time)

                    continue

                else:

                    # End function for shop if API call fails after wait with non
                    # too many requests response code
                    return

    if len(data) > 0:

        # Add the shop's data to the collection of all responses
        output_list.append(data)


async def shop_data_wrapper(output_list, shopify_keys):

    async with aiohttp.ClientSession() as client_session:

        futures = [
            get_shop_data(
                shopify_access_token=shop["shop_app_api_key"],
                shop_domain=shop["shop_domain_name"],
                client_session=client_session,
                output_list=output_list,
            )
            for shop in shopify_keys
        ]

        await asyncio.gather(*futures)


# App will only run in production; running in dev environments requires an override
dev_override = sys.argv[2]

# The app can be set to only run certain steps
steps_to_run = sys.argv[4]

if steps_to_run == "all":

    is_included_step = True

else:

    is_included_step = "shops" in steps_to_run

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

    # Retrieve Snowflake credentials from secrets manager
    snowflake_cred = get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    snowflake_user = snowflake_cred["user"]
    snowflake_password = snowflake_cred["password"]
    snowflake_account = snowflake_cred["account"]

    # ETL setup
    # Create list to extend for response data
    output_list = []

    # Create empty lists to append to
    flat_shops = []

    # Set target database according to environment
    if os.getenv("RES_ENV") == "production":

        database = "raw"

    else:

        database = "raw_dev"

    # Execute async API calls. This API call retrieves one record and is always
    # a full sync
    asyncio.run(shop_data_wrapper(output_list, shopify_keys))

    # Flatten data from all API calls
    flatten_records(output_list)

    # Clean up
    del output_list
    gc.collect()

    # Add to Snowflake
    create_snowflake_tables()
    upsert_shops_data(flat_shops=flat_shops)

    logger.info("Shopify shops sync complete")
