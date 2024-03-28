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

    for product in output_list:

        # Using non-safe get; important to raise Exceptions here
        shop_id = product["shop_id"]
        shop_name = product["shop_name"]
        shop_currency = product["shop_currency"]

        # Unlike many other objects within Shopify, products' primary keys are
        # unique across all shops
        product_id = product["id"]
        updated_at = product["updated_at"]
        historical_product_concat = str(product_id) + str(updated_at)
        historical_product_id = hashlib.md5(
            historical_product_concat.encode("utf-8")
        ).hexdigest()

        flat_products.append(
            {
                "historical_product_id": f"{historical_product_id}",
                "shop_id": shop_id,
                "shop_name": f"{shop_name}",
                "product_id": product_id,
                "product_name": product["title"],
                "body_html": product["body_html"],
                "vendor": product["vendor"],
                "product_type": product["product_type"],
                "created_at": safe_strptime(product["created_at"]),
                "slug": product["handle"],
                "updated_at": safe_strptime(product["updated_at"]),
                "published_at": safe_strptime(product["published_at"]),
                "template_suffix": product["template_suffix"],
                "status": product["status"],
                "published_scope": product["published_scope"],
                "tags": str(product["tags"].split(", ")),
                "admin_graphql_api_id": product["admin_graphql_api_id"],
            }
        )

        if len(product.get("variants", [])) > 0:

            for variant in product["variants"]:

                variant_id = variant["id"]
                updated_at = variant["updated_at"]
                historical_variant_concat = str(variant_id) + str(updated_at)
                historical_variant_id = hashlib.md5(
                    historical_variant_concat.encode("utf-8")
                ).hexdigest()

                # The parameter tax_code applies only to the stores that have
                # the Avalara AvaTax app installed
                tax_code = variant.get("tax_code")

                flat_variants.append(
                    {
                        "historical_variant_id": f"{historical_variant_id}",
                        "shop_id": shop_id,
                        "shop_name": f"{shop_name}",
                        "variant_id": f"{variant_id}",
                        "product_id": product_id,
                        "title": variant["title"],
                        "shop_currency": shop_currency,
                        "price": variant["price"],
                        "sku": variant["sku"],
                        "position": variant["position"],
                        "inventory_policy": variant["inventory_policy"],
                        "compare_at_price": variant["compare_at_price"],
                        "fulfillment_service_type": variant["fulfillment_service"],
                        "inventory_management": variant["inventory_management"],
                        "option_one": variant["option1"],
                        "option_two": variant["option2"],
                        "option_three": variant["option3"],
                        "created_at": safe_strptime(variant["created_at"]),
                        "updated_at": safe_strptime(variant["updated_at"]),
                        "is_taxable": variant["taxable"],
                        "barcode": variant["barcode"],
                        "weight_grams": variant["grams"],
                        "image_id": variant["image_id"],
                        "weight": variant["weight"],
                        "weight_unit": variant["weight_unit"],
                        "inventory_item_id": variant["inventory_item_id"],
                        "inventory_quantity": variant["inventory_quantity"],
                        "old_inventory_quantity": variant["old_inventory_quantity"],
                        "tax_code": tax_code,
                        "is_requires_shipping": variant["requires_shipping"],
                        "admin_graphql_api_id": variant["admin_graphql_api_id"],
                    }
                )

        if len(product.get("options", [])) > 0:

            for option in product["options"]:

                option_id = option["id"]
                historical_product_option_concat = str(historical_product_concat) + str(
                    option_id
                )
                historical_product_option_id = hashlib.md5(
                    historical_product_option_concat.encode("utf-8")
                ).hexdigest()

                flat_product_options.append(
                    {
                        "historical_product_option_id": f"{historical_product_option_id}",
                        "historical_product_id": f"{historical_product_id}",
                        "shop_id": shop_id,
                        "shop_name": shop_name,
                        "product_id": product_id,
                        "option_id": option_id,
                        "name": option["name"],
                        "position": option["position"],
                        "option_values": str(option["values"]),
                        "product_created_at": safe_strptime(product["created_at"]),
                        "product_updated_at": safe_strptime(product["updated_at"]),
                    }
                )

        if len(product.get("images", [])) > 0:

            for image in product["images"]:

                image_id = image["id"]
                updated_at = image["updated_at"]
                historical_product_image_concat = str(image) + str(updated_at)
                historical_product_image_id = hashlib.md5(
                    historical_product_image_concat.encode("utf-8")
                ).hexdigest()

                flat_product_images.append(
                    {
                        "historical_product_image_id": f"{historical_product_image_id}",
                        "shop_id": shop_id,
                        "shop_name": f"{shop_name}",
                        "product_id": product_id,
                        "image_id": image_id,
                        "position": image["position"],
                        "created_at": safe_strptime(image["created_at"]),
                        "updated_at": safe_strptime(image["updated_at"]),
                        "alt_text": image["alt"],
                        "width": image["width"],
                        "height": image["height"],
                        "image_url": image["src"],
                        "variant_ids": str(image["variant_ids"]),
                        "admin_graphql_api_id": image["admin_graphql_api_id"],
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
            create table if not exists products (
                historical_product_id varchar,
                shop_id integer,
                shop_name varchar,
                product_id integer,
                product_name varchar,
                body_html varchar,
                vendor varchar,
                product_type varchar,
                created_at timestamp_tz,
                slug varchar,
                updated_at timestamp_tz,
                published_at timestamp_tz,
                template_suffix varchar,
                status varchar,
                published_scope varchar,
                tags varchar,
                admin_graphql_api_id varchar
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists variants (
                historical_variant_id varchar,
                shop_id integer,
                shop_name varchar,
                variant_id integer,
                product_id integer,
                title varchar,
                shop_currency varchar,
                price number(38, 2),
                sku varchar,
                position integer,
                inventory_policy varchar,
                compare_at_price number(38, 2),
                fulfillment_service_type varchar,
                inventory_management varchar,
                option_one varchar,
                option_two varchar,
                option_three varchar,
                created_at timestamp_tz,
                updated_at timestamp_tz,
                is_taxable boolean,
                barcode varchar,
                weight_grams number(32, 4),
                image_id integer,
                weight number(32, 4),
                weight_unit varchar,
                inventory_item_id integer,
                inventory_quantity integer,
                old_inventory_quantity integer,
                tax_code varchar,
                is_requires_shipping boolean,
                admin_graphql_api_id varchar           
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists product_options (
                historical_product_option_id varchar,
                historical_product_id varchar,
                shop_id integer,
                shop_name varchar,
                product_id integer,
                option_id integer,
                name varchar,
                position integer,
                option_values varchar,
                product_created_at timestamp_tz,
                product_updated_at timestamp_tz                                   
            )
        """
    )
    base_connection.cursor().execute(
        """
            create table if not exists product_images (
                historical_product_image_id varchar,
                shop_id integer,
                shop_name varchar,
                product_id integer,
                image_id integer,
                position integer,
                created_at timestamp_tz,
                updated_at timestamp_tz,
                alt_text varchar,
                width integer,
                height integer,
                image_url varchar,
                variant_ids varchar,
                admin_graphql_api_id varchar                 
            )
        """
    )

    # Close base connection
    base_connection.close()
    logger.info("Snowflake for python connection closed")


def upsert_products_data():

    # Create a SQLAlchemy connection to the Snowflake database
    logger.info("Creating SQLAlchemy Snowflake session")
    engine = create_engine(
        f"snowflake://{snowflake_user}:{snowflake_password}@{snowflake_account}/{database}/shopify?warehouse=loader_wh"
    )
    session = sessionmaker(bind=engine)()
    alchemy_connection = engine.connect()

    # Create explicit datatype dicts
    products_dtypes = {
        "historical_product_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "product_id": INTEGER,
        "product_name": VARCHAR,
        "body_html": VARCHAR,
        "vendor": VARCHAR,
        "product_type": VARCHAR,
        "created_at": TIMESTAMP(timezone=True),
        "slug": VARCHAR,
        "updated_at": TIMESTAMP(timezone=True),
        "published_at": TIMESTAMP(timezone=True),
        "template_suffix": VARCHAR,
        "status": VARCHAR,
        "published_scope": VARCHAR,
        "tags": VARCHAR,
        "admin_graphql_api_id": VARCHAR,
    }
    variants_dtypes = {
        "historical_variant_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "variant_id": INTEGER,
        "product_id": INTEGER,
        "title": VARCHAR,
        "shop_currency": VARCHAR,
        "price": NUMERIC(38, 2),
        "sku": VARCHAR,
        "position": INTEGER,
        "inventory_policy": VARCHAR,
        "compare_at_price": NUMERIC(38, 2),
        "fulfillment_service_type": VARCHAR,
        "inventory_management": VARCHAR,
        "option_one": VARCHAR,
        "option_two": VARCHAR,
        "option_three": VARCHAR,
        "created_at": TIMESTAMP(timezone=True),
        "updated_at": TIMESTAMP(timezone=True),
        "is_taxable": BOOLEAN,
        "barcode": VARCHAR,
        "weight_grams": NUMERIC(32, 4),
        "image_id": INTEGER,
        "weight": NUMERIC(32, 4),
        "weight_unit": VARCHAR,
        "inventory_item_id": INTEGER,
        "inventory_quantity": INTEGER,
        "old_inventory_quantity": INTEGER,
        "tax_code": VARCHAR,
        "is_requires_shipping": BOOLEAN,
        "admin_graphql_api_id": VARCHAR,
    }
    product_options_dtypes = {
        "historical_product_option_id": VARCHAR,
        "historical_product_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "product_id": INTEGER,
        "option_id": INTEGER,
        "name": VARCHAR,
        "position": INTEGER,
        "option_values": VARCHAR,
        "product_created_at": TIMESTAMP(timezone=True),
        "product_updated_at": TIMESTAMP(timezone=True),
    }
    product_images_dtypes = {
        "historical_product_image_id": VARCHAR,
        "shop_id": INTEGER,
        "shop_name": VARCHAR,
        "product_id": INTEGER,
        "image_id": INTEGER,
        "position": INTEGER,
        "created_at": TIMESTAMP(timezone=True),
        "updated_at": TIMESTAMP(timezone=True),
        "alt_text": VARCHAR,
        "width": INTEGER,
        "height": INTEGER,
        "image_url": VARCHAR,
        "variant_ids": VARCHAR,
        "admin_graphql_api_id": VARCHAR,
    }

    dict_to_sql(flat_products, "products_stage", engine, dtype=products_dtypes)
    dict_to_sql(flat_variants, "variants_stage", engine, dtype=variants_dtypes)
    dict_to_sql(
        flat_product_options,
        "product_options_stage",
        engine,
        dtype=product_options_dtypes,
    )
    dict_to_sql(
        flat_product_images, "product_images_stage", engine, dtype=product_images_dtypes
    )

    # Bind SQLAlchemy session metadata
    meta = MetaData()
    meta.reflect(bind=session.bind)

    logger.info("Merging data from stages table to corresponding target tables")

    if len(flat_products) > 0:

        # Create stage and target tables
        products_stage_table = meta.tables["products_stage"]
        products_target_table = meta.tables["products"]

        # Structure merge
        products_merge = MergeInto(
            target=products_target_table,
            source=products_stage_table,
            on=products_target_table.c.historical_product_id
            == products_stage_table.c.historical_product_id,
        )

        products_cols = {
            "historical_product_id": products_stage_table.c.historical_product_id,
            "shop_id": products_stage_table.c.shop_id,
            "shop_name": products_stage_table.c.shop_name,
            "product_id": products_stage_table.c.product_id,
            "product_name": products_stage_table.c.product_name,
            "body_html": products_stage_table.c.body_html,
            "vendor": products_stage_table.c.vendor,
            "product_type": products_stage_table.c.product_type,
            "created_at": products_stage_table.c.created_at,
            "slug": products_stage_table.c.slug,
            "updated_at": products_stage_table.c.updated_at,
            "published_at": products_stage_table.c.published_at,
            "template_suffix": products_stage_table.c.template_suffix,
            "status": products_stage_table.c.status,
            "published_scope": products_stage_table.c.published_scope,
            "tags": products_stage_table.c.tags,
            "admin_graphql_api_id": products_stage_table.c.admin_graphql_api_id,
        }

        products_merge.when_not_matched_then_insert().values(**products_cols)

        products_merge.when_matched_then_update().values(**products_cols)

        # Execute merge
        alchemy_connection.execute(products_merge)

    if len(flat_variants) > 0:

        # Create stage and target tables
        variants_stage_table = meta.tables["variants_stage"]
        variants_target_table = meta.tables["variants"]

        # Structure merge
        variants_merge = MergeInto(
            target=variants_target_table,
            source=variants_stage_table,
            on=variants_target_table.c.historical_variant_id
            == variants_stage_table.c.historical_variant_id,
        )

        variants_cols = {
            "historical_variant_id": variants_stage_table.c.historical_variant_id,
            "shop_id": variants_stage_table.c.shop_id,
            "shop_name": variants_stage_table.c.shop_name,
            "variant_id": variants_stage_table.c.variant_id,
            "product_id": variants_stage_table.c.product_id,
            "title": variants_stage_table.c.title,
            "shop_currency": variants_stage_table.c.shop_currency,
            "price": variants_stage_table.c.price,
            "sku": variants_stage_table.c.sku,
            "position": variants_stage_table.c.position,
            "inventory_policy": variants_stage_table.c.inventory_policy,
            "compare_at_price": variants_stage_table.c.compare_at_price,
            "fulfillment_service_type": variants_stage_table.c.fulfillment_service_type,
            "inventory_management": variants_stage_table.c.inventory_management,
            "option_one": variants_stage_table.c.option_one,
            "option_two": variants_stage_table.c.option_two,
            "option_three": variants_stage_table.c.option_three,
            "created_at": variants_stage_table.c.created_at,
            "updated_at": variants_stage_table.c.updated_at,
            "is_taxable": variants_stage_table.c.is_taxable,
            "barcode": variants_stage_table.c.barcode,
            "weight_grams": variants_stage_table.c.weight_grams,
            "image_id": variants_stage_table.c.image_id,
            "weight": variants_stage_table.c.weight,
            "weight_unit": variants_stage_table.c.weight_unit,
            "inventory_item_id": variants_stage_table.c.inventory_item_id,
            "inventory_quantity": variants_stage_table.c.inventory_quantity,
            "old_inventory_quantity": variants_stage_table.c.old_inventory_quantity,
            "tax_code": variants_stage_table.c.tax_code,
            "is_requires_shipping": variants_stage_table.c.is_requires_shipping,
            "admin_graphql_api_id": variants_stage_table.c.admin_graphql_api_id,
        }

        variants_merge.when_not_matched_then_insert().values(**variants_cols)

        variants_merge.when_matched_then_update().values(**variants_cols)

        # Execute merge
        alchemy_connection.execute(variants_merge)

    if len(flat_product_options) > 0:

        # Create stage and target tables
        product_options_stage_table = meta.tables["product_options_stage"]
        product_options_target_table = meta.tables["product_options"]

        # Structure merge
        product_options_merge = MergeInto(
            target=product_options_target_table,
            source=product_options_stage_table,
            on=product_options_target_table.c.historical_product_option_id
            == product_options_stage_table.c.historical_product_option_id,
        )

        product_options_cols = {
            "historical_product_option_id": product_options_stage_table.c.historical_product_option_id,
            "historical_product_id": product_options_stage_table.c.historical_product_id,
            "shop_id": product_options_stage_table.c.shop_id,
            "shop_name": product_options_stage_table.c.shop_name,
            "product_id": product_options_stage_table.c.product_id,
            "option_id": product_options_stage_table.c.option_id,
            "name": product_options_stage_table.c.name,
            "position": product_options_stage_table.c.position,
            "option_values": product_options_stage_table.c.option_values,
            "product_created_at": product_options_stage_table.c.product_created_at,
            "product_updated_at": product_options_stage_table.c.product_updated_at,
        }

        product_options_merge.when_not_matched_then_insert().values(
            **product_options_cols
        )

        product_options_merge.when_matched_then_update().values(**product_options_cols)

        # Execute merge
        alchemy_connection.execute(product_options_merge)

    if len(flat_product_images) > 0:

        # Create stage and target tables
        product_images_stage_table = meta.tables["product_images_stage"]
        product_images_target_table = meta.tables["product_images"]

        # Structure merge
        product_images_merge = MergeInto(
            target=product_images_target_table,
            source=product_images_stage_table,
            on=product_images_target_table.c.historical_product_image_id
            == product_images_stage_table.c.historical_product_image_id,
        )

        product_images_cols = {
            "shop_id": product_images_stage_table.c.shop_id,
            "shop_name": product_images_stage_table.c.shop_name,
            "product_id": product_images_stage_table.c.product_id,
            "image_id": product_images_stage_table.c.image_id,
            "position": product_images_stage_table.c.position,
            "created_at": product_images_stage_table.c.created_at,
            "updated_at": product_images_stage_table.c.updated_at,
            "alt_text": product_images_stage_table.c.alt_text,
            "width": product_images_stage_table.c.width,
            "height": product_images_stage_table.c.height,
            "image_url": product_images_stage_table.c.image_url,
            "variant_ids": product_images_stage_table.c.variant_ids,
            "admin_graphql_api_id": product_images_stage_table.c.admin_graphql_api_id,
        }

        product_images_merge.when_not_matched_then_insert().values(
            **product_images_cols
        )

        product_images_merge.when_matched_then_update().values(**product_images_cols)

        # Execute merge
        alchemy_connection.execute(product_images_merge)

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

    is_included_step = "products" in steps_to_run

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
            table="PRODUCTS",
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
    flat_products = []
    flat_variants = []
    flat_product_options = []
    flat_product_images = []

    # Execute async API calls
    asyncio.run(
        fetch_all_data(
            api_endpoint="products",
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
    upsert_products_data()

    logger.info("Shopify products sync complete")
