import os, json
from datetime import datetime
import boto3
from res.utils import logger
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from queries import (
    orders_upsert,
    orders_table_create,
    line_items_table_create,
    line_items_upsert,
)


class ShopifySnowflakeHelper:
    """Functions for upserting shopify data into snowflake"""

    def __init__(self):
        self.env = os.getenv("RES_ENV", "development")
        self.staging_client = ResSnowflakeClient(
            schema=f"IAMCURIOUS_{self.env}_STAGING"
        )
        self.snowflake_client = ResSnowflakeClient()
        self.boto_client = boto3.client("s3")
        self.bucket = f"res-data-{self.env}"
        self.schema = f"IAMCURIOUS_{self.env}"

    def upsert_products(self, products):
        self.snowflake_client.load_json_data(products, "shopify_products", "id")

    def upsert_orders(self, orders):
        """Upserts orders into Snowflake"""
        # Write to S3
        dt = datetime.strftime(datetime.now(), "%Y_%m_%d_%s")
        self.boto_client.put_object(
            Body=json.dumps(orders),
            Bucket=self.bucket,
            Key=f"shopify/orders/orders_{dt}.json",
        )

        # Create temp table
        logger.info("\tBuilding temp table...")
        self.snowflake_client.execute(
            f"CREATE OR REPLACE TABLE {self.schema}_STAGING.shopify_orders_temp (data VARIANT);"
        )
        # Create permanent table
        logger.info("\tBuilding permanent table...")
        self.snowflake_client.execute(orders_table_create.format(env=self.env))
        # Insert variant data
        logger.info(f"\tLoading orders. Total count: {str(len(orders))}")

        self.staging_client.execute(
            f"""COPY INTO {self.schema}_STAGING.shopify_orders_temp
                    FROM @res_data_{self.env}/shopify/orders/orders_{dt}.json
                    FILE_FORMAT = (type = 'JSON' strip_outer_array = true);"""
        )
        # Upsert into permanent
        logger.info("\tUpserting orders...")
        self.snowflake_client.execute(orders_upsert.format(env=self.env))
        # Build permanent items table
        logger.info("\tBuilding items table...")
        self.snowflake_client.execute(line_items_table_create.format(env=self.env))
        # Upsert line items
        logger.info("\tUpserting items...")
        self.snowflake_client.execute(line_items_upsert.format(env=self.env))
        logger.info("Finished orders!")
