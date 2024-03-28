import os, traceback
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger
from resonance_helpers import get_brands

from snowflake_helpers import ShopifySnowflakeHelper
from shopify_helpers import ShopifyHelper


if __name__ == "__main__":
    """Pulls down orders and products from Shopify, and upserts into Snowflake"""
    snowflake_helper = ShopifySnowflakeHelper()

    # Get list of brands and their Shopify credentials from the Graph API
    if os.getenv("brand_code", "KT").lower() == "false":
        creds_array = get_brands()
    else:
        creds_array = get_brands(os.getenv("brand_code", "KT"))

    # Determine if this is a backfill and get days back
    backfill = False if os.getenv("backfill", "false") == "false" else True
    days_back = int(os.getenv("days", 1))

    # Loop through brands and get shopify data, upsert to Snowflake
    for brand in creds_array:
        try:
            if "code" not in brand or brand["code"] is None:
                logger.warn(
                    "Brand does not have a code: "
                    + brand.get("name", "Unknown brand name")
                )
                continue
            logger.info("Processing orders for brand " + brand["code"])
            shopify_helper = ShopifyHelper(brand)
            orders = shopify_helper.get_orders(backfill, days_back)
            if len(orders) > 0:
                snowflake_helper.upsert_orders(orders)

            logger.info("Processing products for brand...")
            products = shopify_helper.get_products()
            if len(products) > 0:
                snowflake_helper.upsert_products(products)
            logger.info("Finished Brand!")

        except Exception as e:
            ERR = f"Error processing: {traceback.format_exc()}"
            logger.critical(
                ERR,
                exception=e,
            )
            continue
