import shopify
import time
from datetime import datetime
from datetime import timedelta
from res.utils import logger
from resonance_helpers import is_resonance_item
from pyactiveresource.connection import ClientError
from shopify_product_status import get_online_store_status

MAX_SHOPIFY_ATTEMPTS = 3
MIN_DATE = "2018-01-01"
DAYS_BACK = 1


class ShopifyHelper:
    def __init__(self, brand):
        self.brand = brand
        self.session = self.get_session(brand)

    def get_session(self, brand):
        """Gets a shopify session for a given brand and activates it"""
        session = shopify.Session(
            "https://" + brand["shopifyStoreName"] + ".myshopify.com/",
            "2022-10",
            brand["shopifyApiPassword"],
        )
        shopify.ShopifyResource.activate_session(session)
        return session

    def get_products(self, attempt=1):
        """Pulls down all products for a given Shopify store"""
        try:
            product_array = []
            products = shopify.Product.find()
            has_more = True
            page = 0
            while has_more:
                for every in products:
                    msg = every.to_dict()
                    # Check variant skus to make sure this is a resonance item
                    for variant in msg["variants"]:
                        if is_resonance_item(variant["sku"]):
                            msg["brand_code"] = self.brand["code"]
                            # Pick the res_code of the first variant
                            msg["res_code"] = " ".join(variant["sku"].split(" ")[:3])
                            product_array.append(msg)
                            break
                page = page + 1
                logger.info("Page: " + str(page))
                if products.has_next_page():
                    products = products.next_page()
                else:
                    has_more = False
            # Check online store status for all products
            logger.info(
                f"Looking up online store status for all products...{len(product_array)} in total"
            )
            product_statuses = get_online_store_status(
                self.brand["shopifyApiPassword"],
                self.brand["shopifyStoreName"],
            )
            for product in product_array:
                product["published_to_online_store"] = product_statuses.get(
                    str(product["id"]), None
                )
            return product_array
        except ClientError as e:
            if e.response.code == 429 and attempt <= MAX_SHOPIFY_ATTEMPTS:
                logger.warn(
                    "Too many requests to Shopify -- pausing for 60 seconds and retrying..."
                )
                time.sleep(60)
                return self.get_orders(attempt=attempt + 1)
            raise

    def get_orders(self, backfill=False, days_back=DAYS_BACK, attempt=1):
        f"""Pulls down shopify orders. If backfill is set to true, goes back to {MIN_DATE}"""
        try:
            order_array = []
            page = 0

            if backfill:
                orders = shopify.Order.find(status="any", created_at_min="2018-01-01")
            else:
                orders = shopify.Order.find(
                    status="any",
                    updated_at_min=datetime.strftime(
                        datetime.today() - timedelta(days=days_back),
                        "%Y-%m-%d",
                    ),
                )
            total_orders = 0
            has_more = True
            while has_more:
                for every in orders:
                    msg = every.to_dict()
                    msg["brand_code"] = self.brand["code"]
                    # Only include resonance items/orders
                    res_line_items = []
                    for item in msg["line_items"]:
                        if is_resonance_item(item["sku"]):
                            res_line_items.append(item)
                    # If 1 or more resonance item, send to Snowflake
                    if len(res_line_items) > 0:
                        msg["line_items"] = res_line_items
                        order_array.append(msg)
                    total_orders += 1
                page = page + 1
                logger.info("Page: " + str(page))
                if orders.has_next_page():
                    orders = orders.next_page()
                else:
                    has_more = False
            logger.info("Total orders, including non-resonance: " + str(total_orders))
            return order_array
        except ClientError as e:
            if e.response.code == 429 and attempt <= MAX_SHOPIFY_ATTEMPTS:
                logger.warn(
                    "Too many requests to Shopify -- pausing for 60 seconds and retrying..."
                )
                time.sleep(60)
                return self.get_orders(backfill, attempt=attempt + 1)
            if e.response.code == 402:
                logger.warn("Brand's store is deactivated due to lack of payments!")
                return []
            raise
