import json

from res.utils import logger
from res.connectors.shopify.ShopifyConnector import Shopify


def is_product_public(ecommerce_id, shopify_client: Shopify):
    try:
        logger.debug("Checking if product {} is public".format(ecommerce_id))
        response = shopify_client.get_product(ecommerce_id)
        if response:
            if response["status"] == "ACTIVE" or response["status"] == "active":
                return response
            else:
                return False
        else:
            return False
    except:
        logger.warn(f"is_product_public fail")
        return False
