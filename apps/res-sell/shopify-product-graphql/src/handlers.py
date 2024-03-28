import json
from typing import Any, Dict

from src.graphql_queries import SHOPIFY_GET_STORE_PUBLICATION
from src.utils_graph import update_product

from res.connectors.shopify.utils.ShopifyProduct import ShopifyProduct
from res.utils import logger


def handle_check_live_status_on_product_webhook(
    brand_code: str, product: Dict[str, Any], ecommerce_id: str
):
    """Handle change on the live status from the webhook of products
    We are using as ref the ShopifyDocs "published_at" as our reference and
    also the check_live_status.
    [url]
    https://shopify.dev/api/admin-rest/2022-04/resources/product#resource-object
    """
    instance = ShopifyProduct(brand_code=brand_code)
    sp_product = instance.find_by_id(ecommerce_id)
    if not sp_product:
        raise Exception("Product not found in ecommerce")

    is_live = instance.check_product_is_live(id=product["id"])
    logger.info(f"product {product['id']} live: {is_live}")
    if product["isLive"] != is_live:
        update_product(
            product["id"],
            {
                "isLive": is_live,
            },
        )
    return


def get_store_publication(brand_code: str, ecommerce_id: str):
    instance = ShopifyProduct(brand_code=brand_code)
    response = instance.shopify_graphql.execute(
        SHOPIFY_GET_STORE_PUBLICATION, {"productId": get_gid(ecommerce_id)}
    )
    response = json.loads(response)
    try:
        response = response["data"]["product"]["resourcePublications"]["edges"]
        return [
            {
                "publicationId": x["node"]["publication"]["id"],
                "publishDate": x["node"]["publishDate"],
            }
            for x in response
        ]
    except KeyError:
        return []


# Function that return a shopify gid passing just an id
def get_gid(id):
    return f"gid://shopify/Product/{id}"
