import os

import arrow
from src.graphql_queries import GET_PRODUCT_QUERY, GET_STYLE, UPDATE_STYLE
from src.handlers import (
    get_store_publication,
    handle_check_live_status_on_product_webhook,
)
from src.utils_graph import graphql_client, update_product

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger

ENV = os.getenv("RES_ENV")
TOPIC = "res_sell.shopify_webhooks.products"


def is_resonance_item(sku):
    # Resonance skus need to have 4 parts, the first part being 6 characters
    # e.g. TK6074 SLCTN FLOWJY 1ZZXS
    parts = sku.split(" ")
    return False if len(parts) != 4 else len(parts[0]) == 6


# Main kafka consumer/ graphql query loop
def run_consumer(kafka_consumer):
    while True:
        try:
            raw_product = kafka_consumer.poll_avro()

            if raw_product is None:
                logger.info(f"kafka poll timed out, continuing in topic: {TOPIC}")
                continue
            logger.info(f'Product ecommerceId: {raw_product["id"]}')
            logger.info(f"Found product event: {raw_product}")

            products = graphql_client.query(
                GET_PRODUCT_QUERY,
                {
                    "where": {
                        "ecommerceId": {"is": str(raw_product["id"])},
                        "brandCode": {"is": raw_product["brand"]},
                    }
                },
            )

            if raw_product["type"] == "delete":
                logger.info("This product has been deleted from Shopify")
                # Some brands have 1 ecommerce product and multiple products in
                # the platform, i.e JCRT shirts with different fit
                for platform_product in products["data"]["products"]["products"]:
                    payload = {
                        "isImported": False,
                        "ecommerceId": None,
                        "wasDeletedInEcommerce": True,
                        "deletedInEcommerceAt": arrow.utcnow().isoformat(),
                    }
                    update_product(platform_product["id"], payload)
                    style = graphql_client.query(
                        GET_STYLE, {"id": platform_product.get("style").get("id")}
                    )
                    graphql_client.query(
                        UPDATE_STYLE,
                        {
                            "id": style.get("id"),
                            "input": {"isLocked": style.get("isLocked")},
                        },
                    )
                continue

            if len(products["data"]["products"]["products"]) > 0:
                logger.info("Product exists in create.one")

                for product in products["data"]["products"]["products"]:
                    # check the live status
                    logger.debug(f"checking live status on product{raw_product['id']}")
                    handle_check_live_status_on_product_webhook(
                        brand_code=raw_product["brand"],
                        product=product,
                        ecommerce_id=raw_product["id"],
                    )

                    system_date = arrow.get()
                    product_date = arrow.get(product["updatedAt"])

                    update_minutes_difference = system_date - product_date

                    minutes_between_dates = (
                        update_minutes_difference.total_seconds() / 60
                    )

                    # Ensuring the update was not trigger in create.one
                    if minutes_between_dates >= 2:
                        logger.info("Updating product in the platform.")
                        payload = {
                            "title": raw_product["title"],
                            "tags": raw_product["tags"],
                            "description": raw_product["description"],
                            "price": float(raw_product["price"]),
                            "publishedOn": get_store_publication(
                                raw_product["brand"], raw_product["id"]
                            ),
                        }
                        update_product(product["id"], payload)
            elif "sku" in raw_product:
                logger.info("Product not found in create.one")
                if is_resonance_item(raw_product["sku"]):
                    logger.info(
                        f"Shopify Product id {raw_product['id']} from brand {raw_product['brand']} with Resonance SKU was not found in the products table."
                    )
        except Exception as e:
            logger.info(f"An error has ocurred: {str(e)}")


if __name__ == "__main__":
    # Initialize clients
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, TOPIC)

    run_consumer(kafka_consumer)
