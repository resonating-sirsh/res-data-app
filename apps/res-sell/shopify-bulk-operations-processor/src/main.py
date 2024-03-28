import json
from typing import Any, Dict, Optional, List

import requests

from datetime import datetime
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.mongo.MongoConnector import MongoConnector
from pymongo.database import Database
from res.connectors.shopify.utils.ShopifyProduct import ShopifyProduct
from res.utils import logger


class BulkOperationMsg:
    id: str
    status: Optional[str]
    raw_data: Dict[str, Any]

    def __init__(self, msg):
        self.id = msg["id"]
        self.status = msg["status"]
        self.raw_data = json.loads(msg["raw_data"])


# This long running process shows how to consume + produce with Kafka

KAFKA_TOPIC = "res_sell.shopify_webhooks.bulk_operations"
BULK_OPERATION = """
query readBulkOperation($id: ID!) {
    node(id: $id) {
        ... on BulkOperation {
            id
            status
            url
        }
    }
}
"""
SYNC_PHOTOS_WITH_ECOMMERCE = """
mutation syncPhotos($id: ID!){
  syncProductPhotosWithEcommerce(id: $id) {
    product{
      id
    }
  }
}
"""

SET_DEFAULT_INVENTORY = """
mutation setInventoryLevel($id: ID!){
  setProductDefaultInventoryLevel(id: $id) {
    product {
      id
    }
  }
}
"""

SHOPIFY_GET_STORE_PUBLICATION = """
query publicationFromProducts($productId: ID!) {
    product(id: $productId){
        id
        resourcePublications(first: 25, onlyPublished: true) {
            edges {
                node{
                    publication{
                        id
                        name
                    }
                    publishDate
                }
            }
        }
    }
}
"""


def get_gid(id):
    return f"gid://shopify/Product/{id}"


def get_store_publication(client: ShopifyProduct, ecommerce_id: str):
    response = client.shopify_graphql.execute(
        SHOPIFY_GET_STORE_PUBLICATION, {"productId": get_gid(ecommerce_id)}
    )
    response = json.loads(response)
    response = response["data"]["product"]["resourcePublications"]["edges"]
    return [
        {
            "publicationId": x["node"]["publication"]["id"],
            "publishDate": x["node"]["publishDate"],
        }
        for x in response
    ]


def handle(msg: BulkOperationMsg):
    try:
        db = MongoConnector().get_client().get_database("resmagic")
        products = list(
            db.products.find(
                {
                    "bulkOperationsStatus": "RUNNING",
                    "bulkOperations.id": msg.id,
                }
            )
        )

        if not products:
            logger.info(f"No products found for {msg.id}")
            return

        logger.debug(
            json.dumps([str(product["_id"]) for product in products], indent=2)
        )

        store_code = products[0]["storeCode"]
        client = ShopifyProduct(brand_code=store_code)
        data_json = client.shopify_graphql.execute(BULK_OPERATION, {"id": msg.id})
        data = json.loads(data_json)
        bulk_operation = data["data"]["node"]
        if bulk_operation["status"] == "FAILED":
            logger.error(f"Something failed on {bulk_operation}")
            return
        bulk_operation_url = bulk_operation["url"]
        bulk_operation_response = requests.get(bulk_operation_url)
        if bulk_operation_response.status_code != 200:
            logger.error(
                f"Something failed on reading response from {msg.id}: {bulk_operation_response.content}"
            )
            return
        operation: Optional[str] = None
        for bulk_operation in products[0].get("bulkOperations", []):
            if bulk_operation["id"] == msg.id:
                operation = bulk_operation["operation"]
                break

        records = []
        for line in bulk_operation_response.content.splitlines():
            records.append(json.loads(line))
        logger.debug(f"Records: {json.dumps(records, indent=2)}")
        handle_operation(
            operation=operation,
            records=records,
            db=db,
            products=products,
            sp_client=client,
            msg=msg,
        )
    except Exception as e:
        logger.error(str(e))
        raise e


def handle_operation(operation, records, db, products, sp_client, msg):
    if operation == "PRODUCT_PUBLISH":
        handle_product_publish(
            records=records, db=db, products=products, client=sp_client
        )
    elif operation == "PRODUCT_UNPUBLISH":
        handle_product_unpublish(
            records=records, db=db, products=products, client=sp_client
        )
    elif operation == "PRODUCT_CREATE":
        handle_product_create(
            records=records, db=db, products=products, client=sp_client, msg=msg
        )
    else:
        logger.error(f"Operation {operation} not supported")


def handle_product_publish(
    records: Dict[str, Any],
    db: Database,
    products: List[Dict[str, Any]],
    client: ShopifyProduct,
):
    for record in records:
        product = record["data"]["publishablePublish"]["publishable"]
        ecommerce_id = product["id"].split("/")[-1]
        logger.info(f"Processing product with ecommerce_id: {ecommerce_id}")
        # find the product in already looked up products
        find_product: Optional[Dict[str, Any]] = None
        for product in products:
            if product["ecommerceId"] == ecommerce_id:
                find_product = product
                break
        if find_product is None:
            logger.error(f"Could not find product with ecommerce_id: {ecommerce_id}")
            continue
        publish_on = get_store_publication(client, ecommerce_id)

        db.products.update_one(
            {
                "_id": find_product["_id"],
            },
            {
                "$set": {
                    "bulkOperationsStatus": "COMPLETED",
                    "publishedOn": publish_on,
                }
            },
        )


def handle_product_create(
    records: Dict[str, Any],
    db: Database,
    products: List[Dict[str, Any]],
    client: ShopifyProduct,
    msg: BulkOperationMsg,
):
    logger.debug(f"Records: {json.dumps(records, indent=1)}")
    for record in records:
        if record["data"]["productCreate"]["userErrors"]:
            logger.error(
                f"Error on product create: {record} in bulk operation {msg.id}"
            )
            continue
        product = record["data"]["productCreate"]["product"]
        ecommerce_id = product["id"].split("/")[-1]
        logger.info(f"Processing product with ecommerce_id: {ecommerce_id}")
        sp_product = client.find_by_id(ecommerce_id)
        sku = sp_product["variants"][0].get("sku")
        # find the product in already looked up products based on the sku in the all sku array
        find_product: Optional[Any] = None
        for product in products:
            if sku in product.get("allSku", []):
                find_product = product
                break
        if not find_product:
            logger.error(f"Could not find product with sku: {sku}")
            continue
        product_record = db.products.update_one(
            {"_id": find_product["_id"]},
            {
                "$set": {
                    "ecommerceId": str(ecommerce_id),
                    # "bulkOperationsStatus": "COMPLETED",
                    "isImported": True,
                    "importedAt": datetime.utcnow(),
                }
            },
        )
        client.graphQlClient.query(
            query=SYNC_PHOTOS_WITH_ECOMMERCE,
            variables={
                "id": str(find_product["_id"]),
            },
        )
        client.graphQlClient.query(
            query=SET_DEFAULT_INVENTORY,
            variables={
                "id": str(find_product["_id"]),
            },
        )


def handle_product_unpublish(
    records: Dict[str, Any],
    db: Database,
    products: List[Dict[str, Any]],
    client: ShopifyProduct,
):
    for record in records:
        product = record["data"]["publishableUnpublish"]["publishable"]
        ecommerce_id = product["id"].split("/")[-1]
        logger.info(f"Processing product with ecommerce_id: {ecommerce_id}")
        # find the product in already looked up products
        find_product: Optional[Dict[str, Any]] = None
        for product in products:
            if product["ecommerceId"] == ecommerce_id:
                find_product = product
                break
        if find_product is None:
            logger.error(f"Could not find product with ecommerce_id: {ecommerce_id}")
            continue

        publish_on = get_store_publication(client, ecommerce_id)
        db.products.update_one(
            {
                "_id": find_product["_id"],
            },
            {
                "$set": {
                    "bulkOperationsStatus": "COMPLETED",
                    "publishedOn": publish_on,
                }
            },
        )


if __name__ == "__main__":
    # Initialize client and consumer

    from res.connectors import load

    load("mongo")

    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_TOPIC)
    while True:
        logger.debug("Retrieving message from Kafka...")
        msg: Optional[Dict[str, Any]] = kafka_consumer.poll_avro()
        if msg is None:
            # Sometimes the message is None the first try, will show up eventually
            logger.debug("...")
            continue
        logger.debug(f"Message retrieved: {msg}")
        msg = BulkOperationMsg(msg)
        handle(msg)

    # msg_test = BulkOperationMsg(
    #     {
    #         "id": "gid://shopify/BulkOperation/2820279173235",
    #         "status": "COMPLETED",
    #         "raw_data": json.dumps(
    #             {
    #                 "id": "gid://shopify/BulkOperation/2820279173235",
    #                 "status": "RUNNING",
    #                 "url": None,
    #                 "operation": "PRODUCT_CREATE",
    #                 "createdBy": "system",
    #             }
    #         ),
    #     }
    # )
    # handle(msg_test)
