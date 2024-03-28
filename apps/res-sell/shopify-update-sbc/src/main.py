import typing

from bson import ObjectId
from res.connectors import load
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.mongo.MongoConnector import MongoConnector
from res.connectors.shopify.ShopifyConnector import Shopify
from res.utils.logging import logger
import shopify
from src.query_definition import (
    GET_PRODUCT,
    GET_SBC_BASE_ON_BODY_MATERIAL_AND_STORE,
    MAKE_FILE_PUBLIC,
    UPDATE_SBC_BY_ID,
)

# This long running process shows how to consume + produce with Kafka
KAFKA_TOPIC = "res_sell.shopify_product_live.live_products"
PRODUCE_TOPIC = "res_sell.shop_by_color.release_sbc"


def run_consumer(
    kafka_consumer: ResKafkaConsumer,
    graphql: ResGraphQLClient,
    kafka_client: ResKafkaClient,
):
    msg = get_message_from_consumer(kafka_consumer)
    if msg is None:
        logger.info("...")
        return
    product_id, live_status = (msg.get("id"), msg.get("live"))

    db = get_mongo_database()
    product = get_product_by_id(product_id)
    if not product:
        logger.info(f"product '{product_id}' not found")
        return

    styleId = product["style"]["id"]

    ban_style = db.banSbcStyles.find_one(
        {
            "styleId": styleId,
            "active": True,
        }
    )

    if ban_style:
        logger.info(f"product '{product_id}' is banned not added to SBC")
        return

    store_code = product.get("storeCode")
    body_code, material_code, store_code = get_body_material_store_code(product)
    shop_by_color = get_sbc_by_body_material_and_store_code(
        body_code,
        material_code,
        store_code,
        graphql,
    )

    if not shop_by_color:
        logger.info(
            f"product '{product['id']}' is not going to be add in a SBC instance"
        )
        return

    with Shopify(brand_id=store_code):
        shopify_product = shopify.Product().find(id_=int(product["ecommerceId"]))
        if live_status:
            add_color_option_if_possible(
                product, shop_by_color, shopify_product, graphql
            )
        else:
            remove_color_option_if_possible(product, shop_by_color, graphql)

    with ResKafkaProducer(
        kafka_client,
        topic=PRODUCE_TOPIC,
    ) as producer:
        producer.produce({"shop_by_color_id": shop_by_color["id"]})

    db.client.close()


def get_body_material_store_code(product):
    body_code = product["style"]["body"]["code"]
    material_code = product["style"]["material"]["code"]
    store_code = product["storeCode"]
    return body_code, material_code, store_code


def add_color_option_if_possible(product, shop_by_color, shopify_product, graphql):
    style_id = product["style"]["id"]
    color_name = product["style"]["color"]["name"]
    color_image_url = verify_image_public(
        url=product["style"]["color"]["images"][0]["smallThumbnail"],
        file_collection=MongoConnector().get_client().get_database("resmagic").files,
        graphql=graphql,
    )
    images = []
    if shopify_product.images:
        images.append(shopify_product.images[0].src)
        if len(shopify_product.images) > 1:
            images.append(shopify_product.images[1].src)
    option = {
        "id": str(ObjectId()),
        "handle": shopify_product.handle,
        "images": images,
        "colorName": color_name,
        "styleId": style_id,
        "price": shopify_product.price_range(),
        "imageUrl": color_image_url,
        "referenceDate": shopify_product.published_at,
    }
    options = list(shop_by_color["options"])
    options.append(option)
    options_dict = {option["styleId"]: option for option in options}
    options = list(options_dict.values())
    logger.info(f"sbc update {shop_by_color['id']}")
    logger.info(f"{options}")
    graphql.query(
        query=UPDATE_SBC_BY_ID,
        variables={
            "id": shop_by_color["id"],
            "input": {
                "status": "need-sync",
                "options": options,
            },
        },
    )


def remove_color_option_if_possible(product, shop_by_color, graphql):
    shop_by_color_id = shop_by_color.get("id")

    filtered_options = [
        option
        for option in shop_by_color["options"]
        if option["styleId"] != product["style"]["id"]
    ]
    logger.info(f"sbc update {shop_by_color_id}, removing color option{product}")
    logger.debug(filtered_options)
    graphql.query(
        query=UPDATE_SBC_BY_ID,
        variables={
            "id": shop_by_color_id,
            "input": {
                "status": "need-sync",
                "options": filtered_options,
            },
        },
    )


def get_message_from_consumer(kafka_consumer: ResKafkaConsumer):
    logger.info("Retrieving message from Kafka...")
    msg = kafka_consumer.poll_avro()
    logger.debug("New record from kafka: {}".format(msg))
    return msg


def get_product_by_id(id) -> typing.Dict:
    graphql: ResGraphQLClient = load("graphql")
    return graphql.query(query=GET_PRODUCT, variables={"id": id})["data"]["product"]


def get_sbc_by_body_material_and_store_code(
    body_code, material_code, store_code, graphql
):
    logger.info(
        f"getting shop by color instance based on body code '{body_code}', material code '{material_code}' and store code '{store_code}'"
    )
    data = graphql.query(
        query=GET_SBC_BASE_ON_BODY_MATERIAL_AND_STORE,
        variables={
            "bodyCode": body_code,
            "materialCode": material_code,
            "storeCode": store_code,
        },
    )["data"]["shopByColorInstances"]["shopByColorInstances"]
    response = data[0] if data else None
    logger.debug(data)
    return response


def verify_image_public(url, file_collection, graphql) -> str:
    url_slug = url.split("/")[-1]
    url_slug_splitted = str(url_slug).split("?")
    file_name = url_slug_splitted[0]
    logger.info(f"verifing if file '{file_name}' is public")
    # file = graphql.query(GET_FILE_PRODUCT_BY_NAME, {"name": file_name})
    file_s3_id = file_name.split(".")[0]
    file = file_collection.find_one({"s3.id": file_s3_id})
    if not file.get("isPublic"):
        logger.info(f"file '{file_name}' is not public")
        # make file public
        data = graphql.query(MAKE_FILE_PUBLIC, {"id": str(file["_id"])})["data"][
            "makeFilePublic"
        ]
        url = data["file"]["thumbnail"]["url"]
        return url
    logger.info(f"file '{file_name}' is public")
    return url.split("?")[0]


def get_mongo_database():
    mongo_connector: MongoConnector = load("mongo")
    return mongo_connector.get_client().get_database("resmagic")


if __name__ == "__main__":
    from res.connectors import load

    load("mongo")
    # creating color option
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_TOPIC)
    graphql = ResGraphQLClient()
    while True:
        try:
            run_consumer(kafka_consumer, graphql, kafka_client)
        except Exception as e:
            logger.error(str(e), exception=e)
