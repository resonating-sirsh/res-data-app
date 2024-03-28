"""
This is kafka app that create all the sbc update due to swap material.

The idea is when a swap_material start this process would create multiple 
sbc-swap-requests persisted on redis.
"""


import json
import redis
import os
from res.connectors.graphql.ResGraphQLClient import GraphQLException, ResGraphQLClient
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.flows.sell.product.group.src.graphql import get_shop_by_color_record_by_id
from res.utils.logging import logger
from res.utils import group_by
from src.query_definition import GET_SWAP_MATERIAL, GET_STYLES, GET_LIST_SBC

KAFKA_TOPIC = "res_sell.swap_material_sell.swap_material_sell"

NO_GROUP = "no_sbc"


def simple_error_handle(function_to_decorate):
    def wrapper_function(*args, **kwargs):
        try:
            return function_to_decorate(*args, **kwargs)
        except Exception as e:
            logger.error(e)
            raise e

    return wrapper_function


@simple_error_handle
def run_consumer(
    kafka_consumer: ResKafkaConsumer,
    graphql: ResGraphQLClient,
    redis_client: redis.Redis,
):
    msg = get_message_from_kafka(kafka_consumer)
    if not msg:
        return
    swap_material_request_id = msg.get("id")
    swap_material = get_swap_material_requests(swap_material_request_id, graphql)
    from_material = swap_material["fromMaterial"]
    to_material = swap_material["toMaterial"]
    styles = get_styles(json.loads(swap_material["stylesIds"]), graphql)
    grouped_styles = group_styles_by_body_material(styles)
    logger.debug(grouped_styles.keys())
    from_sbcs = {
        sbc["id"]: sbc
        for key, list_style in grouped_styles.items()
        for sbc in get_list_shop_by_colors(
            material_codes=[from_material],
            body_codes=[key.split("%")[0]],
            store_code=list_style[0]["brand"]["code"],
            graphql=graphql,
        )
    }
    logger.debug(f"len from_sbc {len(list(from_sbcs.values()))}")
    payload_to_redis = {}

    for sbc_id, sbc in from_sbcs.items():
        logger.info(f"saved sbc{sbc_id}")
        logger.debug(sbc)
        style_ids = {
            option["styleId"]
            for option in sbc["options"]
            if option["styleId"] in swap_material["stylesIds"]
        }
        to_sbcs = get_list_shop_by_colors(
            material_codes=[to_material],
            body_codes=[bodie["code"] for bodie in sbc["bodies"]],
            store_code=sbc["storeBrand"]["code"],
            graphql=graphql,
        )
        payload_to_redis[sbc_id] = {
            "from": from_material,
            "to": to_material,
            "from_sbc": sbc_id,
            "style_ids": list(style_ids),
            "swapped_styles": [],
            "to_sbcs": [sbc_i["id"] for sbc_i in to_sbcs],
        }

    if payload_to_redis:
        logger.debug("Saving {swap_material_request_id} sbc-swap-requests")
        logger.debug(payload_to_redis)
        redis_client.set(swap_material_request_id, json.dumps(payload_to_redis))


def group_styles_by_body_material(styles):
    logger.info(f"styles: {[style['id'] for style in styles]}")

    def get_key(style):
        return "{}%{}".format(style["body"]["code"], style["material"]["code"])

    return group_by(get_key, styles)


def get_list_shop_by_colors(body_codes, material_codes, store_code, graphql):
    variables = {
        "where": {
            "and": [
                {
                    "bodyCodes": {"isAnyOf": body_codes},
                },
                {
                    "materialCodes": {"isAnyOf": material_codes},
                },
                {
                    "storeCode": {"is": store_code},
                },
            ]
        }
    }
    logger.info(f"variables: {variables}")
    response = graphql.query(GET_LIST_SBC, variables)
    if response.get("errors"):
        logger.warn(response["errors"])
        msg = f"Error pulling sbc body_code: {body_codes} material_code: {material_codes} store_brand: {store_code}"
        raise GraphQLException(msg)

    sbc = response["data"]["shopByColorInstances"]["shopByColorInstances"]
    logger.debug(f"len_sbc: {len(sbc)}")
    return sbc


def get_message_from_kafka(kafka_consumer):
    logger.info("Retrieving message from Kafka...")
    msg = kafka_consumer.poll_avro()
    if msg is None:
        # Sometimes the message is None the first try, will show up eventually
        logger.info("...")
        return
    logger.debug(f"New record from kafka: {msg}")
    return msg


def get_styles(ids: list, graphql: ResGraphQLClient):
    variables = {"ids": ids}

    def get_data(response: dict):
        hasMore = response["data"]["styles"]["hasMore"]
        styles = response["data"]["styles"]["styles"]
        after = response["data"]["styles"]["cursor"]
        return hasMore, styles, after

    response = graphql.query(query=GET_STYLES, variables=variables)
    if response.get("errors"):
        raise GraphQLException(f"Error pulling styles on variables {variables}")

    hasMore, styles, after = get_data(response)

    while hasMore == True:
        variables["after"] = after
        response = graphql.query(query=GET_STYLES, variables=variables)
        if response.get("errors"):
            raise GraphQLException(f"Error pulling styles on variables {variables}")
        hasMore, data, after = get_data(response)
        styles.extend(data)
    return styles


def get_swap_material_requests(id: str, client):
    response = client.query(GET_SWAP_MATERIAL, {"id": id})
    if response.get("errors"):
        raise GraphQLException(f"Error pulling swapMaterialRequest {id}")

    swap_material = response["data"]["materialSwapRequest"]

    if not swap_material:
        raise GraphQLException(f"Swap Material Request do not exists{swap_material}")
    logger.info(swap_material)
    return swap_material


if __name__ == "__main__":
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_TOPIC)
    graphql = ResGraphQLClient()
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0
    )
    while True:
        try:
            run_consumer(kafka_consumer, graphql, redis_client)
        except:
            logger.warn(f"fail, restarting... ")
