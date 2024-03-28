"""
This is a kafa app which hear swapped style changes
- marks style as swapped
- Once all the style are swapped within a shop_by_color, update shop_by_color in shopify.
"""
import redis, os, json
import src.query_definition as query_definition
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.graphql.ResGraphQLClient import GraphQLException, ResGraphQLClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.flows.sell.product.group.src.graphql import (
    get_shop_by_color_record_by_id,
    update_shop_by_color_record,
)
from res.utils import logger

TOPIC = "res_sell.swap_material_sell.swap_material_style"


def run_consumer(kafka_consumer, graphql_client, redis_client):
    msg = get_message_from_kafka(kafka_consumer)
    if not msg:
        return
    swap_id = msg["swap_material_request"]
    style_id = msg["style_id"]
    sbc_swap_requests = read_from_redis(swap_id, redis_client)
    update, data = mark_style_as_swapped(style_id, sbc_swap_requests)
    if update:
        # update in c1
        sbc_swap_requests = update_sbc(data["sbc_ids"], data["data"], graphql_client)
        # only delete when all sbc have been update or change the sbc_swap_requests shape?
        logger.debug(sbc_swap_requests)
        if not sbc_swap_requests:
            delete_on_redis(swap_id, redis_client)
        else:
            write_on_redis(swap_id, sbc_swap_requests, redis_client)
        return
    # just mark the style as swapped.
    # update on redis
    write_on_redis(swap_id, data["data"], redis_client)


def mark_style_as_swapped(style_id, sbc_swap_requests: dict):
    logger.info(f"Marking swapped_style {style_id}")
    sbc_ids = []
    update = False
    for sbc_id, data in sbc_swap_requests.items():
        # how to get all the sbcs by the style_id from the sbc_swap_requests?
        list_styles_to_swap = data["swapped_styles"]
        list_styles_to_swap.append(style_id)

        data["style_ids"] = [id for id in data["style_ids"] if id != style_id]

        logger.debug(data["style_ids"])

        is_fully_swapped = len(data["style_ids"]) == 0
        logger.info(f"is_fully_swapped: {is_fully_swapped}")

        # post_condition or update_condition
        if is_fully_swapped:
            sbc_ids.append(sbc_id)
            update = True

        sbc_swap_requests[sbc_id] = data
    return update, {"sbc_ids": sbc_ids, "data": sbc_swap_requests}


# update sbc should occur on both places the originals sbcs and the target sbcs
def update_sbc(sbc_ids, sbc_swap_requests, graphql_client: ResGraphQLClient):
    for sbc_id in sbc_ids:
        sbc_swap_changes = sbc_swap_requests[sbc_id]
        to_sbc_ids = sbc_swap_changes["to_sbcs"]
        logger.debug(to_sbc_ids)
        remove_color_options(sbc_id, sbc_swap_changes, graphql_client)
        invoke_release_sbc(sbc_id, graphql_client)
        for id in to_sbc_ids:
            logger.debug(id)
            update_sbc_changes(id, sbc_swap_changes, graphql_client)
            invoke_release_sbc(id, graphql_client)
        del sbc_swap_requests[sbc_id]
    return sbc_swap_requests


def invoke_release_sbc(sbc_id, graphql_client):
    response = graphql_client.query(query_definition.RELEASE_SBC, {"id": sbc_id})
    if "errors" in response:
        logger.warn(response["erros"])
        raise GraphQLException(f"Error invoking release SBC {sbc_id}")
    return response["data"]["releaseShopByColorInstanceToEcommerce"][
        "shopByColorInstance"
    ]


def update_sbc_changes(id, sbc_swap_changes, graphql_client):
    sbc_instance = get_shop_by_color_record_by_id(id, graphql_client)
    options = sbc_instance["options"]
    swapped_styles = sbc_swap_changes["swapped_styles"]
    dict_options = {option["styleId"]: option for option in options}
    for style_id in swapped_styles:
        if style_id in dict_options:
            continue
        style = get_style(style_id, graphql_client)
        dict_options[style_id] = {
            "styleId": style_id,
            "colorName": style["color"]["name"],
            "imageUrl": style["color"]["images"][0]["fullThumbnail"],
        }
    new_options = list(dict_options.values())
    logger.debug(new_options)
    update_shop_by_color_record(id, {"options": new_options}, graphql_client)


def get_style(style_id, graphql_client: ResGraphQLClient):
    response = graphql_client.query(query_definition.GET_STYLE_ID, {"id": style_id})
    if "errors" in response:
        logger.warn(response["errors"])
        raise GraphQLException(f"Error getting style {style_id}")
    return response["data"]["style"]


def remove_color_options(from_sbc_id, sbc_swap_changes, graphql_client):
    sbc_instance = get_shop_by_color_record_by_id(from_sbc_id, graphql_client)
    from_options = sbc_instance["options"]
    swapped_styles = sbc_swap_changes["swapped_styles"]
    removed_options = [
        option_item
        for option_item in from_options
        if not option_item["styleId"] in swapped_styles
    ]
    input = {"options": removed_options}
    update_shop_by_color_record(from_sbc_id, input, graphql_client)


def read_from_redis(swap_id: str, redis_client: redis.Redis):
    data_json = redis_client.get(swap_id)
    logger.debug(f"readin on redis key {swap_id} : {data_json}")
    if not data_json:
        raise Exception(f"Pulling from redis {swap_id} = None")
    return json.loads(data_json)


def write_on_redis(swap_id: str, data: dict, redis_client: redis.Redis):
    return redis_client.set(swap_id, json.dumps(data))


def delete_on_redis(swap_id: str, redis_client: redis.Redis):
    redis_client.delete(swap_id)


def get_message_from_kafka(kafka_consumer):
    logger.info("Retrieving message from Kafka...")
    msg = kafka_consumer.poll_avro()
    if msg is None:
        # Sometimes the message is None the first try, will show up eventually
        logger.info("...")
        return
    logger.debug(f"New record from kafka: {msg}")
    return msg


if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client=kafka_client, topic=TOPIC)
    graphql = ResGraphQLClient()
    redis_client = redis.Redis(
        host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0
    )
    while True:
        try:
            run_consumer(kafka_consumer, graphql, redis_client)
        except Exception as error:
            logger.warn(error)
        finally:
            redis_client.close()
