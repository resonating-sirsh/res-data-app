import os, json, logging, random, time, datetime, asyncio
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import logger
from shopify_processor import get_shopify_items_objects, get_shopify_order_object
from create_one_processor import (
    get_create_one_items_objects,
    get_create_one_order_object,
)

OUTPUT_ORDERS_KAFKA_TOPIC = "res_sell.one_orders.one_orders"
OUTPUT_ITEMS_KAFKA_TOPIC = "res_sell.one_orders.one_order_items"
SHOPIFY_ORDERS_KAFKA_TOPIC = "res_sell.shopify_webhooks.orders_raw"
CREATE_ONE_ORDERS_KAFKA_TOPIC = "res_sell.create_one_orders.orders"


async def process_orders(kafka_client, source_type):
    logger.info(f"Starting orders processor for {source_type}")
    # Initialize consumer and producer
    shopify_consumer = ResKafkaConsumer(kafka_client, SHOPIFY_ORDERS_KAFKA_TOPIC)
    create_one_consumer = ResKafkaConsumer(kafka_client, CREATE_ONE_ORDERS_KAFKA_TOPIC)
    with ResKafkaProducer(kafka_client, OUTPUT_ORDERS_KAFKA_TOPIC) as orders_producer:
        with ResKafkaProducer(kafka_client, OUTPUT_ITEMS_KAFKA_TOPIC) as items_producer:
            while True:
                try:
                    logger.info("processing message...")
                    if source_type == "shopify":
                        msg = shopify_consumer.poll_json()
                        if msg:
                            order = get_shopify_order_object(msg)
                            items = get_shopify_items_objects(msg)
                    elif source_type == "create_one":
                        msg = create_one_consumer.poll_avro()
                        if msg:
                            order = get_create_one_order_object(msg)
                            items = get_create_one_items_objects(msg)
                    else:
                        logger.error(f"Unknown source type: {source_type}")
                        raise Exception()
                    logger.debug(msg)
                    orders_producer.produce(order)
                    for item in items:
                        items_producer.produce(item)
                except Exception as e:
                    logger.error(e)
                    continue


async def main_loop():
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    logger.info(f"started at {time.strftime('%X')}")
    await process_orders(kafka_client, "shopify")
    await process_orders(kafka_client, "create_one")


if __name__ == "__main__":
    asyncio.run(main_loop())
