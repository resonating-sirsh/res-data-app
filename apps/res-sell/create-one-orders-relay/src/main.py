"""
receive create one orders and relay them to the make order processor 
nudge...  ...  ...
"""

import traceback
import asyncio
import time
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.flows.sell.orders.FulfillmentNode import create_one_handler
from res.flows.FlowContext import FlowContext
from res.utils import logger


CREATE_ONE_ORDERS_KAFKA_TOPIC = "res_sell.create_one_orders.orders"
PROCESS_NAME = "Sell.CreateOneOrderRelay.0.1"
DEAD_LETTER_THRESHOLD = 10


async def process_orders(kafka_client, source_type):
    logger.info(f"Starting orders processor for {source_type}")
    create_one_consumer = ResKafkaConsumer(kafka_client, CREATE_ONE_ORDERS_KAFKA_TOPIC)
    flow_context = FlowContext.from_queue_name(CREATE_ONE_ORDERS_KAFKA_TOPIC)

    while True:
        raw_message = None
        try:
            logger.info("processing message... ..")
            if source_type == "create_one":
                raw_message = create_one_consumer.poll_avro()
                if raw_message is None:
                    logger.debug("kafka poll timed out, continuing....")
                    continue
                flow_context.queue_entered_metric_inc()
                logger.info(raw_message)
                create_one_handler(raw_message)
                flow_context.queue_exit_success_metric_inc()
            else:
                logger.error(f"Unknown source type: {source_type}")
                raise Exception()
            logger.debug(raw_message)
        except Exception as exception:
            the_trace = traceback.format_exc()
            logger.warn(the_trace)
            logger.warn(f"Error processing event: {raw_message}. Error -> {the_trace}")
            flow_context.queue_exception_metric_inc()
            flow_context.write_dead_letter(
                CREATE_ONE_ORDERS_KAFKA_TOPIC,
                raw_message,
                exception_trace=the_trace,
                error_after_counter=DEAD_LETTER_THRESHOLD,
            )
            continue


async def main_loop():
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    logger.info(f"started at {time.strftime('%X')}")
    await process_orders(kafka_client, "create_one")


if __name__ == "__main__":
    asyncio.run(main_loop())
