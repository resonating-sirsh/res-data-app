"""
receive shopify orders and relay them to the make order processor
shopifyOrder -> 

to test this process locally try something like this :> there are past messages in production
 
    select 
        parse_json(RECORD_CONTENT):id::string as "id" ,
        parse_json(RECORD_CONTENT):sku::string as "sku" ,
        parse_json(RECORD_CONTENT):type::string as "type",
        parse_json(RECORD_CONTENT):created_at::string as "created_at",
        RECORD_CONTENT
    from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_SHOPIFYORDER" 
    where parse_json(RECORD_CONTENT):brand::string = 'TK' limit 100

import res
import json

snowflake. res.connectors.load('snowflake')
orders = snowflake.execute([QUERY HERE])
orders['o'] = orders['RECORD_CONTENT'].map(json.loads)

from res.flows.sell.orders.process import shopify_handler
raw_message = orders.iloc[0]['o']
shopify_handler(raw_message)

....  
 
"""

import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.sell.orders.FulfillmentNode import shopify_handler
from res.flows.FlowContext import FlowContext

KAFKA_JOB_EVENT_TOPIC = "res_sell.shopify.orders"
PROCESS_NAME = "Sell.OrderRelay.0.1"
DEAD_LETTER_THRESHOLD = 10

if __name__ == "__main__":
    kafka_client = ResKafkaClient(process_name=PROCESS_NAME)
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    raw_message = {}

    logger.info("consuming.........")
    while True:
        try:
            raw_message = kafka_consumer.poll_avro(timeout=50)
            if raw_message is None:
                logger.info("kafka poll timed out, continuing.....")
                continue
            logger.info("message received")
            fc.queue_entered_metric_inc()
            logger.info("processing...")
            shopify_handler(raw_message)
            fc.queue_exit_success_metric_inc()
        except Exception as e:
            the_trace = traceback.format_exc()
            logger.warn(f"Error processing event: {raw_message}. Error -> {the_trace}")
            fc.queue_exception_metric_inc()
            # PAGER DUTY FOR PRODUCTION WHEN MATURE EITHER ON EVERY ERROR / DEAD LETTER THRESHOLD
            fc.write_dead_letter(
                KAFKA_JOB_EVENT_TOPIC,
                raw_message,
                exception_trace=the_trace,
                error_after_counter=DEAD_LETTER_THRESHOLD,
            )
