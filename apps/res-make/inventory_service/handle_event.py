"""
nudge... ...
"""

import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.FlowContext import FlowContext
import res
from res.flows.make.inventory.set_inventory_checkin_hasura.event_processor import (
    handle_event,
)

KAFKA_JOB_EVENT_TOPIC = "res_inventory.check_out_process.one_event"
PROCESS_NAME = "one_event"

if __name__ == "__main__":
    kafka_client = ResKafkaClient(process_name=PROCESS_NAME)
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    raw_message = {}

    res.utils.logger.info(f"consuming...")
    # run through the cursor for now
    while True:
        # temp disable
        # continue

        try:
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing...")
                continue
            fc.queue_entered_metric_inc()

            handle_event(raw_message)

            fc.queue_exit_success_metric_inc()
        except Exception as e:
            the_trace = traceback.format_exc()
            logger.warn(f"Error processing event: {raw_message}. Error -> {the_trace}")
            fc.queue_exception_metric_inc()
