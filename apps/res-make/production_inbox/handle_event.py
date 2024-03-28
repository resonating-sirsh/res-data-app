"""
nudge...    ....
"""

import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.FlowContext import FlowContext
import res
from res.flows.make.production.inbox import handle_event


KAFKA_JOB_EVENT_TOPIC = "res_make.orders.one_requests"
PROCESS_NAME = "production-inbox-listener"
DEAD_LETTER_THRESHOLD = 10

if __name__ == "__main__":
    kafka_client = ResKafkaClient(process_name=PROCESS_NAME)
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    raw_message = {}

    res.utils.logger.info(f"consuming... ")
    # run through the cursor for now
    while True:
        # temp disable
        # continue

        try:
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing.... ")
                continue
            fc.queue_entered_metric_inc()

            # we raise errors for dead letter queue
            # disabling the creation here - will resume when kafka sources fully trusted
            # handle_event(raw_message, on_error="raise")

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
