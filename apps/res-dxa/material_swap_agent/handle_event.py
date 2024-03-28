"""

 
"""

import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.meta.ONE.style_node import swap_event_handler
from res.flows.FlowContext import FlowContext

KAFKA_JOB_EVENT_TOPIC = "res_sell.swap_material_sell.swap_material"
PROCESS_NAME = "dxa.material-swap-agent"
DEAD_LETTER_THRESHOLD = 10

if __name__ == "__main__":
    kafka_client = ResKafkaClient(process_name=PROCESS_NAME)
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    raw_message = {}

    while True:
        try:
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing.....")
                continue
            fc.queue_entered_metric_inc()
            swap_event_handler(raw_message)
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
