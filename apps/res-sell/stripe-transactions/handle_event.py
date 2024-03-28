import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.sell.subscription.transaction import handle_new_event

from res.flows.FlowContext import FlowContext

KAFKA_JOB_EVENT_TOPIC = "res_sell.subscription.transaction"
DEAD_LETTER_THRESHOLD = 10
# nudge

if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    while True:
        try:
            # Get new messages... ...
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing...")
                continue
            logger.info("Processing message of res_sell.subscription.transaction")
            fc.queue_entered_metric_inc()
            handle_new_event(raw_message)
            fc.queue_exit_success_metric_inc()

        except Exception as e:
            print(f"{e}")
            trace = traceback.format_exc()
            error = f"Error processing event: {raw_message}. Error -> {trace}"
            logger.warn(error)
            fc.queue_exception_metric_inc()
            fc.write_dead_letter(
                KAFKA_JOB_EVENT_TOPIC,
                raw_message,
                exception_trace=trace,
                error_after_counter=DEAD_LETTER_THRESHOLD,
            )
