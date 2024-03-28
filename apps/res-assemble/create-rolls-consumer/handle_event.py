import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.flows.FlowContext import FlowContext
from res.utils import logger
from res.flows.assemble.rolls.create_rolls import handle_new_event

KAFKA_JOB_EVENT_TOPIC = "res_assemble.rolls.create_rolls"

if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    while True:
        try:
            # Get new messages... ...
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.info("kafka poll timed out, continuing... ... ... ...")
                continue
            logger.info(f"Found a new message! -> {raw_message}")
            fc.queue_entered_metric_inc()
            handle_new_event(raw_message)
            fc.queue_exit_success_metric_inc()
        except Exception as e:
            logger.info(f"an error ocurred: {e}")
            err = f"Error processing event: {raw_message}. Error -> {traceback.format_exc()}"
            logger.critical(
                err,
                exception=e,
            )
            fc.queue_exception_metric_inc()
