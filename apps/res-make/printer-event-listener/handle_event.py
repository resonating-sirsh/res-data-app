import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger
from res.flows.make.printer_events_relay import (
    TOPIC as KAFKA_JOB_EVENT_TOPIC,
    handle_new_event,
)
from res.flows.make.print_node_observations_relay import produce_print_node_observations
from res.flows.FlowContext import FlowContext


if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    logger.info("will starting polling for messages and do some work when we find some")
    while True:
        try:
            # Get new messages... nudge...
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing... ....")
                continue
            logger.debug("Found new message!! :)")
            fc.queue_entered_metric_inc()
            handle_new_event(raw_message, fc=fc)
            produce_print_node_observations(raw_message)
            fc.queue_exit_success_metric_inc()

        except Exception as e:
            print(f"{e}")
            err = f"Error processing event: {raw_message}. Error -> {traceback.format_exc()}"
            logger.critical(
                err,
                exception=e,
            )
            fc.queue_exception_metric_inc()
