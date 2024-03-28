import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger, ping_slack
from res.flows.finance.stripe_events_handler import (
    TOPIC as KAFKA_JOB_EVENT_TOPIC,
    handle_new_event,
)
from res.flows.FlowContext import FlowContext

# nudge

if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)
    fc = FlowContext.from_queue_name(KAFKA_JOB_EVENT_TOPIC)
    logger.info("will starting polling for messages and do some work when we find some")
    while True:
        try:

            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.debug("kafka poll timed out, continuing..")
                continue
            logger.debug("Found new message")
            fc.queue_entered_metric_inc()
            handle_new_event(raw_message, fc=fc)

            fc.queue_exit_success_metric_inc()

        except Exception as e:
            slack_id_to_notify = " <@U04HYBREM28> "
            payments_api_slack_channel = "payments_api_notification"
            logger.error(f"{e}")
            err = f"{slack_id_to_notify}kafka topic handler: {KAFKA_JOB_EVENT_TOPIC} \n\n\n\n\n Error processing event: {raw_message}. \n\n\n\n Error -> {traceback.format_exc()}"
            logger.error(
                err,
                exception=e,
            )

            ping_slack(err, "payments_api_notification")
            fc.queue_exception_metric_inc()
