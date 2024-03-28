import traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger

import res.flows.dxa.event_handler as dxa_event_handler

KAFKA_JOB_EVENT_TOPIC = "res_platform.job.event"

# _nudge ............
# Fix the consumer group, so that we don't replay all the messages in the topic
# on app-name change
CONSUMER_GROUP = "dxa-job-consumer"

if __name__ == "__main__":
    kafka_client = ResKafkaClient(process_name=CONSUMER_GROUP)
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)

    while True:
        try:
            # Get new messages
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.info(
                    "kafka poll timed out, continuing......................................................"
                )
                continue
            logger.info("Found new message!")
            dxa_event_handler.handle(raw_message)

        except Exception as e:
            err = f"Error processing: {traceback.format_exc()}.\n {raw_message}"

            logger.critical(
                err,
                exception=e,
            )
