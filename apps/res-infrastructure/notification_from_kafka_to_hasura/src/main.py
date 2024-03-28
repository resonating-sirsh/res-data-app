import os
import traceback

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.graphql.hasura import Client
from res.utils import logger

from src.connectors.queries import INSERT_MESSAGE

ENV = os.getenv("RES_ENV")
TOPIC = "res_infrastructure.notification_system.airtable_event"


# Main kafka consumer loop
def run_consumer(kafka_consumer):
    hasMore = True
    hasura_client = Client()
    while hasMore:
        try:
            # Get new messages
            raw_message = kafka_consumer.poll_avro()
            if raw_message is None:
                logger.info("kafka poll timed out, continuing...")
                continue

            logger.info("Found new notification: {}".format(str(raw_message)))

            insert_hasura_data = hasura_client.execute(
                INSERT_MESSAGE,
                {
                    "record_value": {
                        "base_id": raw_message["base_id"],
                        "table_id": raw_message["table_id"],
                        "record_id": raw_message["record_id"],
                        "notification_type": raw_message["notification_type"]
                        if raw_message["notification_type"]
                        else "Late Order Notification",
                    },
                },
            )
            logger.info(insert_hasura_data)
        except Exception as e:
            err = "Error processing hasura insert: {}".format(traceback.format_exc())
            logger.critical(
                err,
                exception=e,
            )


if __name__ == "__main__":
    # Initialize clients
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, TOPIC)
    # # Start Kafka consumer
    run_consumer(kafka_consumer)
