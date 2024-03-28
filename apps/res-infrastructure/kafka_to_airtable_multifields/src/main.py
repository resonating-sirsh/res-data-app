import os, time, traceback

from tenacity import retry, wait_random_exponential
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from airtable import Airtable
from res.utils import logger, secrets_client

### Consumes messages from airtable_updates and makes the corresponding update to Airtable

ENV = os.getenv("RES_ENV")
TOPIC = "res_infrastructure.kafka_to_airtable.airtable_multifields_update"
AIRTABLE_API_SECRET = "AIRTABLE_API_KEY"
# Milliseconds required between Airtable API calls
API_RATE_LIMIT_MS = 1000


def get_time_ms():
    return round(time.time() * 1000)


@retry(wait=wait_random_exponential(multiplier=5, max=600))
def update_airtable(airtable_client, airtable_table, airtable_record, payload):
    airtable_client.update(
        airtable_table,
        airtable_record,
        payload,
    )


# Main kafka consumer loop
def run_consumer(kafka_consumer):
    airtable_api_key = secrets_client.get_secret(AIRTABLE_API_SECRET)
    last_airtable_api_call = get_time_ms()
    while True:
        try:
            # Get new messages
            raw_message = kafka_consumer.poll_avro()
            if raw_message is None:
                logger.info("kafka poll timed out, continuing...")
                continue
            logger.info("Found new message: {}".format(str(raw_message)))

            # Get reference to Airtable
            airtable_client = Airtable(
                raw_message["airtable_base"],
                airtable_api_key,
            )

            # Check if we are calling Airtable too fast
            if get_time_ms() - last_airtable_api_call < API_RATE_LIMIT_MS:
                logger.info("Rate limit hit, sleeping...")
                time.sleep(
                    (API_RATE_LIMIT_MS - (get_time_ms() - last_airtable_api_call))
                    / 1000
                )

            update_airtable(
                airtable_client,
                raw_message["airtable_table"],
                raw_message["airtable_record"],
                raw_message["payload"],
            )

            last_airtable_api_call = get_time_ms()
            logger.info("    Updated airtable!")
        except Exception as e:
            err = "Error processing airtable update: {}".format(traceback.format_exc())
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
