import os, json, logging, random, time, datetime
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.dynamo.ResDynamoClient import ResDynamoClient
from res.utils import logger

# This long running process shows how to consume + produce with Kafka

KAFKA_TOPIC = "example_topic"
# Below shows how to avoid black formatter raising an error
# fmt: off
SAMPLE_NAMES = [
    "Zed",
"Ognyana",
"Rakhi", "Quinto", "Kir"]
# fmt: on


def run_kafka():
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_TOPIC)
    while True:
        try:
            ### Basic Kafka Usage ###

            # Create random message
            temp_message = {
                "name": SAMPLE_NAMES[random.randint(0, 4)],
                "age": random.randint(1, 100),
            }
            # Send to kafka
            logger.debug("Sending message to Kafka...")
            with ResKafkaProducer(kafka_client, KAFKA_TOPIC) as kafka_producer:
                kafka_producer.produce(temp_message)
            # Sleep for a while, just used in this example to avoid unnecessary load
            time.sleep(3)
            # Retrieve the message we just sent
            logger.debug("Retrieving message from Kafka...")
            msg = kafka_consumer.poll_avro()
            if msg is None:
                # Sometimes the message is None the first try, will show up eventually
                logger.debug("...")
                continue
            logger.debug("New record from kafka: {}".format(msg))

            ### Using Deduplication ###
            new_message = {
                "name": SAMPLE_NAMES[random.randint(0, 4)],
                "age": random.randint(1, 100),
            }
            logger.info(f"Message: {new_message}")
            ts = datetime.datetime.now()
            with ResKafkaProducer(kafka_client, KAFKA_TOPIC) as kafka_producer:
                logger.debug("Producing for the first time...")
                produced = kafka_producer.produce(
                    new_message, dedupe=True, key_value=new_message["name"], key_ts=ts
                )
                logger.debug(f"Produced: {produced}")
                logger.debug("Producing for the second time...Should not produce!")
                produced = kafka_producer.produce(
                    new_message, dedupe=True, key_value=new_message["name"], key_ts=ts
                )
                logger.debug(f"Produced: {produced}")
            logger.debug(f"Done!")
            time.sleep(10)

        except Exception as e:
            logger.error(str(e))
            raise e


if __name__ == "__main__":
    run_kafka()
