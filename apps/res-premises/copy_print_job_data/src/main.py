import os, json, logging, random, time
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger

# This long running process shows how to consume + produce with Kafka

KAFKA_INPUT_TOPIC = "printJobData"
KAFKA_OUTPUT_TOPIC = "res_premises.printer_data_collector.print_job_data"

def run_consumer(kafka_client, kafka_consumer):
    i = 0
    while True:
    # while i == 0:
        msg = kafka_consumer.poll_avro()
        if msg is None:
            # Sometimes the message is None the first try, will show up eventually
            continue
        logger.debug("New record from kafka: {}".format(str(i)))
        i += 1

        # Create and send random message
        kafka_client.produce(KAFKA_OUTPUT_TOPIC, msg)

        

if __name__ == "__main__":
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_INPUT_TOPIC)
    run_consumer(kafka_client, kafka_consumer)
