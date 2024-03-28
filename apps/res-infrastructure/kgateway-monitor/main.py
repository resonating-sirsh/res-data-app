from res.utils import logger
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.kafka.ResKafkaIntegrations import ResKafkaIntegrations

# This long running process monitors the "_schemas" topic in Kafka for new schemas
# if it finds any, it creates a Kafka topic and S3 Sink if they don't exist


def run_consumer():
    topic = "_schemas"
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, topic)
    kafka_integrations = ResKafkaIntegrations(kafka_client)

    while True:
        msg = kafka_consumer.poll_json(include_key=True)
        if msg is None:
            continue
        logger.info(f"New record from _schemas... {str(msg)}")
        # Ignore Noops and empty schemas
        if (
            not msg["key"]
            or "keytype" not in msg["key"]
            or msg["key"]["keytype"] == "NOOP"
            or msg["value"] is None
        ):
            logger.info("Ignoring, either a NOOP or an empty value")
            continue
        changed_schema = msg["value"]
        if msg["key"]["keytype"] == "DELETE_SUBJECT":
            # Need to delete the S3 Sink and topic?
            logger.info("\tFound deleted subject")
            continue
        if "subject" not in changed_schema or changed_schema["subject"][-4:] == "-key":
            # Don't create topics or sinks for key schemas or schemas without a subject
            logger.info("\tSubject not found or subject is for a key schema, ignoring")
            continue
        topic = changed_schema["subject"].replace("-value", "")
        logger.info(f"\tFound schema : {topic}")
        kafka_client.create_topic(topic)
        # If schema is just "string" sink needs to be in JSON format
        if changed_schema["schema"] == '"string"':
            kafka_integrations.create_s3_sink(topic, format="json")
            logger.info("\tCreated new JSON s3 sink")
        else:
            kafka_integrations.create_s3_sink(topic)
            logger.info("\tCreated new AVRO s3 sink")


if __name__ == "__main__":
    run_consumer()
