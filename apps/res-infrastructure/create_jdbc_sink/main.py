import os
from res.utils import logger
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaIntegrations import ResKafkaIntegrations


# Gets topic from UI and creates Snowflake Sink
if __name__ == "__main__":
    topic = os.getenv("KAFKA_TOPIC")
    insert_mode = os.getenv("INSERT_MODE")
    pk_mode = os.getenv("PK_MODE")
    pk_fields = os.getenv("PK_FIELDS")
    logger.info("Creating JDBC sink for topic: {}".format(topic))
    kafka_client = ResKafkaClient()
    kafka_integration = ResKafkaIntegrations(kafka_client)
    response_code = kafka_integration.create_postgres_sink(
        topic, insert_mode=insert_mode, pk_mode=pk_mode, pk_fields=pk_fields
    )
    if response_code == 201:
        logger.info("Sink created!")
    else:
        msg = "Error creating JDBC sink: {}".format(str(response_code))
        logger.warn(msg)
        raise Exception(msg)
