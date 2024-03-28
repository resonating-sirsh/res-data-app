import os
from res.utils import logger
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaIntegrations import ResKafkaIntegrations


# Gets topic from UI and creates Snowflake Sink
if __name__ == "__main__":
    topic = os.getenv("KAFKA_TOPIC")
    logger.info("Creating snowflake sink for topic: {}".format(topic))
    kafka_client = ResKafkaClient()
    kafka_integrations = ResKafkaIntegrations(kafka_client)
    response_code = kafka_integrations.create_snowflake_sink(topic)
    if response_code == 201:
        logger.info("Topic created!")
    else:
        msg = "Error creating Snowflake topic: {}".format(str(response_code))
        logger.warn(msg)
        raise Exception(msg)
