from datetime import datetime
import os
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.utils import logger


def message_produce(transaction, topic: str):
    kafka_client = ResKafkaClient()

    try:
        with ResKafkaProducer(
            kafka_client, topic, message_type="avro"
        ) as kafka_producer:
            logger.info("on producer...")
            result = {"valid": False}
            result = kafka_producer.produce(
                transaction,
                dedupe=True,
                key_value=transaction["reference_id"],
                key_ts=datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S.%f")[:-3],
            )
            # print(result)
            return result
    except Exception as error:
        logger.error(error)
        print(error)
