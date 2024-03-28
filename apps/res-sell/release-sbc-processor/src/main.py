import hashlib
import json

from src.query_definition import GET_SBC_ID, RELEASE_SBC, UPDATE_SBC

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger

# This long running process shows how to consume + produce with Kafka

KAFKA_TOPIC = "res_sell.shop_by_color.release_sbc"
# Below shows how to avoid black formatter raising an error


def run_kafka():
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_TOPIC)
    graphql_client = ResGraphQLClient()
    while True:
        try:
            logger.debug("Retrieving message from Kafka...")
            msg = kafka_consumer.poll_avro()
            if msg is None:
                # Sometimes the message is None the first try, will show up eventually
                logger.debug("...")
                continue
            logger.info("New record from kafka: {}".format(msg))

            sbc_id: str = msg["shop_by_color_id"]
            logger.info(f"Retrived shop by color: {sbc_id}")
            response = graphql_client.query(GET_SBC_ID, {"id": sbc_id})

            sbc = response["data"]["shopByColorInstance"]
            logger.debug(f"shop by color: {sbc}")
            logger.info(f"releasing shop by color: {sbc['id']}")
            logger.debug(f"configurations: {sbc['configurations']}")

            sbc_checksum = hashlib.md5(
                bytes(json.dumps(sbc["configurations"]), "utf-8")
            ).hexdigest()

            logger.debug(f"checksum: {sbc_checksum}")

            if "checksum" in sbc and sbc["checksum"] == sbc_checksum:
                logger.info("skipping release the groups has been changes")
                continue

            logger.debug(f"releasing products: {[p['id'] for p in sbc['products']]}")
            for product in sbc["products"]:
                logger.info(f"releasing product: {product['id']}")
                graphql_client.query(
                    RELEASE_SBC, {"id": product["id"], "sbc_id": sbc["id"]}
                )

            graphql_client.query(
                UPDATE_SBC,
                {
                    "id": sbc["id"],
                    "input": {"status": "success", "checksum": sbc_checksum},
                },
            )

        except Exception as e:
            raise e


if __name__ == "__main__":
    run_kafka()
