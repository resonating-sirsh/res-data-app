import os
from typing import Tuple, Optional
from confluent_kafka import Consumer, admin, KafkaException
from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    SchemaRegistryError,
    RegisteredSchema,
)
from res.connectors.dynamo.ResDynamoClient import ResDynamoClient
from res.utils import logger


# This is a Resonance Kafka Client, which wraps the utilities of the core kafka library
class ResKafkaClient:
    def __init__(
        self,
        process_name=os.getenv("RES_APP_NAME", "unlabeled"),
        bootstrap_servers=os.getenv(
            "KAFKA_BROKERS",
            "localhost",
        ),
        schema_registry_url=os.getenv(
            "KAFKA_SCHEMA_REGISTRY_URL",
            "localhost",
        ),
        kafka_connect_url=os.getenv("KAFKA_CONNECT_URL", "localhost:8003"),
        environment=os.getenv("RES_ENV", "development"),
    ):
        self._schema_registry_url = schema_registry_url
        self._kafka_connect_url = kafka_connect_url
        self._bootstrap_servers = bootstrap_servers
        self.environment = environment
        self._process_name = process_name
        self._schema_registry_client = SchemaRegistryClient(
            {"url": "http://{}".format(self._schema_registry_url)}
        )
        self.dynamo_client = ResDynamoClient()
        # logger.debug(f"Initialized kafka client {self}", **self.__dict__)

    @property
    def schema_registry_url(self):
        return self._schema_registry_url

    @property
    def process_name(self):
        return self._process_name

    @property
    def bootstrap_servers(self):
        return self._bootstrap_servers

    @property
    def kafka_connect_url(self):
        return self._kafka_connect_url

    @property
    def schema_registry_client(self):
        return self._schema_registry_client

    # Prometheus Logging Helper Function
    def send_kafka_metrics(self, topic, verb, status, value):
        logger.incr(
            "res_kafka." + topic.replace(".", "_") + "." + verb + "." + status, value
        )

    def get_schema(self, topic) -> Tuple[Optional[str], Optional[RegisteredSchema]]:
        # Returns a confluent Schema object
        try:
            schema_obj = self._schema_registry_client.get_latest_version(
                f"{topic}-value"
            )
            return None, schema_obj
        except SchemaRegistryError as exc:
            msg = f"Schema may not exist: {exc!s}"
            logger.error(msg)
            return msg, None

    # Checks if a topic exists, creates if not
    def create_topic(self, topic, partitions=1):
        c = Consumer(
            {
                "bootstrap.servers": self._bootstrap_servers,
                "group.id": "topic_checkers",
                "default.topic.config": {"auto.offset.reset": "smallest"},
                "security.protocol": "ssl",
            }
        )
        topic_metadata = c.list_topics(topic)
        c.close()
        if (
            topic_metadata.topics[topic].error is not None
            and topic_metadata.topics[topic].error.code() == 3
        ):
            # Topic doesn't exist, create it
            admin_client = admin.AdminClient(
                {
                    "bootstrap.servers": self._bootstrap_servers,
                    "security.protocol": "ssl",
                }
            )
            topic_config = {"max.message.bytes": 15048576, "retention.ms": -1}
            topic_list = [
                admin.NewTopic(
                    topic=topic,
                    num_partitions=partitions,
                    replication_factor=3,
                    config=topic_config,
                )
            ]
            fs = admin_client.create_topics(topic_list)
            for _, f in fs.items():
                try:
                    f.result()
                except KafkaException as ex:
                    logger.error(f"  {ex.args[0]}")
            logger.info(f"Topic created: {topic}")
        else:
            logger.info(f"Topic already exists: {topic}")
        return 0
