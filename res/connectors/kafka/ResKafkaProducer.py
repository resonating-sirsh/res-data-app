import io, json, requests, uuid, os, time, hashlib
from res.utils import logger
from confluent_kafka import avro, Producer
import redis


class ResKafkaProducer:
    """
    Wrapper around avro_producer.

    :param ResKafkaClient kafka_client: An initialized Kafka Client that handles
                            schema registry and bootstrap server registration
    :param string topic: The kafka topic to produce to.
    :param RegisteredSchema value_schema: An optional Avro value schema. If not
                            provided, will fetch the latest schema via kafka_client
                            from the Schema Registry
    :param bool auto_flush: When true, the producer will flush the existing
                            messages upon either exit or uncaught exception.
    :param string message_type: One of avro,json . Will determine settings in the
                            producer to send the right data type to kafka.

    intended usage:
    with ResKafkaProducer(...) as producer:
        producer.produce(my_dict_data)
    """

    def __init__(
        self,
        kafka_client,
        topic,
        value_schema=None,
        auto_flush=True,
        message_type="avro",
    ):
        self.kafka_client = kafka_client
        self.topic = topic
        self._producer_config = {
            "bootstrap.servers": self.kafka_client._bootstrap_servers,
            "security.protocol": "ssl",
        }
        self._auto_flush = auto_flush
        self.message_type = message_type
        if message_type == "avro":
            self._producer_config["schema.registry.url"] = "http://{}".format(
                self.kafka_client._schema_registry_url
            )
            if not value_schema:
                error, value_schema = self.kafka_client.get_schema(topic)
                if error != None:
                    msg = "Error retrieving schema from Kafka! Schema may not be initialized for this topic."
                    logger.error(msg)
            avro_schema = avro.loads(value_schema.schema.schema_str)
            self.producer = avro.AvroProducer(
                self._producer_config, default_value_schema=avro_schema
            )
        else:
            self.producer = Producer(self._producer_config)
        redis_host = os.getenv("REDIS_HOST", "localhost")
        self._redis_client = redis.Redis(host=redis_host, port=6379, db=0)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        if self._auto_flush:
            self.flush()

    def flush(self):
        self.producer.flush()

    def produce(self, data, dedupe=False, key_value=None, key_ts=None):
        # Check for duplicates if requested
        to_produce = True
        if dedupe and key_value and key_ts:
            key_ts = str(key_ts)
            # Generate hash of the data
            hash = hashlib.md5(json.dumps(data).encode()).hexdigest()
            # The key is a combo of the topic and primary key -- e.g. order number
            key = f"kafka_hash_checks_{self.topic}_{key_value}"
            existing_data = self._redis_client.get(key)
            if existing_data:
                existing_data = json.loads(existing_data)
                # If a record for that primary key exists and the hash is the same, don't produce
                if existing_data["h"] == hash:
                    logger.debug("Found duplicate!")
                    to_produce = False
                # If the record exists but the value in Redis is more recent than the one we're trying
                #    to produce, don't produce the data
                elif existing_data["t"] > key_ts:
                    logger.warn(
                        "Hash is different, but current value is older. Not pushing to Kafka!"
                    )
                    to_produce = False
        if to_produce:
            try:
                if self.message_type == "avro":
                    self.producer.produce(topic=self.topic, value=data)
                else:
                    self.producer.produce(self.topic, json.dumps(data))
                self.kafka_client.send_kafka_metrics(self.topic, "received", "ok", 1)
                if dedupe:
                    self._redis_client.set(key, json.dumps({"h": hash, "t": key_ts}))

            except Exception as e:
                msg = "Error sending to Kafka! Check your json to make sure it is compatible with the topic schema. {}".format(
                    e
                )
                logger.warn(msg)
                self.kafka_client.send_kafka_metrics(self.topic, "received", "error", 1)
                return msg
            if self._auto_flush:
                self.flush()

        return to_produce if dedupe else True
