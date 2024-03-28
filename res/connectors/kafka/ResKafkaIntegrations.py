import io, json, requests, uuid, os, time, hashlib
from res.utils import logger, secrets_client
from confluent_kafka import avro, Producer


class ResKafkaIntegrations:
    """
    Class for manipulating Kafka Connect Sinks + Sources.

    :param ResKafkaClient kafka_client: An initialized Kafka Client that handles
                            schema registry and bootstrap server registration
    """

    def __init__(
        self,
        kafka_client,
    ):
        self.kafka_client = kafka_client
        self._env = os.getenv("RES_ENV", "development")

    # Sinks to S3. Flush size is # of messages to buffer before writing to S3, flush_schedule is # of ms
    # Will write when the first of the 2 criteria are met
    def create_s3_sink(
        self, topic, format="avro", flush_size=10000, flush_schedule=180000
    ):
        url = "http://{}/connectors".format(self.kafka_client._kafka_connect_url)
        if format == "json":
            format_class = "io.confluent.connect.s3.format.json.JsonFormat"
        else:
            format_class = "io.confluent.connect.s3.format.avro.AvroFormat"
        payload = {
            "name": "{}-s3-sink".format(topic),
            "config": {
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "tasks.max": "1",
                "topics": topic,
                "timezone": "America/New_York",
                "s3.region": "us-east-1",
                "s3.bucket.name": "res-kafka-out-{}".format(
                    self.kafka_client.environment
                ),
                "s3.part.size": "5242880",
                "rotate.schedule.interval.ms": flush_schedule,
                "flush.size": flush_size,
                "storage.class": "io.confluent.connect.s3.storage.S3Storage",
                "format.class": format_class,
                "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
                "schema.compatibility": "NONE",
            },
        }
        response = requests.post(url, json=payload)
        logger.info(
            "Attempted to create S3 sink: {}, status_code:{}".format(
                topic, str(response.status_code)
            )
        )
        if response.status_code < 200 or response.status_code > 299:
            logger.warn("Error creating sink: {}".format(response.text))
        return response.status_code

    # Sinks to Snowflake
    def create_snowflake_sink(self, topic, format="avro"):
        url = "http://{}/connectors".format(self.kafka_client._kafka_connect_url)
        clean_topic = topic.replace(".", "_").replace("-", "_")
        if format == "avro":
            format_class = (
                "com.snowflake.kafka.connector.records.SnowflakeAvroConverter"
            )
        else:
            format_class = (
                "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
            )
        payload = {
            "name": "snowflake_{}_{}".format(clean_topic, str(round(time.time()))),
            "config": {
                "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
                "topics": topic,
                "snowflake.topic2table.map": f"{topic}:kafka_{clean_topic}",
                "snowflake.url.name": "https://jk18804.us-east-1.snowflakecomputing.com:443",
                "snowflake.user.name": "kafka_connector_user_{}".format(
                    self.kafka_client.environment
                ),
                "snowflake.database.name": "iamcurious_db",
                "snowflake.schema.name": "iamcurious_{}".format(
                    self.kafka_client.environment
                ),
                "key.converter": "com.snowflake.kafka.connector.records.SnowflakeJsonConverter",
                "value.converter": format_class,
                "value.converter.schema.registry.url": "http://{}".format(
                    self.kafka_client._schema_registry_url
                ),
                "snowflake.private.key": secrets_client.get_secret(
                    "SNOWFLAKE_KAFKA_PRIVATE_KEY"
                ),
            },
        }
        logger.info("url: {}".format(url))
        logger.info("payload: {}".format(json.dumps(payload, indent=4)))
        response = requests.post(url, json=payload)
        logger.info(
            "Attempted to create Snowflake sink: {}, status_code:{}".format(
                topic, str(response.status_code)
            )
        )
        if response.status_code < 200 or response.status_code > 299:
            logger.warn("Error creating sink: {}".format(response.text))
        return response.status_code

    # Sinks to Postgres
    def create_postgres_sink(
        self, topic, insert_mode="insert", pk_mode="none", pk_fields="none"
    ):
        # Insert modes can be insert, upsert, or update
        # PK modes can be none or record_value
        # If PK mode is record_value, pk_fields should be a comma separated list of fields
        url = "http://{}/connectors".format(self.kafka_client._kafka_connect_url)
        clean_topic = topic.replace(".", "_").replace("-", "_")
        pg_hostname = os.getenv("PRIMARY_RDS_ENDPOINT")
        pg_url = f"jdbc:postgresql://{pg_hostname}:5432/res_primary?sslmode=require&currentSchema=kafka"
        pg_user = "kafka"
        pg_pass = secrets_client.get_secret("RDS_PRIMARY_KAFKA_PASSWORD")
        payload = {
            "name": "postgres_sink_{}_{}".format(clean_topic, str(round(time.time()))),
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                "topics": topic,
                "connection.url": pg_url,
                "connection.user": pg_user,
                "connection.password": pg_pass,
                "dialect.name": "PostgreSqlDatabaseDialect",
                "insert.mode": insert_mode,
                "table.name.format": topic.replace(".", "__"),
                "pk.mode": pk_mode,
                "pk.fields": pk_fields,
                "auto.create": "true" if self._env == "development" else "false",
                "auto.evolve": "true" if self._env == "development" else "false",
            },
        }
        logger.info("url: {}".format(url))
        logger.debug("payload: {}".format(json.dumps(payload, indent=4)))
        response = requests.post(url, json=payload)
        logger.info(
            "Attempted to create Postgres sink: {}, status_code:{}".format(
                topic, str(response.status_code)
            )
        )
        if response.status_code < 200 or response.status_code > 299:
            logger.error("Error creating sink: {}".format(response.text))
        return response.status_code
