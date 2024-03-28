from . import logger
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.dynamo.DynamoConnector import (
    KafkaTopicDynamoTable,
)
import pandas as pd
import numpy as np
import os
import json
from res.utils import dataframes, safe_http
from tenacity import retry, wait_fixed, stop_after_attempt
import res
import fastavro
import confluent_kafka
from confluent_kafka import Producer, avro
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField,
)
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import os
import traceback

# TODO: refactor
# the functionality in this class is experimental but should be refactored
# We should have a single Kafka client (now we have multiple)
# Dataframes are just a wrapper around record writing with some caching of things like schema
# We also use pandas for coercing schema etc as data manipulation with pandas is much better than raw python
# There was an inner "Pandas" provider that became the connector but the idea was we could take all the dataframe stuff and put it in the ultimate kafka client
# However this makes some of the semantics as is a bit weird
# There are some methods that implement "res connectors" interfaces which are a WIP. For example ever connector should do certain things
# One such thing is taking res-schema and exporting the schema to its system. For kafka this means updating a topic/schema registry entry
# Those methods should be called the same thing on all connectors but will implement in a provider specific way


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


def _check_field_value(v):
    try:
        if isinstance(v, list):
            return v
        if v is None or pd.isnull(v):
            return None
    except:
        return v


class SimpleProducer:
    """
    https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/avro_producer.py
    """

    def __init__(self, topic):
        self._topic = topic
        schema_host_and_port = f"http://{os.getenv('KAFKA_SCHEMA_REGISTRY_URL')}"

        schema_registry_conf = {"url": schema_host_and_port}
        reg = SchemaRegistryClient(schema_registry_conf)
        s = reg.get_latest_version(f"{topic}-value")

        self._string_serializer = StringSerializer("utf_8")
        self._avro_serializer = AvroSerializer(
            reg, s.schema.schema_str, lambda data, ctx: data
        )

        self._schema = s
        self._schema_str = s.schema.schema_str

        producer_conf = {
            "bootstrap.servers": os.environ.get("KAFKA_BROKERS"),
            "security.protocol": "ssl",
        }
        self._producer = Producer(producer_conf)
        self._auto_flush = True

        self._avro_producer = avro.AvroProducer(
            producer_conf, default_value_schema=avro.loads(self._schema_str)
        )

    @staticmethod
    def delivery_report(err, msg):
        """."""

        if err is not None:
            res.utils.logger.warn(
                f"(Confluent Kafka Version:{confluent_kafka.__version__}):Error: Delivery failed for User record {msg.key()}: {err}"
            )
            # raise Exception(f"Failed to produce message")
        res.utils.logger.debug(
            "User record {} successfully produced to {} [{}] at offset {}".format(
                msg.key(), msg.topic(), msg.partition(), msg.offset()
            )
        )

    def __call__(self, data):
        return self.produce(data)

    def produce(self, data, redecode=False, pre_validate=False):
        if redecode:
            """
            to get rid of any numpy funnyness we can redecode
            """
            data = json.loads(json.dumps(data, cls=NpEncoder))

        if pre_validate:
            try:
                # check for better error handling that we can validate this thing
                msg = self._avro_serializer(
                    data, SerializationContext(self._topic, MessageField.VALUE)
                )
            except:
                res.utils.logger.warn(
                    f"Failing to serialize message with fields { {k:type(data[k]) for k in data.keys()}} validation errors are {fastavro.validate(data, self._schema_str)}  "
                )
                raise

        # this produces magic byte error :shrug
        # response = self._producer.produce(
        #     topic=self._topic,
        #     key=self._string_serializer(str(uuid4()), ctx=None),
        #     value=msg,
        #     on_delivery=SimpleProducer.delivery_report,
        # )
        return self._avro_producer.produce(data)

        if self._auto_flush:
            self._producer.flush()

        return response


class _DataFramePubSub(object):
    # we do not yet distinguish ints and longs. longs are safer but more expensive.
    PYTHON_TYPE_MAP = {
        "int": "long",
        "str": "string",
        "datetime": "string",
        "bool": "boolean",
        "float": "float",
    }

    def __init__(
        self,
        topic,
        process_name=None,
        namespace=None,
        topic_config=None,
        max_errors=-1,
        on_max_errors="warn",
        consumer_config={},
    ):
        # TODO = the namespace can be the kafka connector schema but not sure if we can use it like that

        self._namespace = (
            "res-data" if "." not in topic else topic.split(".")[0]
        )  # os.environ.get("RES_NAMESPACE", "res-data" if "." not in topic else topic.split(".")[0] )
        process_name = process_name or os.environ.get("RES_APP_NAME", "res-data")
        self._max_errors = max_errors
        self._on_max_errors = on_max_errors

        logger.debug(f"Setting up kafka connector for process {process_name}")
        self._topic = topic
        self._process_name = process_name
        self._env = os.environ.get("RES_ENV", "development")
        self._topic_config = topic_config
        # this nshould not always be the gateway but leaving it for now
        self._submit_url = os.environ.get(
            "KAFKA_KGATEWAY_URL", "https://datadev.resmagic.io/kgateway/submitevent"
        )

        # just for back compat - the urls should use a full scheme convention but sometimes do not
        if "http" not in self._submit_url[:5]:
            # assuming that internal one is just http:// - bit weird
            self._submit_url = f"http://{self._submit_url}/kgateway/submitevent"

        self._replica_url = self._make_replica_url(self._submit_url)
        self._consumer_config = consumer_config
        self._bootstrap_servers = os.environ.get("KAFKA_BROKERS")
        self._schema_host_and_port = os.getenv(
            "KAFKA_SCHEMA_REGISTRY_URL", "localhost:8001"
        )
        self._schema_reg_url = f"http://{self._schema_host_and_port}"
        self._consumer = None
        self._producer = None
        self._resKafkaClient = None

        # the dedup uses dynamo and we need to distinguish the env as it would be a separate table
        dedup_table = "kafka_topic_keys"
        if self._env in ["production"]:
            dedup_table = f"{dedup_table}_{self._env}"

        self._deduper = KafkaTopicDynamoTable(table_id=dedup_table)

    @property
    def res_client(self):
        if self._resKafkaClient is None:
            self._resKafkaClient = ResKafkaClient(
                self._process_name,
                os.getenv("KAFKA_BROKERS"),
                self._schema_host_and_port,
                os.getenv("KAFKA_CONNECT_URL", "localhost:8003"),
                self._env,
            )
        return self._resKafkaClient

    @property
    def consumer(self):
        if self._consumer is None:
            self._consumer = ResKafkaConsumer(
                kafka_client=self.res_client,
                topic=self._topic,
                topic_config=self._topic_config,
                consumer_config=self._consumer_config,
            )
        return self._consumer

    @property
    def producer(self):
        if self._producer is None:
            self._producer = SimpleProducer(self._topic)
        return self._producer

    def produce(self, data, redecode=False):
        # return self.producer.produce(data, redecode=redecode)
        with ResKafkaProducer(self.res_client, self._topic) as kafka_producer:
            return kafka_producer.produce(data)

    def _make_replica_url(self, url):
        """
        We determine a replica from the url
        This could be a new env variable but hard coding the rule for now as not sure how this will be used
        """
        if "datadev.resmagic" in url:
            return url.replace("datadev.resmagic", "data.resmagic")
        else:
            return url.replace("data.resmagic", "datadev.resmagic")

    def iter_partition_files(self, since_date=None):
        from res.connectors.s3 import S3Connector

        path = f"s3://res-kafka-out-{self._env}/topics/{self._topic}"
        logger.debug(f"Loading samples from {path}")
        s3 = S3Connector()
        for file in s3.ls(path):
            yield file

    @property
    def all_topics(self):
        """
        list all the topics/subjects that we have created
        """
        return safe_http.request_get(f"{self._schema_reg_url}/subjects").json()

    def get_samples(self, since_date=None, limit=10, raw=False):
        from res.connectors.s3 import S3Connector

        s3 = S3Connector()
        data = []
        for i, f in enumerate(self.iter_partition_files(since_date=since_date)):
            if i < limit:
                # read the avro files
                data.append(s3.read(f))

        data = (
            pd.concat(data).reset_index(drop=True)[:limit]
            if len(data) > 0
            else pd.DataFrame()
        )

        return data if not raw else data.to_dict("records")

    def _make_record(self, row):
        return {
            "topic": self._topic,
            "process_name": self._process_name,
            "version": "1.0.0",
            "data": row,
        }

    @property
    def topic_schema(self):
        schema = self.get_schema()
        # in general there can be multiple schema so add them to a dict by name
        return (
            {s["name"]: s for s in schema}
            if isinstance(schema, list)
            else {self._topic: schema}
        )

    def get_schema(self):
        # try to get it locally from the repo and only then go elsewhere
        pth = ""
        try:
            # assuming convention for now or go to registry
            parts = self._topic.split(".")
            parts[0] = parts[0].replace("_", "-")
            parts[1] = parts[1].replace("_", "-")
            parts = f"/".join(parts)
            pth = str(res.utils.get_res_root() / "res-schemas/avro" / f"{parts}.avsc")
            with open(pth) as f:
                schema = json.load(f)
        except:
            res.utils.logger.warn(f"Schema at path not in repo: {pth}")
            # get the last part of the response tuple
            _, schema_obj = self.res_client.get_schema(self._topic)
            schema = schema_obj.schema.schema_str
            schema = json.loads(schema)
        return schema

    def validate_payload(self, data):
        """
        todo refactor so that schema
        """
        import fastavro

        # TODO handle collections of payloads

        return fastavro.validate(data, self.get_schema())

    @property
    def snowflake_table_name_if_exists(self):
        return f"KAFKA_{self._topic.upper()}".replace(".", "_")

    def create_or_update_res_schema(
        self, python_field_types, nullable_fields, namespace="res-data"
    ):
        """
        This is a provider interface method that calls into the avro/kafka one
        These methods use the same python dict of types
        """
        entity = self._topic
        schema = _DataFramePubSub.make_avro_schema(
            entity, namespace, python_field_types, nullable_fields=nullable_fields
        )

        # we should always return the "response structure" even if we have to wrap
        return self._create_or_update_res_schema(schema)

    def _create_or_update_res_schema(
        self, avro_schema, name=None, allow_nested_types=False, compatibility="NONE"
    ):
        """
        Given a schema for avro that we trust generated from something in source control
        we can upsert the schema and set the compatibility level to whatever
        This breaks down if things are updated outside this flow

        The caller should check the status code 200 on the transaction

        #for example if you know the type -> we return the avro schema from some methods in a dict so it needs to be a list
        #but in simple cases for example you can pull out the top level single item - dont do this normally, this is just to illustrate
        #you should send the single item or the list of items to schema registry
        k = kafka['res_sell.create_one_orders.orders']
        s = k.topic_schema['res_sell.create_one_orders.orders']
        k._create_or_update_res_schema(s)


        """
        if name:
            a = avro_schema[name]
            fqname = f"{a['namespace']}.{a['name']}".replace("-", "_")
        else:
            fqname = f"{avro_schema['namespace']}.{avro_schema['name']}".replace(
                "-", "_"
            )
        url = f"{self._schema_reg_url}/subjects/{fqname}-value/versions"
        logger.info(f"posting to {url}")

        # the schema is json strong insteide a json object
        schema = {"schema": json.dumps(avro_schema)}
        logger.debug(f"posting payload {schema}")
        # res.utils.safe_http.request_post(url, json=schema, headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"})
        req = safe_http.request_post(
            url,
            json=schema,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        )

        if req.status_code == 200:
            logger.debug("schema updated")
            url = f"{self._schema_reg_url}/config/{fqname}-value"
            logger.debug(f"defaulting compatibility to {compatibility}")
            req = safe_http.request_put(
                url,
                json={"compatibility": compatibility},
                headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            )
            if req.status_code != 200:
                logger.debug(
                    f"created schema but could not set compatibility which will lead to problems updating in future: {req.json()}"
                )
                return req
            else:
                logger.debug("compatibility set")

    def _delete_res_entity(self, version="latest"):
        """
        This deletes the topic and may do other cleanup
        """
        entity = self._topic
        url = f"{self._schema_reg_url}/subjects/{entity}-value/versions/{version}"
        return safe_http.request_delete(url)

    def is_published(self, message, key_field):
        key = message[key_field]
        # if we add then return True. If we cannot add, return False
        return not self._deduper.try_add_new_key(self._topic, key)

    def _filter_new(self, df, key_field):
        if key_field is None:
            return df

        keys = self._deduper._get_existing_keys(df, key_field)

        return df[df[key_field].isin(keys)]

    @retry(wait=wait_fixed(2), stop=stop_after_attempt(3))
    def safe_produce(self, record, schema):
        return self.producer(record)

    def coerce_record(self, payload):
        """
        convenience method to see what the converted payload looks like after coercion
        """
        topic_name_no_qual = self._topic.split(".")[-1]
        aschema = self.topic_schema.get(
            self._topic, self.topic_schema.get(topic_name_no_qual)
        )

        dataframe = pd.DataFrame([payload])
        dataframe = dataframes.coerce_to_avro_schema(dataframe, avro_schema=aschema)

        return dict(dataframe[[f["name"] for f in aschema["fields"]]].iloc[0])

    def publish(
        self,
        dataframe,
        dedup_on_key=None,
        coerce=False,
        show_failed=False,
        use_kgateway=False,
        publish_to_replica=False,
        snake_case_names=False,
        redecode=False,
    ):
        # just for convenience to publish a single record or list of records
        if isinstance(dataframe, dict):
            dataframe = pd.DataFrame([dataframe])
        if isinstance(dataframe, list):
            dataframe = pd.DataFrame(dataframe)

        if snake_case_names:
            dataframe.columns = dataframes.snake_case_names(list(dataframe.columns))

        if len(dataframe) > 0:
            logger.debug(
                f"Writing dataframe of length {len(dataframe)} to topic {self._topic} - use_kgateway: {use_kgateway}"
            )

            error_count = 0
            error, schema = None, self.topic_schema
            if error != None:
                raise Exception(f"Error getting schema - {error}")

            if coerce:
                topic_name_no_qual = self._topic.split(".")[-1]
                logger.debug(f"coercing dataframe to schema for topic {self._topic}")
                schema = self.topic_schema
                dataframe = dataframes.coerce_to_avro_schema(
                    dataframe,
                    avro_schema=schema.get(self._topic, schema.get(topic_name_no_qual)),
                )

            skipped_duplicates = 0
            row_counter = 0

            # this can be added for performance in batch but we still check and write later i.e. this is a read only check
            # unlike the try_publish which will commit if not exists
            # records = self._filter_new(dataframe, dedup_on_key)
            records = dataframe.to_dict("records")
            for record in records:
                # kgwateway is easy and accessible everywhere but there is a perf hit
                # outside of the cluster we can go via the gateway by default but otherwise use the client directly
                # especially when coercing schema etc. properly

                # im going to push this down to kafka client but testing for now
                if dedup_on_key is not None and row_counter == 0:
                    logger.info(
                        f"Checking for duplicates in topic {self._topic} on key [{dedup_on_key}] - record key is [{record[dedup_on_key]}] "
                    )

                row_counter += 1

                if dedup_on_key is not None:
                    # using a single item here: we can improve this in future e,g, in a transaction block
                    # check batch, write to kafka, commit batch
                    # for the moment if topic writing fails we are screwed but our coercing is pretty good at writing records.
                    # the other thing to keep in mind is we want to dedup in the stream itself so that we only try to write one in the first place
                    # that way there is new risk of writing the same thing twice within the transaction
                    # the thing is, the first time, the slow time, we expect them all to be new anyway so there is no major gain
                    if self.is_published(record, dedup_on_key):
                        skipped_duplicates += 1
                        continue

                if not use_kgateway:
                    # direct publish via kafka client

                    try:
                        # skipping the simple one inside this method and using the default until we figure out the encoding
                        self.produce(record)
                    except:
                        error_count += 1
                        message = f"Error posting payload to topic {self._topic} -  {traceback.format_exc()}"
                        logger.warn(message)
                        # raise Exception()
                else:
                    # kgateway publish
                    res.utils.logger.debug(f"Posting to kgateway: {self._submit_url}")

                    # remove some nulls and trust the default?
                    # im doing this because kgateway is not allowing a null list where the schema defines a type
                    record = {k: v for k, v in record.items() if v is not None}

                    response = safe_http.request_post(
                        self._submit_url, json=self._make_record(record)
                    )

                    if response.status_code != 200:
                        logger.warn(
                            f"Failed to write message: {response.reason}: {response.text} {record if show_failed else ''}"
                        )
                        error_count += 1
                if publish_to_replica:
                    # TODO - we could make this async - fire and forget?
                    logger.debug(
                        f"Sending to replica gateway {self._replica_url} via fire-and-forget post"
                    )
                    safe_http.request_post_fire_and_forget(
                        self._replica_url, json=self._make_record(record)
                    )

            if len(records) > 0:
                # start down this path but have some reservations
                # successful_count = len(records) - error_count
                # create a metric writer that writes to data flows for kafka topic res-infra.kafka-connector.messages.<topic>.<status>, counter
                # metric_writer(f"ok", successful_count)
                # metric_writer(f"failed", error_count)
                logger.info(
                    f"{self._topic}: {len(records)} records to write; there were {error_count} that failed. Skipped {skipped_duplicates} that were already published."
                )
                return records
            return []

    # TODO make this a generator and create consume_dataframe as a wrapper
    def consume(
        self,
        give_up_after_records=100,
        give_up_after_polls=2,
        give_up_after_time=None,
        timeout_poll_avro=30.0,
        filter=None,
        errors="raise",
    ):
        """
        batch consumer workflow with chunking semantics -if you want to stream just iterate over the connector e.g.
        foreach message in connector
        """
        logger.info(f"Consuming batch into dataframe from the topic {self._topic}")
        items = []
        counter = 0
        if give_up_after_polls is None:
            give_up_after_polls = give_up_after_records
        # TODO check a useful stubbing approach e.g. most recent packets
        # if str(os.environ.get("STUB_KAFKA_CONSUMER", False)).lower() == "true":
        #     return self.get_samples(limit=give_up_after_records)
        # start a clock
        poll_counter = 0
        while True:
            poll_counter += 1
            raw = None
            try:
                raw = self.consumer.poll_avro(timeout=timeout_poll_avro)
            except Exception as ex:
                # dont count the ones that fail
                poll_counter -= 1

                if errors == "raise":
                    raise
                else:
                    logger.warn(
                        f"Failed to parse {repr(ex)} ignoring if errors != raise"
                    )
                    # parse errors are different to timeouts so we shoudl not break due to poll counts

                    continue
            if raw is None:
                # stop trying so hard unless we expect lots of parse errors
                if poll_counter > give_up_after_polls:
                    break
                continue

            # we reset this because we want polls after success
            poll_counter = 0

            if filter and not filter(raw):
                # logical filter passed in by caller
                continue

            items.append(raw)
            counter += 1
            if give_up_after_records <= counter:
                break
            # TODO check the clock

        return pd.DataFrame(items)

    def __iter__(self):
        logger.debug(f"consuming topic {self._topic}")
        errors = 0
        while True:
            raw = None
            try:
                raw = self.consumer.poll_avro(timeout=2)
            except Exception as ex:
                errors += 1
                if errors >= self._max_errors:
                    if self._on_max_errors == "raise":
                        raise Exception(
                            f"Reached max parsing errors - terminating consumer"
                        ) from ex
                    else:
                        logger.warn(
                            f"Reached or exceed max parse errors on topic {self._topic} - {repr(ex)}"
                        )
            if raw is None:
                continue
            yield raw


class KafkaConnector(object):
    """
    A wrapper for indexing into topics and consuming and publushing dataframes to/from kafka
    the group/process can be passed in but is expected to be from the RES_APP_NAME
    to load the connector for a different group e.g. to test
    res.connectors.load('kafka', group_id='test-group')
    """

    def __init__(
        self,
        group_id=None,
        max_errors=-1,
        on_max_errors="warn",
        topic_config=None,
        consumer_config={},
    ) -> None:
        super().__init__()
        self._group_id = group_id
        self._max_errors = max_errors
        self._on_max_errors = on_max_errors
        self._topic_config = topic_config
        self._consumer_config = consumer_config

    def __getitem__(self, topic):
        # construct the wrapped object for this topic with behaviors
        return _DataFramePubSub(
            topic=topic,
            process_name=self._group_id,
            max_errors=self._max_errors,
            on_max_errors=self._on_max_errors,
            topic_config=self._topic_config,
            consumer_config=self._consumer_config,
        )

    @staticmethod
    def make_avro_schema_from_res_schema(schema, plan=False, **kwargs):
        key = schema.get("key")
        namespace = "res-data" if "." in key else key.split("")[0]
        field_python_types = schema["fields"]
        nullable_fields = [
            f.get("name", f.get("key"))
            for f in field_python_types
            if not f.get("is_required", False)
        ]
        field_python_types = {
            f.get("name", f.get("key")): f.get("type", "str")
            for f in field_python_types
        }
        avro_schema = KafkaConnector.make_avro_schema(
            key, namespace, field_python_types, nullable_fields=nullable_fields
        )
        if plan:
            return avro_schema

        return _DataFramePubSub(topic=schema.get("key"))._create_or_update_res_schema(
            avro_schema, **kwargs
        )

    @staticmethod
    def make_avro_schema(name, namespace, field_python_types, **kwargs):
        """
        in the kwargs we can add key specific properties such as required fields
        """
        nullable_fields = kwargs.get("nullable_fields")
        doc = kwargs.get("doc")

        def ensure_valid_names(s):
            # placeholder - not sure what the rules are
            return s.replace("-", "_")

        def _make_type(k, t, default="string"):
            t = _DataFramePubSub.PYTHON_TYPE_MAP.get(t, default)
            if nullable_fields and k in nullable_fields:
                t = [t, "null"]
            return t

        fields = [
            {"name": k, "type": _make_type(k, v)} for k, v in field_python_types.items()
        ]

        schema = {
            "type": "record",
            "name": ensure_valid_names(name),
            "namespace": ensure_valid_names(name.split(".")[0]),
            "doc": doc or f"{name} type created by res-schema",
            "fields": fields,
        }

        return schema

    @staticmethod
    def update_queue(schema, plan=False, **kwargs):
        """
        upsert the schema for this topic - we trust the data type so just do it
        """
        import res

        if isinstance(schema, str):
            schema = json.loads(schema)

        # the version is wrong for confluent to support enums so removing
        for f in schema["fields"]:
            if isinstance(f["type"], dict) and f["type"]["type"] == "enum":
                res.utils.logger.info(f'removing enum type on {f["name"]}')
                f["type"] = ["string", "null"]

            # for complex enum array types - again we dont support enums
            # this is horrendous but just experimenting
            if isinstance(f["type"], dict) and f["type"]["type"] == "array":
                if isinstance(f["type"]["items"], dict):
                    if f["type"]["items"]["type"] == "enum":
                        res.utils.logger.info(f'removing enum type on {f["name"]}')
                        # setting the array type to be just an array of strings
                        f["type"]["items"] = ["string", "null"]

            # not sure how i want to handle default values in kafka
            # this requires some care for null handling etc. avro does not go 1:1 with kafka
            if "default" in f:
                res.utils.logger.warn(
                    f"Removing defaults from kafka schema for now - its enough to make nullable"
                )
                f.pop("default")

        # TODO - do we want to override datetime types

        fqname = f"{schema['namespace']}.{schema['name']}".replace("-", "_")
        kafka = res.connectors.load("kafka")

        # dont support aliases for now
        if "aliases" in schema:
            schema.pop("aliases")

        res.utils.logger.info(f"updating topic/queue {fqname}")
        K = kafka[fqname]
        if plan:
            res.utils.logger.info("Would create {fqname} with the returned schema")
            return schema
        return K._create_or_update_res_schema(schema)
