from datetime import datetime
import json
import os
import atexit
from operator import methodcaller
from sys import exc_info
from traceback import format_tb
from typing import Callable, Optional, Tuple, Union
from typing_extensions import Literal, TypedDict
from confluent_kafka import Consumer, Message
from confluent_kafka.error import KafkaError
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryError,
    RegisteredSchema,
)
from confluent_kafka.serialization import (
    Deserializer,
    MessageField,
    SerializationContext,
    SerializationError,
)

from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from res.utils import logger
from .ConfluentSchemaRegistryAvro import AvroDeserializer

DeserializerFn = Callable[[str, SerializationContext], Optional[dict]]

SchemaType = Literal["AVRO", "JSON", "PLAIN"]


class DeserializerWrapped(TypedDict):
    deserializer: Union[Deserializer, DeserializerFn]
    t_sid: Tuple[SchemaType, int]


class ConsumerDeserializersDict(TypedDict):
    key: Optional[DeserializerWrapped]
    value: Optional[DeserializerWrapped]


# This is a Resonance Kafka Consumer, which wraps the confluent consumer
class ResKafkaConsumer:
    _deserializers: ConsumerDeserializersDict

    def __init__(self, kafka_client, topic, topic_config=None, consumer_config={}):
        self.kafka_client = kafka_client
        self.schema_registry_client = kafka_client._schema_registry_client
        self.topic = topic
        consumer_opts = {
            "bootstrap.servers": self.kafka_client._bootstrap_servers,
            "group.id": self.kafka_client._process_name,
            "default.topic.config": topic_config or {"auto.offset.reset": "smallest"},
            "security.protocol": "ssl",
            **(
                {"debug": "consumer"}
                if os.getenv("RES_ENV", "development") == "development"
                else {}
            ),
            **consumer_config,
        }
        # logger.debug(f"Initializing kafka consumer {self}", **consumer_opts)

        self.consumer = Consumer(consumer_opts)
        self.consumer.subscribe([topic])
        self.value_serialization_ctx = SerializationContext(
            self.topic, MessageField.VALUE
        )
        self._deserializers: ConsumerDeserializersDict = {"key": None, "value": None}
        # dont know what happened but the thing is not properly committing its offset and we need to
        # close it explicitly.
        atexit.register(self.consumer.close)

    def deserialize_message(self, msg):
        error, schema = self.kafka_client.get_schema(self.topic)
        if error != None:
            logger.error(f"Error retrieving schema! {error}")
        logger.debug(schema.schema.schema_str)
        try:
            avro_deserializer = AvroDeserializer(
                schema.schema.schema_str, self.kafka_client._schema_registry_client
            )
            return avro_deserializer(msg, self.value_serialization_ctx)
        except Exception as ex:
            logger.warn(f"Failed to deserialize message {repr(ex)}")

    def poll_avro(self, timeout=100.0):
        msg = self.consumer.poll(timeout)
        if msg is None:
            logger.debug("No new messages...")
            return None
        logger.debug("Found new message!")
        msg_value = msg.value()
        deserialized_message = self.deserialize_message(msg_value)
        logger.debug("New msg: {}".format(str(deserialized_message)))
        return deserialized_message

    def poll_json(self, timeout=100.0, include_key=False):
        msg = self.consumer.poll(timeout)
        if msg is None:
            logger.debug("No new messages...")
            return None
        logger.debug(msg.value())
        if include_key:
            key = None if not msg.key() else json.loads(msg.key())
            msg = None if not msg.value() else json.loads(msg.value())
            return {"key": key, "value": msg}
        return json.loads(msg.value())

    ## v2 APIs

    def _get_schema(self, field=MessageField.VALUE) -> RegisteredSchema:
        if field not in [MessageField.KEY, MessageField.VALUE]:
            logger.warn(
                f"Don't know how to get schema for field '{field}'! Using 'value'.",
                topic=self.topic,
                field=field,
            )
            field = MessageField.VALUE
        subject = f"{self.topic}-{field}"
        try:
            schema = self.schema_registry_client.get_latest_version(subject)
        except SchemaRegistryError as err:
            logger.error(
                f"Error retrieving schema! May not exist for topic {self.topic}: {err}",
                topic=self.topic,
                http_status_code=err.http_status_code,
                error_code=err.error_code,
                error_message=err.error_message,
            )
            schema = RegisteredSchema(
                schema_id=-1,
                schema=Schema("", "PLAIN", []),
                subject=subject,
                version=1,
            )
        logger.debug(schema.schema.schema_str)
        return schema

    def _get_deserializer(self, field=MessageField.VALUE) -> DeserializerWrapped:
        schema = self._get_schema(field)
        schema_type: SchemaType = schema.schema.schema_type
        schema_id = schema.schema_id
        des: Optional[DeserializerWrapped] = self._deserializers.get(field)
        if not des or des["t_sid"] != (schema_type, schema_id):
            t_sid = (schema_type, schema_id)
            if schema_type == "AVRO":
                deserializer = AvroDeserializer(
                    schema.schema.schema_str, self.schema_registry_client
                )
            elif schema_type == "JSON":
                deserializer = JSONDeserializer(schema.schema.schema_str)
            else:
                deserializer = lambda value, ctx: json.loads(value)

            des = DeserializerWrapped(t_sid=t_sid, deserializer=deserializer)
            self._deserializers[field] = des
        return des

    @staticmethod
    def error_to_dict(err: Optional[Union[KafkaError, Exception]], source: str):
        if err is None:
            return {}
        if isinstance(err, Exception):
            return {
                "exception": err,
                "error_code": getattr(err, "error_code", None) if err else None,
                "error_is_fatal": getattr(err, "fatal", None) if err else None,
                "error_is_retriable": getattr(err, "retriable", None) if err else None,
                "error_name": repr(type(err)) if err else None,
                "error_str": str(err) if err else None,
                "error_dict": vars(err) if err and hasattr(err, "__dict__") else {},
                "error_source": source,
            }
        return {
            "exception": err,
            "error_code": err.code() if err else None,
            "error_is_fatal": err.fatal() if err else None,
            "error_is_retriable": err.retriable() if err else None,
            "error_name": err.name() if err else None,
            "error_str": err.str() if err else None,
            "error_dict": vars(err) if err and hasattr(err, "__dict__") else {},
            "error_source": source,
        }

    def deserialize_field(self, msg, field=MessageField.VALUE):
        raw = methodcaller(field)(msg)
        if not raw:
            return raw, None, None
        ctx = SerializationContext(self.topic, field)
        try:
            des = self._get_deserializer(field)
        except SchemaRegistryError as err:
            exc_type, exc, tb = exc_info()
            return (
                raw,
                None,
                {
                    "error": {
                        "http_status_code": err.http_status_code,
                        "error_code": err.error_code,
                        "error_message": err.error_message,
                        "msg": str(err),
                        "tb": format_tb(tb),
                        "exc_type": repr(exc_type),
                    },
                    "ctx": ctx.__dict__,
                    "raw": raw,
                    "msg": msg,
                    "field": field,
                    "topic": self.topic,
                    **self.error_to_dict(
                        err, f"{field.upper()}:self._get_deserializer({field})"
                    ),
                },
            )
        deserializer = des["deserializer"]
        try:
            return raw, deserializer(raw, ctx), None
        except SerializationError as err:
            exc_type, exc, tb = exc_info()
            logger.debug(
                f"Deserialization failed for {field} {raw}",
                raw=raw,
                message=msg,
                field=field,
                topic=self.topic,
                error=err,
            )
            return (
                raw,
                None,
                {
                    "error": {
                        "msg": str(err),
                        "tb": format_tb(tb),
                        "exc_type": repr(exc_type),
                    },
                    "ctx": ctx.__dict__,
                    "raw": raw,
                    "msg": msg,
                    "field": field,
                    "topic": self.topic,
                    **self.error_to_dict(
                        err,
                        f"{field.upper()}:deserializer['{des['t_sid']}']({raw}, {ctx})",
                    ),
                },
            )
        except json.JSONDecodeError as err:
            exc_type, exc, tb = exc_info()
            logger.debug(
                f"JSON decode failed for {field} {raw}",
                raw=raw,
                message=msg,
                field=field,
                topic=self.topic,
                error=err,
            )
            return (
                raw,
                None,
                {
                    "error": {
                        "msg": err.msg,
                        "doc": err.doc,
                        "pos": err.pos,
                        "lineno": err.lineno,
                        "colno": err.colno,
                        "tb": format_tb(tb),
                        "exc_type": repr(exc_type),
                    },
                    "exc_name": repr(exc_type),
                    "error_str": str(err),
                    "ctx": ctx.__dict__,
                    "raw": raw,
                    "msg": msg,
                    "field": field,
                    "topic": self.topic,
                    **self.error_to_dict(
                        err,
                        f"{field.upper()}:deserializer['{des['t_sid']}']({raw}, {ctx})",
                    ),
                },
            )
        except Exception as err:
            logger.error(
                f"Unknown exception occured while trying to deserialize message! {err}",
                raw=raw,
                message=msg,
                field=field,
                topic=self.topic,
                exception=err,
                errors=[repr(err)],
            )
            return (
                raw,
                None,
                self.error_to_dict(
                    err, f"{field.upper()}:deserializer['{des['t_sid']}']({raw}, {ctx})"
                ),
            )

    def read_message_timestamp(self, msg):
        timestamp_code, timestamp = msg.timestamp()
        if 1 <= timestamp_code <= 2:
            timestamp_type = "CREATE_TIME" if timestamp_code < 2 else "LOG_APPEND_TIME"
            timestamp = datetime.utcfromtimestamp(timestamp / 1000)
        else:
            timestamp = datetime.utcnow().timestamp()
            timestamp_type = "CONSUMER_READ_TIME"
        return timestamp_code, timestamp, timestamp_type

    def extract_msg_metadata(self, msg: Message):
        timestamp_code, timestamp, timestamp_type = self.read_message_timestamp(msg)
        return {
            "latency": msg.latency() if hasattr(msg, "latency") else 0,
            "len": msg.len() if hasattr(msg, "len") else len(msg.value() or ""),
            "timestamp": timestamp,
            "timestamp_code": timestamp_code,
            "timestamp_type": timestamp_type,
            "headers": msg.headers() or [],
        }

    def message_to_dict(self, msg: Message):
        """Convert a Kafka Message to a dict."""
        raw_key, key, key_error = self.deserialize_field(msg, "key")
        raw_value, msg_value, value_error = self.deserialize_field(msg, "value")
        try:
            meta = self.extract_msg_metadata(msg)
            meta_error = None
        except Exception as err:
            meta = {}
            meta_error = self.error_to_dict(err, "extract_msg_metadata")
        return {
            **(
                meta_error
                or self.error_to_dict(msg.error(), "kafka_message.error()")
                or value_error
                or key_error
                or {}
            ),
            **meta,
            "raw_key": raw_key,
            "key": key,
            "key_error": key_error,
            "raw_value": raw_value,
            "value": msg_value,
            "value_error": value_error,
            "topic": msg.topic(),
            "offset": msg.offset(),
            "partition": msg.partition(),
        }

    def poll_verbose(self, timeout=100.0, include_errors=False, level="warn"):
        msg = self.consumer.poll(timeout)
        if msg is None:
            logger.debug("No new messages...")
            return None
        msg_dict = self.message_to_dict(msg)
        if msg_dict.get("exception", None) is not None:
            getattr(logger, level)(
                f"Encountered Exception in '{self.topic}' consumer!",
                **msg_dict,
            )
            if not include_errors:
                return None
        return msg_dict
