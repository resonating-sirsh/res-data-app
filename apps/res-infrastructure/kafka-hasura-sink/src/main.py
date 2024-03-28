import os
from pathlib import Path
from sys import exc_info
from time import sleep
from typing import Optional

from tenacity import retry
from tenacity.wait import wait_chain, wait_fixed, wait_random_exponential
from tenacity.stop import stop_after_attempt

from schemas.pydantic.kafka_dump_models import (
    KafkaMessageDumpInput,
    VerboseKafkaMessage,
)
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.graphql.hasura import Client as HasuraClient
from res.utils import env, logger, secrets

DUMP_KAFKA_MESSAGE = (Path(__file__).parent / "dump_kafka_message.gql").read_text()

INTERMESSAGE_DELAY = float(os.getenv("INTERMESSAGE_DELAY", "0.0"))  # 30ms


def initialize_secrets():
    if "HASURA_API_SECRET_KEY" not in os.environ:
        secrets.secrets_client.get_secret("HASURA_API_SECRET_KEY")


def get_config(topic: Optional[str] = None, include_errors: Optional[bool] = None):
    topic = topic or os.environ.get("KAFKA_TOPIC")
    if not topic:
        raise ValueError("Topic not provided!")
    include_errors = (
        include_errors
        if include_errors is not None
        else env.boolean_envvar("INCLUDE_ERRORS", "f")
    )
    return topic, include_errors


def init_kafka_consumer(topic):
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, topic)
    return kafka_consumer


def init_hasura_client(api_url=None, api_key=None):
    hasura_params = dict(
        api_url=api_url or os.getenv("HASURA_ENDPOINT"),
        api_key=api_key or os.getenv("HASURA_API_SECRET_KEY"),
    )
    return HasuraClient(**hasura_params)


@retry(
    wait=wait_chain(
        wait_fixed(INTERMESSAGE_DELAY), wait_random_exponential(multiplier=0.1, max=20)
    ),
    stop=stop_after_attempt(10),
)
def dump_message_to_hasura(input_params, hasura_client=None):
    hasura_client = hasura_client or init_hasura_client()
    return hasura_client.execute(
        DUMP_KAFKA_MESSAGE, {"object": input_params.serializable_dict()}
    )


def parse_new_message(msg):
    parsed_message = VerboseKafkaMessage(**msg)
    input_params = KafkaMessageDumpInput(**parsed_message.dict())
    logger.debug(
        f"Dumping parsed message to kafka: {parsed_message!r}",
        input_params=input_params,
    )

    return parsed_message, input_params


def run_kafka():
    initialize_secrets()

    topic, include_errors = get_config()
    # Initialize kafka client, consumer, and hasura client.
    kafka_consumer = init_kafka_consumer(topic)
    hasura_client = init_hasura_client()

    while True:
        msg = None
        parsed_message = None
        input_params = None
        try:
            msg = kafka_consumer.poll_verbose(include_errors=include_errors)
            if msg is not None:
                logger.debug(f"New record from kafka! {msg!r}")
                parsed_message, input_params = parse_new_message(msg)
                dump_message_to_hasura(input_params, hasura_client)
        except Exception:
            logger.error(
                "Got error while trying to dump message to hasura!",
                exception=exc_info(),
                message=repr(msg),
                parsed_message=repr(parsed_message),
                input_params=repr(input_params),
            )
        if INTERMESSAGE_DELAY:
            sleep(INTERMESSAGE_DELAY)


if __name__ == "__main__":
    run_kafka()
