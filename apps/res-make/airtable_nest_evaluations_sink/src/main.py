import json, os
from functools import lru_cache

from pyairtable import Table
from tenacity import retry, wait_fixed, stop_after_attempt

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger, secrets


NESTS_FIELDS = ["Argo Jobkey"]


def initialize_secrets():
    if "AIRTABLE_API_KEY" not in os.environ:
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY")


@lru_cache(maxsize=1)
def cnx_nests():
    return Table(
        os.environ.get("AIRTABLE_API_KEY"),
        os.environ.get("PRINT_BASE_ID"),
        os.environ.get("NESTS_TABLE"),
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    response = getattr(cnx, method)(*args, **kwargs)
    logger.info(json.dumps(response))
    return response


def nests_by_jobkey(msg):
    filter_formula = f"""AND({{Argo Jobkey}}='{msg["job_key"]}')"""
    return call_airtable(
        cnx_nests(),
        formula=filter_formula,
        fields=NESTS_FIELDS,
    )


def create_nests_payload(msg, nests):
    for n in nests:
        yield {
            "id": n["id"],
            "fields": {
                "Nesting Utilization Score": msg["area_nest_utilized_pct"],
                "Padded Pieces Area in Yards": msg["area_pieces_yds"],
            },
        }


def handle_message(msg):
    # Create the Airtable data
    nests = nests_by_jobkey(msg)
    if not nests:
        logger.warn("No nest found with that Argo Jobkey")
        return

    nests_payload = list(create_nests_payload(msg, nests))

    logger.info(f"Recording nesting utilization for {len(nests)} nests.")
    logger.debug(nests)
    logger.debug(nests_payload)

    if os.environ.get("WRITE_TO_AIRTABLE", "f").lower()[0] == "t":
        call_airtable(cnx_nests(), nests_payload, typecast=True, method="batch_update")
    else:
        logger.info("Skipped writing to Airtable")


def run_kafka():
    initialize_secrets()

    # Initialize kafka client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, os.environ.get("KAFKA_TOPIC"))

    while True:
        try:
            msg = kafka_consumer.poll_avro()
            if msg is None:
                # Sometimes the message is None the first try, will show up eventually
                logger.debug("...")
                continue

            logger.debug("New record from kafka: {}".format(msg))

            handle_message(msg)
        except Exception as e:
            logger.error(str(e))
            # raise e


if __name__ == "__main__":
    run_kafka()
