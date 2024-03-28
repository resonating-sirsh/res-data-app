"""Handle Kafka events to unpack vstitcher asset bundles. _nudge . . . """

import traceback
import res.flows.meta.body.unpack_asset_bundle.unpack_asset_bundle as unpack_asset_bundle
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.utils import logger

KAFKA_JOB_EVENT_TOPIC = "res_meta.body.unpack_asset_bundle"


def handle_event(event):
    """Process a body asset bundle creation event........."""
    logger.info(event)

    body_id = event.get("record_id", None)
    body_code = event.get("body_code", None)
    zipped_asset_bundle_uri = event.get("asset_bundle_uri", None)
    body_version = event.get("body_version", None)

    if body_id is None:
        raise Exception(f"body id not valid: {body_id}...")
    elif body_code is None:
        raise Exception(f"body code not valid: {body_code} ...")
    elif body_version is None or body_version == 0:
        raise Exception(f"body version not valid: {body_version}...")
    elif zipped_asset_bundle_uri is None:
        raise Exception(
            f"asset bundle uri not valid: {zipped_asset_bundle_uri}........."
        )
    else:
        logger.info("Process event..............................")
        unpack_asset_bundle.handler(event)


if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, KAFKA_JOB_EVENT_TOPIC)

    while True:
        try:
            # Get new messages
            raw_message = kafka_consumer.poll_avro(timeout=60)
            if raw_message is None:
                logger.info("kafka poll timed out, continuing..")
                continue
            logger.info(f"Found new message! {raw_message}")
            handle_event(raw_message)

        except Exception as e:
            print(f"{e}")
            err = "Error processing: {}".format(traceback.format_exc())
            logger.critical(
                err,
                exception=e,
            )
