import res
from datetime import datetime, timedelta

from res.flows.kafka import kafka_consumer_worker
from res.utils import logger

from res.flows.make.rfid_event_processing.rfid_processor import handle_roll_rfid_event


RESPONSE_TOPIC = "res_premises.rfid_telemetry.submit_rfid_event_response"


@kafka_consumer_worker("res_premises.rfid_telemetry.submit_rfid_event_request")
def start(event: dict) -> dict:
    logger.info(f"We've received a message! {event}")
    if (
        event["reader"] == "Print RFID Reader"
        or event["reader"] == "Processing RFID Reader"
    ):
        response = handle_roll_rfid_event(event)

    if response:
        logger.info(f"Response: {response}")

        kafka = res.connectors.load("kafka")
        kafka[RESPONSE_TOPIC].publish(response.dict(), use_kgateway=True)

    else:
        logger.info(
            f"Tag: {event['tag_epc']} seen at: {event['reader']}, channel: {event['channel']}. But no response."
        )


if __name__ == "__main__":
    event = {
        "id": "d86a11aa-b16c-aef9-de53-2140c6ffedc4",
        "reader": "Print RFID Reader",
        "channel": "11",
        "tag_epc": "300F4F573AD003C251D7816E",
        "observed_at": "2024-01-05T14:38:21.658321+00:00",
        "metadata": {},  #
    }

    start()
