import res
from res.flows.kafka import kafka_consumer_worker
from res.utils import logger
from res.flows.make.rfid_event_processing.rfid_processor import handle_event


RESPONSE_TOPIC = "res_premises.rfid_telemetry.submit_rfid_event_response"


@kafka_consumer_worker("res_premises.rfid_telemetry.submit_rfid_event_request")
def start(event: dict) -> dict:
    logger.info(f"We've received a message! {event}")
    response = handle_event(event)

    logger.info(f"Response: {response}")

    kafka = res.connectors.load("kafka")
    kafka[RESPONSE_TOPIC].publish(response.dict(), use_kgateway=True)


if __name__ == "__main__":
    event = {
        "id": "434eac6f-0502-b8ff-abe5-e93b2e837f1d",
        "channel": "bla",
        "tag_epc": "bla",
        "observed_at": "2023-08-29T16:53:58.344945+00:00",
    }

    start()
