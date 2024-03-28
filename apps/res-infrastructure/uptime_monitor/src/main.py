import os
import json
import time
import datetime

import requests

from res.utils import logger

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer

# This long running process rotates thru the list of endpoints/payloads and calls each.
# If one doesn't respond, raise an alarm

SLEEP_SECONDS = 30
retries = {}
MAX_RETRIES = 3
ENDPOINT_FILE = "src/endpoints.json"
KAFKA_TOPIC = "res_infrastructure.uptime_monitor.endpoint_response"


def get_response_status(response_text):
    try:
        response_dict = json.loads(response_text)
        status = response_dict.get("status", {}).get("indicator", "critical")
        return status
    except Exception as e:
        logger.critical(
            "Uptime Monitor could not parse response response! {}".format(str(e))
        )


def check_uptime(endpoint):
    try:
        if "method" in endpoint and endpoint["method"] == "POST":
            response = requests.post(endpoint["site"], json=endpoint["payload"])
        else:
            response = requests.get(endpoint["site"])
    except Exception as e:
        logger.error(
            "Uptime monitor had an error calling site: {}, error: {}".format(
                endpoint["site"], str(e)
            )
        )
        return None

    is_up = False
    if response.status_code < 200 or response.status_code >= 300:
        if retries.get(endpoint["site"], 0) >= MAX_RETRIES:
            logger.error(
                "Uptime monitor received error code from site: {}, code: {}, response: {}".format(
                    endpoint["site"], response.status_code, response.text
                )
            )
            retries[endpoint["site"]] = 0
        else:
            retries[endpoint["site"]] = retries.get(endpoint["site"], 0) + 1
    else:
        if endpoint.get("check_response", False):
            status_indicator = get_response_status(response.text)
            if status_indicator != "none":
                logger.critical(
                    "Uptime monitor site {} returned a non-none status: {}! This site may be down.".format(
                        endpoint["site"], status_indicator
                    )
                )
            else:
                is_up = True
                logger.info(
                    "Uptime monitor received a good status from site: {}".format(
                        endpoint["site"]
                    )
                )
        else:
            is_up = True
            logger.info(
                "Uptime monitor successfully pinged site: {}".format(endpoint["site"])
            )
    return {
        "timestamp": str(datetime.datetime.now()),
        "endpoint": endpoint["site"],
        "endpoint_name": endpoint["name"],
        "status_code": str(response.status_code),
        "response": str(response.text),
        "is_up": is_up,
    }


def run_monitor():
    kafka_client = ResKafkaClient()
    kafka_producer = ResKafkaProducer(kafka_client, KAFKA_TOPIC)
    env = os.getenv("RES_ENV", "development")
    endpoints = json.load(open(ENDPOINT_FILE))
    while True:
        for endpoint in endpoints:
            if env in endpoint["environments"]:
                uptime_result = check_uptime(endpoint)
                if uptime_result:
                    logger.info(uptime_result)
                    kafka_producer.produce(uptime_result)
        time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    run_monitor()
