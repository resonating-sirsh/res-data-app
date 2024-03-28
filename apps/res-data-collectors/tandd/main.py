# IMPORTS
from airtable import Airtable
import requests
import boto3
import json
import time
from datetime import datetime
import boto3
import base64
from botocore.exceptions import ClientError
from res.utils.logging.ResLogger import ResLogger
from res.utils.configuration.ResConfigClient import ResConfigClient
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import secrets_client

logger = ResLogger()
kafka_client = ResKafkaClient()
config_client = ResConfigClient()
KAFKA_TOPIC = "res_data_collectors.tandd.sensor_data"
kafka_producer = ResKafkaProducer(kafka_client, KAFKA_TOPIC)


def get_active_sensors_list():
    AIRTABLE_API_KEY = secrets_client.get_secret("AIRTABLE_API_KEY")
    # AIRTABLE_API_KEY= {
    #         "development": "####",
    #         "production": "####",
    #     }
    sensors = []
    res_infrastructure_sensors_table = Airtable(
        "appoftTcaySVg5DXz",
        AIRTABLE_API_KEY,
    )

    for sensor in res_infrastructure_sensors_table.iterate(
        "tblxlzYVYNlWkzNF0",
        filter_by_formula="AND({Enabled_is_true} = 1, {Manufacturer} = 'TANDD')",
    ):

        sensors.append(sensor["fields"]["Serial"])
    logger.info(f"Active Sensors: {sensors}")
    return sensors


def get_sensor_readings(sensor_serial, latest_timestamp):
    logger.info("Getting Credentials from Secrets Manager...")
    TANDD_CREDENTIALS = secrets_client.get_secret("TANDD_WEB_STORAGE_CREDENTIALS")
    # TANDD_CREDENTIALS = {
    #         "api-key": "####",
    #         "login-id": "####",
    #         "login-pass": "####",
    #     }
    url = "https://api.webstorage-service.com/v1/devices/data"

    params = {
        **TANDD_CREDENTIALS,
        "remote-serial": sensor_serial,
        "unixtime-from": int(latest_timestamp)
        + 1,  # Adding 1 so that we don't get the same reading all the time,
    }

    headers = {
        "Host": "api.webstorage-service.com:443",
        "Content-Type": "application/json",
        "X-HTTP-Method-Override": "GET",
    }
    logger.info("Requesting for {} since {}".format(sensor_serial, latest_timestamp))
    response = requests.post(url, headers=headers, json=params)
    return response.json()


def push_event_to_kafka(sensor_data):
    event_data = {
        "serial": sensor_data["serial"],
        "model": sensor_data["model"],
        "name": sensor_data["name"],
        "time_diff": sensor_data["time_diff"],
        "std_bias": sensor_data["std_bias"],
        "dst_bias": sensor_data["dst_bias"],
        "channels": sensor_data["channel"],
        "reading": None,
    }
    if sensor_data["time_diff"] != "-240":
        logger.error(
            "The Timezone for sensor {} is set to {} and not -240.... Please Fix Logger/Sensor Settings".format(
                sensor_data["serial"], sensor_data["time_diff"]
            )
        )

    # n = 1
    for reading in sensor_data["data"]:

        event_data["reading"] = json.loads(json.dumps(reading).replace("-", "_"))

        kafka_producer.produce(event_data)

    logger.info("Done sending {} events...".format(len(sensor_data["data"])))


def update_latest_reading_timestamp(sensor_serial, latest_timestamp):
    logger.info(
        "Now updating LATEST_READING_TIMESTAMP_{} to {}".format(
            sensor_serial, str(latest_timestamp)
        )
    )
    config_client.set_config_value(
        f"LATEST_READING_TIMESTAMP_{sensor_serial}", str(latest_timestamp)
    )

    new_ts = config_client.get_config_value(f"LATEST_READING_TIMESTAMP_{sensor_serial}")

    logger.info(
        "LATEST_READING_TIMESTAMP_{} updated to {}".format(sensor_serial, new_ts)
    )


def start():
    while True:
        # 1 GET ACTIVE T&D SENSORS FROM DATABASE(AIRTABLE).
        active_sensors_list = get_active_sensors_list()
        logger.info("Requesting Readings from T&D for the following sensors:")
        logger.info("{}".format(active_sensors_list))
        logger.info("-------------------------------------------------")
        # FOR EACH ACTIVE SENSOR GET LATEST READING TIMESTAMP

        # FOR EACH ACTIVE SENSOR GET LATEST READING TIMESTAMP
        # 2,3 FOR EACH SENSOR: COLLECT THE DATA, AND PUSH TO KAFKA
        for active_sensor in active_sensors_list:
            logger.info("Now working with sensor {}".format(active_sensor))
            logger.info("Retrieving Latest Reading Timestamp from res.Config")

            latest_reading_timestamp = config_client.get_config_value(
                f"LATEST_READING_TIMESTAMP_{active_sensor}"
            )

            if latest_reading_timestamp == None:
                logger.info(f"No timestamp Saved for sensor {active_sensor}...")
                logger.info(f"Setting Timestamp value to 0...")
                config_client.set_config_value(
                    f"LATEST_READING_TIMESTAMP_{active_sensor}", "0"
                )
                latest_reading_timestamp = config_client.get_config_value(
                    f"LATEST_READING_TIMESTAMP_{active_sensor}"
                )
                logger.info(f"Value set to {latest_reading_timestamp}")

            else:

                logger.info(
                    "Timestamp previously saved for {} is {}".format(
                        active_sensor, latest_reading_timestamp
                    )
                )

            sensor_readings = get_sensor_readings(
                active_sensor, latest_reading_timestamp
            )
            # 3 PUSH TO KAFKA.
            if len(sensor_readings["data"]) > 0:
                logger.info(
                    "{} events will be sent to kakfa for sensor {}".format(
                        len(sensor_readings["data"]), active_sensor
                    )
                )
                push_event_to_kafka(sensor_readings)
                new_latest_reading = sensor_readings["data"][-1]["unixtime"]

                # 4 SAVE LATEST READING TIMESTAMPS (FOR EACH SENSOR)
                update_latest_reading_timestamp(active_sensor, new_latest_reading)
            elif (
                len(sensor_readings["data"]) == 0
                and ((time.time() - int(latest_reading_timestamp)) / 3600) >= 2
                and datetime.today().isoweekday() > 5
            ):
                logger.warning(
                    f"The sensor {active_sensor} has not reported readings for more than 2 hours... Check its Power"
                )
        logger.info("Process will now sleep for 1 minute(s)...")
        time.sleep(60 * 1)


if __name__ == "__main__":
    while True:
        try:
            start()
        except Exception as e:
            logger.error(f"An Unknow error has ocurred {e}")
