# from typing_extensions import ParamSpecArgs
import boto3
import requests
from res.utils.secrets import secrets_client
from res.utils.logging import ResLogger
from res.utils.configuration.ResConfigClient import ResConfigClient
from res.utils.secrets.ResSecretsClient import ResSecretsClient
import json
import os
from datetime import datetime
import time

os.getenv("RES_ENV", "development")
logger = ResLogger()
config_client = ResConfigClient()
secrets_client = ResSecretsClient()

old_config = None


def convert_ts_to_epoch(time_ts):
    p = "%Y-%m-%dT%H:%M:%S.%fZ"
    mytime = time_ts
    epoch = datetime(1970, 1, 1)
    time_epoch = (datetime.strptime(mytime, p) - epoch).total_seconds()
    return time_epoch


# def set_cameras_rtsp_info_from_secrets():
#     client = boto3.client(service_name="secretsmanager")
#     new_secret = json.dumps(data1)
#     try:
#         client.update_secret(SecretId="NEST_GOOGLE_RTSP_INFO", SecretString=new_secret)
#     except Exception as e:
#         logger.error(f"Failed to update secret on Secrets Manager {e}")
#         raise


def get_active_cameras_rtsp_info_from_secrets():  # for now. this can come from anywhere in the future
    logger.info("Getting Cameras List")
    try:
        camera_info = secrets_client.get_secret("NEST_GOOGLE_RTSP_INFO")
    except Exception as e:
        logger.error(f"Failed to get secret on Secrets Manager {e}")
        raise

    logger.info("Done getting cameras info")
    return camera_info


def extend_rtsp_stream(camera_name, extension_token):

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(
            secrets_client.get_secret("NEST_GOOGLE_ACCESS_TOKEN")[
                "nest_google_access_token"
            ]
        ),
    }
    # data = '{\n  "command" : "sdm.devices.commands.CameraLiveStream.ExtendRtspStream",\n  "params" : {\n    "streamExtensionToken" : "{}".format()\n  }\n}'
    data = {
        "command": "sdm.devices.commands.CameraLiveStream.ExtendRtspStream",
        "params": {"streamExtensionToken": "{}".format(extension_token)},
    }
    response = requests.post(
        f"https://smartdevicemanagement.googleapis.com/v1/{camera_name}:executeCommand",
        headers=headers,
        json=data,
    )
    if response.status_code == 200:
        logger.info("ALL OK")

        set_camera_configs(response.json()["results"])

    else:
        logger.error("Error Extending the RSTP URL")


def get_camera_configs(camera_type, camera_name):
    global old_config
    logger.info("Getting Camera Configs")
    configs = config_client.get_config_value(f"{camera_type}_{camera_name}")
    logger.info("Done Getting Camera configs")
    old_config = configs
    return configs


def set_camera_configs(new_camera_configs):
    logger.info("Updating Camera Configs")
    global old_config
    new_config = old_config

    try:
        url = new_camera_configs["streamUrls"]["rtspUrl"]
    except:
        url = old_config["rtspUrl"]

    url = url.split("auth=")[0]
    new_url = url + "auth=" + new_camera_configs["streamToken"]
    print(f"\nNEW URL\n\n{new_url}")

    new_config["rtspUrl"] = new_url
    new_config["streamExtensionToken"] = new_camera_configs["streamExtensionToken"]
    new_config["streamToken"] = new_camera_configs["streamToken"]
    new_config["expiresAt"] = new_camera_configs["expiresAt"]
    config_client.set_config_value(
        "{}_{}".format(
            new_config["type"],
            new_config["traits"]["sdm.devices.traits.Info"]["customName"],
        ),
        new_config,
    )
    logger.info("Done Setting Camera configs")


def rtsp_needs_extension(camera):
    logger.info("Checking if RTSP URL Need Extension")
    camera_configs = get_camera_configs(camera["type"], camera["customName"])
    result = (convert_ts_to_epoch(camera_configs["expiresAt"]) - time.time()) / 60.0
    if result < 1.5:
        logger.info("It's been more than 3.5 minutes")

        extend_rtsp_stream(
            camera_configs["name"], camera_configs["streamExtensionToken"]
        )
    else:
        logger.info("No need to extend")


def get_rtsp_url(camera):
    camera_configs = get_camera_configs(camera["type"], camera["customName"])
    camera_name = camera_configs["name"]

    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(
            secrets_client.get_secret("NEST_GOOGLE_ACCESS_TOKEN")[
                "nest_google_access_token"
            ]
        ),
    }
    data = {
        "command": "sdm.devices.commands.CameraLiveStream.GenerateRtspStream",
        "params": {},
    }

    response = requests.post(
        f"https://smartdevicemanagement.googleapis.com/v1/{camera_name}:executeCommand",
        headers=headers,
        json=data,
    )

    if response.status_code == 200:
        logger.info("ALL OK")
        print("\n\nRESPONSE\n\n")
        print(response.json()["results"])
        set_camera_configs(response.json()["results"])

    else:
        logger.error("Error Getting the RSTP URL")


def start():
    cameras = get_active_cameras_rtsp_info_from_secrets()
    for camera in cameras["cameras"]:
        get_rtsp_url(camera)

    while True:

        for camera in cameras["cameras"]:
            rtsp_needs_extension(camera)

            logger.info("this will sleep for 20secs")
            time.sleep(20)


if __name__ == "__main__":

    try:
        start()
    except Exception as e:
        logger.error(f"An unexpected error has ocurred: {e}")
