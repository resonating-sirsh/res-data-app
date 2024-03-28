from posix import environ
from airtable import Airtable
import os
import requests
import boto3
import json
import time
import boto3
import base64
from botocore.exceptions import ClientError
from requests.models import Response
from res.utils.logging.ResLogger import ResLogger
from res.utils.configuration.ResConfigClient import ResConfigClient
from res.utils import secrets_client
from requests.structures import CaseInsensitiveDict

# Refreshes Tokens
config_client = ResConfigClient()
logger = ResLogger()

access_token_secret_name = "NEST_GOOGLE_ACCESS_TOKEN"
refresh_token_secret_name = "NEST_GOOGLE_REFRESH_TOKEN"


def get_access_token(secret_name_f):
    logger.info(f"Checking if {access_token_secret_name} exists in Secrets Manager...")
    try:
        secret = secrets_client.get_secret(access_token_secret_name)
        logger.info(f"Secret {access_token_secret_name} does exist")
        return secret
    except:
        logger.error(
            f"Secret {access_token_secret_name} Does Not Exist... Check Secrets Manager"
        )


def token_needs_refresh(token):
    url = f"https://www.googleapis.com/oauth2/v1/tokeninfo?access_token={token}"
    response = requests.get(url)

    if "error" in response.json():
        logger.error("The Access Token is Invalid or Expired...")
        return True
    elif int(response.json()["expires_in"]) > 600:
        logger.info(
            "Token lifespan is {} minutes.... \n Skipping Refresh".format(
                int(int(response.json()["expires_in"]) / 60)
            )
        )
        return False
    else:
        logger.info(
            "Access token lifespan is less than 10 minutes.... \n Needs Refresh"
        )
        return True


def refresh_access_token():
    logger.info("Trying to refresh Access Token")
    secrets = secrets_client.get_secret("NEST_GOOGLE_API_SECRETS")
    client_id = secrets["oauth2-client-id"]
    client_secret = secrets["oauth2-client-secret"]
    refresh_token = secrets_client.get_secret("NEST_GOOGLE_REFRESH_TOKEN")[
        "NEST_GOOGLE_REFRESH_TOKEN"
    ]

    url = f"https://www.googleapis.com/oauth2/v4/token?client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}&grant_type=refresh_token"

    headers = CaseInsensitiveDict()
    headers["Content-Length"] = "0"
    logger.info("Trying request")
    response = requests.post(url, headers=headers)
    if response.status_code == 200:
        update_secret_access_token(response.json()["access_token"])
        return True
    else:
        logger.error("Error while refreshing access token")
        return False


def update_secret_access_token(token):
    logger.info("Updating Secret on Secrets Manager")
    client = boto3.client(service_name="secretsmanager")
    new_secret = json.dumps({"nest_google_access_token": token})
    try:
        client.update_secret(
            SecretId="NEST_GOOGLE_ACCESS_TOKEN", SecretString=new_secret
        )
    except Exception as e:
        logger.error(f"Failed to update secret on Secrets Manager {e}")
        raise


def start():
    while True:
        access_token = get_access_token(access_token_secret_name)[
            "nest_google_access_token"
        ]

        if token_needs_refresh(access_token):
            if refresh_access_token():
                logger.info("Refreshed Token")

        else:
            logger.info("Access Token is Good. No need to Update.")

        logger.info("Sleeping for 5 minutes...")
        time.sleep(300)


if __name__ == "__main__":
    try:
        start()
    except Exception as e:
        logger.error(f"An unexpected error has ocurred {e}")
        pass
