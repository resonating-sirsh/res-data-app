# rotate endpoints on print table etc
# for some sample changes, create an info object on rolls
# test some kafka records
import base64
import json
import os
from boto3.session import Session
from . import logging

RES_ENV = os.environ.get("RES_ENV")

# env functions allow runtime override
GET_RES_APP = lambda: os.getenv("RES_APP_NAME")
GET_RES_NODE = lambda: os.getenv("RES_NODE", "res-flow")
GET_RES_NAMESPACE = lambda: os.getenv("RES_NAMESPACE")

# default e.g. local
RES_DATA_BUCKET = "res-data-platform"
if RES_ENV == "development":
    RES_DATA_BUCKET = "res-data-development"
if RES_ENV == "production":
    RES_DATA_BUCKET = "res-data-production"

ARGO_WORKFLOW_LOGS = (
    "s3://argo-dev-artifacts"
    if RES_ENV != "production"
    else "s3://argo-production-artifacts"
)

# store fonts in the resources fonts folder and make sure they are dumped on docker images so we can access this
FONT_LOCATION = os.environ.get("FONT_LOCATION", ".")  # /resources/fonts ..< add to


def load_secret(secret_name):
    ADDED_KEYS = []
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # In this sample we only handle the specific exceptions for the 'GetSecretValue'
    # API.
    # See docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        logging.logger.info("requesting secret...")
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields
        # will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

        logging.logger.info("adding secret keys...")

        for k, v in json.loads(secret).items():
            os.environ[k] = v
            ADDED_KEYS.append(k)

    except Exception as ex:
        logging.logger.warn(f"Unable to get the secret - {repr(ex)}")
        raise ex

    return ADDED_KEYS


def get_res_connect_token():
    from res.utils import secrets

    secrets.secrets_client.get_secret("RES_CONNECT_TOKEN")

    return os.environ.get("RES_CONNECT_TOKEN")


def try_get_env(key, default=None):
    return os.environ[key] if key in os.environ else default


def boolean_envvar(envvar, default):
    return str(os.environ.get(envvar, default)).lower() not in ["false", "f", "0", "no"]
