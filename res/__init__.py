from importlib import import_module
import os
from . import utils

__version__ = 0.3

CLUSTER_NAME = os.getenv("RES_CLUSTER", "localhost")

# the default is not pinned to an environment: dev and prod can be on res-data-dev
# res-data-prod
BUCKET_S3_PRIMARY = os.environ.get("RES_DATA_BUCKET", "res-data-platform")
RES_DATA_BUCKET = BUCKET_S3_PRIMARY

secret_name = os.environ.get("RES_SECRET_NAME")


def get_meta_one_endpoint(endpoint, verb):
    import os
    import requests
    from functools import partial
    from res.utils.secrets import secrets

    if not os.environ.get("RES_META_ONE_API_KEY"):
        secrets.get_secret("RES_META_ONE_API_KEY")

    headers = {
        "Authorization": f"Bearer {os.environ['RES_META_ONE_API_KEY']}",
        "Content-type": "application/json",
    }
    f = getattr(requests, verb)
    f = partial(f, f"https://data.resmagic.io/{endpoint.lstrip('/')}", headers=headers)
    return f


if secret_name:
    utils.logger.info(
        f"RES_SECRET_NAME set to {secret_name} - loading secrets from secret manager..."
    )
    try:
        keys = utils.env.load_secret(secret_name)
        utils.logger.info(f"Added keys from secret - {keys}")
        # when we have the other details we can update the logger
        utils.logger.setup_metric_provider()
    except Exception as ex:
        utils.logger.warn(f"Failed to load secrets from secret manager - {repr(ex)}")


import_module(".connectors", __name__)

try:
    import_module(".flows", __name__)
except Exception as ex:
    pass
    # utils.logger.warn(
    #     "unable to load flows - catching for now because sometimes we do need flows - "
    #     f"{repr(ex)}"
    # )


class ResDataException(Exception):
    """
    Simple exception base
    We can try and raise exceptions from other excpetions and add res-specific context

    Example (note we raise 'from'):

    try:
        raise Exception('test inner exception')
    except Exception as ex:
        raise ResDataException("message", tags= {}) from ex

    in res logger, we should call `logger.error( "message", exception=inner_exception)`
    but we also build it into this exception as an option e.g. `send_to_sentry=True`

    When constructing an error message use a string that allows sentry to group the
    issue sensibly

    """

    def __init__(self, message, tags=None, send_to_sentry=True):
        super().__init__(message)
        if send_to_sentry:
            utils.logger.error(message, exception=self, tags=tags)


def _hello():
    print(f"Hello res! version {__version__}")


utils.logger.debug(f"Module '{__name__}' finished initializing!")
