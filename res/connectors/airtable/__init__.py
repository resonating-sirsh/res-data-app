from res.utils import logger, env, safe_http, dates, dataframes

alist = [logger, env, safe_http, dates, dataframes]
import os

from .. import (
    DatabaseConnector,
    DatabaseConnectorSchema,
    DatabaseConnectorTable,
)

blist = [
    DatabaseConnector,
    DatabaseConnectorSchema,
    DatabaseConnectorTable,
]


AIRTABLE_LATEST_S3_BUCKET = "airtable-latest"
AIRTABLE_VERSIONED_S3_BUCKET = "airtable-versioned"
AIRTABLE_API_ROOT = "https://api.airtable.com/v0"
METADATA_REPO_NAME = "test"  # for now this is just the name of mongo database


def get_airtable_request_header():
    return {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}


from ..mongo import MongoConnector
from ..s3 import S3Connector
from .AirtableWebhooks import AirtableWebhooks
from .AirtableConnector import AirtableConnector
from .AirtableMetadataCache import AirtableMetadataCache
from .AirtableMetadata import AirtableMetadata
from .AirtableWebhookSession import AirtableWebhookSession
from .AirtableWebhookSpec import AirtableWebhookSpec
from . import readers

clist = [
    MongoConnector,
    S3Connector,
    AirtableWebhooks,
    AirtableConnector,
    AirtableMetadata,
    AirtableMetadataCache,
    AirtableWebhookSession,
    AirtableWebhookSpec,
    readers,
]


def load_view_from_path(path, **kwargs):
    path = path.split("/")
    return AirtableConnector()[path[0]][path[1]].to_dataframe(view=path[2], **kwargs)
