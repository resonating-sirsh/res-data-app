import os
import res
from res.connectors.airtable import AirtableMetadataCache

RES_ENV = os.environ.get("RES_ENV")


def handler(event, context={}):

    res.utils.logger.info(
        "Starting task schedule handle for AirtableMetadataCache reload cache.  This process RES_ENV: {RES_ENV}"
    )

    cache = AirtableMetadataCache.get_instance()

    cache.reload_cache()

    res.utils.logger.info(
        "Completed task schedule handle for AirtableMetadataCache reload cache "
    )

    return {}
