from res.utils import logger
from res.connectors.airtable.AirtableClient import ResAirtableClient

BASE = "appu11yom3UIdHF2M"
TABLE = "tblfjTT2mVQhsC8vm"


def handler(event, context=None):
    logger.info("Event: " + str(event))
    record_ids = event["assets"][0]["record_ids"]
    client = ResAirtableClient()
    # Loop thru any records
    for record in record_ids:
        logger.info("Setting field to triggered...")
        logger.info(client.set_record(BASE, TABLE, record, "Triggered", True).text)
