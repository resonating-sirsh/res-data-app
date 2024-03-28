from res.utils import logger
from res.connectors.airtable import AirtableWebhookSpec
from res.utils.secrets import secrets_client


def handler(event, context):
    """
    Handler trigger in the crons main or via a res-connect trigger
    Loops through all configured webhooks and calls `.sync()`
    Sync does two things
    1. It makes sure the webhook on airtable has the same spec as our configured version
    2. It makes sure the callbacks are enabled on the webhook ie. that the webhook is healthy
    """

    secrets_client.get_secret("AIRTABLE_API_KEY")

    specs = AirtableWebhookSpec.load_all()

    logger.info(f"running webhooks sync for {len(specs)} webhooks")

    for spec in specs:
        try:
            logger.info(f"Syncing spec {spec}")
            spec.sync()
        except Exception as ex:
            logger.warning(f"Failed when syncing spec", exception=ex)

    logger.info("Done")


if __name__ == "__main__":
    handler({}, {})
