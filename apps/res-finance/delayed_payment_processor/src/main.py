import os
import traceback

from res.flows.sell.orders.delayed_payment import DelayedPaymentResolver
from res.utils import logger, ping_slack


def pop_hasura_endpoint():
    if "HASURA_ENDPOINT" in os.environ:
        os.environ.pop("HASURA_ENDPOINT")


# nudge ....


def set_prod_variables():
    environment = os.getenv("RES_ENV", "production")
    logger.info(environment)
    if environment == "production":
        os.environ["RES_ENV"] = "production"
        os.environ["HASURA_ENDPOINT"] = "https://hasura.resmagic.io"


if __name__ == "__main__":
    pop_hasura_endpoint()
    set_prod_variables()

    try:
        logger.info(
            "Getting all unpaid orders for all brands that use stripe. ver 26 mar...."
        )
        payment_resolver = DelayedPaymentResolver()
        payment_resolver.process()
    except Exception as errors:
        # nudge nudge
        slack_id_to_notify = " <@U04HYBREM28> "
        payments_api_slack_channel = "payments_api_notification"
        logger.warn("Error on DelayedPaymentResolver ", errors=errors)
        ping_slack(
            f"Error on DelayedPaymentResolver {slack_id_to_notify} {traceback.format_stack()} ",
            payments_api_slack_channel,
        )
