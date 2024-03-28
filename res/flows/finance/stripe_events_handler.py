from datetime import datetime
import pandas as pd
import urllib
import res
from res.utils import logger, secrets_client
from res.utils import ping_slack, safe_json_dumps
from res.flows import FlowContext
from res.utils import logger
import traceback
import stripe

TOPIC = "res_finance.payments.stripe_webhook_payload"
USE_KGATEWAY = True

VALID_STATUSES = ["1"]


def handle_new_event(event, fc):
    """
    handle events from stripe webhooks - could be payment intents, setup intents, anything
    """

    stripe_keys = secrets_client.get_secret(
        "STRIPE_API_SECRET_KEY", return_entire_dict=True
    )

    if event.get("is_dev") == True:
        stripe.api_key = stripe_keys.get("development")
    else:
        stripe.api_key = stripe_keys.get("production")

    stripe_event = stripe.Event.retrieve(event.get("id"))

    ping_slack(
        f"in res_finance.payments.stripe_webhook_payload.handle_new_event {safe_json_dumps(stripe_event)}",
        "payments_api_verbose",
    )
    res.utils.logger.info(safe_json_dumps(stripe_event))
