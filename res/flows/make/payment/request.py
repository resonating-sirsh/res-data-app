from res.utils import logger
import os
from res.flows.make.payment.actions import PaymentRequestResolver


def handle_new_event(event):
    resolver = PaymentRequestResolver()

    logger.info(event)
    try:
        resolver.process(event)
    except Exception as error:
        logger.info("Error on Payment Request")
        logger.error(error)
        return {"error": True, "valid": False, "reason": "Error on Payment Request"}
