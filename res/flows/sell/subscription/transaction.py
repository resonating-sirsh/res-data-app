from res.utils import logger
import os
from res.flows.sell.subscription.actions import TransactionResolver


def handle_new_event(event):
    """
    Function handle_new_event for transaction.
    """
    resolver = TransactionResolver()

    logger.info(event)
    try:
        resolver.process(event)
    except Exception as error:
        logger.error(error)
        raise error


if __name__ == "__main__":
    pass
    # event = {
    #     "amount": 100000,
    #     "customer_stripe_id": "cus_NGa0tC99qrrplP",
    #     "type": "debit",
    #     "source": "subscription_recharge",
    #     "reference_id": "pi_3NDZ4lA5RKqObLFc0BBe8ssF",
    #     "currency": "usd",
    #     "description": "Subscription payment",
    #     "stripe_charge_id": "ch_3NDZ4lA5RKqObLFc09XR1NHL",
    #     "receipt_url": "https://pay.stripe.com/receipts/invoices/CAcQARoXChVhY2N0XzFKdHhlUUE1UktxT2JMRmMowrbZowYyBmifoBQCPTosFm45vaO4_T1Ys78xWocCVo5vMLvFqbijXT6cVKuXkRwEWNrqK3FW1oKBESk?s=ap",
    #     "direct_stripe_payment": None,
    #     "details": [],
    # }
    # handle_new_event(event)
