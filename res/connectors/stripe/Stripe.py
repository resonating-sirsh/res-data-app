"""
Stripe API controller.
"""
import stripe
import json

from res.utils import secrets_client, logger
from res.connectors.stripe.error import StripeErrorCodes, StripeException


class StripeController(object):
    """
    Controller for Stripe API.
    """

    def __init__(self):
        stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")

    def create_payment_intent(
        self,
        amount: int,
        customer_id: str,
        payment_method_id: str,
        currency: str = "usd",
    ) -> stripe.PaymentIntent:
        """
        Creates a payment intent for a given customer and payment method.

        TODO: Create model or DTO for PaymentIntent
        """
        logger.info(
            f"Creating payment intent for customer {customer_id} with {amount} {currency}"
        )
        try:
            response = stripe.PaymentIntent.create(
                amount=amount,
                customer=customer_id,
                currency=currency,
                confirm=True,
                payment_method=payment_method_id,
            )
            return response
        except stripe.error.CardError as error:
            logger.error(error, exception=error, exception_type=type(error))
            charge = stripe.Charge.retrieve(error.error.payment_intent.latest_charge)
            logger.debug(json.dumps(charge, indent=4, sort_keys=True))
            logger.info(
                f"Charge id {error.error.payment_intent.latest_charge} outcome type: {charge.outcome.type}"
            )
            if charge.outcome.type == "blocked":
                raise StripeException(code=StripeErrorCodes.CHARGE_BLOCKED)
            elif charge.outcome.reason == "insufficient_funds":
                raise StripeException(code=StripeErrorCodes.INSUFFICIENT_FUNDS)
            elif error.code == "expired_card":
                raise StripeException(code=StripeErrorCodes.EXPIRED_CARD)
            elif error.code == "card_declined":
                raise StripeException(code=StripeErrorCodes.CARD_DECLINE)
            else:
                raise error
