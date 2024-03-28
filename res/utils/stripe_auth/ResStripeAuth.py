"""
This is a Resonance Stripe Based authenticator. To Authenticate public facing request that needs payment validation.

"""

import stripe
import os
from ..secrets import secrets_client

environments = ["development", "production"]


class ResStripeAuthException(Exception):
    pass


class ResStripeAuth:
    def __init__(self, environment=None):
        # sa temp: changed this because self._env needs to be set + this is more concise
        self._environment = environment or os.getenv("RES_ENV", "development")
        stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")

    def authenticate_with_stripe(self, client_secret):
        error_message = "Client Secret not valid"
        array_client_secret = client_secret.split("_")
        setup_intent_id = "_".join(array_client_secret[0:2])
        try:
            setup_intent = stripe.SetupIntent.retrieve(setup_intent_id)
            ##Payment amount comes in cents value
            ## and paymentIntent['amount']/100 == 2500
            if not (
                setup_intent
                and setup_intent["client_secret"] == client_secret
                and setup_intent["status"] == "succeeded"
            ):
                raise ResStripeAuthException(error_message)
        except Exception as exc:
            raise ResStripeAuthException(error_message) from exc

    def authenticate_stripe_subscription(
        self, subscription_id, subscription_amount, subscription_status
    ):
        error_message = "Client subscription not valid."
        try:
            subscription = stripe.Subscription.retrieve(subscription_id)
            ##Payment amount comes in cents value
            ## and paymentIntent['amount']/100 == 2500
            if not (
                subscription
                and subscription["items"]["data"][0]["plan"]["amount"] / 100
                == subscription_amount
                and subscription["items"]["data"][0]["plan"]["active"]
                == subscription_status
            ):
                raise ResStripeAuthException(error_message)
        except Exception as exc:
            raise ResStripeAuthException(error_message) from exc
