import os
from datetime import datetime
from res.utils import logger
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.flows.make.payment.queries import (
    GET_SUBSCRIPTION,
    GET_BRAND,
    ORDER_UPDATE_MUTATION,
)
from res.utils.kafka_utils import message_produce
from res.flows.make.payment.actions_calculator import PaymentInvoiceCalculator


class PaymentRequestResolver:
    """This process payment request for orders from Shopify/Create One."""

    def __init__(self):
        self.hasura_client = Client()
        self.graphql_client = ResGraphQLClient()
        self.payment_calculator = PaymentInvoiceCalculator()

    def process(self, event):
        logger.info(event)
        data = None
        # Call here calculator
        calculations = self.payment_calculator.process(event)
        data = calculations.get("data")
        total_resonance_cents = 0
        dict_array_details = calculations.get("dict_array_details")
        if calculations.get("total_resonance_cents"):
            total_resonance_cents = calculations.get("total_resonance_cents")

        sell_brands_array = self.hasura_client.execute(
            GET_BRAND,
            {"brand_id_string": event["brand_id"]},
        )
        if sell_brands_array["sell_brands"] and sell_brands_array["sell_brands"][0]:
            brand_data = sell_brands_array["sell_brands"][0]
            logger.info(f"Brand Data {brand_data}")
            subscription_data = self.hasura_client.execute(
                GET_SUBSCRIPTION,
                {"brand_id": brand_data["id"]},
            )
            print(f" Subscription {subscription_data}")
            is_subscription_active = (
                subscription_data
                and subscription_data["sell_subscriptions"]
                and subscription_data["sell_subscriptions"][0]
                and (
                    (subscription_data["sell_subscriptions"][0]["end_date"] is None)
                    or datetime.fromisoformat(
                        subscription_data["sell_subscriptions"][0]["end_date"]
                    ).date()
                    > datetime.now().date()
                )
            )
            logger.info(f"is_subscription_active: {is_subscription_active}")

            balance = 0
            subscription = {}

            if (
                subscription_data
                and subscription_data["sell_subscriptions"]
                and subscription_data["sell_subscriptions"][0]
            ):
                balance = subscription_data["sell_subscriptions"][0].get("balance", 0)
                subscription = subscription_data["sell_subscriptions"][0]

            if (
                is_subscription_active
                and balance > total_resonance_cents
                and not subscription_data["sell_subscriptions"][0].get(
                    "is_direct_payment_default"
                )
            ):
                temp_message = {
                    "amount": -total_resonance_cents,
                    "type": "credit",
                    "source": "order",
                    "reference_id": event["order_id"],
                    "currency": "usd",
                    "description": f'{calculations["brand_name"]} Ordered payment: {calculations["order_number"]}',
                    "customer_stripe_id": subscription.get("stripe_customer_id"),
                    "details": dict_array_details,
                    "stripe_charge_id": event.get("stripe_charge_id"),
                    "is_failed_transaction": False,
                    "transaction_status": "succeeded",
                }
                ##This is a valid transaction send/ No need for status here
                ##Done: Add is_failed_transaction
                logger.info(f"temp_message: {temp_message}")
                message_produce(temp_message, "res_sell.subscription.transaction")
                return {
                    "valid": True,
                    "reason": "Send balance transaction",
                    "transaction": temp_message,
                    "customer_stripe_id": subscription.get("stripe_customer_id"),
                }
            elif is_subscription_active or subscription_data["sell_subscriptions"][
                0
            ].get("is_direct_payment_default"):
                reason = "Marked as balance not enough order"
                ##Payment saved only on production
                if os.getenv("RES_ENV") == "production" or subscription.get(
                    "stripe_customer_id"
                ) == os.getenv("TECH_PIRATES_TEST_CUSTOMER_ID"):
                    order_fields = {"wasBalanceNotEnough": True}
                    order_update_data = self.graphql_client.query(
                        ORDER_UPDATE_MUTATION,
                        {"id": event["order_id"], "input": order_fields},
                    )
                    logger.info("Order Updated for wasBalanceNotEnough.")
                    logger.info(order_update_data)
                else:
                    logger.info(
                        "Order Not Updated for wasBalanceNotEnough, running in development."
                    )
                return {"valid": True, "reason": reason}
            else:
                return {"valid": False, "reason": "Is not an active subscription."}
