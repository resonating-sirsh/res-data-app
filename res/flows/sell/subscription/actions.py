from res.utils import logger
from res.connectors.graphql.hasura import Client
from res.utils import ping_slack
import traceback
from res.utils import safe_json_dumps
from res.flows.sell.subscription.queries import (
    INSERT_TRANSACTION,
    UPDATE_BALANCE_SUBSCRIPTION,
    GET_SUBSCRIPTION,
    INSERT_TRANSACTION_DETAILS,
)
from res.flows.make.payment.actions_order_paid import PaymentOrderPaid

slack_id_to_notify = " <@U04HYBREM28> "


class TransactionResolver:
    """This process saves transactions for brand balance and direct payments."""

    def __init__(self):
        self.hasura_client = Client()
        self.order_payment = PaymentOrderPaid()

    def process(self, event):
        try:
            element = dict()
            subscription_data = self.hasura_client.execute(
                GET_SUBSCRIPTION,
                {"stripe_customer_id": event["customer_stripe_id"]},
            )

            logger.info(f"subscription_data: {safe_json_dumps(subscription_data)}")
            logger.info(
                f"TransactionResolver.Process. Event receieved: \n {safe_json_dumps(event)}"
            )

            is_failed_transaction = event.get("is_failed_transaction", False)

            if not subscription_data["sell_subscriptions"]:
                msg = f"No Subscription data in postgres for the given strip customer ID.  Unable to top up balance of this brand cust id:{event['customer_stripe_id']}"
                raise Exception(msg)

            self.order_payment.update(
                event["source"],
                event["reference_id"],
                is_failed_transaction,
                event.get("customer_stripe_id"),
            )

            element = self.hasura_client.execute(
                INSERT_TRANSACTION,
                {
                    "input": {
                        "amount": event["amount"],
                        "description": event["description"],
                        "reference_id": event["reference_id"],
                        "currency": event["currency"],
                        "source": event["source"],
                        "stripe_charge_id": event.get("stripe_charge_id"),
                        "subscription": subscription_data["sell_subscriptions"][0][
                            "id"
                        ],
                        "type": event["type"],
                        "direct_stripe_payment": event.get("direct_stripe_payment"),
                        "is_failed_transaction": is_failed_transaction,
                        "transaction_status": event.get("transaction_status"),
                        "transaction_error_message": event.get(
                            "transaction_error_message"
                        ),
                        ##Done: Add is_failed_transaction
                    }
                },
            )
            logger.info(f"transaction element: {safe_json_dumps(element)}")
            logger.info(f"direct_stripe_payment: {event.get('direct_stripe_payment')}")
            if not event.get("direct_stripe_payment") and not is_failed_transaction:
                self.hasura_client.execute(
                    UPDATE_BALANCE_SUBSCRIPTION,
                    {
                        "subscription_id": subscription_data["sell_subscriptions"][0][
                            "id"
                        ],
                        "amount": event["amount"],
                    },
                )

            detail_insert = []
            if (
                event["details"]
                and event["details"][0]
                and event["details"][0].get("shipping")
                and int(event["details"][0].get("shipping")) > 0
            ):
                # One shipping Item
                detail_insert.append(
                    {
                        "type": "shipping",
                        "sku": "Shipping",
                        "total_amount": event["details"][0]["shipping"],
                        "make_cost": event["details"][0]["shipping"],
                        "transaction_id": element["insert_sell_transactions_one"]["id"],
                        "order_number": event["details"][0]["order_number"],
                        "order_date": event["details"][0]["order_date"],
                        "order_type": event["details"][0]["order_type"],
                    }
                )

            for detail in event["details"]:
                detail_insert.append(
                    {
                        "make_cost": detail["make_cost"],
                        "price": detail["price"],
                        "sku": detail["sku"],
                        "transaction_id": element["insert_sell_transactions_one"]["id"],
                        "order_number": detail["order_number"],
                        "order_date": detail["order_date"],
                        "order_type": detail["order_type"],
                        "revenue_share": detail["revenue_share"],
                        "total_amount": detail["total_amount"],
                        "type": "item",
                    }
                )

            logger.info("detail_insert: ")
            logger.info(detail_insert)

            self.hasura_client.execute(
                INSERT_TRANSACTION_DETAILS,
                {"details": detail_insert},
            )
        except Exception as error:
            msg = f"{traceback.format_exc()}  \n\n processing: \n{safe_json_dumps(event)} "
            logger.error(msg)

            ping_slack(
                f"{msg} {slack_id_to_notify} ",
                "damo-test",
            )
            raise error

        return element
