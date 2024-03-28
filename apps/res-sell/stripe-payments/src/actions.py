import inspect
import os
from datetime import datetime

import arrow
import stripe

from res.utils import ping_slack, safe_json_dumps

# from src.query_definitions import (
#     GET_BRAND,
#     GET_BRAND_DIRECT_PAYMENT_DEFAULT,
#     GET_SUBSCRIPTION,
#     GET_TRANSACTION,
#     ORDER_UPDATE_MUTATION,
#     SAVING_BRAND,
#     SAVING_STRIPE_SUBSCRIPTION,
# )

SAVING_BRAND = """
mutation saveBrand(
  $input: [sell_brands_insert_input!]!
) {
  insert_sell_brands(objects: $input) {
    returning {
      id
      airtable_brand_code
      airtable_brand_id
    }
  }
}
"""


SAVING_STRIPE_SUBSCRIPTION = """
mutation saveStripeSubscription(
  $input: [sell_subscriptions_insert_input!]!
) {
  insert_sell_subscriptions(objects: $input) {
    returning {
      balance
      collection_method
      created_at
      deleted_status
      end_date
      id
      name
      start_date
      subscription_id
      updated_at
      stripe_customer_id
      brand_id
      payment_method
      current_period_start
      current_period_end
      is_direct_payment_default
    }
  }
}
"""


GET_SUBSCRIPTION = """
query getSubscription(
  $brand_id: Int!
) {
  sell_subscriptions(where: {brand_id: {_eq: $brand_id}}) {
    id
    balance
    brand_id
    collection_method
    created_at
    deleted_status
    end_date
    current_period_start
    current_period_end
    name
    payment_method
    start_date
    stripe_customer_id
    subscription_id
    updated_at
  }
}
"""


GET_BRAND = """
query getBrand(
  $brand_id: String!
) {
  sell_brands(where: {_or: [{airtable_brand_id: {_eq: $brand_id}}, {meta_record_id: {_eq: $brand_id}}]}) {
    id
  }
}
"""

GET_BRAND_DIRECT_PAYMENT_DEFAULT = """
query getBrandDirectPaymentDefault($id:ID!) {
	brand(id:$id){
    id
    isDirectPaymentDefault
  }
}
"""

GET_TRANSACTION = """
query GetTransactions($transaction_id: Int!) {
  sell_transactions(where: {id: {_eq: $transaction_id}}) {
    amount
    created_at
    currency
    description
    direct_stripe_payment
    id
    is_failed_transaction
    reference_id
    source
    stripe_charge_id
    subscription
    type
    updated_at
    transaction_type {
      value
    }
    transaction_source {
      value
    }
    subscriptionBySubscription {
      balance
      brand_id
      collection_method
      created_at
      currency
      current_period_end
      current_period_start
      deleted_status
      id
      end_date
      name
      is_direct_payment_default
      payment_method
      price
      price_amount
      start_date
      stripe_customer_id
      subscription_id
      updated_at
      brand {
        subscription {
          name
        }
        airtable_brand_code
      }
    }
    transaction_details {
      id
      make_cost
      order_date
      order_number
      order_type
      price
      revenue_share
      sku
      total_amount
      transaction_id
      type
    }
  }
}

"""

ORDER_UPDATE_MUTATION = """
mutation ChangeOrderPaymentState($id:ID!, $input:UpdateOrderInput!) {
  updateOrder(id:$id, input:$input){
    order{
      id
      wasBalanceNotEnough
    }
  }
}
"""


from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.stripe.Stripe import StripeController
from res.utils import logger, secrets_client
from res.utils.kafka_utils import message_produce


# Custom exception subclass
class PaymentMethodRequiredException(stripe.error.StripeError):
    def __init__(self, message=None, code=None, param=None):
        super().__init__(message, code, param)


class PaymentIntentNotCreatedException(stripe.error.StripeError):
    def __init__(self, message=None, code=None, param=None):
        super().__init__(message, code, param)


class PaymentOrderPaid:
    """This process updates airtable orders for wasPaymentSuccessful field."""

    def __init__(self):
        self.res_graphql_client = ResGraphQLClient()

    def caller_info(self):
        stack = inspect.stack()
        calling_function = stack[2].function
        caller = stack[2].frame.f_locals.get("self", None)
        calling_class = caller.__class__.__name__ if caller else None
        return calling_function, calling_class

    def update(self, transaction_source, order_id, is_failed, customer_id=None):
        order_update_data = {}
        ##Payment saved only on production
        if (
            transaction_source == "order"
            and order_id
            and (
                os.getenv("RES_ENV") == "production"
                or customer_id == os.getenv("TECH_PIRATES_TEST_CUSTOMER_ID")
            )
            and not is_failed
        ):
            callerDetails = "(notSet)"
            try:
                callerDetails = self.caller_info()
            except:
                pass
            ping_slack(
                f"wasPaymentSuccessful being set to true on PaymentOrderPaid by : {callerDetails} - order_id : {order_id}"
            )

            order_fields = {"wasPaymentSuccessful": True}
            order_update_data = self.res_graphql_client.query(
                ORDER_UPDATE_MUTATION, {"id": order_id, "input": order_fields}
            )
            logger.info(f"update_order_was_paid: {order_update_data}")
        else:
            logger.info(
                "No proper input for update or running on development environment or failed transaction."
            )
        return order_update_data


class TransactionPaymentResolver:
    stripe_controller: StripeController

    def __init__(self):
        self.graphql_client = ResGraphQLClient()
        self.order_payment = PaymentOrderPaid()
        self.stripe_controller = StripeController()

    def send_payment_intent(
        self, amount, customer, payment_method
    ) -> stripe.PaymentIntent:
        return self.stripe_controller.create_payment_intent(
            amount, customer, payment_method
        )

    def send_to_transaction_model(
        self,
        result_intent,
        orders_results,
        subscription,
        failed,
        status,
        error_message,
        customer_id,
    ):
        try:
            charge = {}
            charge["id"] = "0"
            if result_intent:
                charge = result_intent["charges"]["data"][0]
            source = "order"
            for result in orders_results:
                try:
                    failed_on_order_update = False
                    if not failed:
                        try:
                            if source or result:
                                self.order_payment.update(
                                    source, result["order_id"], failed, customer_id
                                )
                        except:
                            logger.warning("Issue on order update")
                            failed_on_order_update = True
                    logger.info(f"failed: {failed}")
                    if failed_on_order_update and not error_message:
                        error_message = "Error on update"
                    temp_message = {
                        "amount": result["amount"],
                        "type": "credit",
                        "source": source,
                        "reference_id": result["order_id"],
                        "currency": "usd",
                        "description": result["description"],
                        "customer_stripe_id": subscription["stripe_customer_id"],
                        "details": result["details"],
                        "stripe_charge_id": charge["id"],
                        "direct_stripe_payment": True,
                        "is_failed_transaction": failed or failed_on_order_update,
                        "transaction_status": status,
                        "transaction_error_message": error_message,
                    }

                    message_produce(temp_message, "res_sell.subscription.transaction")
                except Exception as error:
                    logger.warn("Error sending transaction.", errors=error)
        except Exception as error:
            logger.warn("Error sending transaction.", errors=error)


class TransactionReTriggerPayment:
    """This process payment request for orders from Shopify/Create One."""

    def __init__(self):
        self.hasura_client = Client()
        self.payment_resolver = TransactionPaymentResolver()

    def process(self, event):
        transaction_id = event["transaction_id"]

        sell_transaction_info = self.hasura_client.execute(
            GET_TRANSACTION,
            {"transaction_id": transaction_id},
        )

        if (
            sell_transaction_info["sell_transactions"]
            and sell_transaction_info["sell_transactions"][0]
        ):
            transaction_data = sell_transaction_info["sell_transactions"][0]
            subscription_data = transaction_data["subscriptionBySubscription"]
            stripe_customer_id = subscription_data["stripe_customer_id"]
            is_subscription_active = subscription_data and (
                (subscription_data["end_date"] is None)
                or datetime.fromisoformat(subscription_data["end_date"]).date()
                > datetime.now().date()
            )

            total_resonance_cents = -transaction_data["amount"]
            order_id = transaction_data["reference_id"]
            dict_array_details = transaction_data.get("transaction_details", [])
            if (
                is_subscription_active
                and subscription_data["balance"] > total_resonance_cents
                and not subscription_data.get("is_direct_payment_default")
            ):
                subscription = subscription_data
                temp_message = {
                    "amount": -total_resonance_cents,
                    "type": "credit",
                    "source": "order",
                    "reference_id": order_id,
                    "currency": "usd",
                    "description": f'{subscription["brand"]["subscription"]["name"]} Orderd payment: {subscription["brand"]["airtable_brand_code"]}-{dict_array_details[0]["order_number"]}',
                    "customer_stripe_id": subscription["stripe_customer_id"],
                    "details": dict_array_details,
                    "stripe_charge_id": event.get("stripe_charge_id"),
                    "transaction_status": "succeed",
                }

                logger.info(f"temp_message: {temp_message}")
                message_produce(temp_message, "res_sell.subscription.transaction")
                return {
                    "valid": True,
                    "reason": "Send balance transaction",
                    "data": {
                        "transaction": temp_message,
                        "customer_stripe_id": subscription["stripe_customer_id"],
                    },
                }
            elif is_subscription_active or subscription_data.get(
                "is_direct_payment_default"
            ):
                if subscription_data and total_resonance_cents:
                    subscription = subscription_data
                    stripe_customer_id = subscription["stripe_customer_id"]
                    payment_method = subscription["payment_method"]
                    result_intent = None
                    failed = True
                    orders_results = []

                    try:
                        result_intent = self.payment_resolver.send_payment_intent(
                            total_resonance_cents,
                            stripe_customer_id,
                            payment_method,
                        )
                        if (
                            result_intent
                            and result_intent["status"] == "requires_payment_method"
                        ):
                            raise PaymentMethodRequiredException(
                                "Payment requires a new payment method/Insufficient funds."
                            )
                        elif not result_intent:
                            raise PaymentIntentNotCreatedException(
                                "Not able to receive stripe intent."
                            )
                    except Exception as error:
                        logger.warn(
                            "Error on payment_ Intent fr delayed payment. ",
                            errors=error,
                        )
                        if result_intent:
                            self.payment_resolver.send_to_transaction_model(
                                result_intent,
                                orders_results,
                                subscription,
                                True,
                                result_intent["status"],
                                error,
                                stripe_customer_id,
                            )
                        else:
                            self.payment_resolver.send_to_transaction_model(
                                None,
                                orders_results,
                                subscription,
                                True,
                                "processing",
                                error,
                                stripe_customer_id,
                            )
                        result_intent = None
                        raise error

                    status = ""
                    error_message = ""

                    if (
                        result_intent != None
                        and result_intent.get("status") == "succeeded"
                        and len(result_intent.get("charges").get("data")) > 0
                        and result_intent["charges"]["data"][0]["paid"]
                        and result_intent["charges"]["data"][0]["status"] == "succeeded"
                    ):
                        status = result_intent["charges"]["data"][0]["status"]
                        failed = False
                        if total_resonance_cents:
                            amount = total_resonance_cents
                            orders_results.append(
                                {
                                    "amount": -amount,
                                    "order_id": order_id,
                                    "details": dict_array_details,
                                    "description": f'{subscription["brand"]["subscription"]["name"]} Direct Orderd payment: {subscription["brand"]["airtable_brand_code"]}-{dict_array_details[0]["order_number"]}',
                                }
                            )
                    else:
                        if (
                            result_intent
                            and result_intent["charges"]
                            and result_intent["charges"]["data"]
                            and result_intent["charges"]["data"][0]
                        ):
                            status = result_intent["charges"]["data"][0]["status"]
                            error_message = result_intent["charges"]["data"][0][
                                "failure_message"
                            ]
                    self.payment_resolver.send_to_transaction_model(
                        result_intent,
                        orders_results,
                        subscription,
                        failed,
                        status,
                        error_message,
                        stripe_customer_id,
                    )
                return {"valid": True, "reason": "Valid"}
            else:
                return {
                    "valid": False,
                    "reason": "Is not an active subscription.",
                    "data": None,
                }


class PaymentsHelper:
    def __init__(self):
        stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
        self.client = Client()
        self.graphql_client = ResGraphQLClient()

    def get_price(self, price_id):
        return stripe.Price.retrieve(price_id)

    def confirm_setup(self, client_secret):
        set_up = stripe.SetupIntent.retrieve(
            client_secret,
        )
        return set_up

    def create_secret(self):
        intent = stripe.SetupIntent.create(
            payment_method_types=[
                "card"
            ],  ## This is very important we only should support Credit-Card/ Debit-Card payemnts
        )
        return intent

    def create_customer(self, name, email, payment_method, price_id, brand_id, code):
        # Create Customer
        brand_data = self.client.execute(GET_BRAND, {"brand_id": brand_id})
        logger.info(f"in create customer. brand_data: {safe_json_dumps(brand_data)}")
        brand_default_setting = self.graphql_client.query(
            GET_BRAND_DIRECT_PAYMENT_DEFAULT, {"id": brand_id}
        )
        logger.info(
            f"in create customer. brand_default_setting: {safe_json_dumps(brand_default_setting)}"
        )
        is_direct_payment_default = False
        if brand_default_setting.get("data") and brand_default_setting.get("data").get(
            "brand"
        ):
            is_direct_payment_default = (
                brand_default_setting.get("data")
                .get("brand")
                .get("isDirectPaymentDefault")
            )

        brands = brand_data.get("sell_brands")
        logger.info(
            f"in create customer. sell_brands (from postgres): {safe_json_dumps(brands)}"
        )
        subscription_intance = None
        if brands:
            brand_instance = brands[0]
            subscription_data = self.client.execute(
                GET_SUBSCRIPTION, {"brand_id": brand_instance["id"]}
            )
            subscriptions = subscription_data.get("sell_subscriptions")
            logger.info(
                f"in create customer. subscriptions (from postgres): {safe_json_dumps(subscriptions)}"
            )
            if subscriptions:
                subscription_intance = subscriptions[0]
                if subscription_intance.get("stripe_customer_id"):
                    logger.info(
                        f"Customer and subscription already exist.: {safe_json_dumps(subscription_intance)}"
                    )
                    return {
                        "valid": False,
                        "data": "Customer and subscription already exist.",
                    }

            # return {
            #     "valid": False,
            #     "data": "Brand exist but subscription is missing.",
            # }
        if not subscription_intance or not subscription_intance.get(
            "stripe_customer_id"
        ):

            try:
                st_payment_method = stripe.PaymentMethod.retrieve(payment_method)
                new_customer = stripe.Customer.retrieve(st_payment_method.customer)
            except:
                new_customer = None

            if new_customer is None:
                new_customer = stripe.Customer.create(
                    description="Resonance Brand",
                    name=name,
                    email=email,
                    payment_method=payment_method,
                    invoice_settings={"default_payment_method": payment_method},
                )

            logger.info(f"stripe customer : {safe_json_dumps(new_customer)}")

            if payment_method:
                stripe.PaymentMethod.attach(
                    payment_method,
                    customer=new_customer["id"],
                )
                stripe.Customer.modify(
                    new_customer["id"],
                    invoice_settings={"default_payment_method": payment_method},
                )
                new_subscription = False
                logger.info(f"payment_method attached: {payment_method}")

                price_id = "price_1MCMvxA5RKqObLFcMF6fSuER"  # hard coding the 0 dollar sub to get us over a problem

                if (
                    new_customer
                    and payment_method
                    and price_id
                    and not is_direct_payment_default
                ):
                    price = self.get_price(price_id)
                    logger.info(f"price retrieved: {price_id}")
                    new_subscription = self.__create_subscription(
                        new_customer["id"], payment_method, price_id
                    )
                    logger.info(
                        f"new_subscription created: {new_subscription.stripe_id}"
                    )
                    if new_subscription and new_subscription["status"] == "active":
                        subscription_validation_status = new_subscription["items"][
                            "data"
                        ][0]["plan"]["active"]
                        subscription_validation_amount = new_subscription["items"][
                            "data"
                        ][0]["plan"]["amount"]

                        if (
                            subscription_validation_status
                            and subscription_validation_amount
                            == price.get(
                                "unit_amount"
                            )  ## Where are we setting the Brand subscription amount
                        ):
                            create_brand_payload = {
                                "airtable_brand_id": brand_id,
                                "airtable_brand_code": code,
                            }

                            brand_dbpkid = 99999
                            if not brands:
                                logger.info(
                                    f"creating brand in hasura : {safe_json_dumps(create_brand_payload)}"
                                )
                                new_brand_hasura = self.client.execute(
                                    SAVING_BRAND,
                                    {"input": [create_brand_payload]},
                                )

                                brand_dbpkid = new_brand_hasura["insert_sell_brands"][
                                    "returning"
                                ][0]["id"]
                            else:
                                logger.info(
                                    f"brand already existed in hasura : {safe_json_dumps(brands)}"
                                )
                                brand_dbpkid = brands[0]["id"]

                            create_subscription_payload = {
                                "collection_method": new_subscription[
                                    "collection_method"
                                ],
                                "name": name,
                                "subscription_id": new_subscription["id"],
                                "brand_id": brand_dbpkid,
                                "stripe_customer_id": new_customer["id"],
                                "current_period_start": arrow.get(
                                    new_subscription["current_period_start"]
                                ).for_json(),
                                "current_period_end": arrow.get(
                                    new_subscription["current_period_end"]
                                ).for_json(),
                                "currency": new_subscription["currency"],
                                "payment_method": payment_method,
                                "price": price_id,
                                "start_date": arrow.get(
                                    new_subscription["start_date"]
                                ).for_json(),
                                "end_date": (
                                    new_subscription["ended_at"]
                                    if new_subscription["ended_at"] is None
                                    else (
                                        arrow.get(
                                            new_subscription["ended_at"]
                                        ).for_json()
                                    )
                                ),
                                "price_amount": subscription_validation_amount,
                                "is_direct_payment_default": is_direct_payment_default,
                            }
                            logger.info(
                                f"about to exec: create_subscription_payload : {safe_json_dumps(create_subscription_payload)}"
                            )
                            new_subscription_hasura = self.client.execute(
                                SAVING_STRIPE_SUBSCRIPTION,
                                {"input": create_subscription_payload},
                            )
                            logger.info(f"sub saved!")
                            return {"valid": True, "data": new_subscription_hasura}

                elif is_direct_payment_default and new_customer and payment_method:
                    logger.info(
                        f"in is_direct_payment_default branch. not sure we should end up here march 2024"
                    )
                    create_brand_payload = {
                        "airtable_brand_id": brand_id,
                        "airtable_brand_code": code,
                    }
                    new_brand_hasura = self.client.execute(
                        SAVING_BRAND,
                        {"input": [create_brand_payload]},
                    )
                    create_subscription_payload = {
                        "name": name,
                        "brand_id": new_brand_hasura["insert_sell_brands"]["returning"][
                            0
                        ]["id"],
                        "stripe_customer_id": new_customer["id"],
                        "currency": "usd",
                        "payment_method": payment_method,
                        "start_date": arrow.now().for_json(),
                        "is_direct_payment_default": is_direct_payment_default,
                    }
                    print("create_subscription_payload")
                    new_subscription_hasura = self.client.execute(
                        SAVING_STRIPE_SUBSCRIPTION,
                        {"input": create_subscription_payload},
                    )
                    return {"valid": True, "data": new_subscription_hasura}

        return {"valid": False}

    def __create_subscription(self, customer_id, payment_method, price):
        # Here we need to define this price to be configurable
        # As welll as the Products
        new_subscription = stripe.Subscription.create(
            customer=customer_id,
            items=[{"price": price}],  # {"price": "price_1Jtxr3A5RKqObLFc4ch7Z0ZE"},
            default_payment_method=payment_method,
            metadata={"customer": customer_id},
        )

        return new_subscription

    def get_charge_errors(self, customer_id):
        print("get stripe data ")
        stripe_customer_id = customer_id
        requires_payment_method_intents = stripe.PaymentIntent.search(
            query=f"status:'requires_payment_method' AND customer:'{stripe_customer_id}'",
        )

        canceled_intents = stripe.PaymentIntent.search(
            query=f"status:'canceled' AND customer:'{stripe_customer_id}'",
        )

        unsuccessful_intents = (
            requires_payment_method_intents["data"] + canceled_intents["data"]
        )

        result_errors = []
        for intent in unsuccessful_intents:
            result = {
                "payment_intent_id": intent["id"],
                "customer": stripe_customer_id,
                "amount": intent["amount"],
                "status": intent["status"],
                "payment_method": intent["payment_method"],
            }
            if intent["charges"] and intent["charges"]["total_count"] > 0:
                charge = intent["charges"]["data"][0]
                result["charges_exist"] = True
                charge_data = {
                    "id": charge["id"],
                    "amount": charge["amount"],
                    "status": charge["status"],
                    "failure_message": charge["failure_message"],
                    "failure_balance_transaction": charge[
                        "failure_balance_transaction"
                    ],
                    "code": charge["failure_code"],
                }
                result["charge"] = charge_data
            else:
                result["charges_exist"] = False

            if result["charges_exist"] == False or (
                result["charges_exist"] == True
                and result["charge"]["status"] == "failed"
            ):
                result_errors.append(result)

        return result_errors


class CreateSubscription:
    def __init__(self, input) -> None:
        self.stripe = PaymentsHelper()
        self.input = input

    def create(self):
        try:
            ping_slack(
                f"\n\n[CreateSubscription] Save Sub attempt {safe_json_dumps(self.input)} <@U04HYBREM28> ",
                "payments_api_notification",
            )
            result = self.stripe.create_customer(
                self.input.get("brand").get("name"),
                self.input.get("email"),
                self.input["payment_method"],
                self.input["price"],
                self.input.get("brand").get("id"),
                self.input.get("brand").get("code"),
            )
            logger.info(result)
            return result
        except Exception as error:
            ping_slack(
                f"\n\n[CreateSubscription] Error {error} {safe_json_dumps(self.input)} @U04HYBREM28 ",
                "payments_api_notification",
            )
            logger.error("Error on Hasura save", error)

            raise error


class GetChargeErrors:
    # customer is the customer_id
    def __init__(self, customer) -> None:
        self.stripe = PaymentsHelper()
        self.customer = customer

    def get_all(self):
        try:
            all_failed_intents = self.stripe.get_charge_errors(self.customer)
            return all_failed_intents
        except Exception as error:
            print("Error on errors collector.")
            print(error)
            logger.error("Error on Hasura save", error)

            raise error


if (__name__) == "__main__":
    import json

    # e = {
    #     "payment_method": "pm_1OwSMXA5RKqObLFcUNAJoGEw",
    #     "price": "price_1MCMvxA5RKqObLFcE9odnFL7",
    #     "email": "Lolafaturotiloves@gmail.com",
    #     "brand": {
    #         "id": "recSDNDz3fETbsNM9",
    #         "name": "Lola Faturoti Loves",
    #         "code": "LL",
    #     },
    # }
    e = {
        "payment_method": "pm_1OwUeRA5RKqObLFcTtxyuHUb",
        "price": "null",
        "email": "dliddane@resonance.nyc",
        "brand": {"id": "recOPIxQHlMvmvEIt", "name": "Damien Liddane", "code": "DE"},
    }

    s = CreateSubscription(e)
    var = s.create()

    print(var)
