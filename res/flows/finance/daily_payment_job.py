import json
import traceback
from datetime import datetime

import stripe

import res
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.flows.finance.order_cost_price_resolver import OrderCostPriceResolver
from res.flows.finance.order_payment_details_provider import OrderPaymentDetailsProvider
from res.flows.make.payment.actions_calculator import PaymentInvoiceCalculator
from res.flows.make.payment.actions_order_paid import PaymentOrderPaid
from res.utils import logger, ping_slack, secrets_client
from res.utils.kafka_utils import message_produce

slack_id_to_notify = " <@U04HYBREM28> "
payments_api_slack_channel = "payments_api_notification"

GET_DELAYED_ORDERS_FOR_BRANDS = """
query GetOrders($after:String, $brand_code:WhereValue!) {
  orders(first: 100, after:$after, where:{and:[{brandCode:{is:$brand_code}}, {wasPaymentSuccessful: {is: false}}, {wasBalanceNotEnough: {is: true}}]}){
    count
    cursor
    orders{
      id
      name
      code
      sourceName
      wasBalanceNotEnough
      wasPaymentSuccessful
      brand{
        id
        code
        name
      }
    }
  }
}
"""

GET_SUBSCRIPTIONS = """
query MyQuery($brand_id: String!) {
  sell_subscriptions(where: {brand: {airtable_brand_id: {_eq: $brand_id}}}) {
    id
    balance
    brand_id
    collection_method
    stripe_customer_id
    end_date
    payment_method
    brand {
      id
      airtable_brand_id
    }
  }
}
"""


class DailyPaymentJob:
    """This process payment request for 'out of balance'/Direct payment Orders."""

    def __init__(self):
        self.hasura_client = Client()
        self.postgres = res.connectors.load("postgres")
        self.graphql_client = ResGraphQLClient()
        self.payment_calculator = PaymentInvoiceCalculator()
        self.order_payment = PaymentOrderPaid()
        stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
        # stripe.api_key = "sk_test_51JtxeQA5RKqObLFcis2VuftakrCr6tcY1N5B6ooijCmvIEik8XvHvA1zUQ3jQ48ofL2kzQrh0bwG7X2D9vPaYiji00bMdqr7xC"
        self.date_run_this_process_from = datetime(2024, 3, 1)

    def send_payment_intent(
        self,
        amount,
        customer,
        payment_method,
        list_order_ids=None,
        list_order_amounts=None,
        payment_aggregate_id=None,
        brand_code=None,
        new_vs_old_dict={},
    ):
        metadata = {
            "list_order_ids_1": ",".join(list_order_ids[0:40])[:499],
            "list_order_ids_2": ",".join(list_order_ids[41:81])[:499],
            "list_order_ids_3": ",".join(list_order_ids[82:102])[:499],
            "list_order_amounts_1": ",".join(list_order_amounts[0:40])[:499],
            "list_order_amounts_2": ",".join(list_order_amounts[41:81])[:499],
            "list_order_amounts_3": ",".join(list_order_amounts[82:102])[:499],
            "payment_aggregate_id": payment_aggregate_id,
            "brand_code": brand_code,
        }
        metadata.update(new_vs_old_dict)

        ping_slack(
            f"Sending payment intent. {customer} {payment_method} Brand:{brand_code} amount: {amount}",
            payments_api_slack_channel,
        )
        intent = stripe.PaymentIntent.create(
            amount=amount,
            customer=customer,
            currency="usd",
            confirm=True,
            payment_method=payment_method,
            # automatic_payment_methods={"enabled": True, "allow_redirects": "never"}, # this is needed for newer vesion of strip api
            description="Product Cost, Shipping, and Rev Share",
            # max keys = 50 in metadat, and max len of any key = 500. Making assumption no brand will go over 102 orders in one payment, for metadata logging
            metadata=metadata,
        )
        ping_slack(
            f"Sent payment intent. {customer}  intent: {intent}",
            "payments_api_notification",
        )
        return intent

    def send_to_transaction_model(
        self, result_intent, orders_results, subscription, failed, status, error_message
    ):
        temp_message = {}
        try:
            charge = {}
            charge["id"] = None
            if result_intent:
                charge = result_intent["charges"]["data"][0]
            source = "order"
            for result in orders_results:
                try:
                    failed_on_order_update = False
                    if not failed:
                        try:
                            if source or result:
                                # update the order in airtable to mark wasPaymentSuccessful = True
                                self.order_payment.update(
                                    source,
                                    result["order_id"],
                                    failed,
                                    subscription.get("stripe_customer_id"),
                                )
                        except Exception as ex:
                            msg = f"[DELAYED PAYMENT] Issue on order update {traceback.format_ex()} {slack_id_to_notify}"
                            logger.warning(msg)
                            ping_slack(msg, payments_api_slack_channel)
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
                        "customer_stripe_id": subscription.get("stripe_customer_id"),
                        "details": result["details"],
                        "stripe_charge_id": charge["id"],
                        "direct_stripe_payment": True,
                        "is_failed_transaction": failed or failed_on_order_update,
                        "transaction_status": status,
                        "transaction_error_message": error_message,
                    }

                    ##Done: Add is_failed_transaction
                    message_produce(temp_message, "res_sell.subscription.transaction")
                    ping_slack(
                        f"published transaction. {temp_message.get('amount')}, {temp_message.get('description')} {temp_message.get('stripe_charge_id')}",
                        payments_api_slack_channel,
                    )
                except Exception as ex:
                    msg = f"Error sending res_sell.subscription.transaction.{traceback.format_exc()} {slack_id_to_notify}"
                    logger.error(msg, errors=error)
                    ping_slack(msg, payments_api_slack_channel)
        except Exception as error:
            msg = f"Error sending res_sell.subscription.transaction. {traceback.format_exc()} {slack_id_to_notify}"
            logger.error(msg, errors=error)
            ping_slack(msg, payments_api_slack_channel)

    def get_unpaid_orders_ready_for_payment(
        self, list_brand_codes, dfBrands, start_date_check_from
    ):
        provider = OrderPaymentDetailsProvider(postgres=self.postgres)
        dfOrders = provider.get_unpaid_orders_for_brands(
            list_brand_codes, start_date_check_from
        )
        dfOrders = provider.update_df_with_missing_revenue_share(dfOrders, dfBrands)
        dfOrders = provider.filter_out_orders_with_missing_price_or_costs(dfOrders)
        return dfOrders

    def build_transactions_list_dics(self, dfOrdersToBill):
        dfOrdersToBillWithCostPriceSet = dfOrdersToBill[
            ~dfOrdersToBill["missing_cost_price_info"]
        ].copy()

        # Calculate revenue share amount for each item based on sales channel
        dfOrdersToBillWithCostPriceSet[
            "revenue_share_amount"
        ] = dfOrdersToBillWithCostPriceSet.apply(
            lambda row: (
                row["price"] * row["revenue_share_percentage_retail"]
                if row["sales_channel"] == "ECOM"
                else (
                    row["price"] * row["revenue_share_percentage_wholesale"]
                    if row["sales_channel"].lower() == "wholesale"
                    else 0
                )
            ),
            axis=1,
        )

        dfOrdersToBillWithCostPriceSet[
            "order_line_total_amount"
        ] = dfOrdersToBillWithCostPriceSet.apply(
            lambda row: (row["revenue_share_amount"] + row["basic_cost_of_one"]),
            axis=1,
        )

        # Group by order and calculate the order total
        orders = (
            dfOrdersToBillWithCostPriceSet.groupby("name")
            .apply(
                lambda x: {
                    "brand_code": x["brand_code"].iloc[0],
                    "order_total": x["order_line_total_amount"].sum(),
                    "sales_channel": x["sales_channel"].iloc[0],
                    "ecommerce_source": x["ecommerce_source"].iloc[0],
                    "list_order_line_items": x[
                        [
                            "sku",
                            "order_line_total_amount",
                            "revenue_share_amount",
                            "basic_cost_of_one",
                            "price",
                        ]
                    ].to_dict("records"),
                }
            )
            .reset_index(name="order_details")
        )

        import pandas as pd

        orders = orders.join(pd.json_normalize(orders["order_details"]))

        # Convert orders to the required list of dicts format
        orders_dict = (
            orders.groupby("brand_code")["order_details"].apply(list).to_dict()
        )

        return orders_dict

    def prepare_for_payments_run(self):
        # The OrderCostPriceResolver runs regularly, but we try one last attempt to get sell order line items ready for payments by stamping prices and costs on order lines, for any that might be missing
        resolver = OrderCostPriceResolver()
        resolver.check_and_fix_orders_for_recent_days(publish_missing_sku_nag=False)

    def process_new(self):
        logger.info("Start delayed payment run process_new. ver 16 mar.")

        # self.prepare_for_payments_run()

        dfBrands = self.get_brands_from_postgres_with_payment_methods()

        brand_codes_with_paymethods = list(
            dfBrands[~dfBrands["payment_method"].isnull()]["brand_code"]
        )  # ['RS', 'BS']
        brand_codes_with_paymethods.append("TK")
        logger.info(f"brand_codes_with_paymethods: {brand_codes_with_paymethods}")

        dfOrdersToBill = self.get_unpaid_orders_ready_for_payment(
            brand_codes_with_paymethods, dfBrands, self.date_run_this_process_from
        )
        dfOrdersToBill.to_parquet("billingOrders.parquet")
        import pandas as pd

        dfOrdersToBill = pd.read_parquet("billingOrders.parquet")

        self.build_transactions_list_dics(dfOrdersToBill)

    def process(self):
        result_intent = None
        logger.info("Start delayed payment run process. ver 22 feb..")

        brands_list = []

        brands_list = self.get_brands_from_postgres()

        for brand_info in brands_list:
            after_delayed_order_string = "0"
            errors_on_brand_payment_continue = False
            total_orders = 0
            count_orders = 0
            total_resonance_cents = 0

            # total_resonance_cents_all_orders = 0
            orders_first_iteration = True
            logger.info(f"orders_first_iteration: {orders_first_iteration}")
            orders_results = []
            result_intent = None
            order_update_data = {}
            subscription = {}
            payment_status = "processing"
            try:
                is_transaction_recorded = False
                while orders_first_iteration or count_orders < total_orders:
                    payment_status = "processing"
                    logger.info("In while: ")
                    if errors_on_brand_payment_continue:
                        continue
                    orders_first_iteration = False
                    count_orders = count_orders + 100
                    logger.info(
                        f"count_orders: {count_orders} total_orders: {total_orders} after_delayed_order_string: {after_delayed_order_string} brand_id: {brand_info['id']} brand code: {brand_info['code']}"
                    )
                    # First 100 always
                    delayed_orders_request = self.graphql_client.query(
                        GET_DELAYED_ORDERS_FOR_BRANDS,
                        {
                            "after": after_delayed_order_string,
                            "brand_code": brand_info["code"],
                        },
                    )

                    logger.info("Delayed Orders")
                    order_update_data = delayed_orders_request["data"]
                    logger.info("order_update_data: ")
                    logger.info(
                        json.dumps(
                            order_update_data,
                            default=res.utils.safe_json_serialiser,
                            indent=4,
                        )
                    )
                    if order_update_data and order_update_data["orders"]:
                        total_orders = order_update_data["orders"]["count"]
                        after_delayed_order_string = order_update_data["orders"][
                            "cursor"
                        ]
                    msg = f"{total_orders} orders for {brand_info['code']} "
                    res.utils.logger.info(msg)
                    ping_slack(msg, payments_api_slack_channel)

                    for order in order_update_data["orders"]["orders"]:
                        logger.info(f"Iterate Over orders count: {count_orders}")
                        calculator_input = {}
                        calculator_input["order_id"] = order["id"]
                        calculator_input["brand_id"] = order["brand"]["id"]

                        logger.info("calculator_input: ")
                        logger.info(calculator_input)

                        calculations = None
                        try:
                            calculations = self.payment_calculator.process(
                                calculator_input
                            )
                        except Exception as e:
                            logger.warning(
                                "Issue calculating process payment calculator."
                            )
                            ping_slack(
                                f"[DELAYED PAYMENT] Issue calculating process payment calculator. {e} {slack_id_to_notify}",
                                payments_api_slack_channel,
                            )
                            logger.warning(e)

                            continue
                        logger.info("Calculations: ")
                        logger.info(
                            json.dumps(
                                calculations,
                                default=res.utils.safe_json_serialiser,
                                indent=4,
                            )
                        )
                        if calculations.get("total_resonance_cents"):
                            amount = calculations.get("total_resonance_cents")
                            total_resonance_cents = total_resonance_cents + amount
                            orders_results.append(
                                {
                                    "amount": -amount,
                                    "order_id": order["id"],
                                    "details": calculations.get("dict_array_details"),
                                    "description": f'{order["brand"]["name"]} Direct Orderd payment: {order["brand"]["code"]}-{order["code"]}',
                                    "order_number": f'{order["brand"]["code"]}-{order["code"]}',
                                    "brand_code": f'{order["brand"]["code"]}',
                                }
                            )
                order_numbers = [o["order_number"] for o in orders_results]
                order_amounts_str = [
                    f"${round(float(o['amount'])/100, 2)*-1}" for o in orders_results
                ]
                order_amounts_float = [
                    round(float(o["amount"]) / 100, 2) * -1 for o in orders_results
                ]
                brand_code_stripe = (
                    orders_results[0]["brand_code"] if orders_results else ""
                )
                self.log_blank_lines(10)

                logger.info(
                    f"brand name: {brand_info.get('name')} brand code: {brand_info.get('code') }"
                )
                logger.info(
                    f"order_numbers to attempt payment for: {','.join(order_numbers)}"
                )
                logger.info(
                    f"order_amounts_float to attempt payment for: {','.join(map(str, order_amounts_float))}"
                )
                new_vs_old_dict = self.determine_new_orders_vs_old_orders(
                    order_numbers, order_amounts_float
                )

                logger.info(json.dumps(new_vs_old_dict, indent=4))

                if total_resonance_cents > 0:
                    subscriptions_data = self.hasura_client.execute(
                        GET_SUBSCRIPTIONS,
                        {"brand_id": brand_info["id"]},
                    )

                    if (
                        subscriptions_data.get("sell_subscriptions")
                        and len(subscriptions_data.get("sell_subscriptions")) > 0
                    ):
                        subscription = subscriptions_data["sell_subscriptions"][0]
                        stripe_customer_id = subscription["stripe_customer_id"]
                        payment_method = subscription["payment_method"]
                        logger.info("Send Payment")
                        logger.info(
                            f"total_resonance_cents: {total_resonance_cents} stripe_customer_id {stripe_customer_id} brand code: {brand_info['code']} payment method: {payment_method}"
                        )

                        # stripe_customer_id = "cus_P5fOSOJSmwPcu7"  # this is a random customer ID for test
                        # payment_method = "pm_1OHU3OA5RKqObLFcEFerbvZh"   PM for that customer
                        # "pm_card_visa_chargeDeclinedInsufficientFunds"
                        # payment_method = "pm_card_visa_chargeDeclinedInsufficientFunds"
                        # payment_method = "pm_card_visa"

                        payment_error_message = ""
                        local_stripe_error = None
                        try:
                            result_intent = self.send_payment_intent(
                                total_resonance_cents,
                                stripe_customer_id,
                                payment_method,
                                list_order_amounts=order_amounts_str,
                                list_order_ids=order_numbers,
                                brand_code=brand_code_stripe,
                                new_vs_old_dict=new_vs_old_dict,
                            )
                        except Exception as stripe_error:
                            result_intent = None
                            local_stripe_error = stripe_error
                            logger.warn(
                                "Error on payment_ Intent fr delayed payment.",
                                errors=stripe_error,
                            )
                            payment_error_message = stripe_error.__repr__()
                            errors_on_brand_payment_continue = True
                            ping_slack(
                                f"{stripe_customer_id} {payment_method} {payment_error_message} {slack_id_to_notify}",
                                payments_api_slack_channel,
                            )
                        logger.info(result_intent)

                        payment_failed = True
                        payment_status = "processing"

                        if (
                            result_intent
                            and result_intent.get("charges", {}).get("data")
                            and len(result_intent.get("charges").get("data")) > 0
                        ):
                            payment_status = result_intent["charges"]["data"][0][
                                "status"
                            ]
                            payment_error_message += str(
                                result_intent["charges"]["data"][0]["failure_message"]
                            )

                            if (
                                result_intent != None
                                and result_intent.get("status") == "succeeded"
                                and result_intent["charges"]["data"][0]["paid"]
                                and result_intent["charges"]["data"][0]["status"]
                                == "succeeded"
                            ):
                                payment_failed = False

                        self.send_to_transaction_model(
                            result_intent,
                            orders_results,
                            subscription,
                            payment_failed,
                            payment_status,
                            payment_error_message,
                        )
                        is_transaction_recorded = True
            except Exception as error:
                # TODO: Use stripe specific error handler.
                message = "Error on delayed payment while trying to update."
                logger.warn(message, errors=error)
                ping_slack(
                    f"{message} {error} {subscription} {result_intent} {slack_id_to_notify}",
                    payments_api_slack_channel,
                )
                if not is_transaction_recorded:
                    self.send_to_transaction_model(
                        result_intent,
                        orders_results,
                        subscription,
                        True,
                        payment_status,
                        f"{message} error: {error}",
                    )

    def get_brands_from_postgres(self):
        postgres = res.connectors.load("postgres")
        brands_list2 = postgres.run_query(GET_ALL_BRANDS_SQL).to_dict("records")
        for d in brands_list2:
            d["id"] = d["airtable_brand_id"]
            d["code"] = d["airtable_brand_code"]
            d.pop("airtable_brand_id")
            d.pop("airtable_brand_code")
        return brands_list2

    def get_brands_from_postgres_with_payment_methods(self):
        self.postgres = self.postgres or res.connectors.load("postgres")
        dfBrands = self.postgres.run_query(GET_ALL_BRANDS_SQL_WITH_PAYMENT_METHODS)

        return dfBrands

    def determine_new_orders_vs_old_orders(
        self, to_pay_for_order_numbers, to_pay_for_order_amounts_float
    ):
        """figure out which, if any, orders we have already tried to bill for"""
        alreadybilledAmount = float(0)
        notYetBilledAmount = float(0)
        alreadyBilledOrderNumbers = []
        notYetBilledOrderNumbers = []
        query_parts = []
        for to_pay_order_number in to_pay_for_order_numbers:
            query_parts.append(f"description LIKE '%{to_pay_order_number}'")

        query = (
            "SELECT description FROM sell.transactions WHERE ("
            + " OR ".join(query_parts)
            + ") and is_failed_transaction=True;"
        )  # and transaction failed?

        if to_pay_for_order_numbers or to_pay_for_order_amounts_float:
            postgres = res.connectors.load("postgres")
            rows = postgres.run_query(query)["description"].unique()
            db_order_numbers = []
            for description in rows:
                db_order_number = description.split()[-1]
                db_order_numbers.append(db_order_number)

            to_pay_order_index = 0
            for to_pay_order_num in to_pay_for_order_numbers:
                if to_pay_order_num in db_order_numbers:
                    alreadybilledAmount += to_pay_for_order_amounts_float[
                        to_pay_order_index
                    ]
                    alreadyBilledOrderNumbers.append(to_pay_order_num)
                else:
                    notYetBilledAmount += to_pay_for_order_amounts_float[
                        to_pay_order_index
                    ]
                    notYetBilledOrderNumbers.append(to_pay_order_num)

                to_pay_order_index += 1

        retval = {
            "notYetBilledAmount": notYetBilledAmount,
            "alreadyBilledButFailedAmount": alreadybilledAmount,
            "alreadyBilledOrderNumbers_1": ",".join(alreadyBilledOrderNumbers[:40]),
            "notYetBilledOrderNumber_1": ",".join(notYetBilledOrderNumbers[:40]),
            "alreadyBilledOrderNumbers_2": ",".join(alreadyBilledOrderNumbers[41:81]),
            "notYetBilledOrderNumber_2": ",".join(notYetBilledOrderNumbers[41:81]),
            "alreadyBilledOrderNumbers_3": ",".join(alreadyBilledOrderNumbers[82:102]),
            "notYetBilledOrderNumber_3": ",".join(notYetBilledOrderNumbers[82:102]),
        }

        return retval

    def log_blank_lines(self, num):
        for i in range(0, num):
            logger.info("-------------------")


if __name__ == "__main__":
    d = DailyPaymentJob()

    d.process_new()

# 'order_id':
# 'recVL9Z0V9tCBPeKq'
# 'brand_id':
# 'recnOyNmC6mAIRRCm'
