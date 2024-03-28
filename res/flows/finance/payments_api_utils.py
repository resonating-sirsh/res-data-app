import datetime
import inspect
import json
import traceback
from typing import Optional
from warnings import filterwarnings

import pandas as pd
import stripe
from fastapi import HTTPException

from schemas.pydantic.payments import *

# nudge
import res
from res.flows.finance import sql_queries
from res.utils import (
    logger,
    ping_slack,
    safe_json_dumps,
    safe_json_serialiser,
    secrets_client,
)
from res.utils.kafka_utils import message_produce

filterwarnings("ignore")

slack_id_to_notify = " <@U04HYBREM28> "

FULFILLMENT_BASE_ID = "appfaTObyfrmPHvHc"
RES_META_BASE_ID = "appc7VJXjoGsSOjmw"

stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
endpoint_secret_aws = secrets_client.get_secret("STRIPE_WEBHOOK_SECRET")

slack_channel_to_nofity = "payments_api_notification"


def datetime_converter(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {key: datetime_converter(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [datetime_converter(element) for element in obj]
    elif isinstance(obj, tuple):
        return tuple(datetime_converter(element) for element in obj)
    else:
        return obj


def insert_or_update_brand(
    brand: BrandCreateUpdate,
    is_update: bool,
    create_stripe_customer: bool = False,
    postgres=None,
):
    util_logger_and_slack(
        f"Processing function payment_api_utils.{inspect.currentframe().f_code.co_name}"
    )
    postgres = postgres or res.connectors.load("postgres")

    if brand.brand_code:
        q = "SELECT brand_code from sell.brands WHERE brand_code = %s "

        existing = not postgres.run_query(q, (brand.brand_code,)).empty

        filtered_dict = {
            k: v
            for k, v in brand.dict().items()
            if k not in ["brand_pkid", "active_subscription_id"]
        }
        filtered_dict["airtable_brand_id"] = filtered_dict["fulfill_record_id"]
        filtered_dict["airtable_brand_code"] = filtered_dict["brand_code"]

        if is_update:
            if existing:
                postgres.update_records(
                    "sell.brands", filtered_dict, {"brand_code": brand.brand_code}
                )

            else:
                return f"Error: brand code {brand.brand_code} does not exist to update"

        else:  # is insert
            if not existing:
                postgres.insert_records("sell.brands", [filtered_dict])
            else:
                return (
                    f"Brand: {brand.brand_code} already exists or brand code is empty"
                )

        if create_stripe_customer and not is_update:
            util_logger_and_slack(
                f"creating stripe customer for {brand.name} - code:{brand.brand_code}"
            )
            stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
            new_customer = stripe.Customer.create(
                description=f"{brand.name} -Code: {brand.brand_code}",
                name=brand.name,
                email=brand.register_brand_email_address,
                metadata={
                    "brand_code": brand.brand_code,
                    "contact_email": brand.contact_email,
                },
                address=brand.address,
            )
            util_logger_and_slack(
                f"created stripe customer: { res.utils.safe_json_dumps(new_customer.to_dict())})  "
            )

        return "OK"


def util_logger_and_slack(message):
    res.utils.logger.info(message)
    ping_slack(message, "payments_api_verbose")


def update_order(order_details: PaymentsGetOrUpdateOrder, postgres=None):
    res.utils.logger.info(
        f"Processing function payment_api_utils.{inspect.currentframe().f_code.co_name}"
    )
    postgres = postgres or res.connectors.load("postgres")
    order_line_item_id = None
    validate_inputs_update_order(order_details, postgres)
    if order_details.one_number:
        fulfillment_id = determine_fulfillment_id_from_ones(
            order_details.one_number, postgres
        )
        if not fulfillment_id:
            order_line_item_id = determine_order_line_item_id(
                order_details,
                postgres,
            )
            if not order_line_item_id:
                er_msg = f"ERROR: {order_details.one_number} one number was not found in postgres during attempt to save fulfillment details. Also failed to find fulfillment ID using SKUs and order num"
                ping_slack(
                    f"exception {slack_id_to_notify}{er_msg} {safe_json_dumps(order_details.dict())}",
                    "fulfillment_api_notification",
                )
                raise HTTPException(
                    status_code=404,
                    detail=er_msg,
                )

    if not order_line_item_id:
        order_line_item_id = determine_order_line_item_id(
            order_details,
            postgres,
        )

    order_dict = order_details.dict()
    if order_details.one_number:
        order_dict["order_item_fulfillment_id"] = fulfillment_id
    else:
        order_dict["order_item_fulfillment_id"] = None

    order_dict["metadata"] = json.dumps(order_dict["metadata"])
    order_dict["order_line_item_id"] = order_line_item_id

    order_dict["id"] = uuid_str_from_dict(datetime_converter(order_dict))

    postgres.run_update(sql_queries.INSERT_FULLFILMENT_STATUS_HISTORY, order_dict)

    res.utils.logger.info(
        f"Saved to postgres: {order_dict} in {inspect.currentframe().f_code.co_name}"
    )

    ping_slack(
        f"order:{order_dict.get('order_number')} one:{order_dict.get('one_number')} c_sku:{order_dict.get('current_sku')} o_sku:{order_dict.get('old_sku')} qty:{order_dict.get('quantity')} shipped_dt:{order_dict.get('shipped_datetime')}",
        "fulfillment_api_notification",
    )

    # Also save to bridge SKU to ONE for observability
    update_infraestructure_bridge_sku_one_counter(postgres, order_dict)

    return order_dict


def _map_products_stripe(raw_product):
    from datetime import datetime

    return {
        "id": raw_product.id,
        "description": raw_product.description,
        "name": raw_product.name,
        "createdAt": datetime.utcfromtimestamp(raw_product.created).isoformat(),
        "updatedAt": datetime.utcfromtimestamp(raw_product.updated).isoformat(),
    }


def stripe_load_product(id):
    products = stripe_load_products()
    product = next((p for p in products["stripeProducts"] if p["id"] == id), None)
    return product


def stripe_load_products():
    import stripe

    from res.utils import secrets_client

    stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
    products_data = stripe.Product.list(active=True, limit=200)
    products = [_map_products_stripe(product) for product in products_data.data]
    for product in products:
        prices = stripe.Price.list(
            limit=200, active=True, type="recurring", product=product["id"]
        )
        product["prices"] = [
            {"id": price.id, "amount": price.unit_amount} for price in prices.data
        ]
    return {"stripeProducts": products}


def stripe_webhook(payloadbody, stripe_signature, endpoint_secret, is_dev):
    """We check the signing to verify its from stripe and then hand off for processing"""
    try:

        # payload_dict and event are identical - the benefit of event is its gone thru signing to verify its def from stripe and not spoofed
        event = stripe.Webhook.construct_event(
            payloadbody, stripe_signature, endpoint_secret
        )

        util_logger_and_slack(
            f"stripe_webhook - stripe constructed event - {event.get('type')} is_dev: {is_dev}. event ID: {event.get('id')} "
        )
        res.utils.logger.info(f"full stripe event: {res.utils.safe_json_dumps(event)}")
    except ValueError as e:

        util_logger_and_slack(
            f"\n\nstripe_webhook error {slack_channel_to_nofity} \n\namepayload: {safe_json_dumps(payload_dict)} \n\nstacktrace:{traceback.format_exc()}"
        )
        raise e
    except stripe.error.SignatureVerificationError as e:
        payload_dict = json.loads(payloadbody.decode("utf-8"))
        util_logger_and_slack(
            f"\n\n\n\n\nstripe_webhook Error - Invalid Signature:  {slack_channel_to_nofity} \namepayload: {safe_json_dumps(payload_dict)} \nstacktrace:{traceback.format_exc()}"
        )
        raise e

    retval = process_stripe_valid_webhookevent(event, is_dev)
    return retval


def process_stripe_valid_webhookevent(event, is_dev):

    event["is_dev"] = is_dev
    event["full_json_dump_payload"] = json.dumps(event)
    event.pop("data")
    util_logger_and_slack(
        f"stripe_webhook - sending to res_finance.payments.stripe_webhook_payload - {event.get('type')} {event.get('id')} "
    )
    res.connectors.load("kafka")["res_finance.payments.stripe_webhook_payload"].publish(
        event, use_kgateway=True, coerce=True
    )

    return True


def get_customer_stripe_billing_session_url(stripe_customer_id: str, brand_code: str):
    import stripe

    res.utils.logger.info(
        f"in function: {inspect.currentframe().f_code.co_name} stripe customer id: {stripe_customer_id}"
    )

    if not stripe_customer_id or not stripe_customer_id.startswith("cus"):
        if not brand_code or len(brand_code) < 2:
            raise Exception(
                f"Brand Code not supplied and invalid stripe customer ID ({stripe_customer_id}) supplied. Cannot create billing session URL"
            )
        brand = get_brand(brand_code)
        if (
            brand is None
            or brand.stripe_customer_id is None
            or not brand.stripe_customer_id.startswith("cus")
        ):
            raise Exception(
                f"Invalid stripe customer ID in brands table: ({getattr(brand, 'stripe_customer_id', None)})"
            )

        stripe_customer_id = brand.stripe_customer_id

    stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
    session = stripe.billing_portal.Session.create(customer=stripe_customer_id)
    res.utils.logger.info(
        f"billing session response for {stripe_customer_id} : {session}"
    )

    res.utils.logger.metric_node_state_transition_incr(
        "None",
        "None",
        stripe_customer_id,
        flow="get_customer_stripe_billing_session_url",
        process="PaymentsAPI",
    )
    return session


def log_error_metrics(*args):
    list_args = list(args)
    while len(list_args) < 4:
        list_args.append("None")

    list_args = [str(i) for i in list_args]

    res.utils.logger.metric_node_state_transition_incr(
        list_args[1],
        list_args[2],
        list_args[3],
        flow=list_args[0],
        process="PaymentsAPI",
    )


def update_infraestructure_bridge_sku_one_counter(postgres, order_dict):
    if not order_dict.get("one_number"):
        res.utils.logger.info(
            "no one number supplied (likely fulfilled from inventory) so not possible to update infraestructure.bridge_sku_one_counter"
        )
        return

    res.utils.logger.info(
        f"attempting update_infraestructure_bridge_sku_one_counter: {order_dict.get('one_number')} order num: {order_dict.get('order_number')} "
    )
    postgres.run_update(sql_queries.UPDATE_BRIDGE_SKU_ONE_COUNTER, order_dict)


def determine_fulfillment_id_from_ones(one_number: int, postgres):
    df = postgres.run_query(
        sql_queries.GET_ORDER_ITEM_FULFILLMENT_ID_GIVEN_ONE_NUMBER,
        (one_number,),
        keep_conn_open=True,
    )
    if df.empty:
        res.utils.logger.error(
            f"{one_number} one number was not found during attempt to save fulfillment details."
        )

        log_error_metrics(inspect.currentframe().f_code.co_name, one_number)
        return None
    else:
        res.utils.logger.info(
            f"{one_number} was matched with this info from DB for fulfilment: {df.head(10).to_json(orient='records')}"
        )
        return df.iloc[0].oif_id


def determine_order_line_item_id(order_details: PaymentsGetOrUpdateOrder, postgres):
    df = postgres.run_query(
        sql_queries.GET_ORDER_LINE_ITEM_ID_GIVEN_ORDER_AND_SKU,
        (order_details.order_number, order_details.current_sku),
        keep_conn_open=True,
    )

    if df.empty:
        df = postgres.run_query(
            sql_queries.GET_ORDER_LINE_ITEM_ID_GIVEN_ORDER_AND_SKU,
            (order_details.order_number, order_details.old_sku),
            keep_conn_open=True,
        )

    # its possible that the one number supplied with the order number IS FOR A DIFFERNT order if fulfilled from inventory. SO we want to get the SKU for the one, then find that SKU + ORDER number combo
    if df.empty:
        df = postgres.run_query(
            sql_queries.GET_ORDER_LINE_ITEM_ID_GIVEN_ONE_NUMBER_AND_ORDER_NUM,
            (order_details.one_number, order_details.order_number),
            keep_conn_open=True,
        )

    if df.empty:
        er_msg = f"{order_details.order_number} order and currentsku: {order_details.current_sku} or oldsku:{order_details.old_sku} or one_number: or {order_details.one_number} was not found during attempt to save fulfillment details."
        res.utils.logger.error(er_msg)
        ping_slack(
            f" {slack_id_to_notify}  {er_msg}",
            "fulfillment_api_notification",
        )
        log_error_metrics(
            inspect.currentframe().f_code.co_name,
            order_details.order_number,
            order_details.current_sku,
            order_details.one_number,
        )
        return None

    else:
        res.utils.logger.info(
            f"{ order_details.order_number} order and { order_details.current_sku} was matched with this info from DB for fulfilment: {df.head(10).to_json(orient='records')}"
        )
        return df.iloc[0].id


def validate_inputs_update_order(order_details: PaymentsGetOrUpdateOrder, postgres):
    if not order_details.order_number:
        errMsg = (
            f"Order number was not supplied. It is a required field.  {order_details}"
        )
        res.utils.logger.error(errMsg)
        ping_slack(f" {slack_id_to_notify}  {errMsg}", "fulfillment_api_notification")
        raise HTTPException(status_code=500, detail=errMsg)

    if order_details.one_number:
        pass
    else:
        if not (order_details.current_sku and order_details.quantity):
            errMsg = f"a one number was not supplied, so you must supply all of: sku, quantity and order number {order_details}"
            ping_slack(
                f" {slack_id_to_notify}  {errMsg}", "fulfillment_api_notification"
            )

            res.utils.logger.error(errMsg)
            raise HTTPException(status_code=500, detail=errMsg)

    if (
        order_details.current_sku
        and order_details.order_number
        and order_details.quantity
    ):
        verify_sku_order_number_exists(order_details, postgres)


def verify_sku_order_number_exists(order_details: PaymentsGetOrUpdateOrder, postgres):
    df = postgres.run_query(
        sql_queries.GET_SELL_ORDER_ITEM_GIVEN_SKU_ORDERNUMBER,
        (order_details.current_sku, order_details.order_number),
        keep_conn_open=True,
    )

    errMsg = None
    if df.empty:
        res.utils.logger.info(
            f"current_sku: {order_details.current_sku} and {order_details.order_number} found no entries"
        )
        if order_details.old_sku is not None:
            res.utils.logger.info(
                f"old_sku: {order_details.old_sku} was supplied so will search against that for order {order_details.order_number} "
            )
            df = postgres.run_query(
                sql_queries.GET_SELL_ORDER_ITEM_GIVEN_SKU_ORDERNUMBER,
                (order_details.old_sku, order_details.order_number),
                keep_conn_open=True,
            )

    if df.empty:
        errMsg = f"SKU:{order_details.current_sku} nor old_sku: {order_details.old_sku } on order:{order_details.order_number} not found in sell.order_line_items.  Cannot update shipped info"
    else:
        if df.iloc[0]["quantity"] < order_details.quantity:
            errMsg = f"SKU:{order_details.current_sku}  exists at quantity:{df.iloc[0]['quantity']} which is less than supplied {order_details.quantity} on order:{order_details.order_number}.  Cannot update shipped info"

    if errMsg:
        res.utils.logger.error(errMsg)
        ping_slack(f" {slack_id_to_notify}  {errMsg}", "fulfillment_api_notification")
        raise HTTPException(status_code=500, detail=errMsg)


def get_brand(brand_code: str, postgres=None):
    res.utils.logger.info(
        f"Processing function payment_api_utils.{inspect.currentframe().f_code.co_name}"
    )

    postgres = postgres or res.connectors.load("postgres")

    brandsdf = postgres.run_query(sql_queries.GET_BRAND_BY_BRANDCODE, (brand_code,))
    brand_dict = dict()
    b = None
    if not brandsdf.empty:
        brand_dict = brandsdf.to_dict("records")[0]
        b = BrandAndSubscription(**brand_dict)

    return b


def get_transactions(
    start_date: datetime.date = None,
    end_date: datetime.date = None,
    postgres=None,
    by_brand_code: Optional[str] = None,
):
    res.utils.logger.info(
        f"Processing function payment_api_utils.{inspect.currentframe().f_code.co_name}"
    )
    postgres = postgres or res.connectors.load("postgres")

    query = sql_queries.GET_TRANSACTIONS_BASE_QUERY

    params = []

    query_addition = ""

    if not start_date:
        start_date = datetime.date(2015, 1, 1)

    if not end_date:
        end_date = datetime.date(2099, 1, 1)

    if by_brand_code and len(by_brand_code) > 1:
        query_addition += f"""       
        sb.brand_code = %s
        AND 
        """
        params.append(by_brand_code)

    query_addition += f"""
    ST.created_at >= %s AND 
    ST.created_at <= %s

    """
    params.append(str(start_date))
    params.append(str(end_date))

    query = query.format(query_addition)
    # add alias exec and exec with kwargs, like hasura conn
    df = postgres.run_query(query, params)

    df = df.sort_values("created_at", ascending=False)

    transactions = [PaymentsTransaction(**row) for _, row in df.iterrows()]

    return transactions


def verify_res_payments_api_key(key: str):
    RES_PAYMENTS_API_KEY = secrets_client.get_secret("RES_PAYMENTS_API_KEY").get(
        "RES_PAYMENTS_API_KEY"
    )

    if key == RES_PAYMENTS_API_KEY:
        return True
    else:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid RES_PAYMENTS_API_KEY.  Please refresh this value from AWS secrets manager",
        )
        return False


# if __name__ == "__main__":
#     evt = """{
#   "id": "evt_3OqKajA5RKqObLFc1bJKEuOI",
#   "object": "event",
#   "api_version": "2020-08-27",
#   "created": 1709493670,
#   "data": {
#     "object": {
#       "id": "pi_3OqKajA5RKqObLFc1nmKPMhK",
#       "object": "payment_intent",
#       "amount": 900,
#       "amount_capturable": 0,
#       "amount_details": {
#         "tip": {
#         }
#       },
#       "amount_received": 900,
#       "application": null,
#       "application_fee_amount": null,
#       "automatic_payment_methods": null,
#       "canceled_at": null,
#       "cancellation_reason": null,
#       "capture_method": "automatic",
#       "charges": {
#         "object": "list",
#         "data": [
#           {
#             "id": "py_3OqKajA5RKqObLFc123UN3OO",
#             "object": "charge",
#             "amount": 900,
#             "amount_captured": 900,
#             "amount_refunded": 0,
#             "application": null,
#             "application_fee": null,
#             "application_fee_amount": null,
#             "balance_transaction": "txn_3OqKajA5RKqObLFc1I35AkOh",
#             "billing_details": {
#               "address": {
#                 "city": null,
#                 "country": null,
#                 "line1": null,
#                 "line2": null,
#                 "postal_code": null,
#                 "state": null
#               },
#               "email": "1909damo@examle.com",
#               "name": "1909damo",
#               "phone": null
#             },
#             "calculated_statement_descriptor": null,
#             "captured": true,
#             "created": 1709493657,
#             "currency": "usd",
#             "customer": "cus_PffknMyF9SiKyM",
#             "description": "Charge for customer cus_PffknMyF9SiKyM",
#             "destination": null,
#             "dispute": null,
#             "disputed": false,
#             "failure_balance_transaction": null,
#             "failure_code": null,
#             "failure_message": null,
#             "fraud_details": {
#             },
#             "invoice": null,
#             "livemode": false,
#             "metadata": {
#             },
#             "on_behalf_of": null,
#             "order": null,
#             "outcome": {
#               "network_status": "approved_by_network",
#               "reason": null,
#               "risk_level": "not_assessed",
#               "seller_message": "Payment complete.",
#               "type": "authorized"
#             },
#             "paid": true,
#             "payment_intent": "pi_3OqKajA5RKqObLFc1nmKPMhK",
#             "payment_method": "pm_1OqKQBA5RKqObLFc2bfTI4X6",
#             "payment_method_details": {
#               "type": "us_bank_account",
#               "us_bank_account": {
#                 "account_holder_type": "individual",
#                 "account_type": "checking",
#                 "bank_name": "STRIPE TEST BANK",
#                 "fingerprint": "dfRxgCyrQnL5dsRc",
#                 "last4": "6789",
#                 "routing_number": "110000000"
#               }
#             },
#             "radar_options": {
#             },
#             "receipt_email": "B1855@example.com",
#             "receipt_number": null,
#             "receipt_url": "https://pay.stripe.com/receipts/payment/CAcaFwoVYWNjdF8xSnR4ZVFBNVJLcU9iTEZjKKabk68GMgZVjb-ppWo6LBYpU49ZsaOfSEy-cKvexbi0JlwYwOwTHxBniNc6lD07ehRktPGcSQlcGwG2",
#             "refunded": false,
#             "refunds": {
#               "object": "list",
#               "data": [
#               ],
#               "has_more": false,
#               "total_count": 0,
#               "url": "/v1/charges/py_3OqKajA5RKqObLFc123UN3OO/refunds"
#             },
#             "review": null,
#             "shipping": null,
#             "source": null,
#             "source_transfer": null,
#             "statement_descriptor": null,
#             "statement_descriptor_suffix": null,
#             "status": "succeeded",
#             "transfer_data": null,
#             "transfer_group": null
#           }
#         ],
#         "has_more": false,
#         "total_count": 1,
#         "url": "/v1/charges?payment_intent=pi_3OqKajA5RKqObLFc1nmKPMhK"
#       },
#       "client_secret": "pi_3OqKajA5RKqObLFc1nmKPMhK_secret_sa5EiQ2aKtFN3aaMEsyV29Yyc",
#       "confirmation_method": "automatic",
#       "created": 1709493657,
#       "currency": "usd",
#       "customer": "cus_PffknMyF9SiKyM",
#       "description": "Charge for customer cus_PffknMyF9SiKyM",
#       "invoice": null,
#       "last_payment_error": null,
#       "latest_charge": "py_3OqKajA5RKqObLFc123UN3OO",
#       "livemode": false,
#       "metadata": {
#       },
#       "next_action": null,
#       "on_behalf_of": null,
#       "payment_method": "pm_1OqKQBA5RKqObLFc2bfTI4X6",
#       "payment_method_configuration_details": null,
#       "payment_method_options": {
#         "us_bank_account": {
#           "mandate_options": {
#           },
#           "verification_method": "automatic"
#         }
#       },
#       "payment_method_types": [
#         "us_bank_account"
#       ],
#       "processing": null,
#       "receipt_email": "B1855@example.com",
#       "review": null,
#       "setup_future_usage": null,
#       "shipping": null,
#       "source": null,
#       "statement_descriptor": null,
#       "statement_descriptor_suffix": null,
#       "status": "succeeded",
#       "transfer_data": null,
#       "transfer_group": null
#     }
#   },
#   "livemode": false,
#   "pending_webhooks": 1,
#   "request": {
#     "id": null,
#     "idempotency_key": null
#   },
#   "type": "payment_intent.succeeded"
# }"""

# # event = json.loads(evt)
# # process_stripe_valid_webhookevent(event, True)
