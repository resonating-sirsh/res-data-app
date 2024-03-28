from flask import Flask, jsonify, request
from res.utils import logger, secrets_client
import stripe
from flask_cors import CORS
import os
from res.utils.kafka_utils import message_produce


absolute_path = os.path.abspath(__file__)

app = Flask(__name__)
CORS(app)

stripe.api_key = secrets_client.get_secret("STRIPE_API_SECRET_KEY")
endpoint_secret = secrets_client.get_secret("STRIPE_WEBHOOK_SECRET")

GET_TRANSACTION = """
query getTransaction(
  $stripe_charge_id: String!
) {
  sell_transactions(where: {stripe_charge_id: {_eq: $stripe_charge_id}}) {
    id
    stripe_charge_id
    amount
    subscription
    created_at
    updated_at
  }
}
"""


@app.route("/stripe-webhooks/healthcheck")
def healthcheck():
    return {"status": "ok."}


@app.route("/stripe-webhooks/stripe-webhooks", methods=["POST"])
def process_webhook_event():
    payload = request.data
    logger.info(payload)
    # print(payload)
    sig_header = request.headers.get("STRIPE_SIGNATURE")
    # print(sig_header)
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, endpoint_secret)
        # print(event)
    except ValueError as e:
        # Invalid payload
        logger.error(e)
        raise e
    except stripe.error.SignatureVerificationError as e:
        # Invalid signature
        print("Bad Stripe Signature!!")
        logger.error(e)
        raise e

    event_data = event["data"]["object"]
    print(event_data["customer"])
    if event.get("type") == "payment_intent.succeeded":

        charge_data = event_data["charges"]["data"][0]
        print(charge_data)
        if charge_data["customer"]:
            temp_message = {
                "amount": event_data["amount"],
                "type": "debit",
                "source": "subscription_recharge",
                "reference_id": event_data["id"],
                "stripe_charge_id": charge_data["id"],
                "receipt_url": charge_data["receipt_url"],
                "currency": "usd",
                "description": "Subscription payment",
                "customer_stripe_id": charge_data["customer"],
                "details": [],
                "is_failed_transaction": False,
            }

            logger.info("message")
            logger.info(temp_message)
            invoice = {}
            logger.info("Invoice")
            logger.info(charge_data["invoice"])

            if charge_data["invoice"] != None:
                invoice = stripe.Invoice.retrieve(
                    charge_data["invoice"],
                )
                invoice_item_type = ""
                if len(invoice.get("lines").get("data")) > 0:
                    invoice_item_type = invoice["lines"]["data"][0]["type"]

                temp_message["transaction_status"] = charge_data["status"]
                temp_message["transaction_error_message"] = charge_data[
                    "failure_message"
                ]

                if (
                    charge_data["status"] == "succeeded"
                    and charge_data["paid"]
                    and invoice_item_type == "subscription"
                ):
                    ##Done: Add is_failed_transaction
                    temp_message["is_failed_transaction"] = False
                else:
                    temp_message["is_failed_transaction"] = True

                if invoice_item_type == "subscription":
                    message_produce(temp_message, "res_sell.subscription.transaction")

        else:
            print("No customer subscription.")
            logger.info("No customer subscription or charge is direct...")

    else:
        print("Unhandled event type {}".format(event["type"]))

    return jsonify(success=True)


if __name__ == "__main__":
    print("Full path: " + absolute_path)
    app.run(host="0.0.0.0")
