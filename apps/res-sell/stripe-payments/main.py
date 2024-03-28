import json
from http import HTTPStatus
from typing import Any, Optional

from flask import jsonify, request
from flask_cognito import cognito_auth_required
from pydantic import BaseModel
from src.actions import (
    CreateSubscription,
    GetChargeErrors,
    PaymentsHelper,
    TransactionReTriggerPayment,
)

from res.connectors.stripe.error import StripeErrorCodes, StripeException
from res.utils import logger
from res.utils.flask.ResFlask import ResFlask

__version__ = 0.1


logger.info("Setting up flask app.....")
# ResFlask is a wrapper around Flask. It sets up:
#   - metrics exporting, so we can view data in Grafana
#   - trace logging, so we can trace calls thru the stack
#   - CORS so we can call the app from other websites
#         > specify each domain you want to allow requests from
#   - Authentication via Cognito User Pools ..
app = ResFlask(
    __name__,
    enable_trace_logging=True,
    allowed_cors_domains=["*"],
)


with app.app_context():
    stripe = PaymentsHelper()


@app.route("/stripe-payments/confirm-setup")
def confirm_setup():
    client_secret = request.args.get("setup_intent_client_secret")
    result = stripe.confirm_setup(client_secret)
    return "Done" if result else "Failed"


@cognito_auth_required
@app.route("/stripe-payments/create-secret", methods=["POST"])
def create_secret():
    try:
        # data = json.loads(request.data)
        # print(data)
        intent = stripe.create_secret()
        return jsonify(clientSecret=intent.client_secret)
    except Exception as error:
        return jsonify(error=str(error)), 403


# Content-Type: application/json
# @cognito_auth_required
@app.route("/stripe-payments/setup-brand-payment", methods=["POST"])
def setup_payment():
    try:
        logger.info("Brand setup start.")
        logger.info(request.data)
        # logger.info(json.loads(request.data))
        action = json.loads(request.data)
        logger.info(action)

        if not action:
            return {"error": "No data on body", "action": action, "request": request}
        logger.debug(action)
        input = action["input"]
        logger.info(input)
        logger.info("input")

        actions = CreateSubscription(input)
        result = actions.create()
        return {"data": result}, 200

    except Exception as error:
        logger.error(error)
        return {"error": "Something fail on create brand flow"}, 500


@cognito_auth_required
##TODO: Figure how we use this.
@app.route("/stripe-payments/get-errors", methods=["POST"])
def get_errors():
    try:
        logger.info("Get erors start.")
        action = json.loads(request.data)
        if not action or not action.get("stripe_customer_id"):
            return {
                "error": "No data on body, missing stripe_customer_id.",
                "action": action,
                "request": request,
            }
        erros = GetChargeErrors(action["stripe_customer_id"])

        return {"data": erros}, 200

    except Exception as error:
        logger.warning(error)
        return {"error": "Something fail on errors service..."}, 500


class TriggerTransationErrorResponse(BaseModel):
    code: Optional[str]
    message: Optional[str]


class TriggerTransactionResponse(BaseModel):
    valid: Optional[bool]
    reason: Optional[str]
    action: Optional[Any] = None
    error: Optional[TriggerTransationErrorResponse] = None


@cognito_auth_required
@app.route("/stripe-payments/trigger-transaction", methods=["POST"])
def trigger_transaction():
    try:
        transaction_payments = TransactionReTriggerPayment()
        action = request.get_json()
        if not action or not action.get("transaction_id"):
            return {
                "error": "No data on body, missing transaction_id."
            }, HTTPStatus.BAD_REQUEST
        result = transaction_payments.process(action)
        response = TriggerTransactionResponse(
            valid=result["valid"],
            reason="Transaction triggered successfully",
            data=result.get("data"),
            error=None,
        )
        response = response.dict()
        del response["error"]
        return {"data": response}, HTTPStatus.OK
    except StripeException as error:
        response = TriggerTransactionResponse(
            valid=False,
            reason="StripeException: No handle",
        )
        httpStatus: HTTPStatus = HTTPStatus.INTERNAL_SERVER_ERROR

        if error.code == StripeErrorCodes.CARD_DECLINE:
            response.reason = "Card was declined"
            response.error = TriggerTransationErrorResponse(
                message=response.reason, code=error.code.value
            )
            httpStatus = HTTPStatus.BAD_REQUEST

        if error.code == StripeErrorCodes.INSUFFICIENT_FUNDS:
            response.reason = "Insufficient funds to fulfill the transaction"
            response.error = TriggerTransationErrorResponse(
                message=response.reason, code=error.code.value
            )
            httpStatus = HTTPStatus.BAD_REQUEST

        if error.code == StripeErrorCodes.EXPIRED_CARD:
            response.reason = "Card has expired"
            response.error = TriggerTransationErrorResponse(
                message=response.reason, code=error.code.value
            )
            httpStatus = HTTPStatus.BAD_REQUEST

        response = response.dict()
        del response["action"]
        logger.debug(f"Error response: {response} - http_status: {httpStatus}")
        return {"error": response}, httpStatus
    except Exception as error:
        logger.error(error, exception=error, exception_type=type(error))
        response = TriggerTransactionResponse(
            valid=False,
            reason="Exception: No handle",
            error=TriggerTransationErrorResponse(message=str(error), code="UNKNOWN"),
        )
        response = response.dict()
        del response["action"]
        logger.debug(
            f"Error in requests: {response} / {HTTPStatus.INTERNAL_SERVER_ERROR.value}"
        )
        return {
            "error": response,
        }, HTTPStatus.INTERNAL_SERVER_ERROR


if __name__ == "__main__":
    app.run(host="0.0.0.0")
