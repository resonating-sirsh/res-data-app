import flask
from helper_functions import get_secret, to_snowflake, validate_signature

from res.utils import logger
from res.utils.flask.ResFlask import ResFlask

APP_NAME = "aftership-webhook-collector"
__version__ = 0.1


logger.info("Setting up flask app")
app = ResFlask(
    __name__,
    enable_trace_logging=True,
)


@app.route(f"/{APP_NAME}/webhook_test", methods=["POST"])
def webhook_test():
    # Print headers, print request json
    headers = flask.request.headers
    logger.info(headers)
    request = flask.request.get_json()
    logger.info(request)
    return {"success": True}, 200


@app.route(f"/{APP_NAME}/returns_webhook", methods=["POST"])
def handle_returns_webhook():
    # Check for signature in request header. The lack of this header means the
    # request did not come from Aftership and should be ignored
    try:
        logger.info("Received request")
        request_sig = flask.request.headers["as-signature-hmac-sha256"]
        request_raw_body = flask.request.get_data()

    except KeyError:
        logger.info(
            "This request is missing the correct HTTP header webhook signature value and likely did not originate from Aftership"
        )
        return ("Bad Request", 400)

    try:
        # Validate the signature using account(s) webhook secrets. The validation
        # function can return either a string (in case there is a validation match)
        # or a tuple (indicating an invalid signature)
        validation_obj = validate_signature(request_sig, request_raw_body)

        if isinstance(validation_obj, tuple):
            logger.info("401 Unauthorized; invalid signature")
            return validation_obj

        elif isinstance(validation_obj, str):
            account_name = validation_obj

        else:
            logger.error(
                f"Invalid account name datatype {type(validation_obj)} in handler webhook secret"
            )
            raise TypeError

        # Log event details
        request = flask.request.get_json()["data"]
        logger.info(request)

        # Add account name. Rename 'order' key as it's a Snowflake reserved word
        request["account_name"] = account_name
        request["order_info"] = request.pop("order")

        # Send to Snowflake
        to_snowflake(request, get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS"))

        return {"success": True}, 200

    except Exception as e:
        logger.error(e)
        return ("Handler Error", 500)
