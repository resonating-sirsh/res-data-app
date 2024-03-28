import flask
import json
from src.actions import CreateBrandAction

from res.utils import logger
from res.utils.flask.ResFlask import ResFlask
from res.utils.stripe_auth import ResStripeAuth

__version__ = 0.1


logger.info("Setting up flask app....")
# ResFlask is a wrapper around Flask. It sets up:
#   - metrics exporting, so we can view data in Grafana
#   - trace logging, so we can trace calls thru the stack
#   - CORS so we can call the app from other websites
#         > specify each domain you want to allow requests from
#   - Authentication via Cognito User Pools
app = ResFlask(
    __name__,
    enable_trace_logging=True,
    allowed_cors_domains=[
        "https://create.one",
    ],
)

authenticator = ResStripeAuth()


@app.route("/create", methods=["POST"])
def create_brand():
    try:
        client_secret = flask.request.headers.get("client_secret")
        subscription_validation_amount = flask.request.headers.get(
            "subscription_validation_amount"
        )
        subscription_validation_status = flask.request.headers.get(
            "subscription_validation_status"
        )
        subscription_id = flask.request.headers.get("subscription_id")

        authenticator.authenticate_with_stripe(client_secret)
        authenticator.authenticate_stripe_subscription(
            subscription_id,
            subscription_validation_amount,
            subscription_validation_status,
        )

    except Exception as error:
        logger.error(error)

    try:
        action = flask.request.get_json()
        if not action:
            return {"error": "No data on body"}
        logger.debug(action)
        input = json.loads(action["input"]["input"])

        logger.debug(input["brand"])

        actions = CreateBrandAction(input)
        actions.create_brand_record()
        actions.create_brand_address()
        actions.create_user_record()
        actions.create_shopify_data_source_instance()
        actions.sync_brand_with_meta()
        actions.update_brand_label()
        actions.create_brand_space()
        actions.create_style()
        actions.create_style_space()
        return {"data": actions.brand}, 200

    except Exception as error:
        logger.error(error)
        return {"error": "Something fail on create brand flow"}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0")
