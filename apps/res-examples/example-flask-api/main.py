import os, json
from res.utils import logger
from res.utils.flask.ResFlask import ResFlask
from flask import request, Response
from flask_cognito import cognito_auth_required

__version__ = 0.1

logger.info("Setting up flask app...")
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


# Setting an unauthenticated route. Be very careful exposing unauthenticated routes to the open internet!!!
### NOTE: every route starts with the name of the app, so you can access via https://datadev.resmagic.io/my-app-name/my-route
@app.route("/example-flask-api/test", methods=["GET"])
def test():
    # Handle event
    logger.info("Handing test event (no authentication)!")
    return {"success": True}, 200


# Below shows an authenticated route. User must be logged in to create.ONE via
# Google login to access. Remove the `@cognito_auth_required` to remove authentication
@app.route("/example-flask-api/test-auth", methods=["GET", "POST"])
@cognito_auth_required
def test_auth():
    # Handle event
    logger.info("Handing test event!")
    return {"success": True}, 200


if __name__ == "__main__":
    app.run(host="0.0.0.0")
