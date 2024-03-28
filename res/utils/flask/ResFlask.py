import os
from flask import Flask, request, Response
import res.utils.flask.helpers as flask_helpers
from res.utils import logger

# SA Note: apps import these sometimes without them being on docker images e.g. res-data-lite / OR app needs to install in its own docker image
from flask_cognito import CognitoAuth, cognito_auth_required
from flask_cors import CORS


def ResFlask(
    __name__, enable_trace_logging=False, allowed_cors_domains=[], enable_cognito=True
):
    app = Flask(__name__)
    # Set up cors for all routes, for specified domains
    if len(allowed_cors_domains) > 0:
        CORS(app, origins=allowed_cors_domains)

    # Set up metrics + tracing
    flask_helpers.attach_metrics_exporter(app)
    if enable_trace_logging:
        flask_helpers.enable_trace_logging(app)

    # Set up Cognito user pool for authentication
    if enable_cognito:
        userpool = os.getenv("COGNITO_USERPOOL_ID", "")
        client_id = os.getenv("COGNITO_APP_CLIENT_ID", "")
        try:
            app.config.update(
                COGNITO_REGION="us-east-1",
                COGNITO_USERPOOL_ID=userpool,
                COGNITO_APP_CLIENT_ID=client_id,
            )
            CognitoAuth(app)
        except Exception as e:
            logger.warn(
                f"Error setting the cognito user pools! You may need to set env vars COGNITO_USERPOOL_ID and COGNITO_APP_CLIENT_ID. {e}"
            )

    # Basic healthcheck route
    @app.route("/healthcheck", methods=["GET"])
    def healthcheck():
        return Response("ok", status=200)

    return app
