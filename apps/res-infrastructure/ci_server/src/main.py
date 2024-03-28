import os, json, xmltodict, jsonschema
from res.utils import logger, secrets_client
from res.utils.flask import helpers as flask_helpers
from flask import Flask, request, Response, abort, g
import requests
from functools import wraps
import time

app = Flask(__name__)
flask_helpers.attach_metrics_exporter(app)
flask_helpers.enable_trace_logging(app)

# API Key Decorator
def require_appkey(view_function):
    @wraps(view_function)
    def decorated_function(*args, **kwargs):
        api_key = secrets_client.get_secret("GITHUB_CI_SERVER_API_KEY")
        if (
            request.json
            and request.json.get("api_key")
            and request.json.get("api_key") == api_key
        ):
            return view_function(*args, **kwargs)
        else:
            logger.warn("Invalid API Key Sent!")
            abort(401)

    return decorated_function


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return Response("ok", status=200)


# Used by Github Actions to upgrade Avro schemas
@app.route("/ci-server/update-avro-schema", methods=["POST"])
@require_appkey
def update_avro_schema():
    # Validate Payload
    request_data = request.json
    if "subject" not in request_data or "schema" not in request_data:
        logger.warn("Bad payload sent to CI Server: missing subject or schema")
        return Response("Payload must include subject and schema", status=400)
    required_postfix = "-value"
    if request_data["subject"][-len(required_postfix) :] != required_postfix:
        logger.warn("Bad subject name sent to CI Server, missing -value")
        return Response("Subject name must have -value at the end", status=400)

    # Build Schema Registry Payload
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    subject_name = request_data["subject"]
    schema = {"schema": request_data["schema"]}
    url = "http://{}/subjects/{}/versions".format(
        os.getenv("KAFKA_SCHEMA_REGISTRY_URL"), subject_name
    )

    # Update Schema
    result = requests.post(url, json=schema, headers=headers)
    return Response(json.dumps(result.json()), status=result.status_code)


# Used by Github Actions to upgrade GraphQL schemas
@app.route("/ci-server/update-graphql-schema", methods=["POST"])
@require_appkey
def update_graphql_schema():
    return Response("not_implemented", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
