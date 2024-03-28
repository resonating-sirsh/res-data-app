import os
import flask

from res.utils import logger
from res.utils.flask.ResFlask import ResFlask
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import src.controller as controller

APP_NAME = "ecommerce-collection-app"


__version__ = 0.1

logger.info("Setting up flask app...")
app = ResFlask(
    __name__,
    enable_trace_logging=True,
    allowed_cors_domains=[
        os.environ.get("URL_DOMAIN", "https://create.one"),
    ],
)

with app.app_context():
    hasura_client = Client()
    graphql_client = ResGraphQLClient()


@app.route(f"/{APP_NAME}/publish", methods=["POST"])
def publish_collection_in_ecommerce():
    request = flask.request.get_json()
    if not request:
        return {"error": "No allow request"}, 422
    logger.debug(request)
    collection_id = request["input"]["id"]
    response = controller.publish_collection_in_ecommerce(collection_id, hasura_client)
    return response


@app.route(f"/{APP_NAME}/unpublish", methods=["POST"])
def unpublish_collection_in_ecommercer():
    request = flask.request.get_json()
    if not request:
        return {"error": "No allow request"}, 422
    logger.debug(request)
    collection_id = request["input"]["id"]

    response = controller.unpublish_collection_in_ecommerce(
        collection_id, hasura_client
    )
    return response


@app.route(f"/{APP_NAME}/webhook", methods=["POST"])
def webhook():
    request = flask.request.get_json()
    # pre-condition
    if not request:
        return {"error": "Not data in body"}, 422

    collection, operation = get_from_event(request)
    collection_id = collection["id"]
    collection_status = collection["status"]

    if operation == "INSERT":
        return controller.export_collection_to_ecommerce(
            collection_id, collection, hasura_client, graphql_client
        )

    elif operation == "UPDATE":

        if collection_status == "delete":
            return controller.delete_collection_from_ecommerce(
                collection_id, collection, hasura_client
            )

        if collection_status == "created":
            return controller.export_collection_to_ecommerce(
                collection_id, collection, hasura_client, graphql_client
            )

        return controller.update_collection_in_ecommerce(
            collection_id, collection, hasura_client, graphql_client
        )

    elif operation == "DELETE":

        if collection_status == "delete":
            return {"error": "This collection is already delete in ecommerce"}, 422

        return controller.delete_collection_from_ecommerce(
            collection_id, collection, hasura_client
        )

    return {"error": "Not op"}, 422


@app.route(f"/{APP_NAME}", methods=["GET"])
def init():
    return {"success": "This is a test", "ok": True}, 200


def get_from_event(json_request):
    event = json_request["event"]
    metadata = event["session_variables"]
    operation = event["op"]
    logger.info(f"Event received event {metadata}, OPERATION: {operation}")
    logger.debug(event)
    changes = event["data"]
    old_changes = changes["old"]
    new_changes = changes["new"]
    logger.info(f"\nold: {old_changes}\n\nnew: {new_changes}")
    return new_changes, operation


def draft_collection_in_ecommerce():
    pass


if __name__ == "__main__":
    app.run(host="0.0.0.0")
