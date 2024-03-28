import flask

from src.controller import create_nft, transfer_nft
from src.connectors.query_definition import CHANGING_STATUS
from src.connectors.graphql import validate_not_duplicate_product_request
from src.utils import DigitialProductRequestType, DigitalProductRequestStatus
from res.utils import logger, secrets
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.flask.ResFlask import ResFlask
from res.connectors.graphql.utils import HasuraEventType, from_hasura_event_payload

__version__ = 0.1

logger.info("Setting up flask app...")
app = ResFlask(
    __name__, enable_trace_logging=True, allowed_cors_domains=["https://create.one"]
)

with app.app_context():
    hasura_client = Client()
    res_graphql_client = ResGraphQLClient()
    secrets_key = [
        "MORALIS_NODE_URL",
        "WALLET_PRIVATE_KEY",
        "WALLET_ADDRESS",
        "CONTRACT_ADDRESS",
        "CONTRACT_ABI",
    ]
    for secret_key in secrets_key:
        secrets.secrets.get_secret(secret_key, force=False)


@app.route("/digital-product/webhook", methods=["POST"])
def webhook():
    event = flask.request.get_json()
    logger.info(event)
    if not event:
        return {"error": "Not data in body"}, 400

    record_created, operation, _ = from_hasura_event_payload(event)

    def change_status(status, reason=""):
        hasura_client.execute(
            CHANGING_STATUS,
            {"id": record_created["id"], "status": status, "reason": reason},
        )

    change_status(DigitalProductRequestStatus.PROCESSING)
    if operation == HasuraEventType.INSERT:
        valid = validate_not_duplicate_product_request(
            brand_id=record_created["brand_id"],
            type=record_created["type"],
            hasura_client=hasura_client,
            animation_id=record_created["animation_id"],
        )

        try:
            if not valid:
                change_status(
                    DigitalProductRequestStatus.ERROR,
                    "Duplicated request or NFT already mint",
                )
                return {"error": "Duplicated"}, 400

            if record_created["type"] == DigitialProductRequestType.CREATED:
                return create_nft(record_created, hasura_client, res_graphql_client)

            if record_created["type"] == DigitialProductRequestType.TRANSFER:
                return transfer_nft(record_created, hasura_client)

        except Exception as err:
            logger.error(
                f"Something went wrong during the processing of the requerst {record_created['id']}"
            )
            change_status(DigitalProductRequestStatus.ERROR, "Error on the server")
            logger.warn(err)
            return {"error": "Internal Error"}, 500

    return {"error": "Not match"}, 404


def test_auth():
    """
    Just dumb function to make sure that the module can be imported correctly
    """
    logger.info("must show in console")


if __name__ == "__main__":
    app.run(host="0.0.0.0")
