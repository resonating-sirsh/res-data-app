from http import HTTPStatus

from flask import request
from flask_cognito import cognito_auth_required
from src.shopify.views.publications import publication_routes
from src.shopify.views.media import media_routes

from res.connectors.graphql.ResGraphQLClient import GraphQLException, ResGraphQLClient
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.shopify.utils.ShopifyProduct import GET_PRODUCT, ShopifyProduct
from res.utils import logger
from res.utils.flask.ResFlask import ResFlask

__version__ = 0.1

KAFKA_TOPIC = "res_sell.shopify_product_live.live_products"

logger.info("Setting up flask app...")
app = ResFlask(
    __name__,
    enable_trace_logging=True,
    allowed_cors_domains=[
        "https://create.one",
    ],
)

app.register_blueprint(publication_routes)
app.register_blueprint(media_routes)


@app.route("/product-api/check-liveness", methods=["POST"])
@cognito_auth_required
def check_liveness():
    """Check if a Product is live on the brand site on Shopify
    POST /check-liveness
    Headers
        Authorization: Bearer <JWT>
        Content-Type: application/json
    Body
        { "productId": "<product-id>"}
    """
    graphql_client = ResGraphQLClient()
    body = request.get_json()
    if not body:
        return {"error": "Body is empty"}, HTTPStatus.BAD_REQUEST
    product_id = str(body.get("productId"))
    if not product_id:
        return {"error": "'productId' is required"}, HTTPStatus.UNPROCESSABLE_ENTITY
    try:
        response = graphql_client.query(GET_PRODUCT, {"id": product_id})
        product = response["data"]["product"]

        with ShopifyProduct(brand_code=product["storeCode"]) as instance:
            _, client = instance
            status = client.check_product_is_live(product["id"])
            logger.info(f"product {product_id} liveness status {status}")
            kafka_client = ResKafkaClient()
            with ResKafkaProducer(kafka_client, KAFKA_TOPIC) as producer:
                producer.produce({"id": product_id, "live": status})

            return {"data": status}, HTTPStatus.OK
    except GraphQLException:
        logger.error(
            f"fail check-live-status of product{product_id} - GraphQLException"
        )
        return {
            "error": "Problems requesting data from module 'graph-api'"
        }, HTTPStatus.INTERNAL_SERVER_ERROR
    except Exception as e:
        logger.error(f"fail check-live-status of product{product_id} unhandle", e)
        return {"error": "Something fail"}, HTTPStatus.INTERNAL_SERVER_ERROR


if __name__ == "__main__":
    app.run(host="0.0.0.0")
