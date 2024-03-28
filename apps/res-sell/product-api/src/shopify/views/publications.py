from functools import wraps
from http import HTTPStatus

import flask
import pydantic
from flask_cognito import cognito_auth_required
from src.graph_api.queries.products import GET_PRODUCT
from src.shopify.controllers.publication import ShopifyPublication
from src.shopify.models.publication import PublicationInput, PublishResourceInput

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger

publication_routes = flask.Blueprint("publication", __name__)


def validate_publication_request(f):
    @wraps(f)
    def __validator():
        body_json = flask.request.json

        if not body_json:
            return {"error": "body is empty"}, HTTPStatus.BAD_REQUEST

        try:
            payload = PublishResourceInput(
                resourceId=body_json["resourceId"],
                resourceType=body_json["resourceType"],
                publications=[
                    PublicationInput(
                        publicationId=publication["publicationId"],
                        # Using the Dict.get because the publicationDate is optional value
                        publishDate=publication.get("publishDate"),
                    )
                    for publication in body_json["publications"]
                ],
            )
        except pydantic.error_wrappers.ValidationError as error:
            errors = [error["msg"] for error in error.errors()]
            fields = list({error["loc"] for error in error.errors()})
            return {
                "error": "Validation error",
                "errors": errors,
                "fields": fields,
            }, HTTPStatus.BAD_REQUEST
        except KeyError as err:
            return {"error": f"Missing propery '{err}' in body"}, HTTPStatus.BAD_REQUEST

        sp_resource_id = ""
        store_code = ""

        if payload.resourceType == "product":
            graphql_client = ResGraphQLClient()
            response = graphql_client.query(GET_PRODUCT, {"id": payload.resourceId})
            if "data" not in response:
                return {
                    "error": "Error trying to retreving the product from the create.ONE"
                }, HTTPStatus.INTERNAL_SERVER_ERROR
            product_id = response["data"]["product"]["ecommerceId"]
            store_code = response["data"]["product"]["storeCode"]
            if not product_id:
                return {
                    "error": "Product is not in ecommerce, make sure that the product has been imported first"
                }, HTTPStatus.BAD_REQUEST
            sp_resource_id = f"gid://shopify/Product/{product_id}"

        elif payload.resourceType == "collection":
            # Not needed for now but we can used later, just leave here if we need it before.
            return {
                "error": "'collection' is not supported yet"
            }, HTTPStatus.UNPROCESSABLE_ENTITY

        else:
            return {
                "error": "'resourceType' must be either 'product' | 'collection' "
            }, HTTPStatus.BAD_REQUEST

        return f(store_code, sp_resource_id, payload)

    return __validator


@publication_routes.post("/product-api/publications")
@cognito_auth_required
@validate_publication_request
def publish_product(
    store_code: str, sp_resource_id: str, payload: PublishResourceInput
):
    """
    Publish an create.ONE resource (product | collection) to Shopify Publication/Channel

    Payload Shape:
        resourceId: ID -> Resource that you want to publish
        resourceType: "product" | "collection"
        publications: {
            "publicationId": ID,
            "publishDate": Date | None
        }[]

    """
    try:
        with ShopifyPublication(brand_code=store_code) as (_, self):
            response, error = self.publish_publishable_resource(
                id=sp_resource_id, input=payload.publications
            )

            if error:
                return {
                    "error": "Fail to comunicate with Shopify",
                    "message": error.args[0],
                }, HTTPStatus.INTERNAL_SERVER_ERROR

            return {**response, "create_one_id": payload.resourceId}, HTTPStatus.OK
    except Exception as error:
        logger.error("error on publish resource", error)
        return {"error": "Internal Server Error"}, HTTPStatus.INTERNAL_SERVER_ERROR


@publication_routes.delete("/product-api/publications")
@cognito_auth_required
@validate_publication_request
def unpublish_product(
    store_code: str, sp_resource_id: str, payload: PublishResourceInput
):
    """
    Unpublish an create.ONE resource (product | collection) to Shopify Publication/Channel

    Payload Shape:
        resourceId: ID -> Resource that you want to publish
        resourceType: "product" | "collection"
        publications: {
            "publicationId": ID,
            "publishDate": Date | None
        }[]

    """

    try:
        with ShopifyPublication(brand_code=store_code) as (_, self):
            response, error = self.unpublish_publishable_resource(
                id=sp_resource_id, input=payload.publications
            )

            if error:
                return {
                    "error": "Fail to comunicate with Shopify",
                    "message": error.args[0],
                }, HTTPStatus.INTERNAL_SERVER_ERROR

            return {**response, "create_one_id": payload.resourceId}, HTTPStatus.OK
    except Exception as error:
        logger.error("error on unpublish resource", error)
        return {"error": "Internal Server Error"}, HTTPStatus.INTERNAL_SERVER_ERROR
