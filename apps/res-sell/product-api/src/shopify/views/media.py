import json
from http import HTTPStatus
from typing import List, Optional, Dict, Any

import flask
import requests
from flask import request

from flask_cognito import cognito_auth_required
from pydantic import BaseModel, error_wrappers

from src.graph_api.models import Product
from res.connectors.shopify.resource.media import (
    ProductCreateMediaInput,
    ShopifyProductMedia,
)

from res.connectors.shopify.resource.staged_url import (
    HttpMethod,
    StageUploadInput,
    get_resource_from_mime_type,
    create_staged_upload,
    upload_media_to_stage_upload,
)

from res.connectors.graphql.hasura import Client as HasuraClient
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.s3.S3Connector import S3Connector
from res.connectors.shopify.utils.ShopifyProduct import GET_PRODUCT
from res.utils import logger

media_routes = flask.Blueprint("media", __name__)


class UploadMediaInput(BaseModel):
    store_code: str
    staged_url: str
    parameters: List[Dict[str, str]]
    s3_path: str
    method: str
    filename: str


class UploadMediaResponse(BaseModel):
    ok: bool
    message: str
    errors: Optional[List[Any]]
    fields: Optional[List[str]]


@media_routes.route("/product-api/media", methods=["PUT"])
def upload_media():
    """Updload a media to an shopify staged url from S3
    Media accepts:
        - "VIDEO"
        - "EXTERNAL_VIDEO" from YouTube or Vimeo
        - "IMAGE"
        - "MODEL_3D"
    Body Payload:
    {
        "store_code": str,
        "staged_url": str
        "parameters": List[Dict[str, str]],
        "s3_path": str,
        "method": str, -> "POST" or "PUT"
        "filename": str,
    }
    """
    try:
        body = request.get_json()
        if not body:
            return {"error": "body is empty"}, HTTPStatus.BAD_REQUEST
        logger.debug(json.dumps(body, indent=2))
        payload = UploadMediaInput(**body)
        s3_client = S3Connector()
        file_data = s3_client.get_file_info(payload.s3_path)
        mime_type = file_data["ContentType"]
        file_object = s3_client.file_object(payload.s3_path)
        upload_media_to_stage_upload(
            http_method=HttpMethod.POST,
            staged_url=payload.staged_url,
            parameters=payload.parameters,
            filename=payload.filename,
            mime_type=mime_type,
            media_raw=file_object.read(),
        )
        response = UploadMediaResponse(
            ok=True, message="Upload success", errors=[], fields=[]
        )
        return response.dict(exclude_none=True), HTTPStatus.OK
    except requests.exceptions.HTTPError as error:
        response = UploadMediaResponse(
            ok=False,
            message="Upload error",
            errors=[error.response.text],
            fields=[],
        )
        return response.dict(), HTTPStatus.INTERNAL_SERVER_ERROR
    except error_wrappers.ValidationError as error:
        errors = [error["msg"] for error in error.errors()]
        fields = list(str(error["loc"]) for error in error.errors())
        response = UploadMediaResponse(
            ok=False,
            message="Validation error",
            errors=errors,
            fields=fields,
        )
        return response.dict(), HTTPStatus.BAD_REQUEST


@media_routes.route("/product-api/media", methods=["GET"])
def get_3d_file_generated_url():
    """
    Generate a Signed url to the 3D file resource.
    """
    graphql_client = ResGraphQLClient()
    hasura_client = HasuraClient()
    s3_client = S3Connector()
    body = request.get_json()
    logger.debug(body)
    if not body:
        return {"error": "body is empty"}, HTTPStatus.BAD_REQUEST
    try:
        style_id = body["styleId"]
        style = graphql_client.query(
            """
            query getStyle($id: ID!) { 
                style(id: $id){
                    id
                    code
                    name
                    body {
                        id
                        code
                    }
                    color {
                        id
                        code
                    }
                }
            }
            """,
            {"id": style_id},
        )["data"]["style"]
        body_code: str = style["body"]["code"]
        color_code: str = style["color"]["code"]
        formatted_body_code = body_code.lower().replace("-", "_")
        formatted_color_code = color_code.lower().replace("-", "_")
        meta_bodies: Optional[List] = hasura_client.execute(
            """
            query bodies_with_3d($_eq: String = "") {
                meta_bodies(where: {body_code: {_eq: $_eq}}) {
                    body_code
                    version
                }
            }
            """,
            {"_eq": body_code},
        )["meta_bodies"]
        not_found_response = {"error": "Not found"}, HTTPStatus.NOT_FOUND
        if meta_bodies is None:
            return not_found_response
        meta_bodies.sort(key=lambda x: float(x["version"]), reverse=True)
        if not meta_bodies:
            return not_found_response

        for meta_body in meta_bodies:
            body_version = meta_body["version"]
            s3_path = f"s3://meta-one-assets-prod/color_on_shape/{formatted_body_code}/v{int(body_version)}/{formatted_color_code}/3d.glb"
            logger.info(s3_path)
            if s3_client.exists(s3_path):
                file_info = s3_client.get_file_info(s3_path)
                return {
                    "path": s3_path,
                    "size": file_info["ContentLength"],
                    "contentType": file_info["ContentType"],
                }, HTTPStatus.OK
        return not_found_response

    except Exception as err:
        logger.error("Error trying to get the 3D file generated url", errors=err)
        return {"error": "Internal Error"}, HTTPStatus.INTERNAL_SERVER_ERROR


@media_routes.route("/product-api/media", methods=["POST"])
@cognito_auth_required
def create_product_media():
    """Upload a S3 asset to Shopify using the Shopify GraphQL
    Media accepts:
        - "VIDEO"
        - "EXTERNAL_VIDEO" from YouTube or Vimeo
        - "IMAGE"
        - "MODEL_3D"
    Body Payload:
    {
        product_id: str
        photo_id: str,
        alt: str,
    }
    """
    graphql_client = ResGraphQLClient()
    body = request.get_json()
    s3_client = S3Connector()
    if not body:
        return {"error": "Body is empty"}, HTTPStatus.BAD_REQUEST
    try:
        response = graphql_client.query(GET_PRODUCT, {"id": body["product_id"]})
        product = Product(response["data"]["product"])

        with ShopifyProductMedia(brand_code=product.storeCode) as instance:
            instance = instance[1]
            for photo in product.photos:
                if photo.id != body["photo_id"]:
                    continue

                resource = get_resource_from_mime_type(photo.type)
                if not resource:
                    return {"error": "MimeType is not allow"}, HTTPStatus.BAD_REQUEST

                stage_input = StageUploadInput(
                    filename=photo.name,
                    httpMethod=HttpMethod.POST.value,
                    fileSize=str(photo.size),
                    mimeType=photo.type,
                    resource=resource,
                )
                staged = create_staged_upload([stage_input])[0]
                if staged.resourceUrl is None:
                    return {
                        "error": "Error trying to create a staged upload: Resource URL is not defined"
                    }, HTTPStatus.INTERNAL_SERVER_ERROR

                s3_path = f"s3://{photo.bucket}/{photo.key}"

                file_object = s3_client.file_object(s3_path)

                upload_media_to_stage_upload(
                    http_method=HttpMethod.POST,
                    staged_url=staged.url,
                    mime_type=photo.type,
                    filename=photo.name,
                    parameters=staged.parameters,
                    media_raw=file_object.read(),
                )

                response = instance.create_product_media(
                    ecommerce_id=product.ecommerceId,
                    input=[
                        ProductCreateMediaInput(
                            alt=body["alt"],
                            mediaContentType=resource,
                            originalSource=staged.resourceUrl,
                        )
                    ],
                )

                logger.debug(f"{response.dict()}")

                break
        return {"ok": True, "product_id": product.id}, HTTPStatus.OK
    except Exception as e:
        logger.error(f"{e}", exception=e)
        return {
            "ok": False,
            "error": "Internal server error",
        }, HTTPStatus.INTERNAL_SERVER_ERROR
