from enum import Enum
import traceback
from typing import Any, Dict, List, Optional
from pydantic import BaseModel
import requests
import shopify
import json

from res.utils import logger


class MimeType(str, Enum):
    video = "VIDEO"

    external_video = "EXTERNAL_VIDEO"
    """
    External video not implemented yet
    """

    model_3d = "MODEL_3D"

    image = "IMAGE"


ALLOW_MIMETYPES = {
    MimeType.video: [
        "video/mp4",
        "video/quicktime",
    ],
    MimeType.image: [
        "image/gif",
        "image/png",
        "image/jpeg",
    ],
    MimeType.model_3d: [
        "model/gltf-binary",
        "model/vnd.usd+zip",
        "binary/octet-stream",
    ],
}


class HttpMethod(Enum):
    """
    Allow http methods to upload assets to shopify staged upload
    """

    POST = "POST"
    PUT = "PUT"


class StageUploadInput(BaseModel):
    resource: str
    filename: str
    mimeType: str
    httpMethod: str
    fileSize: str


class StagedTarget(BaseModel):
    """
    Staged Tager is a container in Shopify Storage to upload assets
    """

    url: str
    resourceUrl: Optional[str]
    parameters: List[Dict[str, str]]


class UserErrors(BaseModel):
    field: str
    message: str


class StageUploadResponse(BaseModel):
    stagedTargets: List[StagedTarget]
    userErrors: Optional[List[UserErrors]]


def upload_media_to_stage_upload(
    http_method: HttpMethod,
    staged_url: str,
    parameters: List[Dict[str, str]],
    filename: str,
    mime_type: str,
    media_raw: Any,  # TODO: check type s3.file_object.read()
):
    logger.info(
        f"Uploading media to stage filename: {filename} mime_type: {mime_type}..."
    )
    """
    Upload assets to an shopify stage upload
    """
    if http_method not in [HttpMethod.POST, HttpMethod.PUT]:
        raise Exception("HTTP method not allowed only POST and PUT are allowed")

    payload = {}

    for param in parameters:
        name = param["name"]
        value = param["value"]
        payload[name] = value

    file = (
        filename,
        media_raw,
        mime_type,
    )

    request = None

    if http_method == HttpMethod.POST:
        request = requests.post(
            url=staged_url,
            data=payload,
            files=[("file", file)],
        )
    else:
        # TODO: The put is differnt double check the shopify docs
        request = requests.put(
            url=staged_url,
            data=payload,
            files=[("file", file)],
        )
    request.raise_for_status()
    logger.info("Upload successfull...")
    logger.incr("upload_media_to_shopify", 1)


CREATE_STAGED_UPLOAD_URL = """
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
    stagedUploadsCreate(input: $input) {
        stagedTargets {
            url
            resourceUrl
            parameters {
                name
                value
            }
        }
        userErrors {
            field
            message
        }
    }
}
"""


def create_staged_upload(input: List[StageUploadInput]) -> List[StagedTarget]:
    sp_graphql_client = shopify.GraphQL()
    payload = {"input": [media_input.dict() for media_input in input]}
    sp_response_json = sp_graphql_client.execute(CREATE_STAGED_UPLOAD_URL, payload)
    sp_response = json.loads(sp_response_json)
    logger.debug(f"{sp_response}")
    staged_response = sp_response["data"]["stagedUploadsCreate"]["stagedTargets"]
    user_errors_response = (
        sp_response["data"]["stagedUploadsCreate"]["userErrors"] or []
    )
    response = StageUploadResponse(
        userErrors=list(UserErrors(**user_error) for user_error in user_errors_response)
        if user_errors_response
        else None,
        stagedTargets=list(StagedTarget(**staged) for staged in staged_response),
    )
    if response.userErrors:
        logger.warn(f"Create staged upload partially completed, some have errors")
        logger.info(json.dumps(response.dict(), indent=4))
        raise Exception(
            "UserError on creating staged upload",
            response.userErrors,
            traceback.format_exc(),
        )
    return response.stagedTargets


def get_resource_from_mime_type(type: str) -> str:
    for k, v in ALLOW_MIMETYPES.items():
        if type in v:
            return k
    raise Exception("Mime type not allowed")
