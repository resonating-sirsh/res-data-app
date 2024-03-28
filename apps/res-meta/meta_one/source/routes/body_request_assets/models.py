from typing import Optional
from uuid import UUID
from fastapi_utils.api_model import APIModel
from pydantic import Field
from schemas.pydantic.body_requests.body_request_asset import (
    BodyRequestAsset,
    BodyRequestAssetType,
)


class BodyRequestAssetInput(APIModel):
    """
    Create Body Request Asset
    """

    name: str
    """
    Asset name
    """

    uri: str
    """
    S3 URI of the asset

    Example: s3://bucket-name/folder-name/asset-name
    """

    type: BodyRequestAssetType
    """
    Asset type
    """

    body_request_id: Optional[UUID] = Field(default=None)
    """
    Body Request ID
    """


class UpsertBodyRequestAsset(APIModel):
    id: Optional[UUID] = Field(
        default=None,
    )
    """
    Asset ID

    if id is present update the asset, otherwise create a new asset
    """

    name: str = Field()
    """
    Asset name
    """

    uri: str = Field()
    """
    S3 URI of the asset
    """

    type: BodyRequestAssetType = Field(default=BodyRequestAssetType.ASSET_2D)


class BodyRequestAssetResponse(APIModel):
    """
    Body Request Asset Response
    """

    asset: BodyRequestAsset
    """
    Body Request Asset
    """
