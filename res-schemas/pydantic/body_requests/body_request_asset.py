from datetime import datetime
import enum
from uuid import uuid4
from pydantic import Field, validator

from res.connectors.s3.utils import is_s3_uri
from ..common import CommonBaseModel
from typing import Optional
from fastapi_utils.api_model import APIModel


class BodyRequestAssetType(str, enum.Enum):
    """
    Type of asset
    """

    ASSET_3D = "3D"
    """
    Asset is a 3D model of the body. Use only for the
    CREATE_FROM_PARENT flow
    """

    ASSET_2D = "2D"
    """
    Asset is a 2D image sketch. Use only for the
    CREATE_FROM_PARENT flow
    """


class BodyRequestAsset(CommonBaseModel, APIModel):
    """
    Asset use in the Brand Onboarding Body Request
    """

    def __init__(self, **data):
        data["id"] = data.get("id", str(uuid4()))
        super().__init__(**data)

    id: str

    name: str = Field(
        description="Name of the asset",
    )

    uri: str = Field(
        description="Url of the asset",
    )

    type: BodyRequestAssetType = Field(
        description="Type of the asset",
    )

    brand_body_request_id: str = Field(
        description="Id of the brand body request",
    )

    created_at: datetime = Field(
        description="Date of creation",
        default=datetime.utcnow(),
    )

    updated_at: datetime = Field(
        description="Date of last update",
        default=datetime.utcnow(),
    )

    deleted_at: Optional[datetime] = Field(
        default=None,
        description="Date of deletion",
    )

    @validator("uri")
    def validate_s3_path(cls, v):
        if is_s3_uri(v):
            return v
        raise ValueError("Invalid S3 path")
