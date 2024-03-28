"""
DTO for requests and response on the body-requests API endpoints.
"""

from fastapi_utils.api_model import APIModel
import schemas.pydantic.body_requests.body_requests as bbr_schemas

from typing import List, Optional
from uuid import UUID
from pydantic import EmailStr, Field

from ..body_request_assets.models import (
    BodyRequestAssetInput,
    UpsertBodyRequestAsset,
)


class CreateBodyRequestInput(APIModel):
    name: str

    request_type: bbr_schemas.BodyRequestType

    brand_code: str

    status: bbr_schemas.BodyRequestStatus = bbr_schemas.BodyRequestStatus.DRAFT

    body_code: Optional[str] = Field(
        default=None,
    )

    combo_material_id: Optional[str] = Field(
        default=None,
    )

    lining_material_id: Optional[str] = Field(
        default=None,
    )

    size_scale_id: Optional[str] = Field(
        default=None,
    )

    base_size_code: Optional[str] = Field(
        default=None,
    )

    meta_body_id: Optional[UUID] = Field(
        default=None,
    )

    body_category_id: Optional[str] = Field(
        default=None,
    )

    fit_avatar_id: Optional[str] = Field(
        default=None,
    )

    body_onboarding_material_ids: Optional[List[str]] = Field(
        default=[],
    )

    assets: List[BodyRequestAssetInput] = Field(default=[])

    created_by: Optional[EmailStr] = Field(
        default="system@resonance.nyc",
    )


class UpdateBodyRequestInput(APIModel):
    name: Optional[str]

    status: Optional[bbr_schemas.BodyRequestStatus] = Field(default=None)

    body_category_id: Optional[str] = Field(
        default=None,
    )

    fit_avatar_id: Optional[str] = Field(
        default=None,
    )

    body_onboarding_material_ids: List[str] = Field(
        default=None,
    )

    assets: List[UpsertBodyRequestAsset] = Field(default=[])

    delete_assets_ids: List[UUID] = Field(default=[])

    combo_material_id: Optional[str] = Field(
        default=None,
    )

    lining_material_id: Optional[str] = Field(
        default=None,
    )

    size_scale_id: Optional[str] = Field(
        default=None,
    )

    base_size_code: Optional[str] = Field(
        default=None,
    )


class BodyRequestResponse(APIModel):
    body_request: bbr_schemas.BodyRequest = Field()
