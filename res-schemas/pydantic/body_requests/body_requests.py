import datetime
from typing import List, Optional
from fastapi_utils.api_model import APIModel
from fastapi_utils.enums import StrEnum

from pydantic import EmailStr, Field

from ..common import CommonBaseModel


class BodyRequestType(StrEnum):
    """
    Type of request
    """

    CREATE_FROM_SCRATCH = "CREATE_FROM_SCRATCH"
    """
    CREATE_FROM_SCRATCH: Create a new body from scratch
    """

    CREATE_FROM_EXISTING_BODY = "CREATE_FROM_EXISTING_BODY"
    """
    CREATE_FROM_EXISTING_BODY: Create a new body from a existing body
    """


class BodyRequestStatus(StrEnum):
    """
    Status of the request
    """

    DRAFT = "DRAFT"
    """
    Draft status, the request is not ready to be processed
    """

    PENDING = "PENDING"
    """
    Pending status, the request is ready to be processed
    """

    APPROVED = "APPROVED"
    """
    Approved status, the request is approved and the body is created
    """

    REJECTED = "REJECTED"
    """
    Rejected status, the request is rejected and the body is not created
    """


class BodyRequest(CommonBaseModel, APIModel):
    """
    Brand Request use to create/modify a body
    """

    id: str
    request_type: BodyRequestType
    status: BodyRequestStatus
    brand_code: str
    name: str

    body_category_id: Optional[str] = Field(
        default=None,
    )

    body_code: Optional[str] = Field(
        default=None,
    )
    """
    Body code comes from the airtable body.
    """

    combo_material_id: Optional[str] = Field(
        default=None,
    )
    """
    Material use in combo.
    """

    lining_material_id: Optional[str] = Field(
        default=None,
    )
    """
    Material use in lining
    """

    base_size_code: Optional[str] = Field(
        default=None,
    )
    """
    Base Size where would create the body in a 3D render
    """

    size_scale_id: Optional[str] = Field(
        default=None,
    )
    """
    Size scale id comes from airtable
    """

    meta_body_id: Optional[str] = Field(
        default=None,
    )
    """
    Body version id comes from the table `meta_bodies`
    """

    fit_avatar_id: Optional[str] = Field(
        default=None,
    )

    body_onboarding_material_ids: Optional[List[str]] = Field(
        default=None,
    )
    """
    Material onboarding id comes from the airtable Onboarding Materials file
    """

    created_by: EmailStr = Field(
        default="system@resonance.nyc",
    )

    created_at: datetime.datetime = Field(
        default=datetime.datetime.now(),
    )

    updated_at: datetime.datetime = Field(
        default=datetime.datetime.now(),
    )

    deleted_at: Optional[datetime.datetime] = Field(
        default=None,
    )
