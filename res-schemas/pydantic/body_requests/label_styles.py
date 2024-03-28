from uuid import uuid4
from fastapi_utils.api_model import APIModel
from ..common import CommonBaseModel
from pydantic import Field


class LabelStyle(CommonBaseModel, APIModel):
    """
    Label style use in the Brand Onboarding Body Request
    """

    id: str = Field(
        description="Id of the label style",
    )

    name: str = Field(
        description="Name of the label style",
    )

    thumbnail_url: str = Field(
        description="Thumbnail uri of the label style",
    )

    dimensions: str = Field(
        description="Dimensions of the label style",
    )

    body_code: str = Field(
        description="Id of the body label",
    )
