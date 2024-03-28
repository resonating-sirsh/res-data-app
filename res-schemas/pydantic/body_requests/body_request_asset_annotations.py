from datetime import datetime
from typing import Optional
from fastapi_utils.api_model import APIModel
from fastapi_utils.enums import StrEnum
from uuid import UUID, uuid4

from pydantic import Field, validator

from res.connectors.s3.utils import is_s3_uri

from ..common import CommonBaseModel


class BodyRequestAssetAnnotationSupplierType(StrEnum):
    """
    Type of supplier
    """

    RESONANCE = "RESONANCE"
    BRAND = "BRAND"


class BodyRequestAssetAnnotationType(StrEnum):
    """
    Type of node
    """

    MEASUREMENT_VALUE = "MEASUREMENT_VALUE"
    MEASUREMENT_COMMENT = "MEASUREMENT_COMMENT"
    PRINT_LABEL = "PRINT_LABEL"
    ADD_LABEL_DESIGN_MY_LABEL = "ADD_LABEL_DESIGN_MY_LABEL"
    ADD_LABEL_SUPPLY_LABEL = "ADD_LABEL_SUPPLY_LABEL"
    TRIM_ANNOTATION = "TRIM_ANNOTATION"
    BODY_CONTRUCTION_ANNOTATION = "BODY_CONTRUCTION_ANNOTATION"


class BodyRequestAnnotation(CommonBaseModel, APIModel):
    """
    Node use in the Brand Onboarding Body Request

    This node is just a way to poin a x,y,z position in the asset
    or TODO: Highlighting a piece on the 3D models.
    """

    def __init__(self, **data):
        data["id"] = data.get("id", str(uuid4()))
        super().__init__(**data)

    id: str = Field(
        description="Id of the node",
    )

    name: str = Field(
        description="Name of the node",
    )

    annotation_type: BodyRequestAssetAnnotationType = Field(
        description="Type of the node",
    )

    coordinate_x: float = Field(
        description="X coordinate of the node",
    )

    coordinate_y: float = Field(
        description="Y coordinate of the node",
    )

    coordinate_z: Optional[float] = Field(
        description="Z coordinate of the node",
        default=None,
    )

    asset_id: UUID = Field(
        description="asset id which node belongs to",
    )

    created_at: datetime = Field(
        description="Date of creation",
        default_factory=datetime.utcnow,
    )

    updated_at: datetime = Field(
        description="Date of last update",
        default_factory=datetime.utcnow,
    )

    deleted_at: Optional[datetime] = Field(
        description="Date of deletion",
        default=None,
    )

    value: Optional[str] = Field(
        description="value of the annotation",
    )

    value2: Optional[str] = Field(
        default=None,
        description="value of the annotation",
    )

    label_style_id: Optional[str] = Field(
        default=None,
    )

    artwork_uri: Optional[str] = Field(
        default=None,
    )

    main_label_uri: Optional[str] = Field(
        default=None,
    )

    size_label_uri: Optional[str] = Field(
        default=None,
    )

    bill_of_material_id: Optional[str] = Field(
        default=None,
    )

    supplier_type: Optional[BodyRequestAssetAnnotationSupplierType] = Field(
        default=None,
    )

    @validator("artwork_uri", "main_label_uri", "size_label_uri")
    def validate_s3_path(cls, v: str) -> str:
        if v is None:
            return v

        if is_s3_uri(v):
            return v
        raise ValueError("s3 path must start with s3://")
