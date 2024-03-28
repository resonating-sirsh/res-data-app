from typing import Optional
from uuid import UUID
from fastapi_utils.api_model import APIModel
from res.connectors.s3.utils import is_s3_uri
from schemas.pydantic.body_requests.body_request_asset_annotations import (
    BodyRequestAnnotation,
    BodyRequestAssetAnnotationSupplierType,
    BodyRequestAssetAnnotationType,
)

from pydantic import Field, validator


class BodyRequestAnnotationResponse(APIModel):
    body_request_annotation: BodyRequestAnnotation


class _BaseInput(APIModel):
    value: Optional[str] = Field(
        default=None,
    )
    """
    Value of the annotation
    """

    value2: Optional[str] = Field(
        default=None,
    )
    """
    Value2 of the annotation
    """

    label_style_id: Optional[str] = Field(
        default=None,
    )
    """
    Label style id of the annotation
    """

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
        default=BodyRequestAssetAnnotationSupplierType.RESONANCE,
    )

    @validator("artwork_uri", "main_label_uri", "size_label_uri")
    def validate_uri(cls, v):
        if v is not None:
            if not is_s3_uri(v):
                raise ValueError("URI must start with http or https")
        return v


class BodyRequestAnnotationInput(_BaseInput):
    name: str
    """
    Name of the asset annotation
    """

    annotation_type: BodyRequestAssetAnnotationType
    """
    Type of the asset annotation
    """

    coordinate_x: float
    """
    X coordinate of the asset annotation
    """

    coordinate_y: float
    """
    Y coordinate of the asset annotation
    """

    coordinate_z: Optional[float] = Field(
        default=None,
    )
    """
    Z coordinate of the asset annotation
    """

    asset_id: UUID
    """
    Asset id which annotation belongs to
    """


class BodyRequestAnnotationUpdateInput(_BaseInput):
    name: Optional[str] = Field(
        default=None,
    )
    """
    Name of the asset annotation
    """
