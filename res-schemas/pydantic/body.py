from typing import List, Optional
from pydantic import EmailStr
from schemas.pydantic.common import CommonBaseModel, Field
from enum import Enum


class BodyOnboardingFlowType(Enum):
    LEGACY_ONBOARDING = "LEGACY_ONBOARDING"
    NEW_ONBOARDING = "NEW_ONBOARDING"


class CreateBodyInput(CommonBaseModel):
    name: str
    brand_code: str
    category_id: str
    cover_images_files_ids: Optional[List[str]] = Field(default_factory=list)
    campaigns_ids: Optional[List[str]] = Field(default_factory=list)
    onboarding_materials_ids: Optional[List[str]] = Field(default_factory=list)
    onboarding_combo_material_ids: Optional[List[str]] = Field(default_factory=list)
    onboarding_lining_materials_ids: Optional[List[str]] = Field(default_factory=list)
    fit_avatars_ids: Optional[List[str]] = Field(default_factory=list)
    size_scales_ids: Optional[List[str]] = Field(default_factory=list)
    base_pattern_size_id: Optional[str] = Field(
        default=None,
    )
    created_by_email: EmailStr
    onboarding_flow_type: Optional[BodyOnboardingFlowType] = Field(
        default=None,
    )
    body_onboarding_request_id: Optional[str] = Field(
        default=None,
    )
