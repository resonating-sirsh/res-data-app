from typing import List, Optional
from schemas.pydantic.common import CommonBaseModel, Field
from typing import Optional, List
from enum import Enum
from pydantic import EmailStr
from datetime import datetime


class RequestType(Enum):
    BRAND_STYLE_ONBOARDING = "Brand Style Onboarding"
    MIGRATION_3D_REQUEST = "3D Migration Request"
    FINISHED_GOODS = "Finished Goods"
    META_ONE_MODIFICATION = "Meta ONE Modification"
    SAMPLE = "Sample"


class ColorApplicationMode(Enum):
    AUTOMATICALLY = "Automatically"
    MANUALLY = "Manually"


class ColorType(Enum):
    CUSTOM = "Custom"
    DEFAULT = "Default"


class ApplyColorFlowType(Enum):
    VSTITCHER = "VStitcher"
    APPLY_DYNAMIC_COLOR_WORKFLOW = "Apply Dynamic Color Workflow"


class AssetsGenerationMode(Enum):
    CREATE_NEW_ASSETS = "Create New Assets"
    COPY_ASSETS_FROM_PREVIOUS_VERSION = "Copy Assets from Previous Version"


class CreateApplyColorRequestInput(CommonBaseModel):
    style_id: str
    style_version: Optional[int]
    assignee_email: Optional[EmailStr]
    has_open_ones: Optional[bool]
    requests_ids: Optional[List[str]] = Field(default_factory=list)
    request_type: RequestType
    original_request_placed_at: datetime
    priority: Optional[int]
    requester_email: Optional[EmailStr]
    design_direction: Optional[str]
    request_reasons_contract_variables_ids: Optional[List[str]] = Field(
        default_factory=list
    )
    target_complete_date: Optional[datetime] = None


class ReproduceApplyColorRequestInput(CommonBaseModel):
    request_reasons_contract_variables_ids: Optional[List[str]] = Field(
        default_factory=list
    )
    reproduce_reasons: List[str]
    reproduce_details: Optional[str]
    canceller_email: EmailStr
