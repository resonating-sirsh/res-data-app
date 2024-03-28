from typing import List, Optional
from schemas.pydantic.common import CommonBaseModel, Field
import datetime
from typing import Optional, List, Union
from enum import Enum
from pydantic import EmailStr, BaseModel


class BodyOneReadyRequestType(Enum):
    BODY_ONBOARDING = "Body Onboarding"
    BODY_MODIFICATION = "Body Modification"
    SIMULATION_FEEDBACK = "Simulation Feedback"
    FIRST_ONE_FEEDBACK = "First ONE Feedback"
    BODY_DUPLICATION = "Body Duplication"
    CONTRACT_VIOLATION = "Contract Violation"
    STATIC_META_ONE = "Static Meta ONE"


class BodyOneReadyFlow(Enum):
    FLOW_2D = "2D"
    FLOW_3D = "3D"


class OrderFirstOneMode(Enum):
    MANUALLY = "Manually"
    AUTOMATICALLY = "Automatically"


class RequestorLayer(Enum):
    BRAND = "Brand"
    SEW_DEVELOPMENT = "Sew Development"
    PLATFORM = "Platform"


class StartingPointType(Enum):
    STARTING_FROM_SCRATCH = "Starting from Scratch"
    STARTING_FROM_EXISTING_BODY = "Starting from Existing Body"


class BillOfMaterialSupplier(Enum):
    RESONANCE = "Resonance"
    BRAND = "Brand"


class BrandIntakeTrimsInput(BaseModel):
    category: str
    size: str
    quantity: float
    supplier: BillOfMaterialSupplier


class StartingFromExistingBodyBillOfMaterialSettings(BaseModel):
    parent_bill_of_material_id: str
    supplier: Optional[BillOfMaterialSupplier]


class StartingFromScratchBillOfMaterialSettings(BaseModel):
    # are all required?
    supplier: BillOfMaterialSupplier
    category: str
    size: str
    placement_location: str
    quantity: int


class File(BaseModel):
    uri: str


class LabelSettings(BaseModel):
    supplier: BillOfMaterialSupplier
    label_id: Optional[str]
    brand_intake_files: Optional[List[File]]


class CreateBodyOneReadyRequestInput(CommonBaseModel):
    body_id: str
    parent_body_id: Optional[str] = None
    one_sketch_files_ids: Optional[List[str]] = Field(default_factory=list)
    reference_images_files_ids: Optional[List[str]] = Field(default_factory=list)
    base_size_id: Optional[str] = None
    body_operations_ids: Optional[List[str]] = Field(default_factory=list)
    requested_by_email: EmailStr
    was_parent_body_selected_by_brand: Optional[bool] = False
    body_design_notes: Optional[str] = None
    sizes_notes: Optional[str] = None
    trim_notes: Optional[str] = None
    label_notes: Optional[str] = None
    prospective_launch_date: Optional[datetime.date] = None
    body_one_ready_request_type: BodyOneReadyRequestType
    changes_requested: Optional[List[str]] = Field(default_factory=list)
    areas_to_update: Optional[List[str]] = Field(default_factory=list)
    requestor_layers: List[RequestorLayer]
    starting_point_type: Optional[StartingPointType]
    label_settings: Optional[LabelSettings]
    bill_of_materials_settings: (
        Optional[List[StartingFromExistingBodyBillOfMaterialSettings]]
        | Optional[List[StartingFromScratchBillOfMaterialSettings]]
    )
    body_one_ready_flow: Optional[BodyOneReadyFlow] = None
    techpack_files: Optional[List[File]] = Field(default_factory=list)
    pattern_files: Optional[List[File]] = Field(default_factory=list)
