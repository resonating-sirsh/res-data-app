from pydantic import BaseModel, Field
from typing import List, Optional
from fastapi_utils.enums import StrEnum


class TrimImage(BaseModel):
    url: str


class BrandTrims(BaseModel):
    id: Optional[str]
    airtable_trim_base_id: Optional[str]
    name: Optional[str]
    code: Optional[str]


class ColorTrims(BaseModel):
    airtable_trim_base_id: Optional[str]
    name: Optional[str]
    hex_code: Optional[str]


class SizeTrims(BaseModel):
    airtable_trim_base_id: Optional[str]
    name: Optional[str]
    code: Optional[str]


class TrimTaxonomy(BaseModel):
    id: Optional[str]
    airtable_trim_base_id: Optional[str]
    name: Optional[str]
    parent_id: Optional[str]


class VendorTrims(BaseModel):
    id: Optional[str]
    airtable_trim_base_id: Optional[str]
    name: Optional[str]
    code: Optional[str]


class ReturnGetTrims(BaseModel):
    id: str
    airtable_trim_base_id: str
    name: Optional[str]
    status: str
    type: str
    brand: BrandTrims
    color: Optional[ColorTrims]
    size: Optional[SizeTrims]
    trim_taxonomy: Optional[TrimTaxonomy]
    vendor: Optional[VendorTrims]
    image: Optional[list[TrimImage]]
    expected_delivery_date: Optional[str]
    order_quantity: Optional[int]
    available_quantity: Optional[int]
    warehouse_quantity: Optional[int]
    trim_node_quantity: Optional[int]
    in_stock: Optional[bool]


class SortDirection(StrEnum):
    ASC = "ASC"
    DESC = "DESC"


class SqlSortFields(StrEnum):
    NAME = "NAME"
    STATUS = "STATUS"
    TYPE = "TYPE"
    EXPECTED_DELIVERY_DATE = "EXPECTED_DELIVERY_DATE"
    ORDER_QUANTITY = "ORDER_QUANTITY"
    IN_STOCK = "IN_STOCK"


class SqlSort(BaseModel):
    name: SqlSortFields
    direction: SortDirection


class SortFieldQuery(StrEnum):
    NAME = "name"
    STATUS = "status"
    TYPE = "type"
    EXPECTED_DELIVERY_DATE = "expected_delivery_date"
    ORDER_QUANTITY = "order_quantity"
    IN_STOCK = "in_stock"


class TrimStatus(StrEnum):
    PENDING_RECEIPT = "PENDING_RECEIPT"
    RECEIVED_REGISTERED = "RECEIVED_REGISTERED"
    DO_NOT_RESTOCK = "DO_NOT_RESTOCK"
    APPROVED = "APPROVED"
    INACTIVE = "INACTIVE"


class TrimStatusQuery(StrEnum):
    PENDING_RECEIPT = "MAKE: Pending Receipt"
    RECEIVED_REGISTERED = "MAKE: Received & Registered"
    DO_NOT_RESTOCK = "MAKE: Do Not Restock"
    APPROVED = "Approved"
    INACTIVE = "Inactive"


class TrimType(StrEnum):
    SUPPLIED = "SUPPLIED"
    RESONANCE_MADE = "RESONANCE_MADE"


class TrimTypeQuery(StrEnum):
    SUPPLIED = "Supplied"
    RESONANCE_MADE = "Resonance Made"


class TrimSuppliedType(StrEnum):
    BRAND_SUPPLIED = "BRAND_SUPPLIED"
    RESONANCE_SUPPLIED = "RESONANCE_SUPPLIED"


class TrimSuppliedTypeQuery(StrEnum):
    BRAND_SUPPLIED = "Brand Supplied"
    RESONANCE_SUPPLIED = "Resonance Supplied"


class TrimsInput(BaseModel):
    name: Optional[str] = Field(
        description="Name of the trim",
    )
    brand_code: Optional[str] = Field(
        description="Brand code of the trim",
    )
    airtable_brand_id: Optional[str] = Field(
        description="Brand id from res.Meta base",
    )
    status: Optional[TrimStatus] = Field(
        description="Status of the trim, this by defualt should be PENDING_RECEIPT",
        default=TrimStatus.PENDING_RECEIPT,
    )
    type: TrimType = Field(
        description="Type of the trim indicated if the trim was supplied or made by resonance",
    )
    airtable_color_id: Optional[str] = Field(
        description="Color id of the trim",
    )
    color_name_by_brand: Optional[str] = Field(
        description="Color name of the trim input by brand",
    )
    color_hex_code_by_brand: Optional[str] = Field(
        description="Color hex code of the trim input by brand",
    )
    airtable_trim_taxonomy_id: Optional[str] = Field(
        description="Trim taxonomy id of the trim",
    )
    image: Optional[list[TrimImage]] = Field(
        description="Image of the trim",
    )
    custom_brand_ai: Optional[list[TrimImage]] = Field(
        description="Custom brand ai of the trim",
    )
    airtable_trim_category_id: str = Field(
        description="Trim category id of the trim, this represent also the trim taxonomy but just the level 3 that is Button, Zipper, etc...",
    )
    order_quantity: int = Field(
        description="Order quantity of the trim",
    )
    in_trim_node_quantity: Optional[int] = Field(
        description="In trim node quantity of the trim",
    )
    vendor_name: Optional[str] = Field(
        description="Vendor name of the trim",
    )
    airtable_vendor_id: Optional[str] = Field(
        description="Vendor id of the trim",
    )
    airtable_size_id: Optional[str] = Field(
        description="Size id of the trim",
    )
    expected_delivery_date: Optional[str] = Field(
        description="Expected delivery date of the trim",
    )

    class Config:
        use_enum_values = False


class Trim(BaseModel):
    id: str
    airtable_id: str
    name: str
    sell_brands_pkid: int
    status: TrimStatus
    type: TrimType
    supplied_type: TrimSuppliedType
    airtable_trim_base_meta_brand_id: str
    airtable_color_id: str
    airtable_trim_taxonomy_id: str
    airtable_vendor_id: Optional[str]
    airtable_size_id: str
    image: Optional[list[TrimImage]]
    custom_brand_ai: Optional[list[TrimImage]]
    order_quantity: Optional[int]
    expected_delivery_date: Optional[str]

    class Config:
        use_enum_values = False


class PrepRequest(BaseModel):
    one_number: str
    status: str
    trim_notes: str
    make_one_production_id: str
    sku: str
    body_code: str
    style_code: str
    material_code: str
    size: str
    brand: str


class CreateTrimTaxonomy(BaseModel):
    name: str
    type: str
    parent_id: Optional[str]
    friendly_name: Optional[str]
    record_id: Optional[str]
    trim_size: Optional[str]


class ReturnTrimTaxonomySet(BaseModel):
    id: str
    type: Optional[str]
    name: Optional[str]
    parent_meta_trim_taxonomy_pkid: Optional[str]
    friendly_name: Optional[str]
    airtable_record_id: Optional[str]
    airtable_trim_size: Optional[str]


class TrimTaxonmyList(BaseModel):
    id: str
    airtable_trim_base_id: str
    name: str
    trims: Optional[List[ReturnGetTrims]]


class ReturnTrimTaxonomyList(TrimTaxonmyList):
    taxonomy_childs: Optional[List[TrimTaxonmyList]]


class BillOfMaterials(BaseModel):
    id: str
    name: str
    trim_type: str
    trim_taxonomy_id: str
    trim_taxonomy_name: str
    quantity: Optional[int]
    length: Optional[float]
