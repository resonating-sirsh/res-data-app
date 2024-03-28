from pydantic import BaseModel
from schemas.pydantic.common import CommonBaseModel

from typing import List, Optional


# this class will represent the line item info payload
class LineItemInfo(BaseModel):
    sku: str
    quantity: int
    order_number: Optional[str] = None
    channel_order_id: Optional[str] = None
    product_name: Optional[str] = None
    product_variant_title: Optional[str] = None
    price: Optional[float] = None
    shopify_fulfillment_status: Optional[str] = None
    shopify_fulfillable_quantity: Optional[int] = None
    variant_id: Optional[str] = None
    basic_cost_of_one: Optional[float] = None
    retail_price: Optional[float] = None
    wholesale_price: Optional[float] = None
    customizations: Optional[str] = None


# this class will represent the payload for the create order endpoint
class CreateOrderPayload(BaseModel):
    request_name: str
    brand_code: str
    email: str
    order_id: Optional[int] = None
    order_channel: str
    sales_channel: str
    shipping_name: str
    shipping_address1: str
    shipping_address2: str
    shipping_city: str
    shipping_country: str
    shipping_province: str
    shipping_zip: str
    shipping_phone: str
    revenue_share_percentage: Optional[float] = None
    is_sample: Optional[bool] = None
    channel_order_id: Optional[int] = None
    line_items_info: List[LineItemInfo]


class GroupFulfillment(BaseModel):
    id: str
    name: str


class OrderRecord(BaseModel):
    id: str
    order_name: str
    number: str
    flag_for_review_notes: str
    group_fulfillment: GroupFulfillment
    sales_channel: str
    request_type: str
    request_name: str
    order_channel: str
    channel_order_id: str


class CreateLineItemPayload(BaseModel):
    order_id: str
    line_items_info: List[LineItemInfo]


class SkuInventoryStatus(CommonBaseModel):
    id: Optional[str] = None
    warehouse_location: Optional[str] = None
    warehouse_bin_location: Optional[str] = None
    bin_location: Optional[str] = None
    link_to_inventory: Optional[str] = None
    unit_order_number: Optional[str] = None


class MetaOneStatus(CommonBaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    style_code: Optional[str] = None
    body_code: Optional[str] = None
    body_version: Optional[str] = None
    material_code: Optional[str] = None
    color_code: Optional[str] = None
    size_code: Optional[str] = None
    style_version: Optional[str] = None
    basic_cost_of_one: Optional[float] = 0
    _json_price_breakdown: Optional[list[dict]] = None
    price: Optional[float] = 0
    is_one_ready: Optional[bool] = False
    is_style_3d: Optional[bool] = False


class LineItemJson(BaseModel):
    id: Optional[str] = None
    sku: str
    total_needed: int
    total_created: int
    total_insurense_needed: int
    total_insurense_created: int
    quantity: int
    channel_order_line_item_id: Optional[str] = None
    ecommerce_order_number: Optional[str] = None
    line_item_price: Optional[float] = 0
    basic_cost_of_one: Optional[float] = 0
    shopify_fulfillment_status: Optional[str] = None
    shopify_fulfillableQuantity: Optional[int] = None
    product_variant_title: Optional[str] = None
    variant_id: Optional[int] = None
    product_name: Optional[str] = None
    customizations: Optional[List[str]] = None
    is_sku_available: bool
    is_unit_available: bool
    should_be_hold: bool
    meta_one_status: MetaOneStatus
    sku_inventory_status: SkuInventoryStatus
    hold_type: Optional[str] = None


class FulfillmentOrderPayload(CreateOrderPayload):
    order_airtable_id: Optional[str]
    postgres_id: Optional[str]
    order_name: Optional[str]
    number: Optional[str]
    flag_for_review_notes: Optional[str]
    request_type: Optional[str]
    friendly_name: Optional[str]
    was_payment_successful: Optional[bool]


class FulfillmentGroupPayload(CommonBaseModel):
    id: str
    name: str
