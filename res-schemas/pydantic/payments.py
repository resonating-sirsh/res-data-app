"""Pydantic models for the event hooks system."""

from __future__ import annotations

import datetime
import math
from datetime import date
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import validator

from schemas.pydantic.common import (
    CommonBaseModel,
    CommonConfig,
    Field,
    root_validator,
    uuid_str_from_dict,
)


class PaymentsTransaction(CommonBaseModel):
    transaction_pkid: int
    order_total_amount: float
    subscription_pkid: Optional[str]
    created_at: Optional[datetime.date]
    updated_at: Optional[datetime.date]
    transaction_type: Optional[str]
    source: Optional[str]
    reference_id: Optional[str]
    currency: Optional[str]
    description: Optional[str]
    stripe_charge_id: Optional[str]
    direct_stripe_payment: Optional[bool]
    is_failed_transaction: Optional[bool]
    transaction_status: Optional[str]
    transaction_error_message: Optional[str]
    brand_code: str
    subscription_name: Optional[str]
    transactions_detail_pkid: Optional[int]
    transactions_detail_type: Optional[str]
    make_cost: Optional[float]
    price: Optional[float]
    transaction_id: Optional[str]
    order_number: Optional[str]
    order_date: Optional[date]
    order_type: Optional[str]
    revenue_share: Optional[float]
    line_item_total_amount: Optional[float]

    @validator("order_total_amount", pre=True, always=True)
    def int_to_decimal_for_amount(cls, v):
        if isinstance(v, int):
            return float(v) / 100
        return v

    @validator(
        "revenue_share", "line_item_total_amount", "price", "make_cost", pre=True
    )
    def parse_decimal(cls, v, field):
        if v in (None, "", "NaN", "Infinity", "-Infinity") or math.isnan(v):
            return None
        if type(v) == float:
            return v
        try:
            return float(v)
        except (ValueError, TypeError):
            raise ValueError(f"{field.name} is not a valid float")

    @validator("transactions_detail_pkid", pre=True)
    def parse_int(cls, v, field):
        if v in (None, "", "NaN", "Infinity", "-Infinity", "nan") or math.isnan(v):
            return None
        try:
            return int(v)
        except (ValueError, TypeError):
            raise ValueError(f"value: {v} of {field.name} is not a valid int")


class BrandCreateUpdate(CommonBaseModel):
    meta_record_id: str
    fulfill_record_id: str
    brand_code: str

    name: Optional[str]
    shopify_storename: Optional[str]
    order_delayed_email_v2: Optional[str]
    created_at_airtable: Optional[date]
    shopify_store_name: Optional[str]
    homepage_url: Optional[str]
    sell_enabled: Optional[bool] = Field(default=False)
    shopify_location_id_nyc: Optional[str]
    contact_email: Optional[str]
    payments_revenue_share_ecom: Optional[Decimal]
    created_at_year: Optional[int]
    payments_revenue_share_wholesale: Optional[Decimal]
    subdomain_name: Optional[str]
    brands_create_one_url: Optional[str]
    shopify_shared_secret: Optional[str]
    quickbooks_manufacturing_id: Optional[str]
    address: Optional[str]
    is_direct_payment_default: Optional[bool]
    brand_success_active: Optional[bool]
    shopify_api_key: Optional[str]
    shopify_api_password: Optional[str]
    register_brand_email_address: Optional[str]
    start_date: Optional[date]
    end_date: Optional[date]
    is_exempt_payment_setup_for_ordering_createone: Optional[bool] = Field(
        default=False
    )


class BrandSubscription(CommonBaseModel):
    sub_name: Optional[str]
    stripe_subscription_id: Optional[str]
    sub_pkid: Optional[str]
    sub_created_at: Optional[date]
    sub_updated_at: Optional[date]
    sub_start_date: Optional[date]
    sub_end_date: Optional[date]
    sub_collection_method: Optional[str]
    balance: Optional[Decimal]
    stripe_price_id: Optional[str]
    stripe_customer_id: Optional[str]
    stripe_payment_method: Optional[str]
    currency: Optional[str]
    sub_current_period_start: Optional[date]
    sub_current_period_end: Optional[date]
    sub_price_amount: Optional[Decimal]
    sub_is_direct_payment_default: Optional[bool]


class BrandAndSubscription(BrandCreateUpdate, BrandSubscription):
    brand_pkid: int
    id_uuid: Optional[str]
    active_subscription_id: Optional[int]
    must_pay_before_make: Optional[bool]


class PaymentsCreateOrder(CommonBaseModel):
    order_number: str
    one_numbers: List[int]
    subscription_type: str
    brand_code: str


class PaymentsGetOrUpdateOrder(CommonBaseModel):
    one_number: Optional[int]
    current_sku: Optional[str]
    old_sku: Optional[str]
    order_number: Optional[str]
    quantity: Optional[int]
    shipping_notification_sent_cust_datetime: Optional[datetime.datetime]
    ready_to_ship_datetime: Optional[datetime.datetime]
    shipped_datetime: Optional[datetime.datetime]
    received_by_cust_datetime: Optional[datetime.datetime]
    process_id: Optional[str]
    metadata: Optional[Dict[str, Any]]
    fulfilled_from_inventory: Optional[bool]
    hand_delivered: Optional[bool]
    updated_at: datetime.datetime


# class BrandBillingParams(CommonBaseModel):
