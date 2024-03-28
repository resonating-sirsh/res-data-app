"""
these are sell types 
they can be registered in airtable for a given dev/prod env with the flow api
For example
#api = FlowAPI(OrderQueue)
#api._airtable_queue.refresh_schema_from_type(OrderQueue)
"""

from __future__ import annotations

from typing import List, Optional

import numpy as np
import pandas as pd
from pydantic import Field

from schemas.pydantic.common import (
    CommonConfig,
    FlowApiModel,
    root_validator,
    uuid_str_from_dict,
)


def make_order_id(values):
    return uuid_str_from_dict(
        {
            "id": str(values["source_order_id"]),
            "order_channel": values["order_channel"],
        }
    )


class Shipping(FlowApiModel):
    id: Optional[str] = Field(primary_key=True)
    name: str = Field(exclude=True)
    address1: str
    address2: Optional[str]
    email: str
    city: str
    zipcode: str
    province: Optional[str] = Field(exclude=True, default="N/A")
    phone: Optional[str]
    # TODO add this to the schema in hasura
    country: str = Field(exclude=True)
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)

    @root_validator
    def _metadata(cls, values):
        hash_keys = [
            "email",
            "shipping_address1",
            "shipping_zip",
            "shipping_city",
            "shipping_country",
        ]

        def cleaner(d):
            return d

        s = {}
        for field in hash_keys:
            s[cleaner(field)] = values.get(field)
        sid = uuid_str_from_dict(s)

        values["id"] = sid
        return values


# This is Our request Type for now.
class OrderItem(FlowApiModel):
    """
    This is an Order Item model from Fulfillment Flow

    To track our pending shipments we must look at the fulfllable quantity P (for pending)
    The original qty Q can be deducted with cancellations C and fulfillments F

    P = Q - C - F

    """

    class Config(CommonConfig):
        airtable_attachment_fields = {}

    id: Optional[str] = Field(primary_key=True)
    # the total originally ordered
    quantity: int  # this
    # this shows currently how many of the qty we have yet to fulfill
    fulfillable_quantity: Optional[int]
    # this tracks how many things we have registered as fulfilled
    fulfilled_quantity: Optional[int] = Field(default=0)
    # if we have processed refunds we see here
    refunded_quantity: Optional[int] = Field(default=0)
    source_order_line_item_id: str
    ecommerce_quantity: Optional[int] = Field(default=0)
    ecommerce_order_number: Optional[str] = Field(default=None)
    ecommerce_line_item_id: Optional[str] = Field(default=None)
    ecommerce_fulfillment_status: Optional[str] = Field(default=None)
    sku: str  # this
    # shopify gives the product name, we dont really need it
    name: Optional[str]
    order_id: str
    stage: Optional[str] = Field(exclude=True)
    status: Optional[str] = Field(default="Unknown")
    node: Optional[str] = Field(exclude=True)
    contracts_failed: Optional[List[str]] = Field(default_factory=list, db_write=False)
    customization: Optional[dict] = Field(default_factory=dict)
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)
    price: float
    basic_cost_of_one: Optional[float] = Field(default=0)
    type: Optional[str] = Field(default="Unknown", exclude=True)
    # resolved before we save or we provide an error on the order
    product_id: Optional[str]

    product_variant_title: Optional[str] = Field(default="Unknown")
    product_name: Optional[str] = Field(default="Unknown")

    inventory_airtable_id: Optional[str] = Field(default=None)
    warehouse_location: Optional[str] = Field(default=None)
    stock_state_day_of_order: Optional[str] = Field(default=None)
    inventory_checkin_type: Optional[str] = Field(default="Unknown")
    unit_ready_in_warehouse: Optional[bool] = Field(default=False)

    one_number: Optional[str] = Field(default=None)

    # this may be resolved from a response
    product_style_id: Optional[str] = Field(exclude=True)
    size_code: Optional[str] = Field(exclude=True)
    address: Optional[str] = Field(exclude=True)

    @root_validator
    def _metadata(cls, values):
        # this is very important - we are saying the order is a function of the sku (which has quantity) and the order header id
        # we could change this to be the channel id and channel like we do for the order too

        if values.get("fulfillable_quantity") == None:
            # in theory some could set this to something else but probably not via this order/item object so we just set it like this here
            values["fulfillable_quantity"] = values["quantity"]

        sku = values.get("sku")
        # hyphenate the sku
        if len(sku) and sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"
            values["sku"] = sku

        # use the clean sku to make the key
        values["id"] = uuid_str_from_dict({"order_key": values["order_id"], "sku": sku})

        values["size_code"] = sku.split(" ")[-1]
        return values


class OrderQueue(FlowApiModel):
    id: Optional[str]
    key: Optional[str] = Field(primary_key=True)
    source_order_id: Optional[str]
    email: str
    brand_code: str
    status: str = Field(default="Unknown")
    order_channel: str
    sales_channel: str
    name: str
    skus: Optional[List[str]] = Field(default_factory=list)
    contracts_failing: Optional[List[str]] = Field(default_factory=list)
    order_size: Optional[int]

    @root_validator
    def _metadata(cls, values):
        # shopify what we can name is a number and a hash. we dont need the hash
        values["key"] = f'{values["name"]}'.strip()

        # if the value is set to None we set this default
        if values["status"] == None:
            values["status"] = "Pending"

        if values["key"][:3] != values["brand_code"] + "-":
            values["key"] = f"{values['brand_code']}-{values['key']}"

        values["order_size"] = len(values["skus"])
        return values


class Order(FlowApiModel):
    class Config(CommonConfig):
        airtable_attachment_fields = {}

    id: Optional[str] = Field(primary_key=True)
    shipping: Optional[Shipping] = Field(db_write=False)
    email: str
    brand_code: str
    order_items: List[OrderItem] = Field(db_write=False)
    order_channel: str
    sales_channel: str
    name: str
    type: Optional[str] = Field(default="Unknown", exclude=True)
    status: Optional[str] = Field(default="Unknown")
    # this is maybe important in the database but add the metadata properly
    description: Optional[str] = Field(exclude=True)
    source_order_id: str
    contracts_failing: Optional[List[str]] = Field(default_factory=list)
    metadata: Optional[dict] = Field(default_factory=dict, db_write=False)
    ordered_at: str
    created_at: Optional[str] = Field(exclude=True)
    updated_at: Optional[str] = Field(exclude=True)
    was_payment_successful: Optional[bool] = Field(default=False, db_write=True)
    was_balance_not_enough: Optional[bool] = Field(default=False, db_write=True)
    # not sure if we need this in resonance order
    ecommerce_source: str
    revenue_share_percentage_wholesale: Optional[float] = Field(default=0)
    revenue_share_percentage_retail: Optional[float] = Field(default=0)
    sell_brands_pkid: Optional[int] = Field(default=0)
    line_item_json: Optional[list[dict]] = Field(default=[])
    request_type: Optional[str] = Field(default="Unknown")
    request_name: Optional[str] = Field(default="Unknown")
    channel_order_id: Optional[str] = Field(default=None)

    @root_validator
    def _metadata(cls, values):
        # shopify what we can name is a number and a hash. we dont need the hash
        values["name"] = values["name"].strip("#").strip()

        # if the value is set to None we set this default
        if values["status"] == None:
            values["status"] = "Pending"
        # the id is a hash of their id and the channel name that partitions them
        # this key uses the title case to generate the ids
        values["id"] = make_order_id(values)
        metadata = values.get("metadata", {})
        # qualify if we have to - not very clean to put this here though
        if values["name"][:3] != values["brand_code"] + "-":
            values["name"] = f"{values['brand_code']}-{values['name']}"

        if values["was_payment_successful"] == None:
            values["was_payment_successful"] = False
        if values["was_balance_not_enough"] == None:
            values["was_balance_not_enough"] = False

        metadata["order_key"] = f"{values['name']}"

        values["metadata"] = metadata
        return values

    ################################################################################################
    ################  BELOW WE HAVE VARIOUS ADAPTER TO MAP TO ONE.PLATFORM ORDER  #################
    ################################################################################################

    @staticmethod
    def from_create_one_payload(cone_order, metadata=None):
        """
        map create one create one topics to these resonance contract
        """
        channel = cone_order.get("orderChannel") or "CREATEONE"
        status = "Pending"
        if cone_order.get("isClosed"):
            status = "Fulfilled"

        def norm_lists(d, cols):
            for c in cols:
                if isinstance(d.get(c), list):
                    d[c] = d[c][0]
            return d

        def make_shipping(d):
            s = {
                "name": d.get("shippingName"),
                "email": d.get("email"),
                "address1": d.get("shippingAddress1"),
                "address2": d.get("shippingAddress2"),
                "city": d.get("shippingCity"),
                "country": d.get("shippingCountry"),
                "phone": d.get("shippingPhone"),
                "province": d.get("shippingProvince"),
                "zipcode": d.get("shippingZip"),
            }
            return s

        def _make_items(p, source_id, order_channel):
            """ """
            li = p.get("lineItemsInfo")

            _default_status = li[0].get("status") or "Pending Inventory"
            temp_li = li[0]

            # note we determine the order id here but we dont always have to do this depending how our crud works
            oid = uuid_str_from_dict(
                {
                    "id": str(source_id),
                    "order_channel": order_channel,
                }
            )
            li = pd.DataFrame(li)
            li = (
                li[["sku", "line_item_price", "id", "quantity", "basic_cost_of_one"]]
                .groupby("sku")
                .agg(
                    {
                        "line_item_price": sum,
                        "id": set,
                        "quantity": sum,
                        "basic_cost_of_one": sum,
                    }
                )
                .reset_index()
            )

            li["source_order_line_item_id"] = (
                li["id"]
                .fillna("")
                .map(lambda x: ",".join([i for i in x if pd.notnull(i)]))
            )

            keys = [
                "ecommerce_quantity",
                "ecommerce_order_number",
                "ecommerce_line_item_id",
                "ecommerce_fulfillment_status",
                "product_variant_title",
                "product_name",
                "inventory_airtable_id",
                "warehouse_location",
                "stock_state_day_of_order",
                "inventory_checkin_type",
                "unit_ready_in_warehouse",
                "one_number",
            ]

            for key in keys:
                if temp_li[key]:
                    li[key] = temp_li[key]

            li["order_id"] = oid
            li["status"] = _default_status
            li = li.drop(columns=["id"], axis=1)
            li = li.rename(columns={"line_item_price": "price"})
            return li.to_dict("records")

        cone_order = norm_lists(cone_order, ["brandCode"])

        """
        bumping the schema
        """
        return Order(
            **{
                "source_order_id": cone_order["id"],
                "ecommerce_source": "SHOPIFY" if channel == "Shopify" else "CREATEONE",
                "email": cone_order["email"],
                "brand_code": cone_order["brandCode"],
                "name": cone_order["code"],
                "description": cone_order["friendlyName"],
                "contracts_failed": [],
                "status": status,
                "shipping": make_shipping(cone_order),
                "order_items": _make_items(
                    cone_order, source_id=cone_order["id"], order_channel=channel
                ),
                "sales_channel": cone_order.get("salesChannel"),
                "order_channel": channel,
                "metadata": metadata or {},
                "ordered_at": cone_order["createdAt"],
                "type": cone_order.get("type"),
                "was_payment_successful": cone_order.get("was_payment_successful"),
                "was_balance_not_enough": cone_order.get("was_balance_not_enough"),
                "sell_brands_pkid": cone_order.get("sell_brands_pkid"),
                "line_item_json": cone_order.get("lineItemsInfo"),
                "request_type": cone_order.get("requestType"),
                "request_name": cone_order.get("requestName"),
                "channel_order_id": cone_order.get("channelOrderId"),
                "revenue_share_percentage_wholesale": cone_order.get(
                    "revenueSharePercentageWholesale"
                ),
                "revenue_share_percentage_retail": cone_order.get(
                    "revenueSharePercentageRetail"
                ),
            }
        )

    @staticmethod
    def from_shopify_payload(
        order, brand_code=None, elevate_shipping_attributes=False, metadata=None
    ):
        """
        map shopify topics to these resonance contract...
        """
        channel = "SHOPIFY"

        if elevate_shipping_attributes:
            # for some old schema we did this
            s = order.get("shipping_address") or order.get("billing_address")
            for k, v in s.items():
                order[f"shipping_{k}"] = v

        def make_shipping(d):
            s = {
                "name": d.get("customer_first_name", d.get("shipping_first_name", ""))
                + " "
                + d.get("customer_last_name", d.get("shipping_last_name", "")),
                "email": d.get("email"),
                "address1": d.get("shipping_address1"),
                "address2": d.get("shipping_address2"),
                "city": d.get("shipping_city"),
                "country": d.get("shipping_country"),
                "phone": d.get("shipping_phone"),
                "province": d.get("shipping_province"),
                "zipcode": d.get("shipping_zip"),
                #                 'shipping_country_code': 'null',
                #                  'shipping_province_code': 'null',
            }
            return s

        def make_items(p):
            li = pd.DataFrame(p.get("line_items"))
            # note we determine the order id here but we dont always have to do this depending how our crud works
            oid = uuid_str_from_dict(
                {
                    "id": str(p["id"]),
                    "order_channel": channel,
                }
            )

            li["order_id"] = oid
            # no price for create one
            li = li.reset_index()
            # because create one does not do qty we have to generate an order item id ourselves
            li["source_order_line_item_id"] = li["id"]
            return li.to_dict("records")

        return Order(
            **{
                "source_order_id": order["id"],
                "ecommerce_source": "SHOPIFY",
                "email": order["email"],
                # pass it in because its not actually in the payload from shopify so it would be added upstream and this may be easier
                "brand_code": brand_code or order["brand"],
                "name": order[
                    "order_number"
                ],  # e.g. 9988 for an order with name DIJ-9988
                "status": order["fulfillment_status"],
                "description": order["name"],
                "contracts_failed": [],
                "shipping": make_shipping(order),
                "order_items": make_items(order),
                "sales_channel": "ECOM",
                "order_channel": channel,
                "metadata": metadata or {},
                "ordered_at": order["created_at"],
                "type": order.get("type"),
                "was_payment_successful": order.get("was_payment_successful"),
                "was_balance_not_enough": order.get("was_balance_not_enough"),
            }
        )

    @staticmethod
    def from_shopify_warehouse_payload(order, brand_code_lookup, metadata=None):
        """
        map shopify topics to these resonance contract

        we need to supply a lookup between shops and brands to map to brand codes that we store!
        imagine for example a batch job that loads those once, ges lots of orders and then queries snowflake in batch  and then resolves the order here
        """
        channel = "SHOPIFY"
        brand_code = brand_code_lookup[order["SHOP_NAME"]]

        def make_shipping(d):
            s = {
                "name": d["SHIPPING_NAME"],
                "email": d.get("EMAIL"),
                "address1": d.get("SHIPPING_ADDRESS_ONE"),
                "address2": d.get("SHIPPING_ADDRESS_TWO"),
                "city": d.get("SHIPPING_CITY"),
                "country": d.get("SHIPPING_COUNTRY"),
                "phone": d.get("BILLING_PHONE"),
                "province": d.get("SHIPPING_PROVINCE"),
                "zipcode": d.get("SHIPPING_ZIP"),
                #                 'SHIPPING_COUNTRY_CODE': 'null',
                #                  'SHIPPING_PROVINCE_CODE': 'null',
            }
            return s

        def make_items(p):
            li = p.get("line_items")
            # note we determine the order id here but we dont always have to do this depending how our crud works
            # BE VERY CAREFUL - NEED To refactor this somewhere
            oid = uuid_str_from_dict(
                {
                    "id": str(p["SHOP_ORDER_ID"]),
                    "order_channel": channel,
                }
            )
            li = pd.DataFrame(li)
            li["order_id"] = oid
            # no price for create one
            li["price"] = li["PRICE"]
            li["sku"] = li["SKU"]
            li["quantity"] = li["QUANTITY"]
            li = li.reset_index()
            # because create one does not do qty we have to generate an order item id ourselves
            li["source_order_line_item_id"] = li["SHOP_ORDER_LINE_ITEM_ID"]
            return li.to_dict("records")

        return Order(
            **{
                "source_order_id": order["SHOP_ORDER_ID"],
                "ecommerce_source": "SHOPIFY",
                "email": order["EMAIL"],
                "brand_code": brand_code,
                "name": order["NAME"],
                "status": order["FULFILLMENT_STATUS"],
                "description": order["NAME"],
                "contracts_failed": [],
                "shipping": make_shipping(order),
                "order_items": make_items(order),
                "sales_channel": "ECOM",
                "order_channel": channel,
                "metadata": metadata or {},
                "ordered_at": order["ORDERED_AT"].isoformat(),
            }
        )

    @staticmethod
    def from_create_one_warehouse_payload(order, metadata=None):
        """
        map shopify topics to these resonance contract

        we need to supply a lookup between shops and brands to map to brand codes that we store!
        imagine for example a batch job that loads those once, ges lots of orders and then queries snowflake in batch  and then resolves the order here
        """
        channel = "CREATEONE" if order["ORDER_CHANNEL"] == "resmagic.io" else "SHOPIFY"

        # TODO:
        # status = ""

        def make_shipping(d):
            s = {
                "name": d["SHIPPING_NAME"],
                "email": d.get("CUSTOMER_EMAIL"),
                "address1": d.get("SHIPPING_ADDRESS_LINE_ONE"),
                "address2": d.get("SHIPPING_ADDRESS_LINE_TWO"),
                "city": d.get("SHIPPING_CITY"),
                "country": d.get("SHIPPING_COUNTRY"),
                "phone": d.get("BILLING_PHONE"),
                "province": d.get("SHIPPING_PROVINCE"),
                "zipcode": d.get("SHIPPING_ZIP"),
                #                 'SHIPPING_COUNTRY_CODE': 'null',
                #                  'SHIPPING_PROVINCE_CODE': 'null',
            }
            return s

        def make_items(p, source_order_id):
            li = p.get("line_items")
            # note we determine the order id here but we dont always have to do this depending how our crud works
            # BE VERY CAREFUL - NEED To refactor this somewhere
            oid = uuid_str_from_dict(
                {
                    "id": str(source_order_id),
                    "order_channel": channel,
                }
            )
            li = pd.DataFrame(li)

            # no price for create one
            li["price"] = li["LINE_ITEM_PRICE_USD"]
            li["sku"] = li["LINE_ITEM_SKU"]
            li["quantity"] = 1
            # 'ONE_STATUS': 'Done', Should we use these values for fulfillment status
            li = (
                li[["sku", "price", "ORDER_LINE_ITEM_ID", "quantity"]]
                .groupby("sku")
                .agg({"price": sum, "ORDER_LINE_ITEM_ID": set, "quantity": sum})
                .reset_index()
            )
            li["source_order_line_item_id"] = li["ORDER_LINE_ITEM_ID"].map(
                lambda x: ",".join(x)
            )
            li["order_id"] = oid

            return li.to_dict("records")

        # shopify provides an external order id but create one does not
        # we have already chosen the airtable record id in the kafka topic as the external and this "database" key seems analogous
        external_order_id_field = (
            "external_order_id".upper() if channel != "CREATEONE" else "ORDER_ID"
        )

        # print(f"SOURCE_ORDER_ID", order[external_order_id_field].strip())

        return Order(
            **{
                "source_order_id": order[external_order_id_field].strip(),
                "ecommerce_source": "SHOPIFY",
                "email": (order.get("CUSTOMER_EMAIL", "") or "").strip(),
                "brand_code": order["ORDER_BRAND_CODE"].strip(),
                "name": order["ORDER_KEY"].strip(),
                "description": order["ORDER_KEY"].strip(),
                "contracts_failed": [],
                "shipping": make_shipping(order),
                "order_items": make_items(
                    order, source_order_id=order[external_order_id_field].strip()
                ),
                "sales_channel": order["SALES_CHANNEL"],
                "order_channel": channel,
                "metadata": metadata or {},
                "status": order["ORDER_STATUS"],
                "ordered_at": order["CREATED_AT"].isoformat(),
            }
        )

    @staticmethod
    def process_shopify_order_payload(order, brand_code=None, schema=None):
        """
        map the schema to our kafka schema

        caller should load schema from source

        with open(res.utils.get_res_root() / 'res-schemas' / 'avro' / 'res-sell' / 'shopify' / 'orders.avsc') as f:
            SCHEMA = json.load(f)

        for webhook handler or other rest processes, load this once and assume it has not changed for speed - restart handler if schema changes

        """

        customer_fields = [
            "id",
            "first_name",
            "orders_count",
            "total_spent",
            "tags",
            "created_at",
        ]

        def _coerce_to_schema(o, schema_map):
            top_keys = [f["name"] for f in schema_map["fields"]]

            def _map(_o):
                # poor mans recursion
                def __map(k, v):
                    if k == "line_items":
                        sch = [
                            f for f in schema["fields"] if f["name"] == "line_items"
                        ][0]["type"]["items"]
                        return _coerce_to_schema(v, sch)
                    return v

                return {k: __map(k, v) for k, v in _o.items() if k in top_keys}

            if isinstance(o, list):
                return [_map(_o) for _o in o]
            return _map(o)

        # promote shipping
        if "shipping_address" in order:
            for k, v in order["shipping_address"].items():
                order[f"shipping_{k}"] = v
        # otherwise we cannot or it may already have been promoted

        # confusingly the line item id is used in the refunds but the id is used on the fulfillments
        refunds = [
            {"id": v["line_item_id"], "quantity": v["quantity"]}
            for r in order.get("refunds", [])
            for v in r["refund_line_items"]
        ]
        fulfillments = [
            {"id": v["id"], "quantity": v["quantity"]}
            for r in order.get("fulfillments", [])
            for v in r["line_items"]
        ]

        # promote line item tracking? not sure if we have everything
        for oi in order["line_items"]:
            oi["refunded_quantity"] = int(
                np.sum([v["quantity"] for v in refunds if v["id"] == oi["id"]] or 0)
            )
            oi["fulfilled_quantity"] = int(
                np.sum(
                    [v["quantity"] for v in fulfillments if v["id"] == oi["id"]] or 0
                )
            )
        if brand_code:
            order["brand"] = brand_code

        order["discount_codes"] = ",".join(
            discount["code"] for discount in order.get("discount_codes", [])
        )

        if "shipping_title" not in order:
            order["shipping_title"] = (
                order["shipping_lines"][0]["title"]
                if len(order["shipping_lines"]) > 0
                else ""
            )

        # promote customer
        if "customer" in order:
            for field in customer_fields:
                if order.get("customer"):
                    order["customer_" + field] = order["customer"].get(field, None)

        order = _coerce_to_schema(order, schema)

        return order

    ################################################################################################
    ################ ^ THOSE ARE THE 4 WAYS TO MAP FROM STREAMING OR HISTORY ON CHANNELS  ##########
    ################################################################################################
