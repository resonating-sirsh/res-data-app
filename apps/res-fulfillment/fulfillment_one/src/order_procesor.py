"""
Goals: (see diagram)

1. Extract services such as brand, meta one, inventory to separate functions/services
2. call said services once in the context of the order to compile states
3. add pydantic objects at all interfaces e.g. kafka, airtable, postgres, apis allowing us to
   a. validate 
   b. set default values and other boring stuff
   c. readability of intent on functions
4. refactor to create the high level easy to read flow which calls out to services etc
5. save at the end

"""

import json
import math
import sys

import arrow
import boto3

from schemas.pydantic.fulfillment_one import (
    CreateOrderPayload,
    FulfillmentGroupPayload,
    FulfillmentOrderPayload,
    LineItemJson,
    MetaOneStatus,
    SkuInventoryStatus,
)
from schemas.pydantic.payments import BrandAndSubscription

import res
from . import queries
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import ping_slack
from res.utils.logging import logger

gql = ResGraphQLClient()


from res.airtable.misc import FULFILLMENT_TABLE, ORDER_LINE_ITEMS, ORDERS_TABLE

CREATE_ORDER_REQUEST_KAFKA_TOPIC = "res_sell.fulfillment_api_order.create_request"
CREATE_MAKE_ONE_KAFKA_TOPIC = "res_make.make_one_request.create"


def _get_brand_status(brand_code):
    """
    takes a brand_code as an argument and retrieves brand details from a remote API.
    """
    if brand_code is None:
        return None

    brand_data = res.utils.get_with_token(
        f"https://data.resmagic.io/payments-api/GetBrandDetails/?brand_code={brand_code}",
        "RES_PAYMENTS_API_KEY",
    )

    return BrandAndSubscription(**brand_data.json())


def _build_sku_dict_and_insurenace_items(order_request):
    """
    takes an order_request object as an argument and builds a dictionary sku_dict where each key is a SKU from the order request's line items, and the value is an instance of LineItemJson containing details about the line item.
    The function iterates over each line item in order_request.line_items_info. If the SKU of the current line item is not already a key in sku_dict, it creates a new dictionary sku_payload with various properties of the line item and some default values. It then logs the sku_payload and adds a new entry to sku_dict with the SKU as the key and a new LineItemJson object created from sku_payload as the value.
    If the SKU of the current line item is already a key in sku_dict, it increments the total_needed property of the corresponding LineItemJson object by the quantity of the current line item.
    If the sales channel of the order request is "wholesale", it calculates an extra quantity as 20% of the line item's quantity, rounds it up to the nearest whole number using math.ceil, and adds it to the total_insurense_needed property of the corresponding LineItemJson object.
    """
    sku_dict = {}
    try:
        for item in order_request.line_items_info:
            if item.sku not in sku_dict:
                sku_payload = {
                    "sku": item.sku,
                    "total_needed": 0,
                    "total_created": 0,
                    "total_insurense_needed": 0,
                    "total_insurense_created": 0,
                    "quantity": item.quantity,
                    "channel_order_line_item_id": item.channel_order_id,
                    "ecommerce_order_number": item.order_number,
                    "line_item_price": item.price,
                    "shopify_fulfillment_status": item.shopify_fulfillment_status,
                    "shopify_fulfillableQuantity": item.shopify_fulfillable_quantity,
                    "product_variant_title": item.product_variant_title,
                    "variant_id": item.variant_id,
                    "product_name": item.product_name,
                    "customizations": item.customizations,
                    "is_sku_available": False,
                    "is_unit_available": False,
                    "should_be_hold": False,
                    "meta_one_status": MetaOneStatus(),
                    "sku_inventory_status": SkuInventoryStatus(),
                    "basic_cost_of_one": 0,
                    "hold_type": None,
                }
                logger.info(f"sku_payload: {sku_payload}")
                sku_dict[item.sku] = LineItemJson(**sku_payload)

            sku_dict[item.sku].total_needed += item.quantity

            if str(order_request.sales_channel).lower() == "wholesale":
                extra_qty = item.quantity * 0.2
                sku_dict[item.sku].total_insurense_needed += math.ceil(extra_qty)

        return sku_dict
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in building sku dict and insurance items - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def _get_cost_of_one(style_id, style_size):
    """
    takes two arguments: style_id and style_size. This function is designed to retrieve the cost and price breakdown of a specific style and size from a GraphQL API.
    """
    try:
        get_one_price = gql.query(
            queries.GET_STYLE_PRICE, {"id": style_id, "sizeCodes": [style_size]}
        )
        if len(get_one_price["data"]["style"]["onePrices"]) > 0:
            cost = get_one_price["data"]["style"]["onePrices"][0]["cost"]
            price_breakdown = get_one_price["data"]["style"]["onePrices"][0][
                "priceBreakdown"
            ]
            return cost, price_breakdown

        ping_slack(
            f"Error in getting cost of one - {style_id} - {style_size}",
            "fulfillment_api_alerts",
        )
        return None, []

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in getting cost of one - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def _get_retail_price(sales_channel, style_id):
    """
    takes two arguments: sales_channel and style_id. This function is designed to retrieve the retail price of a specific style from a GraphQL API.
    """
    try:
        logger.info(f"style_id: {style_id}")
        get_produce_price = gql.query(
            queries.GET_PRODUCT_PRICE, {"where": {"styleId": {"is": style_id}}}
        )
        logger.info(f"get_produce_price: {get_produce_price}")
        if len(get_produce_price["data"]["products"]["products"]) > 0:
            if sales_channel == "wholesale":
                return get_produce_price["data"]["products"]["products"][0][
                    "wholesalePrice"
                ]
            return get_produce_price["data"]["products"]["products"][0]["price"]

        ping_slack(
            f"Error in getting retail price - {sales_channel} - {style_id}",
            "fulfillment_api_alerts",
        )
        return None

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in getting retail price - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def _get_meta_one_status(line_items_info, order_request: FulfillmentOrderPayload):
    """
    takes two arguments: line_items_info and order_request. This function is designed to retrieve the meta one status of each line item in the order request from a GraphQL API.
    """

    logger.info(
        "--------------------------------------> _get_meta_one_status <--------------------------------------"
    )
    try:
        for sku in line_items_info:
            t_sku = sku.split(" ")
            size = t_sku[3]
            style_code = f"{t_sku[0][0:2]}-{t_sku[0][2:6]} {t_sku[1]} {t_sku[2]}"
            get_style = gql.query(
                queries.GET_STYLE,
                {"where": {"code": {"is": style_code}, "isTrashed": {"is": False}}},
            )
            if len(get_style["data"].get("styles").get("styles")) > 0:
                style_id = get_style["data"].get("styles").get("styles")[0].get("id")
                style_size = size

                # get factory price and price breakdown
                basic_cost, price_breakdown = _get_cost_of_one(style_id, style_size)

                # get retail price, if the order is wholesale, get the wholesale price
                price = _get_retail_price(
                    str(order_request.sales_channel).lower(), style_id
                )

                style_record = get_style["data"].get("styles").get("styles")[0]
                is_style_3d_onboarded = style_record.get("isStyle3dOnboarded")
                style_created_at = style_record.get("createdAt")
                styles_started_being_created_in_3d_at = "2023-02-11T00:00:00.000Z"
                style_payload = {
                    "id": style_record.get("id"),
                    "name": style_record.get("name"),
                    "style_code": style_code,
                    "body_code": style_record.get("body").get("code"),
                    "body_version": style_record.get("body").get(
                        "patternVersionNumber"
                    ),
                    "material_code": style_record.get("material").get("code"),
                    "color_code": style_record.get("color").get("code"),
                    "size_code": style_size,
                    "basic_cost_of_one": basic_cost,
                    "price": price,
                    "style_version": style_record.get("version"),
                    "_json_price_breakdown": price_breakdown,
                    "is_one_ready": style_record.get("isOneReady"),
                    "is_style_3d": (
                        style_created_at > styles_started_being_created_in_3d_at
                        and is_style_3d_onboarded
                    ),
                }
                meta_one_status = MetaOneStatus(**style_payload)
                line_items_info[sku].meta_one_status = meta_one_status
                line_items_info[sku].basic_cost_of_one = basic_cost
                line_items_info[sku].is_sku_available = True
        logger.info(
            " --------------------------------------> end _get_meta_one_status <--------------------------------------\n\n"
        )
        return line_items_info
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in getting meta one status - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def _get_inventory_status(line_items_info):
    """
    takes a dictionary line_items_info as an argument and retrieves the inventory status of each line item in the order request from a GraphQL API.
    """
    logger.info(
        "--------------------------------------> _get_inventory_status <--------------------------------------"
    )
    try:
        for sku in line_items_info:
            get_inventory = gql.query(
                queries.GET_INVENTORY_UNIT,
                {
                    "where": {
                        "sku": {"is": sku},
                        "reservationStatus": {"is": None},
                        "isAvailable": {"is": True},
                        "isApprovedDistributionCenter": {"is": True},
                    }
                },
            )
            are_units_available = get_inventory.get("data")["units"]["count"] > 0
            unit = None
            if are_units_available:
                unit = get_inventory["data"]["units"]["units"][0]
                unit_payload = {
                    "id": unit.get("id"),
                    "warehouse_location": unit.get("warehouseLocation"),
                    "warehouse_bin_location": unit.get("warehouseBinLocations"),
                    "bin_location": unit.get("binLocation"),
                    "link_to_inventory": f"https://airtable.com/tblhCz4t3QVjxWOpv/{unit['id']}",
                    "unit_order_number": unit.get("resMagicOrderNumber"),
                }
                sku_inventory_status = SkuInventoryStatus(**unit_payload)
                line_items_info[sku].sku_inventory_status = sku_inventory_status
                line_items_info[sku].is_unit_available = are_units_available
        logger.info(
            "--------------------------------------> end _get_inventory_status <--------------------------------------\n\n"
        )
        return line_items_info
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in getting inventory status - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def _should_be_hold(
    line_items_info,
    order_request: FulfillmentOrderPayload,
    brand_status: BrandAndSubscription,
):
    """
    takes three arguments: line_items_info, order_request, and brand_status. This function is designed to check if the line item should be put on hold based on various conditions such as whether the order is a sample, whether the brand is in the whitelist for nonpayment, and whether the style is 3D.
    """
    try:
        for sku in line_items_info:
            # if the order is not a sample, validate if was pay and the style is 3D
            if not order_request.is_sample:
                if brand_status.must_pay_before_make:
                    if str(order_request.sales_channel).lower() != "first one":
                        line_items_info[sku].should_be_hold = True
                        line_items_info[sku].hold_type = "Non Payment"

                if line_items_info[sku].meta_one_status.is_style_3d:
                    line_items_info[sku].should_be_hold = True
                    line_items_info[sku].hold_type = "3D Style"

                # if the cost of one is not found, hold the line item
                if not line_items_info[sku].meta_one_status.basic_cost_of_one:
                    line_items_info[sku].should_be_hold = True
                    line_items_info[sku].hold_type = "Has no cost"

                # if the price is not found, hold the line item
                if not line_items_info[sku].meta_one_status.price and str(
                    order_request.sales_channel
                ).lower() in [
                    "wholesale",
                    "ecom",
                ]:
                    line_items_info[sku].should_be_hold = True
                    line_items_info[sku].hold_type = "Has no price"

        return line_items_info
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = (
            f"Error in checking if the line item should be hold - {exc_value} "
        )
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def process_line_items(
    order_request: FulfillmentOrderPayload, brand_status: BrandAndSubscription
):
    """
    takes two arguments: order_request and brand_status. This function is designed to process the line items in the order request, and return the updated line_items_info and list_line_items_to_save.
    """
    logger.info(
        "--------------------------------------> process_line_items <--------------------------------------"
    )
    try:
        line_items_info = _build_sku_dict_and_insurenace_items(order_request)
        logger.info(f"line_items_info: {line_items_info}\n")

        line_items_info = _get_meta_one_status(line_items_info, order_request)
        logger.info(f"meta_one_status_for_all_skus: {line_items_info}\n")

        line_items_info = _get_inventory_status(line_items_info)
        logger.info(f"inventory_levels_for_all_skus: {line_items_info}\n")

        line_items_info = _should_be_hold(line_items_info, order_request, brand_status)
        logger.info(f"should_be_hold_for_all_skus: {line_items_info}\n")

        list_line_items_to_save = []
        order_sales_channel = str(order_request.sales_channel).lower()
        for sku in line_items_info:
            list_line_items_to_save = create_line_items_for_saving(
                line_items_info, sku, order_sales_channel
            )
        logger.info(
            "--------------------------------------> end process_line_items <--------------------------------------\n\n"
        )
        return line_items_info, list_line_items_to_save
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in processing line items - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def create_line_items_for_saving(line_items_info, sku, order_sales_channel):
    """
    takes three arguments: line_items_info, sku, and order_sales_channel. This function is designed to create a list of line items to be saved to an external data store, and return it.
    """
    list_line_items_to_save = []
    for _ in range(
        line_items_info[sku].total_created, line_items_info[sku].total_needed
    ):
        line_item_payload = build_line_item_payload(
            line_items_info[sku], order_sales_channel
        )
        line_items_info[sku].total_created += 1
        list_line_items_to_save.append(line_item_payload)

    # Try to create insurance line items for wholesale orders
    if order_sales_channel == "wholesale":
        for _ in range(
            line_items_info[sku].total_insurense_created,
            line_items_info[sku].total_insurense_needed,
        ):
            line_item_payload = build_line_item_payload(
                line_items_info[sku],
                order_sales_channel,
                is_insurance=True,
            )
            line_items_info[sku].total_insurense_created += 1
            list_line_items_to_save.append(line_item_payload)

    return list_line_items_to_save


def build_line_item_payload(line_item_info, sales_channel, is_insurance=False):
    """
    takes three arguments: line_item_info, sales_channel, and is_insurance. This function is designed to build a dictionary line_item_payload containing the details of a line item, and return it.
    """
    logger.info(
        "--------------------------------------> build_line_item_payload <--------------------------------------"
    )
    try:
        # if the line item price is 0, use the price from the meta one status
        line_item_price = (
            line_item_info.line_item_price
            if line_item_info.line_item_price
            else line_item_info.meta_one_status.price
        )

        line_item_payload = {
            "LINEITEM  SKU": line_item_info.sku,
            "quantity": line_item_info.quantity,
            "platform_fulfillable_quantity": line_item_info.shopify_fulfillableQuantity,
            "ORDER_LINK": None,
            "__ordernumber": line_item_info.ecommerce_order_number,
            "FULFILLMENT_STATUS": "HOLD",
            "lineitem_id": line_item_info.channel_order_line_item_id,
            "Lineitem price": str(line_item_price),
            "platform_fulfillment_status": line_item_info.shopify_fulfillment_status,
            "Group Fulfillment": None,
            "Product Variant Title": line_item_info.product_variant_title,
            "Product Name": line_item_info.product_name,
            "customizations": line_item_info.customizations,
            "styleId": None,
            "Factory Invoice Price": None,
        }

        if is_insurance:
            line_item_payload["Flag For Review Tag"] = ["Extra SKU"]
            line_item_payload[
                "Flag for Review Reason"
            ] = "Insurence SKU generated by the system."
            line_item_payload["Is an extra item?"] = True

        if line_item_info.is_sku_available:
            line_item_payload["styleId"] = line_item_info.meta_one_status.id

            if not line_item_info.should_be_hold:
                line_item_payload["FULFILLMENT_STATUS"] = "PENDING_INVENTORY"

            if line_item_info.meta_one_status.basic_cost_of_one:
                _factory_invoice_price = (
                    math.floor(line_item_info.meta_one_status.basic_cost_of_one * 100)
                    / 100
                )
                line_item_payload["Factory Invoice Price"] = _factory_invoice_price
                line_item_payload["_json_price_breakdown"] = json.dumps(
                    line_item_info.meta_one_status._json_price_breakdown
                )

        if (
            line_item_info.is_unit_available
            and not is_insurance
            and not line_item_info.should_be_hold
        ):
            inventory = line_item_info.sku_inventory_status
            line_item_payload["inventory_id"] = inventory.id
            line_item_payload["Warehouse Location"] = inventory.warehouse_location
            line_item_payload["link_to_inventory"] = inventory.link_to_inventory
            line_item_payload["Stock State Day of Order"] = "In Stock"
            line_item_payload["Inventory Checkin Type"] = "Found In Inventory"
            line_item_payload["Unit Ready in Warehouse?"] = True
            line_item_payload["Active ONE Number"] = inventory.unit_order_number

            if not line_item_info.should_be_hold:
                line_item_payload["FULFILLMENT_STATUS"] = "HOLD_FOR_GROUP_FULFILLMENT"

            # if the ONE is in NYC and this order is not a Wholesale order,
            # then set the fulfillment status to PROCESSING
            if inventory.warehouse_location == "NYC" and sales_channel != "wholesale":
                line_item_payload["FULFILLMENT_STATUS"] = "PROCESSING"
                line_item_payload["inventory_location"] = inventory.bin_location

        if line_item_info.should_be_hold:
            line_item_payload["Flag For Review Tag"] = [
                f"HOLD Line item, {line_item_info.hold_type}"
            ]
            line_item_payload[
                "Flag for Review Reason"
            ] = f"HOLD Line Item, {line_item_info.hold_type}"
            line_item_payload["Flag for Review"] = True

        logger.info(
            "--------------------------------------> end build_line_item_payload <--------------------------------------\n\n"
        )
        return line_item_payload

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in processing line items - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def sent_order_request_to_kafka(order_request: CreateOrderPayload):
    """
    takes an order_request object as an argument and sends a message to a Kafka topic..
    """
    try:
        logger.info(
            "--------------------------------------> sent_order_request_to_kafka <--------------------------------------"
        )
        kafka_message = {
            "request_name": order_request.request_name,
            "brand_code": order_request.brand_code,
            "email": order_request.email,
            "order_id": order_request.channel_order_id,
            "order_channel": order_request.order_channel,
            "sales_channel": order_request.sales_channel,
            "shipping_name": order_request.shipping_name,
            "shipping_address1": order_request.shipping_address1,
            "shipping_address2": order_request.shipping_address2,
            "shipping_city": order_request.shipping_city,
            "shipping_country": order_request.shipping_country,
            "shipping_province": order_request.shipping_province,
            "shipping_zip": order_request.shipping_zip,
            "shipping_phone": order_request.shipping_phone,
            "revenue_share_percentage": order_request.revenue_share_percentage,
            "is_sample": order_request.is_sample,
            "channel_order_id": order_request.channel_order_id,
            "line_items_info": [
                {"sku": item.sku, "quantity": item.quantity}
                for item in order_request.line_items_info
            ],
        }
        logger.info(f"kafka_message: {kafka_message}")
        res.connectors.load("kafka")[CREATE_ORDER_REQUEST_KAFKA_TOPIC].publish(
            kafka_message, use_kgateway=True
        )
        logger.info(
            "--------------------------------------> end sent_order_request_to_kafka <--------------------------------------\n\n"
        )
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in sending order request to kafka - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def save_order_record_to_airtable(
    fulfillment_order_payload: FulfillmentOrderPayload, brand: BrandAndSubscription
):
    """
    takes two arguments: fulfillment_order_payload and brand. This function is designed to save the details of an order to an external data store, and return the updated fulfillment_order_payload.
    """
    logger.info(
        "--------------------------------------> save_order_record <--------------------------------------"
    )
    try:
        line_items_info = [
            {key: value for key, value in item.dict().items() if value is not None}
            for item in fulfillment_order_payload.line_items_info
        ]
        order_payload = {
            "BRAND": [brand.fulfill_record_id],
            "Email": fulfillment_order_payload.email,
            "Shipping Name": fulfillment_order_payload.shipping_name,
            "Shipping Address1": fulfillment_order_payload.shipping_address1,
            "Shipping Address2": fulfillment_order_payload.shipping_address2,
            "Shipping City": fulfillment_order_payload.shipping_city,
            "Shipping Country": fulfillment_order_payload.shipping_country,
            "Shipping Province": fulfillment_order_payload.shipping_province,
            "Shipping Zip": fulfillment_order_payload.shipping_zip,
            "Shipping Phone": fulfillment_order_payload.shipping_phone,
            "Sales Channel": fulfillment_order_payload.sales_channel,
            "Order Channel": fulfillment_order_payload.order_channel,
            "lineItemsJson": json.dumps(line_items_info),
            "Request Name": fulfillment_order_payload.request_name,
            "Request_Type": fulfillment_order_payload.request_type,
            "ORDER_ID": fulfillment_order_payload.channel_order_id,
            "was_payment_successful": fulfillment_order_payload.was_payment_successful,
        }
        logger.info(f"order_payload: {order_payload}")
        logger.info(
            "--------------------------------------> end save_order_record <--------------------------------------\n\n"
        )

        saved_order = ORDERS_TABLE.create(order_payload)
        fulfillment_order_payload.order_airtable_id = saved_order["id"]
        fulfillment_order_payload.order_name = saved_order.get("fields").get("KEY")
        fulfillment_order_payload.number = saved_order.get("fields").get("__number")
        fulfillment_order_payload.flag_for_review_notes = saved_order.get("fields").get(
            "Flag for Review Notes"
        )
        fulfillment_order_payload.friendly_name = saved_order.get("fields").get(
            "Friendly Name"
        )
        sync_order_to_mongo(
            fulfillment_order_payload.order_airtable_id, "tblhtedTR2AFCpd8A"
        )
        logger.info(f"fulfillment_order_payload: {fulfillment_order_payload}")
        return fulfillment_order_payload
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in creating airtable order record - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def save_group_fulfillment_to_airtable(order_request):
    """
    takes an order_request object as an argument and saves the details of a group fulfillment to an external data store, and returns the updated order_request.
    """
    try:
        logger.info(
            "--------------------------------------> save_group_fulfillment <--------------------------------------"
        )
        if order_request.order_airtable_id is None:
            raise Exception(" Fail to create group fulfillment, order_id is None")

        payload = {
            "Orders": [order_request.order_airtable_id],
            "Fulfillment Setting": "Complete - hold until all units are ready",
            "Include Packing List?": "Yes - default",
            "Sales Channel": order_request.sales_channel,
            "Group Fulfillment Status": "IN PROGRESS",
            "Use Shopify address?": False,
        }
        create_fulfillment = FULFILLMENT_TABLE.create(payload)
        fulfillment_group_payload = {
            "id": create_fulfillment["id"],
            "name": create_fulfillment["fields"]["Name"],
        }
        logger.info(
            "--------------------------------------> end save_group_fulfillment <--------------------------------------\n\n"
        )
        sync_order_to_mongo(fulfillment_group_payload.get("id"), "tblZ6JDrAtMhxqFnT")
        return FulfillmentGroupPayload(**fulfillment_group_payload)
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = (
            f"Error in creating airtable groupfulfillment record - {exc_value} "
        )
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def save_order_item_records_to_airtable(
    line_items,
    order_record,
    fulfillment_record,
):
    """
    takes four arguments: line_items, order_record, fulfillment_record, and line_item_json. This function is designed to save the details of line items to an external data store, and return the updated line_item_json.
    """
    logger.info(
        "--------------------------------------> save_order_item_records <--------------------------------------"
    )
    try:
        for item in line_items:
            item["ORDER_LINK"] = [order_record.order_airtable_id]
            item["Group Fulfillment"] = [fulfillment_record.id]
            logger.info(f"item: {item}")
            new_record = ORDER_LINE_ITEMS.create(item)
            logger.info(f"new_record: {new_record.get('id')}")
            item["id"] = new_record["id"]
            sync_order_to_mongo(new_record["id"], "tblUcI0VyLs7070yI")
        logger.info(
            "--------------------------------------> end save_order_item_records <--------------------------------------\n\n"
        )
        return line_items
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = (
            f"Error in creating airtable order line item record - {exc_value} "
        )
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def build_line_item_json_for_hasura(saved_line_items, line_item_json):
    logger.info(
        "--------------------------------------> build_line_item_json_for_hasura <--------------------------------------"
    )
    try:
        line_items = []

        for item in saved_line_items:
            _line_item_json = line_item_json[item["LINEITEM  SKU"]]
            _line_item = {
                "id": item["id"],
                "sku": _line_item_json.sku,
                "quantity": _line_item_json.quantity,
                "ecommerce_quantity": _line_item_json.shopify_fulfillableQuantity,
                "ecommerce_order_number": _line_item_json.ecommerce_order_number,
                "ecommerce_line_item_id": _line_item_json.channel_order_line_item_id,
                "ecommerce_fulfillment_status": _line_item_json.shopify_fulfillment_status,
                "product_variant_title": _line_item_json.product_variant_title,
                "product_name": _line_item_json.product_name,
                "status": item["FULFILLMENT_STATUS"],
                "styleId": item["styleId"],
                "line_item_price": _line_item_json.line_item_price,
                "basic_cost_of_one": item[
                    "Factory Invoice Price"
                ],  # this factory invoice price is reference to basic_cost_price
                "customization": _line_item_json.customizations,
                "inventory_airtable_id": item.get("inventory_id"),
                "warehouse_location": item.get("Warehouse Location"),
                "stock_state_day_of_order": item.get("Stock State Day of Order"),
                "inventory_checkin_type": item.get("Inventory Checkin Type"),
                "unit_ready_in_warehouse": item.get("Unit Ready in Warehouse?"),
                "one_number": item.get("Active ONE Number"),
            }
            line_items.append(_line_item)
        logger.info(
            "--------------------------------------> end build_line_item_json_for_hasura <--------------------------------------\n\n"
        )
        return line_items
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in building line item json for hasura - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def save_order_to_hasura(
    fulfillment_order_payload: FulfillmentOrderPayload,
    line_item_json,
    brand_status: BrandAndSubscription,
):
    """
    takes four arguments: order_request, saved_order_record, line_item_json, and brandStatus. This function is designed to save the details of an order to an external data store, and return the updated saved_order_record.
    """
    try:
        logger.info(
            "--------------------------------------> save_order_to_hasura <--------------------------------------"
        )
        from res.flows.sell.orders.FulfillmentNode import create_one_handler

        order_payload = {
            "id": fulfillment_order_payload.order_airtable_id,
            "brandCode": fulfillment_order_payload.brand_code,
            "code": fulfillment_order_payload.number,
            "email": fulfillment_order_payload.email,
            "friendlyName": fulfillment_order_payload.friendly_name,
            "shippingName": fulfillment_order_payload.shipping_name,
            "shippingAddress1": fulfillment_order_payload.shipping_address1,
            "shippingAddress2": fulfillment_order_payload.shipping_address2,
            "shippingCity": fulfillment_order_payload.shipping_city,
            "shippingCountry": fulfillment_order_payload.shipping_country,
            "shippingPhone": fulfillment_order_payload.shipping_phone,
            "shippingProvince": fulfillment_order_payload.shipping_province,
            "shippingZip": fulfillment_order_payload.shipping_zip,
            "isClosed": False,
            "lineItemsInfo": line_item_json,
            "was_payment_successful": fulfillment_order_payload.was_payment_successful,
            "was_balance_not_enough": False,
            "salesChannel": fulfillment_order_payload.sales_channel,
            "orderChannel": fulfillment_order_payload.order_channel,
            "requestType": fulfillment_order_payload.request_type,
            "requestName": fulfillment_order_payload.request_name,
            "channelOrderId": fulfillment_order_payload.channel_order_id,
            "createdAt": arrow.now().format("YYYY-MM-DDTHH:mm:ss.SSSZ"),
            "type": fulfillment_order_payload.request_type,
            "sell_brands_pkid": brand_status.brand_pkid,
            "revenueSharePercentageWholesale": brand_status.payments_revenue_share_wholesale,
            "revenueSharePercentageRetail": brand_status.payments_revenue_share_ecom,
        }

        logger.info(f"-------> send order_payload to postgress: {order_payload}")
        hander_retval = create_one_handler(order_payload)
        logger.info(f"hander_retval: {hander_retval}")
        fulfillment_order_payload.postgres_id = hander_retval.id
        logger.info(
            "--------------------------------------> end save_order_to_hasura <--------------------------------------\n\n"
        )
        return fulfillment_order_payload

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in creating order in hasura - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def generate_prod_request(
    saved_line_items,
    line_item_json,
    order_request: FulfillmentOrderPayload,
    brand_status: BrandAndSubscription,
):
    """
    takes three arguments: line_item_json, order_request, and brand_status. This function is designed to generate and post a message to a Kafka topic.
    """
    try:
        logger.info(
            "--------------------------------------> generate_prod_request <--------------------------------------"
        )

        for item in saved_line_items:
            logger.info(f"item: {item}")
            _line_item = line_item_json[item["LINEITEM  SKU"]]
            logger.info(f"_line_item: {_line_item}")
            if not _line_item.should_be_hold:
                kafka_message = {
                    "type": order_request.request_type,
                    "style_id": str(_line_item.meta_one_status.id),
                    "style_version": str(_line_item.meta_one_status.style_version),
                    "body_code": _line_item.meta_one_status.body_code,
                    "body_version": int(_line_item.meta_one_status.body_version),
                    "material_code": _line_item.meta_one_status.material_code,
                    "color_code": _line_item.meta_one_status.color_code,
                    "size_code": _line_item.meta_one_status.size_code,
                    "brand_code": brand_status.brand_code,
                    "brand_name": brand_status.name,
                    "sales_channel": order_request.sales_channel,
                    "order_line_item_id": item.get("id"),
                    "sku": _line_item.sku,
                    "order_number": f"{brand_status.brand_code}-{_line_item.ecommerce_order_number}",
                    "request_name": order_request.request_name,
                    "channel_line_item_id": _line_item.channel_order_line_item_id,
                    "channel_order_id": (
                        order_request.order_id if order_request.order_id else ""
                    ),
                    "line_item_creation_date": arrow.now().format(
                        "YYYY-MM-DDTHH:mm:ss.SSSZ"
                    ),
                }
                logger.info(
                    f"payload to sent to CREATE_MAKE_ONE_KAFKA_TOPIC: {kafka_message}"
                )
                res.connectors.load("kafka")[CREATE_MAKE_ONE_KAFKA_TOPIC].publish(
                    kafka_message, use_kgateway=True
                )

        logger.info(
            "--------------------------------------> end generate_prod_request <--------------------------------------\n\n"
        )
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"Error in generate prod request - {exc_value} "
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)


def notify_order_created(order_request: FulfillmentOrderPayload, line_item_json):
    from res.utils import post_with_token

    payload = {
        "notificationId": "recBWh8rafByUISDt",
        "brandCode": order_request.brand_code,
        "data": {
            "orderNumber": order_request.number,
            "lineItems": [
                {
                    "styleName": line_item_json[item.sku].meta_one_status.name,
                }
                for item in order_request.line_items_info
            ],
        },
    }
    logger.info(f"payload: {payload}")
    post_with_token(
        "https://data.resmagic.io/notification-api/submit",
        secret_name="FULFILLMENT_ONE_API_KEY",
        json=payload,
    )


def sync_order_to_mongo(request_id, table_id):
    """
    Some stuff still needs the graph api
    and it breaks if mongo isnt up to date with airtable seemingly.
    """
    client = boto3.client("lambda", region_name="us-east-1")

    client.invoke(
        FunctionName="res-meta-prod-sync-mongodb-collection-from-airtable",
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {"record_id": request_id, "table_id": table_id, "dry_run": False}
        ),
    )


# nudge


def create_order(order_request: CreateOrderPayload):
    """
    takes an order_request object as an argument and processes the order request, saving the details of the order to an external data store, and returning the updated order_request.
    """
    try:
        logger.info(
            "--------------------------------------> create_order <--------------------------------------"
        )

        # sent to kafka topic order_request before we process all data
        # sent_order_request_to_kafka(order_request)

        # sent to slack the order request received
        ping_slack(
            f"""[FULFILLMENT-API] Order {order_request.request_name} from {order_request.sales_channel} request by {order_request.email} under brand {order_request.brand_code} received
            ```
            {order_request}
            ```
            """,
            "fulfillment_api_alerts",
        )
        _order_request = order_request.dict()
        # set the fulfillment_order_payload
        fulfillment_order_payload = FulfillmentOrderPayload(**_order_request)
        fulfillment_order_payload.request_type = (
            "Sample" if order_request.is_sample else "Finished Goods"
        )
        fulfillment_order_payload.was_payment_successful = (
            str(order_request.sales_channel).lower() == "first one"
        )
        brand_status: BrandAndSubscription = _get_brand_status(
            fulfillment_order_payload.brand_code
        )
        line_item_json, line_item_to_save = process_line_items(
            fulfillment_order_payload, brand_status
        )

        logger.info("\n\n")
        logger.info(f"fulfillment_order_payload: {fulfillment_order_payload.dict()}")
        logger.info(f"brand_status: {brand_status.dict()}")
        logger.info(f"line_item_json: {line_item_json}")
        logger.info(f"line_item_to_save: {line_item_to_save}")

        # push to airtable with error checking
        fulfillment_order_payload: FulfillmentOrderPayload = (
            save_order_record_to_airtable(fulfillment_order_payload, brand_status)
        )
        new_group_fulfillment: FulfillmentGroupPayload = (
            save_group_fulfillment_to_airtable(fulfillment_order_payload)
        )
        line_item_to_save = save_order_item_records_to_airtable(
            line_item_to_save, fulfillment_order_payload, new_group_fulfillment
        )

        """
        Update external services here
        """

        line_item_json_for_hasura = build_line_item_json_for_hasura(
            line_item_to_save, line_item_json
        )

        # save new record into hasura
        fulfillment_order_payload = save_order_to_hasura(
            fulfillment_order_payload, line_item_json_for_hasura, brand_status
        )

        # generate and post kafka message - todo check for errors
        generate_prod_request(
            line_item_to_save, line_item_json, fulfillment_order_payload, brand_status
        )

        # notify order created
        notify_order_created(fulfillment_order_payload, line_item_json)

        logger.info(
            "--------------------------------------> end create_order <--------------------------------------\n\n"
        )

        ping_slack(
            f"""[FULFILLMENT-API] Order {fulfillment_order_payload.request_name} from {fulfillment_order_payload.sales_channel} request by {fulfillment_order_payload.email} under brand {fulfillment_order_payload.brand_code} created
            ```
            {fulfillment_order_payload}
            ```
            """,
            "fulfillment_api_alerts",
        )

        return fulfillment_order_payload

    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        enhanced_msg = f"This order {order_request.request_name} from {order_request.sales_channel} request by {order_request.email} under brand {order_request.brand_code} Fail --> {exc_value} "
        # general comment on the other exceptions - you should try to do somethign like you have done here where you help to identify what the order might have been/who it was for etc. it wont always be possible but you should have a look to see if it is possible
        raise exc_type(enhanced_msg).with_traceback(exc_traceback)
