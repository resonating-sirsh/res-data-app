import arrow
import http.client
import json

from res.utils import logger, secrets_client
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

from .queries import (
    UPDATE_INVENTORY,
    GET_INVENTORY_DATA,
    GET_LINE_ITEM,
    GET_MAKE_DATA,
    UPDATE_LINE_ITEM,
    UPDATE_MAKE_ONE_PRODUCTION,
)

HASURA_CLIENT = Client()


def handler(event):
    logger.info("Starting process..")
    logger.info(f"Event: {event}")

    # get hasura_record_info data
    hasura_record_info = event.get("insert_make_make_order_in_inventory").get(
        "returning"
    )[0]
    hasura_id = hasura_record_info.get("id")

    # get inventory record from graphQl
    inventory_info = get_inventory_data(hasura_record_info["one_number"])

    if not inventory_info:
        logger.error(
            f"Could not find inventory with one number: {hasura_record_info['one_number']}"
        )
        return False

    # get make record from graphQl
    make_info = get_make_data(hasura_record_info["one_number"])
    logger.info({"get make record from graphQl": make_info})

    # assign the allocation status from the make record
    is_one_reserved = make_info.get("allocationStatus", "Unreserved") == "Reserved"

    if not is_one_reserved:
        # is_one_reserved will be true if make_info.get("orderLineItem").get("fulfillmentStatus") is one of the following:
        # HOLD_FOR_GROUP_FULFILLMENT, PROCESSING, PENDING_INVENTORY
        if make_info.get("orderLineItem"):
            is_one_reserved = make_info.get("orderLineItem", {}).get(
                "fulfillmentStatus", ""
            ) in ["HOLD_FOR_GROUP_FULFILLMENT", "PENDING_INVENTORY"]

    is_sku_match = True

    line_item_info = {}
    if make_info.get("orderLineItem"):
        # get lineItem record from graphQL
        line_item_info = get_line_item_data_by_id(
            make_info.get("orderLineItem").get("id")
        )
        logger.info({"get the line item record from graphQl": line_item_info})

    if is_one_reserved:
        # get sku from the request if the make request is not found
        # get the sku from line_item record

        request_sku = None
        if make_info:
            request_sku = make_info["sku"]

        if not make_info and request_sku is None:
            request_sku = line_item_info["sku"]

        # if the sku of the request is not the same of the
        if request_sku != inventory_info["barcodeUnit"]:
            logger.info("SKU mismatch, flagging inventory record")
            is_sku_match = False

    payload_to_update = {"inventory_payload": {}, "line_item_payload": {}}

    if not is_sku_match:
        logger.info(" SKU mismatch, flagging inventory record")
        # update inventory with mismatch sku
        flag_for_review_notes = (
            f"{inventory_info.get('flagForReviewNotes')} - Error - ⛔️️ Sku Mismatch"
        )
        fields_to_update = {
            "gateSkuMismatch": "Error - ⛔️️ Sku Mismatch",
            "isFlagForReview": True,
            "flagForReviewNotes": flag_for_review_notes,
        }

        return logger.info(
            graph_request(
                UPDATE_INVENTORY,
                {"id": inventory_info["id"], "input": fields_to_update},
            )
        )

    # if the ONE is not reserved
    # then search for line item record by ONE Number
    # if not found search for the most late SKU in Assembly
    if not is_one_reserved:
        logger.info("ONE is not reserved")
        logger.info("Search if there is a line item record with the same ONE Number")
        line_item_info = get_line_item_data_by_order_number(
            hasura_record_info["one_number"]
        )
        logger.info({"line_item_info": line_item_info})
        if not line_item_info:
            logger.info("search for the most late SKU in Assembly")
            line_item_info = search_for_late_sku_in_assembly(
                inventory_info["barcodeUnit"]
            )

    # build the line item object to update the record
    if line_item_info:
        logger.info("Building line item")
        payload_to_update["line_item_payload"] = validate_line_item(
            line_item_info, inventory_info
        )

        # this will indicate if the ONE have a valid line item to reserve
        will_be_one_reserved = (
            payload_to_update["line_item_payload"].get("fulfillmentStatus", None)
            is not None
        )

        # build the inventory object to update the record
        logger.info("Building inventory")
        payload_to_update["inventory_payload"] = build_update_inventory(
            inventory_info, line_item_info, will_be_one_reserved
        )

        # since the ONE is not reserved and we have a new line_item_info
        # we need to make the ONE that is in prod unreserved and swap all the information
        # to this new line item
        if not is_one_reserved:
            logger.info("Making the ONE that is in production unreserved")
            update_reservation(
                line_item_info,
                line_item_info["makeOneProductionRequest"],
                hasura_record_info["one_number"],
            )
        logger.info("Update Inventory and Line Item record")
        update_inventory(inventory_info["id"], payload_to_update["inventory_payload"])
        update_line_item_record(
            line_item_info["id"], payload_to_update["line_item_payload"]
        )
        logger.info(
            {
                "one_number": hasura_record_info["one_number"],
                "payload_to_update": payload_to_update,
            }
        )
        update_payment_api(
            hasura_record_info["one_number"],
            payload_to_update["line_item_payload"]["fulfillmentStatus"],
        )
        logger.info("Done...\n")
    else:
        logger.info("Could not find the line item record")
        payload_to_update["inventory_payload"] = build_update_inventory(
            inventory_info, make_info.get("orderLineItem", {}), False
        )
        logger.info({"payload_to_update": payload_to_update})
        update_inventory(inventory_info["id"], payload_to_update["inventory_payload"])
        logger.info("Done...\n")

    # TODO: update heasura record...


def get_inventory_data(one_number):
    response = graph_request(
        GET_INVENTORY_DATA,
        {
            "where": {
                "resMagicOrderNumber": {
                    "is": int(one_number),
                }
            }
        },
    )["units"]["units"]
    return response[0] if len(response) > 0 else None


def get_line_item_data_by_order_number(one_number):
    response = graph_request(
        GET_LINE_ITEM,
        {
            "where": {
                "fulfillmentStatus": {"is": "PENDING_INVENTORY"},
                "activeOneNumber": {
                    "is": str(one_number),
                },
            }
        },
    )["orderLineItems"]["orderLineItems"]
    return response[0] if len(response) > 0 else None


def get_line_item_data_by_id(line_item_id):
    response = graph_request(
        GET_LINE_ITEM,
        {
            "where": {
                "id": {
                    "is": str(line_item_id),
                },
            }
        },
    )["orderLineItems"]["orderLineItems"]
    return response[0] if len(response) > 0 else None


def get_make_data(one_number):
    response = graph_request(
        GET_MAKE_DATA,
        {
            "where": {
                "orderNumber": {
                    "is": str(one_number),
                }
            }
        },
    )["makeOneProductionRequests"]["makeOneProductionRequests"]
    return response[0] if len(response) > 0 else {}


def graph_request(query, variables):
    graphql_client = ResGraphQLClient()
    return graphql_client.query(
        query,
        variables,
    )["data"]


# build the object to update line item record
def validate_line_item(line_item_info, inventory_info):
    valid_statuses = ["PROCESSING", "FULFILLED", "CANCELED"]
    fulfillment_status = line_item_info.get("fulfillmentStatus", "")
    response = {}

    if fulfillment_status not in valid_statuses:
        response = {
            "oneFinishedAt": arrow.utcnow().isoformat(),
            "activeOneNumber": str(inventory_info.get("resMagicOrderNumber")),
            "stockStateDayOfOrder": "In Stock",
            "linkToInventory": None,
            "inventoryLocation": f"https://airtable.com/tblhCz4t3QVjxWOpv/{inventory_info.get('id')}",
            "inventoryId": inventory_info.get("id"),
            "fulfillmentStatus": "HOLD_FOR_GROUP_FULFILLMENT",
            "fulfilledWithOneCode": str(inventory_info.get("resMagicOrderNumber")),
            "warehouseLocation": inventory_info.get("warehouseCheckinLocation"),
            "allocationLink": (
                f"https://airtable.com/tblptyuWfGEUJWwKk/{inventory_info.get('id')}"
                if inventory_info.get("checkinType") == "Receive res.Magic Request"
                else ""
            ),
            "inventoryCheckinType": inventory_info.get("checkinType"),
        }

        if inventory_info.get("warehouseCheckinLocation") == "NYC":
            response["inventoryLocation"] = "🛒"
            response["fulfillmentStatus"] = "PROCESSING"

    return response


# if the make one production request is not reserved or not open
# then search for a open production request with the same sku that is in assembly
def search_for_late_sku_in_assembly(sku):
    graphql_client = ResGraphQLClient()
    make_requests = graphql_client.query(
        GET_MAKE_DATA,
        {
            "where": {
                "sku": {"is": sku},
                "isOpen": {"is": True},
                "currentMakeNode": {"is": "Assembly"},
            }
        },
    )["data"]["makeOneProductionRequests"]["makeOneProductionRequests"]
    response = None
    # assign the first request since we are sorting by the oldest first
    if len(make_requests) > 0:
        response = make_requests[0].get("orderLineItem", None)

    return response


# build the payload to update the inventory record with the needed information
def build_update_inventory(inventory_info, line_item_info, will_be_one_reserved):
    MAKE_ONE_URL = "https://airtable.com/tblptyuWfGEUJWwKk/"
    LINE_ITEM_TABLE_URL = "https://airtable.com/tblUcI0VyLs7070yI"

    line_item_info = {} if not line_item_info else line_item_info
    make_prod_request_line_item = line_item_info.get("makeOneProductionRequest", {})

    response = {
        "requestType": line_item_info.get("requestType", "") if line_item_info else "",
        "brandCode": line_item_info.get("brand", {}).get("code", "")
        if line_item_info
        else "",
        "channelUnit": line_item_info.get("salesChannel", [""])[0]
        if line_item_info
        else None,
        "requestUrl": f"{MAKE_ONE_URL}{make_prod_request_line_item.get('id', '') if make_prod_request_line_item else ''}"
        if line_item_info
        else "",
        "placeAction": "Bin",
        "reservationStatus": "Belongs to a Group Fulfillment Order"
        if will_be_one_reserved
        else None,
        # "warehouseLocation": None,
        "processedTime": arrow.utcnow().isoformat(),
        "orderLineItemId": line_item_info.get("id", "") if line_item_info else "",
        "styleId": line_item_info.get("style", {}).get("id", "")
        if line_item_info
        else "",
    }

    # if the record is not in NYC search for available bin location
    if inventory_info["warehouseCheckinLocation"] != "NYC":
        # response["warehouseLocation"] = find_warehouse_location(inventory_info)
        if response["reservationStatus"] == "Belongs to a Group Fulfillment Order":
            response["groupFulfillmentId"] = line_item_info.get("fulfillment", {}).get(
                "id", ""
            )
    # since we are goint to send this ONE, add line item info to inventory record
    else:
        response["placeAction"] = "Transport"
        response["reservationStatus"] = (
            "TO - PICK:STAGE" if will_be_one_reserved else "OUT OF CAGE"
        )
        if will_be_one_reserved:
            response[
                "deduction"
            ] = f"{LINE_ITEM_TABLE_URL}{line_item_info.get('id', '')}"
            response[
                "notes"
            ] = f"Order Number - {line_item_info.get('order', {}).get('key', '')}"
            response["orderNumber"] = line_item_info.get("order", {}).get("key", "")

    return response


# get all warehose locations and see where we can store this ONE...
def find_warehouse_location(inventory_info):
    inventory_requests_where = {
        "reservationStatus": {"is": None},
        "styleCode": {"is": str(inventory_info.get("styleCode"))},
        "warehouseBinLocations": {
            "is": str(inventory_info.get("warehouseCheckinLocation"))
        },
        "warehouseLocation": {"isNot": None},
    }
    response = graph_request(GET_INVENTORY_DATA, {"where": inventory_requests_where})
    inventory_items = response["units"]["units"]
    locations_list = {}
    for item in inventory_items:
        for location in item.get("warehouseLocation", {}).get("warehouseLocation", []):
            location_id = location["id"]
            if location_id in locations_list:
                locations_list[location_id]["count"] += 1
            else:
                locations_list[location_id] = {
                    "count": 1,
                    "capacity": item.get("binStatus")[0],
                }

    warehouse_location = None

    for location in locations_list:
        if location["capacity"] == "0":
            warehouse_location = location
            break

    return warehouse_location


# make the Make ONE Prod request as unreserved since we get the line item for the ONE that is in Inventory
# update line item record to match the ONE that is in Inventory
def update_reservation(line_item, production_request_to_update, one_number):
    payload_to_update_line_item = {
        "makeOneStatus": "Warehouse",
        "oneStatus": "Warehouse",
        "makeOneProductionRequestId": None,
        "activeOneNumber": str(one_number),
    }

    payload_to_update_make_record = {"allocationStatus": "Unreserved"}

    logger.info(
        {
            "payload_to_update_line_item": payload_to_update_line_item,
            "payload_to_update_make_record": payload_to_update_make_record,
        }
    )

    logger.info(
        graph_request(
            UPDATE_LINE_ITEM,
            {"id": line_item["id"], "input": payload_to_update_line_item},
        )
    )

    logger.info(
        graph_request(
            UPDATE_MAKE_ONE_PRODUCTION,
            {
                "id": production_request_to_update["id"],
                "input": payload_to_update_make_record,
            },
        )
    )


def update_inventory(inventory_id, inventory_payload):
    logger.info(
        {
            "id": inventory_id,
            "payload to update inventory": inventory_payload,
        }
    )

    logger.info(
        graph_request(
            UPDATE_INVENTORY,
            {
                "id": inventory_id,
                "input": inventory_payload,
            },
        )
    )


def update_line_item_record(line_item_id, line_item_payload):
    logger.info({"id": line_item_id, "payload to update line item": line_item_payload})
    logger.info(
        graph_request(
            UPDATE_LINE_ITEM,
            {"id": line_item_id, "input": line_item_payload},
        )
    )


def update_payment_api(order_number, fulfillment_status):
    conn = http.client.HTTPSConnection("data.resmagic.io")

    headersList = {
        "Accept": "*/*",
        "Authorization": f"Bearer {secrets_client.get_secret('RES_PAYMENTS_API_KEY')}",
        "Content-Type": "application/json",
    }

    payload = json.dumps(
        {
            "one_number": order_number,
            "paymentsapi_current_status": fulfillment_status,
            "ready_to_ship_datetime": arrow.utcnow().isoformat(),
        }
    )

    conn.request("POST", "/payments-api/UpdateOrder", payload, headersList)
    response = conn.getresponse()
    result = response.read()

    logger.info(result.decode("utf-8"))


if __name__ == "__main__":
    records = [
        {"one": 10335184, "sku": "KT1012 CTSPS NAVYRN 6ZZ2X"},
        {"one": 10335849, "sku": "RB6018 PIMA7 NATUQX 2ZZSM"},
    ]

    for record in records:
        handler(
            {
                "insert_make_make_order_in_inventory": {
                    "returning": [
                        {
                            "id": "03a542de-52b8-5271-6455-c747359cda52",
                            "one_number": record["one"],
                            "checkin_type": "Receive res.Magic Request",
                            "sku_bar_code_scanned": record["sku"],
                            "warehouse_checkin_location": "STI",
                        }
                    ]
                }
            }
        )
