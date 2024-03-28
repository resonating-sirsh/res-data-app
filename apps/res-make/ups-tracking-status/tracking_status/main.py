import json
import xmltodict
import urllib3
import os

from res.utils import secrets_client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger
from datetime import datetime, timedelta

import xml.etree.ElementTree as ET
import requests as req

CLIENT = ResGraphQLClient()
INFO_NOT_AVAILABLE = "Tracking info no longer available in UPS"
QUERY_UPDATE_LINE_ITEM = """
    mutation updateOrderLineItem($id: ID!, $input: UpdateOrderLineItemInput){
        updateOrderLineItem(id:$id, input:$input){
            orderLineItem {
                id
                trackingNumber
                upsTrackingStatus
                upsDeliveryDate
            }
        }
    }
"""

QUERY_GET_ITEMS = """
    query getLineItemWithOutUpsInfo($where: OrderLineItemsWhere!, $after: String){
    orderLineItems(
        first: 100
        after: $after
        where: $where 
    ) {
        orderLineItems {
        id
        activeOneNumber
        trackingNumber
        upsTrackingStatus
        upsDeliveryDate
        timestampFulfillment
        }
        count
        cursor
        hasMore
    }
    }
"""


def handler(event):
    if os.getenv("BACKFILL", "False").lower() == "false":
        # Query non archived Fulfilled lineitems with empty UPS Delivery Date
        get_line_items = CLIENT.query(
            QUERY_GET_ITEMS,
            {
                "where": {
                    "fulfillmentStatus": {"is": "FULFILLED"},
                    "shippingCompany": {"is": "UPS"},
                    "isArchived": {"is": False},
                    "upsTrackingStatus": {
                        "isNoneOf": [
                            "Delivered",
                            INFO_NOT_AVAILABLE,
                        ]
                    },
                },
            },
            paginate=True,
        )
    else:
        # Query Fulfilled lineitems (archived included) with empty UPS Delivery Date
        START_DATE = (
            (datetime.date(datetime.today()) - timedelta(days=365)).isoformat()
            if os.getenv("START_DATE", "ONE_YEAR_AGO") == "ONE_YEAR_AGO"
            else datetime.strptime(os.getenv("START_DATE"), "%Y-%m-%d").isoformat()
        )
        logger.info(f"Backfilling Data. Starting Date: {START_DATE}")

        get_line_items = CLIENT.query(
            QUERY_GET_ITEMS,
            {
                "where": {
                    "fulfillmentStatus": {"is": "FULFILLED"},
                    "shippingCompany": {"is": "UPS"},
                    "timestampFulfillment": {"isGreaterThan": START_DATE},
                    "upsTrackingStatus": {
                        "isNoneOf": ["Delivered", INFO_NOT_AVAILABLE]
                    },
                },
            },
            paginate=True,
        )

    count = 0
    if (
        get_line_items
        and len(get_line_items.get("data").get("orderLineItems").get("orderLineItems"))
        > 0
    ):
        total_line_items = get_line_items.get("data").get("orderLineItems").get("count")
        for line_item in (
            get_line_items.get("data").get("orderLineItems").get("orderLineItems")
        ):
            count += 1
            logger.info(f"{count} of {total_line_items}")
            tracking_number = line_item.get("trackingNumber")
            ups_info = getUpsInfo(tracking_number)
            current_status = line_item["upsTrackingStatus"]

            if ups_info:
                xpars = xmltodict.parse(ups_info.content)
                json_str = json.dumps(xpars)
                json_info = json.loads(json_str)
                if not json_info.get("TrackResponse").get("Response").get("Error"):
                    package_info = (
                        json_info.get("TrackResponse").get("Shipment").get("Package")
                    )
                    if package_info.get("Message"):
                        status = package_info.get("Message").get("Description")
                        delivery_datetime = None

                    else:
                        status = (
                            package_info.get("Activity")
                            .get("Status")
                            .get("StatusType")
                            .get("Description")
                        )
                        delivery_datetime = "{}T{}Z".format(
                            package_info.get("Activity").get("GMTDate"),
                            package_info.get("Activity").get("GMTTime"),
                        )

                    if current_status != status:
                        logger.info(f"Updating line item to: {status}")
                        CLIENT.query(
                            QUERY_UPDATE_LINE_ITEM,
                            {
                                "id": line_item.get("id"),
                                "input": {
                                    "upsTrackingStatus": status,
                                    "upsDeliveryDate": delivery_datetime,
                                },
                            },
                        )
                    else:
                        logger.info(f"Skipping, same status as before")
                else:
                    logger.info(
                        "Error retrieving tracking response from UPS, info not available."
                    )
                    CLIENT.query(
                        QUERY_UPDATE_LINE_ITEM,
                        {
                            "id": line_item.get("id"),
                            "input": {
                                "upsTrackingStatus": INFO_NOT_AVAILABLE,
                                "upsDeliveryDate": None,
                            },
                        },
                    )


def getUpsInfo(tracking_number):
    # Create access_request XML
    AccessLicenseNumber = bytes(_AccessLicenseNumber, "utf-8").decode("utf-8", "ignore")
    userId = bytes(_userId, "utf-8").decode("utf-8", "ignore")
    password = bytes(_password, "utf-8").decode("utf-8", "ignore")
    AccessRequest = ET.Element("AccessRequest")
    ET.SubElement(AccessRequest, "AccessLicenseNumber").text = _AccessLicenseNumber
    ET.SubElement(AccessRequest, "UserId").text = _userId
    ET.SubElement(AccessRequest, "Password").text = _password

    TrackToolsRequest = ET.Element("TrackRequest")
    Request = ET.SubElement(TrackToolsRequest, "Request")
    TransactionReference = ET.SubElement(Request, "TransactionReference")
    ET.SubElement(TransactionReference, "CustomerContext").text = "Customer context"
    ET.SubElement(TransactionReference, "XpciVersion").text = "1.0"
    ET.SubElement(Request, "RequestAction").text = "Track"
    ET.SubElement(Request, "RequestOption").text = "none"
    ET.SubElement(TrackToolsRequest, "TrackingNumber").text = tracking_number

    _reqString = ET.tostring(AccessRequest)

    tree = ET.ElementTree(ET.fromstring(_reqString))
    root = tree.getroot()

    _QuantunmRequest = ET.tostring(TrackToolsRequest)
    quantunmTree = ET.ElementTree(ET.fromstring(_QuantunmRequest))
    quantRoot = quantunmTree.getroot()

    _XmlRequest = ET.tostring(root, encoding="utf8", method="xml") + ET.tostring(
        quantRoot, encoding="utf8", method="xml"
    )
    _XmlRequest = _XmlRequest.decode().replace("\n", "")
    _host = "https://onlinetools.ups.com/ups.app/xml/Track"
    urllib3.disable_warnings()
    return req.post(_host, _XmlRequest, verify=False)


if __name__ == "__main__":
    try:
        _AccessLicenseNumber = secrets_client.get_secret("UPS_API_KEY")
        _userId = secrets_client.get_secret("UPS_USER_ID")
        _password = secrets_client.get_secret("UPS_PASSWORD")
        handler("")
    except Exception as e:
        logger.error(f"An unexpected error has ocurred.\n{e!r}")
