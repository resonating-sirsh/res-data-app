import pytest


@pytest.mark.service
def test_create_one_order():
    """
    this is an integration test that requires bootstrapping the env with data (meta ones)
    for now this is added as a script to remind how to test that the orders are created and relayed to make
    """
    true, false = True, False
    null = None
    e = {
        "id": "recwcgvvorZaG7mKs",
        "isArchived": false,
        "key": "BG-2127918",
        "code": "2127918",
        "number": "2127918",
        "friendlyName": "bgla",
        "requestName": "bgla",
        "email": "bruceglentwins@gmail.com",
        "brandId": ["recDnyvtQu9HlMzAW"],
        "brandCode": ["BG"],
        "brandName": ["BruceGlen"],
        "orderChannel": "resmagic.io",
        "salesChannel": "PHOTO",
        "fulfillmentId": ["recm04LO5sNa2zZNP"],
        "lineItemIds": ["recq43GstA4Bngdcf"],
        "containsExternalItem": false,
        "shippingAddress1": "548 South Spring Street",
        "shippingAddress2": "PH 13",
        "shippingCity": "Los Angeles",
        "shippingZip": "90013",
        "shippingCountry": "USA",
        "shippingProvince": "California",
        "shippingName": "BG LA",
        "shippingPhone": "+1 (646) 715-4703",
        "orderStatus": "✅",
        "isClosedv2": "1",
        "requestType": "Finished Goods",
        "createdAt": "2023-03-05T06:39:39.000Z",
        "keyid": "2127918",
        "styleCodes": ["CC-3093 CTNBA ZEBRVW"],
        "isClosed": true,
        "lineItemsInfo": [
            {
                "sku": "CC3093 CTNBA ZEBRVW 2ZZSM",
                "quantity": 1,
                "id": "recq43GstA4Bngdcf",
            }
        ],
        "numberLineItems": 1,
        "lineItemsCreated": 1,
        "firstTouchSent": false,
        "secondTouchSent": false,
        "thirthTouchSent": false,
        "hasItemsFlaggedForReview": false,
        "deadlineStatus": null,
        "releaseStatus": "HOLD",
        "isFlaggedForReview": false,
        "orderStatusV3": "Fulfilled",
        "daysOpenInMake": 0,
        "activeIssuesIds": [],
        "daysSinceOrder": 5,
        "holdEmail": false,
        "isOrderClosed": false,
    }
    from res.flows.sell.orders import process
    from res.flows.sell.orders import queries

    # plan true good for testing parser
    r = queries.update_create_one_order(dict(e), plan=False)

    # TODO assert some things
    r = process.create_one_handler(e, skip_make_update=False)

    # todo assert some things

    assert 1 == 1, "TO DO keeping the placeholder snippet for now"
