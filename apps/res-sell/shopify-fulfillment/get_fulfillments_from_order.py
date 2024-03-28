import os
import json
import arrow
import datetime
from datetime import timedelta
import requests
from res.utils.logging.ResLogger import ResLogger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import shopify
from res.utils.logging import logger


PROCESS_NAME = "shopify-validations"

BRAND_CREDENTIALS_QUERY = """
    query brandShopifyCredentials($code: String) {
        brand(code: $code) {
            name
            shopifyStoreName
            shopifyApiKey
            shopifyApiPassword
        }
    }
"""

FIND_BODY_QUERY = """
       query Body($number: String!) {
        body(number: $number) {
            name  
        }
    }
"""


shopify_order_map = {
    "id": "channelOrderId",
    "email": "email",
    "tags": "tags",
    "source_name": "sourceName",
    "discount_codes": "discountCode",
    "fulfillment_status": "fulfillmentStatus",
    "closed_at": "platformClosedAt",
    "financial_status": "financialStatus",
    "total_discounts": "discountAmount",
    "created_at": "dateCreatedAt",
}

shopify_order_line_item_map = {
    "id": "channelOrderLineItemId",
    "name": "productName",
    "variant_title": "productVariantTitle",
    "sku": "sku",
    "quantity": "quantity",
    "price": "lineItemPrice",
    "fulfillment_status": "shopifyFulfillmentStatus",
    "fulfillable_quantity": "shopifyFulfillableQuantity",
}

shopify_shipping_map = {"title": "shippingMethod"}

shopify_customer_map = {
    "id": "customerIdChannel",
    "first_name": "requesterFirstName",
    "orders_count": "customerOrderCount",
    "total_spent": "customerTotalSpent",
    "tags": "customerTags",
    "created_at": "customerFirstPurchaseDate",
}


def handler(logger, graphql_client, mode="production"):
    if mode == "testing":
        print("Testing...")

    brands = get_brands(mode, graphql_client)

    # THe Handler showld have the Brand API passwors ?
    for brand in brands:
        logger.info(f"Brands: {[brand.get('name')]}")

        session = shopify.Session(
            "https://" + brand["shopifyStoreName"] + ".myshopify.com/",
            "2022-10",
            brand["shopifyApiPassword"],
        )

        shopify.ShopifyResource.activate_session(session)
        shop = shopify.Shop.current()
        logger.info(shop)
        fulfillments = []
        if os.getenv("backfill", "false").lower() == "true":
            fulfillments = shopify.Fulfillment.find(status="any")
        else:
            fulfillments = shopify.Fulfillment.find(
                status="any",
                created_at_min=datetime.strftime(
                    datetime.today() - timedelta(days=int(os.getenv("days", 1))),
                    "%Y-%m-%d",
                ),
            )
            order_array = []
            page = 0
            while fulfillments.has_next_page():
                for every in fulfillments:
                    msg = every.to_dict()
                    msg["brand_code"] = brand["code"]

                    # Here isntead of Kafka just Update with the API flow
                page = page + 1
                logger.info("Page: " + str(page))
                orders = orders.next_page()
        logger.info(fulfillments)
        return fulfillments

        # get_unfulfilled_orders_for_brand(brand, mode, logger, graphql_client)


def get_brands(mode, graphql_client):
    print("\nGetting brands...")

    if mode == "production":
        filter_formula = "{isFetchingOrdersOn: {is: true}, isSellEnabled: {is: true}}"
    elif mode == "testing":
        filter_formula = (
            "{isTestingFetchingOrdersOn: {is: true}, isSellEnabled: {is: true}}"
        )
    else:
        raise ValueError(f"Unknown mode {mode}")

    get_brands_query = (
        """
        query Brand {
            brands(
                first: 100, 
                where: """
        + filter_formula
        + """, 
                sort: {field: NAME, direction: ASCENDING}) {
                    brands{
                        name
                        code
                        fulfillmentId
                    }
            }
        }
    """
    )

    response = graphql_client.query(get_brands_query, None)

    brands = response["data"]["brands"]

    return brands["brands"]


if __name__ == "__main__":
    logger = ResLogger()
    graphql_client = ResGraphQLClient(PROCESS_NAME)
    handler(logger, graphql_client, mode="production")
