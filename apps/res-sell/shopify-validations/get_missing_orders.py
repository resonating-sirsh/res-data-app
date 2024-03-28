import json
import arrow
import requests
from res.utils.logging.ResLogger import ResLogger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import res
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer

PROCESS_NAME = "shopify-fulffillment"

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

CREATE_ORDER_MUTATION = """
    mutation createEcommerceOrder($input: CreateEcommerceOrderInput!) {
    createEcommerceOrder(input: $input) {
      order{
        id
                    brand{
                        name
                        code
                    }
                    email
                    fulfillmentStatus
                    financialStatus
                    discountCode
                    sourceName
                    isFlaggedForReview
                    flagForReviewNotes
                    dateCreatedAt
                    closedAt
                    name
        }
             
        }
}
"""

CREATE_ISSUE_MUTATION = """
    mutation createIssues($input: CreateIssueInput!) {
    createIssues(input: $input) {
      issues{
            id
      }        
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

SHOPIFY_TOPIC_SCHEMA = None
RES_DATA_SHOPIFY_TOPIC = "res_sell.shopify.orders"
try:
    # try adding the schema for the new kafka topic
    with open(
        res.utils.get_res_root()
        / "res-schemas"
        / "avro"
        / "res-sell"
        / "shopify"
        / "orders.avsc"
    ) as f:
        SHOPIFY_TOPIC_SCHEMA = json.load(f)
except:
    pass

# set up a kafka client
kafka_client = ResKafkaClient()


def handler(logger, graphql_client, mode="production"):
    if mode == "testing":
        print("Testing...")

    brands = get_brands(mode, graphql_client)

    print(f"Brands: {[brand.get('name') for brand in brands]}")

    for brand in brands:
        get_unfulfilled_orders_for_brand(brand, mode, logger, graphql_client)


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


def send_to_res_data_kafka(data, brand_code, topic=RES_DATA_SHOPIFY_TOPIC):
    try:
        from schemas.pydantic.sell import Order

        data = Order.process_shopify_order_payload(
            dict(data), schema=SHOPIFY_TOPIC_SCHEMA, brand_code=brand_code
        )

        with ResKafkaProducer(kafka_client, topic) as kafka_producer:
            kafka_producer.produce(data)
    except Exception as e:
        # warn for now, error later
        logger.warn(f"Error sending res data shopify data to Kafka: {str(e)}")


def get_unfulfilled_orders_for_brand(brand, mode, logger, graphql_client):
    res.utils.logger.info(f"\nProcessing all orders from: {brand.get('name')}...")
    try:
        brand_code = brand.get("code")
        response = graphql_client.query(
            BRAND_CREDENTIALS_QUERY, {"code": brand.get("code")}
        )

        brand_credentials = response["data"]["brand"]
        brand_shopify_store_name = brand_credentials["shopifyStoreName"]
        brand_shopify_api_key = brand_credentials["shopifyApiKey"]
        brand_shopify_api_password = brand_credentials["shopifyApiPassword"]

        now = arrow.utcnow()
        start_at = now.shift(days=-2)
        end_at = now.shift(hours=-1).isoformat()

        res.utils.logger.info("Getting unfulfilled orders from shopify...")

        orders = []
        has_more_orders = True
        next_orders_url = f"https://{brand_shopify_store_name}.myshopify.com/admin/api/2023-01/orders.json?fulfillment_status=unfulfilled&status=open&created_at_min={start_at}&created_at_max={end_at}&limit=250"

        while has_more_orders:
            response = requests.get(
                next_orders_url,
                auth=(brand_shopify_api_key, brand_shopify_api_password),
            )
            if "orders" not in response.json():
                break

            next_orders = response.json()["orders"]
            next_link = response.links.get("next")
            next_orders_url = next_link.get("url") if next_link else None

            # maybe we want to do something where we only update if our database has something older. if we do this once a day its worth doing a timestamp lookup to save re-posting
            for order in next_orders:
                send_to_res_data_kafka(order, brand_code=brand_code)

            orders.extend(next_orders)

            if not next_orders_url:
                has_more_orders = False

        print(f"Count of open orders: {len(orders)} created less than 3 days ago")

        for order in orders:
            sales_channel = "ECOM"
            tags = order["tags"].lower().split(", ")
            contains_external_items = False
            external_items = count_external_items(order)

            if external_items == len(order["line_items"]):
                # Order is legacy, ignore it
                print(
                    "Order #"
                    + str(order.get("order_number"))
                    + " skipped: Does not contains ONEs"
                )
                continue
            elif external_items > 0:
                print(
                    "Order #"
                    + str(order.get("order_number"))
                    + ": Contains External and ONEs products"
                )
                contains_external_items = True

            if "invoice" in tags:
                # Order is wholesale, ignore it
                continue

            if "wholesale" in tags:
                sales_channel = "Wholesale"

            if mode == "production":
                record_order(
                    order,
                    sales_channel,
                    brand,
                    contains_external_items,
                    logger,
                    graphql_client,
                )
            elif mode == "testing":
                print("testing")
                continue
            else:
                raise ValueError(f"Unknown mode {mode}")

    except Exception as e:
        msg = "Error while validating Shopify Orders: {}".format(e)
        logger.critical(msg)
        raise e

    stopped_at = arrow.utcnow()
    time_to_complete = stopped_at - now
    print(f"\nTime taken to complete: {time_to_complete.total_seconds()}")

    return "done"


def count_external_items(order):
    line_items = order["line_items"]
    legacy_items = 0
    for lineItem in line_items:
        if is_external_item(lineItem, graphql_client):
            legacy_items = legacy_items + 1

    return legacy_items


def get_body_code(sku):
    sku_parts = sku.split()
    body_code = sku

    if len(sku_parts) >= 2:
        body_code = sku_parts[0][:2] + "-" + sku_parts[0][2:]

    return body_code


def is_external_item(order_line, graphql_client):
    if order_line.get("sku"):
        body_code = get_body_code(order_line["sku"])
        response = graphql_client.query(FIND_BODY_QUERY, {"number": body_code})

        body_exist = response["data"]["body"]

        return not body_exist
    else:
        return True


def record_order(
    order, sales_channel, brand, has_external_items, logger, graphql_client
):
    # normalize fieldnames for res.Magic.fulfillment
    order_fields = {}
    for field_name in shopify_order_map:
        try:
            # I want to concatenate the codes together before writing to res.Magic.fulfillment
            if field_name == "discount_codes":
                concatenated_codes = ""
                for code_object in order[field_name]:
                    concatenated_codes += code_object["code"] + " "
                order_fields[shopify_order_map[field_name]] = concatenated_codes
            elif field_name == "id":
                order_fields[shopify_order_map[field_name]] = str(order[field_name])
            else:
                order_fields[shopify_order_map[field_name]] = order[field_name]
        except Exception as e:
            order_fields[shopify_order_map[field_name]] = "unknown"

    for field_name in shopify_shipping_map:
        try:
            order_fields[shopify_shipping_map[field_name]] = order["shipping_lines"][0][
                field_name
            ]
        except:
            order_fields[shopify_shipping_map[field_name]] = "unknown"

    for field_name in shopify_customer_map:
        try:
            order_fields[shopify_customer_map[field_name]] = str(
                order["customer"][field_name]
            )
        except Exception as e:
            order_fields[shopify_customer_map[field_name]] = "unknown"

    # ensure you write the last checked date in the database
    modified_date = arrow.utcnow().isoformat()
    order_fields["lastUpdatedAt"] = modified_date
    order_fields["brandId"] = brand.get("fulfillmentId")
    order_fields["name"] = f"#{str(order['order_number'])}"

    get_orders_query = (
        """
         query Orders {
            orders(first: 500, where: {
                orderChannel: {is: "Shopify"}, 
                name: {is: \""""
        + order_fields["name"]
        + """\"}, 
                brandCode: {is:\""""
        + brand.get("code")
        + """\"} }) {
                orders{
                    id
                    
                }
            }
        }   
    """
    )

    response = graphql_client.query(get_orders_query, None)

    orders = response["data"]["orders"]["orders"]

    order_fields["approvedForFulfillment"] = "Approved"
    order_fields["orderChannel"] = "Shopify"
    order_fields["salesChannel"] = sales_channel
    order_fields["containsExternalItem"] = has_external_items
    order_fields["releaseStatus"] = "HOLD"

    if len(orders) == 0:
        print("create new record")
        msg = "Order missing in platform: {}".format(order["id"])
        logger.warn(msg)
        orderLineitems = []

        for line_item_index in range(0, len(order["line_items"])):
            line_fields = {}
            if not is_external_item(
                order["line_items"][line_item_index], graphql_client
            ):
                for field in shopify_order_line_item_map:
                    if field == "id":
                        line_fields[shopify_order_line_item_map[field]] = str(
                            order["line_items"][line_item_index][field]
                        )
                    else:
                        line_fields[shopify_order_line_item_map[field]] = order[
                            "line_items"
                        ][line_item_index][field]

                if line_fields["shopifyFulfillmentStatus"] is None:
                    line_fields["shopifyFulfillmentStatus"] = "unfulfilled"

                line_item_quantity = order["line_items"][line_item_index]["quantity"]
                order["line_items"][line_item_index]["quantity"] = 1
                line_fields["orderNumber"] = order_fields["name"]

                print("create new line item")
                for _ in range(0, line_item_quantity):
                    orderLineitems.append(line_fields)

        order_fields["lineItemsInfo"] = orderLineitems
        order_fields["wasCreatedWithValidator"] = True
        query_name = "createEcommerceOrder"
        action = "created"

        response = graphql_client.query(CREATE_ORDER_MUTATION, {"input": order_fields})

        if "errors" in response:
            print(response["errors"][0]["message"])
            return response["errors"][0]["message"]

        order_record = response["data"][query_name]["order"]
        print(f'Order {order_fields["name"]} was {action}')
        return order_fields


if __name__ == "__main__":
    logger = ResLogger()
    graphql_client = ResGraphQLClient(PROCESS_NAME)
    handler(logger, graphql_client, mode="production")
