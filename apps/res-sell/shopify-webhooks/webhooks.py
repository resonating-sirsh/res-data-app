import base64
import hashlib
import hmac
import json
import traceback
import typing

from flask import Response, request
import res
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import logger
from res.utils.flask.ResFlask import ResFlask

app = ResFlask(__name__, enable_trace_logging=True, enable_cognito=False)

kafka_client = ResKafkaClient()
kafka_topics = {
    "legacy_order": "shopifyOrder",
    "order": "res_sell.shopify_webhooks.orders_raw",
    "product": "res_sell.shopify_webhooks.products",
    "bulk_operations": "res_sell.shopify_webhooks.bulk_operations",
}

# Sirsh adding this for parsing from the start
# this should be restarted if the schema changes but we dont want to load it each time because
# we want a fast response on the webhook - this could (probably will) lead to confusion if the schema changes
# without restarting the listener
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


shopify_fields = [
    "id",
    "name",
    "order_number",
    "email",
    "tags",
    "source_name",
    "discount_codes",
    "fulfillment_status",
    "closed_at",
    "financial_status",
    "total_discounts",
    "total_tax",
    "created_at",
    "updated_at",
]

customer_fields = [
    "id",
    "first_name",
    "orders_count",
    "total_spent",
    "tags",
    "created_at",
]

line_item_fields = [
    "id",
    "name",
    "variant_title",
    "sku",
    "quantity",
    "price",
    "fulfillment_status",
    "fulfillable_quantity",
    "customizations",
    "variant_id",
]
# fulfillment_quantity
shipping_lines_fields = ["title"]


def verify_webhook(data, secret, hmac_header):
    digest = hmac.new(secret.encode("utf-8"), data, hashlib.sha256).digest()
    genHmac = base64.b64encode(digest)
    return hmac.compare_digest(genHmac, hmac_header.encode("utf-8"))


def is_resonance_item(sku):
    # Resonance skus need to have 4 parts, the first part being 6 characters
    # e.g. TK6074 SLCTN FLOWJY 1ZZXS
    parts = sku.split(" ") if sku else []
    if len(parts) != 4:
        return False
    if len(parts[0]) != 6:
        return False
    return True


def prepare_order_output(data):
    output = {}
    # Set top-level fields (and concatenate discount codes)
    for field in shopify_fields:
        if field == "discount_codes":
            output[field] = ",".join(discount["code"] for discount in data[field])
        else:
            output[field] = data[field]

    # Set shipping field based on first shipping entry
    if len(data["shipping_lines"]) > 0:
        output["shipping_title"] = data["shipping_lines"][0]["title"]
    else:
        output["shipping_title"] = ""

    # Set customer fields
    for field in customer_fields:
        if "customer" in data and data["customer"]:
            output["customer_" + field] = data["customer"].get(field, None)

    # Build array of line items
    line_items = []
    for line_item in data["line_items"]:
        temp_line_item = {}
        for field in line_item_fields:
            if field == "fulfillable_quantity" and field in line_item.keys():
                temp_line_item["fulfillment_quantity"] = line_item[field]
            elif field in line_item.keys():
                temp_line_item[field] = line_item[field]
        temp_line_item["orderNumber"] = data["name"]
        line_items.append(temp_line_item)

    output["line_items"] = line_items
    logger.debug(line_items)

    return output


def prepare_product_output(data, event_type):
    output = {"id": data["id"]}
    if event_type != "delete":
        output["title"] = data["title"]
        output["description"] = data["body_html"]
        output["price"] = data["variants"][0]["price"]
        output["sku"] = data["variants"][0]["sku"]
        output["product_type"] = data["product_type"]
        output["updated_at"] = data["updated_at"]
        output["published_at"] = data["published_at"]
        output["status"] = data["status"]
        output["tags"] = data["tags"]
        output["raw_data"] = json.dumps(data)
    return output


def prepare_bulk_operations_output(
    data: typing.Dict[str, typing.Any]
) -> typing.Dict[str, typing.Any]:
    """
    Map the bulk operation data to kafka input data
    {
      "admin_graphql_api_id": "gid://shopify/BulkOperation/147595010",
      "completed_at": "2023-06-06T10:11:26-04:00",
      "created_at": "2023-06-06T10:11:26-04:00",
      "error_code": null,
      "status": "completed",
      "type": "query"
    }
    """
    payload = {"id": data["admin_graphql_api_id"]}
    payload["status"] = data["status"]
    payload["type"] = data["type"]
    payload["raw_data"] = json.dumps(data)
    return payload


def send_to_res_data_kafka(data, brand_code):
    """
    receive a data with the update type set
    """
    try:
        from schemas.pydantic.sell import Order

        topic = RES_DATA_SHOPIFY_TOPIC
        res.utils.logger.metric_node_state_transition_incr(
            node="shopify-webhooks", asset_key=brand_code, status="Enter"
        )
        # note added the update 'type' field to conform to schema, otherwise process as is
        data = Order.process_shopify_order_payload(
            data, schema=SHOPIFY_TOPIC_SCHEMA, brand_code=brand_code
        )
        res.utils.logger.debug(f"sending to {topic} for brand {brand_code}:> {data}")
        # res.connectors.load("kafka")[topic].produce(data)
        with ResKafkaProducer(kafka_client, topic) as kafka_producer:
            ret_val = kafka_producer.produce(data)
            if ret_val != True:
                raise Exception("unable to produce")

        res.utils.logger.metric_node_state_transition_incr(
            node="shopify-webhooks", asset_key=brand_code, status="Exit"
        )
        res.utils.logger.debug("message sent")
    except Exception as e:
        # warn for now, error later - we have a repair process anyway..
        logger.warn(
            f"Error sending Shopify (res) data {data} to Kafka: {traceback.format_exc()}"
        )
        res.utils.logger.metric_node_state_transition_incr(
            node="shopify-webhooks", asset_key=brand_code, status="Failed"
        )


def send_to_kafka(data, type, message_type="avro"):
    # Send to Kafka Topic based on the type of data
    with ResKafkaProducer(
        kafka_client, kafka_topics[type], message_type=message_type
    ) as kafka_producer:
        try:
            kafka_producer.produce(data)
        except Exception as e:
            logger.error(f"Error sending Shopify data to Kafka: {str(e)}")


def _handle_endpoint(data, event_type, brand_code, webhook_endpoint):
    if "product" in webhook_endpoint:
        output = prepare_product_output(data, event_type)
        output["brand"] = brand_code
        output["type"] = event_type
        logger.info("Event type: ")
        logger.info(event_type)
        send_to_kafka(output, "product")

    elif "order" in webhook_endpoint:
        # res data wants all updates
        res_line_items = []
        for item in data["line_items"]:
            if is_resonance_item(item["sku"]):
                res_line_items.append(item)
        # first send to new source controlled shopify topic - deprecate the "order: one further down
        if len(res_line_items) > 0:
            # copy the data and set extras, then process within the function
            output = dict(data)
            output["type"] = webhook_endpoint.replace("-order", "")
            send_to_res_data_kafka(output, brand_code=brand_code)
        else:
            res.utils.logger.debug(f"Skipping order without any resonance skus")
        # No need to process fulfilled orders
        if data["fulfillment_status"] == "fulfilled":
            return Response("Success!", status=200)

        output = prepare_order_output(data)
        output["brand"] = brand_code
        output["type"] = webhook_endpoint.replace("-order", "")
        # Send subset of fields to old kafka topic
        send_to_kafka(output, "legacy_order")

        # Prepare the full raw JSON to send to Kafka
        data["brand_code"] = brand_code
        # Only include resonance SKUs
        # If 1 or more resonance item, send to Kafka
        if len(res_line_items) > 0:
            data["line_items"] = res_line_items
            send_to_kafka(data, "order", message_type="json")
    elif "bulk-operations" in webhook_endpoint:
        output = prepare_bulk_operations_output(data)
        output["brand"] = brand_code
        output["type"] = "bulk_operations/type"
        logger.info(f"Event type: {event_type}")
        logger.info(f"Output: {output}")
        send_to_kafka(output, "bulk_operations")

    else:
        # Other types, such as customer-redact and shop-redact
        logger.warn(
            "Other request received: {}, brand code: {}".format(
                request.data, brand_code
            )
        )


@app.route("/shopify-webhooks/<webhook_endpoint>", methods=["POST"])
def handle_endpoint(webhook_endpoint):
    started = res.utils.dates.utc_now()
    data = json.loads(request.data)
    event_type = webhook_endpoint.replace("-product", "")
    brand_code = request.args.get("brand_code")

    # dont log the entire message, its not useful. just reference the id and time stamp (works for entire webhook endpoint)
    logger.info(
        f"{webhook_endpoint} event received for brand_code {brand_code} with id {data.get('id')} updated at {data.get('updated_at')}"
    )

    # refactored this out so we can unit test it
    _handle_endpoint(
        data,
        event_type=event_type,
        brand_code=brand_code,
        webhook_endpoint=webhook_endpoint,
    )

    # record latency
    ended = res.utils.dates.utc_now()

    res.utils.logger.metric_set_endpoint_latency_ms(
        (ended - started).total_seconds(), "shopify-webhooks", "post"
    )

    return Response("Success!", status=200)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
