import shopify
import os
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.logging.ResLogger import ResLogger
from datetime import datetime
from datetime import timedelta

logger = ResLogger()


def get_brands():
    graph = ResGraphQLClient()

    BRAND_CREDENTIALS_QUERY = """
    query brandShopifyCredentials($after:String) {
        brands(first:100, after:$after) {
            brands{
            name
            code
            shopifyStoreName
                shopifyApiKey
                shopifyApiPassword
            }
        count
        cursor
        }
    }
    """

    creds_array = []
    # Ideally the following filtering should occur entirely within the WHEREVALUE of the graph querry
    result = graph.query(BRAND_CREDENTIALS_QUERY, {}, paginate=True)
    for each in result["data"]["brands"]["brands"]:
        if each["shopifyApiKey"] == "" or each["shopifyApiKey"] == None:
            continue
        else:
            creds_array.append(each)
    return creds_array


def get_brand(code):
    graph = ResGraphQLClient()

    BRAND_CREDENTIALS_QUERY = f"""
    query brandShopifyCredentials($after:String) {{
        brands(first:100, after:$after, where:{{code: {{is: "{code}"}}}}) {{
            brands{{
            name
            code
            shopifyStoreName
                shopifyApiKey
                shopifyApiPassword
            }}
        count
        cursor
        }}
    }}
    """

    creds_array = []
    # Ideally the following filtering should occur entirely within the WHEREVALUE of the graph querry
    result = graph.query(BRAND_CREDENTIALS_QUERY, {}, paginate=True)
    for each in result["data"]["brands"]["brands"]:
        if each["shopifyApiKey"] == "" or each["shopifyApiKey"] == None:
            continue
        else:
            creds_array.append(each)
    return creds_array


def get_orders(kafka_producer, brand):
    session = shopify.Session(
        "https://" + brand["shopifyStoreName"] + ".myshopify.com/",
        "2022-10",
        brand["shopifyApiPassword"],
    )

    shopify.ShopifyResource.activate_session(session)
    shop = shopify.Shop.current()
    logger.info(shop)

    order_array = []
    page = 0

    if os.getenv("backfill", "false").lower() == "true":
        orders = shopify.Order.find(status="any")
    else:
        orders = shopify.Order.find(
            status="any",
            created_at_min=datetime.strftime(
                datetime.today() - timedelta(days=int(os.getenv("days", 1))), "%Y-%m-%d"
            ),
        )

    while orders.has_next_page():
        for every in orders:
            msg = every.to_dict()
            msg["brand_code"] = brand["code"]
            kafka_producer.produce(
                data=msg,
                dedupe=True,
                key_value=msg["id"],
                key_ts=datetime.strftime(datetime.today(), "%Y-%m-%d %H:%M:%S.%f")[:-3],
            )
        page = page + 1
        logger.info("Page: " + str(page))
        orders = orders.next_page()


if __name__ == "__main__":
    KAFKA_TOPIC = "res_sell.shopify_webhooks.orders_raw"
    kafka_client = ResKafkaClient()
    with ResKafkaProducer(
        kafka_client, KAFKA_TOPIC, message_type="json"
    ) as kafka_producer:
        if os.getenv("brand_code", "KT").lower() == "false":
            creds_array = get_brands()

        else:
            creds_array = get_brand(os.getenv("brand_code", "KT"))

        for brand in creds_array:
            try:
                get_orders(kafka_producer, brand)
            except Exception as e:
                logger.info(e)
                continue
