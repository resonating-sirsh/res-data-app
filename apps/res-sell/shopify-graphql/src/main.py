import os, json, datetime, time, traceback
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.s3.S3Connector import S3Connector
from res.utils import logger, ping_slack
from src.ShopifyGraphQLTransformer import ShopifyGraphQLTransformer

### Pulls Shopify data, transforms some fields and sends to GraphQL which adds to Airtable
### Batching is used to group similar styles, so they enter Airtable around the same time
### Logic for batching:
### Kafka poll has a timeout of 100s, but is blocking -- the loop will wait 100s for a message,
### if none is found it will continue the loop. At the start of the loop we also check for >3k messages

ENV = os.getenv("RES_ENV")
TOPIC = "shopifyOrder"


# Main kafka consumer/ graphql query loop
def run_consumer(kafka_consumer, graphql_client):

    s3_connector = S3Connector()
    while True:
        try:
            # Get new messages
            raw_order = kafka_consumer.poll_avro()
            if raw_order is None:
                logger.info("kafka poll timed out, continuing...")
                continue
            logger.info("Found new order: {}".format(raw_order["id"]))
            logger.debug("Found new order: {}".format(json.dumps(raw_order, indent=4)))
            shopify_transformer = ShopifyGraphQLTransformer(graphql_client)
            shopify_transformer.load_order(raw_order)
            error, sent = shopify_transformer.send_order(ENV)
            if error:
                logger.critical(error)
            logger.info(
                "Send status for order {}: {}, errors: {}".format(
                    str(raw_order.get("id", "no id found")), str(sent), str(error)
                )
            )
        except Exception as e:
            logger.info(str(raw_order))
            err = "Error pulling or transforming order from Shopify: {}".format(
                traceback.format_exc()
            )
            logger.critical(
                err,
                exception=e,
                tags={
                    "brand_code": raw_order["brand"],
                    "order_number": raw_order.get("name", ""),
                },
            )
            # ping_slack(
            #     f"[SHOPIFY-GRAPHQL] <@U0361U4B84X> somethings fucked with shopify-graphql ```{traceback.format_exc()}```",
            #     "autobots",
            # )
            order_batch = []


if __name__ == "__main__":
    # Initialize clients
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, TOPIC)
    graphql_client = ResGraphQLClient()
    # # Start Kafka consumer
    run_consumer(kafka_consumer, graphql_client)
