import os, json, logging, random, time, datetime
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import logger
from res.connectors.airtable.AirtableClient import ResAirtableClient

INPUT_KAFKA_TOPIC = "res_sell.stripe_webhooks.payment_intent_succeeded_raw"
OUTPUT_KAFKA_TOPIC = "res_sell.merge_brand_payments.payments"

BRAND_BASE_ID = "appc7VJXjoGsSOjmw"
BRAND_TABLE_ID = "tblMnwRUuEvot969I"
STRIPE_CUSTOMER_TO_BRAND = {}


def load_brand_stripe_customers():
    airtable_client = ResAirtableClient()
    brands = airtable_client.get_records(BRAND_BASE_ID, BRAND_TABLE_ID)
    for brand in brands:
        customer_id = brand["fields"].get("stripe_customer_id", None)
        if (
            customer_id is not None
            and customer_id != ""
            and customer_id not in STRIPE_CUSTOMER_TO_BRAND
        ):
            STRIPE_CUSTOMER_TO_BRAND[customer_id] = brand["fields"]["Code"]
    logger.debug(f"Stripe customer IDs: {str(STRIPE_CUSTOMER_TO_BRAND)}")


def run_kafka():
    # Initialize client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, INPUT_KAFKA_TOPIC)
    with ResKafkaProducer(kafka_client, OUTPUT_KAFKA_TOPIC) as kafka_producer:
        while True:
            msg = kafka_consumer.poll_json()
            if msg is None:
                # Sometimes the message is None the first try, will show up eventually
                logger.debug("...")
                continue
            logger.debug("New record from kafka: {}".format(msg))
            # Load JSON and parse out required fields
            payment_id = msg["data"]["object"]["id"]
            charge = msg["data"]["object"]["charges"]["data"][0]
            customer_id = charge["customer"]
            if customer_id not in STRIPE_CUSTOMER_TO_BRAND:
                load_brand_stripe_customers()
                if customer_id not in STRIPE_CUSTOMER_TO_BRAND:
                    # Wait 10 seconds and try again -- if this is a new customer, the Graph API takes a while to save the ID
                    time.sleep(10)
                    load_brand_stripe_customers()
                    if customer_id not in STRIPE_CUSTOMER_TO_BRAND:
                        logger.error(f"Customer not found in Airtable! {customer_id}")
                        continue
            clean_msg = {
                "source": "stripe",
                "source_payment_id": payment_id,
                "source_brand_id": customer_id,
                "payment_type": "subscription"
                if charge["description"] == "Subscription creation"
                else "order_payment",
                "payment_amount": charge["amount"] * 1.0 / 100,
                "payment_datetime": charge["created"],
                "brand_code": STRIPE_CUSTOMER_TO_BRAND[customer_id],
            }
            logger.info(f"Creating payment {payment_id}")
            kafka_producer.produce(clean_msg)


if __name__ == "__main__":
    load_brand_stripe_customers()
    run_kafka()
