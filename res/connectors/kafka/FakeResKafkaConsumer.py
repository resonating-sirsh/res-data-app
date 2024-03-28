import io, json, requests, uuid, logging, datetime

logging.basicConfig()

# This is a Resonance Kafka Consumer, which wraps the confluent consumer


class FakeResKafkaConsumer:
    def __init__(self, kafka_client, topic, environment=None):
        pass

    def poll_avro(self, timeout=100.0):
        return {
            "brand": "RB",
            "type": "update",
            "id": 3719147159646,
            "name": "#1154172",
            "email": "jane.tester@gmail.com",
            "tags": "",
            "source_name": "web",
            "discount_codes": "2mh6d3-mhtj7g",
            "fulfillment_status": None,
            "closed_at": None,
            "financial_status": "paid",
            "total_discounts": "29.85",
            "created_at": "2021-04-14T13:01:53-04:00",
            "updated_at": "2021-04-14T13:01:58-04:00",
            "shipping_title": "Free Fedex Ground Shipping",
            "customer_id": 123456789012,
            "customer_first_name": "Jane",
            "customer_orders_count": 4,
            "customer_total_spent": "950.10",
            "customer_tags": "",
            "customer_created_at": "2018-07-23T16:46:29-04:00",
            "line_items": [
                {
                    "product_id": None,
                    "id": 9747558727774,
                    "name": "Julian Backpack",
                    "variant_title": "",
                    "sku": "HU20EBLB01-DARK LUGGAGE",
                    "quantity": 1,
                    "price": "199.00",
                    "fulfillment_status": None,
                    "fulfillable_quantity": 1,
                }
            ],
        }


"""
This is a simple general fake test double of a kafka consumer.

To use it, you only need create an instance and set the message that your consumer will be return
to your test function

Example:

# ./main.py
# your function definition
# NOTE:               vvv       dependency injection.
def my_function(kafka_consumer)
    msg = kafka_consumer.poll_avro()
    ... implementation ...
    return something


# -------------------------------------


# ./test_main.py

from res.connectors.kafka.FakeResKafkaConsumer import FakeKafkaConsumer
from somefile import my_function

def test_my_function():
    kafka_consumer = FakeKafkaConsumer() 
    kafka_consumer.set_message({
        ... test message definition ...
    })

    response = my_function(kafka_consumer)

    ... assertions ...

"""


class FakeKafkaConsumer:
    def __init__(self):
        self.message = {}
        pass

    def set_message(self, message):
        self.message = message

    def poll_avro(self, timeout=100.0):
        return self.message
