import os
from res.connectors.graphql.FakeResGraphQLClient import FakeResGraphQLClient
from src.ShopifyGraphQLTransformer import ShopifyGraphQLTransformer
from src.main import run_consumer  # noqa
from res.connectors.kafka.FakeResKafkaConsumer import FakeResKafkaConsumer
import pytest

PROCESS_NAME = "test-shopify-graphql"
ENV = os.getenv("RES_ENV")


class TestShopifyGraphQLMain:
    @pytest.fixture
    def kafka_consumer(self):
        kafka_consumer = FakeResKafkaConsumer(None, None, None)
        return kafka_consumer

    @pytest.fixture
    def graphql_client(self):
        graphql_client = FakeResGraphQLClient(PROCESS_NAME, None, None, "development")
        return graphql_client

    def test_main(self, kafka_consumer, graphql_client):
        shopify_transformer = ShopifyGraphQLTransformer(graphql_client)
        raw_order = kafka_consumer.poll_avro()
        shopify_transformer.load_order(raw_order)
        error, sent = shopify_transformer.send_order(ENV)
        # assert error == None
        # assert sent == False
