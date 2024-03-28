import os
from res.connectors.graphql.FakeResGraphQLClient import FakeResGraphQLClient
from res.connectors.kafka.FakeResKafkaConsumer import FakeResKafkaConsumer
import pytest

PROCESS_NAME = "test-shopify-graphql"
ENV = os.getenv("RES_ENV")


class TestShopifyGraphQLMain:
    @pytest.fixture
    def kafka_consumer(self):
        return FakeResKafkaConsumer(None, None, None)

    @pytest.fixture
    def graphql_client(self):
        return FakeResGraphQLClient(PROCESS_NAME, None, None, "development")
