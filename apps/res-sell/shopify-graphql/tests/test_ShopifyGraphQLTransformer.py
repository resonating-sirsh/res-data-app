from src.ShopifyGraphQLTransformer import ShopifyGraphQLTransformer
from res.connectors.graphql.FakeResGraphQLClient import FakeResGraphQLClient
import json
import pytest


class TestShopifyGraphQLTransformer:
    @pytest.fixture
    def graphql_client(self):
        # Add in fake url and api key for graphql
        graphql_client = FakeResGraphQLClient(
            "test_shopify_graphql", "abcd", "test.com", "development"
        )
        return graphql_client

    @pytest.fixture
    def shopify_transformer(self, graphql_client):
        transformer = ShopifyGraphQLTransformer(graphql_client)
        return transformer

    @pytest.fixture
    def base_shopify_data(self):
        with open("/app/tests/sample_shopify_order_INPUT.json") as shopify_sample_file:
            sample_data = json.load(shopify_sample_file)
        return sample_data

    def test_extract_body(self, shopify_transformer):
        # non-Resonance codes
        sku1 = "201585-GREY"
        sku2 = "HU20EBLB01-DARK LUGGAGE"
        # Resonance code
        sku3 = "CC2028 CTF19 BLUECR 3ZZMD"
        # Empty string
        sku4 = ""

        body1 = shopify_transformer._get_body_code(sku1)
        body2 = shopify_transformer._get_body_code(sku2)
        body3 = shopify_transformer._get_body_code(sku3)
        body4 = shopify_transformer._get_body_code(sku4)

        assert body1 == None
        assert body2 == None
        assert body3 == "CC-2028"
        assert body4 == None

    def test_get_external_line_items(self, shopify_transformer):
        line_items = [
            {"id": 123, "sku": None},
            {"id": 234, "sku": "ABCD123"},
            {"id": 345, "sku": "HU20EBLB01-DARK LUGGAGE"},
            {"id": 456, "sku": "CC2028 CTF19 BLUECR 3ZZMD"},
        ]
        expected_marked_line_items = [123, 234, 345]
        marked_line_items = shopify_transformer._get_external_items(line_items)
        assert marked_line_items == expected_marked_line_items
