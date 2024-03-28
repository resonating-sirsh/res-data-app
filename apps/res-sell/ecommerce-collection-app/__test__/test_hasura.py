import pytest
from src.hasura import (
    get_ecommerce_collection_by_id,
    update_ecommerce_collection_by_id,
)
from src.hasura_graphql_definition import (
    CREATE_ECOMMERCE_COLLECTION,
    DELETE_ECOMMERCE_COLLECTION,
)
from res.connectors.graphql.hasura import Client


class TestEcommerceCollectionHasura:
    @pytest.fixture
    def hasura_client(self):
        return Client()

    @pytest.fixture
    def test_record(self, hasura_client: Client):
        mock_record = {
            "title": "Created by integration_test",
            "body_html": "<div>This is a test</div>",
            "sort_order_fk": "manual",
            "store_code": "TEST",
            "handle": "should-test",
        }
        response = hasura_client.execute(
            CREATE_ECOMMERCE_COLLECTION, {"input": mock_record}
        )
        return response["insert_sell_ecommerce_collections_one"]

    def _delete_record(self, id, hasura_client):
        hasura_client.execute(DELETE_ECOMMERCE_COLLECTION, {"id": id})

    @pytest.mark.slow
    def test_get_ecommerce_collection(self, test_record, hasura_client: Client):
        test_id = test_record["id"]
        response = get_ecommerce_collection_by_id(test_id, hasura_client)
        self._delete_record(test_record["id"], hasura_client)
        assert response["id"] == test_id
        assert response["title"] is not None
        assert response["type_collection"] == test_record["type_collection"]

    @pytest.mark.slow
    def test_update_commerce_collection(self, test_record, hasura_client: Client):
        title_test = "This is a change"
        response = update_ecommerce_collection_by_id(
            test_record["id"],
            {
                "title": title_test,
            },
            hasura_client,
        )
        self._delete_record(test_record["id"], hasura_client)
        assert test_record["title"] is not response["title"]
        assert response["title"] == title_test
