import json
import pytest
import os
import src.main as main
import src.query_definition as query_definition
import res

from res.flows.sell.product.group.src.querys import SHOP_BY_COLOR_BY_ID
from res.connectors.kafka.FakeResKafkaConsumer import FakeKafkaConsumer
from res.connectors.graphql.FakeResGraphQLClient import MockGraphQl

PROCESS_NAME = "test-swap-material-request"
ENV = os.getenv("RES_ENV")


class FakeRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value):
        self.data[key] = json.loads(value)


class TestSwaptMaterialRequest:
    @pytest.fixture
    def kafka_consumer(self):
        return FakeKafkaConsumer()

    @pytest.fixture
    def graphql_client(self):
        return res.connectors.load("graphql")

    @pytest.fixture
    def redis_client(self):
        return FakeRedis()

    def test_get_request(self):
        REQUESTED_ID = "REQUESTED_ID"

        def response(query, variables):
            global request_query
            global request_variables
            request_query = query
            request_variables = variables
            if query == query_definition.GET_SWAP_MATERIAL:
                return {
                    "data": {
                        "materialSwapRequest": {
                            "id": "REQUESTED_ID",
                            "styles": [
                                {"id": "style1"},
                                {"id": "style2"},
                                {"id": "style3"},
                                {"id": "style4"},
                            ],
                        }
                    }
                }

        graphql = MockGraphQl(response)

        data = main.get_swap_material_requests(REQUESTED_ID, graphql)

        assert request_query is query_definition.GET_SWAP_MATERIAL
        assert "id" in request_variables
        assert request_variables["id"] is REQUESTED_ID
        assert data is not None

    def test_get_style_ids(self):
        REQUESTED_IDS = [f"REQUESTED_ID{no}" for no in range(0, 5)]

        def response(query, variables):
            global resquest_query
            global request_variables
            request_query = query
            request_variables = variables
            if query == query_definition.GET_STYLES:
                return {
                    "data": {
                        "styles": {
                            "styles": [{"id": id} for id in REQUESTED_IDS],
                            "hasMore": False,
                            "cursor": None,
                        }
                    }
                }

            graphql = MockGraphQl(response)

            data = main.get_styles(REQUESTED_IDS, graphql)

            assert request_query is query_definition.GET_STYLES
            assert "ids" in request_variables
            assert request_variables["ids"] is REQUESTED_IDS
            assert data is not None

    def test_group_styles(self):
        styles = [
            {
                "id": "style1",
                "body": {"code": "body1"},
                "material": {"code": "material1"},
            },
            {
                "id": "style2",
                "body": {"code": "body1"},
                "material": {"code": "material1"},
            },
            {
                "id": "style3",
                "body": {"code": "body1"},
                "material": {"code": "material1"},
            },
        ]

        data = main.group_styles_by_body_material(styles)

        KEY = "body1%material1"

        assert KEY in data
        assert len(data[KEY]) == 3

    def test_run_consumer(self, kafka_consumer, redis_client):
        # assemble
        SWAP_ID = "swap_material_request_id"
        STYLE_IDS = [f"style{no}" for no in range(0, 5)]
        SHOP_BY_COLOR_ID = "shop_by_color_1"
        FROM_MATERIAL = "material1"
        TO_MATERIAL = "material2"

        def response(query, variables):
            if query is query_definition.GET_SWAP_MATERIAL:
                return {
                    "data": {
                        "materialSwapRequest": {
                            "id": SWAP_ID,
                            "stylesIds": json.dumps(STYLE_IDS),
                            "fromMaterial": FROM_MATERIAL,
                            "toMaterial": TO_MATERIAL,
                        }
                    }
                }
            elif query is query_definition.GET_STYLES:
                data = {
                    "styles": [
                        {
                            "id": id,
                            "body": {"code": "body1"},
                            "material": {"code": "material1"},
                            "brand": {"code": "TT"},
                            "allProducts": [
                                {
                                    "id": f"product_id_{id}",
                                    "styleId": id,
                                    "shopByColor": {"id": SHOP_BY_COLOR_ID},
                                }
                            ],
                        }
                        for id in STYLE_IDS
                    ],
                    "cursor": None,
                    "hasMore": False,
                }
                return {
                    "data": {
                        "styles": data,
                    }
                }
            elif query is SHOP_BY_COLOR_BY_ID:
                return {
                    "data": {
                        "shopByColorInstance": {
                            "id": "sbc_id_1",
                            "bodies": [{"code": "body1"}],
                            "materials": [{"code": "material1"}],
                            "storeBrand": {"code": "TT"},
                            "options": [{"styleId": id} for id in STYLE_IDS],
                        }
                    }
                }
            elif query is query_definition.GET_LIST_SBC:
                return {
                    "data": {
                        "shopByColorInstances": {
                            "shopByColorInstances": [
                                {
                                    "id": f"shop_by_color_{id}",
                                    "bodies": [{"code": "body1"}],
                                    "materials": [{"code": "material1"}],
                                    "storeBrand": {"code": "TT"},
                                    "options": [{"styleId": id} for id in STYLE_IDS],
                                }
                                for id in range(0, 4)
                            ]
                        }
                    }
                }

        graphql_client = MockGraphQl(response=response)
        kafka_consumer.set_message({"id": SWAP_ID})

        # action
        main.run_consumer(kafka_consumer, graphql_client, redis_client)

        # assert
        assert SWAP_ID in redis_client.data

        data = redis_client.data[SWAP_ID][SHOP_BY_COLOR_ID]
        assert data["from_sbc"] == SHOP_BY_COLOR_ID
        assert data["from"] == FROM_MATERIAL
        assert data["to"] == TO_MATERIAL
        assert len(data["to_sbcs"]) == 4

    @pytest.mark.slow
    def test_run_consumer_integration(
        self, graphql_client, kafka_consumer, redis_client
    ):
        # @see_more at https://airtable.com/appKGWiGPT52oCPC4/tbla99gcjrhqUmRKy/viwjGOBiobkglUrAX/recRgXSXde1nPhsPT?blocks=bipRuUf8AG4e29A4V
        SWAP_ID = "recRgXSXde1nPhsPT"
        kafka_consumer.set_message({"id": SWAP_ID})

        main.run_consumer(kafka_consumer, graphql_client, redis_client)
        assert SWAP_ID in redis_client.data
        data = redis_client.data[SWAP_ID]
        assert data is not None
        assert len(list(data)) is 1
