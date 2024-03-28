import json, pytest, os
import src.main as main
import src.query_definition as query_definition
import res

from res.flows.sell.product.group.src.querys import (
    SHOP_BY_COLOR_BY_ID,
    UPDATE_SHOP_BY_COLOR,
)
from res.connectors.kafka.FakeResKafkaConsumer import FakeKafkaConsumer
from res.connectors.graphql.FakeResGraphQLClient import MockGraphQl

PROCESS_NAME = "test-swap-material-request"
ENV = os.getenv("RES_ENV")


class FakeRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value):
        self.data[key] = json.loads(value)

    def get(self, key):
        return json.dumps(self.data[key])

    def delete(self, key):
        del self.data[key]


class TestSwaptMaterialRequest:
    @pytest.fixture
    def kafka_consumer(self):
        return FakeKafkaConsumer()

    @pytest.fixture
    def graphql_client(self):
        return res.connectors.load("graphql")

    @pytest.fixture
    def fake_redis_client(self):
        return FakeRedis()

    def test_mark_style_as_swap_return_update_false(self):
        STYLE_ID = "style_id_1"
        STYLE_ID_2 = "style_id_2"
        SBC_ID_1 = "shop_by_color_1"
        SBC_ID_2 = "shop_by_color_2"
        SBC_ID_3 = "shop_by_color_3"
        SBC_ID_4 = "shop_by_color_4"
        SBC_SWAP_REQUESTS = {
            SBC_ID_1: {
                "from_sbc": SBC_ID_1,
                "style_ids": [STYLE_ID, STYLE_ID_2],
                "swapped_styles": [],
                "to_sbcs": [SBC_ID_3],
            },
            SBC_ID_2: {
                "style_ids": [STYLE_ID],
                "from_Sbc": SBC_ID_2,
                "style_ids": [STYLE_ID, STYLE_ID_2],
                "swapped_styles": [],
                "to_sbcs": [SBC_ID_4],
            },
        }

        update, response = main.mark_style_as_swapped(STYLE_ID, SBC_SWAP_REQUESTS)

        assert update is False
        assert SBC_ID_2 not in response["sbc_ids"]

    def test_mark_style_as_swap_return_update_true(self):
        STYLE_ID = "style_id_1"
        STYLE_ID_2 = "style_id_2"
        SBC_ID_1 = "shop_by_color_1"
        SBC_ID_2 = "shop_by_color_2"
        SBC_ID_3 = "shop_by_color_3"
        SBC_ID_4 = "shop_by_color_4"
        SBC_SWAP_REQUESTS = {
            SBC_ID_1: {
                "from_sbc": SBC_ID_1,
                "style_ids": [STYLE_ID],
                "swapped_styles": [],
                "to_sbcs": [SBC_ID_3],
            },
            SBC_ID_2: {
                "style_ids": [STYLE_ID],
                "from_Sbc": SBC_ID_2,
                "style_ids": [STYLE_ID],
                "swapped_styles": [STYLE_ID_2],
                "to_sbcs": [SBC_ID_4],
            },
        }

        update, response = main.mark_style_as_swapped(STYLE_ID, SBC_SWAP_REQUESTS)

        assert update is True
        assert SBC_ID_2 in response["sbc_ids"]

    def test_update_sbc(self):
        STYLE_ID = "style_id_1"
        STYLE_ID_4 = "style_id_4"
        SBC_ID_1 = "shop_by_color_1"
        SBC_ID_2 = "shop_by_color_2"
        SBC_ID_3 = "shop_by_color_3"
        SBC_ID_4 = "shop_by_color_4"
        SBC_SWAP_REQUESTS = {
            SBC_ID_1: {
                "from_sbc": SBC_ID_1,
                "style_ids": [STYLE_ID],
                "swapped_styles": [],
                "to_sbcs": [SBC_ID_3],
            },
            SBC_ID_2: {
                "style_ids": [],
                "from_Sbc": SBC_ID_2,
                "swapped_styles": [STYLE_ID_4],
                "to_sbcs": [SBC_ID_4],
            },
        }
        SBC_IDS = [SBC_ID_2]

        global variables_update
        variables_update = {}

        def response(query, variables):
            if query is SHOP_BY_COLOR_BY_ID:
                return {
                    "data": {
                        "shopByColorInstance": {
                            "id": variables["id"],
                            "options": [
                                {
                                    "styleId": style_id,
                                    "colorName": f"this a color name {style_id}",
                                }
                                for style_id in ["style_id_2", "style_id_3"]
                            ],
                        }
                    }
                }

            elif query is query_definition.GET_STYLE_ID:
                return {
                    "data": {
                        "style": {
                            "id": variables["id"],
                            "color": {
                                "name": f"this color {variables['id']}",
                                "images": [{"fullThumbnail": "url_1"}],
                            },
                        }
                    }
                }

            elif query is UPDATE_SHOP_BY_COLOR:
                variables_update[variables["id"]] = variables["input"]
                return {
                    "data": {
                        "updateShopByColorInstance": {
                            "shopByColorInstance": {
                                "id": variables["id"],
                                **variables["input"],
                            }
                        }
                    }
                }
            elif query is query_definition.RELEASE_SBC:
                return {
                    "data": {
                        "releaseShopByColorInstanceToEcommerce": {
                            "shopByColorInstance": {"id": variables["id"]}
                        }
                    }
                }

        fake_graphql_client = MockGraphQl(response)

        main.update_sbc(SBC_IDS, SBC_SWAP_REQUESTS, fake_graphql_client)

        data = variables_update[SBC_ID_4]
        options = data["options"]

        assert STYLE_ID_4 in [option["styleId"] for option in options]

    def test_run_consumer(self, kafka_consumer, fake_redis_client):
        STYLE_ID = "style_id_1"
        SWAP_ID = "swap_request_id_1"
        STYLE_ID_4 = "style_id_4"
        SBC_ID_1 = "shop_by_color_1"
        SBC_ID_2 = "shop_by_color_2"
        SBC_ID_3 = "shop_by_color_3"
        SBC_ID_4 = "shop_by_color_4"
        SBC_SWAP_REQUESTS = {
            SBC_ID_1: {
                "from_sbc": SBC_ID_1,
                "style_ids": [STYLE_ID, "style_id_2"],
                "swapped_styles": [],
                "to_sbcs": [SBC_ID_3],
            },
            SBC_ID_2: {
                "style_ids": [],
                "from_Sbc": SBC_ID_2,
                "swapped_styles": [STYLE_ID_4],
                "to_sbcs": [SBC_ID_4],
            },
        }
        fake_redis_client.set(SWAP_ID, json.dumps(SBC_SWAP_REQUESTS))

        global variables_update
        variables_update = {}

        def response(query, variables):
            if query is SHOP_BY_COLOR_BY_ID:
                return {
                    "data": {
                        "shopByColorInstance": {
                            "id": variables["id"],
                            "options": [
                                {
                                    "styleId": style_id,
                                    "colorName": f"this a color name {style_id}",
                                }
                                for style_id in ["style_id_2", "style_id_3"]
                            ],
                        }
                    }
                }

            elif query is query_definition.GET_STYLE_ID:
                return {
                    "data": {
                        "style": {
                            "id": variables["id"],
                            "color": {
                                "name": f"this color {variables['id']}",
                                "images": [{"fullThumbnail": "url_1"}],
                            },
                        }
                    }
                }

            elif query is UPDATE_SHOP_BY_COLOR:
                variables_update[variables["id"]] = variables["input"]
                return {
                    "data": {
                        "updateShopByColorInstance": {
                            "shopByColorInstance": {
                                "id": variables["id"],
                                **variables["input"],
                            }
                        }
                    }
                }
            elif query is query_definition.RELEASE_SBC:
                return {
                    "data": {
                        "releaseShopByColorInstanceToEcommerce": {
                            "shopByColorInstance": {"id": variables["id"]}
                        }
                    }
                }

        fake_graphql_client = MockGraphQl(response)
        kafka_consumer.set_message(
            {"style_id": STYLE_ID, "swap_material_request": SWAP_ID}
        )

        main.run_consumer(kafka_consumer, fake_graphql_client, fake_redis_client)

        assert len(list(fake_redis_client.data.keys())) is not 0

    def test_run_consumer_2(self, kafka_consumer, fake_redis_client):
        STYLE_ID = "style_id_1"
        SWAP_ID = "swap_request_id_1"
        STYLE_ID_4 = "style_id_4"
        SBC_ID_2 = "shop_by_color_2"
        SBC_ID_4 = "shop_by_color_4"
        SBC_SWAP_REQUESTS = {
            SBC_ID_2: {
                "style_ids": [],
                "from_Sbc": SBC_ID_2,
                "swapped_styles": [STYLE_ID_4],
                "to_sbcs": [SBC_ID_4],
            },
        }
        fake_redis_client.set(SWAP_ID, json.dumps(SBC_SWAP_REQUESTS))

        global variables_update
        variables_update = {}

        def response(query, variables):
            if query is SHOP_BY_COLOR_BY_ID:
                return {
                    "data": {
                        "shopByColorInstance": {
                            "id": variables["id"],
                            "options": [
                                {
                                    "styleId": style_id,
                                    "colorName": f"this a color name {style_id}",
                                }
                                for style_id in ["style_id_2", "style_id_3"]
                            ],
                        }
                    }
                }

            elif query is query_definition.GET_STYLE_ID:
                return {
                    "data": {
                        "style": {
                            "id": variables["id"],
                            "color": {
                                "name": f"this color {variables['id']}",
                                "images": [{"fullThumbnail": "url_1"}],
                            },
                        }
                    }
                }

            elif query is UPDATE_SHOP_BY_COLOR:
                variables_update[variables["id"]] = variables["input"]
                return {
                    "data": {
                        "updateShopByColorInstance": {
                            "shopByColorInstance": {
                                "id": variables["id"],
                                **variables["input"],
                            }
                        }
                    }
                }
            elif query is query_definition.RELEASE_SBC:
                return {
                    "data": {
                        "releaseShopByColorInstanceToEcommerce": {
                            "shopByColorInstance": {"id": variables["id"]}
                        }
                    }
                }

        fake_graphql_client = MockGraphQl(response)
        kafka_consumer.set_message(
            {"style_id": STYLE_ID, "swap_material_request": SWAP_ID}
        )

        main.run_consumer(kafka_consumer, fake_graphql_client, fake_redis_client)

        assert len(list(fake_redis_client.data.keys())) is 0

    @pytest.mark.slow
    def test_run_consumer_integration(
        self, kafka_consumer, fake_redis_client, graphql_client
    ):
        SWAP_ID = "reclde9RzQ04jzUzU"
        STYLE_ID = "rec2afpIox16Kx7hw"
        SBC_ID = "62166318fd302400091f0638"
        SBC_UPDATE_REQUESTS = {
            SBC_ID: {
                "from": "CTNL2",
                "to": "CTNL2",
                "from_sbc": SBC_ID,
                "style_ids": ["recX8Xj1A0gTLj8Eo", "rec2afpIox16Kx7hw"],
                "swapped_styles": [],
                "to_sbcs": [SBC_ID],
            }
        }
        fake_redis_client.set(SWAP_ID, json.dumps(SBC_UPDATE_REQUESTS))

        msg = {
            "swap_style_id": "reclde9RzQ04jzUzU",
            "swap_material_request": SWAP_ID,
            "style_id": STYLE_ID,
        }
        kafka_consumer.set_message(msg)

        main.run_consumer(kafka_consumer, graphql_client, fake_redis_client)

        json_data = fake_redis_client.get(SWAP_ID)
        dict_data = json.loads(json_data)

        assert STYLE_ID in dict_data[SBC_ID]["swapped_styles"]
        assert STYLE_ID not in dict_data[SBC_ID]["style_ids"]
