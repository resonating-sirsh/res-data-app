from datetime import datetime

import pytest
from src.main import add_color_option_if_possible, remove_color_option_if_possible
from src.query_definition import (
    GET_SBC_BASE_ON_BODY_MATERIAL_AND_STORE,
    UPDATE_SBC_BY_ID,
)

from res.connectors.graphql.FakeResGraphQLClient import MockGraphQl


class FakeImage:
    def __init__(self, src):
        self.src = src


class FakeProduct:
    def __init__(self, handle, price, images):
        self.handle = handle
        self.price = price
        self.images = [FakeImage(src=image) for image in images]
        self.published_at = datetime.now()

    def price_range(self):
        return self.price


@pytest.fixture
def fake_product():
    return FakeProduct(handle="my_handle", price=100, images=["1", "2", "3"])


def test_remove_color_option_if_possible():
    shop_by_color_fake_id = "SHOP_BY_COLOR_ID"
    style_id = "STYLE_ID"
    mock_shop_by_color = {
        "id": shop_by_color_fake_id,
        "options": [
            {
                "styleId": style_id,
            },
            {
                "styleId": "STYLE_ID_2",
            },
            {
                "styleId": "STYLE_ID_3",
            },
        ],
    }

    mock_product = {
        "id": "PRODUCT_ID",
        "style": {
            "id": style_id,
        },
        "shopByColor": mock_shop_by_color,
    }

    def response(query, variables):
        global remove_variables
        if UPDATE_SBC_BY_ID in query:
            remove_variables = variables
            return {"data": {"removeColorOption": {"shopByColor": mock_shop_by_color}}}

    fake_graphl = MockGraphQl(response)

    remove_color_option_if_possible(mock_product, mock_shop_by_color, fake_graphl)

    assert remove_variables["id"] == shop_by_color_fake_id


# def test_add_color_option_if_possible(fake_product):
#     shop_by_color_id = "SHOP_BY_COLOR_ID"
#     style_id = "STYLE_ID"
#     color_name = "COLOR_NAME"
#     body_code = "BODY_CODE"
#     material_code = "MATERIAL_CODE"
#     color_images = [
#         {
#             "smallThumbnail": "SMALL_THUMBNAIL",
#         }
#     ]
#     store_code = "STORE_CODE"
#     # ISO 8601 date example
#     created_at = "2020-01-01T00:00:00.000Z"

#     mock_product = {
#         "id": "PRODUCT_ID",
#         "storeCode": store_code,
#         "style": {
#             "id": style_id,
#             "color": {
#                 "name": color_name,
#                 "images": color_images,
#             },
#             "material": {
#                 "code": material_code,
#             },
#             "body": {
#                 "code": body_code,
#             },
#             "createdAt": created_at,
#         },
#     }

#     mock_shopify_product = fake_product

#     elements = [
#         {"id": shop_by_color_id, "options": []},
#         {"id": shop_by_color_id, "options": []},
#         {"id": shop_by_color_id, "options": []},
#     ]

#     def response(query, variables):
#         global add_variables
#         global get_sbc
#         if GET_SBC_BASE_ON_BODY_MATERIAL_AND_STORE in query:
#             get_sbc = variables
#             return {
#                 "data": {
#                     "shopByColorInstances": {
#                         "count": 1,
#                         "shopByColorInstances": elements,
#                     }
#                 }
#             }
#         elif UPDATE_SBC_BY_ID in query:
#             add_variables = variables
#             return {
#                 "data": {"addColorOption": {"shopByColor": {"id": shop_by_color_id}}}
#             }

#     fake_graphql = MockGraphQl(response)

#     add_color_option_if_possible(
#         mock_product, elements[0], mock_shopify_product, fake_graphql
#     )

#     assert add_variables["id"] == shop_by_color_id
