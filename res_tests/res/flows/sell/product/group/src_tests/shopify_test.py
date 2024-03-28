import json
import pytest
from res.flows.sell.product.group.src.shopify import is_product_public
from res.utils import logger


@pytest.mark.parametrize(
    "data, expect_response",
    [
        (
            {"status": "active", "handle": "product-handle"},
            {"status": "active", "handle": "product-handle"},
        ),
        (
            {"status": "ACTIVE", "handle": "product-handle"},
            {"status": "ACTIVE", "handle": "product-handle"},
        ),
        ({"status": "false", "handle": "product-handle"}, False),
        ({"status": "FALSE", "handle": "product-handle"}, False),
    ],
)
def test_is_product_public(data, expect_response):
    class Shopify:
        def get_product(self, _):
            return data

    shopify_client = Shopify()
    ecommerce_id = "ecommerce_id"
    response = is_product_public(ecommerce_id, shopify_client)
    assert response == expect_response


def test_is_product_public_fail_shopify(monkeypatch):
    class Shopify:
        def get_product(self, _):
            raise Exception()

    shopify_client = Shopify()

    def spylogger(mess):
        global message
        message = mess

    monkeypatch.setattr(logger, "warn", spylogger)
    ecommerce_id = "ecommerce_id"
    response = is_product_public(ecommerce_id, shopify_client)
    assert response == False
    assert message == "is_product_public fail"
