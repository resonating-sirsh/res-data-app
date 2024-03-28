import json
import typing

import shopify

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.shopify.base.ShopifyBase import ShopifyClientBase
from res.connectors.shopify.utils import PublicationChannel
from res.connectors.shopify.utils.exceptions import ShopifyException
from res.connectors.shopify.utils.graphql_query import (
    GET_PRODUCT,
    GET_PRODUCT_ONLINE_STORE,
    LIST_PUBLICATIONS,
)
from res.utils import logger

SBC_TOPIC = "res_sell.shopify_product_live.live_products"


def send_mesage_to_kafka(product_id, live_status):
    status = "live" if live_status else "not live"
    logger.info(f"{product_id} is {status}")
    kafka_client = ResKafkaClient()
    with ResKafkaProducer(kafka_client, SBC_TOPIC) as producer:
        producer.produce({"id": product_id, "live": live_status})


class ShopifyProduct(ShopifyClientBase):
    """Connector which connect to the Product API"""

    def is_product_public(
        self, ecommerce_id: int
    ) -> typing.Tuple[bool, typing.Optional[Exception]]:
        """Check is a product is live on the online store of brand"""
        publications_response_json = self.shopify_graphql.execute(
            LIST_PUBLICATIONS,
        )
        publications_response: typing.Dict[str, typing.Any] = json.loads(
            publications_response_json
        )
        publications = (
            publications_response.get("data", {}).get("publications", {}).get("edges")
        )
        if not publications:
            return False, ShopifyException(
                "Error trying to read publications channels from Shopify"
            )
        filter_publications = filter(
            lambda x: x["node"]["name"] == PublicationChannel.online_store.value,
            publications,
        )
        online_channel: typing.Optional[typing.Dict] = next(filter_publications)
        if not online_channel:
            return False, ShopifyException(
                f"Online Store channel not find in store: {self.brand_code}"
            )
        online_channel_id = online_channel["node"]["id"]
        product_json = self.shopify_graphql.execute(
            GET_PRODUCT_ONLINE_STORE,
            {
                "id": self.ecommerce_id_to_graphql_id(ecommerce_id),
                "publication_id": online_channel_id,
            },
        )
        response = json.loads(product_json)
        if "data" not in response or not response["data"]["product"]:
            return False, ShopifyException(
                f"Error requesting the product on Online Store to Shopify"
            )

        return response["data"]["product"]["publishedOnPublication"], None

    def ecommerce_id_to_graphql_id(self, ecommerce_id: int) -> str:
        return f"gid://shopify/Product/{ecommerce_id}"

    def check_product_is_live(self, id: str) -> bool:
        product = self.make_create_one_api_request(
            GET_PRODUCT, {"id": id}, get_data=lambda x: x["data"]["product"]
        )
        response, exception = self.is_product_public(int(product["ecommerceId"]))
        if exception:
            logger.error("Check live status fail", exception=Exception)
            return False

        logger.debug(f"product {product['isLive']} is live: {response}")
        if product["isLive"] != response:
            send_mesage_to_kafka(id, response)
        return bool(response)

    def find_by_id(self, ecommerce_id: str) -> typing.Optional[typing.Dict]:
        response = shopify.Product.find(id_=int(ecommerce_id))
        if isinstance(response, shopify.ShopifyResource):
            return response.to_dict()
        return
