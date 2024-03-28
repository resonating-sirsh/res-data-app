import abc
import json
import os
import typing

import shopify

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.shopify.ShopifyConnector import CURRENT_VERSION
from res.connectors.shopify.utils.exceptions import ShopifyException
from res.utils import logger

GET_BRAND = """
    query getBrandStoreCredentials($brandCode: String) {
            brand(code: $brandCode) {
                id
                shopifyApiKey
                shopifyApiPassword
                shopifyStoreName
            }
    }
"""


class ShopifyClientBase(abc.ABC):
    """Abstract Class to connect to the Shopify API.
    This is a base class you can used to implement general logic like to add
    get_store_credentials
    """

    shopify_graphql: typing.Optional[shopify.GraphQL]
    graphQlClient: ResGraphQLClient
    shopify_session: typing.Optional[shopify.Session]
    brand_code: str

    def __init__(
        self,
        endpoint=None,
        method=None,
        brand_code=None,
        payload=None,
        version=CURRENT_VERSION,
    ):
        self.graphQlClient = ResGraphQLClient(
            api_url=os.environ.get("GRAPHQL_API_URL", "http://localhost:3000")
        )
        self._endpoint = endpoint
        self._method = method
        self._brand_code = brand_code
        self.payload = payload
        self._version = version
        self._store_credentials = {}
        self.shopify_session = None

        self.shopify_graphql = None
        if brand_code:
            self.active_shopify_session()
            self.shopify_graphql = shopify.GraphQL()

    def __enter__(self):
        self.active_shopify_session()
        return shopify, self

    def __exit__(self, *args, **kwargs):
        self.terminate_shopify_session()

    def get_store_credentials(self):
        response = self.graphQlClient.query(GET_BRAND, {"brandCode": self._brand_code})
        self._store_credentials = response["data"]["brand"]

    def active_shopify_session(self):
        """Get credentials and active the shopfiy session to send request to shopify"""
        if self.shopify_session is None:
            self.get_store_credentials()
            shop_url = "https://{}.myshopify.com".format(
                self._store_credentials.get("shopifyStoreName")
            )
            self.shopify_session = shopify.Session(
                shop_url=shop_url,
                version=shopify.Release(self._version).name,
                token=self._store_credentials.get("shopifyApiPassword"),
            )
        shopify.ShopifyResource.activate_session(self.shopify_session)

    def terminate_shopify_session(self):
        shopify.ShopifyResource.clear_session()

    def make_create_one_api_request(
        self,
        query: str,
        variable: dict,
        get_data: typing.Optional[typing.Callable],
    ) -> typing.Optional[typing.Dict[str, typing.Any]]:
        data = self.graphQlClient.query(query, variable)
        if get_data and data:
            return get_data(data)

    def secure_graphql_execute(
        self, query: str, variables: typing.Dict[str, typing.Any]
    ) -> typing.Tuple[
        typing.Optional[typing.Dict[str, typing.Any]], typing.Optional[ShopifyException]
    ]:
        """
        Make a query or mutation to shopify only when the session is stablished
        and return the data only when not error are present.
        """
        if not self.shopify_graphql:
            return None, ShopifyException("Shopify Session hasn't started")

        response_json = self.shopify_graphql.execute(query, variables)
        response_dict = json.loads(response_json)
        logger.debug(response_json)

        if "data" not in response_dict:
            return None, ShopifyException(
                "Error calling the `publishablePublish` mutation",
                errors=response_dict["errors"],
            )
        return response_dict, None
