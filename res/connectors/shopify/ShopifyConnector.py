""" 
Helper Class which connect with to Shopify using the ShopifyAPI package

Example:

with Shopify(brand_id="<STORE_CODE>") as shopify:
    product = shopify.Product.find_one(product_id)

reference: http://shopify.github.io/shopify_python_api/
"""

import json
import os
import time
from pathlib import Path
import numpy as np
import requests
import shopify
from pyactiveresource.connection import ClientError
import pandas as pd
import res
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.shopify.ShopifyQuery import ShopifyQuery
from res.connectors.shopify.utils.brands import get_brand
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

CURRENT_VERSION = "2023-04"


def handle_api_rate_limit(max_attempts=3):
    def wrapper(function):
        attempt = 0

        def wrapper_function(*args, **kwargs):
            nonlocal attempt
            attempt += 1
            logger.info("trying to request shopify...")
            try:
                return function(*args, **kwargs)
            except ClientError as error:
                if error.response.code == 429 and attempt <= max_attempts:
                    logger.warn(
                        "Too many requests to Shopify -- pausing for 60 seconds and retrying..."
                    )
                    time.sleep(60)
                    return wrapper_function(*args, **kwargs)
                if error.response.code == 429:
                    logger.warn("Too many requests: All attempts consume")
                raise error

        return wrapper_function

    return wrapper


class ShopifyConnector(object):
    def __getitem__(self, brand_code):
        return ShopifyShopConnector(brand_code)


class ShopifyShopConnector(object):
    def __init__(self, brand_code=None):
        self._brand_code = brand_code
        self._brand = get_brand(brand_code)[0] if brand_code else None
        self._session = shopify.Session(
            "https://" + self._brand["shopifyStoreName"] + ".myshopify.com/",
            "2022-10",
            self._brand["shopifyApiPassword"],
        )

        self._brand_shopify_store_name = self._brand["shopifyStoreName"]
        self._brand_shopify_api_key = self._brand["shopifyApiKey"]
        self._brand_shopify_api_password = self._brand["shopifyApiPassword"]

        shopify.ShopifyResource.activate_session(self._session)
        self._shop = shopify.Shop.current()
        res.utils.logger.debug(self._shop)
        self._gclient = shopify.graphql.GraphQL()

        self._query_document = Path(__file__).parent / "queries.gql"
        with open(self._query_document) as f:
            self._query_document = f.read()

    def fetch_full_order_detail(self, oid, fields=None):
        if fields:
            fields = ",".join(fields)
            fields = f"&fields={fields}"
        else:
            fields = ""

        response = requests.get(
            f"{self.store_endpoint}/orders.json?ids={oid}&status=any{fields}",
            auth=(self._brand_shopify_api_key, self._brand_shopify_api_password),
        )
        # check r.status_code and response length

        return response.json()["orders"][0]

    def fetch_full_order_detail_for_order_ids(self, order_ids, fields=None):
        """ """

        if isinstance(order_ids, list) or isinstance(order_ids, np.ndarray):
            order_ids = f",".join(list(order_ids))

        if fields:
            fields = ",".join(fields)
            fields = f"&fields={fields}"
        else:
            fields = ""

        response = requests.get(
            f"{self.store_endpoint}/orders.json?ids={order_ids}&status=any{fields}",
            auth=(self._brand_shopify_api_key, self._brand_shopify_api_password),
        )
        # check r.status_code and response length

        return response.json()["orders"]

    def fetch_full_order_detail(self, oid, fields=None):
        return self.fetch_full_order_detail_for_order_ids(order_ids=oid)[0]

    @property
    def store_endpoint(self):
        return (
            f"https://{self._brand_shopify_store_name}.myshopify.com/admin/api/2023-01"
        )

    def iter_orders(self, since=2, fulfillment_status="any"):
        """
        fulfillment_status:>
            shipped: Show orders that have been shipped. Returns orders with fulfillment_status of [fulfilled].
            partial: Show partially shipped orders.
            unshipped: Show orders that have not yet been shipped. Returns orders with fulfillment_status of [null].
            any: Show orders of any fulfillment status.
            unfulfilled: Returns orders with fulfillment_status of [null] or [partial].

        """
        start_at = res.utils.dates.utc_days_ago(since)
        end_at = res.utils.dates.utc_days_ago(0)
        has_more_orders = True

        next_orders_url = f"{self.store_endpoint}/orders.json?fulfillment_status={fulfillment_status.lower()}&status=any&created_at_min={start_at}&created_at_max={end_at}&limit=250"

        while has_more_orders:
            response = requests.get(
                next_orders_url,
                auth=(self._brand_shopify_api_key, self._brand_shopify_api_password),
            )
            if "orders" not in response.json():
                break

            next_orders = response.json()["orders"]
            next_link = response.links.get("next")
            next_orders_url = next_link.get("url") if next_link else None

            for order in next_orders:
                yield order

            if not next_orders_url:
                has_more_orders = False

    def query(self, **kwargs):
        return self._gclient.execute(**kwargs)

    def _parse_order_node(self, node):
        try:
            node["lineItems"] = [n["node"] for n in node["lineItems"]["edges"]]
            node["fulfillments"] = [
                n["node"]["lineItem"]
                for f in node["fulfillments"]
                for n in f["fulfillmentLineItems"]["edges"]
            ]
        except:
            pass
        return node

    def list_order_info_since(self, date):
        logger.info(f"querying brand {self._brand_code} orders after {date}")
        data = self.query(
            query=self._query_document,
            variables={"query": f"created_at:>='{date.isoformat()}'"},
            operation_name="getOrdersInfoByDates",
        )
        try:
            nodes = json.loads(data)["data"]["orders"]["edges"]

            return [n["node"] for n in nodes]
        except Exception as ex:
            logger.warn(ex)
            return data

    def get_recent_orders(self, date=None):
        date = date or res.utils.dates.utc_days_ago(2)
        data = pd.DataFrame(self.list_order_info_since(date))
        if not len(data):
            return data
        # out convention of BrandCode-Number
        data["order_number"] = data["name"].map(
            lambda x: f"{self._brand_code}-{x.lstrip('#')}"
        )
        data["id"] = data["id"].map(lambda x: x.split("/")[-1])
        return data

    def get_order_by_name(self, name):
        """
        for the kit example: #66509 (if in last 60 days)
        """
        data = self.query(
            query=self._query_document,
            variables={"query": f"name:{name}"},
            operation_name="getOrdersByQuery",
        )
        try:
            node = json.loads(data)["data"]["orders"]["edges"][0]["node"]

            return self._parse_order_node(node)
        except Exception as ex:
            logger.warn(ex)
            return data

    def get_order_by_id(self, id):
        """
        for the kit example: 4714411851859 (if in last 60 days)
        """
        data = self.query(
            query=self._query_document,
            variables={"order_id": f"gid://shopify/Order/{id}"},
            operation_name="getOneOrder",
        )
        # check response
        try:
            node = json.loads(data)["data"]["node"]
            return self._parse_order_node(node)
        except Exception as ex:
            return data


class Shopify(object):
    _store_credentials: dict

    def __init__(
        self,
        endpoint=None,
        method=None,
        brand_id=None,
        payload=None,
        version=CURRENT_VERSION,
    ):
        self._endpoint = endpoint
        self._method = method
        # TODO: Change to use brand_code instead of brand_id
        self._brand_id = brand_id
        self.payload = payload
        self._version = version
        self._store_credentials = {}
        self.shopify_session = None
        if brand_id:
            self.active_shopify_session()

    def __enter__(self):
        self.active_shopify_session()
        return shopify

    def __exit__(self, type, value, traceback):
        self.terminate_shopify_session()

    def make_request(self):
        self.get_store_credentials()
        base_url = os.path.join(
            f'https://{self._store_credentials.get("shopifyStoreName")}.myshopify.com/admin/api/',
            self._version,
            self._endpoint,
        )
        response = requests.get(
            base_url,
            auth=(
                self._store_credentials.get("shopifyApiKey"),
                self._store_credentials.get("shopifyApiPassword"),
            ),
        )

    def post_request(self):
        self.get_store_credentials()
        base_url = f'https://{self._store_credentials.get("shopifyStoreName")}.myshopify.com/admin/api/{self._version}{self._endpoint}'

        response = requests.put(
            base_url,
            headers={"Content-Type": "application/json"},
            auth=(
                self._store_credentials.get("shopifyApiKey"),
                self._store_credentials.get("shopifyApiPassword"),
            ),
            data=json.dumps({"product": self.payload}),
        )

    def get_store_credentials(self):
        api_url = os.environ.get("GRAPHQL_API_URL", "http://localhost:3000")
        client = ResGraphQLClient(api_url=api_url)
        response = client.query(GET_BRAND, {"brandCode": self._brand_id})
        self._store_credentials = response["data"]["brand"]

    # create a shopify session
    def open_session(self):
        self.get_store_credentials()
        shop_url = "https://{}.myshopify.com".format(
            self._store_credentials.get("shopifyStoreName")
        )
        self.shopify_session = shopify.Session(
            shop_url=shop_url,
            version=shopify.Release(self._version).name,
            token=self._store_credentials.get("shopifyApiPassword"),
        )

    def active_shopify_session(self):
        if self.shopify_session is None:
            self.open_session()
        shopify.ShopifyResource.activate_session(self.shopify_session)

    def terminate_shopify_session(self):
        shopify.ShopifyResource.clear_session()

    def get_product(self, product_id):
        response = shopify.Product.find(id_=product_id).__dict__["attributes"]
        return response

    def update_product(self, product_id, payload):
        product = shopify.Product.find(product_id)
        for key in payload:
            setattr(product, key, payload[key])
        response = product.save()

    def query(self, query, variables):
        client = shopify.GraphQL()
        json_response = client.execute(query, variables)
        response = json.loads(json_response)
        logger.debug(json.dumps(response, indent=4))
        if "errors" in response:
            raise response.get("errors")
        return response.get("data")

    # Create/Update a product metafield in shopify using the shopify graphql api.
    def set_product_metafield(self, product_id, metafield_data):
        metafield_data.owner_id = int(product_id)
        metafield_data.owner_resource = "product"
        request = shopify.Metafield()
        data = metafield_data.__dict__
        if data["raw"]:
            del data["raw"]
        response = request.create(data)
        return response

    # Right now we do no store any record of the metafield ID but with can use the namespace and the key as alter keys.
    def get_product_metafield(self, product_id, namespace="", key=""):
        variables = {
            "product_id": "gid://shopify/Product/{}".format(product_id),
            "namespace": namespace,
            "key": key,
        }
        client = shopify.GraphQL()
        response = client.execute(ShopifyQuery.GET_METAFIELD, variables)
        product = json.loads(response).get("data", {}).get("product", {})
        if product:
            response = product.get("metafield", None)
        else:
            response = None
        logger.debug(json.dumps(response, indent=4))
        return response

    def delete_product_metafield(self, product_id, namespace, key):
        metafield = self.get_product_metafield(product_id, namespace, key)
        if metafield:
            variables = {"input": {"id": metafield["id"]}}
            client = shopify.GraphQL()
            return client.execute(ShopifyQuery.DELETE_METAFIELD, variables)

        logger.warn("Product {} do not have metafield".format(product_id), 1)
        return None

    def get_new_variant_instance(self):
        return shopify.Variant()

    def create_product_variant(self, product_id, new_variant_instance):
        product = shopify.Product.find(product_id)
        if isinstance(product, (shopify.Product, shopify.ShopifyResource)):
            product.add_variant(new_variant_instance)
