"""

This is a one time script that would be adding a product variant metafields
in all live products.

"""

from binascii import Error
import json
import os
import copy
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from res.utils.secrets import secrets_client
from res.connectors.shopify.ShopifyConnector import Shopify, ShopifyMetafield, ShopifyMetafieldTypes, ResonanceShopifyMetafieldKey, ResonanceShopifyMetafieldNamespaces, shopify
from res.connectors.mongo.MongoConnector import MongoConnector
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.logging import logger

QUERY_ACTIVE_BRANDS = """
{
  brands(first: 100, where: { isActive: { is: true } }) {
    brands {
      id
      code
      name
    }
    cursor
    count
  }
}
"""
GET_SIZE = """
query getSize($size_code: String!){
  size(code: $size_code){
    id
    code
  }
}
"""

GET_PRODUCT = """
query getProduct($product_id: ID!){
	product(id: $product_id){
    id
    legacyResourceId
    title
    variants(first: 250){
      edges{
        node{
          id
          legacyResourceId
          sku
        }
      }
    }
  }
}
"""


def init():
    all_secrets = [
        'MONGODB_USER',
        'MONGODB_PASSWORD',
        'GRAPH_API_KEY',
    ]
    [secrets_client.get_secret(secret) for secret in all_secrets]


def get_all_products(product_collection: Collection, store_code) -> Cursor:
    return product_collection.find({'storeCode': store_code, 'isImported': True})


def generator(event, context):
    def create_node_event(a):
        e = copy.deepcopy(event)
        if "args" not in e:
            e['args'] = {}
        e["args"].update({
            'brand': a,
            'test': event.get('args').get('test')
        })
        return e

    graphql_client = ResGraphQLClient(
        api_url=os.getenv('GRAPHQL_API_URL', 'http://localhost:3000'))
    # getting all actives brands
    brands = graphql_client.query(QUERY_ACTIVE_BRANDS, {}).get(
        'data').get('brands').get('brands')
    response = [create_node_event(brand) for brand in brands]
    logger.debug(response)
    return response


def create_variant_metafield(products, variant, graphql_client: ResGraphQLClient):
    sku = variant.get('node').get('sku')
    current_product = None
    for product in products:
        if sku in product.get('allSku'):
            current_product = product
    if len(products) == 1:
        current_product = products[0]
    if not current_product:
        return
    sku_divided = sku.split(' ')
    size_code = sku_divided[-1]
    response = graphql_client.query(GET_SIZE, {"size_code": size_code})
    size = response.get('data').get('size')

    payload = {
        'styleId': current_product.get('styleId'),
        'sizeId': size.get('id'),
        'sizeCode': size_code
    }
    id = variant.get('node').get('legacyResourceId')
    metafield = ShopifyMetafield()
    metafield.owner_resource = 'variant'
    metafield.owner_id = int(id)
    metafield.type = ShopifyMetafieldTypes.json
    metafield.namespace = ResonanceShopifyMetafieldNamespaces.VARIANT
    metafield.key = ResonanceShopifyMetafieldKey.META
    metafield.value = json.dumps(payload)
    data = metafield.__dict__
    del data['raw']
    return data

def submit_metafield(variant):
    if not variant:
        return
    request = shopify.Metafield()
    request.create(variant)

def handler(event: dict, context: dict):
    init()
    brand = event.get('args').get('brand')
    test = event.get('args').get('test')
    brand_code = brand.get('code')
    product_collection = MongoConnector().get_client().get_database(
        'resmagic').get_collection('products')
    shopify_client = Shopify(brand_id=brand_code)
    products = get_all_products(product_collection, brand_code)
    graphql_client = ResGraphQLClient(
        api_url=os.getenv('GRAPHQL_API_URL', 'http://localhost:3000'))
    grouped_products = {}

    for product in products:
        ecommerce_id = product.get('ecommerceId')
        if ecommerce_id not in grouped_products:
            grouped_products[ecommerce_id] = []
        grouped_products[ecommerce_id].append(product)
    shopify_client.active_shopify_session()
    for ecommerce_id, products in grouped_products.items():
        shopify_product = shopify_client.query(
            GET_PRODUCT, {'product_id': f"gid://shopify/Product/{ecommerce_id}"}).get('product')
        variants = shopify_product.get('variants').get('edges')
        metafields = [create_variant_metafield(
            products, variant, graphql_client) for variant in variants]
        logger.debug(metafields)
        if not test:
            [submit_metafield(item) for item in metafields]
    
    shopify_client.terminate_shopify_session()


def on_success(event, context):
    pass


def on_failure(event, contex):
    pass


if __name__ == "__main__":
    handler({
        "args": {
            'brand':{
            'code': "TT"
        },
        'test': True}
    }, {})
