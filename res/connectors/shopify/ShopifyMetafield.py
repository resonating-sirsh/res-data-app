import res.connectors.shopify.ShopifySerialize as ShopifySerialize
from enum import Enum


# Metafields Types supported by shopify.
# @link: https://shopify.dev/api/admin/rest/reference/metafield
class ShopifyMetafieldTypes:
    json = 'json_string'
    date = 'date'
    url = 'url'
    boolean = 'boolean'
    number_integer = 'number_integer'
    single_line_text_field = 'single_line_text_field'
    multi_line_text_field = 'multi_line_text_field'
    product_reference = 'product_reference'
    page_reference = 'page_reference'
    # there are more metafields define in shopify


class ResonanceShopifyMetafieldNamespaces:
    PRODUCT = 'create.ONE Product'
    VARIANT = 'create.ONE Variant'


class ResonanceShopifyMetafieldKey:
    PRODUCT_GROUP = 'Product Group'
    GROUP_BY = 'Group By'
    META = 'Meta'


class ShopifyMetafield(ShopifySerialize.Normalizer):
    key: str
    namespace: str
    value: str
    type: str
    owner_id: int
    owner_resource: str
    description: str

    def __init__(self, raw={}):
        super().__init__(raw)

    def _from_raw(self):
        self.key = self.raw['key']
        self.namespace = self.raw['namespace']
        self.value = self.raw['value']
        self.type = self.raw['type']
