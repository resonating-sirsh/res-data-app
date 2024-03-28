import res
from res.utils import logger
from pymongo import MongoClient
from res.connectors.shopify.ShopifyConnector import Shopify

db: MongoClient = res.connectors.load("mongo").get_client()
mongo = db.get_database("resmagic")
products_cursor = mongo.get_collection("products").find({"isImported": True})

products = [product for product in products_cursor]

logger.info(len(products))


def _map_from_variant(variant):
    return {
        "id": variant.id,
        "sku": variant.sku,
        "option1": variant.option1,
        "option2": variant.option2,
        "option3": variant.option3,
        "compareAtPrice": variant.compare_at_price,
        "price": variant.price,
        "position": variant.position,
        "inventoryPolicy": variant.inventory_policy,
        "imageId": variant.image_id,
    }


for product in products:
    with Shopify(brand_id=product["storeCode"]) as sp:
        try:
            sp_product = sp.Product.find(id_=int(product["ecommerceId"]))
            variants = [_map_from_variant(variant) for variant in sp_product.variants]
            mongo.get_collection("products").find_one_and_update(
                {"_id": product["_id"]}, {"$set": {"variants": variants}}
            )
            logger.info(f"product_id: {product['_id']}")
        except:
            logger.info("except continue")
            continue
