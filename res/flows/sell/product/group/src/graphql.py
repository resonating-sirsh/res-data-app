from typing import Dict, List

from bson.objectid import ObjectId
from shopify import Product

import res.flows.sell.product.group.src.querys as query
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.mongo.MongoConnector import MongoConnector
from res.connectors.shopify.utils.ShopifyProduct import ShopifyProduct
from res.utils import logger


def verify_image_public(url, file_collection, graphql) -> str:
    url_slug = url.split("/")[-1]
    url_slug_splitted = str(url_slug).split("?")
    file_name = url_slug_splitted[0]
    logger.info(f"verifing if file '{file_name}' is public")
    # file = graphql.query(GET_FILE_PRODUCT_BY_NAME, {"name": file_name})
    file_s3_id = file_name.split(".")[0]
    file = file_collection.find_one({"s3.id": file_s3_id})
    if not file.get("isPublic"):
        logger.info(f"file '{file_name}' is not public")
        # make file public
        data = graphql.query(query.MAKE_FILE_PUBLIC, {"id": str(file["_id"])})["data"][
            "makeFilePublic"
        ]
        url = data["file"]["thumbnail"]["url"]
        return url
    logger.info(f"file '{file_name}' is public")
    return url.split("?")[0]


def get_all_products(variables, graphql: ResGraphQLClient):
    data = graphql.query(
        query=query.GET_PRODUCTS, variables={"first": 50, "where": variables}
    )["data"]["products"]
    products = data["products"]
    has_more = data["hasMore"]
    cursor = data["cursor"]
    while has_more:
        data = graphql.query(
            query=query.GET_PRODUCTS,
            variables={"first": 50, "where": variables, "after": cursor},
        )["data"]
        products.extend(data["products"]["products"])
        cursor = data["products"]["cursor"]
        has_more = data["products"]["hasMore"]
    return products


def disable_shop_by_color_from_product(id: str, graphql: ResGraphQLClient):
    logger.info(f"removing sbc from ecommerce on product {id} ")
    return graphql.query(query.REMOVE_FROM_SHOPIFY, {"id": id})["data"][
        "disableProductShopByColor"
    ]["product"]


def get_products(first, variables, graphql):
    data = graphql.query(
        query=query.GET_PRODUCTS, variables={"first": first, "where": variables}
    )["data"]["products"]
    return data["products"]


def get_shop_by_color_record_by_id(id, graphql: ResGraphQLClient):
    return graphql.query(query=query.SHOP_BY_COLOR_BY_ID, variables={"id": id,},)[
        "data"
    ]["shopByColorInstance"]


def update_shop_by_color_record(id, input, graphql: ResGraphQLClient):
    logger.info(f"updating shop by color record {id}")
    return graphql.query(
        query=query.UPDATE_SHOP_BY_COLOR, variables={"id": id, "input": input}
    )["data"]["updateShopByColorInstance"]["shopByColorInstance"]


def release_shop_by_color(id, instance_id, graphql: ResGraphQLClient):
    logger.info(f"releasing shop_by_color for product '{id}'")
    return graphql.query(
        query=query.RELEASE_TO_SHOPIFY,
        variables={"id": id, "instance_id": instance_id},
    )["data"]["releaseProductShopByColor"]["product"]


def update_product_shop_by_color(id, graphql: ResGraphQLClient):
    logger.info(f"update_shop_by_color '{id}'")
    return graphql.query(
        query.UPDATE_PRODUCT_ECOMMERCE_SBC,
        {
            "id": id,
        },
    )["data"]


def get_shop_by_color_instance(id: str, graphql: ResGraphQLClient):
    logger.info(f"getting shop by color instance for product '{id}'")
    return graphql.query(
        query=query.GET_SHOP_BY_COLOR_INSTANCE,
        variables={
            "id": id,
        },
    )["data"]["shopByColorInstance"]


def validate_color_options(
    shop_by_color_instance,
    file_collection,
    graphql,
):
    brand_code = shop_by_color_instance["storeBrand"]["code"]
    options = shop_by_color_instance["options"]

    with ShopifyProduct(brand_code=brand_code) as response:
        shopify, client = response
        options = [
            update_option_from_shopify(
                option, brand_code, shopify, graphql, file_collection
            )
            for option in options
        ]
        options = [
            option
            for option in options
            if option and client.check_product_is_live(option["productId"])
        ]

    product_ids = []
    logger.info(f"options: {len(options)}")
    for option in options:
        product_ids.append(option["productId"])
        del option["productId"]
    logger.info("updating shop by color instance")

    update_shop_by_color_record(
        id=shop_by_color_instance["id"], input={"options": options}, graphql=graphql
    )
    return product_ids


def get_instance_id_from_event(event):
    return event.get("args").get("id")


def update_option_from_shopify(option, brand_code, shopify, graphql, file_collection):
    logger.info(f"updating option {option['colorName']} {option['styleId']}")
    product = get_product_from_style_id(
        style_id=option["styleId"], brand_code=brand_code, graphql=graphql
    )
    if not product or not product["ecommerceId"]:
        return None
    logger.debug(f"product {product}")
    shopify_product: Product = shopify.Product().find(id_=int(product["ecommerceId"]))
    logger.debug(f"shopify_product {shopify_product}")
    option["productId"] = product["id"]
    option["handle"] = shopify_product.handle
    option["price"] = shopify_product.price_range()
    option["imageUrl"] = verify_image_public(
        option["imageUrl"], file_collection, graphql
    )
    option["referenceDate"] = shopify_product.published_at
    images = shopify_product.images
    if images:
        image = images[0]
        logger.info(f"image: {image}")
        if image:
            option["images"] = [image.src]
        else:
            option["images"] = []
    else:
        option["images"] = []
    logger.debug(f"option {option}")
    return option


def get_product_from_style_id(style_id, brand_code, graphql):
    query = {
        "storeCode": {"is": brand_code},
        "styleId": {"is": style_id},
        "isLive": {"is": True},
    }
    product = get_products(first=1, variables=query, graphql=graphql)
    if product:
        return product[0]
    return None


def list_shop_by_color_instance(first: int, where: dict, client: ResGraphQLClient):
    logger.info(f"looking for BM materials where: {where}")
    return client.query(
        query=query.GET_SBC_INSTANCES, variables={"first": first, "where": where}
    )["data"]["shopByColorInstances"]["shopByColorInstances"]


def validate_if_other_sbc_live(shop_by_color, graphql):
    logger.info(f"check if there is a instance in the same BM for")
    bodyCodes = [body["code"] for body in shop_by_color["bodies"]]
    materialCodes = [material["code"] for material in shop_by_color["materials"]]
    store_code = shop_by_color["storeBrand"]["code"]
    where = {
        "and": [
            {"id": {"isNot": shop_by_color["id"]}},
            {"bodyCodes": {"is": bodyCodes}},
            {"materialCodes": {"isAnyOf": materialCodes}},
            {"live": {"is": True}},
            {"storeCode": {"is": store_code}},
        ]
    }
    instances = list_shop_by_color_instance(first=25, where=where, client=graphql)
    logger.info(f"instances: {instances}")
    if not instances:
        return []
    # remove shop by color.
    products = []
    for instance in instances:
        products.extend(product["id"] for product in instance["products"])

    products = set(products)

    for product in products:
        disable_shop_by_color_from_product(product, graphql=graphql)
    for instance in instances:
        update_shop_by_color_record(
            instance["id"], {"live": False, "status": "success"}, graphql=graphql
        )
    return instances


def create_stamped_group(product_ids: List[str]) -> List[Dict]:
    response = []
    db = MongoConnector().get_client().get_database("resmagic")
    products = db.products.find({"_id": {"$in": [ObjectId(id) for id in product_ids]}})
    for product in products:
        response.append(
            {
                "matchColumn": "ProductId",
                "matchType": "Equals",
                "isInclude": True,
                "value": product["ecommerceId"],
            }
        )

    return response
