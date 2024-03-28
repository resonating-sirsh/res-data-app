import os
from res.utils.logging.ResLogger import ResLogger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.shopify.ShopifyConnector import Shopify
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer

# Change below before deployinng
import graphql_queries

logger = ResLogger()

TOPIC = "res_sell.shopify_variants.creation_request"


def run_consumer(kafka_consumer):
    graphql_client = ResGraphQLClient()

    while True:
        new_variant_request = kafka_consumer.poll_avro()
        # new_variant_request = {"size_id": "recqTzZTIQlE43nkT", "body_code": "TT-3008"}
        if new_variant_request:
            body_code = new_variant_request.get("body_code")
            size_id = new_variant_request.get("size_id")

            logger.info(
                f"Starting to create new size variants for body: {body_code}, for the size id: {size_id}"
            )

            response = graphql_client.query(graphql_queries.GET_SIZE, {"id": size_id})
            size = response.get("data").get("size")
            styles = get_styles(body_code, graphql_client)
            products = []

            if len(styles) > 0:
                products = get_products(styles, graphql_client)
                missing_variants, ecommerce_products = create_missing_variants(
                    graphql_client, products, size
                )
                logger.info(f"Missing Variants: {missing_variants}")
                update_product_tags(missing_variants, ecommerce_products)
            else:
                logger.info("This does not have styles, skipping flow!")


def get_styles(body_code, graphql_client):
    query_response = graphql_client.query(
        query=graphql_queries.GET_STYLES,
        variables={"where": {"bodyCode": {"is": body_code}}},
        paginate=True,
    )

    return query_response["data"]["styles"]["styles"]


def create_missing_variants(graphql_client, products, size):
    ecommerce_products = []
    missing_variants = []

    for product in products:
        missing_variants = []
        shopify_client = Shopify(brand_id=product["storeCode"])
        ecommerce_product = shopify_client.get_product(product["ecommerceId"])
        ecommerce_product["store_code"] = product["storeCode"]
        ecommerce_products.append(ecommerce_product)
        variants = ecommerce_product["variants"]
        missing_variants.extend(
            get_missing_variants(
                variants, product, ecommerce_product, size, graphql_client
            )
        )
        options = get_product_option(product, ecommerce_product)
        payload = assemble_variants_payload(missing_variants, product, graphql_client)
        product_payload = {"id": ecommerce_product["id"], "variants": []}
        for item in payload:
            if len(options) > len(ecommerce_product["options"]):
                product_payload["options"] = options
                product_payload["variants"].append(item)
                for variant in ecommerce_product["variants"]:
                    category_option = (
                        "option3"
                        if variant.__dict__["attributes"]["option2"]
                        else "option2"
                    )
                    # find size based on code and use the record category
                    sku_parts = variant.__dict__["attributes"]["sku"].split(" ")
                    response = graphql_client.query(
                        graphql_queries.GET_SIZE, {"code": sku_parts[3]}
                    )
                    variant_size = response.get("data").get("size")
                    product_payload["variants"].append(
                        {
                            "title": variant.__dict__["attributes"]["title"],
                            "price": variant.__dict__["attributes"]["price"],
                            "sku": variant.__dict__["attributes"]["sku"],
                            "weight": 0,
                            "weight_unit": "g",
                            "inventory_management": "shopify",
                            "inventory_policy": "continue",
                            "compare_at_price": variant.__dict__["attributes"][
                                "compare_at_price"
                            ],
                            "option1": variant.__dict__["attributes"]["option1"],
                            "option2": variant.__dict__["attributes"]["option2"]
                            if variant.__dict__["attributes"]["option2"]
                            else variant_size.get("category"),
                            "option3": variant_size.get("category")
                            if category_option == "option3"
                            else variant.__dict__["attributes"]["option3"],
                        }
                    )
                shopify_client = Shopify(
                    brand_id=product["storeCode"],
                    endpoint=f"/products/{ecommerce_product['id']}.json",
                    payload=product_payload,
                )

                shopify_client.post_request()
            else:
                new_variant_instance = shopify_client.get_new_variant_instance()
                new_variant_instance.title = item["title"]
                new_variant_instance.price = item["price"]
                new_variant_instance.sku = item["sku"]
                new_variant_instance.weight = item["weight"]
                new_variant_instance.weight_unit = item["weight_unit"]
                new_variant_instance.inventory_management = item["inventory_management"]
                new_variant_instance.inventory_policy = item["inventory_policy"]
                new_variant_instance.compare_at_price = item["compare_at_price"]
                if "option1" in item.keys():
                    new_variant_instance.option1 = item["option1"]
                if "option2" in item.keys():
                    new_variant_instance.option2 = item["option2"]
                if "option3" in item.keys():
                    new_variant_instance.option3 = item["option3"]
                    shopify_client.create_product_variant(
                        product_id=ecommerce_product["id"],
                        new_variant_instance=new_variant_instance,
                    )
    return missing_variants, ecommerce_products


def get_products(styles, graphql_client):
    products = []
    for style in styles:
        query_response = graphql_client.query(
            query=graphql_queries.GET_PRODUCTS,
            variables={
                "where": {
                    "styleId": {"is": style["id"]},
                    "isImported": {"is": True},
                    "isLive": {"is": True},
                }
            },
        )
        for product in query_response["data"]["products"]["products"]:
            product["style"] = style
            products.append(product)
    return products


def get_product_option(product, ecommerce_product):
    should_create = True
    current_options = []
    size_categories = []
    for size in product.get("style").get("availableSizes"):
        if size.get("category") not in size_categories:
            size_categories.append(size.get("category"))
    for option in ecommerce_product.get("options"):
        current_options.append(
            {
                "name": option.__dict__["attributes"]["name"],
                "values": option.__dict__["attributes"]["values"],
            }
        )
        if option.__dict__["attributes"]["name"] == "Size Type":
            should_create = False
    if should_create and len(size_categories) > 1:
        current_options.append({"name": "Size Type", "values": ["Regular", "Petite"]})
    return current_options


def get_missing_variants(variants, product, ecommerce_product, size, graphql_client):
    missing_sizes = []
    variants_sizes = map(lambda x: x.__dict__["attributes"]["option1"], variants)
    variants_sizes = list(variants_sizes)
    option_2_arr = []
    option_3_arr = []
    size_category_option_position = None
    ecomm_size_options = get_product_option(product, ecommerce_product)
    if variants[0].__dict__["attributes"]["option2"]:
        for variant in variants:
            if variant.__dict__["attributes"]["option2"] not in option_2_arr:
                option_2_arr.append(variant.__dict__["attributes"]["option2"])
            if (
                variant.__dict__["attributes"]["option3"]
                and variant.__dict__["attributes"]["option3"] not in option_3_arr
            ):
                option_3_arr.append(variant.__dict__["attributes"]["option3"])
    if (
        size.get("category") not in option_2_arr
        and size.get("category") not in option_3_arr
    ):
        for option in ecommerce_product.get("options"):
            option = option.__dict__["attributes"]
            if option.get("name") == "Size Type" and option.get("position") == 2:
                option_2_arr.append(size.get("category"))
            elif option.get("name") == "Size Type" and option.get("position") == 3:
                option_3_arr.append(size.get("category"))
            else:
                if len(option_2_arr) > 0 and len(ecomm_size_options) > 1:
                    size_category_option_position = 3
                elif len(ecomm_size_options) > 1:
                    size_category_option_position = 2

    elif len(ecomm_size_options) > 1:
        for option in ecommerce_product.get("options"):
            option = option.__dict__["attributes"]
            size_category_option_position = option.get("position")

    brand_size = graphql_client.query(
        graphql_queries.GET_BRAND_SIZE,
        {
            "where": {
                "sizeId": {"is": size["id"]},
                "brandCode": {"is": product["storeCode"]},
            }
        },
    )["data"]["brandSizes"]["brandSizes"]
    brand_size_name = brand_size[0]["brandSizeName"] if len(brand_size) > 0 else None
    if not brand_size_name:
        brand_size = create_brand_size(size, product["storeCode"], graphql_client)
        brand_size_name = brand_size["brandSizeName"]
    if size_category_option_position == 2:
        if len(option_3_arr) > 0:
            for option3 in option_3_arr:
                missing_sizes.append(
                    {
                        "id": size["id"],
                        "code": size["code"],
                        "name": size["name"],
                        "ecommerceTag": size["ecommerceTag"],
                        "option1": brand_size_name,
                        "option2": size.get("category"),
                        "option3": option3,
                    }
                )
        else:
            missing_sizes.append(
                {
                    "id": size["id"],
                    "code": size["code"],
                    "name": size["name"],
                    "ecommerceTag": size["ecommerceTag"],
                    "option1": brand_size_name,
                    "option2": size.get("category"),
                }
            )
    elif size_category_option_position == 3:
        for option2 in option_2_arr:
            missing_sizes.append(
                {
                    "id": size["id"],
                    "code": size["code"],
                    "name": size["name"],
                    "ecommerceTag": size["ecommerceTag"],
                    "option1": brand_size_name,
                    "option2": option2,
                    "option3": size.get("category"),
                }
            )
    elif len(ecomm_size_options) > 1:
        missing_sizes.append(
            {
                "id": size["id"],
                "code": size["code"],
                "name": size["name"],
                "ecommerceTag": size["ecommerceTag"],
                "option1": brand_size_name,
                "option2": size.get("category"),
            }
        )
    else:
        missing_sizes.append(
            {
                "id": size["id"],
                "code": size["code"],
                "name": size["name"],
                "ecommerceTag": size["ecommerceTag"],
                "option1": brand_size_name,
            }
        )
    return missing_sizes


def create_brand_size(size, brand_code, graphql_client):
    payload = {
        "code": size["code"],
        "customName": size["sizeNormalized"],
        "sizeId": size["id"],
        "brandCode": brand_code,
        "isEnabled": True,
    }

    response = graphql_client.query(
        graphql_queries.CREATE_BRAND_SIZE, {"input": payload}
    )
    return response["data"]["brandSize"]["brandSize"]


def get_variant_options(product):
    options = []
    style = product["style"]

    if style["shopifyOption1Name"]:
        options.append({"name": style["shopifyOption1Name"]})

    if style["shopifyOption2Name"]:
        options.append(
            {
                "name": "Color"
                if style["shopifyOption2Name"] == "Color Name"
                and style["brandCode"] == "RB"
                else style["shopifyOption2Name"]
            }
        )

    if style["shopifyOption3Name"]:
        options.append({"name": style["shopifyOption3Name"]})

    return options


def assemble_variants_payload(missing_variants, product, graphql_client):
    variants_payload = []
    options = get_variant_options(product)

    for variant in missing_variants:
        payload = {
            "title": variant["name"],
            "price": product["price"],
            "sku": f"{product['style']['code'].replace('-', '')} {variant['code']}",
            "weight": 0,
            "weight_unit": "g",
            "inventory_management": "shopify",
            "inventory_policy": "continue",
            "compare_at_price": product["price"],
            "option1": variant.get("option1", None),
            "option2": variant.get("option2", None),
            "option3": variant.get("option3", None),
        }

        option_count = 1
        for option in options:
            sizeOption = f"option{option_count}"
            if option["name"] == "Size":
                brandSize = graphql_client.query(
                    graphql_queries.GET_BRAND_SIZE,
                    {
                        "where": {
                            "sizeId": {"is": variant["id"]},
                            "brandCode": {"is": product["storeCode"]},
                        }
                    },
                )["data"]["brandSizes"]["brandSizes"][0]
                payload[sizeOption] = brandSize["brandSizeName"]
            # else:
            #     key = f"shopifyOption{option_count}Value"
            #     payload[sizeOption] = product["style"][key].replace("-", " ").lower()
            option_count += 1
        variants_payload.append(payload)
    return variants_payload


def update_product_tags(missing_variants, ecommerce_products):
    new_tags = map(lambda x: f"{x['ecommerceTag']}", missing_variants)
    new_tags = ",".join(list(new_tags))
    for product in ecommerce_products:
        shopify_client = Shopify(brand_id=product["store_code"])
        tags = product["tags"]
        payload = {"tags": f"{tags},{new_tags}"}
        shopify_client.update_product(product["id"], payload)


if __name__ == "__main__":
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, TOPIC)
    # # Start Kafka consumer
    run_consumer(kafka_consumer)
