from res.connectors.shopify.ShopifyConnector import Shopify, handle_api_rate_limit
from res.utils import logger


def decorator(check=True):
    def wrapper(function):
        @handle_api_rate_limit(max_attempts=3)
        def inner_wrapper(*args, **kwargs):
            response = function(*args, **kwargs)
            if not check:
                return response
            return post_condition(response)

        return inner_wrapper

    return wrapper


def post_condition(response):
    errors = response.errors.full_messages()
    if not errors:
        return response
    logger.warning(errors)
    raise Exception("Something happen on your requests")


@decorator()
def create_smart_collection_in_ecommerce(store_code, payload):
    logger.info(f"creating collection on store {store_code}")
    logger.debug(f"create_collection_payload: {payload}")
    with Shopify(brand_id=store_code) as shopify_client:
        response = shopify_client.SmartCollection.create(payload)
        logger.debug(f"collection created: {response.id}")
        return response


@decorator()
def update_smart_collection_in_ecommerce(collection_id, store_code, payload):
    logger.info(f"updating collection {collection_id} on store {store_code}")
    logger.debug(f"create_collection_payload: {payload}")
    with Shopify(brand_id=store_code) as shopify_client:
        response = shopify_client.SmartCollection.find(id_=collection_id)
        response.title = payload["title"]
        response.body_html = payload["body_html"]
        response.rules = payload["rules"]
        response.disjuntive = payload["disjuntive"]
        response.handle = payload["handle"]

        if payload.get("image_url"):
            response.image = payload["image_url"]

        response.sort_order = payload["sort_order"]
        response.save()
        return response


@decorator()
def create_custom_collection_in_ecommerce(store_code, payload):
    logger.info(f"creating collection on store {store_code}")
    logger.debug(f"create_collection_payload: {payload}")
    with Shopify(brand_id=store_code) as shopify_client:
        response = shopify_client.CustomCollection.create(payload)
        logger.debug(f"collection created: {response.id}")
        return response


@decorator(check=False)
def delete_custom_collection_in_ecommerce(store_code, collection_id):
    logger.info(f"delete collection {collection_id} on store {store_code}")
    with Shopify(brand_id=store_code) as shopify_client:
        response = shopify_client.CustomCollection.delete(int(collection_id))
        return response


@decorator()
def update_custom_collection_in_ecommerce(store_code, collection_id, payload):
    logger.info(f"updating collection {collection_id} on store {store_code}")
    logger.debug(f"create_collection_payload: {payload}")
    with Shopify(brand_id=store_code) as shopify_client:
        response = shopify_client.CustomCollection.find(id_=collection_id)
        response.title = payload["title"]
        response.body_html = payload["body_html"]
        # here a error where you send the same collect throw error in shopify
        # response.collects = payload["collects"]
        if payload.get("image_url"):
            response.image = {"src": payload["image_url"], "alt": "Collection Logo"}
        response.handle = payload["handle"]
        response.sort_order = payload["sort_order"]
        response.save()
        return response


@decorator(check=False)
def delete_smart_collection_in_ecommerce(store_code, collection_id):
    logger.info(f"deleting collection {collection_id} on store {store_code}")
    with Shopify(brand_id=store_code) as shopify_client:
        response = shopify_client.SmartCollection.delete(int(collection_id))
        return response


"""
Publish a collection in every available sale channel of the brand store.

TODO: In the future maybe we want to handle which channel we want to publish collection.
"""


@decorator(check=False)
def publish_collection(collection_id, store_code):
    logger.info(f"unpublish collection {collection_id}")
    responses = []
    with Shopify(brand_id=store_code) as shopify_client:
        publications = shopify_client.Publication.find()
        logger.debug([publication.name for publication in publications])
        for publication in publications:
            logger.info(
                f"publish collection {collection_id} on sale channel: {publication.name} "
            )
            publication_id = publication.id
            payload = {
                "publication_id": publication_id,
                "collection_id": collection_id,
                "published": True,
            }
            response = shopify_client.CollectionPublication.create(payload)
            responses.append(response)
    return responses


@decorator(check=False)
def unpublish_collection(collection_publications, store_code):
    with Shopify(brand_id=store_code) as shopify_client:
        for collection_publication in collection_publications:
            print(collection_publication)
            record = shopify_client.CollectionPublication.find(
                collection_publication["id"],
                publication_id=collection_publication["publication_id"],
            )
            record.destroy()


def map_from_ecommerce_collection_to_smart_collection(collection):
    rules = collection["rules"]
    base = map_common_fields(collection)
    smart_collection_fields = dict()
    if not rules:
        return base

    smart_collection_fields["disjuntive"] = collection["disjuntive"]
    smart_collection_fields["rules"] = rules

    return {
        **base,
        **smart_collection_fields,
    }


def map_from_ecommerce_collection_to_custom_collection(collection):
    rules = collection["rules"]
    base = map_common_fields(collection)
    if not rules:
        return base

    custom_collection_field = dict()
    custom_collection_field["collects"] = map_collects_from_collection_rules(collection)

    return {
        **base,
        **custom_collection_field,
    }


def map_common_fields(collection):
    return {
        "title": collection["title"],
        "body_html": collection["body_html"],
        "handle": collection["handle"],
        "sort_order": collection["sort_order_fk"].replace("_", "-"),
        "template_suffix": None,
        "published": False,
        **(
            {"image": {"src": collection["image_url"], "alt": "Collection Logo"}}
            if "image_url" in collection
            else {}
        ),
    }


def map_collects_from_collection_rules(rules):
    return [
        {
            "product_id": rule["value"],
        }
        for rule in rules["rules"]
    ]


def map_rules_from_ecommerce_collection(rules):
    return [
        {
            "column": rule["property"],
            "relation": rule["condition"],
            "condition": rule["value"],
        }
        for rule in rules
    ]
