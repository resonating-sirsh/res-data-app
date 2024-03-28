import arrow
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import src.shopify_collection as ShopifyCollection
from src.graph_api import get_image_url
from src.hasura import get_ecommerce_collection_by_id, update_ecommerce_collection_by_id
from src.utils import is_smart_collection
from res.connectors.graphql.hasura import Client


def export_collection_to_ecommerce(
    collection_id: str,
    collection: dict,
    hasura_client: Client,
    graphql_client: ResGraphQLClient,
):
    store_code = collection["store_code"]

    image_id = collection.get("image_id")
    if image_id:
        image_url = get_image_url(image_id, graphql_client)
        collection["image_url"] = image_url

    if is_smart_collection(collection):
        payload = ShopifyCollection.map_from_ecommerce_collection_to_smart_collection(
            collection
        )
        sp_collection = ShopifyCollection.create_smart_collection_in_ecommerce(
            store_code,
            payload,
        )
        ecommerce_id = str(sp_collection.id)
    else:
        payload = ShopifyCollection.map_from_ecommerce_collection_to_custom_collection(
            collection
        )
        sp_collection = ShopifyCollection.create_custom_collection_in_ecommerce(
            store_code,
            payload,
        )
        ecommerce_id = str(sp_collection.id)

    response_collection = update_ecommerce_collection_by_id(
        collection_id,
        {
            "ecommerce_id": ecommerce_id,
            "synced_at": arrow.utcnow().isoformat(),
            "status": "exported",
        },
        hasura_client,
    )

    return {"sell_ecommerce_collection": response_collection}, 200


def delete_collection_from_ecommerce(
    collection_id: str,
    ecommerce_collection: dict,
    hasura_client: Client,
):

    store_code = ecommerce_collection["store_code"]
    ecommerce_id = ecommerce_collection["ecommerce_id"]

    if is_smart_collection(ecommerce_collection):
        ShopifyCollection.delete_smart_collection_in_ecommerce(
            store_code=store_code,
            collection_id=ecommerce_id,
        )
    else:
        ShopifyCollection.delete_custom_collection_in_ecommerce(
            store_code=store_code,
            collection_id=ecommerce_id,
        )

    response_collection = update_ecommerce_collection_by_id(
        collection_id,
        {
            "ecommerce_id": None,
            "published_at": None,
            "synced_at": arrow.utcnow().isoformat(),
            "publication_metadata": None,
            "status": "delete",
        },
        hasura_client,
    )

    return {"sell_ecommerce_collection": response_collection}, 200


def publish_collection_in_ecommerce(collection_id: str, hasura_client: Client):
    ecommerce_collection = get_ecommerce_collection_by_id(
        collection_id,
        hasura_client,
    )

    store_code = ecommerce_collection["store_code"]
    ecommerce_id = ecommerce_collection["ecommerce_id"]

    publications_list = ShopifyCollection.publish_collection(ecommerce_id, store_code)
    response_collection = update_ecommerce_collection_by_id(
        collection_id,
        {
            "published_at": arrow.utcnow().isoformat(),
            "publication_metadata": list(
                {
                    collection_publication.id: {
                        "id": collection_publication.id,
                        "publication_id": collection_publication.attributes.get(
                            "publication_id"
                        ),
                        "published_at": collection_publication.attributes.get(
                            "published_at"
                        ),
                    }
                    for collection_publication in publications_list
                    if collection_publication.id is not None
                }.values()
            ),
        },
        hasura_client,
    )

    return {
        "store_code": response_collection["store_code"],
        "ecommerce_id": response_collection["ecommerce_id"],
    }, 200


def unpublish_collection_in_ecommerce(collection_id: str, hasura_client: Client):
    ecommerce_collection = get_ecommerce_collection_by_id(
        collection_id,
        hasura_client,
    )
    store_code = ecommerce_collection["store_code"]

    ShopifyCollection.unpublish_collection(
        store_code=store_code,
        collection_publications=ecommerce_collection["publication_metadata"],
    )
    update_ecommerce_collection_by_id(
        collection_id,
        {
            "published_at": None,
            "publication_metadata": None,
            "status": "unpublish",
            "synced_at": arrow.utcnow().isoformat(),
        },
        hasura_client,
    )

    return {"ok": True}, 200


def update_collection_in_ecommerce(
    collection_id: str,
    ecommerce_collection: dict,
    hasura_client: Client,
    graphql_client: ResGraphQLClient,
):
    store_code = ecommerce_collection["store_code"]
    ecommerce_id = ecommerce_collection["ecommerce_id"]

    image_id = ecommerce_collection.get("image_id")
    if image_id:
        image_url = get_image_url(image_id, graphql_client)
        ecommerce_collection["image_url"] = image_url

    if is_smart_collection(ecommerce_collection):
        payload = ShopifyCollection.map_from_ecommerce_collection_to_smart_collection(
            ecommerce_collection
        )

        ShopifyCollection.update_smart_collection_in_ecommerce(
            store_code=store_code,
            collection_id=ecommerce_id,
            payload=payload,
        )
    else:
        payload = ShopifyCollection.map_from_ecommerce_collection_to_custom_collection(
            ecommerce_collection
        )

        ShopifyCollection.update_custom_collection_in_ecommerce(
            store_code=store_code,
            collection_id=ecommerce_id,
            payload=payload,
        )

    response_collection = update_ecommerce_collection_by_id(
        collection_id,
        {
            "updated_at": arrow.utcnow().isoformat(),
            "synced_at": arrow.utcnow().isoformat(),
        },
        hasura_client,
    )

    return {"sell_ecommerce_collection": response_collection}, 200
