from res.connectors.graphql.hasura import Client
from .hasura_graphql_definition import (
    GET_ECOMMERCE_COLLECTION_BY_ID,
    UPDATE_ECOMMERCE_COLLECTION,
)


def get_ecommerce_collection_by_id(id, hasura_client: Client):
    response = hasura_client.execute(GET_ECOMMERCE_COLLECTION_BY_ID, {"id": id})
    return response["sell_ecommerce_collections"][0]


def update_ecommerce_collection_by_id(id, input, hasura_client: Client):
    response = hasura_client.execute(
        UPDATE_ECOMMERCE_COLLECTION, {"id": id, "input": input}
    )
    return response["update_sell_ecommerce_collections_by_pk"]
