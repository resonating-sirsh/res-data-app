from typing import Any, Dict

from src.graphql_queries import UPDATE_PRODUCT_MUTATION

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

graphql_client = ResGraphQLClient()


def update_product(id: str, input: Dict[str, Any]) -> Dict[str, Any]:
    return graphql_client.query(
        UPDATE_PRODUCT_MUTATION,
        {
            "id": id,
            "input": input,
        },
    )
