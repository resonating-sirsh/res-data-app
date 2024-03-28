from typing import Mapping, Union, TypedDict


class GraphQLJSONResponse(TypedDict):

    """
    TypedDict class representing the JSON structure of a GraphQL response
    """

    data: Mapping[str, Mapping[str, list | int | str]]
