from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import res.utils.meta.sizes.graphql_queries as graphql_queries

try:
    gql = ResGraphQLClient()
except:
    # bad practice to init objects that use secrets in module loading
    pass


def get_size_from_size_name(size_name):
    found_sizes = gql.query(graphql_queries.QUERY_SIZES, {"search": size_name})["data"][
        "sizes"
    ]["sizes"]

    if len(found_sizes) > 0:
        size = found_sizes[0]
    else:
        raise Exception(
            f"Size {size_name} not found, please verify if the size its correct or if this variant its recognized as an Alias."
        )

    return size


def get_sizes_from_aliases(size_alias_arr):
    # Sizes seems to not be normalized at times to the names we have in the sizes table (use case: vStitcher outputs).
    # As a workaround an "Alias" field was added to the sizes table so we can search by it.
    found_sizes = gql.query(
        graphql_queries.QUERY_SIZES,
        {"where": {"aliases": {"hasAnyOf": size_alias_arr}}},
    )["data"]["sizes"]["sizes"]

    if len(found_sizes) == 0:
        raise Exception(
            f"Sizes {size_alias_arr} not found, please verify if the size its correct or if this variant its recognized as an Alias."
        )

    return found_sizes


def get_alias_to_size_lookup(size_aliases):
    sizes = gql.query(
        graphql_queries.QUERY_ALL_SIZES,
        {"where": {"aliases": {"hasAnyOf": size_aliases}}},
    )["data"]["sizes"]["sizes"]

    size_lookup = {}

    def add_alias(size):
        if size != None:
            for alias in size["aliases"]:
                size_lookup[alias] = size["code"]

    for s in sizes:
        add_alias(s)

    return size_lookup
