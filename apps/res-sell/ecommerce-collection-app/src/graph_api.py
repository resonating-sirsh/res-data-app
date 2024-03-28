from res.utils import logger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from src.graphql_definition import GET_IMAGE_URL


def get_image_url(id: str, client: ResGraphQLClient):
    response = client.query(GET_IMAGE_URL, {"id": id})
    logger.debug(response)
    return response["data"]["file"]["thumbnail"]["url"]
