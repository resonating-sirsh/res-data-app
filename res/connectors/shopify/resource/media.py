import traceback
from typing import Any, Dict, List

import shopify
from pyactiveresource.util import json
from pydantic import BaseModel

from res.connectors.shopify.base.ShopifyBase import ShopifyClientBase
from res.utils import logger


class ProductCreateMediaInput(BaseModel):
    """
    Payload used on the productCreateMedia mutation for Shopify Product Media.

    alt: str
    mediaContentType: MediaType ['VIDEO', 'EXTERNAL_VIDEO', ... etc]
    originalSource: str -> url where the asset is upload

    Note:

    In case of EXTERNAL_VIDEO shopify use Youtube and Vimeo as trusted source.

    For VIDEO and MODEL_3D, the asset must be upload in stagedUpload and use the stageUpload.resourceUrl as the originalSource
    """

    alt: str
    mediaContentType: str
    originalSource: str


class ProducCreateMediaResponse(BaseModel):
    media: List[Dict[str, Any]]
    mediaUserErrors: List[Dict[str, Any]]
    product: Dict[str, Any]


CREATE_PRODUCT_MEDIA = """
 mutation productCreateMedia($media: [CreateMediaInput!]!, $id: ID!) {
   productCreateMedia(media: $media, productId: $id) {
     media {
       alt
       mediaContentType
       status
     }
     mediaUserErrors {
       field
       message
     }
     product {
       id
       title
     }
   }
 }
 """

DELETE_PRODUCT_MEDIA = """
mutation productDeleteMedia ($productId: ID!, $mediaIds: [ID!]!) {
    productDeleteMedia(productId: $productId, 
    mediaIds: $mediaIds
    ){
        deletedMediaIds
    }
}
"""

GET_PRODUCT_MEDIA = """
query getProduct ($id: ID!, $after: String) {
  product(id: $id) {
    media(first: 40, after: $after) {
      edges {
        node {
            ... on MediaImage {
                id
            }
            ... on Video {
                id
            }
            ... on Model3d {
                id
            }
            ... on ExternalVideo {
                id
            }
        }
      }
      pageInfo {
          hasPreviousPage
          endCursor
          startCursor
          hasNextPage
      }
    }
  }
}
"""


class ShopifyProductMedia(ShopifyClientBase):
    """
    Shopify Product Media API

    This helper class try to be a wrapper arround the Shopify Product Media GraphQL API
    """

    def _get_product_id(self, ecommerce_id: str) -> str:
        return f"gid://shopify/Product/{ecommerce_id}"

    def create_product_media(
        self, ecommerce_id: str, input: List[ProductCreateMediaInput]
    ) -> ProducCreateMediaResponse:
        """
        Create media for a product in Shopify

        Args:
            ecommerce_id (str): Shopify Product id
            input (List[ProductCreateMediaInput]): List of media to create
        """
        gq_product_id = self._get_product_id(ecommerce_id)

        payload = {"id": gq_product_id, "media": [v.dict() for v in input]}
        logger.info(f"{payload}")
        response, err = self.secure_graphql_execute(CREATE_PRODUCT_MEDIA, payload)
        if err:
            logger.error(err.message, exception=err, stack_info=traceback.format_exc())
            raise err
        logger.debug(f"{response}")

        if not response:
            raise Exception("Error sending request to Shopify")

        if "data" not in response:
            logger.warn(f"{response}")
            raise Exception("Error sending request to Shopify")

        data_response = response["data"]["productCreateMedia"]

        media_response = ProducCreateMediaResponse(
            media=data_response["media"],
            mediaUserErrors=data_response["mediaUserErrors"],
            product=data_response["product"],
        )
        return media_response

    def delete_all_product_media(self, ecommerce_id: str) -> List[str]:
        """
        Delete all media from a product in Shopify

        Args:
            ecommerce_id (str): Shopify Product id
        """
        gq_product_id = self._get_product_id(ecommerce_id)
        graphql_client = shopify.GraphQL()
        payload = {"id": gq_product_id}
        response, err = self.secure_graphql_execute(GET_PRODUCT_MEDIA, payload)
        if err:
            logger.error(err.message, exception=err, stack_info=traceback.format_exc())
            raise err
        if not response:
            raise Exception("Error sending request to Shopify")

        logger.debug(f"{response}")
        data_response = response["data"]["product"]["media"]

        media_ids = []
        for media in data_response["edges"]:
            media_ids.append(media["node"]["id"])

        if len(media_ids) > 0:
            payload = {"productId": gq_product_id, "mediaIds": media_ids}
            response_json = graphql_client.execute(DELETE_PRODUCT_MEDIA, payload)
            response = json.loads(response_json)
            logger.debug(f"{response}")
            return response["data"]["productDeleteMedia"]["deletedMediaIds"]
        else:
            return []
