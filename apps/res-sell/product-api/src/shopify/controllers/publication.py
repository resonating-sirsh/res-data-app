import typing

from pymongo import MongoClient
from src.shopify.models.publication import PublicationInput
from src.shopify.queries.publication import (
    PUBLISH_RESOURCE_ON_PUBLICATION,
    UNPUBLISH_RESOURCE_ON_PUBLICATION,
)

from res.connectors.mongo import MongoConnector
from res.connectors.shopify.base.ShopifyBase import ShopifyClientBase
from res.connectors.shopify.utils.exceptions import ShopifyException
from res.utils import logger


class ShopifyPublication(ShopifyClientBase):
    """
    This controller class implements all the associated method to interact with
    the Shopify Publication API.
    [See more] https://shopify.dev/docs/api/admin-graphql/2023-01
    """

    from res.connectors import load

    # load mongodb credentials from ssm
    load("mongo")

    mongo: MongoClient

    def __enter__(self):
        self.mongo = MongoConnector().get_client()
        return super().__enter__()

    def __exit__(self, *args, **kwargs):
        self.mongo.close()
        return super().__exit__(*args, **kwargs)

    def publish_publishable_resource(
        self, id: str, input: typing.List[PublicationInput]
    ) -> typing.Tuple[typing.Any, ShopifyException]:
        """
        Publish a publishabel resource (Product/Collection) on a Shopify Channel/Publication
        -- params --
        id: str -> ID of the resource that is going to be publish. Note: should graphql id format
        input: List[PublishPublishableInput] -> A List of object that represents all the
        channels/publication where the resources it's going to be publish.
        """
        logger.info(f"Publishing in shopify resource {id}...")
        response_dict, error = self.secure_graphql_execute(
            PUBLISH_RESOURCE_ON_PUBLICATION,
            {"resourceId": id, "input": [value.dict() for value in input]},
        )

        if error:
            return None, error

        logger.info(
            f"Published in shopify resource {id} in {','.join([publish.publicationId for publish in input])}"
        )

        publishable_response = response_dict["data"]["publishablePublish"][
            "publishable"
        ]

        return publishable_response, None

    def unpublish_publishable_resource(
        self, id: str, input: typing.List[PublicationInput]
    ) -> typing.Tuple[typing.Any, ShopifyException]:
        """
        Unpublish a publishabel resource (Product/Collection) on a Shopify Channel/Publication
        -- params --
        id: str -> ID of the resource that is going to be publish. Note: should graphql id format
        input: List[PublishPublishableInput] -> A List of object that represents all the
        channels/publication where the resources it's going to be publish.
        """
        logger.info(f"Unpublishing in shopify resource {id}...")
        response_dict, error = self.secure_graphql_execute(
            UNPUBLISH_RESOURCE_ON_PUBLICATION,
            {"resourceId": id, "input": [value.dict() for value in input]},
        )

        if error:
            return None, error

        logger.info(
            f"Unpublished in shopify resource {id} in {','.join([publish.publicationId for publish in input])}"
        )

        publishable_response = response_dict["data"]["publishableUnpublish"][
            "publishable"
        ]
        return publishable_response, None
