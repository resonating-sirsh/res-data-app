import typing

from pydantic import BaseModel


class PublicationInput(BaseModel):
    """
    Represent the data model expected by shopify to call `publishPublishable` mutation in the GraphQL API
    to publish a publishable resource in the shopify store.
    [See more] https://shopify.dev/docs/api/admin-graphql/2023-01/mutations/publishablePublish

    On the current time only Products and Collection are the only one able to be publish on a channel.
    """

    publicationId: str
    """
    Id that links to the Channel/Publication that the desired resoucers wants to be publish
    """

    publishDate: typing.Optional[str]
    """
    Future date where the resource is going to be publish on the channel.
    Only Online Store has enable future publication.
    """


class PublishResourceInput(BaseModel):
    """
    Payload send to /publication POST to publish a resource to Shopify
    """

    resourceId: str
    resourceType: typing.Literal["product", "collection"]
    publications: typing.List[PublicationInput]
