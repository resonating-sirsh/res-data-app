PUBLISH_RESOURCE_ON_PUBLICATION = """
mutation publishResourceOnChannel($resourceId: ID!, $input: [PublicationInput!]!) {
    publishablePublish( id: $resourceId, input: $input){
        publishable{
            publicationCount
            ... on Product {
                id
                title
                description
            }
            ... on Collection {
                id
                title
                description
            }
        }
    }

}
"""

UNPUBLISH_RESOURCE_ON_PUBLICATION = """
mutation publishResourceOnChannel($resourceId: ID!, $input: [PublicationInput!]!) {
    publishableUnpublish( id: $resourceId, input: $input){
        publishable{
            publicationCount
            ... on Product {
                id
                title
                description
            }
            ... on Collection {
                id
                title
                description
            }
        }
    }

}
"""
