GET_PRODUCT = """
query getProduct($id: ID!){
  product(id: $id){
    id
    name
    storeCode
    styleId
    ecommerceId
    productHandle
    isLive
    photos{
      id
      key
      bucket
      name
      size
      type
    }
  }
}
"""

LIST_PUBLICATIONS = """
query listPublications {
  publications(first: 10) {
    edges{
      node{
        id
        name
      }
    }
  }
}
"""

GET_PRODUCT_ONLINE_STORE = """
query productIsLiveOnPublicationChannel($id: ID!, $publication_id:ID!){
  product(id: $id){
    id
    publishedOnPublication(publicationId: $publication_id)
  }
}
"""

GET_SINGLE_ORDER_NY_NAME = """{
  orders(first: 1, query:"name:#66508") {
    edges {
      node {
        id
        name
        createdAt
        email
        displayFulfillmentStatus
      }
    }
  }
}"""
