class ShopifyQuery:
    SET_METAFIELD = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields {
      id
    }
    userErrors {
      code
      field
      message
    }
  }
}
    """

    GET_METAFIELD = """
query getMetafield($product_id: ID!, $namespace: String!, $key: String!) {
  product(id: $product_id) {
    title
    metafield(namespace: $namespace, key: $key) {
      id
      legacyResourceId
      namespace
      key
      description
      value
    }
  }
}
    """

    DELETE_METAFIELD = """
mutation metafieldDelete($input: MetafieldDeleteInput!) {
  metafieldDelete(input: $input) {
    deletedId
    userErrors {
      field
      message
    }
  }
}

    """

    GET_PRODUCT = """
query getProduct($id: ID!){
  product(id: $id){
    id
    title
    publishedOnCurrentPublication
    status
    createdAt
    handle
    seo{
      description
      title
    }
    description
    descriptionHtml
    featuredImage{
      id
    }
  }
}
    """
