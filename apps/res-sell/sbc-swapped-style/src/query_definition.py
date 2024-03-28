GET_STYLE_ID = """
query getStyle($id: ID!) {
  style(id: $id) {
    id
    name
    body {
      id
      name
      code
    }
    material {
      name
      code
    }
    color {
      id
      code
      name
      images{
         fullThumbnail
      }
    }
    allProducts {
      id
      isLive
    }
  }
}
"""

RELEASE_SBC = """
mutation invokeReleaseSBC($id: ID!){
  releaseShopByColorInstanceToEcommerce(id: $id){
    shopByColorInstance{
      id
    }
  }
}
"""
