GET_SBC_ID = """
query getShopByColor($id: ID!) {
  shopByColorInstance(id: $id) {
    id
    products {
      id
    }
    configurations {
      id
      name
      groups {
        id
        title
        options {
          id
          colorName
          styleId
          handle
          price
          imageUrl
          images
          referenceDate
        }
      }
    }
  }
}
"""

RELEASE_SBC = """
mutation releaseSBC($id: ID!, $sbc_id: ID!){
  releaseProductShopByColor(id: $id, shopByColorId: $sbc_id){
    product{
      id
    }
  }
}
"""

UPDATE_SBC = """
mutation updateSBC($id: ID!, $input: UpdateShopByColorInput){
  updateShopByColorInstance(id: $id, input: $input){
    shopByColorInstance{
      id
    }
  }
}
"""
