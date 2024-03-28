UPDATE_SBC_BY_ID = """
mutation updateShopByColor($id: ID! $input: UpdateShopByColorInput){
  updateShopByColorInstance(id: $id input: $input){
    shopByColorInstance{
      id
    }
  }
}
"""

GET_SBC_BY_ID = """
query getShopByColor($id: ID!) {
  shopByColorInstance(id: $id) {
    id
    products{
      id
    }
    options{
      id
      styleId
      colorName
      imageUrl
      referenceDate
      images
      handle
      price
    }
  }
}
"""

UPDATE_PRODUCT = """
mutation update($id: ID!, $input: UpdateProductInput!) {
    updateProduct(id: $id, input: $input) {
        product {
            id
        }
    }
}
"""

GET_SBC_BASE_ON_BODY_MATERIAL_AND_STORE = """
query get($bodyCode: WhereValue!, $materialCode: WhereValue!, $storeCode: WhereValue!){
    shopByColorInstances(first: 1 where:{
      bodyCodes:{
        isAnyOf: [$bodyCode]
      }
      materialCodes: {
        isAnyOf: [$materialCode]
      }
      live: {
        is: true
      }
      storeCode: {
        is: $storeCode
      }
    }){
    shopByColorInstances{
      id
      products{
        id
      }
      options{
        id
        styleId
        colorName
        imageUrl
        referenceDate
        images
        handle
        price
      }
    }
    count
  }
}
"""

GET_PRODUCT = """
query get($id: ID!) {
    product(id: $id) {
        id
        storeCode
        ecommerceId
        style{
            id
            body{
                id
                code
                name
            }
            material{
                id
                code
                name
            }
            color{
                id
                name
                images{
                    id
                    smallThumbnail
                }
            }
            createdAt
        }
        shopByColor{
            id
            options{
                id
                styleId
                colorName
                imageUrl
                referenceDate
                images
                handle
                price
            }
        }
    }
}
"""

DISABLE_SHOP_BY_COLOR = """
mutation disable($id: ID!){
    disableProductShopByColor(id: $id){
        product{
            id
            url
            previewLink
        }
    }
}
"""

UPDATE_SBC = """
mutation updateSBC($id: ID!){
  updateShopByColorInstanceInEcommerce(id: $id) {
    shopByColorInstance{
      id
    }
  }
}
"""

RELEASE_SBC = """
mutation releaseSBC($id: ID!){
  releaseShopByColorInstanceToEcommerce(id: $id){
    shopByColorInstance{
      id
    }
  }
}
"""

MAKE_FILE_PUBLIC = """
mutation makeFilePublic($id: ID!){
  makeFilePublic(id: $id){
    file{
      id
      thumbnail(size: 128){
        url
      }
    }
  }
}
"""
