RELEASE_TO_SHOPIFY = """
mutation release($id: ID!, $instance_id: ID!){
  releaseProductShopByColor(id: $id, shopByColorId: $instance_id){
    product{
      id
      url
      previewLink
    }
  }
}
"""

UPDATE_PRODUCT_ECOMMERCE_SBC = """
mutation update($id: ID!){
  updateProductShopByColor(id: $id){
    product{
      id
    }
  }
}
"""

REMOVE_FROM_SHOPIFY = """
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

UPDATE_CAMPAIGN = """
mutation updateCampaignShopByOptionStatus($campaign_id: ID!, $input: UpdateCampaignInput!) {
  updateCampaign(id: $campaign_id, input: $input) {
    campaign {
      id
      shopByOptionStatus
    }
  }
}
"""


UPDATE_SHOP_BY_COLOR = """
mutation update($id: ID!, $input: UpdateShopByColorInput){
  updateShopByColorInstance(id: $id input: $input){
    shopByColorInstance{
      id
    }
  }
}
"""

SHOP_BY_COLOR_BY_ID = """
query getShopByColorById($id: ID!){
  shopByColorInstance(id: $id){
    id
    bodies{
        id
        code
        name
    }
    options{
      styleId
    }
    materials{
        id
        code
        name
    }
    storeBrand{
        id
        code
        name
    }
    products{
      id
    }
  }
}
"""

SHOP_BY_COLOR_CURSOR = """
query getShopByColor($first: Int! $where: ShopByColorWhere){
  shopByColors(first: $first, where: $where){
    shopByColors{
      id
    }
  }
}
"""

GET_PRODUCTS = """
query getAllProducts($first: Int! $after: String $where: ProductsWhere){
  products(first: $first after: $after where: $where){
    products{
      id
      style{
        id
        color{
          id
          images{
             fileName
          }
        }
      }
      storeCode
      ecommerceId
      productHandle
    }
    hasMore
    cursor
    count
  }
}
"""

UPDATE_PRODUCT = """
mutation updateProduct($product_id: ID!, $input: UpdateProductInput){
  updateProduct(id: $product_id, input:$input){
    product{
      id
      style{
          id
      }
      ecommerceId
      createdAt
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

GET_SHOP_BY_COLOR_INSTANCE = """
query getShopByColor($id: ID!) {
  shopByColorInstance(id: $id) {
    id
    title
    materials{
        id
        code
        name
    }
    bodies{
        id
        code
        name
    }
    storeBrand {
      id
      code
    }
    options{
      id
      styleId
      colorName
      imageUrl
      referenceDate
    }
  }
}
"""

GET_SBC_INSTANCES = """
query getSBCInstances($first: Int!, $where: ShopByColorWhere) {
  shopByColorInstances(first: $first, where: $where) {
    shopByColorInstances {
      id
      products {
        id
      }
    }
  }
}
"""
