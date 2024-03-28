GET_PRODUCT = """
query getProductById($id: ID!){
  product(id: $id){
    id
    name
    storeCode
    styleId
    ecommerceId
    productHandle
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
