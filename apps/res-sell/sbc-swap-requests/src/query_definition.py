GET_SWAP_MATERIAL = """
query GetMaterialSwapRequest($id: ID!) {
  materialSwapRequest(id: $id) {
    id
    fromMaterial
    toMaterial
    status
    stylesIds
  }
}
"""

GET_STYLES = """
query getStylesByIds($ids: [WhereValue], $after: String) {
  styles(first: 25, where: { id: { isAnyOf: $ids } }, after: $after) {
    styles {
      id
      brand {
        id
        code
      }
      body {
        id
        code
        name
      }
      material {
        id
        code
        name
      }
      allProducts {
        id
        storeCode
        styleId
        shopByColor {
          id
        }
      }
    }
    hasMore
    cursor
  }
}
"""


GET_LIST_SBC = """
query getSBC($where: ShopByColorWhere){
  shopByColorInstances(first: 25, where: $where){
    shopByColorInstances{
      id
      bodies{
        id
        code
        name
      }
      materials{
        id
        code
        name
      }
      storeBrand{
        id
        code
      }
      options{
        styleId
      }
    }
  }
}
"""
