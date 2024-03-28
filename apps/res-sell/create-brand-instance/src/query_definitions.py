CREATE_BRAND = """
mutation CreateBrand($input: CreateBrandInput!) {
  createBrand(input: $input) {
    brand {
      id
      code
    }
  }
}
"""

CREATE_USER = """
mutation addUser ($email: String!, $input: AddUserToBrandInput) {
  __typename
  addUserToBrand(email: $email, input: $input) {
    user {
      id
    }
  }
}
"""

CREATE_SHIPPING_ADDRESS = """
mutation shippingAddress($input: CreateShippingAddressInput!) {
  __typename
  createShippingAddress(input: $input){
    shippingAddress{
      id
    }
  }
}
"""

CREATE_DATASOURCE_INSTANCE = """
mutation shippingAddress($input: CreateDataSourceInstanceInput!) {
  __typename
  createDataSourceInstance(input:$input){
    dataSourceInstance {
      id
    }
  }
}
"""

CREATE_STYLE = """
mutation createStyle($input: CreateStyleMutation!){ 
  createStyle(input: $input){
    style{
      id
    }
  }
}
"""

CREATE_COMPOSITION = """
mutation Style($input: CreateCompositionInput!) {
  createComposition(input: $input) {
    composition {
      id
      entityId
    }
  }
}
"""

UPDATE_BRAND = """
mutation updateBrand($id: ID! $input: UpdateBrandInput!){
  updateBrand(id: $id input: $input){
    brand{
      id
      code
    }
  }
}
"""

UPDATE_BRAND_LABEL = """
mutation UpdateBrandLabel($code: String!, $input: UpdateMetaOneInput!) {
  updateMetaOneBrand(code: $code, input: $input) {
    brand {
      id
      sizeLabel
      mainLabelType
      isMainLabelWithSize
    }
  }
}
"""

UPDATE_CREATE_BRAND_INSTANCE = """
mutation updateCreateBrand($status: String = "", $id: uuid!) {
  update_sell_create_brand_instance_by_pk(_set: {status: $status}, pk_columns: {id: $id}) {
    id
    created_at
    input
    status
    updated_at
  }
}
"""

CHANGE_STEP_BRAND_INSTANCE = """
mutation updateCreateBrand($step: String = "", $id: uuid!) {
  update_sell_create_brand_instance_by_pk(_set: {step: $step}, pk_columns: {id: $id}) {
    id
    created_at
    input
    status
    updated_at
  }
}
"""

UPDATE_BRAND_INSTANCE = """
mutation updateCreateBrand($brand_id: String = "", $brand_code: String = "", $id: uuid!) {
  update_sell_create_brand_instance_by_pk(_set: {brand_id: $brand_id, brand_code: $brand_code}, pk_columns: {id: $id}) {
    id
    created_at
    input
    status
    updated_at
    brand_id
    brand_code
  }
}
"""

SYNC_BRAND_WITH_META = """
mutation syncBrand($id: ID!){
  syncBrandsWithMeta(id: $id){
    brand{
      id
    }
  }
}"""
