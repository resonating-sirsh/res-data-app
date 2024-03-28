GET_ASSET_URL = """
query getCreateAssets($id: uuid = "") {
  create_asset_by_pk(id: $id){
    id
    asset_type{
      name
    }
    style_id
    path
  }
}
"""

GET_IPFS_URL_METADATA = """
mutation createIpfsAssets(
  $style_code: String = ""
  $animation_path: String = ""
  $thumbnail_path: String = ""
) {
  createIpfsMetadata(
    input: {
      styleCode: $style_code
      render3DS3Url: $animation_path
      previewImageS3Url: $thumbnail_path
    }
  ) {
    metadataFile
  }
}

"""

UPDATE_DIGITAL_PRODUCT_REQUEST = """
mutation saveMoreDataDigitalProduct(
  $id: Int!
  $costing: json = ""
  $token_id: String = ""
  $contract_address: String = ""
  $ipfs_location: String = ""
  $status: digital_product_digital_product_request_statu_enum = done
  $transfered_at: timestamptz = ""
) {
  update_digital_product_digital_product_request_by_pk(
    pk_columns: { id: $id }
    _set: {
      costing: $costing
      ipfs_location: $ipfs_location
      status: $status
      transfered_at: $transfered_at
      token_id: $token_id
      contract_address: $contract_address
    }
  ) {
    id
    ipfs_location
    costing
    transfered_at
  }
}
"""

CHANGING_STATUS = """
mutation saveMoreDataDigitalProduct(
  $id: Int! 
  $status: digital_product_digital_product_request_statu_enum = done
  $reason: String
) {
  update_digital_product_digital_product_request_by_pk(
    pk_columns: { id: $id }
    _set: {
      status: $status
      reason_message: $reason
    }
  ) {
    id
    status
    ipfs_location
    costing
    transfered_at
  }
}
"""

SAVING_TRANSFER = """
mutation saveMoreDataDigitalProduct(
  $id: Int! 
  $status: digital_product_digital_product_request_statu_enum = done
  $costing: json = ""
) {
  update_digital_product_digital_product_request_by_pk(
    pk_columns: { id: $id }
    _set: {
      status: $status
      costing: $costing
    }
  ) {
    id
    status
    ipfs_location
    costing
    transfered_at
  }
}
"""

GET_DUPLICATE_REQUEST = """
query validate_digital_product(
  $brand_id: String = ""
  $animation_id: uuid = ""
  $status: [digital_product_digital_product_request_statu_enum!] = done
  $type: digital_product_digital_product_request_type_enum = created
) {
  digital_product_digital_product_request(
    where: {
      _and: {
        brand_id: { _eq: $brand_id }
        animation_id: { _eq: $animation_id }
        type: { _eq: $type }
        status: { _in: $status }
      }
    }
  ) {
    id
    brand_id
    status
    animation_id
  }
}
"""

GET_COUNT_DIGITAL_PRODUCT_CREATED = """
query get_count_of_digitla_product_request(
  $brand_id: String = ""
) {
  digital_product_digital_product_request(
    where: {
      _and: {
        brand_id: { _eq: $brand_id }
        type: { _eq: created }
        status: { _in: [done, processing, hold] }
      }
    }
  ) {
    id
  }
}
"""
