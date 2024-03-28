GET_ECOMMERCE_COLLECTION_BY_ID = """
query getEcommerceCollection ($id: uuid) {
  sell_ecommerce_collections(where: {id: {_eq: $id}}) {
    body_html
    created_at
    ecommerce_id
    id
    image_id
    published_at
    sort_order_fk
    store_code
    title
    type_collection
    handle
    updated_at
    publication_metadata
    status
    rules
    disjuntive
  }
}
"""

UPDATE_ECOMMERCE_COLLECTION = """
mutation updateEcommerceCollection($input: sell_ecommerce_collections_set_input = {}, $id: uuid = "") {
  update_sell_ecommerce_collections_by_pk(pk_columns: {id: $id}, _set: $input) {
    body_html
    created_at
    ecommerce_id
    id
    image_id
    published_at
    sort_order_fk
    store_code
    title
    type_collection
    updated_at
    handle
    publication_metadata
    rules
    disjuntive
  }
}
"""

CREATE_ECOMMERCE_COLLECTION = """
mutation createEcommerceCollection($input: sell_ecommerce_collections_insert_input = {}) {
  insert_sell_ecommerce_collections_one(object: $input) {
    body_html
    created_at
    ecommerce_id
    id
    image_id
    published_at
    sort_order_fk
    store_code
    title
    type_collection
    updated_at
    rules
    disjuntive
  }
}
"""

DELETE_ECOMMERCE_COLLECTION = """
mutation deleteEcommerceCollection($id: uuid = "") {
  delete_sell_ecommerce_collections_by_pk(id: $id) {
    id
  }
}
"""
