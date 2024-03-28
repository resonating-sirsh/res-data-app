"""
For help on mutations see
- 

For help on queries see
- [Search](https://hasura.io/docs/latest/queries/postgres/query-filters/)

TODO:
wrappers should have metrics and error handling added
"""

import res
from res.utils import uuid_str_from_dict
from res.connectors.graphql import hasura
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import uuid_str_from_dict
import pandas as pd
from res.media.images.geometry import from_geojson
from tenacity import retry, wait_fixed, stop_after_attempt
from dateparser import parse


def get_transport_error_messages(tex):
    return [t["extensions"]["internal"]["error"]["message"] for t in tex.errors]


# default would be 10 but we can wait a little longer for batch updates with big payloads
HASURA_TIMEOUT = 30

"""
A batch of mutations to insert a style and manage piece history
for the upserts there are two types. a bulk one and one that separates the pieces management from the style headers
"""

UPSERT_SIZES = """mutation upsert_sizes($sizes: [meta_sizes_insert_input!] = {}) {
  insert_meta_sizes(objects: $sizes, on_conflict: {constraint: sizes_pkey, update_columns: [size_chart,size_normalized]}) {
    returning {
      id
      key
    }
  }
}
"""

UPSERT_MATERIALS = """mutation upset_materials($materials: [meta_materials_insert_input!] = {}) {
  insert_meta_materials(objects: $materials, on_conflict: {constraint: materials_pkey,
                                                         update_columns: [material_taxonomy,compensation_length,compensation_width,paper_marker_compensation_length,paper_marker_compensation_width,fabric_type,material_stability,pretreatment_type,cuttable_width_inches,offset_size_inches]}) {
    returning {
      id
      key
    }
  }
}
"""

# when we save costs we also save any attributes that are used for costing purposes
UPSERT_STYLE_COSTS = """mutation update_style_size_costs($costs: jsonb = "", $material_usage_statistics: jsonb = "", $id: uuid = "", $cost_at_onboarding: numeric = 0, $size_yield: numeric = 0, $piece_count_checksum: Int=0) {
  update_meta_style_sizes(where:  {id: {_eq: $id}},  _set: {costs: $costs, material_usage_statistics: $material_usage_statistics, cost_at_onboarding: $cost_at_onboarding, size_yield: $size_yield, piece_count_checksum: $piece_count_checksum})  {
    returning {
      id
      costs
      cost_at_onboarding
    }
  }
}
"""

UPSERT_STYLE_STATUS_BY_SKU = """mutation update_style_status($contracts_failing: jsonb = "", $status: String = "", $metadata_sku: jsonb = "") {
  update_meta_styles(where:   {metadata: {_contains: $metadata_sku}},  _set: {status: $status, contracts_failing: $contracts_failing})  {
    returning {
      id
      contracts_failing  
    }
  }
}
"""

UPSERT_STYLE_CHECKS_BY_SKU = """mutation update_style_status(   $metadata_sku: jsonb = "", $piece_version_delta_test_results: jsonb = "") {
  update_meta_styles(where:   {metadata: {_contains: $metadata_sku}},  _set: {  piece_version_delta_test_results: $piece_version_delta_test_results})  {
    returning {
      id
      contracts_failing  
      piece_version_delta_test_results
      metadata
    }
  }
}
"""


ADD_STYLE_3D_MODEL_URIS = """
  mutation update_style_metadata(
    $metadata: jsonb = "", 
    $id: uuid = "", 
    $model_3d_uri: String = "", 
    $point_cloud_uri: String = "",
    $front_image_uri: String = ""
  ) {
    update_meta_styles(
      where:  {id: {_eq: $id}},  
      _append: {metadata: $metadata}, 
      _set: {model_3d_uri: $model_3d_uri, point_cloud_uri: $point_cloud_uri, front_image_uri: $front_image_uri}
    )  {
      returning {
        id
        metadata
      }
    }
  }
"""

# the batch adds pieces and also history updates.... the setting the style to active is useful for checking style health but may change
# if we can save it successful
BATCH_INSERT_GARMENT_PIECES = """mutation upsert_pieces(
    $pieces: [meta_pieces_insert_input!] = {},  $garment_id: uuid = "",   $history: [meta_style_size_pieces_insert_input!] = {},
       $ended_at: timestamp = null) {
      insert_meta_pieces(objects: $pieces, on_conflict: {constraint: pieces_pkey, update_columns: [normed_size, piece_set_size, piece_ordinal, artwork_uri, base_image_uri,base_image_outline, color_code, material_code, metadata]}) {
        returning {
          id
        }
      }

    update_meta_style_size_pieces(where: {style_size_id: {_eq: $garment_id}}, _set: {ended_at: $ended_at}) {
      returning {
        id
      }
    }
    insert_meta_style_size_pieces(objects: $history, on_conflict: {constraint: style_size_pieces_pkey, update_columns: ended_at}) {
      returning {
        id
      }
    }

   update_meta_style_sizes(where: {id: {_eq: $garment_id}}, _set: {status: "Active"}){
    returning {
        id
      }
   }
  
}
"""

BATCH_INSERT_GARMENT_NO_PIECES = """mutation upsert_style_header_and_history($style: meta_styles_insert_input = {}, 
    $style_sizes: [meta_style_sizes_insert_input!] = {}) {
  insert_meta_styles_one(object: $style, on_conflict: {constraint: styles_pkey, update_columns: [metadata, body_code, brand_code, name, status, labels, style_birthday, contracts_failing, modified_at]}) {
    id
    name
    metadata
  }
  insert_meta_style_sizes(objects: $style_sizes, on_conflict: {constraint: style_sizes_pkey, update_columns: [price, metadata, labels]}) {
    returning {
      id
      metadata
      style {
        id
        name
      }
    }
  }
}
"""

BATCH_INSERT_GARMENT = """mutation upsert_style($style: meta_styles_insert_input = {}, 
    $style_sizes: [meta_style_sizes_insert_input!] = {}, 
    $pieces: [meta_pieces_insert_input!] = {}, 
    $garment_id: uuid = "", 
    $history: [meta_style_size_pieces_insert_input!] = {}, 
$ended_at: timestamp = null) {
  insert_meta_styles_one(object: $style, on_conflict: {constraint: styles_pkey, update_columns: [metadata, body_code, status, name, labels, style_birthday]}) {
    id
    name
  }
  insert_meta_style_sizes(objects: $style_sizes, on_conflict: {constraint: style_sizes_pkey, update_columns: [price, metadata, labels, status]}) {
    returning {
      id
      style {
        id
        name
      }
    }
  }
  insert_meta_pieces(objects: $pieces, on_conflict: {constraint: pieces_pkey, update_columns: [artwork_uri]}) {
    returning {
      id
      code
    }
  }
  update_meta_style_size_pieces(where: {style_size_id: {_eq: $garment_id}}, _set: {ended_at: $ended_at}) {
    returning {
      id
    }
  }
  insert_meta_style_size_pieces(objects: $history, on_conflict: {constraint: style_size_pieces_pkey, update_columns: ended_at}) {
    returning {
      id
    }
  }
}
"""

BATCH_INSERT_GARMENT_PENDING = """mutation MyMutation($style: meta_styles_insert_input = {}, 
    $style_sizes: [meta_style_sizes_insert_input!] = {}, 
    $pieces: [meta_pieces_insert_input!] = {}) {
  insert_meta_styles_one(object: $style, on_conflict: {constraint: styles_pkey}) {
    id
    name
  }
  insert_meta_style_sizes(objects: $style_sizes, on_conflict: {constraint: style_sizes_pkey, update_columns: price}) {
    returning {
      id
      style {
        id
        name
      }
    }
  }
}
"""

#####


"""
The tree from style->style_sizes->style_size_pieces_history(where end date is null)->pieces
This is a given style broken out by size along with their active pieces
"""
GET_STYLE_BY_NAME_AND_SIZE = """query get_style_by_name_and_size($name: String = "", $size_code: String = "") {
  meta_styles(where: {name: {_eq: $name} }) {
      name
      body_code
      id
      meta_one_uri
      metadata
      style_sizes(where: {size_code: {_eq: $size_code} }) {
        id
        size_code
        status
        metadata
        style_size_pieces(where: {ended_at: {_is_null: true}}) {
          piece {
            color_code
            material_code
            artwork_uri
            metadata
            base_image_uri
            body_piece {
              id
              key
              outer_geojson
              inner_geojson
              placeholders_geojson
              outer_notches_geojson
              outer_corners_geojson 
              seam_guides_geojson 
              sew_identity_symbol
              internal_lines_geojson
            }
          }
       }
     }
  }
}
"""

GET_STYLE_BY_STYLE_SKU_ALL_SIZE = """query get_style_by_sku_and_size($metadata_sku: jsonb = "" ) {
  meta_styles(where:  {metadata: {_contains: $metadata_sku}}) {
      name
      body_code
      id
      meta_one_uri
      metadata
      contracts_failing
      modified_at
      created_at
      rank
      pieces_hash_registry {
        id
        rank
      }
      style_sizes  {
        id
        size_code
        status
        metadata
        style_size_pieces(where: {ended_at: {_is_null: true}}) {
          piece {
            id
            color_code
            material_code
            artwork_uri
            metadata
            base_image_uri
            piece_set_size
            piece_ordinal
            normed_size
            body_piece   {
              id
              body_id
              key
              type
              piece_key
              outer_geojson
              inner_geojson
              placeholders_geojson
              outer_notches_geojson
              outer_corners_geojson 
              seam_guides_geojson 
              sew_identity_symbol
              internal_lines_geojson
              buttons_geojson
              pleats_geojson
              pins_geojson  
              deleted_at
              created_at
            }
          }
       }
     }
  }
}
"""

GET_STYLE_BY_STYLE_SKU_AND_SIZE_AND_BODY_VERSION = """query get_style_by_sku_and_size($metadata_sku: jsonb = "", $size_code: String = "", $body_version: numeric = "") {
  meta_styles(where: {metadata: {_contains: $metadata_sku}}) {
    name
    body_code
    id
    meta_one_uri
    metadata
    contracts_failing
    modified_at
    created_at
    rank
    pieces_hash_registry {
      id
      rank
    }
    style_sizes(where: {size_code: {_eq: $size_code}}) {
      id
      size_code
      status
      metadata
      style_size_pieces(where: {piece: {body_piece: {body: {version: {_eq: $body_version}}}}}) {
        piece {
          id
          color_code
          material_code
          artwork_uri
          metadata
          base_image_uri
          piece_set_size
          piece_ordinal
          normed_size
          body_piece {
            id
            body_id
            key
            type
            piece_key
            outer_geojson
            inner_geojson
            placeholders_geojson
            outer_notches_geojson
            outer_corners_geojson
            seam_guides_geojson
            sew_identity_symbol
            internal_lines_geojson
            buttons_geojson
            pleats_geojson
            pins_geojson
            deleted_at
            created_at
          }
        }
      }
    }
  }
}

"""


GET_STYLE_BY_STYLE_SKU_AND_SIZE = """query get_style_by_sku_and_size($metadata_sku: jsonb = "", $size_code: String = "") {
  meta_styles(where:  {metadata: {_contains: $metadata_sku}}) {
      name
      body_code
      id
      meta_one_uri
      metadata
      contracts_failing
      modified_at
      created_at
      rank
      pieces_hash_registry {
        id
        rank
      }
      style_sizes(where: {size_code: {_eq: $size_code} }) {
        id
        size_code
        status
        metadata
        style_size_pieces(where: {ended_at: {_is_null: true}}) {
          piece {
            id
            color_code
            material_code
            artwork_uri
            metadata
            base_image_uri
            piece_set_size
            piece_ordinal
            normed_size
            body_piece   {
              id
              body_id
              key
              type
              piece_key
              outer_geojson
              inner_geojson
              placeholders_geojson
              outer_notches_geojson
              outer_corners_geojson 
              seam_guides_geojson 
              sew_identity_symbol
              internal_lines_geojson
              buttons_geojson
              pleats_geojson
              pins_geojson  
              deleted_at
              created_at
            }
          }
       }
     }
  }
}
"""


GET_STYLE_SIZE_KEYS_BY_SKU = """query get_style_size_keys_by_sku($metadata_sku: jsonb = "") {
  meta_style_sizes(where: {metadata: {_contains: $metadata_sku}}) {
    metadata
    id
    style {
      id
      contracts_failing
    }
    style_size_pieces(where: {ended_at: {_is_null: true}}) {
      piece {
        id
        body_piece {
          id
          body{
            id
            version
          }
          piece_key
        }
      }
    }
  }
}
"""

GET_STYLE_SIZE_KEYS_BY_ID = """query get_style_sizes_by_sku($id: uuid = "") {
  meta_style_sizes(where: {id: {_eq: $id}}) {
    metadata
    id
    style {
      id
    }
    style_size_pieces(where: {ended_at: {_is_null: true}}) {
      piece {
        id
        body_piece {
          id
          key
          piece_key
        }
      }
    }
  }
}
"""

GET_STYLE_SIZE_HEADER_BY_ID = """query get_style_sizes_by_sku($id: uuid = "") {
  meta_style_sizes(where: {id: {_eq: $id}}) {
    metadata
    id
    price
    size_code
    status
    created_at
    labels
    style {
      name
      status
      contracts_failing
      body_code
      rank
      id
      metadata
      pieces_hash_registry {
        id
        rank
      }
    }
  }
  """

GET_STYLE_SIZE_BY_ID = """query get_style_sizes_by_sku($id: uuid = "") {
  meta_style_sizes(where: {id: {_eq: $id}}) {
    metadata
    id
    price
    size_code
    status
    created_at
    labels
    style {
      name
      status
      contracts_failing
      body_code
      rank
      id
      metadata
      pieces_hash_registry {
        id
        rank
      }
    }
    style_size_pieces(where: {ended_at: {_is_null: true}}) {
      piece {
        id
        color_code
        material_code
        artwork_uri
        metadata
        base_image_uri
        body_piece {
          id
          body_id
          key
          type
          piece_key
          outer_geojson
          placeholders_geojson
          outer_notches_geojson
          internal_lines_geojson
        }
      }
    }
  }
}

"""

GET_STYLE_SIZE_HEADER_BY_SKU = """query get_style_by_sku_and_size($metadata_sku: jsonb = "", $size_code: String = "") {
  meta_styles(where:  {metadata: {_contains: $metadata_sku}}) {
      name
      body_code
      id
      metadata
      contracts_failing
      rank
      modified_at
       pieces_hash_registry {
          id
          rank
      }
      style_sizes(where: {size_code: {_eq: $size_code} }) {
        id
        size_code
        costs
        material_usage_statistics
        status
        metadata
       
     }
  }
}
"""


GET_STYLE_SIZE_BY_SKU = """query get_style_sizes_by_sku($metadata_sku: jsonb = "") {
  meta_style_sizes(where: {metadata: {_contains: $metadata_sku}}) {
    metadata
    id
    price
    size_code
    status
    costs
    created_at
    updated_at
    labels
    style {
      name
      status
      contracts_failing
      body_code
      rank
      id
      metadata
      pieces_hash_registry {
        id
        rank
      }
    }
    style_size_pieces(where: {ended_at: {_is_null: true}}) {
      piece {
        color_code
        material_code
        artwork_uri
        metadata
        base_image_uri
        body_piece {
          id
          body_id
          key
          type
          piece_key
          outer_geojson
          placeholders_geojson
          outer_notches_geojson
          sew_identity_symbol
          buttons_geojson
          pleats_geojson
          pins_geojson
        }
      }
    }
  }
}

"""


GET_STYLE_HEADERS_BY_SKU = """query get_style_sizes_by_sku($metadata_sku: jsonb = "") {
  meta_style_sizes(where:  {style:{metadata: {_contains: $metadata_sku}}}) {
    metadata
    id
    price
    size_code
    status
    created_at
    updated_at
    cost_at_onboarding
    costs
    labels
    style {
      name
      rank
      status
      contracts_failing
      body_code
      id
      meta_one_uri
      metadata
      pieces_hash_registry {
        id
        rank
      }
    }
  }
}

"""

GET_STYLE_HEADERS_WITH_SIZES_BY_SKU = """query get_style_sizes_by_sku($metadata_sku: jsonb = "") {
    meta_styles (where: {metadata: {_contains: $metadata_sku}}){
      name
      rank
      status
      modified_at
      contracts_failing
      body_code
      id
      meta_one_uri
      metadata
      pieces_hash_registry {
        id
        rank
      }
      style_sizes{
        metadata
        id
        price
        size_code
        status
        created_at
        updated_at
        cost_at_onboarding
        labels
      }
    }
}

"""

GET_STYLE_HEADERS_BY_ID_AND_SIZE = """query get_style_sizes_by_sku($style_id: uuid = "",  $size_code: String = "") {
  meta_style_sizes(where: {size_code: {_eq: $size_code}, style_id:{_eq: $style_id}}) {
    metadata
    id
    price
    size_code
    status
    created_at
    cost_at_onboarding
    labels
    style  {
      contracts_failing
      name
      rank
      status
      body_code
      id
      meta_one_uri
      metadata
    }
  }
}

"""


GET_ALL_STYLE_HEADERS = """query get_style_sizes_by_sku {
  meta_style_sizes  {
    metadata
    id
    price
    size_code
    status
    created_at
    cost_at_onboarding
    labels
    style {
      name
      status
      body_code
      id
      meta_one_uri
      metadata
    }
  }
}

"""

# NOTE THIS IS A JOB THING - we get stuff that is missing size yield only
GET_ALL_STYLE_HEADERS_WITHOUT_COSTS = """query get_style_sizes_by_sku {
   meta_style_sizes(where: {status: {_in: ["Active", "ACTIVE"]}, size_yield: {_is_null: true}}) {
    metadata
    id
    price
    size_code
    status
    created_at
    cost_at_onboarding
    costs
    material_usage_statistics
    size_yield
    labels
  }
}

"""

# NOTE THIS IS A JOB THING - we get stuff that is missing size yield only
GET_ALL_STYLE_HEADERS_WITH_COSTS = """query get_style_sizes_by_sku {
   meta_style_sizes(where: {status: {_in: ["Active", "ACTIVE"]}, size_yield: {_is_null: false}}) {
    metadata
    id
    price
    size_code
    status
    created_at
    cost_at_onboarding
    costs
    material_usage_statistics
    size_yield
    labels
  }
}

"""

# below is an interesting or stupid pattern, the constraint is on the key and not the id
# this is because we only want there to be a single thing per key but the logic could change the id which is generated from it
# so we must add the id to the update columns
UPDATE_BODY = """
mutation update_body($body: meta_bodies_insert_input = {}, $body_pieces: [meta_body_pieces_insert_input!] = {}) {
  insert_meta_bodies_one(object: $body, on_conflict: {constraint: bodies_pkey, update_columns: brand_code}) {
    id
    body_code
    created_at
  }
  
  insert_meta_body_pieces(objects: $body_pieces, on_conflict: {constraint: body_pieces_pkey, update_columns: [key, piece_key, type, size_code, outer_edges_geojson, outer_notches_geojson, metadata]}) {
    returning {
      id
      key
    }
  }
  
}


"""


GET_BODY_BY_VERSION_CODE = """query get_body($body_code: String = {},$version: numeric = {}) {
  meta_body_pieces(where: {body: {body_code: {_eq: $body_code}, version: {_eq: $version}}}) {
    body {
      body_code
      id
      metadata
      version
      contracts_failing
    }
    size_code
    outer_geojson
    inner_geojson
    metadata
    key
    type
    id
    placeholders_geojson
    outer_notches_geojson
    outer_corners_geojson 
    seam_guides_geojson 
    internal_lines_geojson
    buttons_geojson
    pleats_geojson
    pins_geojson
  }
}
"""

GET_BODY_HEADER_BY_VERSION = """query get_body($body_code: String = "", $version: numeric = 1, $size_code: String = "" ) {
  meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $version}}) {
    id
    body_code
    metadata
    version
    contracts_failing
    body_piece_count_checksum
    body_file_uploaded_at
    created_at
    updated_at
  }
}"""

GET_BODY_PIECES_BY_VERSION = """query get_body($body_code: String = "", $version: numeric = 1, $size_code: String = "" ) {
  meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $version}}) {
    id
    body_code
    metadata
    contracts_failing
    body_pieces{
      id
      key
      deleted_at
      piece_key
      size_code
    }
  }
}"""

GET_BODY_HEADER = """query get_bodies($body_code: String = "") {
  meta_bodies (where: {body_code: {_eq: $body_code}}) {
    id
    body_code
    version
    metadata
    contracts_failing
  }
}"""

GET_BODIES_BY_BRAND = """query get_bodies($brand_code: String = "") {
  meta_bodies (where: {brand_code: {_eq: $brand_code}}) {
    id
    body_code
    version
    metadata
    model_3d_uri
    front_image_uri
    point_cloud_uri
    body_piece_count_checksum
    contracts_failing
  }
}"""

GET_BODY_BY_VERSION_SIZE = """query get_body($body_code: String = "", $version: numeric = 1, $size_code: String = "" ) {
  meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $version}}) {
    id
    body_code
    metadata
    contracts_failing
    body_pieces(where: {size_code: {_eq: $size_code}, deleted_at: {_is_null: true}}) {
      key
      piece_key
      outer_geojson
      inner_geojson
      type
      id
      placeholders_geojson
      outer_notches_geojson
      size_code
      outer_corners_geojson 
      seam_guides_geojson 
      internal_lines_geojson
      buttons_geojson
      pleats_geojson
      pins_geojson
    }
  }
}"""

GET_BODY_BY_ID_SIZE = """query get_body($id: uuid = "", $size_code: String = "" ) {
 meta_bodies(where: {id: {_eq: $id}}){
    id
    body_code
    metadata
    contracts_failing
    body_pieces(where: {size_code: {_eq: $size_code}, deleted_at: {_is_null: true}}) {
      key
      piece_key
      outer_geojson
      inner_geojson
      type
      id
      outer_notches_geojson
      size_code
      placeholders_geojson
      outer_corners_geojson 
      seam_guides_geojson 
      internal_lines_geojson
      buttons_geojson
      pleats_geojson
      pins_geojson
      fuse_type
    }
  }
}"""

GET_BODY_WITH_PIECES_BY_ID = """query get_body($body_code: String = "", $version: numeric = 1, $size_code: String = "" ) {
  meta_bodies(where: {id: {_eq: $id}}){
    id
    body_code
    metadata
    contracts_failing
    body_pieces(where: {deleted_at: {_is_null: true}}) {
      key
      piece_key
      outer_geojson
      inner_geojson
      type
      id
      placeholders_geojson
      outer_notches_geojson
      size_code
      outer_corners_geojson 
      seam_guides_geojson 
      internal_lines_geojson
      buttons_geojson
      pleats_geojson
      pins_geojson
    }
  }
}"""

GET_BODY_STAMPERS = """query get_body_stampers($body_code: String = "", $version: numeric = 1) {
  meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $version}}) {
    id
    body_code
    metadata
    body_pieces(where: {type: {_eq: "stamper"}}) {
      key
      piece_key
      outer_geojson
      inner_geojson
      type
      id
      outer_notches_geojson
      size_code
      outer_corners_geojson
      seam_guides_geojson
      internal_lines_geojson
    }
  }
}
"""
LIST_BODIES = """query all_bodies {
  meta_bodies  {
    id
    body_code
    version
    profile
    metadata
    updated_at
  }
}"""

LIST_BODIES_WITH_PIECES = """query all_bodies {
  meta_bodies  {
    id
    body_code
    version
    metadata
    profile
    body_piece_count_checksum
    body_pieces{
      id
      key
      piece_key
      size_code
    }
  }
}"""

GET_BODY_BY_ID = """query body_lookup_by_id($id: uuid = "") {
  meta_bodies(where: {id: {_eq: $id}}) {
    version
    id
    metadata
    body_code
    estimated_sewing_time
    trim_costs
    body_piece_count_checksum
  }
}
"""

GET_STYLE_STATUS_BY_SKU = """query get_style_status($metadata_sku: jsonb = "") {
  meta_styles(where: {metadata: {_contains: $metadata_sku}}) {
    body_code
    name
    metadata
    modified_at
    point_cloud_uri
    front_image_uri
    model_3d_uri
    brand_code
    contracts_failing
    piece_version_delta_test_results
    id
    style_sizes {
      id
      updated_at
      status
      size_code
    }
    status
  }
}
"""

GET_STYLES_BY_BRAND = """query get_style_status($brand_code: String = "") {
  meta_styles(where: {brand_code: {_eq: $brand_code}}) {
    body_code
    brand_code
    name
    metadata
    modified_at
    point_cloud_uri
    front_image_uri
    model_3d_uri
    brand_code
    contracts_failing
    piece_version_delta_test_results
    id
    status
  }
}
"""

REGISTER_PIECES_HASH_FOR_STYLE_SIZE = """mutation register_pieces_hash($sid: uuid = "", $pieces_hash_id: uuid = "", $piece_material_hash: String = "" $body_code: String = "", $created_at: timestamptz = "") {
  insert_meta_pieces_hash_registry_one(object: {body_code: $body_code, created_at: $created_at, id: $pieces_hash_id}, 
      on_conflict: {constraint: pieces_hash_registry_pkey, update_columns: [id, body_code, created_at]}) {
    rank
    id
    created_at
    body_code
  }
  update_meta_styles(where: {id: {_eq: $sid}}, _set: {pieces_hash_id: $pieces_hash_id, piece_material_hash: $piece_material_hash}) {
    returning {
      id
    }
  }
}

"""

GET_STYLE_STATUS_HISTORY_BY_STYLE_ID_AND_BODY_VERSION = """query get_versioned_style_status($body_version: Int = 10, $style_id: uuid = "") {
  meta_style_status_history(where: {body_version: {_eq: $body_version}, style_id: {_eq: $style_id}}) {
    contracts_failing
    body_version
    id
    body_id
    updated_at
  }
}
"""

GET_STYLE_STATUS_HISTORY_BY_STYLE_ID = """query get_versioned_style_status( $style_id: uuid = "") {
  meta_style_status_history(where: { style_id: {_eq: $style_id}}) {
    contracts_failing
    body_version
    id
    body_id
    updated_at 
  }
}
"""

UPDATE_STYLE_STATUS_HISTORY = """mutation update_style_status($style_status: meta_style_status_history_insert_input = {}) {
  insert_meta_style_status_history_one(object: $style_status, on_conflict: {constraint: style_status_history_pkey, update_columns: [contracts_failing, metadata]}) {
    contracts_failing
    id
    body_version
    body_id
    style_id
  }
}
"""

# may change this to upsert on id - here we audit every attempt to invalidate and we dont dedup
# TODO
# -add process and user doing the invalidation
UPSERT_STYLE_STATUS_BY_SKU_WITH_HISTORY = """mutation update_style_status($contracts_failing: jsonb = "", $status: String = "", $metadata_sku: jsonb = "",
$style_status: meta_style_status_history_insert_input = {}) {
  
  insert_meta_style_status_history_one(object: $style_status, on_conflict: {constraint: style_status_history_pkey, update_columns: [contracts_failing, metadata]}) {
    contracts_failing
    body_version
    body_id
    style_id
  }
  
  update_meta_styles(where:   {metadata: {_contains: $metadata_sku}},  _set: {status: $status, contracts_failing: $contracts_failing})  {
    returning {
      id
      contracts_failing  
    }
  }
}
"""


def register_pieces_hash(
    style_id, pieces_hash, piece_material_hash, body_code, created_at, hasura=None
):
    """
    the pieces hash usually taken from the sample size is use to generate a hash on the current style version
    this is registered and ranked in a pieces hash register and linked to the style
    the pieces hash is a uuid which links the tables
    """
    hasura = hasura or res.connectors.load("hasura")

    return hasura.execute_with_kwargs(
        REGISTER_PIECES_HASH_FOR_STYLE_SIZE,
        sid=style_id,
        pieces_hash_id=pieces_hash,
        piece_material_hash=piece_material_hash,
        body_code=body_code,
        created_at=created_at,
    )


def add_body_contract_failed(contract, hasura=None):
    hasura = hasura or res.connectors.load("hasura")


def add_style_contract_failed(contract, hasura=None):
    hasura = hasura or res.connectors.load("hasura")


def get_body_by_id(id, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(GET_BODY_BY_ID, id=id)


def style_sku(sku):
    """
    this guy knows the difference between a sku and a style sku
    """

    return " ".join([s.strip() for s in sku.split(" ")[:3]])


def get_style_header_with_sizes(sku, hasura=None):
    hasura = hasura or res.connectors.load("hasura")

    # we only take the three parts
    if isinstance(sku, str) and len(sku.split(" ")) == 4:
        res.utils.logger.warn(
            f"You are asking for a style with a four part sku {sku} instead of a three part style sku. We will return all style sizes for {style_sku(sku)}"
        )
    sku = style_sku(sku)

    return hasura.execute_with_kwargs(
        GET_STYLE_HEADERS_WITH_SIZES_BY_SKU, metadata_sku={"sku": sku}
    )


def get_all_active_style_header_with_costs(hasura=None, flatten_to_df=True):
    """
    style look up for the header - cheat is to pass null and you get all of the styles
    """
    hasura = hasura or res.connectors.load("hasura")
    r = hasura.execute(GET_ALL_STYLE_HEADERS_WITH_COSTS)
    if flatten_to_df:
        return pd.DataFrame(r["meta_style_sizes"])
    return r


def get_style_header(sku, hasura=None):
    """
    style look up for the header - cheat is to pass null and you get all of the styles
    """
    hasura = hasura or res.connectors.load("hasura")
    if sku is None:
        return hasura.execute(GET_ALL_STYLE_HEADERS)

    # we only take the three parts
    if isinstance(sku, str) and len(sku.split(" ")) == 4:
        res.utils.logger.warn(
            f"You are asking for a style with a four part sku {sku} instead of a three part style sku. We will return all style sizes for {style_sku(sku)}"
        )
    sku = style_sku(sku)

    return hasura.execute_with_kwargs(
        GET_STYLE_HEADERS_BY_SKU, metadata_sku={"sku": sku}
    )


# get_style_header_by_size_id_and_size
@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_style_header_by_style_id_and_size(style_id, size_code, hasura=None):
    """
    style look up for the header - cheat is to pass null and you get all of the styles
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        GET_STYLE_HEADERS_BY_ID_AND_SIZE, style_id=style_id, size_code=size_code
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_style_header_by_style_size_id(style_size_id, hasura=None):
    """
    style look up for the header - cheat is to pass null and you get all of the styles
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(GET_STYLE_SIZE_HEADER_BY_ID, id=style_size_id)


def list_bodies(body_code=None, with_pieces=False, as_dataframe=False):
    """
    lists all bodies or looks for a body header
    """
    hasura = res.connectors.load("hasura")
    if body_code is None:
        if with_pieces:
            r = hasura.execute_with_kwargs(LIST_BODIES_WITH_PIECES)
        else:
            r = hasura.execute_with_kwargs(LIST_BODIES)

        if as_dataframe:
            r = pd.DataFrame(r["meta_bodies"])
        return r

    return hasura.execute_with_kwargs(GET_BODY_HEADER, body_code=body_code)


def get_body_header(body_code, version=None, hasura=None):
    """
    body_code = KT-6079
    version = 1
    """

    hasura = hasura or res.connectors.load("hasura")
    if not version:
        result = hasura.execute_with_kwargs(GET_BODY_HEADER, body_code=body_code)
    else:
        result = hasura.execute_with_kwargs(
            GET_BODY_HEADER_BY_VERSION, body_code=body_code, version=version
        )

    filter_results = result["meta_bodies"]
    if filter_results:
        data = [
            f
            for f in filter_results
            if f["id"] == _validate_body_id(f["body_code"], f["version"])
        ]
        result["meta_bodies"] = data

    return result


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def _is_3d_body(body_code, version, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    r = get_body_header(body_code, version, hasura=hasura)
    r = r["meta_bodies"]
    if r:
        r[0]["metadata"].get("flow") == "3d"
    else:
        res.utils.logger.warn(
            f"Could not find a body [{body_code}] in version {version}"
        )
    return None


def _validate_body_id(body_code, body_version):
    return res.utils.uuid_str_from_dict(
        {
            "code": body_code,
            "version": int(float(body_version)),
            "profile": "default",
        }
    )


def get_body_pieces(body_code, version, size_code, hasura=None):
    """
    lookup the body pieces
    """
    valid_body_id = _validate_body_id(body_code, version)

    b = pd.DataFrame(
        get_body_by_id_and_size(valid_body_id, size_code, flatten=False)["meta_bodies"][
            0
        ]["body_pieces"]
    )

    def _val(row):
        return

    return dict(b[["piece_key", "id"]].values)


def list_all_body_pieces(body_code, version, hasura=None, flatten_to_df=True):
    """
    lookup the body pieces
    """
    hasura = hasura or res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        GET_BODY_PIECES_BY_VERSION, body_code=body_code, version=version
    )["meta_bodies"]

    if flatten_to_df:
        df = pd.DataFrame(result)
        result = df.explode("body_pieces").reset_index(drop=True)
        result = res.utils.dataframes.expand_column_drop(
            result, "body_pieces", alias="pieces"
        )

    return result


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def list_all_body_sizes(body_code, version, hasura=None):
    """
    lookup the body pieces
    """
    pcs = list_all_body_pieces(body_code, version, hasura=hasura)
    return list(pcs["pieces_size_code"].unique())


def _try_add_combined_geom(d):
    """
    experimental for now for testing - will move this elsewhere - contract here is for every thing to be
    in the format of lines e.g. we can create stampers from it  otherwise its just a visual tool
    all geometries are at the origin in the same scale  DPI=300
    """
    from res.media.images.geometry import (
        dots_to_discs,
        safe_unary_union,
        shift_geometry_to_origin,
    )

    try:
        sg = safe_unary_union(d["body_pieces_seam_guides_geojson"])
        il = safe_unary_union(d.get("body_pieces_internal_lines_geojson"))
        sl = safe_unary_union(d.get("body_pieces_inner_geojson"))
        n = dots_to_discs(
            safe_unary_union(d["body_pieces_outer_notches_geojson"]), buffer=20
        )
        o = d["body_pieces_outer_geojson"]
        g = shift_geometry_to_origin(
            safe_unary_union([n, sg, o, sl, il])  # il,
        )  # todo add the il

        return g
    except:
        return None


def get_stitched_segments(feature, self_name):
    """
    we have some extra meta data on the feature allowing us to find filtered or derived stuff - this is one

    sewn_lines = unary_union([get_stitched_bits(feature) for feature in db_piece["internal_lines_geojson"]["features"]
    if feature.get('properties', {}).get('connections', [])])

    """
    from shapely.ops import substring, unary_union
    from shapely.geometry import shape, LineString

    def shape_from(geojson):
        if geojson is None or geojson == {}:
            return None
        return shape(geojson)

    props = feature.get("properties", {})
    connections = props.get("connections", [])
    if not connections:
        return None

    shape = shape_from(feature["geometry"])

    sewn = None
    for connection in connections:
        from_start = connection.get("from_start_percent", 0) / 100
        from_end = connection.get("from_end_percent", 0) / 100

        if connection.get("shape_name") == self_name:
            continue

        # in case the connection is reversed
        if from_start > from_end:
            from_start, from_end = from_end, from_start

        bit = substring(shape, from_start, from_end, normalized=True)
        if sewn is None:
            sewn = bit
        else:
            sewn = unary_union([sewn, bit])

    return sewn or LineString()


def get_stitched_geometry(geo_json_col, name_col):
    from res.media.images.geometry import unary_union

    def f(row):
        internal_lines_geojson = row.get(geo_json_col, {})
        self_name = row.get(name_col, "")

        try:
            return unary_union(
                [
                    get_stitched_segments(feature, self_name)
                    for feature in internal_lines_geojson.get("features", [])
                    if feature.get("properties", {}).get("connections", [])
                ]
            )
        except:
            return None

    return f


def _flatten_body_result(result):
    # validate
    meta_body_pieces = (
        pd.DataFrame(result["meta_bodies"])
        .explode("body_pieces")
        .reset_index()
        .drop("index", 1)
    )

    meta_body_pieces = meta_body_pieces.dropna(subset=["body_pieces"])

    if len(meta_body_pieces) == 0:
        return meta_body_pieces

    meta_body_pieces = res.utils.dataframes.expand_column(
        meta_body_pieces, "body_pieces"
    ).drop("body_pieces", 1)

    try:
        meta_body_pieces["stitched_lines"] = meta_body_pieces.apply(
            get_stitched_geometry(
                "body_pieces_internal_lines_geojson", "body_pieces_piece_key"
            ),
            axis=1,
        )
    except:
        pass

    for col in [
        "body_pieces_outer_geojson",
        "body_pieces_inner_geojson",
        "body_pieces_outer_notches_geojson",
        "body_pieces_seam_guides_geojson",
        "body_pieces_outer_corners_geojson",
        "body_pieces_internal_lines_geojson",
        # "body_pieces_buttons_geojson",
    ]:  # others
        # extract the sew liens here
        if col in meta_body_pieces:
            meta_body_pieces[col] = meta_body_pieces[col].map(from_geojson)
    meta_body_pieces["geometry"] = meta_body_pieces.apply(
        _try_add_combined_geom, axis=1
    )

    return meta_body_pieces


def get_body_stampers(body_code, version, hasura=None, flatten=True):
    hasura = hasura or res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        GET_BODY_STAMPERS, body_code=body_code, version=version
    )

    if not flatten:
        return result

    return _flatten_body_result(result)


def get_body_by_id_and_size(id, size_code, hasura=None, flatten=True):
    """
    body_code = KT-6079
    version = 1
    """

    hasura = hasura or res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        GET_BODY_BY_ID_SIZE,
        id=id,
        size_code=size_code,
    )

    # validate the result on ids later

    if not flatten:
        return result

    return _flatten_body_result(result)


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def get_body(body_code, version, size_code, hasura=None, flatten=True):
    """
    body_code = KT-6079
    version = 1
    """

    bid = _validate_body_id(body_code=body_code, body_version=version)
    return get_body_by_id_and_size(
        id=bid, size_code=size_code, flatten=flatten, hasura=hasura
    )


def lookup_style_by_name(name):
    """
    lookup styles by name e.g.
     name = "Hooded Wind Breaker - Marketplace Embroidery Pink in Stretch Organic Cotton Ripstop"
    """

    Q = """query style_by_name($name: String = {}) {
    meta_styles(where: {name: {_eq: $name}}) {
      id
      name
      metadata
      }
    }

    """

    return res.connectors.load("hasura").execute_with_kwargs(Q, name=name.strip())


def lookup_style_by_bmc_sku(bmc_sku):
    """
    how to refer to three part of four part skus is unclearn but
    """

    Q = """query MyQuery($metadata_sku: jsonb = "") {
    meta_styles(where: {metadata: {_contains: $metadata_sku}}) {
      id
      name
      metadata
    }
    }
    """
    return res.connectors.load("hasura").execute_with_kwargs(
        Q,
        metadata_sku={"sku": bmc_sku},
    )


def _parse_style_size_pieces_response(r):
    r = r["meta_style_sizes"][0]
    df = pd.DataFrame(r["style_size_pieces"])
    style_header = r["style"]

    df["body_version"] = style_header.get("metadata", {}).get("body_version")
    df["print_type"] = style_header.get("metadata", {}).get("print_type")
    df["style_id"] = style_header["id"]
    df["style_rank"] = style_header.get("rank")

    df = res.utils.dataframes.expand_column_drop(df, "piece")
    df = res.utils.dataframes.expand_column_drop(df, "piece_body_piece", alias="meta")
    for col in ["meta_outer_geojson", "meta_outer_notches_geojson"]:
        if col in df.columns:
            try:
                df[col] = df[col].map(from_geojson)
            except:
                res.utils.logger.warn(f"Failing to parse geojson from column {col}")
                raise

    if len(df) == 0:
        return df
    # the first few terms of the piece key will be the group e.g. KT-2011-V10
    df["style_group"] = df["meta_key"].map(lambda x: "-".join(x.split("-")[:3]))
    try:
        df["piece_registry_rank"] = style_header["pieces_hash_registry"]["rank"]
    except:
        # need to work on this - suppress for now
        pass
    return df


def _parse_style_pieces_response(r):
    from dateparser import parse

    r = r["meta_styles"]

    r = sorted(
        r,
        key=lambda x: parse(x["modified_at"]),
    )[-1:]

    res.utils.logger.debug(f"We filtered the set")

    a = res.utils.dataframes.expand_column_drop(
        pd.DataFrame(r).explode("style_sizes").reset_index(drop=True), "style_sizes"
    )

    a = a.explode("style_sizes_style_size_pieces").reset_index(drop=True)
    a = res.utils.dataframes.expand_column_drop(
        a, "style_sizes_style_size_pieces", alias="garment"
    )
    a = res.utils.dataframes.expand_column_drop(a, "garment_piece")
    a = res.utils.dataframes.expand_column_drop(
        a, "garment_piece_body_piece", "body_piece"
    )
    try:
        a["stitched_lines"] = a.apply(
            get_stitched_geometry(
                "body_piece_internal_lines_geojson", "body_piece_piece_key"
            ),
            axis=1,
        )
    except Exception as ex:
        res.utils.logger.warn(f"Failed to unstitch the stitch {res.utils.ex_repr(ex)}")
    if len(a):
        for col in [
            "body_piece_outer_geojson",
            "body_piece_inner_geojson",
            "body_piece_outer_notches_geojson",
            "body_piece_outer_corners_geojson",
            "body_piece_seam_guides_geojson",
        ]:
            # mapping the geojson properties to objects in the parser

            try:
                a[col] = a[col].map(from_geojson)
            except Exception as ex:
                import traceback

                res.utils.logger.warn(
                    f"Failing to parse geojson from column {col} - {traceback.format_exc()}"
                )
                raise ex
                pass  # for now

    if len(a):
        a = a[a["style_sizes_status"].notnull()].reset_index(drop=True)
    return a


# def _flatten_style_pieces(result):
#     """
#     for the style - > sizes - > history -> pieces result we can flatten it to pieces
#     """
#     df = pd.DataFrame(result["meta_styles"])
#     df = df.explode("style_sizes")
#     df["pieces"] = df["style_sizes"].map(lambda g: g.get("style_size_pieces"))
#     df = df.explode("pieces").drop("style_sizes", 1).reset_index()
#     df["pieces"] = df["pieces"].map(lambda g: g.get("piece"))
#     df = dataframes.expand_column(df, "pieces").drop("pieces", 1).drop("index", 1)
#     return df


def get_style_by_name_and_size_code(name, size_code, flatten_to_df=True):
    h = res.connectors.load("hasura")
    result = h.execute_with_kwargs(
        GET_STYLE_BY_NAME_AND_SIZE,
        name=name,
        size_code=size_code,
    )

    # need to refactor and test these - there are two apertures, styles or style sizes
    return result if not flatten_to_df else _parse_style_pieces_response(result)


def is_style_size_active(sku, hasura=None):
    a = get_style_by_sku(sku, flatten_to_df=False, hasura=hasura)
    if not a:
        return a

    try:
        # Contract on schema structures for parent child queries
        # if there are multiple we take the newest one: re check sorting and renaming of styles
        status = a["meta_styles"][-1]["style_sizes"][0]["status"]
        # these should be enums in future
        return status.upper() == "ACTIVE"
    except:
        res.utils.logger.warn(
            f"Failed to load a style size for {sku} - check if it exists and we are in sync with the query structure"
        )
        return False


def list_inactive_styles():
    Q = """query MyQuery {
      meta_style_sizes(where: {status: {_nin: ["Active", "ACTIVE"]}}) {
        metadata
        id
        status
      }
    }"""
    # TODO there should not be any styles in here that are missing skus TEST!!
    hasura = res.connectors.load("hasura")
    dd = hasura.execute(Q)["meta_style_sizes"]
    inactive = [s["metadata"].get("sku") for s in dd]
    inactive = [a for a in inactive if a is not None]
    return inactive


def list_active_styles():
    Q = """query MyQuery {
      meta_style_sizes(where: {status: {_in: ["Active", "ACTIVE"]}}) {
        metadata
        id
        status
      }
    }"""
    # TODO there should not be any styles in here that are missing skus TEST!!
    hasura = res.connectors.load("hasura")
    dd = hasura.execute(Q)["meta_style_sizes"]
    active = [s["metadata"].get("sku") for s in dd]
    active = [a for a in active if a is not None]
    return active


def get_active_style_size_pieces_by_sku(sku, flatten_to_df=True, hasura=None):
    """
    we can save SKU in four parts in the size later but we can also look up by style sku and size for now at least - down to perf in the end
    """

    assert len(sku.split(" ")) == 4, f"The sku {sku} must have 4 components B M C S"

    h = hasura or res.connectors.load("hasura")

    result = h.execute_with_kwargs(GET_STYLE_SIZE_BY_SKU, metadata_sku={"sku": sku})

    return result if not flatten_to_df else _parse_style_size_pieces_response(result)


def get_active_style_size_pieces_by_id(
    id, flatten_to_df=True, only_keys_and_codes=False, hasura=None
):
    """
    we can save SKU in four parts in the size later but we can also look up by style sku and size for now at least - down to perf in the end
    """
    h = hasura or res.connectors.load("hasura")

    result = h.execute_with_kwargs(
        GET_STYLE_SIZE_BY_ID if not only_keys_and_codes else GET_STYLE_SIZE_KEYS_BY_ID,
        id=id,
    )

    return result if not flatten_to_df else _parse_style_size_pieces_response(result)


def get_style_costs(sku, flatten_to_df=True, hasura=None):
    """
    we can save SKU in four parts in the size later but we can also look up by style sku and size for now at least - down to perf in the end
    THIS IS SUBTLY RETURNING A DIFFERENT APERTURE THAN  get_style_size_by_sku
    """
    h = hasura or res.connectors.load("hasura")
    assert len(sku.split(" ")) == 4, "The sku must have 4 components BMCS"

    # just a little flex
    if "-" != sku[2]:
        sku = f"{sku[:2]}-{sku[2:]}"

    size_code = sku.split(" ")[-1].strip()

    result = h.tenacious_execute_with_kwargs(
        GET_STYLE_SIZE_HEADER_BY_SKU,
        metadata_sku={"sku": style_sku(sku)},
        size_code=size_code,
    )

    return result


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_body_piece_count_by_id_and_size_code(bid, size_code, hasura=None):
    """
    get all non deleted and known pieces
    assumes no null types and all other types are interesting

    """
    Q = """query count_body_pieces($body_id: uuid = "", $size_code: String = "") {
      meta_body_pieces_aggregate(where: {body_id: {_eq: $body_id},
        type: {_neq: "unknown"}, 
        deleted_at: {_is_null: true}, 
        size_code: {_eq: $size_code}}) {
        aggregate {
          count
        }
      }
    }
    """
    # {'meta_body_pieces_aggregate': {'aggregate': {'count': 29}}}

    hasura = hasura or res.connectors.load("hasura")

    return hasura.execute_with_kwargs(Q, body_id=bid, size_code=size_code)[
        "meta_body_pieces_aggregate"
    ]["aggregate"]["count"]


def get_style_by_sku_healz(sku, flatten_to_df=True, hasura=None):
    """
    we can save SKU in four parts in the size later but we can also look up by style sku and size for now at least - down to perf in the end
    THIS IS SUBTLY RETURNING A DIFFERENT APERTURE THAN  get_style_size_by_sku
    """

    h = hasura or res.connectors.load("hasura")
    assert len(sku.split(" ")) == 4, "The sku must have 4 components BMCS"

    # just a little flex
    if "-" != sku[2]:
        sku = f"{sku[:2]}-{sku[2:]}"

    size_code = sku.split(" ")[-1].strip()

    result = h.execute_with_kwargs(
        GET_STYLE_BY_STYLE_SKU_ALL_SIZE, metadata_sku={"sku": style_sku(sku)}
    )

    if not result["meta_styles"]:
        return None

    return result if not flatten_to_df else _parse_style_pieces_response(result)


def get_style_by_sku(sku, flatten_to_df=True, body_version=None, hasura=None):
    """
    we can save SKU in four parts in the size later but we can also look up by style sku and size for now at least - down to perf in the end
    THIS IS SUBTLY RETURNING A DIFFERENT APERTURE THAN  get_style_size_by_sku
    """

    h = hasura or res.connectors.load("hasura")
    assert len(sku.split(" ")) == 4, "The sku must have 4 components BMCS"

    # just a little flex
    if "-" != sku[2]:
        sku = f"{sku[:2]}-{sku[2:]}"

    size_code = sku.split(" ")[-1].strip()

    if body_version:
        body_version = int(body_version)
        # ignore if its the latest
        max_version = get_max_style_body_version(sku)
        if max_version == body_version:
            body_version = None

    if not body_version:
        result = h.execute_with_kwargs(
            GET_STYLE_BY_STYLE_SKU_AND_SIZE,
            metadata_sku={"sku": style_sku(sku)},
            size_code=size_code,
        )
        res.utils.logger.debug(f"Loaded style without body version")
    else:
        res.utils.logger.warn(
            f"Attempting to time travel and load pieces by body version {body_version} for {sku=}"
        )
        result = h.execute_with_kwargs(
            GET_STYLE_BY_STYLE_SKU_AND_SIZE_AND_BODY_VERSION,
            metadata_sku={"sku": style_sku(sku)},
            body_version=body_version,
            size_code=size_code,
        )

    if not result["meta_styles"]:
        return None

    return result if not flatten_to_df else _parse_style_pieces_response(result)


def get_style_status_by_sku(sku, hasura=None, most_recent_only=False):
    """
    will become a tool to quickly check contracts and activity on the style

    we have to be super careful here; the idea is generally that we use uuids
    but here we can select a sku for a meta one say. but we can have one to many e.g. if names change
    so the meta one should only select the most recent match by SKU which we do in the flattener
    it is generally not advised to search by skus elsewhere
    """

    hasura = hasura or res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        GET_STYLE_STATUS_BY_SKU, metadata_sku={"sku": sku}
    )
    if most_recent_only:
        s = sorted(
            result["meta_styles"],
            key=lambda x: parse(x["modified_at"]),
        )
        return {"meta_styles": s[-1:]}
    return result


def get_max_style_body_version(sku):
    if sku[2] != "-":
        sku = f"{sku[:2]}-{sku[2:]}"
    sku = style_sku(sku)
    return pd.DataFrame(
        [s["metadata"] for s in get_style_status_by_sku(sku)["meta_styles"]]
    )["body_version"].max()


def update_products(products):
    """
    the style id should be generated by the helpers for both the style id and product id
    """
    return hasura.Client(timeout=HASURA_TIMEOUT).execute_with_kwargs(
        BATCH_INSERT_GARMENT_PENDING, products=products
    )


def update_contracts_failing_by_skus(
    skus, contracts_failed, status="Active", hasura=None, merge_set=False
):
    for sku in skus:
        update_contracts_failing_by_sku(
            sku,
            contracts_failed=contracts_failed,
            status=status,
            hasura=hasura,
            merge_set=merge_set,
        )


def update_contracts_failing_by_sku(
    sku,
    contracts_failed,
    status="Active",
    hasura=None,
    merge_set=False,
    audited_m1=None,
    process_id=None,
    user_email=None,
):
    """
    auditing is important for api setting contracts status
    so the user and process information can be passed int and saved (TODO)
    """

    hasura = hasura or res.connectors.load("hasura")

    if merge_set:
        s = get_style_status_by_sku(sku)["meta_styles"]
        contracts_failing = []
        if len(s):
            contracts_failing = s[0].get("contracts_failing")
        contracts_failed = list(set(contracts_failing + contracts_failed))
        res.utils.logger.info(f"Merged is {contracts_failed}")

    if len(contracts_failed):
        status = "Failed"

    """
    the default mode should be to do this with an audit
    """
    if audited_m1:
        res.utils.logger.info(f"Adding meta one contracts with audit history")
        return hasura.execute_with_kwargs(
            UPSERT_STYLE_STATUS_BY_SKU_WITH_HISTORY,
            contracts_failing=contracts_failed,
            metadata_sku={"sku": sku},
            status="Active",
            style_status={
                "contracts_failing": contracts_failed,
                "body_version": audited_m1._body_ref["version"],
                "body_id": audited_m1._body_ref["id"],
                "style_id": audited_m1.style_id,
            },
        )

    return hasura.tenacious_execute_with_kwargs(
        UPSERT_STYLE_STATUS_BY_SKU,
        metadata_sku={"sku": sku},
        contracts_failing=contracts_failed,
        status=status,
    )


def remove_style_contracts_failing(sku):
    return update_contracts_failing_by_sku(sku, contracts_failed=[], merge_set=False)


def add_style_contract_failing(
    sku, contracts, m1=None, process_id=None, user_email=None
):
    """
    Merge the incoming with what we have rather than replace what we have
    i think there is a single graphql thing to do this TODO:
    """
    return update_contracts_failing_by_sku(
        sku,
        contracts_failed=contracts,
        merge_set=True,
        audited_m1=m1,
        process_id=None,
        user_email=None,
    )


def update_style(style, garment, pieces, split_batch=True):
    """ """
    garment_id = garment["id"]
    history = [
        {
            "id": uuid_str_from_dict(
                {
                    "garment_id": garment_id,
                    "piece_id": p,
                }
            ),
            "style_size_id": garment_id,
            "piece_id": p,
        }
        for p in [p["id"] for p in pieces]
    ]

    # if there are no pieces then we must just insert the styles without pieces but the style should not be active
    if not pieces:
        # TODO assert style is actually not active somewhere
        return hasura.Client(timeout=HASURA_TIMEOUT).execute_with_kwargs(
            BATCH_INSERT_GARMENT_PENDING,
            style=style,
            # we do one garment at a time
            style_sizes=[garment],
        )

    if not split_batch:
        return hasura.Client().execute_with_kwargs(
            BATCH_INSERT_GARMENT,
            style=style,
            # we do one garment at a time
            style_sizes=[garment],
            # we add all the pieces for the garment
            pieces=pieces,
            # the garment id is used in the mutation
            garment_id=garment_id,
            # we determine the history of relationships that we want to keep
            history=history,
            ended_at=res.utils.dates.utc_now_iso_string(None),
        )

    # the batch can sometimes be convenient but it can put a strain on the server in a single transaction.
    # here we can separate them out and maybe add a rollback strategy later
    # largely it is the gateway timeout we are trying to get around here
    # this is particularly important to think about as the data grow
    result = hasura.Client().execute_with_kwargs(
        BATCH_INSERT_GARMENT_NO_PIECES,
        style=style,
        # we do one garment at a time
        style_sizes=[garment],
    )

    res.utils.logger.debug(
        f"Inserting pieces for style after inserting headers and history"
    )

    presult = hasura.Client().execute_with_kwargs(
        BATCH_INSERT_GARMENT_PIECES,
        pieces=pieces,  # the garment id is used in the mutation
        garment_id=garment_id,
        # we determine the history of relationships that we want to keep
        history=history,
        ended_at=res.utils.dates.utc_now_iso_string(None),
    )

    return result


def sample_case_n1():
    """
    Playing with mutations for data model approach number 1

    In this case we can insert a style, style_sizes (garments), pieces and piece history ie. the one-many link of garments to their pieces with history

    The first pattern that i use here is client side business constraint. We could use Hasura for this but for two reasons i do not yet
    1. I dont yet know what my constraint should be so i want to make it soft for now and decide in code
    2. I personally dont like the idea of locking in some business logic as constraints to a database except as a safety

    My strategy would be to evolve and control the constraint in code and then later
    1. harden it as a DB constraint
    2. optionally move control of the upsert to use this constraint as a matter of taste

    To do a "client side constraint" i use the uuid_str_from_dict which generates a uuid from a set of nested fields
    Then i use the unique constraint on the uuid for the upsert

    The other pattern here is a history table
    1. in the transaction i terminate all the garments  by setting the end date to now (TODO: want to do this as a function)
    2. for all pieces that i am now adding, set the start date to now if necessary and end date to null


    from a dataframe i can build my model
    - generate ids
    - group and explode as required to create nestings and unnestings
    -

    """

    c = ResGraphQLClient(api_url="http://localhost:8081/v1/graphql")

    garment_id = uuid_str_from_dict({"style.name": "test2", "size": "2ZZSM"})
    style_id = uuid_str_from_dict({"name": "test2"})

    return c.query_with_kwargs(
        BATCH_INSERT_GARMENT,
        style={
            "id": style_id,
            "name": "test2",
            "status": "Active",
            "body_code": "Test",
            "labels": ["TEST"],
            "metadata": {},
        },
        style_sizes=[
            {
                "id": garment_id,
                "size_code": "2ZZSM",
                "status": "Active",
                "labels": [],
                "metadata": {},
                "price": 43.0,
                "cost_at_onboarding": 14,
                "style_id": style_id,
            }
        ],
        # these are unique on body,size,artwork and material + a custom hash just in case: BMC-S-C
        pieces=[
            {
                "id": uuid_str_from_dict({"stuff": "test"}),
                "code": "test",
                "versioned_body_code": "KT-2011-V2",
                "color_code": "test",
                "material_code": "test",
                "size_code": "test",
                "artwork_uri": "s3://test",
                "metadata": {},
            },
        ],
        garment_id=garment_id,
        # add the history - default start date is now and end date is null to open the history
        # we also kills old records in the block
        history=[
            {
                "id": uuid_str_from_dict(
                    {
                        "garment_id": garment_id,
                        "piece_id": uuid_str_from_dict({"stuff": "test"}),
                    }
                ),
                "style_size_id": garment_id,
                "piece_id": uuid_str_from_dict({"stuff": "test"}),
            }
        ],
        # this is the session date we will use for history tables
        ended_at=res.utils.dates.utc_now_iso_string(None),
    )


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def _migrate_m1_costs(m1, mcosts=None):
    """
    utility to migrate the costs from old system to the new
    """
    hasura = res.connectors.load("hasura")

    material_usage_statistics = (
        m1.get_costs() if mcosts is None or len(mcosts) == 0 else mcosts
    )
    c = m1.style_cost_capture(
        costs={"piece_stats_by_material": material_usage_statistics}
    )
    cost_at_onboarding = c["Cost"].sum()
    costs = c.to_dict("records")
    size_yield = c[(c["Category"] == "Materials") & (c["Unit"] == "Yards")][
        "Quantity"
    ].sum()

    garment_id = m1.style_size_id

    response = hasura.tenacious_execute_with_kwargs(
        UPSERT_STYLE_COSTS,
        id=garment_id,
        # change this column name to be material stats
        material_usage_statistics=material_usage_statistics,
        size_yield=size_yield,
        # add a foreign key table for actual costs - ends any values that are outdated and keep the latest
        costs=costs,
        piece_count_checksum=m1.body_piece_count,
        # add the snapshot total
        cost_at_onboarding=cost_at_onboarding,
    )
    return response
