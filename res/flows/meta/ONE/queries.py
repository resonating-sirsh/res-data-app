"""
queries can be hasura or snowflake queries and also python wrappers
"""

import res
from res.flows.api.core.node import node_id_for_name
import pandas as pd
import itertools
from functools import lru_cache
from tenacity import retry, wait_fixed, stop_after_attempt
import json
import traceback
import re
from stringcase import snakecase

# CREATE A VARIANT WITH FULL ANNOTATIONS LATER
# the image outline maybe separate
#             base_image_outline

# Q = """query MyQuery($keys: [String!] = "") {
#   meta_body_pieces(where: {key: {_in: $keys}, deleted_at: {_is_null: true}}) {
#     body_id
#     pieces {
#       style_size_pieces(where: {ended_at: {_is_null: false}}) {
#         id
#         style_size {
#           id
#         }
#       }
#       material_code
#       metadata
#       id
#       color_code
#       base_image_uri
#       base_image_s3_file_version_id
#       base_image_outline
#       artwork_uri
#       normed_size
#       piece_ordinal
#       piece_set_size
#     }
#     outer_geojson
#     outer_notches_geojson
#     piece_key
#   }
# }
# """

GET_BODY_INFO_BY_VERSION = """query get_body($body_code: String = "", $version: numeric = 1, $size_code: String = "" ) {
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

GET_BMS_COSTS_BY_ID = """query lookup_bms_costs($id: uuid = "") {
  meta_body_pieces_with_material_by_pk(id: $id) {
    body_code
    body_version
    created_at
    checksum
    id
    metadata
    nesting_statistics
    number_of_materials
    overhead_rate
    piece_map
    sewing_time
    size_code
    trim_costs
    updated_at
  }
}
"""

GET_SKU_METADATA = """query sku_metadata($metadata_sku: jsonb = "") {
  meta_styles(where: {metadata: {_contains: $metadata_sku}}) {
    metadata
    id
    modified_at
    piece_material_hash
  }
}
"""

GET_IS_COST_API_ENABLED = """
query IsCostApiEnabled($brand_code:String) {
  meta_brands(where: {brand_code: {_eq: $brand_code}}) {
    id
    brand_code
    is_cost_api_enabled
  }
}
"""

GET_META_ONE_BY_STYLE_SKU_AND_SIZE = """query get_style_by_sku_and_size($metadata_sku: jsonb = "", $size_code: String = "")  {
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
            piece_set_size
            piece_ordinal
            normed_size
            base_image_uri
            base_image_s3_file_version_id
            annotated_image_uri
            annotated_image_s3_file_version_id

            body_piece   {
              id
              body_id
              key
              type
              piece_key
              outer_geojson
              outer_notches_geojson
              seam_guides_geojson
              outer_corners_geojson  
              sew_identity_symbol
              placeholders_geojson
              deleted_at
              created_at
              updated_at
            }
          }
       }
     }
  }
}
"""

GET_ALL_ACTIVE_STYLE_SIZE_PIECES = """
query get_all_active_style_pieces($metadata_sku: jsonb = "") {
  meta_styles(where: {metadata: {_contains: $metadata_sku} }) {
    body_code
    name
    contracts_failing
    created_at
    modified_at
    style_sizes {
      id
      size_code
      style_size_pieces(where: {ended_at: {_is_null: true}}) {
        ended_at
        piece {
          body_piece {
            body {
              version
            }
            size_code
            key
            piece_key
            updated_at
          }
          base_image_uri
          base_image_s3_file_version_id
          color_code
          normed_size
          updated_at
        }
      }
    }
  }
}
"""


GET_GRAPH_API_STYLE_HEADER_BY_SKU = """query getStyle($sku: String!) {
        style(code: $sku) {
            id
            code
            name
            printType
            isStyle3dOnboarded
            body {
                code
                basePatternSize{
                    code
                }
                patternVersionNumber 
            }
            color {
                code
            }
            material {
                code
            }
            
        }
    
    }"""


BY_ID_PREFIX = """query getStyle($id: ID!) {
        style(id: $id) { """


BY_SKU_PREFIX = """query getStyle($sku: String!) {
        style(code: $sku) {"""


GET_GRAPH_API_STYLE_ = """
            id
            code
            name
            printType
            stylePieces {
                bodyPiece {
                    id
                    code
                }
                material {
                    code
                }
                color {
                    code
                }
                artwork {
                    id
                    color {
                        code
                    }
                }
            }
            body {
                code
                brandCode
                availableSizes{
                    code
                    name
                    sizeScale
                    sizeScaleCode
                    sizeNormalized
                }
                basePatternSize{
                    code
                }
                patternVersionNumber
                
            }
            color {
                code
            }
            material {
                code
            }
            pieceMapping{
                bodyPiece{
                    id
                    code
                }
                material{
                    code
                    offsetSizeInches
                }
                artwork{
                    id
                    file{
                        s3{
                            key
                            bucket
                        }
                    }      
                }
                color{
                    code
                }
            }
        }
    
    }"""

GET_GRAPH_API_STYLE_BY_ID = BY_ID_PREFIX + GET_GRAPH_API_STYLE_

GET_GRAPH_API_STYLE_BY_SKU = BY_SKU_PREFIX + GET_GRAPH_API_STYLE_

GET_MAKE_ONE_PIECES_BY_NODE_AND_MATERIAL = """query MyQuery($material_code: String = "", $status: String = "", $piece_type: String = "", $node_id: uuid = "") {
  make_one_pieces(where: {piece: {material_code: {_eq: $material_code}}, node_id: {_eq: $node_id}, status: {_eq: $status}}) {
    id
    code
    meta_piece_id
    contracts_failed
    set_key
    piece {
      body_piece {
        id
          body_id
          key
          type
          piece_key
          size_code
          outer_geojson
          inner_geojson
          placeholders_geojson
          outer_notches_geojson
          outer_corners_geojson 
          seam_guides_geojson 
          sew_identity_symbol
          internal_lines_geojson
          deleted_at
        body {
          contracts_failing
        }
      }
      material_code
      color_code
      base_image_uri
      artwork_uri
      metadata
      piece_ordinal
      piece_set_size
      normed_size    
    }
    one_order {
      id
      one_code
      one_number
      sku
       style_size {
        style {
         contracts_failing
        }
      }
    }
  }
}
"""

GET_MAKE_ONE_PIECES_BY_NODE_AND_MATERIAL_AND_SET_KEY = """query MyQuery($material_code: String = "",  $status: String = "", $piece_type: String = "", $node_id: uuid = "", $set_key: String = "") {
  make_one_pieces(where: {piece: {material_code: {_eq: $material_code}}, set_key: {_eq: $set_key}, node_id: {_eq: $node_id}, status: {_eq: $status}}) {
    id
    code
    meta_piece_id
    contracts_failed
    set_key
    piece {
      body_piece {
        id
          body_id
          key
          type
          piece_key
          size_code
          outer_geojson
          inner_geojson
          placeholders_geojson
          outer_notches_geojson
          outer_corners_geojson 
          seam_guides_geojson 
          sew_identity_symbol
          internal_lines_geojson
          deleted_at
        body {
          contracts_failing
        }
      }
      material_code
      color_code
      base_image_uri
      artwork_uri
      metadata
      piece_ordinal
      piece_set_size
      normed_size    
    }
    one_order {
      id
      one_code
      one_number
      sku
       style_size {
        style {
         contracts_failing
        }
      }
    }
  }
}
"""

ONE_ORDER_BY_ONE_NUMBER = """query get_order_by_one_number($one_number: Int = 10) {
  make_one_pieces(where: {one_order: {one_number: {_eq: $one_number}}, deleted_at: {_is_null: true}}) {
    one_order {
      oid
      one_code
      one_number
      order_number
      sku
    }
    code
    contracts_failed
    defects
    id
    oid
    node {
      name
    }
  }
}"""

# TODO - loading the meta one by orders is essential what PPP does
GET_META_ONE_FROM_ONE_ORDER = """"""

GET_STYLE_BODY_COST_DATA = """query get_style_and_body($body_code: String = "", $body_version: numeric = "", $style_size_id: uuid = "") {
  meta_style_sizes(where: {id: {_eq: $style_size_id}}) {
    material_usage_statistics
    style {
      pieces_hash_id
      piece_material_hash
    }
    size_code
    style_size_pieces(where: {ended_at: {_is_null: true}}) {
      piece {
        material_code
        body_piece {
          piece_key
        }
      }
    }
  }
  meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $body_version}}) {
    body_code
    trim_costs
    estimated_sewing_time
    version
    body_piece_count_checksum
  }
}"""


def get_airtable_record_ids_and_dates(one_number, materials):
    """
    used for building ids used to generate print assets e.g. for healing requests
    """
    from res.connectors.airtable import AirtableConnector

    preds = AirtableConnector.make_key_lookup_predicate([one_number], "Order Number")
    airtable = res.connectors.load("airtable")
    tab = airtable["apprcULXTWu33KFsh"]["tblaNDuo1nylMVplQ"]
    pr = dict(
        tab.to_dataframe(
            fields=[
                "_recordid",
                "make_one_production_request_id",
                "Order Number",
                "Belongs to Order",
                "Exit Factory Date",
            ],
            filters=preds,
        ).iloc[0]
    )

    preds = AirtableConnector.make_key_lookup_predicate(materials, "Key")
    airtable = res.connectors.load("airtable")
    tab = airtable["apprcULXTWu33KFsh"]["tblJAhXttUzwxdaw5"]
    mr = tab.to_dataframe(fields=["Key", "_record_id"], filters=preds).to_dict(
        "records"
    )

    preds = AirtableConnector.make_key_lookup_predicate([one_number], "Order Number v3")
    airtable = res.connectors.load("airtable")
    tab = airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"]
    m1p = dict(
        tab.to_dataframe(
            fields=["Order Number v3", "_recordid", "_original_request_placed_at"],
            filters=preds,
        ).iloc[0]
    )

    return {
        "print_request_id": pr["_recordid"],
        "materials": {r["Key"]: r["record_id"] for r in mr},
        "make_one_production_request_id": pr["make_one_production_request_id"],
        "original_order_placed_at": m1p["_original_request_placed_at"],
    }


def fix_sku(sku):
    """
    this guy knows the difference between a sku and a style sku
    """
    if sku[2] != "-":
        sku = f"{sku[:2]}-{sku[2:]}"
    return sku


def style_sku(sku):
    """
    this guy knows the difference between a sku and a style sku
    """
    return " ".join([s.strip() for s in sku.split(" ")[:3]])


def get_active_style_pieces_for_sku(sku: str, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    data = hasura.execute_with_kwargs(
        GET_ALL_ACTIVE_STYLE_SIZE_PIECES, metadata_sku={"sku": sku}
    )

    def pull_out_pieces(style):
        for style_size in style["style_sizes"]:
            style_pieces = style_size["style_size_pieces"]
            if style_pieces:
                for p in style_pieces:
                    p = p["piece"]
                    p["contracts_failing"] = style.get("contracts_failing", [])
                    yield p

    # taking the last style which is the newest one
    df = pd.DataFrame(data["meta_styles"]).sort_values("modified_at", ascending=True)
    # be very careful here - we are iterating just the last part of the
    df = pd.DataFrame(pull_out_pieces(dict(df.iloc[-1])))
    df = res.utils.dataframes.expand_column_drop(df, "body_piece")
    df = res.utils.dataframes.expand_column_drop(
        df, "body_piece_body", alias="body_piece"
    )

    # flatten

    failing_contracts = []
    try:
        failing_contracts = df["contracts_failing"][0]
    except:
        pass

    df = df.drop("contracts_failing", axis=1)

    return {
        "style_code": sku,
        "contracts_failing": failing_contracts,
        "pieces": df.to_dict("records"),
    }


@retry(wait=wait_fixed(0.1), stop=stop_after_attempt(2), reraise=True)
def get_bms_checksum(id, hasura=None):
    Q = """query get_checksum($id: uuid = "") {
      meta_body_pieces_with_material(where: {id: {_eq: $id}}) {
        checksum
        id
      }
    }
    """
    hasura = hasura = res.connectors.load("hasura")
    return hasura.execute_with_kwargs(Q, id=id)["meta_body_pieces_with_material"]


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_style_body_cost_data(style_size_id, body_code, body_version, hasura=None):
    """ """
    hasura = hasura or res.connectors.load("hasura")

    return hasura.execute_with_kwargs(
        GET_STYLE_BODY_COST_DATA,
        style_size_id=style_size_id,
        body_code=body_code,
        body_version=body_version,
    )


def get_one_order_by_one_number(one_number, flatten=False, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    one_number = int(float(str(one_number)))
    result = hasura.execute_with_kwargs(ONE_ORDER_BY_ONE_NUMBER, one_number=one_number)
    if flatten:
        return pd.DataFrame(result["make_one_pieces"])
    return result


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_make_pieces_at_node_and_material(
    node_name, material_code, status="Enter", set_key=None, flatten=True, hasura=None
):
    """
    If you specify the set key you use a different path for now
    """
    hasura = hasura or res.connectors.load("hasura")

    result = (
        hasura.execute_with_kwargs(
            GET_MAKE_ONE_PIECES_BY_NODE_AND_MATERIAL,
            node_id=node_id_for_name(node_name),
            status=status,
            material_code=material_code,
        )
        if not set_key
        else hasura.execute_with_kwargs(
            GET_MAKE_ONE_PIECES_BY_NODE_AND_MATERIAL_AND_SET_KEY,
            node_id=node_id_for_name(node_name),
            status=status,
            material_code=material_code,
            set_key=set_key,
        )
    )

    if flatten:
        df = pd.DataFrame(result["make_one_pieces"])
        df = res.utils.dataframes.expand_column_drop(df, "piece", alias="pieces")
        df = res.utils.dataframes.expand_column_drop(
            df, "pieces_body_piece", alias="body_pieces"
        )

        return df

    return result


# @retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
# this might be killing the db
@lru_cache(maxsize=1000)
def _get_costs_match_legacy(sku, body_version=None, stat_name="piece_statistics"):
    from res.flows.dxa.styles import queries
    from dateparser import parse

    a = queries.get_style_costs(sku)["meta_styles"]
    s = sorted(
        a,
        key=lambda x: parse(x["modified_at"]),
    )
    s = s[-1]

    ss = s["style_sizes"][0]
    # this is a legacy thing - we actually just pass on the material part
    costs = ss["material_usage_statistics"]

    return {
        "key": s["metadata"]["sku"],
        "res_key": s["metadata"]["sku"],
        "size": ss["size_code"],
        stat_name: costs,
    }


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_one_costs_by_one_number(one_number, hasura=None):
    """
    get one make costs by one number e.g. pass 10299928

    """
    Q = """query get_one_costs($one_number: Int = 10) {
            make_one_orders(where: {one_number: {_eq: $one_number}}) {
                one_pieces {
                one_pieces_cost {
                    water_consumption
                    material_consumption
                    ink_consumption
                    electricity_consumption
                }
                code
                }
                sku
            }
            }

        """

    hasura = hasura or res.connectors.load("hasura")
    data = hasura.execute_with_kwargs(Q, one_number=one_number)

    def unpack_costs(d):
        material = d.get("material_consumption", {})
        result = {
            "material_printed_length_mm": sum(
                m["printed_length_mm"] for m in material.values()
            ),
            "material_printed_width_mm": sum(
                m["printed_width_mm"] for m in material.values()
            ),
        }
        ink = itertools.chain(
            *list([_d["consumption_data"] for _d in d.get("ink_consumption").values()])
        )
        printed_color = {f"ink_{v['Color']}_ml".lower(): v["ToPrint"] for v in ink}
        result.update(printed_color)

        return result

    df = data["make_one_orders"]
    sku = df[0].get("sku")
    df = pd.DataFrame(df).explode("one_pieces").reset_index(drop=True)
    df = (
        res.utils.dataframes.expand_column_drop(df, "one_pieces")
        .reset_index()
        .drop("index", 1)
    )
    df = df[df["one_pieces_one_pieces_cost"].notnull()].reset_index(drop=True)
    df["costs"] = df["one_pieces_one_pieces_cost"].map(unpack_costs)
    d = dict(
        res.utils.dataframes.expand_column(
            df.drop("one_pieces_code", 1), "costs", alias="qty"
        ).sum()
    )

    d["sku"] = sku
    return d


def get_apply_color_queue_changes(window_minutes):
    """
    extract times series from AC queue


    dfe[dfe['flow_version']==1]

    """

    def make_payloads(row):
        v = row.get("flow_version")
        l = (
            ["Export Colored Pieces", "Generate Meta ONE", "DxA Exit"]
            if v == 1
            else [
                "Place Color Elements",
                "Scale Color Elements",
                "Export Colored Pieces",
                "Unpack Assets",
                "Generate Meta ONE",
                "Review Brand Turntable",
                "Generate Pieces Preview",
                "Review Pieces Preview",
                "DxA Exit",
            ]
        )

        state = row["states_expanded_state"]

        ordinal = l.index(state) if state in l else -1

        d = {
            "id": res.utils.uuid_str_from_dict(
                {
                    "record_id": row["record_id"],
                    "state": state,
                    "entered": row["states_expanded_enterTime"],
                    "exited": row["states_expanded_exitTime"],
                    "env": res.utils.env.RES_ENV,
                }
            ),
            "flow_id": res.utils.uuid_str_from_dict({"flow": "apply-color-queue"}),
            "request_id": row["record_id"],
            "style_code": row["Style Code"],
            # must be an int - if null just make it zero
            "body_version": int(row["Body Version"]),
            "node": state,
            "entered_at": row["states_expanded_enterTime"],
            "exited_at": row["states_expanded_exitTime"],
            "node_ordinal": ordinal,
            "flow_version": row.get("flow_version"),
            "created_at": res.utils.dates.utc_now_iso_string(),  # row['Apply Color Flow Status Last Updated At'],
            "queue_exited_at": row["exited_at"] if state == "Done" else None,
            # use this as cancelled status if we are stuck here
            "cancelled_at": row["entered_at"] if state == "Cancelled" else None,
            "queue_entered_at": row["__timestamp__"],
            "sizes": row["Meta ONE Sizes Required"],
            "contracts_failing_list": row["Meta ONE Contract Variables"],
            "last_updated_at": row["Last Updated At"],
            "metadata": {
                "assignee_email": (
                    row["Assignee Email"] if pd.notnull(row["Assignee Email"]) else None
                ),
                "color_type": row.get("Color Type"),
            },
        }

        for f in [  # adding
            "Request Type",
            "Request Category",
            "Color Application Mode",
            "Has Open ONEs",
            "Requests IDs",
            "Export Asset Bundle Job Started At",
            "Export Asset Bundle Job Ended At",
        ]:
            d["metadata"][f] = row.get(f)
        return d

    ######
    ##     LOAD RAW DATA
    ######
    res.utils.logger.info(f"Loading data for window: {window_minutes} minutes")
    fields = [
        "Style Code",
        "Color Type",
        "Apply Color Flow JSON",
        "Body Version",
        "Apply Color Flow Status Last Updated At",
        "Assignee Email",
        # adding
        "Request Type",
        "Request Category",
        "Color Application Mode",
        "Has Open ONEs",
        "Requests IDs",
        "Meta ONE Contract Variables",
        "Meta ONE Sizes Required",
        "Export Asset Bundle Job Started At",
        "Export Asset Bundle Job Ended At",
        "Last Updated At",
    ]
    tab = res.connectors.load("airtable")["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    if window_minutes:
        df_raw = tab.updated_rows(
            minutes_ago=window_minutes,
            last_modified_field="Apply Color Flow Status Last Updated At",
            fields=fields,
        )
    else:
        res.utils.logger.info(f"As no window supplied, asking for everything")
        df_raw = tab.to_dataframe(fields=fields)

    # add empty fields
    for f in fields:
        if f not in df_raw.columns:
            df_raw[f] = None

    res.utils.logger.info(f"Fetched {len(df_raw)} records from the apply color queue")
    if len(df_raw) == 0:
        return pd.DataFrame()

    df_raw = df_raw[~df_raw["Apply Color Flow JSON"].isnull()].reset_index()
    if len(df_raw) == 0:
        return pd.DataFrame()

    df_raw["Body Version"] = df_raw["Body Version"].fillna(0)
    df_raw["states"] = df_raw["Apply Color Flow JSON"].map(json.loads)
    df_raw = res.utils.dataframes.replace_nan_with_none(df_raw)

    #####
    ##    determines the embedded states
    #####
    res.utils.logger.info(f"Data loaded - explanding states")
    states = pd.DataFrame([d for d in df_raw["states"]])

    def flow_version(x):
        alls = [n["state"] for n in x]
        flow_version = 2 if "Place Color Elements" in alls else 1
        return flow_version

    states["flow_version"] = states["states"].map(flow_version)
    # states = pd.DataFrame([d for d in states.explode('states')['states']])
    df = df_raw.join(
        states.explode("states")[["states", "flow_version", "exitTime"]],
        rsuffix="_expanded",
    )[
        [
            "Style Code",
            "record_id",
            "Assignee Email",
            "Color Type",
            "states_expanded",
            "flow_version",
            "Body Version",
            "index",
            "exitTime",
            # adding
            "Request Type",
            "Request Category",
            "Color Application Mode",
            "Has Open ONEs",
            "Requests IDs",
            "Export Asset Bundle Job Started At",
            "Export Asset Bundle Job Ended At",
            "Meta ONE Contract Variables",
            "Meta ONE Sizes Required",
            "Last Updated At",
            "__timestamp__",
        ]
    ].reset_index()
    df = res.utils.dataframes.expand_column(df, "states_expanded")

    ####
    ##    generate times series payload and some debugging metadata
    #####
    res.utils.logger.info(f"Generating payloads")

    df["payload"] = df.apply(make_payloads, axis=1)
    df["pid"] = df["payload"].map(lambda x: x.get("id"))
    df["payload_enter_date"] = pd.to_datetime(
        df["payload"].map(lambda x: x.get("entered_at")), utc=True
    )
    df["payload_exit_date"] = pd.to_datetime(
        df["payload"].map(lambda x: x.get("exited_at")), utc=True
    )
    df["states_expanded_enterTime"] = pd.to_datetime(df["states_expanded_enterTime"])
    df["flow_version"] = df["payload"].map(lambda x: x.get("flow_version"))
    df = df.sort_values("states_expanded_enterTime").drop_duplicates(
        subset=["pid"], keep="last"
    )[
        [
            "Style Code",
            "payload",
            "payload_enter_date",
            "payload_exit_date",
            "index",
            "flow_version",
            "exitTime",
        ]
    ]

    # filter out anything that never entered..
    df = df[df["payload_enter_date"].notnull()]
    for col in ["payload_enter_date", "payload_exit_date", "exitTime"]:
        df[col] = pd.to_datetime(df[col], utc=True)

    return df


def publish_apply_color_queue_changes_as_model(data, window_minutes=3000, plan=False):
    """
    for the queue model of observability we can can convert the bits
    pass the payloads
    """
    from res.observability.queues import Style, publish_queue_update

    # for testing - we normally have the data when we want to call this
    if data is None:
        data = get_apply_color_queue_changes(window_minutes=window_minutes)

    # contract is to pass the full result from changes and pluck out the payload
    x = pd.DataFrame([d for d in data["payload"]])

    # x[x['contracts_failing_list'].notnull()]
    x["entered_at"] = pd.to_datetime(x["queue_entered_at"])
    x["exited_at"] = pd.to_datetime(x["queue_exited_at"])
    x["scheduled_exit_at"] = x["entered_at"] + pd.Timedelta(1)
    x["airtable_link"] = x["request_id"].map(
        lambda x: f"https://airtable.com/appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/{x}"
    )
    x["is_custom_placement"] = x["metadata"].map(
        lambda x: x.get("color_type") == "Custom"
    )
    x["name"] = x["style_code"]
    x["body_code"] = x["style_code"].map(lambda x: x.split(" ")[0].strip())
    x["material_code"] = x["style_code"].map(lambda x: x.split(" ")[1].strip())
    x["color_code"] = x["style_code"].map(lambda x: x.split(" ")[2].strip())
    x["owner"] = x["metadata"].map(lambda x: x.get("assignee_email"))

    def make_refs(row):
        return {
            "has_body": row["body_code"],
            "has_material": row["material_code"],
        }

    x["refs"] = x.apply(make_refs, axis=1)
    x = res.utils.dataframes.replace_nan_with_none(x)

    # in this case we just current state per style
    x = x.sort_values("queue_entered_at").drop_duplicates(
        subset=["style_code"], keep="last"
    )

    records = [Style(**d) for d in x.to_dict("records")]
    if plan:
        return records
    res.utils.logger.info(f"Publishing {len(records)} records")
    publish_queue_update(records)


def get_body_queue_changes(window_minutes, body_version=None):
    """
    extract times series from body queue


    dfe[dfe['flow_version']==1]

    """

    from res.flows.meta.ONE.contract.ContractVariables import ContractVariables

    dfc = ContractVariables.load(base_id="appa7Sw0ML47cA8D1")

    def parse_list(l, as_ids=False):
        try:
            # tool for parsing list of contracts and also looking up their ids. its a bit sad
            if isinstance(l, str):
                l = l.split(",")
            if isinstance(l, list):
                # confusing but i think we have a type that has lists but the value is a csl
                l = [a for a in l if not pd.isnull(a)]
                if len(l) == 1:
                    l = l[0].split(",")
            if l is None:
                return []
            l = [a.strip() for a in l if not pd.isnull(a) and a != "None"]
            if as_ids:
                l = list(dfc[dfc["name"].isin(l)]["record_id"].unique())
            return l
        except:
            return []

    owner_fields = [
        "Owner_Apply Change Function Recipe",
        "Owner_Review Applied Change Functions",
    ]
    metadata_fields = [
        "Status",
        "Input Sample Received",
        "Modification Request Type (RUN v DEV)",
        "Active Moments Priority Rankings",
        "Date Created",
        "Body Design Notes Onboarding Form",
        "Body ONE Ready Request Type",
        "Flag for Review",
    ]

    def treat_field(k, v):
        if k in owner_fields:
            try:
                return v.get("email")
            except:
                return v
        if k == "Active Moments Priority Rankings" and isinstance(v, list):
            # take the first one

            if isinstance(v, list) and len(v):
                v = v[0]
                if pd.notnull(v):
                    v = v.split(",")
                else:
                    v = []
            else:
                v = []
            v = sorted([int(float(s)) for s in v if isinstance(s, str)])
            return v[0] if len(v) else None
        return str(v)

    def make_payloads(row):
        v = row.get("flow_version")
        l = [
            "Pipeline",
            "Review Brand's Input",
            "Generate Change Function Recipe",
            "Apply Change Function Recipe in 3D",
        ]
        state = row["states_expanded_state"]
        sub_status = row.get("states_expanded_statusesSnapshot") or {}
        sub_status = (
            {
                k.lower().replace("&", "").replace(" ", "_").replace("__", "_"): v
                for k, v in sub_status.items()
            }
            if pd.notnull(sub_status)
            else {}
        )
        metadata = {s: treat_field(s, row.get(s)) for s in metadata_fields}
        # for k, v in sub_status.items():
        #     metadata[k] = v

        try:
            ordinal, flow_version = -1, -1
            ordinal = int(row.get("states_expanded_flowOrder", -1) or -1)
            flow_version = int(row.get("states_expanded_flowVersion", 1) or -1)
        except:
            # this is just paranoia
            pass

        d = {
            "id": res.utils.uuid_str_from_dict(
                {
                    "record_id": row["record_id"],
                    "state": state,
                    "entered": row["states_expanded_enterTime"],
                    "exited": row["states_expanded_exitTime"],
                    "env": res.utils.env.RES_ENV,
                }
            ),
            "sub_statuses": sub_status,
            "flow_id": res.utils.uuid_str_from_dict({"flow": "body-queue"}),
            "request_id": row["record_id"],
            "body_code": row["Body Number"],
            "brand": row["Brand"],
            "owner": row["Owner_Current Status"],
            # must be an int - if null just make it zero
            "body_version": body_version or (1 + int(row["Active Body Version"])),
            "node": state,
            "contracts_failed_ids": parse_list(
                row["Body Meta One Response Contracts Failed"], as_ids=True
            ),
            "contracts_failed": parse_list(
                row["Body Meta One Response Contracts Failed"]
            ),
            "entered_at": row["states_expanded_enterTime"],
            "exited_at": row["states_expanded_exitTime"],
            "node_ordinal": ordinal,
            "flow_version": flow_version,
            "created_at": res.utils.dates.utc_now_iso_string(),  # row['Apply Color Flow Status Last Updated At'],
            "metadata": metadata,
            "last_updated_at": row["Last Updated At"],
            "scheduled_exit_at": row["Target Completion Date"],
            "request_created_at": row["__timestamp__"],
        }
        return d

    ######
    ##     LOAD RAW DATA
    ######
    res.utils.logger.info(f"Loading data for window: {window_minutes} minutes")
    fields = [
        "Body Number",
        "Brand",
        "Owner_Current Status",
        "Last Updated At",
        "Active Body Version",
        "Body ONE Ready Request Flow JSON",
        "Status Last Updated At",
        "Status Mirror",
        "Body Meta One Response Contracts Failed",
        "3DM1 Owner",
        "Flag Owner",
        "Target Completion Date",
    ]

    fields += [f for f in metadata_fields if f not in fields]

    tab = res.connectors.load("airtable")["appa7Sw0ML47cA8D1"]["tblrq1XfkbuElPh0l"]

    if window_minutes:
        df_raw = tab.updated_rows(
            minutes_ago=window_minutes,
            last_modified_field="Status Last Updated At",
            fields=fields,
        )
    else:
        res.utils.logger.info(f"As no window supplied, asking for everything")
        df_raw = tab.to_dataframe(fields=fields)

    res.utils.logger.info(f"Fetched {len(df_raw)} records from the body queue")

    for f in fields:
        if f not in df_raw.columns:
            df_raw[f] = None
    if len(df_raw) == 0:
        return pd.DataFrame()
    df_raw = df_raw[df_raw["Body ONE Ready Request Flow JSON"].notnull()].reset_index()
    if len(df_raw) == 0:
        return pd.DataFrame()

    for f in ["Body Number", "Active Body Version"]:
        df_raw[f] = df_raw[f].map(lambda x: x[0] if isinstance(x, list) else 0)

    df_raw["Active Body Version"] = df_raw["Active Body Version"].fillna(0)

    def _try_json(x):
        try:
            return json.loads(x)
        except:
            return {}

    df_raw["states"] = df_raw["Body ONE Ready Request Flow JSON"].map(_try_json)

    df_raw = res.utils.dataframes.replace_nan_with_none(df_raw)

    #####
    ##    determines the embedded states
    #####
    res.utils.logger.info(f"Data loaded - expanding states")
    states = pd.DataFrame([d for d in df_raw["states"]])
    selected_fields = [
        "Body Number",
        "Brand",
        "Owner_Current Status",
        "Body Meta One Response Contracts Failed",
        "record_id",
        "states_expanded",
        "Active Body Version",
        "index",
        "exitTime",
        "Last Updated At",
        "Target Completion Date",
        "__timestamp__",
    ]
    selected_fields += [
        f for f in metadata_fields if f not in selected_fields and f in df_raw.columns
    ]
    df = df_raw.join(
        states.explode("states")[["states", "exitTime"]],
        rsuffix="_expanded",
    )[selected_fields].reset_index()

    df = res.utils.dataframes.expand_column(df, "states_expanded")

    #

    ####
    ##    generate times series payload and some debugging metadata
    #####
    res.utils.logger.info(f"Generating payloads")

    df["payload"] = df.apply(make_payloads, axis=1)
    df["pid"] = df["payload"].map(lambda x: x.get("id"))
    df["payload_enter_date"] = pd.to_datetime(
        df["payload"].map(lambda x: x.get("entered_at")), utc=True
    )
    df["payload_exit_date"] = pd.to_datetime(
        df["payload"].map(lambda x: x.get("exited_at")), utc=True
    )
    df["payload_ordinal"] = pd.to_datetime(
        df["payload"].map(lambda x: x.get("ordinal")), utc=True
    )
    df["states_expanded_enterTime"] = pd.to_datetime(df["states_expanded_enterTime"])

    df = df.sort_values("states_expanded_enterTime").drop_duplicates(
        subset=["pid"], keep="last"
    )[
        [
            "payload",
            "payload_ordinal",
            "payload_enter_date",
            "payload_exit_date",
            "index",
            "exitTime",
        ]
    ]

    for col in ["payload_enter_date", "payload_exit_date", "exitTime"]:
        df[col] = pd.to_datetime(df[col], utc=True)

    # filter out anything that never entered
    df = df[df["payload_enter_date"].notnull()]

    df = res.utils.dataframes.replace_nan_with_none(df)

    return df


def publish_body_queue_changes_as_model(data, window_minutes=5000, plan=False):
    """
    Get the queue model from the payloads
    """
    from res.observability.queues import Body, publish_queue_update

    if data is None:
        data = get_body_queue_changes(window_minutes=window_minutes).reset_index()

    # contract is to pass the full result from changes and pluck out the payload
    data = pd.DataFrame([d for d in data["payload"]])

    """
    we could remove airtable from the mix here
    """
    airtable = res.connectors.load("airtable")
    res.utils.logger.info("refreshing brand lookup  ")
    lu = dict(
        airtable.get_table_data("appa7Sw0ML47cA8D1/tblqPtMB7IxGmzM5H")[
            ["Code", "Name"]
        ].values
    )

    def resolve_brand_name(x):
        return lu.get(x)

    """
    
    now map to the observability queue model
    """

    data = data.rename(
        columns={
            "contracts_failed": "contracts_failing_list",
            "Owner_Current Status": "owner",
        }
    )
    data["airtable_link"] = data["request_id"].map(
        lambda x: f"https://airtable.com/appa7Sw0ML47cA8D1/tblrq1XfkbuElPh0l/{x}"
    )
    # for our queue purposes
    data["entered_at"] = pd.to_datetime(data["request_created_at"], utc=True)
    data["scheduled_exit_at"] = pd.to_datetime(data["scheduled_exit_at"], utc=True)
    data["scheduled_exit_at"] = data["scheduled_exit_at"].fillna(
        data["entered_at"] + pd.Timedelta(14)
    )

    # for col in ["entered_at", "scheduled_exit_at"]:
    #     data[col] = data[col].dt.tz_convert(None)
    # the final queue
    data.loc[data["node"] == "Done", "exited_at"] = data["exited_at"]
    data.loc[data["node"] != "Done", "exited_at"] = None
    data["name"] = data["body_code"]
    data["owner"] = data["owner"].fillna(data["owner"])
    data["node_status"] = data["node"].map(
        lambda x: "Done" if x == "Done" else "Progressing"
    )

    data["brand_name"] = data["body_code"].map(
        lambda x: resolve_brand_name(str(x).split("-")[0])
    )
    data = res.utils.dataframes.replace_nan_with_none(data)

    # in this case we just current state per body
    data = data.sort_values("entered_at").drop_duplicates(
        subset=["body_code"], keep="last"
    )
    data["brand_name"] = data["brand_name"].fillna("UNKNOWN")
    records = [Body(**d) for d in data.to_dict("records")]
    if plan:
        return records
    res.utils.logger.info(f"Publishing {len(records)} records")
    publish_queue_update(records)


def _publish_events(df, topic, watermark, key, process):
    """
    send statds metrics for things in the queue

    df: data with time series
    topic: the actual client to the topic
    watermark: dates for filtering changes that are fetched (since last run time)
    """
    from res.utils.env import RES_ENV

    res.utils.logger.info(f"Publishing events since {watermark}")
    if watermark is None:
        watermark = res.utils.dates.utc_days_ago(5000)
    try:
        USE_KGATEWAY = True
        df["exited_in_window"] = False
        df["entered_in_window"] = False
        # if we exited since we last ran
        df.loc[(df["payload_exit_date"] > watermark), "exited_in_window"] = True
        # otherwise we entered since we last ran
        df.loc[
            (df["payload_enter_date"] > watermark) & (df["exited_in_window"] == False),
            "entered_in_window",
        ] = True

        # generate two types of event; those that entered in the time window and those that exited
        entries = df[df["entered_in_window"]]
        exits = df[df["exited_in_window"]]
        res.utils.logger.info(
            f"There were {len(entries)} entry events since {watermark}"
        )
        res.utils.logger.info(f"There were {len(exits)} exit events since {watermark}")

        record = None
        if RES_ENV == "production":
            for record in entries["payload"]:
                record["event_type"] = "entered"
                topic.publish(record, use_kgateway=USE_KGATEWAY, dedup_on_key="id")

            for record in exits["payload"]:
                record["event_type"] = "exited"
                topic.publish(record, use_kgateway=USE_KGATEWAY, dedup_on_key="id")
        else:
            res.utils.logger.info(f"Skipping publish for env {RES_ENV}")

        try:
            res.utils.logger.info("Publishing metrics")
            entries["state"] = entries["payload"].get("node")

            for e in entries["payload"]:
                res.utils.logger.metric_node_state_transition_incr(
                    f'{e["node"]}',
                    e[key],
                    status="Enter",
                    process=process,
                )
            exits["exits"] = exits["payload"].get("node")
            for e in exits["payload"]:
                res.utils.logger.metric_node_state_transition_incr(
                    f'{e["node"]}',
                    e[key],
                    status="Exit",
                    process=process,
                )
        except Exception as ex:
            res.utils.logger.warn(f"Failed to publish metrics {ex}")

    except Exception as ex:
        res.utils.logger.warn(
            f"Failed stats publish {traceback.format_exc()} - last record {record}"
        )


def read_expanded_costs():
    """
    utility to read and example costs data
    """
    hasura = res.connectors.load("hasura")

    def make_flat_cost_data(c):
        try:
            dff = pd.DataFrame(c)  # .set_index(['Item', 'Category'])
            dff["label"] = dff.apply(
                lambda row: f"{row['Item']} {row['Category']} {row['Unit']}", axis=1
            )
            dff = (
                dff[dff["label"].map(lambda x: "Materials Yards" not in x)]
                .set_index("label")[["Rate", "Quantity"]]
                .reset_index()
            )

            d = {}
            for record in dff.to_dict("record"):
                d.update(
                    {
                        f"{record['label']} Rate": record["Rate"],
                    }
                )

                d.update(
                    {
                        f"{record['label']} Quantity": record["Quantity"],
                    }
                )

            return d
        except:
            return {}

    Q = """query MyQuery {
      meta_style_sizes(where: {status: {_eq: "Active"}}) {
        costs
        metadata
        id
        size_yield
        size_code
        material_usage_statistics
      }
    }
    """
    data = pd.DataFrame(hasura.execute_with_kwargs(Q)["meta_style_sizes"])
    df = data.explode("material_usage_statistics").reset_index()
    stuff = df["material_usage_statistics"].map(
        lambda x: (
            {d: v for d, v in x.items() if not isinstance(v, dict)}
            if isinstance(x, dict)
            else None
        )
    )
    df = (
        df.join(pd.DataFrame([d or {} for d in stuff]))
        .drop("material_usage_statistics", 1)
        .drop("index", 1)
    )
    df = res.utils.dataframes.replace_nan_with_none(df)
    df["sku"] = df["metadata"].map(lambda x: x.get("sku"))
    df["Style Code"] = df["sku"].map(lambda x: f" ".join([i for i in x.split(" ")[:3]]))

    cost_ex = df["costs"].map(make_flat_cost_data)
    cs = pd.DataFrame([d for d in cost_ex])
    df = df.join(cs)
    # df.to_csv("/Users/sirsh/Downloads/style_costs_expanded.csv",index=None)
    return df


def load_order_history():
    Q = """query MyQuery {
      infraestructure_bridge_sku_one_counter {
        sku
        style_sku
        one_number
        order_number
        airtable_rid
        body_version
        id
        updated_at
        created_at
        cancelled_at
      }
    }
    """

    hasura = res.connectors.load("hasura")

    oned = pd.DataFrame(
        hasura.execute_with_kwargs(Q)["infraestructure_bridge_sku_one_counter"]
    )

    news = oned.groupby(["sku", "body_version"]).agg(
        {"one_number": [len, min], "created_at": [min, max], "updated_at": max}
    )  # .sort_values('one_number')
    news.columns = news.columns.to_flat_index()
    # news = news[news['one_number']==1]
    # news.sort_values('updated_at').tail(50)
    news.columns = [f"{t[0]}_{t[1]}" for t in news.columns]
    news = news.reset_index()
    news["body_code"] = news["sku"].map(lambda x: x.split(" ")[0].strip())
    news["size_code"] = news["sku"].map(lambda x: x.split(" ")[-1].strip())

    # we would ignore 0 versions here but we need to cast to an int
    news["body_version"] = news["body_version"].fillna(0).map(int)

    news["one_number_min"] = news["one_number_min"].map(int)
    news["cut_files"] = news.apply(
        lambda row: f"s3://meta-one-assets-prod/bodies/cut/{row['body_code'].replace('-','_').lower()}/v{row['body_version']}/{row['size_code']}".lower(),
        axis=1,
    )

    # correct body code missing hyphens also at source
    return news.sort_values("created_at_min")


def stamper_handler(event={}, context={}, date_str_iso=None):
    """
    process anything new today
    bit expensive to load the full order history each time but we can live with it

    #use this to bootstrap if we are short on body versions
    reload_body_versions_for_orders

    """
    res.utils.logger.info("loading the history of orders")
    data = load_order_history()
    res.utils.logger.info("process for current date")
    post_new_stampers_for_sku_on_date(data, date_str_iso=date_str_iso)
    return {}


def add_body_header(body_code, body_version, body_file_uploaded_at=None):
    M = """mutation add_body_header($version: numeric = "", $id: uuid = "", $body_file_uploaded_at: timestamptz = "", $body_code: String = "", $brand_code: String = "") {
        insert_meta_bodies(objects: {id: $id, brand_code: $brand_code, body_code: $body_code, version: $version, body_file_uploaded_at: $body_file_uploaded_at}, on_conflict: {constraint: bodies_pkey, update_columns: body_code}) {
            returning {
                id
                updated_at
                body_file_uploaded_at
            }
        }
        }

        """
    id = res.utils.uuid_str_from_dict(
        {
            "code": body_code,
            "version": int(float(body_version)),
            "profile": "default",
        }
    )
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        M,
        id=id,
        body_code=body_code,
        brand_code=body_code.split("-")[0],
        version=body_version,
        body_file_uploaded_at=body_file_uploaded_at,
    )
    return result


def update_body_file_updated_at(
    body_code, body_version, body_file_uploaded_at, contracts=None
):
    """
    TODO we have not implemented adding contracts on the header yet as we need to think about affect on style meta one

    """

    M = """mutation MyMutation($body_file_uploaded_at: timestamptz = "", $body_code: String = "", $body_version: numeric = "") {
        update_meta_bodies(where: {body_code: {_eq: $body_code}, version: {_eq: $body_version}}, _set: {body_file_uploaded_at: $body_file_uploaded_at}) {
            returning {
                id
                updated_at
                body_file_uploaded_at
            }
        }
        }
        """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        M,
        body_code=body_code,
        body_version=body_version,
        body_file_uploaded_at=body_file_uploaded_at,
    )

    if len(result["update_meta_bodies"]["returning"]) == 0:
        res.utils.logger.info(
            f"There is no body  {body_code} V{body_version} yet - going to insert it now"
        )
        return add_body_header(body_code, body_version, body_file_uploaded_at)
    return result


def post_new_stampers_for_sku_on_date(data, date_str_iso=None):
    """
    may be necessary to retro process some stuff e.g. missing body files
    run this every couple of hoursas things enter make

    """
    # default to today

    date_str_iso = date_str_iso or res.utils.dates.utc_now_iso_string()[:10]

    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")
    # for each sku: get the path for all the files we need
    tab = airtable["apps71RVe4YgfOO8q"]["tblR8wj82BfxXM43P"]
    keys = tab.get_key_record_lookup()
    df = data[data["created_at_min"].map(str).map(lambda x: date_str_iso in x)]

    res.utils.logger.info(
        f"After filtering by '{date_str_iso}' there are {len(df)} records"
    )

    # warning - this is a reason it will not be updated
    df = df[df["body_version"].notnull()]
    df["body_version"] = df["body_version"].map(int)

    for record in df.to_dict("records"):
        uri = record["cut_files"]
        files = list(s3.ls(uri))

        files_root = "/".join([f for f in uri.split("/")[:-1]])
        all_files = list(s3.ls(files_root))

        key = f"{record['sku']} V{record['body_version']}"
        d = {
            "Name": key,
            "Sku": record["sku"],
            "Body Version": record["body_version"],
            "One Number": int(record["one_number_min"]),
            "All Size Stampers": [
                {"url": s3.generate_presigned_url(f)}
                for f in all_files
                if "_all_sizes" in f
            ],
            "Stamper": [
                {"url": s3.generate_presigned_url(f)}
                for f in files
                if "/stamper_" in f.lower()
            ],
            "Fuse": [
                {"url": s3.generate_presigned_url(f)}
                for f in files
                if "/fuse_" in f.lower()
            ],
            "Updated At": record["created_at_min"],
        }

        if key in keys:
            d["record_id"] = keys[key]

        tab.update_record(d)

    res.utils.logger.info(f"updated some stuff")


def _get_bm_style_info():
    """
    Loads the style data with anough context to determine what the BMS id would be
    this can be used to then check what we have saved already and we can look into requesting new stuff
    """
    hasura = res.connectors.load("hasura")
    Q = """query MyQuery {
    meta_style_sizes(where: {status: {_eq: "Active"}}) {
        costs
        material_usage_statistics
        id
        size_code
        style_id
        updated_at
        metadata
        style{
            metadata
            piece_material_hash
        }
    }
    }

    """

    data = pd.DataFrame(hasura.execute_with_kwargs(Q)["meta_style_sizes"])

    def extract_sewing_time(d):
        try:
            d = [i for i in d if i["Item"] == "Sew"][0]
            return d["Quantity"]
        except:
            return None

    def extract_trim_costs(d):
        try:
            d = [i for i in d if i["Item"] == "Trim"][0]
            return d["Rate"]  # odd
        except:
            return None

    data["sku"] = data["metadata"].map(lambda x: x.get("sku"))
    data["print_type"] = data["style"].map(
        lambda x: x.get("metadata", {}).get("print_type")
    )
    data["style_sku"] = data["sku"].map(
        lambda x: " ".join([s for s in x.split(" ")[:3]])
    )

    data["sewing_time"] = data["costs"].map(extract_sewing_time)
    data["trim_costs"] = data["costs"].map(extract_trim_costs)
    data["body_code"] = data["style_sku"].map(lambda x: x.split(" ")[0])
    data["material_code"] = data["style_sku"].map(lambda x: x.split(" ")[1])
    data["piece_material_hash"] = data["style"].map(
        lambda x: x.get("piece_material_hash")
    )
    data["bms_id"] = data.apply(
        lambda row: res.utils.uuid_str_from_dict(
            {"style_hash": row["piece_material_hash"], "size_code": row["size_code"]}
        ),
        axis=1,
    )
    data["BM"] = data.apply(lambda x: f"{x['body_code']} {x['material_code']}", axis=1)
    data["has_new_costing"] = (
        data["material_usage_statistics"]
        .map(
            lambda x: (
                x[0].get("height_nest_yds_buffered") is not None
                if isinstance(x, list) and len(x)
                else False
            )
        )
        .map(int)
    )

    # for payload
    data["reference_style_size_id"] = data["id"]
    data["created_at"] = res.utils.dates.utc_now_iso_string()
    data["style_pieces_hash"] = data["piece_material_hash"]
    # body version not know always unless we take it from the style metadata or the first piece name
    return data


def extract_outlines_and_versions(uri, skip_outline=False):
    """
    reads file versions and outlines for saved images
    """
    from glob import glob
    from res.media.images.outlines import get_piece_outline
    from tqdm import tqdm

    s3 = res.connectors.load("s3")
    stuff = s3.get_files_with_versions(uri)
    stuff = [f for f in stuff if ".png" in f["uri"]]
    if not skip_outline:
        for f in tqdm(stuff):
            f["outline"] = get_piece_outline(s3.read(f["uri"]))

    return stuff


def save_outlines_and_versions_for_meta_one(style_sku, plan=False, hasura=None):
    """
    process an entire BMC by updating the image file metadata and outlines
    """

    Q = """query get_style_by_sku_and_size($metadata_sku: jsonb = "" ) {
          meta_styles(where:  {metadata: {_contains: $metadata_sku}}) {
              style_sizes{
                style_size_pieces(where: {ended_at: {_is_null: true}}) {
                  piece {
                    id
                    metadata
                    base_image_uri 
                  }
               }
             }
          }
        }
        """

    # add retries

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(2))
    def update(d):
        M = """mutation MyMutation($id: uuid = "", $metadata: jsonb = "", $base_image_outline: jsonb = "", $base_image_s3_file_version_id: String = "") {
          update_meta_pieces(where: {id: {_eq: $id}}, _set: {base_image_s3_file_version_id: $base_image_s3_file_version_id, base_image_outline: $base_image_outline, metadata: $metadata}) {
            returning {
              id
            }
          }
        }
        """
        return hasura.execute_with_kwargs(
            M,
            id=d["id"],
            metadata=d["metadata"],
            base_image_outline=str(d["base_image_outline"]),
            base_image_s3_file_version_id=d["base_image_s3_file_version_id"],
        )

    hasura = hasura or res.connectors.load("hasura")
    data = hasura.execute_with_kwargs(Q, metadata_sku={"sku": style_sku})["meta_styles"]
    all_pieces = []
    if len(data):
        data = data[0]["style_sizes"]
        for s in data:
            all_pieces += [p["piece"] for p in s["style_size_pieces"]]

    data = all_pieces

    uri = data[0]["base_image_uri"]

    prefix = "/".join(uri.split("/")[:7])
    res.utils.logger.info("fetching versions for prefix")

    mapped = extract_outlines_and_versions(prefix)
    mapped = {m["uri"]: m for m in mapped}
    for d in data:
        key = d["base_image_uri"]
        if key in mapped:
            d["base_image_s3_file_version_id"] = mapped[key]["version_id"]
            d["base_image_outline"] = mapped[key]["outline"]
            d["metadata"]["base_image_saved_at"] = mapped[key]["last_modified"]

            if not plan:
                update(d)

    return data


def _get_recent_kafka_ppp_requests():
    """
    get recent requests for testing with - these are PPP requests in prod and we can use these to relay
    we filter by stuff that has the SKU in it as others are not fully supported
    """
    snowflake = res.connectors.load("snowflake")
    request_q = snowflake.execute(
        f"""SELECT * FROM IAMCURIOUS_PRODUCTION.PREP_PIECES_REQUESTS_QUEUE 
                                       WHERE parse_json(RECORD_CONTENT):sku::string IS NOT NULL
      """
    )

    request_q["rc"] = request_q["RECORD_CONTENT"].map(json.loads)
    request_q = request_q.drop_duplicates("id")

    request_q["sku"] = request_q["rc"].map(lambda x: x.get("sku"))
    request_q = request_q.drop_duplicates(subset=["sku"])

    def mod(x):
        x["flow"] = "cache_only"
        return x

    request_q["rc"] = request_q["rc"].map(lambda x: mod(x))

    return request_q


def save_piece_image_outline(id, base_image_outline):
    q = """
    mutation update_piece_outline($id: uuid = "", $base_image_outline: jsonb = "") {
        update_meta_pieces(where: {id: {_eq: $id}}, _set: {base_image_outline: $base_image_outline}) {
            returning {
            id
            }
        }
        }

    """

    return res.connectors.load("hasura").execute_with_kwargs(
        q, id=id, base_image_outline=base_image_outline
    )


@retry(wait=wait_fixed(0.1), stop=stop_after_attempt(2), reraise=True)
def get_style_id_lookup(specific_style_code=None, hasura=None):
    from res.flows.dxa.styles.queries import get_style_header

    Qstyles = """query MyQuery {
      meta_styles {
        metadata
        id
        name
      }
    }
    """
    if specific_style_code:
        style = get_style_header(sku=specific_style_code)
        sid = style["meta_style_sizes"][0]["style"]["id"]
        return {specific_style_code: sid}

    hasura = hasura or res.connectors.load("hasura")
    styles = pd.DataFrame(hasura.execute_with_kwargs(Qstyles)["meta_styles"])
    styles["sku"] = styles["metadata"].map(lambda x: x.get("sku"))
    lu_styles = dict(styles[["sku", "id"]].values)
    return lu_styles


@retry(wait=wait_fixed(0.1), stop=stop_after_attempt(2), reraise=True)
def get_latest_style_pieces_by_id(
    style_id, check_specific_body_version=None, hasura=None
):
    Q = """query MyQuery($sid: uuid = "") {
        meta_styles(where: {id: {_eq: $sid}}) {
            style_sizes(where: {style_size_pieces: {ended_at: {_is_null: true}}}) {
                style_size_pieces {
                    piece {
                        id
                        base_image_outline
                        base_image_uri
                        material_code
                    }
              }
            }
        }
        }
        """

    hasura = hasura or res.connectors.load("hasura")
    a = (
        pd.DataFrame(
            hasura.execute_with_kwargs(Q, sid=style_id)["meta_styles"][0]["style_sizes"]
        )
        .explode("style_size_pieces")
        .reset_index(drop=True)
    )
    a = res.utils.dataframes.expand_column_drop(a, "style_size_pieces", alias="pieces")
    a = res.utils.dataframes.expand_column_drop(
        a, "pieces_piece", alias="pieces"
    ).rename(
        columns={
            "pieces_id": "id",
            "pieces_base_image_outline": "base_image_outline",
            "pieces_base_image_uri": "base_image_uri",
            "pieces_material_code": "material_code",
        }
    )
    a = a[a["base_image_uri"].notnull()].reset_index(drop=True)
    a["piece_key"] = a["base_image_uri"].map(lambda x: x.split("/")[-1].split(".")[0])
    a["size_code"] = a["base_image_uri"].map(
        lambda x: x.split("/pieces")[0].split("/")[-1].upper()
    )
    a["body_piece_key"] = a.apply(
        lambda row: f"{row['piece_key']}_{row['size_code']}", axis=1
    )
    # now only keep the last version that we have
    a["v"] = (
        a["body_piece_key"].map(lambda x: x.split("-")[2].replace("V", "")).map(int)
    )

    res.utils.logger.debug(f"found {len(a)} pieces in {len(a['v'].unique())} versions")

    a["pnov"] = a["body_piece_key"].map(
        lambda x: "-".join(x.split("-")[:2] + x.split("-")[3:])
    )
    if check_specific_body_version:
        # max check the latest version but accept the best thing after that
        a = a[a["v"] <= check_specific_body_version]

    if len(a):
        a = a.sort_values("v").drop_duplicates(subset="pnov", keep="last")
        # ^ this is actually not enough if we deleted a piece that is still in the v- / so we should only keep pieces in the latest
        # the below is sufficient but keeping the note THIS CASE IS TESTING FOR DELETED PIECES BETWEEN VERSIONS LA-6007 LSG19 MARKIK ZZZ16 v1->v2 when testing against v3
        a = a[a["v"] == a["v"].max()]
    res.utils.logger.debug(f"rem {len(a)} pieces in {len(a['v'].unique())} versions")

    return a


@retry(wait=wait_fixed(0.1), stop=stop_after_attempt(2), reraise=True)
def get_latest_style_pieces(
    style_id, cache_if_missing=False, check_specific_body_version=None
):
    """
    Get all the latest body pieces dropping older version for each style_code e.g. 'JR-3043 CTFLN PRTTPD'

    """

    from res.media.images.outlines import get_piece_outline, get_piece_outline_dataframe
    from res.media.images.geometry import to_geojson
    import numpy as np

    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")

    retrieved = False
    cache_attempted = False
    df = None
    while not retrieved:
        df = get_latest_style_pieces_by_id(
            style_id, check_specific_body_version=check_specific_body_version
        )

        without_outlines = df[
            ((df["base_image_outline"] == {}) | (df["base_image_outline"].isnull()))
            & (df["base_image_uri"].notnull())
        ]
        if cache_if_missing and len(without_outlines) > 0 and not cache_attempted:
            res.utils.logger.info(f"Caching {len(without_outlines)} outlines")
            for record in without_outlines.to_dict("records"):
                im = s3.read(record["base_image_uri"])
                # change of strategy is just to save the uri to say that we have cached it in s3
                uri = (
                    record["base_image_uri"]
                    .replace("/pieces/", "/outlines/")
                    .replace(".png", ".feather")
                )
                outline = get_piece_outline_dataframe(np.asarray(im))
                res.connectors.load("s3").write(uri, outline)
                id = record["id"]
                id = save_piece_image_outline(id=id, base_image_outline=uri)

                res.utils.logger.debug(
                    f"Updated piece outline for image {record['base_image_uri']}"
                )
            cache_attempted = True
        else:
            retrieved = True

    return df


def get_is_cost_api_enabled_for_brand(brand_code):
    hasura = res.connectors.load("hasura")
    data = hasura.execute_with_kwargs(GET_IS_COST_API_ENABLED, brand_code=brand_code)

    if data and data.get("meta_brands"):
        return data["meta_brands"]
    else:
        return []


@retry(wait=wait_fixed(0.1), stop=stop_after_attempt(2), reraise=True)
def fetch_style_bms_data(sku):
    """
    resilient helper to make the data because we cannot join because of a stupid design
    """
    sku = fix_sku(sku)

    hasura = res.connectors.load("hasura")
    size_code = sku.split(" ")[-1]
    sku = style_sku(sku)

    data = hasura.execute_with_kwargs(GET_SKU_METADATA, metadata_sku={"sku": sku})[
        "meta_styles"
    ]

    # get the one last updated
    if len(data):
        data = pd.DataFrame(data).sort_values("modified_at").tail(1).to_dict("records")
    if len(data):
        data = data[0]
        id = res.utils.uuid_str_from_dict(
            {"style_hash": data["piece_material_hash"], "size_code": size_code}
        )
        data["sized_hash"] = id
        res.utils.logger.debug(
            f"Hash for BMS is {id=}, {size_code=}, {data['piece_material_hash']=}"
        )
        bms_data = hasura.execute_with_kwargs(GET_BMS_COSTS_BY_ID, id=id)[
            "meta_body_pieces_with_material_by_pk"
        ]
        if bms_data:
            bms_data["style_metadata"] = data["metadata"]
            body_data = hasura.execute_with_kwargs(
                GET_BODY_INFO_BY_VERSION,
                body_code=bms_data["body_code"],
                version=bms_data["body_version"],
            )["meta_bodies"]
            bms_data["body_piece_count"] = body_data[0]["body_piece_count_checksum"]

            return bms_data


def get_bms_style_costs_for_sku(sku):
    """
    loads the BMS level cost data associated with the sku

    """
    from res.flows.finance.queries import (
        get_material_rates_by_material_codes,
        get_node_rates,
        get_sew_labor_cost,
    )
    from stringcase import titlecase

    sku = fix_sku(sku)

    material_code = sku.split(" ")[1]
    sku = style_sku(sku)

    """
    Now that we have a helper we can load the costs data from the database 
    """

    data = fetch_style_bms_data(sku)
    if not data:
        res.utils.logger.info("unable to fetch data for this styles BMS")
        return

    """
    parse some things
    """
    metadata = data["style_metadata"]
    costs = pd.DataFrame(data["nesting_statistics"])
    material_codes = list(costs["material_code"].unique())
    number_of_pieces = data["body_piece_count"]  # costs["piece_count"].sum()

    material_props = get_material_rates_by_material_codes(material_codes)
    node_rates = get_node_rates(expand=True)

    """ 
    switch on the style type as we have different costs
    """
    style_type = "woven"
    print_type = (
        "directional" if metadata.get("print_type") == "Directional" else "placement"
    )
    height_statistic = "height_nest_yds_raw"
    if metadata.get("is_unstable_material"):
        height_statistic = "height_nest_yds_block_buffered"
        style_type = "knit"
        if not metadata.get("is_directional"):
            height_statistic = "height_nest_yds_buffered"

    res.utils.logger.info(f"Using height {height_statistic}")

    """
    now build the table
    """
    # return costs

    # missing cost error here - check database that these all exist or make them
    costs = pd.DataFrame(costs)[["material_code", height_statistic]]

    material_costs = pd.merge(costs, material_props, on="material_code")
    material_costs["quantity"] = costs[height_statistic]

    # at sew we based on on the sewing effort estimate
    node_rates.loc[node_rates["item"] == "Sew", "quantity"] = data.get("sewing_time", 0)
    sew_labor_rate = get_sew_labor_cost(material_code)
    if sew_labor_rate:
        node_rates.loc[node_rates["item"] == "Sew", "rate"] = float(
            sew_labor_rate.cost_per_minute
        )
    # at cut we base it on the number of pieces
    node_rates.loc[node_rates["item"] == "Cut", "quantity"] = number_of_pieces
    # the size yield (nest height) which is the sum of the printing is the labor rate factor to use
    node_rates.loc[node_rates["item"] == "Print", "quantity"] = material_costs[
        "quantity"
    ].sum()

    new_rows = [
        {
            "item": "Trim",
            "category": "Materials",
            "rate": data.get("trim_costs", 0),
            "quantity": 1,
            "unit": "bundle",
        }
    ]
    node_rates = pd.concat(
        [node_rates, pd.DataFrame(new_rows), material_costs]
    ).reset_index(drop=True)

    # add the regular costs as the quantity used for overheads
    node_rates.loc[node_rates["item"] == "Overhead", "quantity"] = (
        node_rates["rate"] * node_rates["quantity"]
    ).sum()

    # now compute total costs
    node_rates["cost"] = node_rates["rate"] * node_rates["quantity"]
    node_rates.columns = [titlecase(c) for c in node_rates.columns]

    res.utils.logger.info(
        f"Style {sku} with print type {print_type} in material type {style_type} has a total cost {node_rates['Cost'].sum()}"
    )
    return (
        node_rates[["Item", "Category", "Rate", "Unit", "Quantity", "Cost"]]
        .fillna(0)
        .round(3)
        .to_dict("records")
    )


####
#   adding these as hack; material walls need a pin placement solution so for now need to hard code these skus for an action
####
TEMP = [
    "CC-9011 CTW70 MINTEW",
    "CC-9011 CTW70 GRENLA",
    "CC-9011 CTW70 SALVRW",
    "CC-9011 CTW70 MINTPR",
    "CC-9011 CTW70 NATUQX",
    "CC-9011 CTW70 CHILFO",
    "CC-9011 CTW70 LIGHAZ",
    "CC-9011 CTW70 BLACVB",
    "CC-9011 CTW70 NEONHQ",
    "CC-9011 CTW70 LIGHMO",
    "CC-9011 CTW70 NEONZK",
    "CC-9011 CTW70 CHILXM",
    "CC-9011 LSC19 MINTEW",
    "CC-9011 LSC19 BLACVB",
    "CC-9011 LSC19 GRENLA",
    "CC-9011 LSC19 CHILXM",
    "CC-9011 LSC19 LIGHAZ",
    "CC-9011 LSC19 LIGHMO",
    "CC-9011 LSC19 SALVRW",
    "CC-9011 LSC19 NEONZK",
    "CC-9011 LSC19 NATUQX",
    "CC-9011 LSC19 CHILFO",
    "CC-9011 LSC19 NEONHQ",
    "CC-9011 CLTWL NANTIX",
    "CC-9011 CLTWL BEIGBC",
    "CC-9011 CLTWL TOILKI",
    "CC-9011 FBTRY SILHFS",
    "CC-9011 CLTWL DENIZE",
    "CC-9011 FBTRY TOILKI",
    "CC-9011 COMCT BIGSBS",
    "CC-9011 CHRST LIGHCH",
    "CC-9011 CTJ95 SILHFS",
]
