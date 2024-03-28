import res
import uuid
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from res.utils.fp import timeout
import numpy as np
import typing
from datetime import datetime, timedelta


UPDATE_ORDER_MAP = """mutation upsert_order_sku_map($objects: [infraestructure_bridge_order_skus_to_ones_insert_input!] = {}) {
  insert_infraestructure_bridge_order_skus_to_ones(objects: $objects, on_conflict: {constraint: bridge_order_skus_to_ones_pkey, 
    update_columns: [sku_map,order_size]}) {
    returning {
      id
      order_size
      sku_map
      order_key
    }
  }
}
"""

# function below inlines
# GET_ORDER_MAP_BY_ORDER_NAME = """query MyQuery($order_name: String = "") {
#   infraestructure_bridge_order_skus_to_ones(where: {order_key: {_eq: $order_name}}) {
#     sku_map
#     order_size
#     order_key
#     id
#     created_at
#     updated_at
#   }
# }
# """

GET_ONE_PIECES_BY_OID = """query getit($oid: uuid = "") {
  make_one_pieces(where: {one_order: {oid: {_eq: $oid}}}) {
    one_order {
      one_code
      one_number
      order_number
      sku
    }
    deleted_at
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

GET_ONE_PIECES_BY_ORDER_ID = """query getit($oid: uuid = "") {
  make_one_pieces(where:   {one_order_id: {_eq: $oid}}) {
    one_order {
      id
      order_item_id
      order_item_rank

      one_code
      one_number
      order_number
      sku
    }
    deleted_at
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

GET_ONE_PIECES_BY_ONE_NUMBER = """query get_order_by_one_number($one_number: Int = 10) {
  make_one_pieces(where: {one_order: {one_number: {_eq: $one_number}}, deleted_at: {_is_null: true}}) {
    one_order {
      one_code
      one_number
      order_number
      sku
      id
      created_at
      updated_at
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

GET_ONE_PIECES_BY_ONE_CODE = """query get_order_by_one_number($one_code: String = "") {
  make_one_pieces(where: {one_order: {one_code: {_eq: $one_code}}, deleted_at: {_is_null: true}}) {
    one_order {
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


GET_ONE_PIECES_BY_ORDER_NUMBER = """query get_order_by_one_number($order_number: String = "") {
  make_one_pieces(where: {one_order: {order_number: {_eq: $order_number}}, deleted_at: {_is_null: true}}) {
    one_order {
      one_code
      one_number
      order_number
      sku
      id
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


GET_MAKE_ORDER_WITH_PIECES_BY_ORDER_NAME = """query make_order_by_order_name($order_number: String = "") {
  make_one_orders(where: {order_number: {_eq: $order_number}}) {
    one_number
    one_code
    oid
    sku
    id
    order_item_id
    order_item_rank
    deleted_at
    status
    order_number
    created_at
    one_pieces{
      id
      oid
      code
    }
  }
}

"""


GET_MAKE_ORDER_BY_LABEL = """query MyQuery($one_label_code: String = "") {
  make_one_labels(where: {code: {_eq: $one_label_code}}) {
    code
    one_order {
      id
      oid
      make_instance
      one_code
      sku
      style_size {
        style {
          body_code
          id
          name
        }
      }
    }
  }
}
"""

# this is tricky - the sku that was ordered is not necessarily the sku we can make
# assume the flow can use the latest sku or refresh the make order at time of processing
# order_line_item: {sku: {_eq: $sku}
GET_FULFILLMENT_ITEM_FOR_PRODUCT_SKU = """query get_order_fulfillment_item_for_sku($order_number: String = "", $sku: String = "", $size_like: String = "") {
  make_one_orders(where: {order_number: {_eq: $order_number}, order_line_item: {product: {sku: {_eq: $sku}}, sku: {_like: $size_like}}, status: {_neq: "CANCELLED"}}) {
    id
    oid
    one_code
    one_number
    sku
    order_number
    order_item_id
    order_item_rank
    metadata
    style_size_id
    one_pieces {
      id
      make_instance
      code
      oid
      node_id
      piece {
        id
        base_image_uri
        body_piece {
          piece_key
        }
      }
    }
    order_line_item {
      sku
      product {
        sku
      }
    }
    make_instance
  }
}

"""

GET_FULFILLMENT_ITEM_FOR_ORDERED_SKU = """query get_order_fulfillment_item_for_sku($order_number: String = "", $sku: String = "") {
  make_one_orders(where: {order_number: {_eq: $order_number}, order_line_item:  {sku: {_eq: $sku}}, status: {_neq: "CANCELLED"}}) {
    id
    oid
    one_code
    one_number
    sku
    order_number
    order_item_id
    order_item_rank
    metadata
    style_size_id
    one_pieces {
      id
      make_instance
      code
      oid
      node_id
      piece {
        id
        base_image_uri
        body_piece {
          piece_key
        }
      }
    }
    order_line_item {
      sku
      product {
        sku
      }
    }
    make_instance
  }
}

"""

GET_FULFILLMENT_ITEM_FOR_ONE = """query get_order_fulfillment_item_for_sku( $one_number: Int = 0 ) {
  make_one_orders(where: {one_number: {_eq: $one_number}  }) {
    id
    oid
    one_code
    one_number
    sku
    order_number
    order_item_id
    order_item_rank
    metadata  
    style_size_id
    one_pieces {
      id
      make_instance
      code
      oid
      node_id
      piece {
        id
        base_image_uri
        body_piece {
          piece_key
        }
      }
    }
    order_line_item{
      sku
      product {
        sku
      }
    }
    make_instance
  }
}
"""


GET_HEALING_PIECES_SINCE_DATE_SQL = """
select a.*, b.created_at as inspection_at, fail as is_failed
 from make.one_piece_healing_requests a 
 left join make.roll_inspection b on b.piece_id = a.print_asset_piece_id
  where a.created_at > %s
"""


def get_healing_pieces_since(since_date):
    """
    the date window is a simple thing but will break back too much data for stray ones
    a list of ones would be better except today we do not not store the one on the table
    """
    postgres = res.connectors.load("postgres")
    data = postgres.run_query(GET_HEALING_PIECES_SINCE_DATE_SQL, (since_date,))

    data["one_number"] = data["metadata"].map(lambda x: x.get("one_number"))
    data["piece_code"] = data["metadata"].map(lambda x: x.get("piece_code"))
    data["one_number"] = data["one_number"].map(int)
    data = (
        data.groupby(
            ["one_number", "piece_code", "created_at", "request_id", "piece_id"]
        )
        .agg({"inspection_at": max, "is_failed": set})
        .reset_index()
    )
    data.loc[data["is_failed"] == {None}, "is_failed"] = None
    return data


def fix_sku(s):
    if s[2] != "-":
        return f"{s[:2]}-{s[2:]}"
    return s


def add_days(start_date, days):
    return start_date + timedelta(days=days)


def get_expected_delivery_date(one_details: dict):
    EXPECTED_DAYS_UNTIL_DELIVERY_BEFORE_ENTERING_MAKE = 9
    EXPECTED_DAYS_UNTIL_DELIVERY_FROM_NODE = {
        "DxA Graphics": 9,
        "ONE": 9,
        "Material Node": 8,
        "Assemble": 7,
        "Sew": 4,
        "Warehouse": 3,
        "Done": 3,
    }

    if (
        one_details.get("make_node") == "Canceled"
        or one_details.get("make_node") == "Cancelled"
    ):
        return None

    expected_days_until_delivery_from_node = EXPECTED_DAYS_UNTIL_DELIVERY_FROM_NODE.get(
        one_details.get("make_node"), None
    )

    if expected_days_until_delivery_from_node:
        expected_delivery_date = add_days(
            datetime.now(), expected_days_until_delivery_from_node
        )
    else:
        expected_delivery_date = add_days(
            datetime.now(), EXPECTED_DAYS_UNTIL_DELIVERY_BEFORE_ENTERING_MAKE
        )

    return expected_delivery_date


def left_dxa_recent_days(digital_assets_ready_date, numDays):
    if digital_assets_ready_date:
        dxaDate = res.utils.dates.coerce_to_full_datetime(digital_assets_ready_date)
        if datetime.now() - dxaDate < timedelta(days=numDays):
            return True
    return False


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_one_make_status_query(one_number):
    """
    Get node and expected delivery date of one
    """
    postgres = res.connectors.load("postgres")

    query = """SELECT one_number, make_node, assembly_node, order_number, sku, fulfilled_at as fulfilled_date, dxa_assets_ready_at as digital_assets_ready_date 
    FROM infraestructure.bridge_sku_one_counter 
  WHERE one_number = %s """
    dfOnePg = postgres.run_query(query, (one_number,))
    one_details = dict()
    expected_date = None
    if not dfOnePg.empty:
        one_details = dfOnePg.to_dict("records")[0]
        if one_details.get("fulfilled_date"):
            one_details["fulfilled_date"] = res.utils.dates.coerce_to_full_datetime(
                one_details.get("fulfilled_date")
            )
            one_details["make_node"] = "Fulfilled"
        if one_details.get("make_node") == "Assemble":
            one_details[
                "make_node"
            ] = f"{one_details.get('make_node') }  / {one_details.get('assembly_node')}"

        recent_order = left_dxa_recent_days(
            one_details.get("digital_assets_ready_date"), 90
        )

        if recent_order and not one_details.get("fulfilled_date"):
            expected_date = get_expected_delivery_date(one_details)
        one_details["expected_delivery_date"] = expected_date

    return one_details


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_order_make_status_query(one_number, brand_code=None):
    """
    Get node and expected delivery date of an order
    """
    postgres = res.connectors.load("postgres")

    query = """SELECT one_number, make_node, assembly_node, order_number, sku, fulfilled_at as fulfilled_date, dxa_assets_ready_at as digital_assets_ready_date 
    FROM infraestructure.bridge_sku_one_counter 
    WHERE order_number = %s """
    qdata = (one_number,)
    # if brand_code:
    #     query += f""

    dfOnePg = postgres.run_query(query, data=qdata)

    expected_date = None

    if not dfOnePg.empty:
        one_details_on_order = dfOnePg.to_dict("records")
        for one_details in one_details_on_order:
            if one_details.get("fulfilled_date"):
                one_details["fulfilled_date"] = res.utils.dates.coerce_to_full_datetime(
                    one_details.get("fulfilled_date")
                )
            if not one_details.get("fulfilled_date"):
                expected_date = get_expected_delivery_date(one_details)
            one_details["expected_delivery_date"] = expected_date

            if one_details.get("make_node") == "Assemble":
                one_details[
                    "make_node"
                ] = f"{one_details.get('make_node') }  / {one_details.get('assembly_node')}"

    return one_details_on_order


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_one_piece_status(one_number, flatten=False):
    """
    This method is similar to some of the others but may diverge
    this should help understanding if the pieces are flowing
    """
    # from the meta piece take
    # "key",
    # "uri",
    # "piece_set_size",
    # "piece_ordinal",
    # "normed_size",
    # "body_piece_type",
    # "style_sizes_size_code",
    # "garment_piece_material_code",

    Q = """
    
      query get_one_order_piece_status($one_number: Int = 10) {
        make_one_orders(where: {one_number: {_eq: $one_number}}) {
          one_code
          oid
          id
          metadata
          one_number
          order_number
          sku
          status
          created_at
          updated_at
          one_pieces {
            code
            contracts_failed
            make_instance
            node {
              name
            }
            observed_at
            created_at
            updated_at
            oid
            id
            piece {
              color_code
              material_code
              normed_size
              base_image_uri
              piece_ordinal
              piece_set_size
              body_piece {
                piece_key
                size_code
              }
            }
          }
        }
      }
    """
    hasura = res.connectors.load("hasura")

    results = hasura.execute_with_kwargs(Q, one_number=one_number)["make_one_orders"]

    if len(results):
        return results[0]
    return {}


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_one_piece_status_with_healing(one_number, postgres=None):
    """e.g. 10333357 get the one status with the augmented healing data"""
    from res.flows.make.healing.piece_healing_monitor import (
        get_healing_history_for_one_by_piece,
    )
    from operator import or_
    from functools import reduce

    def flatten(data, pcdata):
        p = data.pop("one_pieces")
        p = pd.DataFrame(p)
        p = res.utils.dataframes.expand_column_drop(p, "piece")
        p = res.utils.dataframes.expand_column_drop(
            p, "piece_body_piece", alias="body_piece"
        )
        p["contracted_failing"] = p["code"].map(lambda x: [])
        p["make_instance"] = 0
        p["piece_healing_history"] = p["code"].map(lambda x: pcdata.get(x) or [])
        p["make_instance"] = p["piece_healing_history"].map(
            lambda x: len(x) if isinstance(x, list) else 0
        )

        p["last_inspection_details"] = p["piece_healing_history"].map(
            lambda x: x[0] if isinstance(x, list) and len(x) else None
        )

        def pluck_all_contracts(x):
            if isinstance(x, list):
                return reduce(
                    or_, [i["defect_name"] for i in x if i["defect_name"]], set()
                )
            return []

        p["contracts_failed"] = p["piece_healing_history"].map(pluck_all_contracts)
        p["current_inspection_status"] = p["last_inspection_details"].map(
            lambda x: x["fail"] if isinstance(x, dict) else set()
        )
        p["contracted_failing"] = p["last_inspection_details"].map(
            lambda x: x["defect_name"] if isinstance(x, dict) else []
        )
        p["node"] = p["node"].map(lambda x: x.get("name") if pd.notnull(x) else None)
        p["uri"] = p["piece_base_image_uri"]
        p["piece_ordinal"] = p["piece_piece_ordinal"]
        p["piece_set_size"] = p["piece_piece_set_size"]
        data["one_pieces"] = p.to_dict("records")

        return data

    res.utils.logger.info(f"Loading one data {one_number=}")
    data = get_one_piece_status(one_number)

    res.utils.logger.info(f"Loading healing data...")
    healing_data = get_healing_history_for_one_by_piece(one_number, postgres=postgres)

    res.utils.logger.info(f"flatten data for one order status schema")
    return flatten(data, healing_data)


# ^turn the above into a mutation but we gotta be careful about how we do these assignments idempotently
def get_one_pieces_by_one_order_id(id, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(GET_ONE_PIECES_BY_OID, id=id)["make_one_pieces"]


# TODO: smarter soft association of ones and one orders (oids and one number)
# we can skip this for now by doing a lazy merge in the create-make-one where we need to update the api anyway
def set_make_order_for_sku(order_key, sku, idx, hasura=None):
    # look it up by id and add the one number to the order and the oids to the order and the fulfillment items
    return None


def get_make_order_with_pieces_by_order_number(order_number, hasura=None):
    hasura = hasura or res.connectors.load("hasura")

    data = hasura.execute_with_kwargs(
        GET_MAKE_ORDER_WITH_PIECES_BY_ORDER_NAME, order_number=order_number
    )["make_one_orders"]

    return data


def get_make_order_by_one_number(one_number, hasura=None):
    hasura = hasura or res.connectors.load("hasura")

    data = hasura.execute_with_kwargs(
        GET_ONE_PIECES_BY_ONE_NUMBER, one_number=one_number
    )

    return data


def get_make_order_by_one_label(one_label_code, hasura=None):
    hasura = hasura or res.connectors.load("hasura")

    data = hasura.execute_with_kwargs(
        GET_MAKE_ORDER_BY_LABEL, one_label_code=one_label_code
    )

    return data


def update_one_label_association(data, hasura=None):
    """ """
    UPSERT_LABEL_ASSOC = """mutation upsert_one_label_association($objects: [make_one_labels_insert_input!] = {}) {
      insert_make_one_labels(objects: $objects, on_conflict: {constraint: one_labels_pkey, update_columns: [code, oid]}) {
        returning {
          id
        }
      }
    }
    """

    hasura = hasura or res.connectors.load("hasura")

    if isinstance(data, dict):
        data = [data]
    if isinstance(data, pd.DataFrame):
        data = data.to_dict("records")
    failed = []
    from tqdm import tqdm

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def ten_update(record):
        hasura.execute_with_kwargs(
            UPSERT_LABEL_ASSOC,
            objects=[
                {
                    "id": res.utils.uuid_str_from_dict({"code": str(record["code"])}),
                    "code": str(record["code"]),
                    "oid": record["oid"],
                }
            ],
        )

    res.utils.logger.info(f"Updating {len(data)} records")
    for record in tqdm(data):
        try:
            ten_update(record)
        except:
            failed.append(record)
    return failed


def get_existing_one_orders(order_number, sku=None, hasura=None):
    SKU_PRED = f", sku: {{_eq: $sku}}" if sku else ""
    Q = f"""
    query MyQuery($order_number: String = "", $sku: String = "") {{
          make_one_orders(where: {{order_number: {{_eq: $order_number}}{SKU_PRED}}}) {{
            id
            sku
            order_number
            one_number
            status
          }}
        }}

    """
    hasura = hasura or res.connectors.load("hasura")
    args = {"order_number": order_number}
    if sku:
        args.update({"sku": sku})
    return hasura.execute_with_kwargs(Q, **args)["make_one_orders"]


def get_make_order_for_sku(
    order_key, sku, idx, one_number=None, hasura=None, factory=None
):
    """
    example:

      r = get_make_order_for_sku('LL-2128709', sku='CC-1002 CT406 SPRIRW Z2600', idx=0, one_number=1234556)

    it looks up an ordered item from the order matching the sku
    if a one number is passed in because it is known already, we can link them in reading and write back the merge
    we could also update the association here which is more efficient but because of the existing flow we can skip for now

    there is some thought needed to match skus given how material swaps traditionally screw up skus
    """

    def style_sku(s):
        return " ".join([a for a in s.split(" ")[:3]])

    hasura = hasura or res.connectors.load("hasura")
    rank = idx + 1

    if one_number:
        res.utils.logger.info(
            f"Passed in one number {one_number} will be used to resolve the make order"
        )
        r = hasura.execute_with_kwargs(
            GET_FULFILLMENT_ITEM_FOR_ONE, one_number=one_number
        )["make_one_orders"]
    if len(r) == 0:  # if we failed to fetch it do it this way
        res.utils.logger.info(
            f"No record of a ONE, looking up by rank {rank} and product {sku}"
        )
        # here we are supporting retro swaps
        # we want to use the product sku which could be the same as the ordered sku but maybe not
        # but the product sku only has three components so we need to match the size component on the original sku unfortunately
        r = hasura.execute_with_kwargs(
            GET_FULFILLMENT_ITEM_FOR_PRODUCT_SKU,
            order_number=order_key,
            sku=style_sku(sku),
            size_like=f"%{sku.split(' ')[-1].strip()}",
        )["make_one_orders"]

        """
        below; we should not need to do this but there are some weird states where an order should be swapped but the
        make production request comes through in the older material. Its a failed state but we can allow the order here and flag it 
        otherwise its hard to know what is happening - the meta one can in practice soft swap these anyway

        """
        if 0 == len(r):
            res.utils.logger.info(
                f"no match for product sku - trying ordered sku in case there is a blocked swap"
            )
            r = hasura.execute_with_kwargs(
                GET_FULFILLMENT_ITEM_FOR_ORDERED_SKU, order_number=order_key, sku=sku
            )["make_one_orders"]
            if len(r):
                res.utils.logger.info(
                    "Matched the sku on on the order item's sku but not the product"
                )

        # res.utils.logger.info(f"....Found {len(r)}")
        # we can only consider those that have the non assigned one
        r = sorted(
            [item for item in r if item["one_number"] == 0],
            key=lambda x: x["order_item_rank"],
        )
        res.utils.logger.info(
            f"considering items that have no one numbers - there are {len(r)}"
        )

        # prefer the rank that we own
        _r = [item for item in r if item["order_item_rank"] == rank]
        if len(_r) > 0:
            r = _r
        elif r:  # otherwise take the smallest one that is free as a list of one
            r = [r[0]]
        else:
            r = None
        # filter since we only resolved the style

    if r:
        r = r[0]
        r["one_number"] = one_number
        r["order_number"] = order_key
        r["oid"] = res.utils.uuid_str_from_dict({"id": one_number})

        # single source of truth - material swap ghosts - will look at this more closely
        # r["sku"] = r["order_line_item"]["sku"]
        try:
            # to support swaps we may want to see what the effective product sku is first
            order_sku = r["order_line_item"]["sku"]
            order_size = order_sku.split(" ")[-1].strip(_)
            r["sku"] = r["order_line_item"]["product"]["sku"] + f" {order_size}"

        except:
            r["sku"] = r["order_line_item"]["sku"]

        for p in r["one_pieces"]:
            p["oid"] = res.utils.uuid_str_from_dict(
                {"one_number": one_number, "piece_code": p["code"]}
            )
            # construct the object...
            p["meta_piece_id"] = p["piece"]["id"]
        # these are added for the order that we post ??
        # r["pieces"] = ([p["code"] for p in one_pieces],)
        # r["piece_images"] = ([p["piece"]["base_image_uri"] for p in one_pieces])
        if len(r["one_pieces"]) == 0:
            res.utils.logger.warn(
                f"An order was found with id {r['id']} but there are no attached pieces - they were probably stolen!"
            )
            # raise Exception("There are no pieces in the returned set ")
            return None
        return r if not factory else factory(**r)
    else:
        res.utils.logger.warn(
            f"""When trying to update the make one order we did not fetch the order item with sku {sku} and order {order_key} - this happens if the production inbox failed to save the One Order Item at fulfillment offset {rank} -
              if you think you have the order you should check for sku changes e.g. material swaps. Check the materials carefully in the skus in the order and make orders. The make order will be generated from whatever meta one the product in sell order points to"""
        )


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_existing_sku_orders(order_number, sku, hasura=None):
    Q = """query get_existing_sku_order($order_number: String = "", $sku: String = "") {
      make_one_orders(where: {order_number: {_eq: $order_number}, sku: {_eq: $sku},  status: {_neq: "CANCELLED"}}) {
        one_number
        id
        order_item_rank
        order_item_id
        status
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(Q, order_number=order_number, sku=sku)[
        "make_one_orders"
    ]


def associate_one_number_with_make_order(mapping, sku, one_number):
    """
    we trust that a process has managed the mappings of ones
    we can load this from make one in airtable or mongo/snowflake to bootstrap and maintain it in legacy processes

    this is not needed in a world where we dont need to wait for a one number

    when associating, the default behaviour is to pair ranks but states can get screwed up so fall back to using a rank of anything we have already saved for this one
    """

    one_number = int(float(str(one_number)))

    sku_map = mapping.get("sku_map", {}).get(sku, [])

    if one_number in sku_map:
        res.utils.logger.info(
            f"Associating the one number at index {sku_map.index(one_number)} in the mapped {sku_map} (add 1 for rank)"
        )
    else:
        res.utils.logger.info(
            f"Cannot associate the one number {one_number} as it is not in the mapped {sku_map}"
        )

    # safety
    if sku[2] != "-":
        sku = f"{sku[:2]}-{sku[2:]}"

    if isinstance(mapping, list):
        mapping = mapping[0]

    order_key = mapping["order_key"]

    ones = mapping["sku_map"].get(sku)
    # print(mapping, ones, one_number)
    idx = ones.index(one_number) if one_number in ones else -1
    if idx == -1:
        raise Exception(
            f"The sku {sku} is not in the list of registered ones - invalid state here. Go and pull it from make one and update the mappings"
        )

    # this makes an associate - we can use the flow api to save the whole thing back which is brutal or do a softer update of the one number here and just update airtable in the legacy process
    mo = get_make_order_for_sku(order_key, sku, idx, one_number=one_number)
    return mo


def cancel_one_order_by_ids(ids, status="CANCELLED", hasura=None):
    """
    cancels make one orders - using a logic of generating a dummy id to store the history of cancelled orders
    in future we should not do this but just re-allocate pieces to the same make order
    """
    if not ids:
        return

    if not isinstance(ids, list):
        ids = [ids]

    new_id = str(uuid.uuid1())
    UON = """mutation u($new_id: uuid = "", $ids: [uuid!] = "", $status: String = "") {
      update_make_one_orders(where: {id: {_in: $ids}}, _set: {id: $new_id, status: $status }) {
        returning {
          id
          one_number
          status
        }
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(UON, new_id=new_id, status=status, ids=ids)


def cancel_one_order_by_one_numbers(one_numbers, status="CANCELLED", hasura=None):
    """
    cancels make one orders - using a logic of generating a dummy id to store the history of cancelled orders
    in future we should not do this but just re-allocate pieces to the same make order
    """
    if not isinstance(one_numbers, list):
        one_numbers = [one_numbers]

    new_id = str(uuid.uuid1())
    UON = """mutation u($new_id: uuid = "", $one_numbers: [Int!] = 10, $status: String = "") {
      update_make_one_orders(where: {one_number: {_in: $one_numbers}}, _set: {id: $new_id, status: $status }) {
        returning {
          id
          one_number
          status
        }
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        UON, new_id=new_id, status=status, one_numbers=one_numbers
    )


def cancel_make_order_by_one_number(one_number, status="CANCELLED", hasura=None):
    """
    cancels make one orders - using a logic of generating a dummy id to store the history of cancelled orders
    in future we should not do this but just re-allocate pieces to the same make order
    """
    new_id = str(uuid.uuid1())
    UON = """mutation u($new_id: uuid = "", $one_number: Int = "", $status: String = "") {
      update_make_one_orders(where: {one_number: {_eq: $one_number}}, _set: {id: $new_id, status: $status }) {
        returning {
          id
          one_number
          status
        }
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        UON, new_id=new_id, status=status, one_number=one_number
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def record_cancelled_one(order_key, sku, one_number, plan=False):
    """
    if the order is found by one number it will be cancelled and the ids will be returned
    we also update the map - if the one number is in the map it is removed

    for example:
      record_cancelled_one (order_key='BG-2130909', sku='CC2066 HC293 BRUCQZ Z4200', one_number=10304604)

      the status of the order should be cancelled and it should have a new id
    """
    a = update_order_map_for_association(order_key, sku, one_number, plan, remove=True)
    return cancel_make_order_by_one_number(one_number=one_number), a


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def update_order_map_for_association(
    order_key, sku, one_number, plan=False, remove=False
):
    """
    A trusted caller can add a one number to the map for the SKU if it does not exist - the map always gives an ordered one number

    in processing orders one by one we can register / update this map

    update_order_map_for_association('RS-2130475', 'CC-3068 CTJ95 RESURX 1ZZXS', 10303443)

    its a bit inefficient but fine for our use cases

    if we cancel a one we can remove the order from the map

    """

    # safety
    if sku[2] != "-":
        sku = f"{sku[:2]}-{sku[2:]}"

    def _update_map(order_map, hasura=None):
        hasura = hasura or res.connectors.load("hasura")
        order_map["id"] = res.utils.uuid_str_from_dict(
            {"order_key": str(order_map["order_key"])}
        )

        mapping = hasura.execute_with_kwargs(UPDATE_ORDER_MAP, objects=[order_map])[
            "insert_infraestructure_bridge_order_skus_to_ones"
        ]["returning"]

        if len(mapping):
            # print(mapping)
            return mapping[0]

    one_number = int(float(str(one_number)))

    mapping = get_order_map_by_order_key(order_key)

    if plan:
        return mapping

    if mapping:
        mapping = mapping[0] if isinstance(mapping, list) else mapping
    else:
        res.utils.logger.info(f"Making a new mapping for the order...")
        mapping = {"order_key": order_key, "sku_map": {}}
        # list deref

    sku_mapping = mapping.get("sku_map", {})
    sku_map = sku_mapping.get(sku)
    if sku_map:
        if one_number not in sku_map:
            if remove:
                res.utils.logger.info(f"one number {one_number} is not in {sku_map}")
            else:
                res.utils.logger.info(
                    f"adding one number {one_number} into the sku map {sku_map}"
                )
                sku_map.append(one_number)
                sku_mapping[sku] = sku_map

        else:
            if remove != True:
                res.utils.logger.info(
                    f"already have the one number {one_number} in the sku map at index {sku_map.index(one_number)}"
                )
            if remove == True:
                res.utils.logger.info(f"removing the order from the map")
                sku_map = [s for s in sku_map if s != one_number]
                sku_mapping[sku] = sku_map

    elif not remove:
        res.utils.logger.info("Adding new entry for this sku")
        sku_mapping[sku] = [one_number]

    mapping["order_size"] = sum([len(a) for a in sku_mapping.values()])

    return _update_map(mapping)


@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(2))
def get_order_map_from_bridge_table(order_number):
    """
    get a map from a tabular forma of order one numbers on skus
    """

    a = _get_make_order_bridge_records_by_order_number(order_number)
    a = pd.DataFrame(a).sort_values("created_at")
    a = a[a["one_number"].notnull()][["sku", "one_number", "order_number"]]

    mapped = {}
    for record in a.to_dict("records"):
        sku = record["sku"]
        l = mapped.get(sku) or []
        mapped[sku] = l + [record["one_number"]]

    return mapped


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2))
def get_order_map_by_order_key(key, hasura=None):
    """

    e.g.  get_order_map_by_order_key('RS-2130475')

    """

    #    #    all_ones = _get_make_order_bridge_records_by_order_number("RB-1359536")

    GET_ORDER_MAP = """query get_order_map($id: uuid = "") {
                  infraestructure_bridge_order_skus_to_ones(where: {id: {_eq: $id}}) {
            sku_map
            order_size
            order_key
            id
        }
    }
    """
    rec_id = res.utils.uuid_str_from_dict({"order_key": key})

    hasura = hasura or res.connectors.load("hasura")

    return hasura.execute_with_kwargs(GET_ORDER_MAP, id=rec_id)[
        "infraestructure_bridge_order_skus_to_ones"
    ]


def bulk_load_from_mone(mone, plan=False, hasura=None):
    """
    this bulk loads from mone but we can do better by bulk loading from kafka topic
    """
    maps = []
    from tqdm import tqdm

    hasura = hasura or res.connectors.load("hasura")
    mone["sku"] = mone["SKU"].map(lambda x: f"{x[:2]}-{x[2:]}")
    res.utils.logger.info("loading")
    for key, data in tqdm(mone.groupby("Belongs to Order")):
        skus = {}
        order_size = 0
        for sku, _data in data.groupby("sku"):
            one_numbers = list(_data["Order Number v3"].astype(int))
            skus[sku] = one_numbers
            order_size += len(one_numbers)

        d = {
            "id": res.utils.uuid_str_from_dict({"order_key": key}),
            "order_key": key,
            "sku_map": skus,
            "order_size": order_size,
        }
        maps.append(d)

    maps = pd.DataFrame(maps)
    maps = maps.sort_values("order_size")
    if plan:
        return maps

    res.utils.logger.info("saving....")
    failures = []
    for record in tqdm(maps.to_dict("records")):
        try:
            hasura.execute_with_kwargs(UPDATE_ORDER_MAP, objects=[record])
        except Exception as ex:
            failures.append({"record": record, "error": ex})
    return failures


def get_one_order_id(one_code, piece_code_suffix):
    """
    this is the id generated from our one code (WIP) and unique piece on the style

    eg. TK-3075-V15-G360-MD-1, TOPBNLOP-S

    in this setup we are using the ids just at code level so we do not try to track every unique piece we associated with the order
    but the system does delete old refs so we can evolve our strategy and then upsert different models
    for example we might want to store each physical instance we tried to make (keep in mind its in kafka anyway though)

    plan: in future we can make the id more unique by using id=f(one_code, piece_uuid) while then deprecating one_number to be one code equivalent
    and then using the oid to be f(one_code, piece_suffix)
    that we way get both the very specific and the more broad (best of both worlds)
    we can store each pid every associated but do updates based on the piece code where delete at is not set ??
    """
    return (
        res.utils.uuid_str_from_dict(
            {"one_code": one_code, "piece_code": piece_code_suffix}
        ),
    )


def get_one_order_piece_oid(one_number, piece_code_suffix):
    """
    given an integer one number, and piece code suffix (unique on style) generate the oid
    e.g. 10291345, TOPBNLOP-S
    """
    return res.utils.uuid_str_from_dict(
        {"one_number": int(float(one_number)), "piece_code": piece_code_suffix}
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def increment_instance_for_group_at_time(
    one_code_prefix, make_hash, sku=None, hasura=None
):
    """
    keep a counter of any one code prefix and make the id for the new thing with a discriminator
    the discriminator can be anything but its the handle on the unique make request
    """
    INS = """mutation MyMutation($objects: [make_make_instance_counter_insert_input!] = {}) {
      insert_make_make_instance_counter(objects: $objects, on_conflict: {constraint: one_code_counter_pkey, update_columns: [one_code, metadata]}) {
        returning {
          id
          one_code
          make_instance
          updated_at
        }
      }
    }
    """
    id = res.utils.uuid_str_from_dict(
        {"group": one_code_prefix, "make_hash": make_hash}
    )

    hasura = hasura or res.connectors.load("hasura")
    d = {
        "id": id,
        "one_code": one_code_prefix,
        "metadata": {"make_hash": make_hash, "sku": sku},
    }

    return hasura.execute_with_kwargs(INS, objects=[d])[
        "insert_make_make_instance_counter"
    ]["returning"][0]

    # from tqdm import tqdm

    # for record in tqdm(rankable.to_dict("records")):
    #     g = record["prefix_fix"]
    #     if g:
    #         rrt = increment_instance_for_group_at_time(g, record["id"])


def get_ranked_make_orders(hasura=None):
    """
    we rank make orders based on the order in which they were created / assigned one numbers
    """
    Q = """query MyQuery {
      make_one_orders {
        id
        order_number
        one_number
        one_code
        status
        sku
        created_at
        order_item_id
        order_item_rank
        oid
      }
    }
    """

    hasura = hasura or res.connectors.load("hasura")

    df = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_orders"])
    df["created_at"] = pd.to_datetime(df["created_at"])

    df["pseudo_one"] = df["one_number"].map(lambda x: 1000000000 if x == 0 else x)
    df.loc[df["oid"].isnull(), "pseudo_one"] = df["pseudo_one"] * 10
    df = (
        df.sort_values(["pseudo_one", "created_at"])
        .reset_index(drop=True)
        .reset_index()
    )
    df["rank"] = df.groupby(["order_item_id"])["index"].rank(ascending=True).map(int)
    df["okey"] = df.apply(lambda x: f"{x['order_number']} {x['sku']}", axis=1)
    return df


def update_make_order_ranks(hasura=None, plan=True):
    """
    This is used has a support tool as we roll out make orders
    the make order ranks must respect the date of creation ideally
    this is done so that we have a deterministic function to map incoming one numbers to make orders in order

    df = update_make_order_ranks(plan=False)
    dff=

    """
    UON = """mutation MyMutation($order_item_rank: Int = 0, $id: uuid = "") {
      update_make_one_orders(where: {id: {_eq: $id}}, _set: {order_item_rank: $order_item_rank}) {
        returning {
          id
          order_number
          order_item_id
          order_item_rank
        }
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")

    df = get_ranked_make_orders()
    res.utils.logger.info(f"found {len(df)} make orders")
    # if its already correct skip
    df = df[df["order_item_rank"] != df["rank"].map(float)]
    res.utils.logger.info(
        f"found {len(df)} make orders that are different in terms of ranks"
    )
    retry = []
    if not plan:
        from tqdm import tqdm

        for record in tqdm(df.to_dict("records")):
            #     r = hasura.execute_with_kwargs(UON, order_item_rank=record['rank'], id=record['id'] )
            #     print(r)
            #     break
            try:
                r = hasura.execute_with_kwargs(
                    UON, order_item_rank=record["rank"], id=record["id"]
                )
            except Exception as ex:
                print("failed", ex)
                retry.append(record)
    return df


###########################
##         UTILS TO MANAGE HEALING COUNTS
###########################


def _oid(one_number, piece_code):
    return res.utils.uuid_str_from_dict(
        {"one_number": int(float(one_number)), "piece_code": piece_code}
    )


def _healing_request_id(one_number, piece_code, request_id):
    return res.utils.uuid_str_from_dict(
        {
            "request_id": request_id,
            "one_number": int(float(one_number)),
            "piece_code": piece_code,
        }
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_healing_requests_for_piece(one_number, piece_code):
    """
    e.g get_healing_requests_for_piece(10270769, 'PNTFTPKTLF-S')
    """
    Q = """query get_piece_healing_requests($piece_id: uuid = "") {
          make_one_piece_healing_requests(where: {piece_id: {_eq: $piece_id}}) {
            metadata
            id
            request_id
            piece_id
            updated_at
          }
        }
    """

    hasura = res.connectors.load("hasura")
    data = hasura.execute_with_kwargs(Q, piece_id=_oid(one_number, piece_code))[
        "make_one_piece_healing_requests"
    ]
    return data


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def update_one_status(record, hasura=None):
    """
    get values that are today store on make one production base and track them in the bridge table

    """
    M = """mutation update_one_status($fulfilled_at: timestamptz = "", $exited_completion_at: timestamptz = "", $exited_cut_at: timestamptz = "", $exited_print_at: timestamptz = "", $exited_sew_at: timestamptz = "", $make_node: String = "", $assembly_node: String = "", $one_number: Int = 10, $contracts_failing: jsonb = "") {
      update_infraestructure_bridge_sku_one_counter(where: {one_number: {_eq: $one_number}}, _set: {fulfilled_at: $fulfilled_at, exited_sew_at: $exited_sew_at, exited_print_at: $exited_print_at, exited_cut_at: $exited_cut_at, exited_completion_at: $exited_completion_at, make_node: $make_node, assembly_node: $assembly_node, contracts_failing: $contracts_failing}) {
        returning {
          id
          contracts_failing
        }
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    one_number = record["name"]

    def tv(s):
        # get the time value
        return str(s) if pd.notnull(s) else None

    def lv(s):
        # get the list value
        return list(s) if isinstance(s, list) or isinstance(s, np.ndarray) else None

    return hasura.execute_with_kwargs(
        M,
        one_number=one_number,
        fulfilled_at=tv(record.get("exited_at")),
        exited_completion_at=tv(record.get("exited_completion_at")),
        exited_sew_at=tv(record.get("exited_sew_at")),
        exited_cut_at=tv(record.get("exited_cut_at")),
        exited_print_at=tv(record.get("exited_print_at")),
        make_node=record.get("node"),
        assembly_node=record.get("node_status"),
        contracts_failing=lv(record.get("contracts_failing_list")),
    )


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def _update_id_on_healing_audit(id, print_asset_piece_id, hasura=None):
    """
    adding the key retroactively to the data
    """
    Q = """mutation MyMutation($id: uuid = "", $print_asset_piece_id: String = "") {
      update_make_one_piece_healing_requests(where: {id: {_eq: $id}}, _set: {print_asset_piece_id: $print_asset_piece_id}) {
        returning {
          id
        }
      }
    }
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        Q, id=id, print_asset_piece_id=print_asset_piece_id
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def update_healing_status(
    one_number, piece_code, request_id, versioned_body_code=None, plan=False
):
    """
    update the healing instance
    #this example will add a message that does not exist but it increments the piece counter - if you run it you should delete it and another one and add the other one
    update_healing_status(10270769, 'PNTFTPKTLF-S', request_id='bogus', plan=False)
    #this will use an example that already exists and not do anything
    #update_healing_status(10270769, 'PNTFTPKTLF-S', request_id='recXrCLBOgKujRpXj', plan=False)

    """
    M = """mutation update_make_piece_make_instance($requests: [make_one_piece_healing_requests_insert_input!] = {},  $make_instance: numeric = "", $oid: uuid = "") {
          insert_make_one_piece_healing_requests(objects: $requests, on_conflict: {constraint: one_piece_healing_requests_pkey, update_columns: metadata}) {
            returning {
              id
            }
          }
          update_make_one_pieces(where: {oid: {_eq: $oid}}, _set: {make_instance: $make_instance}) {
            returning {
              id
              make_instance
            }
          }
        }
        """

    # piece names must contain the full body name and version
    piece_name = f"{versioned_body_code}-{piece_code}"

    # up to know we used this for unique piece ids based on airtable record ids for print assets
    print_asset_piece_id = res.utils.res_hash(f"{request_id}{piece_name}".encode())

    hasura = res.connectors.load("hasura")
    # check the history so we can keep a running total
    requests_existing = get_healing_requests_for_piece(one_number, piece_code)
    request_keys = [r["request_id"] for r in requests_existing]
    increment = len(requests_existing)
    pid = _oid(one_number, piece_code)
    # BUMP!
    increment += 1

    res.utils.logger.info(
        f"Recording healing request {request_id=} {piece_name=} {print_asset_piece_id=} {one_number=}"
    )

    if request_id not in request_keys:
        request = {
            "id": _healing_request_id(one_number, piece_code, request_id),
            "request_id": request_id,
            "piece_id": pid,
            "metadata": {
                "one_number": one_number,
                "piece_code": piece_code,
                "increment": increment,
                "versioned_body": versioned_body_code,
            },
            "print_asset_piece_id": print_asset_piece_id,
        }
        if plan:
            res.utils.logger.info("will update requests for piece healing for piece")
            return request
        response = hasura.execute_with_kwargs(
            M, oid=pid, make_instance=increment, requests=[request]
        )
        res.utils.logger.info(f"{response=}")
    else:
        res.utils.logger.info(
            f"We already have the request {request_id} for piece {piece_code}({one_number}) and our increment now is {increment}"
        )
    # Note that we increment because this number of requests + 1 is our conventional counter from the database now
    return increment


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def update_make_one_pieces_increment(oid, make_instance):
    U = """mutation MyMutation($oid: uuid = "",  $make_instance: numeric=0) {
          update_make_one_pieces(where: {oid: {_eq: $oid}}, _set: {make_instance: $make_instance}) {
            returning {
              id
              oid
              code
              make_instance
            }
          }
        }
    """

    hasura = res.connectors.load("hasura")
    return hasura.execute_with_kwargs(U, oid=oid, make_instance=make_instance)


########
###
#######################################


####
#   bridging functions:
####
@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def set_one_cancelled(one_number, cancelled_at):
    M = """mutation MyMutation($cancelled_at: timestamptz = "", $one_number: Int = 10) {
          update_infraestructure_bridge_sku_one_counter(where: {one_number: {_eq: $one_number}}, _set: {cancelled_at: $cancelled_at}) {
            returning {
              cancelled_at
              id
              one_number
            }
          }
        }
        """
    hasura = res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        M, one_number=one_number, cancelled_at=cancelled_at
    )


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def set_one_status_fields(
    one_number,
    dxa_assets_ready_at=None,
    make_assets_checked_at=None,
    print_request_airtable_id=None,
    entered_assembly_at=None,
):
    """
    update the gates in airtable after we have
    - checked the dxa assets
    - create the print request
    - AND/OR checked the make assets are ready / or made them ready

    the last check requires print assets, trims and stampers to be valid or we hold it
    """
    # figure out what to update - there must be a better way...
    field_map = {
        "dxa_assets_ready_at": dxa_assets_ready_at,
        "make_assets_checked_at": make_assets_checked_at,
        "print_request_airtable_id": print_request_airtable_id,
        "entered_assembly_at": entered_assembly_at,
    }
    types_map = {
        "dxa_assets_ready_at": "timestamptz",
        "make_assets_checked_at": "timestamptz",
        "print_request_airtable_id": "String",
        "entered_assembly_at": "timestamptz",
    }
    field_map = {k: v for k, v in field_map.items() if v is not None}
    input_fields = ",".join(["$" + f + ": " + types_map[f] for f in field_map.keys()])
    set_clause = ",".join(f + ": $" + f for f in field_map.keys())
    # this is my mutation - there are many like it but this one is mine
    M = f"""mutation MyMutation($one_number: Int, {input_fields}) {{
          update_infraestructure_bridge_sku_one_counter(where: {{one_number: {{_eq: $one_number}}}}, 
          _set: {{
            {set_clause}
          }}) {{
            returning {{
              dxa_assets_ready_at
              print_request_airtable_id
              make_assets_checked_at
              id
              one_number
            }}
          }}
        }}
        """
    hasura = res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        M,
        one_number=one_number,
        **field_map,
    )


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_one_piece_healing_instance_resolver(one_numbers):
    """
    given a set of one numbers, get all non zero pieces instances for healing counts
    actually this returns a function of one_number, piece code that can be used to resolve items
    in the batch
    """
    Q = """query MyQuery($one_numbers: [Int!] = 10) {
      make_one_pieces(where: {one_order: {one_number: {_in: $one_numbers}}, make_instance: {_gt: 1}}) {
        make_instance
        one_order {
          one_number
        }
        code
      }
    }
    """
    hasura = res.connectors.load("hasura")

    if isinstance(one_numbers, int):
        one_numbers = [one_numbers]

    # coerce
    one_numbers = [int(float(o)) for o in one_numbers]

    df = pd.DataFrame(
        hasura.execute_with_kwargs(Q, one_numbers=one_numbers)["make_one_pieces"]
    )
    # nothing to do
    if len(df) == 0:
        res.utils.logger.warn(
            f"The request for ones {one_numbers} did not return any healing instances - returning a dummy resolver"
        )
        return lambda a, b: None

    df["one_number"] = df["one_order"].map(lambda x: x.get("one_number"))
    df["make_instance"] = df["make_instance"].fillna(0).map(int)
    # the database is not zero indexed - we should have done it that way
    df["make_instance"] -= 1

    d = {}
    for one_number, data in df.groupby("one_number"):
        mapped = dict(data[["code", "make_instance"]].values)
        d[one_number] = mapped

    def f(one_number, code):
        return d.get(one_number, {}).get(code)

    return f


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def add_one_number_record(
    one_number,
    order_number,
    airtable_rid,
    sku,
    body_version,
    created_at=None,
    cancelled_at=None,
    ordered_at=None,
    sales_channel=None,
    brand_code=None,
    dxa_assets_ready_at=None,
    print_request_airtable_id=None,
    blocking_dxa_request_id=None,
    order_line_item_id=None,
    order_priority_type=None,
    **kwargs,
):
    """
    we can seed this with make one production historic data: `data = get_historical_one_orders_for_bridge_schema`

    """
    M = """mutation MyMutation($objects: [infraestructure_bridge_sku_one_counter_insert_input!] = {}) {
      insert_infraestructure_bridge_sku_one_counter(objects: $objects, on_conflict: {constraint: bridge_sku_one_counter_pkey, 
        update_columns: [airtable_rid, sku, style_sku, one_number, order_number, created_at, body_version, ordered_at, sales_channel, brand_code, dxa_assets_ready_at, print_request_airtable_id, blocking_dxa_request_id, order_line_item_id, order_priority_type]}) {
        returning {
          id
          airtable_rid
          one_number
          order_number
          body_version
          sku
          style_sku
          created_at
          updated_at
          ordered_at
          cancelled_at
          sales_channel
          brand_code
          dxa_assets_ready_at
          print_request_airtable_id
          blocking_dxa_request_id
          order_line_item_id
          order_priority_type
        }
      }
    }
    """
    body_version = body_version or 0
    body_version = int(float(str(body_version)))
    created_at = created_at or res.utils.dates.utc_now_iso_string()
    hasura = res.connectors.load("hasura")

    # safety - forbid no hyphens
    sku = fix_sku(sku)

    record = {
        "id": res.utils.uuid_str_from_dict({"one_number": int(one_number)}),
        "airtable_rid": airtable_rid,
        "one_number": one_number,
        "order_number": order_number,
        "body_version": body_version,
        "sku": sku,
        "style_sku": f" ".join([a for a in sku.split(" ")[:3]]),
        "created_at": created_at,
        "ordered_at": ordered_at or created_at,
        "cancelled_at": cancelled_at,
        "sales_channel": sales_channel,
        "brand_code": brand_code,
        "dxa_assets_ready_at": dxa_assets_ready_at,
        "print_request_airtable_id": print_request_airtable_id,
        "blocking_dxa_request_id": blocking_dxa_request_id,
        "order_line_item_id": order_line_item_id,
        "order_priority_type": order_priority_type,
    }

    return hasura.execute_with_kwargs(M, objects=[record])[
        "insert_infraestructure_bridge_sku_one_counter"
    ]["returning"][0]


def reload_body_versions_for_orders():
    Q = """mutation MyMutation($one_number: Int = 10, $body_version: Int = 10) {
      update_infraestructure_bridge_sku_one_counter(where: {one_number: {_eq: $one_number}}, _set: {body_version: $body_version}) {
        returning {
          id
        }
      }
    }

    """

    from res.flows.make.production.utils import get_all_ones

    hasura = res.connectors.load("hasura")

    df = get_all_ones()

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
    def update(r):
        return hasura.execute_with_kwargs(
            Q, one_number=r["one_number"], body_version=r["body_version"]
        )

    from tqdm import tqdm

    for record in tqdm(df[df["body_version"] > 0].to_dict("records")):
        r = update(record)


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def mark_ones_dxa_ready(production_request_ids):
    result = res.connectors.load("hasura").execute_with_kwargs(
        """
        mutation ($production_request_ids: [String!], $dxa_assets_ready_at: timestamptz) {
          update_infraestructure_bridge_sku_one_counter(where: {airtable_rid: {_in: $production_request_ids}}, _set: {dxa_assets_ready_at: $dxa_assets_ready_at}) {
            returning {
              airtable_rid
              one_number
            }
          }
        }
        """,
        production_request_ids=production_request_ids,
        dxa_assets_ready_at=res.utils.dates.utc_now_iso_string(),
    )["update_infraestructure_bridge_sku_one_counter"]["returning"]
    res.utils.logger.info(
        f"Unblock dxa result: {result} for production ids {production_request_ids}"
    )
    return result


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def mark_ones_cancelled(production_request_ids):
    result = res.connectors.load("hasura").execute_with_kwargs(
        """
        mutation ($production_request_ids: [String!], $cancelled_at: timestamptz) {
          update_infraestructure_bridge_sku_one_counter(where: {airtable_rid: {_in: $production_request_ids}}, _set: {cancelled_at: $cancelled_at}) {
            returning {
              airtable_rid
              one_number
              cancelled_at
            }
          }
        }
        """,
        production_request_ids=production_request_ids,
        cancelled_at=res.utils.dates.utc_now_iso_string(),
    )["update_infraestructure_bridge_sku_one_counter"]["returning"]
    res.utils.logger.info(
        f"Cancel result: {result} for production ids {production_request_ids}"
    )
    return result


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_airtable_make_one_request(one_number):
    QUERY_MAKE_ONE_REQUEST = """
    query getMakeOneRequest($one_number:String!){
        makeOneProductionRequest(orderNumber:$one_number){
            id
            name
            sku
            type
            bodyVersion
            requestName
            originalOrderPlacedAt
            orderNumber
            countReproduce
            reproducedOnesIds
            salesChannel
            belongsToOrder
            flagForReviewTags
            contractVariableId
            sewContractVariableId
        }
    }
    """
    gql = res.connectors.load("graphql")
    make_request_response = (
        gql.query(QUERY_MAKE_ONE_REQUEST, {"one_number": one_number})["data"][
            "makeOneProductionRequest"
        ]
        or {}
    )

    make_request_response["is_extra"] = "Extra Prod Request" in str(
        make_request_response.get("flagForReviewTags", [])
    )
    make_request_response["is_photo"] = (
        "photo" in make_request_response.get("salesChannel", "").lower()
    )
    if "optimus" in make_request_response.get("salesChannel", "").lower():
        make_request_response["is_extra"] = True
    return make_request_response


# @retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
# def get_one_number_with_sku_rank(one_number):
#     """

#     returns the ranked orders by increasing one number for a sku and one combo
#     there are smarter things we can do but this was last minute and it works well enough
#     by using less than OR equal to we insist that the one number is registered e.g. we dont try to compare ones and skus that dont match
#     so here zero also means it does not exist and 1 always means its inclusive of the one we are looking at

#     WARNING: if you do use a combination that does not make sense you will get a bogus value

#     get_my_rank(10154356, 'KT-6041 ECVIC PASTDZ')
#     get_my_rank(10150001, 'KT-6041 ECVIC PASTDZ')

#     """
#     Q = """query get_my_one_number_rank($my_one_number: Int = 10, $style_sku: String = "") {
#             infraestructure_bridge_sku_one_counter_aggregate(where: {one_number: {_lte: $my_one_number}, style_sku: {_eq: $style_sku}, ordered_at: {_is_null: false}}) {
#               aggregate {
#                 count
#               }
#             }
#           }
#         """
#     # ensure three parts
#     style_sku = f" ".join([a for a in style_sku.split(" ")[:3]])
#     if "-" != style_sku[2]:
#         style_sku = f"{style_sku[:2]}-{style_sku[2:]}"
#     hasura = res.connectors.load("hasura")

#     return hasura.execute_with_kwargs(
#         Q, my_one_number=int(one_number), style_sku=style_sku
#     )["infraestructure_bridge_sku_one_counter_aggregate"]["aggregate"]["count"]


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_one_number_record(
    one_number, postgres=None, keep_conn_open=False, format="dict"
):
    postgres = postgres or res.connectors.load("postgres")
    result = postgres.run_query(
        f"select * from infraestructure.bridge_sku_one_counter WHERE one_number={one_number}",
        keep_conn_open=keep_conn_open,
    )

    if format == "dict":
        if len(result):
            result = result.to_dict("records")[0]
    return result


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_ones_to_retry_assembly_enter(
    dxa_enter_cutoff_str, postgres=None, keep_conn_open=False, format="dict"
):
    postgres = postgres or res.connectors.load("postgres")
    result = postgres.run_query(
        f"""
          SELECT
            *
          FROM
            infraestructure.bridge_sku_one_counter
          WHERE
            print_request_airtable_id is null
            and cancelled_at is null
            and dxa_assets_ready_at > '{dxa_enter_cutoff_str}'
            and sku not like 'TT%'
        """,
        keep_conn_open=keep_conn_open,
    )
    if format == "dict":
        if len(result):
            result = result.to_dict("records")
        else:
            result = []
    return result


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_one_number_record_by_order_line_item_id(
    order_line_item_id, postgres=None, keep_conn_open=False, format="dict"
):
    postgres = postgres or res.connectors.load("postgres")
    result = postgres.run_query(
        f"select * from infraestructure.bridge_sku_one_counter WHERE order_line_item_id='{order_line_item_id}'",
        keep_conn_open=keep_conn_open,
    )

    if format == "dict":
        if len(result):
            result = result.to_dict("records")[0]
    return result


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def get_one_number_with_sku_rank(
    one_number, postgres=None, keep_conn_open=False, format="dict"
):
    """
    use the postgres connector to get the ranked one based on how/when we ordered the SKU
    """
    postgres = postgres or res.connectors.load("postgres")
    QUERY = f"""
        WITH C AS
        (
            SELECT * , 
            ROW_NUMBER() OVER (  PARTITION BY sku   ORDER BY created_at ASC) as sku_order_rank
           FROM infraestructure.bridge_sku_one_counter
           WHERE SKU = (select  SKU from  infraestructure.bridge_sku_one_counter where ONE_NUMBER = {one_number})
        )
        select * from C WHERE one_number={one_number}
    """

    def _run_query():
        return postgres.run_query(QUERY, keep_conn_open=keep_conn_open)

    result = _run_query()

    if format == "dict":
        if len(result):
            result = result.to_dict("records")[0]
    return result


def get_style_code_ranks_for_style_codes(
    style_codes: typing.List[str], one_number_hint=None, postgres=None
):
    if not isinstance(style_codes, list):
        style_codes = [style_codes]

    postgres = postgres or res.connectors.load("postgres")
    # skus = ['JR-3118 CTJ95 ATDMLI', '']
    style_codes = ",".join([f"'{s}'" for s in style_codes])
    if one_number_hint is not None:
        one_number_hint = ",".join([f"{s}" for s in one_number_hint])

    QUERY = f"""
        with t as (
          SELECT one_number, sku,  
          ROW_NUMBER() OVER (  PARTITION BY style_sku   ORDER BY ordered_at, one_number, created_at  ASC) as sku_order_rank
          FROM infraestructure.bridge_sku_one_counter
          WHERE style_sku in ({style_codes}) and deleted_at is NULL 
        ) select * from t
          """
    if one_number_hint:
        QUERY += f" WHERE one_number in ({one_number_hint})"

    data = postgres.run_query(QUERY)
    data = res.utils.dataframes.replace_nan_with_none(data)
    data["sku_order_rank"] = data["sku_order_rank"].fillna(-1).map(int)
    return dict(data[["one_number", "sku_order_rank"]].values)


def get_one_number_style_sku_order_rank(
    one_number, postgres=None, keep_conn_open=False, format="dict"
):
    """
    use the postgres connector to get the ranked one based on how/when we ordered the style code or BMC
    this is used for special editions - we can sort of ordered items unless they were intentionally deleted
    this was used to reset clocks only
    """

    # ensure
    # style_sku = f" ".join([a for a in style_sku.split(" ")[:3]])
    # if style_sku[2] != "-":
    #     style_sku = f"{style_sku[:2]}-{style_sku[2:]}"

    postgres = postgres or res.connectors.load("postgres")
    QUERY = f"""
        WITH C AS
        (
            SELECT * , 
            ROW_NUMBER() OVER (  PARTITION BY style_sku   ORDER BY ordered_at, one_number, created_at, ASC) as sku_order_rank
           FROM infraestructure.bridge_sku_one_counter
           WHERE style_sku = (select  style_sku from  infraestructure.bridge_sku_one_counter where ONE_NUMBER = {one_number} and deleted_at is NULL) 
        )
        select * from C WHERE one_number={one_number}
    """

    def _run_query():
        return postgres.run_query(QUERY, keep_conn_open=keep_conn_open)

    result = _run_query()

    if format == "dict":
        if len(result):
            result = result.to_dict("records")[0]
    return result


def get_historical_one_orders_for_bridge_schema():
    """
    data to load into the add_one_number_record function

    """
    snowflake = res.connectors.load("snowflake")

    data = snowflake.execute(
        f"""select a.ONE_NUMBER as "one_number", c.CHANNEL_ORDER_ID as "order_number", a.ONE_SKU as "sku", a.CANCELLED_AT, a.PRODUCTION_REQUEST_ID, a.BODY_VERSION,
        a.SALES_CHANNEL, a.EXITED_DXA_AT, a.PRINT_REQUEST_ID,  a.CREATED_AT as CREATED_AT, ORDERED_AT
            from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_PRODUCTION_REQUESTS"  a
            JOIN "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS" b on a.ORDER_LINE_ITEM_ID = b.ORDER_LINE_ITEM_ID and b.VALID_TO IS NULL
            JOIN "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" c on c.ORDER_ID = b.ORDER_ID and c.VALID_TO IS NULL
            AND a.VALID_TO IS NULL 
            ;
            """
    )

    data = data.rename(
        columns={
            "CANCELLED_AT": "cancelled_at",
            "CREATED_AT": "created_at",
            "PRODUCTION_REQUEST_ID": "airtable_rid",
            "EXITED_DXA_AT": "dxa_assets_ready_at",
            "PRINT_REQUEST_ID": "print_request_airtable_id",
            "SALES_CHANNEL": "sales_channel",
            "BODY_VERSION": "body_version",
            "ORDERED_AT": "ordered_at",
            "CHANNEL_ORDER_ID": "order_number",
        }
    )
    # data['id'] = data['one_number'].map(lambda one_number : res.utils.uuid_str_from_dict({"one_number": int(one_number)}),)
    # data['style_sku'] = data['sku'].map(lambda sku: f" ".join([a for a in sku.split(" ")[:3]]))

    data["body_version"] = data["body_version"].fillna(0).map(int)
    data["one_number"] = data["one_number"].map(int)

    for col in ["created_at", "cancelled_at", "dxa_assets_ready_at", "ordered_at"]:
        data[col] = pd.to_datetime(data[col], utc=True)
    data = res.utils.dataframes.replace_nan_with_none(data)
    for col in ["created_at", "cancelled_at", "dxa_assets_ready_at", "ordered_at"]:
        data[col] = data[col].map(lambda d: d.isoformat() if pd.notnull(d) else None)

    return data


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_orders_since_time(time: datetime):
    Q = """     
    query sell_orders_items($time: timestamptz!) {
      sell_order_line_items(where: {created_at: {_gt: $time}}) {
        created_at
        updated_at
        sku
        quantity
      }
    }"""

    hasura = res.connectors.load("hasura")
    # convert time to isoformat string
    str_time = time.isoformat()

    a = hasura.execute_with_kwargs(Q, time=str_time)["sell_order_line_items"]

    return a


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def _get_make_order_bridge_records_by_order_number(order_number):
    """
    the bridge table is where we store the
    this is added for completeness for the hasura query but the postgres one above is move useful as it adds the rank

    we only consider non cancelled orders here
    """
    Q = """query MyQuery($order_number: String = "") {
      infraestructure_bridge_sku_one_counter(where: {order_number: {_eq: $order_number}, cancelled_at: {_is_null: true}}) {
        sku
        sales_channel
        print_request_airtable_id
        ordered_at
        order_number
        one_number
        make_assets_checked_at
        id
        dxa_assets_ready_at
        deleted_at
        created_at
        cancelled_at
        body_version
        airtable_rid
        style_sku
        updated_at
      }
    }

    """

    hasura = res.connectors.load("hasura")

    a = hasura.execute_with_kwargs(Q, order_number=order_number)[
        "infraestructure_bridge_sku_one_counter"
    ]

    return a


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def _get_make_order_bridge_record_by_one_number(one_number):
    """
    the bridge table is where we store the
    this is added for completeness for the hasura query but the postgres one above is move useful as it adds the rank
    """
    Q = """query MyQuery($one_number: Int = 10) {
      infraestructure_bridge_sku_one_counter(where: {one_number: {_eq: $one_number}}) {
        sku
        sales_channel
        print_request_airtable_id
        ordered_at
        order_number
        one_number
        make_assets_checked_at
        id
        dxa_assets_ready_at
        deleted_at
        created_at
        cancelled_at
        body_version
        airtable_rid
        style_sku
        updated_at
      }
    }
    """

    hasura = res.connectors.load("hasura")

    a = hasura.execute_with_kwargs(Q, one_number=one_number)[
        "infraestructure_bridge_sku_one_counter"
    ]
    if len(a):
        return a[0]

    res.utils.logger.warn(
        f"Nothing matching ONE number {one_number} in infraestructure_bridge_sku_one_counter"
    )
    return {}
