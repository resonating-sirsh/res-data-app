"""
experimental CRUD for orders model - combined some util processing with CRUD in hasura - may refactor later
"""

import json
from ast import literal_eval

import pandas as pd
from stringcase import snakecase
from tenacity import retry, stop_after_attempt, wait_fixed

import res
from res.utils.strings import strip_emoji

# duplicate of another but here for product options ..
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

GET_ORDER_BY_ID = """query get_order_by_id($id: uuid = "") {
  sell_orders(where: {id: {_eq: $id}}) {
    id
    name
    brand_code
    contracts_failing
    updated_at
    source_order_id
    ordered_at
    order_line_items {
      id
      quantity
      sku
      product_id
    }
  }
}
"""

GET_ORDERS_BY_BRAND = """query sell_orders_by_brand($brand_code: String = "") {
  sell_orders(where: {brand_code: {_eq: $brand_code}}) {
    name
    status
    id
    order_channel
    sales_channel
    source_order_id
    updated_at
    ordered_at
    email 
  }
}

"""

GET_ORDER_BY_NAME = """query sell_orders_byname($name: String = "") {
  sell_orders(where: {name: {_eq: $name}}) {
    name
    id
    brand_code
    order_line_items {
      quantity
      sku
      id
      product_id
      product{
         sku
      }
    }
    order_channel
    source_order_id
    updated_at
    ordered_at
    deleted_at
    status
  }
}

"""

GET_MAKE_ORDER_BY_ORDER_ONE_CODE = """query make_order_by_order_oid($one_code: uuid = "") {
  make_one_orders(where: {one_code: {_eq: $one_code}}) {
    one_number
    one_code
    oid
    sku
    id
    order_number
    created_at
  }
}

"""

GET_MAKE_ORDER_BY_ORDER_OID = """query make_order_by_order_oid($oid: uuid = "") {
  make_one_orders(where: {oid: {_eq: $oid}}) {
    one_number
    one_code
    oid
    sku
    id
    order_number
    created_at
  }
}

"""

GET_MAKE_ORDER_BY_ORDER_NAME = """query make_order_by_order_name($name: String = "") {
  make_one_orders(where: {order_number: {_eq: $name}}) {
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
  }
}

"""

UPDATE_ORDER = """
mutation update_order($order_line_items: [sell_order_line_items_insert_input!] = {}, $order: sell_orders_insert_input = {}, $customer: sell_customers_insert_input = {}) {
 
  insert_sell_customers_one(object: $customer, on_conflict: {constraint: customers_email_key}) {
    id
  }
  
  insert_sell_orders_one(object: $order, on_conflict: {constraint: orders_pkey, update_columns: [status,source_order_id,order_channel,sales_channel,email]}) {
    id
    name
    email
    source_order_id
    ordered_at
    order_channel
    sales_channel
  }
  
  insert_sell_order_line_items(objects: $order_line_items, on_conflict: {constraint: order_line_items_pkey, update_columns: [quantity, fulfillable_quantity, status, metadata, price, product_id]}) {
    returning {
      id
      name
      sku
      quantity
      status
      fulfillable_quantity
      price
      product{
         id
         style_id
      }
      order_item_fulfillments {
        status
        id
      }
    }
  }
}

"""

UPDATE_ONE_ORDER_ORDER_NAME = """mutation update_order_name($name: String = "", $id: uuid = "") {
  update_make_one_orders_by_pk(pk_columns: {id: $id}, _set: {order_number: $name}) {
    id
    order_number
  }
}
"""


UPDATE_SINGLE_PRODUCT = """mutation update_product($product: sell_products_insert_input = {}) {
  insert_sell_products_one(object: $product, on_conflict: {constraint: products_pkey, update_columns: [sku, style_id, name, metadata]}) {
    sku
    id
    style_id
    created_at
  }
}
"""
UPDATE_PRODUCTS = """mutation update_products($products: [sell_products_insert_input!] = {}) {
  insert_sell_products(objects: $products, on_conflict: {constraint: products_pkey, update_columns: [sku, style_id, name, metadata]}) {
    returning {
      style_id
      sku
    }
  }
}"""

GET_ORDER_ITEM_BY_CHANNEL_ID = """query get_order_item ($id: String = "") {
  sell_order_line_items(where: {source_order_line_item_id: {_eq: $id}}) {
    id
    metadata
    name
    fulfillable_quantity
  }
}

"""

GET_STYLE_RESONANCE_CODE = """
query getStyles($resonance_code:WhereValue!) {
  styles(first: 1, where:{code:{is:$resonance_code}}) {
    count
    styles {
      id
      bodyVersion
      code
    }
  }
}
"""

UPDATE_FULFILLMENT = """
    mutation update_fulfillment_record($record: sell_order_item_fulfillments_insert_input = {}) {
        insert_sell_order_item_fulfillments_one(object: $record, on_conflict: {constraint: order_fulfillment_pkey, 
            update_columns: [started_at,ended_at, make_order_id, order_item_id, order_item_ordinal,status]}) {
            id
            sent_to_assembly_at
        }
    }"""


GET_FULL_ORDER_BY_NAME = """query get_order_by_name($name: String = "") {
  sell_orders(where: {name: {_eq: $name}}) {
    id
    brand_code
    name
    email
    status
    ecommerce_source
    source_order_id
    ordered_at
    order_channel
    sales_channel
    contracts_failing
    order_line_items {
      id
      name
      sku
      quantity
      status
      source_order_line_item_id
      fulfillable_quantity
      fulfilled_quantity
      refunded_quantity
      price
      order_id
      product{
         id
         style_id
      }
      order_item_fulfillments {
        status
        id
      }
    }
  }
}
"""

_hyphenate_res_code = lambda s: s if "-" == s[2] else f"{s[:2]}-{s[2:]}"


def validate_order_id(o):
    def make_order_id(values):
        return res.utils.uuid_str_from_dict(
            {
                "id": str(values["source_order_id"]),
                "order_channel": values["order_channel"],
            }
        )

    def make_item_id(id, sku):
        return res.utils.uuid_str_from_dict({"order_key": id, "sku": sku})

    assert o["id"] == make_order_id(
        o
    ), f"Failed validation due to incorrect order id {o['id']} != {make_order_id(o)}"
    res.utils.logger.info(f"We have the correct expected order id: {make_order_id(o)}")

    for oi in o["order_line_items"]:
        item_id = make_item_id(o["id"], oi["sku"])
        assert (
            oi["id"] == item_id
        ), f"Failed validation due to incorrect order item id: {oi['id']} != {item_id}"
        assert (
            oi["product_id"] is not None
        ), f"Failed validation: The order item {oi} does not have a product id"

    res.utils.logger.info("order item ids are ok")


def delete_order_item_by_id(item_id):
    Q = """mutation MyMutation($item_id: uuid = "") {
      delete_sell_order_line_items(where: {id: {_eq: $item_id}}) {
        returning {
          id
        }
      }
    }
    """
    hasura = res.connectors.load("hasura")
    return hasura.execute_with_kwargs(Q, item_id=item_id)


def get_deleted_orders():
    QM0 = """query MyQuery {
      sell_orders(where: {deleted_at: {_is_null: false}}) {
        id
        deleted_at
      }
    }

    """
    hasura = res.connectors.load("hasura")
    data = pd.DataFrame(hasura.execute_with_kwargs(QM0)["sell_orders"])
    return data


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def delete_order(id, force=True):
    """
    delete an order by id
    it first needs to be marked for delete OR you can force True
    """
    DEL = """
    mutation del($id: uuid = "") {
      delete_sell_orders(where: {deleted_at: {_is_null: false}, id: {_eq: $id}}) {
        returning {
          id
          deleted_at
        }
      }
    }

    """

    # we dont need to mark as delete
    if force:
        DEL = """
            mutation del($id: uuid = "") {
            delete_sell_orders(where: {  id: {_eq: $id}}) {
                returning {
                id
                deleted_at
                }
            }
            }

    """
    hasura = res.connectors.load("hasura")
    hasura.execute_with_kwargs(DEL, id=id)


def reimport_order(order_number, delete_existing=False):
    """
    A brutal method to remove and re-import orders
    """
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode, Order

    if delete_existing:
        o = get_order_by_name(order_number)[0]
        res.utils.logger.info(f"deleting order {o['id']} with force option=True")
        delete_order(o["id"], force=True)

    res.utils.logger.info(f"Loading warehouse order")
    o = get_warehouse_airtable_orders_by_source_order_key(order_number)
    O = Order.from_create_one_warehouse_payload(o)
    res.utils.logger.info(f"running fulfillment node...")
    result = FulfillmentNode.run_request_collector(O)
    res.utils.logger.info(f"fetching saved orders")
    return get_order_by_name(order_number)


def get_full_order_by_name(name, hasura=None, flatten=True):
    def flattener(result):
        order = result["sell_orders"][0]
        order["order_items"] = order.pop("order_line_items")
        # brand and ecom added

        # get the fulfillmen stuff and use it to process
        for o in order["order_items"]:
            o["order_id"] = order["id"]
            # the product should be resolved for any valid order but we support saving invalid orders for tracking
            product = o.get("product")
            if product:
                o["product_id"] = product["id"]
                o["product_style_id"] = product["style_id"]
        return order

    hasura = hasura or res.connectors.load("hasura")
    r = hasura.execute_with_kwargs(GET_FULL_ORDER_BY_NAME, name=name)
    if flatten:
        r = flattener(r)
    return r


def get_order_by_one_code(one_code, hasura=None):
    """
    the name of the order should be of the form BRAND-NUMBER
    in some older data we may have added it with the hash notation but this is wrong as its not unique
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(GET_ORDER_BY_NAME, one_code=one_code)[
        "make_one_orders"
    ]


def get_order_by_id(id, hasura=None):
    """
    order by our own system id - normally we select by name but this is for testing
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(GET_ORDER_BY_ID, id=id)["sell_orders"]


def get_orders_by_brand(brand_code, hasura=None):
    """
    the name of the order should be of the form BRAND-NUMBER
    in some older data we may have added it with the hash notation but this is wrong as its not unique

    the validation flow is baked in for convenience when testing
    """
    hasura = hasura or res.connectors.load("hasura")
    o = hasura.execute_with_kwargs(GET_ORDERS_BY_BRAND, brand_code=brand_code)[
        "sell_orders"
    ]

    return o


def get_order_by_name(name, validate=False, hasura=None):
    """
    the name of the order should be of the form BRAND-NUMBER
    in some older data we may have added it with the hash notation but this is wrong as its not unique

    the validation flow is baked in for convenience when testing
    """
    hasura = hasura or res.connectors.load("hasura")
    o = hasura.execute_with_kwargs(GET_ORDER_BY_NAME, name=name)["sell_orders"]

    if validate:
        validate_order_id(o[0])
    return o


def get_make_order_by_order_name(name, hasura=None, validate=False):
    """
    the name of the order should be of the form BRAND-NUMBER
    in some older data we may have added it with the hash notation but this is wrong as its not unique
    """
    hasura = hasura or res.connectors.load("hasura")
    r = hasura.execute_with_kwargs(GET_MAKE_ORDER_BY_ORDER_NAME, name=name)[
        "make_one_orders"
    ]

    """
    validations:
     - the id and the oid should make sense
     - there should not be duplicate pieces
     - another type of validation on save is to see if piece ids that i would generate exist on any other order
       as this leads to an empty set returned - we could insect AFTER we find this state
     - the id and oid on the pieces should make sense
    these validations could all be used in a look ahead mode in full remake order
    
    """

    return r


def get_make_order_by_oid(oid, hasura=None):
    """
    the name of the order should be of the form BRAND-NUMBER
    in some older data we may have added it with the hash notation but this is wrong as its not unique
    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(GET_MAKE_ORDER_BY_ORDER_OID, oid=oid)[
        "make_one_orders"
    ]


def log_fulfillment_item_request(fid, hasura=None):
    Q = """mutation stamp_assembly_sent($id: uuid = "", $sent_to_assembly_at: timestamptz = "") {
        update_sell_order_item_fulfillments_by_pk(pk_columns: {id: $id}, _set: {sent_to_assembly_at: $sent_to_assembly_at}) {
            id
            sent_to_assembly_at
        }
        }
    """
    hasura = hasura or res.connectors.load("hasura")
    hasura.execute_with_kwargs(
        Q, id=fid, sent_to_assembly_at=res.utils.dates.utc_now_iso_string()
    )


def get_brands():
    graph = res.connectors.load("graphql")

    Q = """
    query brandShopifyCredentials($after:String) {
        brands(first:100, after:$after) {
            brands{
            name
            code
            shopifyStoreName
            }
        count
        cursor
        }
    }
    """

    result = graph.query(Q, {}, paginate=True)
    df = pd.DataFrame(result["data"]["brands"]["brands"])

    lu = dict(df[["name", "code"]].values)
    # add aliases - why are they there?
    lu["THE KIT."] = "KT"
    lu["Walker Wear"] = "AW"
    lu["LolaFaturotiLoves"] = "LL"
    lu["MeganRenee"] = "MR"
    lu["ALLCAPSTUDIO"] = "AS"
    lu["lfantbrand"] = "LT"
    return lu


def get_order_item_by_channel_order_item_id(id, hasura=None):
    """
    prod example: 12308770848843
    """
    hasura = hasura or res.connectors.load("hasura")
    r = hasura.execute_with_kwargs(GET_ORDER_ITEM_BY_CHANNEL_ID, id=id)
    if len(r["sell_order_line_items"]) > 0:
        return r["sell_order_line_items"][0]["id"]


def get_product_by_sku(sku, hasura=None):
    """
    By schema and design this sku is the bmc_sku which is a three part thing
    BMC should have three parts so we add the safety here but we need some sort of convention
    we would look up products by makes if we could trust the names
    """
    sku = sku.split(" ")[:3]

    sku = " ".join([s.strip() for s in sku])
    sku = _hyphenate_res_code(sku)
    hasura = hasura or res.connectors.load("hasura")
    Q = """
        query get_product_by_sku($sku: String = {})  {
          sell_products(where: {sku: {_eq: $sku}}) {
            name
            id
            style_id
          }
        }

        """
    r = hasura.execute_with_kwargs(Q, sku=sku)
    r = r["sell_products"]
    if len(r) == 0:
        raise Exception(f"Failed to resolve style by sku {sku}")
    return r[0]


def transfer_product_to_meta_one(pid, new_sku, old_sku):
    from res.flows.meta.ONE.meta_one import MetaOne

    sid = MetaOne(new_sku).style_id
    return transfer_product_to_new_style(
        pid, new_style_id=sid, metadata={"old_sku": old_sku}
    )


def transfer_product_to_new_style(pid, new_style_id, metadata={}, hasura=None):
    """
    normally a material swap will maintain a customer order sku -> product id -> style-id

    but if things change during processing such that the style id is invalidated we can link the old product to a new style
    this just changes the pointer so that all older ordered products point to something that we have

    except for reprocessing old data, it should be rare that a style changes between the time its ordered and fulfilled but it could happen

    """

    M = """mutation transfer_product_to_sku($pid: uuid = "", $style_id: uuid = "", $metadata: jsonb = "") {
      update_sell_products(where: {id: {_eq: $pid}}, _set: {style_id: $style_id, metadata: $metadata}) {
        returning {
          name
          metadata
          id
          sku
          style_id
          updated_at
        }
      }
    }

    """
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(
        M, pid=pid, style_id=new_style_id, metadata=metadata
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def update_order_item_ids(record):
    M1 = """mutation MyMutation($nid: uuid = "", $id: uuid = "") {
      update_sell_order_line_items(where: {id: {_eq: $id}}, _set: {id: $nid}) {
        returning {
          id
        }
      }
    }
    """
    hasura = res.connectors.load("hasura")
    return hasura.execute_with_kwargs(M1, id=record["id"], nid=record["nid"])


def update_order_name(id, name, hasura=None):
    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(UPDATE_ONE_ORDER_ORDER_NAME, id=id, name=name)


def update_product(style_sku, product_sku, name=None, add_self_ref=False, hasura=None):
    """
    for a known swap make an association between the style sku we have and the product sku inoming
    the product sku can still be ordered but it resolves to the sku for the current meta one
    """
    from res.flows.dxa.styles.queries import get_style_status_by_sku

    style_id = get_style_status_by_sku(style_sku, most_recent_only=True)["meta_styles"][
        0
    ]["id"]
    name = name.strip()

    product = {
        # product_sku -> style id
        "id": res.utils.uuid_str_from_dict({"product_sku": product_sku}),
        "style_id": style_id,
        "sku": product_sku,
        "name": name,  # the product name from before as probably represented on the ecom
        "metadata": {"style_sku": style_sku},
    }

    # the style sku can also be stored as a product but we dont have to - this is just a bootstrapping trick. really sell should manage their products
    #   generate a product whenever the style is created too for now and then add to them only if there are swaps
    product_self_ref = {
        # product_sku -> style id
        "id": res.utils.uuid_str_from_dict({"product_sku": style_sku}),
        "style_id": style_id,
        "sku": style_sku,
        "name": name,
        "metadata": {"style_sku": style_sku},
    }

    # sometimes useful to add self too for know products
    payload = [product, product_self_ref] if add_self_ref else [product]

    hasura = hasura or res.connectors.load("hasura")
    return hasura.execute_with_kwargs(UPDATE_PRODUCTS, products=payload)


def _bootstrap_products(data, on_error=None):
    """
    Loading the swaps add self styles for completeness. these so called self-styles are just the normal styles and their skus registered as products

    data loaded from somewhere but its a table of swaps - the idea is elsewhere we should add products as products
    """
    failed = []
    hasura = res.connectors.load("hasura")

    from tqdm import tqdm

    for record in tqdm(data.to_dict("records")):
        try:
            style_sku = record["sku"]
            product_sku = record["product_sku"]
            name = record["name"]

            update_product(style_sku, product_sku, name, hasura=hasura)
        except Exception as ex:
            failed.append({"record": record, "error": ex})
            if on_error == "raise":
                raise ex

    return failed


def resolve_product(name, sku, hasura=None, **kwargs):
    """
    we do not pre validate here because we are supposed to have strong gates well before this
    if the order fails we pager duty the entire record
    """
    return get_product_by_sku(sku, hasura=hasura)


def get_style_size(style_id, size_code, hasura=None):
    from res.flows.dxa.styles.queries import get_style_header_by_style_id_and_size

    # TODO test this for versioning
    r = get_style_header_by_style_id_and_size(
        style_id=style_id, size_code=size_code, hasura=hasura
    )
    r = r["meta_style_sizes"][0]
    style = r.get("style")
    style_sku = style["metadata"]["sku"]
    sku = f"{style_sku} {size_code}"
    return {"id": r["id"], "sku": sku, "name": style["name"]}


def style_instance_issuer(x):
    """
    This is just a test key for now, we dont flow by it
    Style UID: (Size), BODY-VERSION-COLOR-RANK_ISSUE_NUMBER <- we assume at style level there is only one way to make whatever is in the rank issue
    Instance UID: MakeIssueInstance
    Custom: if custom, the rank is just X and we incorporate into the make issue
    """
    return x


def try_track_fulfillment(
    make_order, make_item_instance, fulfillment_action, hasura=None
):
    """
    Used to create a suitable entry in the database to track an individual order item

    f_status pending when the make instance is def less than fulfillable quantity
    if something is partial then we can set the great ones to fulfilled
    if there is no status then it must be that they are all cancelled - especially the case when order state is cancelled
     but need to test the partial edits examples
    """

    M = """
    mutation update_fulfillment_record($record: sell_order_item_fulfillments_insert_input = {}) {
        insert_sell_order_item_fulfillments_one(object: $record, on_conflict: {constraint: order_fulfillment_pkey, 
            update_columns: [started_at,ended_at, make_order_id, order_item_id, order_item_ordinal,status]}) {
            id
        }
    }

    """

    try:
        hasura = hasura or res.connectors.load("hasura")
        # our line item
        item_id = make_order["order_line_item_id"]
        record = {
            "id": res.utils.uuid_str_from_dict(
                {"order_item_id": item_id, "order_item_ordinal": make_item_instance}
            ),
            "make_order_id": make_order["id"],
            "status": fulfillment_action,
            "started_at": res.utils.dates.utc_now_iso_string(),
            "order_item_id": item_id,
            "order_item_ordinal": make_item_instance,
        }

        r = hasura.execute_with_kwargs(M, record=record)

        return r["insert_sell_order_item_fulfillments_one"]
    except Exception as ex:
        res.utils.logger.warn(f"Failed to track fulfillment - ignoring for now:> {ex}")
        return None


def determine_fulfillment_action_from_status(
    make_item_instance, fulfillment_status, fulfillable_quantity
):
    if pd.isnull(fulfillment_status):
        fulfillment_status = "pending"
    fulfillment_action = "Pending"
    if fulfillment_status == "fulfilled":
        fulfillment_action = "Fulfilled"
    elif make_item_instance + 1 > fulfillable_quantity:
        # but if we dont, its not always clear. if everything is cancelled fine
        if fulfillment_status.lower() == "cancelled":
            fulfillment_action = "Cancelled"
        # but otherwise we don know how many things have been actually fulfilled yet
        # it could be there only n left to fulfil because stuff has shipped or it could be that the customer edited their order
        else:
            # not sure what to do here
            fulfillment_status = (
                "Cancelled" if fulfillment_status != "partial" else "Fulfilled"
            )
            # ^if we want to be smarter about this we need to track fulfillments

    return fulfillment_action


def enqueue_make_orders_for_created_order(
    update_order_response, cost=None, hasura=None, **kwargs
):
    """
    <res_make.orders.one_requests>

      'id'
      'order_line_item_id'  #our id
      'style_size_id'
      'sku' #resolved sku from the style
      'product_name'
      'order_date'
      'price'
      'email'
      'sales_channel'
      'sla_days_at_order',
      'metadata'
         'costs', 'revenue_share', order_channel, source_order_name, tags, original_item_ordered_quantity, ecom_ref, ordered_sku

    the make order response will issue a global one_number and write some sort of response
    it may fulfil from inventory or whatever and it can write back the fulfillment
    """
    # TODO run the OrderNode logic here to make sure that all checks pass for the requests
    # generate any contracts failing and add to the order/items

    # we can sometimes find that there are no valid line items and return None
    order = update_order_response["insert_sell_orders_one"]
    if order:
        line_items = update_order_response["insert_sell_order_line_items"]

        sla_days_at_order = None
        # cost lookup maybe
        for line_item in line_items["returning"]:
            # test this for create one
            existing_fulfillment_items = line_item.get(
                "insert_sell_order_line_items", []
            )
            if existing_fulfillment_items:
                res.utils.logger.debug(
                    "has existing items - we could use an update mode here"
                )
            fulfillable_quantity = line_item.get("fulfillable_quantity")
            fulfillment_status = line_item.get("status", None)

            for make_item_instance in range(line_item["quantity"]):
                # determine f_status - we def want to fulfill if we have more items that are fulfillable
                fulfillment_action = determine_fulfillment_action_from_status(
                    make_item_instance, fulfillment_status, fulfillable_quantity
                )

                product_id = line_item["product"]["id"]
                # this is resolved from a mapping that sell manages
                style_id = line_item["product"]["style_id"]
                # we store this sku which is different possibly from what we make with material swaps
                ordered_sku = line_item["sku"]
                size_code = ordered_sku.split(" ")[-1].strip()
                # style sizes are looked up pr the products style id and a size
                ss = get_style_size(style_id, size_code)
                # this is the style size id, the one we care about for making stuff
                ssid = ss["id"]
                # one_code = style_instance_issuer(ssid)
                make_order = {
                    # the requests may not generate a ONE - but the order date for the ssid is the unique request along with any customization
                    "id": res.utils.uuid_str_from_dict(
                        {
                            "style_size_id": ssid,
                            "customization": None,
                            "instance": order["ordered_at"],
                        }
                    ),
                    # "one_code": one_code,
                    # this is our make sku, not necessarily what was ordered
                    "sku": ss["sku"],
                    "style_size_id": ssid,
                    "product_name": ss["name"],
                    "order_date": order["ordered_at"],
                    # this is our id not the source one
                    "order_line_item_id": line_item["id"],
                    "email": order["email"],
                    "order_channel": order[
                        "order_channel"
                    ],  #'SHOPIFY' , #process constant
                    #'ECOM', #process constant or comes from the payload
                    "sales_channel": order["sales_channel"],
                    "price": line_item["price"],
                    "sla_days_at_order": sla_days_at_order,
                    "created_at": res.utils.dates.utc_now_iso_string(),
                    "quantity": line_item["quantity"],
                    # SA commenting this out - its not in the schema and i dont think we need it because its a function of the fulf items - we have it in metadata
                    # "fulfillment_quantity": line_item["fulfillment_quantity"],
                    "metadata": {
                        "order_name": order["name"],
                        "cost": cost,
                        "tags": kwargs.get("tags"),
                        "brand_code": kwargs.get("brand_code"),
                        "fulfillment_item_ordinal": make_item_instance,
                        "original_item_ordered_quantity": line_item["quantity"],
                        "ecom_ref": kwargs.get("ecom_ref"),
                        "source_order_id": order["source_order_id"],
                        "product_id": product_id,
                        # the ordered one may differ from the make one with swaps
                        "ordered_sku": ordered_sku,
                        "fulfillment_action": fulfillment_action,
                        "fulfillment_quantity": line_item.get(
                            "fulfillment_quantity", line_item.get("quantity")
                        ),
                    },
                }
                # using a subset of the make order, create a fulfillment entry and then post to kafka

                #            print(make_item_instance, fulfillable_quantity, fulfillment_action)
                result = try_track_fulfillment(
                    make_order, make_item_instance, fulfillment_action, hasura=hasura
                )
                # check the result to determine if we have just created this or was it already there
                make_order["metadata"]["item_fulfillment_id"] = result["id"]

                # if its already active and now changes we dont have to yield anythin

                yield make_order


def update_create_one_order(o, plan=False, hasura=None):
    """
    logic to map create-one orders into the database
    """
    # take a copy
    o = dict(o)

    hasura = hasura or res.connectors.load("hasura")

    def try_parse_line_items(li):
        if isinstance(li, list):
            return li
        try:
            return json.loads(li)
        except:
            try:
                return literal_eval(li)
            except:
                res.utils.logger.warn(f"Failing to parse line items")

        return li

    def clean_status(s):
        s = strip_emoji(s)
        s = snakecase(s).upper()
        s = s.strip("_").replace("__", "_")
        return s

    def make_customer(**customer_kwargs):
        hash_keys = [
            "email",
            "shipping_address1",
            "shipping_zip",
            "shipping_city",
            "shipping_country",
        ]
        other_keys = ["shipping_name", "shipping_province", "shipping_phone"]

        c = {}

        def cleaner(s):
            return snakecase(s.replace("_shipping", ""))

        for field in hash_keys:
            c[cleaner(field)] = customer_kwargs.get(field)
        cid = res.utils.uuid_str_from_dict(c)
        for other_key in other_keys:
            c[cleaner(field)] = customer_kwargs.get(field)
        return {"id": cid, "email": c["email"], "metadata": c}

    line_items = o.pop("lineItemsInfo")

    if len(line_items) == 0:
        res.utils.logger.warn(f"There are no line items in the order")
        # we can maybe build them from skus

    orders = pd.DataFrame([o])

    shipping_columns = [
        "shippingAddress1",
        "shippingCity",
        "shippingZip",
        "shippingCountry",
        "shippingProvince",
        "shippingName",
        "shippingPhone",
    ]

    # sometimes these cols are blank
    for col in shipping_columns:
        if col not in orders.columns:
            orders[col] = None

    orders = orders[
        [
            "id",
            "email",
            "requestName",
            "requestName",
            "brandCode",
            "createdAt",
            "orderStatus",
        ]
        + shipping_columns
    ].rename(
        columns={
            "requestName": str(o.get("number")),
            "createdAt": "ordered_at",
            "requestName": "name",
            "orderStatus": "status",
        }
    )

    # TODO: Ask what modes and statuses we can have on create-one orders
    # TYPE removed from the create one order or at least not save to the database
    # orders["type"] = "create"
    # set to the enum in the database
    orders["ecommerce_source"] = "CREATEONE"
    # to deal with airtable shite
    orders["brandCode"] = orders["brandCode"].map(
        lambda x: x if not isinstance(x, list) else x[0]
    )
    orders["order_channel"] = "CREATEONE"
    orders["sales_channel"] = o.get("salesChannel")
    orders["source_order_id"] = str(o.get("number"))
    orders["id"] = orders["id"].map(
        lambda x: res.utils.uuid_str_from_dict({"id": x, "order_channel": "create-one"})
    )
    # orders['customer_id'] = orders['customer_id'].map(lambda x : res.utils.uuid_str_from_dict({'id':x})) #maybe add the email and 1 line of address and postcode into the hash
    orders["tags"] = None
    orders["item_quantity_hash"] = None

    c = make_customer(**o)
    orders = orders[[c for c in orders.columns if c not in shipping_columns]]

    orders = res.utils.dataframes.snake_case_columns(orders)
    orders["status"] = orders["status"].map(clean_status)

    o = orders.to_dict("records")[0]
    o["customer_id"] = c["id"]

    # set based product resolution - get the object and parse out names and ids below
    line_items = try_parse_line_items(line_items)

    # grouped and reduced into qty by sku
    line_items = pd.DataFrame(line_items).groupby("sku").sum().reset_index()
    # now we need to generate an id
    # its unfortunate - create one should really manage quantities and edits but it does not so we have to ignore the record ids which are useless
    line_items["id"] = line_items["sku"].map(
        lambda x: res.utils.uuid_str_from_dict({"order_key": o["id"], "sku": x})
    )
    # dont add this without updating the schema as its pass through
    # line_items["fulfillment_quantity"] = line_items["quantity"]
    line_items = line_items.to_dict("records")

    line_col_defaults = ["id", "fulfillment_status", "price", "quantity", "sku", "name"]
    line_col_defaults = dict(
        zip(line_col_defaults, [None, None, 0, 1, None, "NOT SUPPLIED BY SOURCE"])
    )
    line_items = pd.DataFrame(line_items)
    for k, v in line_col_defaults.items():
        if k not in line_items.columns:
            line_items[k] = v
    line_items = line_items.rename(columns={"fulfillment_status": "status"})
    line_items["sku"] = line_items["sku"].map(_hyphenate_res_code)

    line_items = line_items[line_items["id"].notnull()].reset_index(drop=True)

    if len(line_items) == 0:
        res.utils.logger.info(
            f"If a client id is not specified for the line items we are not going to save the records."
        )
        return None
    # TODO - remove this because we are generating duplicates - if the id is null dont save it
    # look for orders like this 541482f6-31f9-24b6-cc31-922d6ff14fed_0 and remove them
    # why are ids sometimes null here!!!!! have some ideas but we should try not to do things like this -> here generate an id order_id_line_item_ordinal
    # can we manage order edits in create-one?
    line_items["id"] = line_items.apply(
        lambda row: row["id"] if not pd.isnull(row["id"]) else f"{o['id']}_{row.name}",
        axis=1,
    )

    line_items["source_order_line_item_id"] = line_items["id"].map(str)
    line_items["id"] = line_items["source_order_line_item_id"].map(
        lambda x: res.utils.uuid_str_from_dict({"id": x})
    )
    line_items["order_id"] = o["id"]
    line_items["price"] = line_items["price"].fillna(0)
    line_items["metadata"] = line_items.index.map(lambda x: {})
    # conform to the shopify spec for enqueue
    line_items["fulfillable_quantity"] = line_items["quantity"]
    line_items["product_id"] = line_items.apply(
        lambda row: resolve_product(row["name"], row["sku"], hasura=hasura).get("id"),
        axis=1,
    )
    line_items = line_items.to_dict("records")

    if plan:
        res.utils.logger.warn(f"Showing plan only - not saving")
        return {"order": o, "line_items": line_items}

    return hasura.execute_with_kwargs(
        UPDATE_ORDER, order=o, order_line_items=line_items, customer=c
    )


def update_shopify_order(o, plan=False, hasura=None):
    """
    insert from shopify or create one - depends on the existence of product mapping or for now maybe generate a placeholder unresolved product
    """

    o = dict(o)

    def filter_valid_one_sku(x):
        """
        simple filter - could do a lookup but really we want to just fail if we cannot resolve products normally
        this is more of a hint to void excessive data processing
        """
        if pd.notnull(x):
            return len(x.split(" ")) == 4
        return False

    hasura = hasura or res.connectors.load("hasura")

    line_items = o.pop("line_items")
    orders = pd.DataFrame([o])
    orders = orders[
        ["id", "customer_id", "email", "tags", "name", "brand", "created_at", "type"]
    ].rename(columns={"brand": "brand_code", "created_at": "ordered_at"})
    orders["ecommerce_source"] = "SHOPIFY"
    orders["order_channel"] = "SHOPIFY"
    orders["sales_channel"] = "ECOM"
    orders["id"] = orders["id"].map(
        lambda x: res.utils.uuid_str_from_dict({"id": x, "order_channel": "Shopify"})
    )
    orders["customer_id"] = orders["customer_id"].map(
        lambda x: res.utils.uuid_str_from_dict({"id": x})
    )  # maybe add the email and 1 line of address and postcode into the hash
    orders["tags"] = orders["tags"].map(lambda x: x.split(","))
    orders["item_quantity_hash"] = None

    o = orders.to_dict("records")[0]

    UPDATE_TYPE = o.pop("type")

    # TODO create a product resolver at the top level in batch. get the ids and names and the mapped them over the rest

    li_cols = [
        "id",
        "fulfillment_status",
        "price",
        "quantity",
        "sku",
        "name",
        "fulfillable_quantity",
    ]
    line_items = pd.DataFrame(line_items)

    # FROM EXTERNAL WE MUST MAKE THE SKU LOOKS LIKE A ONE SKU BY WHATEVER LOGIC
    line_items = line_items[line_items["sku"].map(filter_valid_one_sku)]
    # we need some default on the line items
    if len(line_items) == 0:
        res.utils.logger.warn(f"NO ITEMS AFTER FILTERING ON VALID SKUS")
        return line_items
    line_items = line_items[li_cols].rename(
        columns={"fulfillment_status": "status", "id": "source_order_line_item_id"}
    )

    line_items["source_order_line_item_id"] = line_items[
        "source_order_line_item_id"
    ].map(str)
    line_items["id"] = line_items["source_order_line_item_id"].map(
        lambda x: res.utils.uuid_str_from_dict({"id": x})
    )
    line_items["order_id"] = o["id"]
    line_items["metadata"] = line_items.index.map(lambda x: {})

    line_items["sku"] = line_items["sku"].map(_hyphenate_res_code)
    line_items["product_id"] = line_items.apply(
        lambda row: resolve_product(row["name"], row["sku"], hasura=hasura)["id"],
        axis=1,
    )

    # Check states TODO: it seems like the order item does not have the cancel state if the parent order is cancelled but we would
    # want the ones to be cancelled -> what about the fulfillment records?
    # not sure what it looks like when an order is partially cancelled

    if UPDATE_TYPE == "cancel":
        # cancel all - the fulfillable quantity will be 0
        res.utils.logger.debug("Setting cancel status on orders")
        line_items["status"] = "Cancelled"

    line_items = line_items.to_dict("records")

    ##
    # generate the order item fulfillment model here for each of the item quants
    #  simple f(order_item_id, instance_ordinal) -> make_order_id
    # might be nicer to do this in response to the insert actually
    ##
    if plan:
        return o, line_items

    return hasura.execute_with_kwargs(
        UPDATE_ORDER,
        order=o,
        order_line_items=line_items,
        customer={"id": o["customer_id"], "email": o["email"]},
    )


def get_style_by_style_sku(style_sku):
    from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

    graphql_client = ResGraphQLClient()
    # graphql_client.
    style_data = graphql_client.query(
        GET_STYLE_RESONANCE_CODE,
        variables={"resonance_code": style_sku},
        paginate=False,
    )
    return style_data


def _snowflake_get_kafka_message_by_order_rid(rid):
    """
    an example usage:
    An order with multiple items with 1 qty per same sku

        df = pd.DataFrame(get_kafka_message_by_order_rid('recFdUoVmWtNHYiNA'))
        a = dict(df.iloc[-1])
        a

    to test saving an older or try
    a = _snowflake_get_kafka_message_by_order_rid('recFdUoVmWtNHYiNA')[-1]
    update_create_one_order(a, plan=False) #plan True to check transformations of data before saving

    TODO: we can alternatively look for messages in the dead letter queue if they were dropped
    """

    import json

    snowflake = res.connectors.load("snowflake")
    Q = f"""  SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_SELL_CREATE_ONE_ORDERS_ORDERS" WHERE parse_json(RECORD_CONTENT):id::string = '{rid}' """
    data = snowflake.execute(Q)
    data["RECORD_CONTENT"] = data["RECORD_CONTENT"].map(json.loads)
    return list(data["RECORD_CONTENT"])


def get_warehouse_orders_by_brand_and_order_number(brand_code, order_number):
    pass


def get_warehouse_batch_orders_by_source_order_ids(oids):
    pass


def get_warehouse_orders_by_source_order_id(oid):
    """
    eg. by 4337914806481
    """
    snowflake = res.connectors.load("snowflake")

    data = snowflake.execute(
        f""" SELECT * FROM   "RAW"."SHOPIFY"."ORDERS" WHERE "ORDER_ID" = {oid} """
    ).to_dict("records")
    # assert len(data) == 0, "did not expect multiple records"
    data = data[-1]
    oid = data["SHOP_ORDER_ID"]
    ldata = snowflake.execute(
        f""" SELECT * FROM   "RAW"."SHOPIFY"."ORDER_LINE_ITEMS" WHERE "SHOP_ORDER_ID" = '{oid}' """
    )
    data["line_items"] = ldata.to_dict("records")

    return data


def get_warehouse_airtable_batch_orders_by_source_record_ids(record_ids):
    pass


def get_warehouse_airtable_orders_by_source_record_id(record_id):
    """
    example pass recUannB2IkBktQp3
                 recjnZWMOpvoHMiAq
    """

    snowflake = res.connectors.load("snowflake")

    Q = f""" SELECT * FROM  "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" where  "ORDER_ID" = '{record_id}' and "VALID_TO" is NULL """
    order = snowflake.execute(Q)
    # assert unique
    order = order.to_dict("records")[-1]
    order_id = order["ORDER_ID"]

    Q = f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS" WHERE "ORDER_ID" = '{order_id}' and "VALID_TO" is NULL """
    order_items = snowflake.execute(Q).to_dict("records")
    order["line_items"] = order_items

    return order


def get_warehouse_airtable_orders_by_source_order_key(order_key):
    """
    example pass recUannB2IkBktQp3
                 recjnZWMOpvoHMiAq
    """

    snowflake = res.connectors.load("snowflake")

    Q = f""" SELECT * FROM  "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" where  "ORDER_KEY" = '{order_key}' and "VALID_TO" is NULL """
    order = snowflake.execute(Q)

    # clean
    for c in [
        "SHIPPING_NAME",
        "SHIPPING_STREET",
        "SHIPPING_ADDRESS_LINE_ONE",
        "SHIPPING_ADDRESS_LINE_TWO",
        "SHIPPING_ZIP",
        "SHIPPING_CITY",
        "SHIPPING_PROVINCE",
        "SHIPPING_COUNTRY",
        "CUSTOMER_EMAIL",
        "SALES_CHANNEL",
    ]:
        if c in order.columns:
            order[c] = order[c].fillna("")

    # assert unique
    order = order.to_dict("records")[-1]
    order_id = order["ORDER_ID"]

    Q = f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS" WHERE "ORDER_ID" = '{order_id}' and "VALID_TO" is NULL """
    order_items = snowflake.execute(Q).to_dict("records")
    order["line_items"] = order_items

    return order


def get_create_one_order_from_kafka_messages(order_number):
    """
    Using the kafka messages that we receive before saving to hasura, for cerate one orders
    can return multiple events for the same order
    e.g CF-2144105
    """
    snowflake = res.connectors.load("snowflake")
    snow = snowflake.execute(
        f""" select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_SELL_CREATE_ONE_ORDERS_ORDERS" where  parse_json(RECORD_CONTENT):key::string = '{order_number}'   """
    )
    snow["payload"] = snow["RECORD_CONTENT"].map(json.loads)
    return list(snow["payload"])


def get_shopify_order_from_kafka_messages(order_number):
    """
    Using the kafka messages that we receive before saving to hasura, for cerate one orders
    can return multiple events for the same order
    e.g KT-73837
    """
    brand_code = order_number.split("-")[0]
    number = order_number.split("-")[1]
    snowflake = res.connectors.load("snowflake")
    snow = snowflake.execute(
        f""" select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_SELL_SHOPIFY_ORDERS" where  parse_json(RECORD_CONTENT):brand::string = '{brand_code}' and parse_json(RECORD_CONTENT):name::string = '#{number}'   """
    )
    snow["payload"] = snow["RECORD_CONTENT"].map(json.loads)
    return list(snow["payload"])


def bulk_load_from_fulfillments_warehouse(
    date=None,
    window=(0, 10000),
    page=None,
    page_size=None,
    offset=0,
    load_airtable=False,
):
    """
    treat as snippet, needs some work - we can load everything from our warehouse into the model in each env
    """

    from schemas.pydantic.sell import Order

    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    snowflake = res.connectors.load("snowflake")

    Q = f""" SELECT ORDER_ID FROM  "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" WHERE  "VALID_TO" is NULL """
    orders = snowflake.execute(Q)

    order_numbers = list(orders["ORDER_ID"])
    res.utils.logger.info(f"PREFETCH {len(order_numbers)} ORDERS")
    total_fetch = len(order_numbers)

    if page is not None and page_size is not None:
        # protect from zero indexing
        start = page_size * (page + 1) - page_size
        start += offset
        window = (start, start + page_size)

    if window[0] >= total_fetch:
        res.utils.logger.warn(f"returning, window exceeds total")
    if window[-1] >= total_fetch:
        window = (window[0], min(window[-1], total_fetch))

    res.utils.logger.info(f"Fetching window {window}")

    order_numbers = ",".join([f"'{f}'" for f in order_numbers[window[0] : window[1]]])

    Q = f""" SELECT * FROM  "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" WHERE ORDER_ID in ({order_numbers}) AND "VALID_TO" is NULL """
    orders = snowflake.execute(Q)
    Q = f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS"WHERE ORDER_ID in ({order_numbers}) AND "VALID_TO" is NULL """
    order_items = snowflake.execute(Q)  # .to_dict("records")

    # clean
    for c in [
        "SHIPPING_NAME",
        "SHIPPING_STREET",
        "SHIPPING_ADDRESS_LINE_ONE",
        "SHIPPING_ADDRESS_LINE_TWO",
        "SHIPPING_ZIP",
        "SHIPPING_CITY",
        "SHIPPING_PROVINCE",
        "SHIPPING_COUNTRY",
        "CUSTOMER_EMAIL",
        "SALES_CHANNEL",
    ]:
        orders[c] = orders[c].fillna("")

    print(len(orders), len(order_items))
    F = None
    for i, order in enumerate(orders.to_dict("records")):
        try:
            # print(f"loading {i}")
            items = order_items[order_items["ORDER_ID"] == order["ORDER_ID"]].to_dict(
                "records"
            )
            # print(len(items))
            order["line_items"] = items

            if len(items) > 0:
                if pd.isnull(order["CREATED_AT"]):
                    order["CREATED_AT"] = items[0]["ORDERED_AT"]
                    print("filling in blank order date")

                O = Order.from_create_one_warehouse_payload(order)
                print(O.name, i, window)
                # we only need to construct this once - and this does all the cache lookup stuff so we dont want to keep doing it
                F = FulfillmentNode(O) if F is None else F
                # when bulk loading just save to hasura and ignore the rest
                F.run(
                    O,
                    relay_queue=load_airtable,
                    update_queue=False,
                    disable_relay_kafka=True,
                )
        except Exception as ex:
            res.utils.logger.warn(f"Failed on {ex}")

    print("DONE")
    return (i, *window, total_fetch)


def reload_order_by_name(order_name):
    from schemas.pydantic.sell import Order

    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    order = get_warehouse_airtable_orders_by_source_order_key(order_key=order_name)
    O = Order.from_create_one_warehouse_payload(order)
    F = FulfillmentNode(O)
    # when bulk loading just save to hasura and ignore the rest
    F.run(
        O,
        relay_queue=False,
        update_queue=False,
        disable_relay_kafka=True,
    )

    return get_order_by_name(order_name)


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def redirect_old_order_products(old_id, new_id, hasura=None):
    M = """mutation migrate_product_ids($new_id: uuid = "", $old_id: uuid = "") {
          update_sell_order_line_items(where: {product_id: {_eq: $old_id}}, _set: {product_id: $new_id}) {
            returning {
              id
              product_id
            }
          }
        }
    """
    hasura = hasura or res.connectors.load("hasura")

    return hasura.execute_with_kwargs(M, old_id=old_id, new_id=new_id)


def update_swapped_product(swaps, hasura=None):
    """
    utility to migrate things between products - this SHOULD be done continuously e.g. in response to materials swaps

    look for `merge_swaps_to_cache` in the styles utility - a cron job will maintain swaps there in future

    we should be doing the swaps in such a way that new orders are already pointing to new products but we can also just point old orders to whatever products we currently want to make

    """
    from tqdm import tqdm

    hasura = hasura or res.connectors.load("hasura")

    for record in tqdm(swaps.to_dict("record")):
        pold = get_product_by_sku(record["old_sku"])

        pnew = get_product_by_sku(record["new_sku"])

        # if we dont have something register it
        # update
        results = redirect_old_order_products(
            old_id=pold["id"], new_id=pnew["id"], hasura=hasura
        )["update_sell_order_line_items"]["returning"]
        # print(len(results), "updated")
