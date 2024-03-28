"""

This is for processing orders on the way and on the way out
We collect all relevant order data onto a topic and store in database

when shipping we solve assignment problems 

we filter out items that have no SKUs / That are not really ONEs

This is pre make so we dont really have metadata in and around and beyond dxa - for example it is not really our job to know the body version but we can log it in metadata

We can test with: http://localhost:8888/notebooks/docs/notebooks/nodes/SellMake.ipynb
       the interface here is for converting stuff from shopify, create-one into or make order interface
       so the notebook is a good way to test that

"""

import pandas as pd
import numpy as np
from stringcase import snakecase
import res
from res.flows.sell.orders import queries
import traceback

"""
add customer affinity hash 

derive:

- sku history / material swaps
- costs from scd costs 
- revenue share stuff / other create one available figures 
- SLA 
- tax
- discount 
- {priority}
- {materials used}
- {determine body version to use}
- {brand balance metadata}
- {order tags, order quantity, order tax, order discounts}

"""
# -> "res_make.orders.one_responses"
KAFKA_RESPONSE_TOPIC = "res_make.orders.one_requests"


def make_metadata(order_row):
    d = {
        "revenue_share": None,
        "brand_priority": None,
        "brand_balance_at_order": None,
        "current_body_version": None,
        "current_meta_one_ref": None,
    }

    for k, v in order_row.items():
        if "shipping" in k:
            d[snakecase(k)] = v
    return d


def norm_sku(s, brand):
    first_term = s.split(" ")[0]
    # print(first_term,brand)
    # first_term = first_term.replace(brand, f"{brand}-")
    first_term = f"{first_term[:2]}-{first_term[2:]}"
    return f"{first_term} {' '.join(s.split(' ')[1:])}"


def filter_valid_sku(df):
    def _vs(s):
        if pd.isnull(s):
            return False

        return 4 == len(s.split(" "))

    df = df[df["sku"].map(_vs)].reset_index(drop=True)

    return df


def cache_swaps_lookup():
    """
    convenience method to lookup known swaps
    """
    airtable = res.connectors.load("airtable")
    swap_history = airtable["appKGWiGPT52oCPC4"]["tblskmrpElqrf7T5x"].to_dataframe()
    style_data = airtable["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"].to_dataframe(
        fields=[
            "Style ID",
            "Resonance_code",
            "Materials Used Codes",
            "Available Sizes Ids",
            "Style Name",
        ]
    )
    X = swap_history.drop_duplicates(
        subset=["Style ID", "Old Material Code", "New Material Code"], keep="last"
    )
    swap_history = swap_history[
        ["Style ID", "Status", "Started At", "Old Material Code", "New Material Code"]
    ]
    completed_swaps = swap_history[swap_history["Status"] == "Done"]

    def make_alias(row):
        parts = row["Resonance_code"].split(" ")
        parts = [p.strip() for p in parts]
        parts[1] = row["Old Material Code"].strip()
        return " ".join(parts)

    S = pd.merge(X, style_data, left_on="Style ID", right_on="record_id")[
        ["Resonance_code", "Old Material Code", "Style Name"]
    ]
    S["alias"] = S.apply(make_alias, axis=1)
    S = S[S["alias"] != S["Resonance_code"]]
    S = S.reset_index()
    res.connectors.load("s3").save(
        "s3://res-data-platform/samples/known-swaps.feather", S
    )

    return S


def _generic_handler(action, event, context=None, skip_make_update=False):
    """
    process create one orders - currently these come via res-connect but it would be good to write them to kafka
    states:
    1. When we call the action we can see if we can save the order to the database - have we got a product relationship and basic checks pass
       if not, we create a flag somewhere - a dead letter queue might be enough as an ingestion error
    2. if we save the order we can run business checks on it e.g. its not in a HOLD state
    3. if we decide to fulfill the item we can create a shipping queue entry in assembly with information about the group of things that needs to be fulfilled
    4. if we have to make some items we can request to make some items
    5. at some later point we need to decide how to fulfill the items for the entire order
    """
    hasura = res.connectors.load("hasura")
    kafka = res.connectors.load("kafka")
    brand = event.get("brand")
    if isinstance(brand, list):
        brand = brand[0]

    # 0. check that the action does not fail and if it does record the contract failing and update flow api straight away

    call_back = action(event, hasura=hasura)

    ########

    if len(call_back) == 0:
        return {}
    UPDATE_TYPE = event.get("type", "").lower()
    # check the status: only if the mode is create should we send a make request
    res.utils.logger.info(f"Event mode is {UPDATE_TYPE}. Processing order items...")
    # we use the database callback to construct the payload to relay - there are multiple items per order header
    for payload in queries.enqueue_make_orders_for_created_order(
        call_back, brand_code=brand, update_type=UPDATE_TYPE, hasura=hasura
    ):
        if not payload:
            continue
        # actually this is redundant - the upstream process can generate the shipping request - but we should add any contracts

        # # create the entry for the thing that needs a shipping from make or warehouse or add any flags such as HOLDS - the shipping queue
        # api = FlowAPI(OrderItem)
        # # validate the thing before here and add contracts to order item if required
        # api.update(OrderItem(payload))

        if UPDATE_TYPE == "create" and not skip_make_update:
            kafka[KAFKA_RESPONSE_TOPIC].publish(payload, use_kgateway=True, coerce=True)
        else:
            res.utils.logger.debug(
                f"We will update the database but not issue a make request for event type {UPDATE_TYPE} with option {skip_make_update}"
            )
    return event


def create_resonance_legacy_order(
    line_items,
    plan=False,
    sales_channel="Development",
    brand_code="RS",
    remove_sku_hyphen=True,
):
    def process_line_item(li):
        # load style ids?
        if "quantity" not in li:
            li["quantity"] = 1

        if remove_sku_hyphen:
            # careful not to remove from SELF--
            sku = li["sku"]
            if sku[2] == "-":
                li["sku"] = f"{sku[:2]}{sku[3:]}"
        # do we need the style id or just the sku

        return li

    # tech pirates email and DR address
    # resonance brand and request name auto gen
    g = res.connectors.load("graphql")
    custom_order_context = {
        "email": "techpirates@resonance.nyc",
        "orderChannel": "resmagic.io",
        "salesChannel": sales_channel,
        "shippingName": "Resonance Manufacturing LTD",
        "shippingAddress1": "Corporacion Zona Franca Santiago",
        "shippingAddress2": "Segunda Etapa, Calle Navarrete No.4",
        "shippingPhone": "00000000",
        "shippingCity": "Santiago",
        "shippingCountry": "Dominican Republic",
        "shippingProvince": "Santiago",
        "shippingZip": "51000",
    }

    UORDER = """mutation createMakeOneOrder1($input: CreateMakeOneOrderInput!) {
      createMakeOneOrder(input: $input) {
        order {
          id
          name
        }
      }
    }"""

    # resolve line items style ids

    line_items = [process_line_item(li) for li in line_items]

    order = {
        # generate some order name
        "requestName": f"{brand_code}-ONE-{res.utils.res_hash()}",
        # use the default resonance brand or whatever is passed
        "brandId": brand_code,
        # line items
        "lineItemsInfo": line_items,
    }

    order.update(custom_order_context)

    if plan:
        res.utils.logger.info("not sending, this would be sent if mode is no [plan]")
        return order

    res.utils.logger.info(f"Posting...")

    r = g.query_with_kwargs(UORDER, input=order)

    try:
        res.utils.logger.info(f"Got a response {r}")
        rid = r["data"]["createMakeOneOrder"]["order"]["id"]
        res.utils.logger.info(
            f"submitted order. visit status at https://airtable.com/appfaTObyfrmPHvHc/tblhtedTR2AFCpd8A/viwB8QIW1xjE8rlqw/{rid}"
        )
    except:
        pass
    return r


def create_legacy_order(row, plan=True):
    """
    this is to clone an order from the RAW schema type in snowflake and regenerate
    we would rarely need to do

    sample input

    {'ONE_NUMBER': 10316237,
    'ORDER_NUMBER': 27601.0,
    'LINE_ITEM_SKU': 'JR-3118 CTJ95 ATDMLI 4ZZLG',
    'CUSTOMER_EMAIL': 'shaunlfletcher@gmail.com',
    'SALES_CHANNEL': 'Ecom',
    'TOTAL_SHIPPING_PRICE_USD': 0.0,
    'SHIPPING_NAME': 'Shaun FLETCHER',
    'SHIPPING_LAST_NAME': 'FLETCHER',
    'SHIPPING_PHONE': None,
    'SHIPPING_ADDRESS_ONE': '3654 35th St',
    'SHIPPING_ADDRESS_TWO': None,
    'SHIPPING_CITY': 'Long Island City',
    'SHIPPING_PROVINCE': 'New York',
    'SHIPPING_COUNTRY': 'United States',
    'SHIPPING_ZIP': '11106',
    'SHIPPING_LATITUDE': None,
    'SHIPPING_LONGITUDE': None}
    """

    sku = row["LINE_ITEM_SKU"]
    size_code = sku.split(" ")[-1].strip()
    sku = " ".join(sku.split(" ")[:-1])

    # client for graph
    g = res.connectors.load("graphql")

    # some queries and mutations
    UORDER = """mutation createMakeOneOrder1($input: CreateMakeOneOrderInput!) {
      createMakeOneOrder(input: $input) {
        order {
          id
          name
        }
      }
    }"""

    QSTYLE = """query getStyle($sku: String!) {
            style(code: $sku) {
                id
                code
                name
                brand{
                    code
                }
                body {
                    basePatternSize{
                        code
                    }

                }
            }

        }
    """

    # get some data about the style
    s = g.query_with_kwargs(QSTYLE, sku=sku)["data"]["style"]
    sample_size = s["body"]["basePatternSize"]["code"]
    size_code = size_code or sample_size
    style_id = s["id"]
    brand_id = s["brand"]["code"]

    sku = f"{sku} {size_code}"
    body = sku.split(" ")[0]
    if "-" in body:
        sku = sku.replace(body, body.replace("-", ""))
    # construct a line item for this sku
    line_items = [
        {
            "id": style_id,
            "sku": sku,
            "quantity": 1,
        }
    ]

    # construct the full order
    order = {
        "requestName": f"{sku.replace(' ','')} REORDER",
        #         "key": "?",
        #         "code": "?",
        #         "number": "?",
        "brandId": f"{brand_id}",
        # "fulfillmentId": None,
        "email": row["CUSTOMER_EMAIL"],
        "salesChannel": "ECOM",
        "orderChannel": "resmagic.io",
        "shippingName": row["SHIPPING_NAME"],
        "shippingAddress1": row["SHIPPING_ADDRESS_ONE"],
        "shippingAddress2": row["SHIPPING_ADDRESS_TWO"],
        "shippingCity": row["SHIPPING_CITY"],
        "shippingPhone": row["SHIPPING_PHONE"] or "00000000",
        "shippingCountry": row["SHIPPING_COUNTRY"],
        "shippingProvince": row["SHIPPING_PROVINCE"],
        "approvedForFulfillment": "Approved",
        # "flagForReviewReasonTag": "SEW_BODY_TEST",
        "shippingZip": row["SHIPPING_ZIP"],
        "lineItemsInfo": line_items,
    }

    if plan:
        res.utils.logger.info("not sending, this would be sent if mode is no [plan]")
        return order

    res.utils.logger.info(f"Posting...")

    r = g.query_with_kwargs(UORDER, input=order)

    try:
        res.utils.logger.info(f"Got a response {r}")
        rid = r["data"]["createMakeOneOrder"]["order"]["id"]
        res.utils.logger.info(
            f"submitted order. visit status at https://airtable.com/appfaTObyfrmPHvHc/tblhtedTR2AFCpd8A/viwB8QIW1xjE8rlqw/{rid}"
        )
    except:
        pass
    return r


def analyze_sku_status(sku, check_meta_one_exists=False, check_product_exists=False):
    """
    when we cannot make a product link we can try to explain why...
    """

    # do we have body in 3d
    # do we have style in 3d or swap of it
    # do we have a product link
    # do we have a meta one

    # its better that we check the product first because we should have a link for historic thhings that can no longer be fetch by sku

    try:
        g = res.connectors.load("graphql")

        # make sure its hyphenated
        if sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"
        style_sku = " ".join(sku.split(" ")[:3])

        res.utils.logger.debug(f"Requesting style for sku {style_sku}")
        r = g.query_with_kwargs(
            queries.GET_GRAPH_API_STYLE_HEADER_BY_SKU, sku=style_sku
        )["data"]
        sheader = r.get("style")
        # print(sheader)
        style_exists = False
        in3d = False
        product_ref_exists = None
        if sheader:
            style_exists = True
            in3d = sheader["isStyle3dOnboarded"]
        else:
            # if these fail, we just need to register the product and try again
            if check_meta_one_exists:
                res.utils.logger.info(f"Checking if the style exists ")
            if check_product_exists:
                res.utils.logger.info(f"Checking if the product link exists ")
        return {
            "sku": sku,
            "body": sheader.get("body") if sheader else None,
            "in3d": in3d,
            "style_exists": style_exists,
            "product_ref_exists": product_ref_exists,
        }
    except:
        res.utils.logger.warn(f"Failed to load try again later ")
        return {}


@res.flows.flow_node_attributes(
    memory="4Gi",
)
def get_next_1500(event, context={}, plan=False):
    # get all order numbers
    # get the ones that are not save
    # reverse from latest, take 1500 in this job and save them
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode
    from schemas.pydantic.sell import Order

    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")
    snowflake = res.connectors.load("snowflake")

    Q = f""" SELECT ORDER_ID, ORDER_KEY, EXTERNAL_ORDER_ID,  CREATED_AT  FROM  "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" WHERE  "VALID_TO" is NULL and CUSTOMER_EMAIL <> 'dgomez@resonance.nyc' """
    # most recent first
    orders = snowflake.execute(Q).sort_values("CREATED_AT", ascending=False)

    order_numbers = list(orders["ORDER_ID"])
    res.utils.logger.info(f"PREFETCH {len(order_numbers)} ORDERS")
    total_fetch = len(order_numbers)

    Q = """query MyQuery {
      sell_orders {
        name
        order_channel
        id
        brand_code
        source_order_id
        updated_at
      }
    }
    """
    od = pd.DataFrame(hasura.execute_with_kwargs(Q)["sell_orders"])
    print("orders from hasura", len(od), f"total order fetch was {total_fetch}")
    chk_base = pd.merge(
        orders, od, left_on=["ORDER_KEY"], right_on="name", how="left"
    ).sort_values("CREATED_AT", ascending=False)

    chk_base["has"] = chk_base["name"].notnull()

    metric = chk_base.groupby("has").count()[["ORDER_ID"]].T
    print(float(metric[True] / metric.sum(axis=1)), "completed")

    page_size = 1500
    count_status_good = 0
    count_status_bad = 0

    for i in range(10):
        pagest = page_size * i
        pageed = 1500 + pagest
        res.utils.logger.info(f"Proc {pagest}:{pageed}")
        chk = chk_base[chk_base["has"] == False].reset_index()[pagest:pageed]

        if plan:
            return chk

        order_numbers = chk["ORDER_ID"].unique()
        order_numbers = ",".join([f"'{f}'" for f in order_numbers])

        res.utils.logger.info("Selected missing orders")
        Q = f""" SELECT * FROM  "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" WHERE ORDER_ID in ({order_numbers}) AND "VALID_TO" is NULL """
        orders = snowflake.execute(Q)

        Q = f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS" WHERE ORDER_ID in ({order_numbers}) AND "VALID_TO" is NULL """

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

        print(len(orders), len(order_items), "most recent first")

        orders = orders.sort_values("CREATED_AT", ascending=False)

        F = None
        for i, order in enumerate(orders.to_dict("records")):
            try:
                print(
                    f"loading {i} - order {order['EXTERNAL_ORDER_ID']} - {order['ORDER_ID']}"
                )
                items = order_items[
                    order_items["ORDER_ID"] == order["ORDER_ID"]
                ].to_dict("records")
                # print(len(items))
                order["line_items"] = items

                if len(items) > 0:
                    if pd.isnull(order["CREATED_AT"]):
                        order["CREATED_AT"] = items[0]["ORDERED_AT"]
                        print("filling in blank order date")

                    O = Order.from_create_one_warehouse_payload(order)
                    result = FulfillmentNode.run_request_collector(O)

                    # res.utils.logger.info("Writing request payloads")
                    # s3.write(
                    #     f"s3://res-data-platform/checkpoints/make-order-requests/{O.name}.json",
                    #     json.dumps(result),
                    # )

                    print(f"saved {O.id} <- {O.name}")
                    count_status_good += 1
            except KeyboardInterrupt:
                print("Interrupted")
                count_status_bad += 1
                break
            except Exception as ex:
                res.utils.logger.warn(f"Failed on {ex}")

    res.utils.logger.send_message_to_slack_channel(
        "sirsh-test",
        f"{count_status_bad=} {count_status_good=} when doing a historical re-import. TODO add consistently failing cases and skip",
    )
    return {}


@res.flows.flow_node_attributes(
    memory="2Gi",
)
def bulk_make_requests_backup(event, context={}):
    import json
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")

    full = [
        f
        for f in list(s3.ls(f"s3://res-data-platform/checkpoints/make-order-requests/"))
    ]
    existing = [f.split("/")[-1].split(".")[0] for f in full]
    Q = """query MyQuery  {
    sell_orders {
        brand_code
        name
    }
    }
    """
    print(len(existing), "existing cached paylaods")
    df = pd.DataFrame(hasura.execute_with_kwargs(Q)["sell_orders"])
    orders = df[df.apply(lambda row: f"{row['brand_code']}-" in row["name"], axis=1)]
    print(len(orders), "orders in hasura")
    orders = orders[~orders["name"].isin(existing)]
    orders = list(orders["name"])
    print(len(orders), "TODO", "working....")
    for order_number in orders:
        try:
            payload = FulfillmentNode.get_make_order_request_for_order(order_number)
            uri = f"s3://res-data-platform/checkpoints/make-order-requests/{order_number}.json"
            s3.write(uri, json.dumps(payload))
            res.utils.logger.info(f"wrote {uri}")
        except:
            res.utils.logger.info(f"failed on {order_number}")


def _requests_to_one_assignment(order_number, requests):
    """
    having generated some requests for an order number we can augment with what is currently in the order map
    this is used with in the fulf node static method that generates the requests in the first place
    """
    from res.flows.make.production.queries import get_order_map_from_bridge_table

    mp = get_order_map_from_bridge_table(order_number)

    def get_one_number_from_map_by_rank(sku, rank):
        sku_map_ones = mp.get(sku, [])
        if len(sku_map_ones) >= rank:
            return sku_map_ones[rank - 1]
        return None

    for r in requests:
        if r.get("one_number", None) != 0:
            r["one_number"] = get_one_number_from_map_by_rank(
                r["sku"], r["metadata"]["fulfillment_item_ordinal"]
            )
        # else dont try to overwrite the same ordinal but maybe warn if we cannot make a full assignment

    # for r in requests:
    #     if not r.get("one_number", None):
    #         res.utils.logger.warn(
    #             f"It was not possible to assign one numbers to all requests"
    #         )
    #         res.utils.logger.metric_exception_log_incr(
    #             "sell_order_lookup_from_map", "order_sync"
    #         )
    #         break
    #         # we could raise if this is illegal but often not
    return requests


####
##  the following are temporary legacy maint. tasks
####
def reload_rrm(event={}, context={}):
    """
    temp cache demand model
    """
    from res.flows.api.rrm import ResilientResonanceMap
    from stringcase import snakecase
    from res.connectors.airtable import formatting
    from res.utils.dataframes import remove_object_types, replace_nan_with_none
    import pandas as pd

    def san_list(s):
        try:
            return s[0] if isinstance(s, list) and len(s) else str(s)
        except:
            return str(s)

    def okey(row):
        return row["key"].split("_")[0]

    def clean(s):
        s = s.replace("_", " ").replace("  ", " ").lower()
        return snakecase(s).lstrip("_").rstrip("_")

    def sp(sl):
        def f(s):
            return " ".join(s.split(" ")[:3]) if pd.notnull(s) else None

        return set([f(s) for s in sl if s != "None" and s != None])

    res.utils.logger.info(f"Loading orders from base fulf....")
    # need to now the

    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")

    order_item_data = airtable.get_table_data(
        "appfaTObyfrmPHvHc/tblUcI0VyLs7070yI",
        fields=[
            "KEY",
            "Shipping Tracking Number Line Item",
            "SKU",
            "SKU Mismatch",
            "BRAND_CODE",
            "Flag for Review",
            "lineitem_id",
            "__order_channel",
            "FULFILLMENT_STATUS",
            "Sales Channel",
            "Cancelled at",
            "Created At",
            "Active ONE Number",
            "_make_one_production_request_id",
            "Reservation Status",
            "Flag For Review Tag",
            "Email",
            "Last Updated At",
        ],
    )

    res.utils.logger.info(f"transforming order items")
    OI = order_item_data.copy()
    OI.columns = [clean(s) for s in order_item_data.columns]

    for col in [
        "brand_code",
        "flag_for_review_tag",
        "order_channel",
        "sales_channel",
        "email",
    ]:
        OI[col] = OI[col].map(san_list)

    for col in ["reservation_status", "sku_mismatch"]:
        OI[col] = OI[col].map(lambda x: formatting.strip_emoji(str(x)))

    OI["order_name"] = OI.apply(okey, axis=1)
    OI = replace_nan_with_none(OI)
    # OI = remove_object_types(OI)
    OI["sku"] = OI["sku"].map(lambda x: f"{x[:2]}-{x[2:]}")
    OI["cancelled_at"] = pd.to_datetime(OI["cancelled_at"], utc=True)
    OI["timestamp"] = pd.to_datetime(OI["timestamp"], utc=True)
    OI["created_at"] = pd.to_datetime(OI["created_at"], utc=True)
    OI["ordered_at"] = OI["created_at"]

    OI["is_cancelled"] = 0
    OI["is_pending"] = 1
    OI["is_fulfilled"] = 0
    OI.loc[OI["fulfillment_status"] == "FULFILLED", "is_fulfilled"] = 1
    OI.loc[OI["fulfillment_status"] == "CANCELED", "is_cancelled"] = 1
    OI.loc[OI["fulfillment_status"] == "FULFILLED", "is_pending"] = 0
    OI.loc[OI["fulfillment_status"] == "CANCELED", "is_pending"] = 0
    OI = OI.drop(
        [
            "key",
            "lineitem_id",
            "flag_for_review",
            "record_id",
            "sku_mismatch",
            "reservation_status",
        ],
        1,
    )

    uri = "s3://res-data-platform/samples/data/order_item_fulfillments.parquet"
    res.utils.logger.info(f"Caching order items to {uri}")
    s3.write(uri, OI)

    res.utils.logger.info(f"Preparing aggregates")
    OI["has_one_number"] = 0
    OI["has_hold_flag"] = 0
    OI["has_flag"] = 0
    # another hack - sometimes we have the production id but not the one number - fill this in here and maybe map it later
    OI["active_one_number"] = OI["active_one_number"].fillna(
        OI["make_one_production_request_id"]
    )
    # THIS IS A TRICK: The thing was fulfilled but we dont have a one number
    # WE should have a one number based on what was in the warehouse but we dony
    OI.loc[
        (OI["is_fulfilled"] == 1) & (OI["active_one_number"].isnull()),
        "active_one_number",
    ] = "-1"
    # same idea for cancelled but use a different indicator
    OI.loc[
        (OI["is_cancelled"] == 1) & (OI["active_one_number"].isnull()),
        "active_one_number",
    ] = "-2"
    # we cannot do this because there are cases of no one number
    # OI["active_one_number"] = OI["active_one_number"].map(int)
    OI.loc[OI["active_one_number"].notnull(), "has_one_number"] = 1
    OI.loc[OI["flag_for_review_tag"].notnull(), "has_flag"] = 1
    OI.loc[
        OI["flag_for_review_tag"].map(lambda x: "hold" in str(x).lower()),
        "has_hold_flag",
    ] = 1
    OI["number_of_order_items"] = 1

    # uri = "s3://res-data-platform/samples/data/order_items.parquet"

    # res.utils.logger.info(f"caching order items to {uri}")
    # s3.write(uri, OI)

    counts = OI.groupby(["order_name", "sales_channel", "brand_code"]).sum()
    dates = OI.groupby(["order_name", "sales_channel", "brand_code"]).agg(
        {"ordered_at": max, "cancelled_at": max, "last_updated_at": max}
    )
    keys = OI.groupby(["order_name", "sales_channel", "brand_code"]).agg(
        {"active_one_number": list, "sku": list, "flag_for_review_tag": list}
    )
    my_data = counts.join(dates).join(keys).reset_index()
    uri = "s3://res-data-platform/samples/data/orders.parquet"

    res.utils.logger.info(f"caching aggregates to {uri}")
    s3.write(uri, my_data)

    my_data["is_completely_fulfilled"] = (
        my_data["number_of_order_items"] == my_data["is_fulfilled"]
    ).map(int)
    my_data["is_sufficiently_fulfilled"] = (
        (my_data["number_of_order_items"] - my_data["is_cancelled"])
        == my_data["is_fulfilled"]
    ).map(int)
    my_data["is_completely_cancelled"] = (
        my_data["number_of_order_items"] == my_data["is_cancelled"]
    ).map(int)
    my_data["is_missing_one_numbers"] = (
        my_data["number_of_order_items"] != my_data["has_one_number"]
    ).map(int)

    my_data["is_awaiting_payment"] = (
        my_data["flag_for_review_tag"].map(lambda x: "not paid" in str(x)).map(int)
    )

    my_data["style_sku"] = my_data["sku"].map(sp)
    my_data["num_m1"] = my_data["style_sku"].map(lambda f: len([i for i in f]))
    # my_data = my_data[my_data['is_sufficiently_fulfilled']==0]
    my_data["ratio_having_one_numbers"] = (
        my_data["has_one_number"] / my_data["number_of_order_items"]
    )
    my_data["days_since_order"] = (
        res.utils.dates.utc_now() - pd.to_datetime(my_data["ordered_at"], utc=True)
    ).dt.days

    # process style codes here - trim the sizes - use to know if the meta ones are pending

    # rename after sum - lesson in columns names changing meaning after aggregation
    my_data = my_data.rename(
        columns={
            "is_pending": "number_of_pending_order_items",
            "is_fulfilled": "number_of_fulfilled_order_items",
            "is_cancelled": "number_of_cancelled_order_items",
            "has_one_number": "number_of_order_items_that_have_one_number",
            "has_hold_flag": "number_of_order_items_that_have_hold_flags",
            "has_flag": "number_of_order_items_that_have_flags",
        }
    )

    # pr cache
    uri = "s3://res-data-development/data-lake/rmm/order_items/sell/partition_0.parquet"
    s3.write(uri, my_data)

    res.utils.logger.info(f"Generating ACQ lookup")

    # optionally merge ACQ figures of interest onto the queue
    B = merge_style_queue_statistics(
        my_data,
        ss=None,
        fields=[
            "order_name",
            "pending_sku_set_acq",
            "is_done_sum_acq",
            "pending_styles",
        ]
        + [
            # 'ratio_having_one_numbers', 'days_since_order', 'is_awaiting_payment', 'brand_code'
        ],
    )

    my_data = pd.merge(my_data, B, on="order_name", how="left")

    # here we do the full think of importing to the RRM which updates the preceding table AND the redis cache (we could do this every 24 hours for now)
    # in truth we should just do an incremental update which will be phase 2
    rrm_orders = ResilientResonanceMap("sell", "order_items")

    res.utils.logger.info(f"Importing entities to rrm (full)")

    rrm_orders.import_entities(my_data, key_field="order_name")

    return my_data


def merge_style_queue_statistics(
    df=None, ss=None, filter_pending_fulfillment=True, fields=None
):
    """
    WIP
    style statistics come from the other node
    We can merge on the style stats for each order by mapping over its skus
    this only seems to make sense for open orders in the 3d flow in the sense that we fulfilled any older orders for which we did not have m1s

    B = merge_style_queue_statistics(df, ss, True,
                                 fields = ['order_name','pending_sku_set_acq','is_done_sum_acq', 'pending_m1'] + [
                                    # 'ratio_having_one_numbers', 'days_since_order', 'is_awaiting_payment', 'brand_code'
                                 ])
    #if you include the filters you can test this gives interesting results for orders we care about
    #but without the filer we can join any ACQ stats to the order queue for context
    CHK = B[(B['ratio_having_one_numbers']<1)&(B['brand_code']!='TT')]
    CHK['days_since_order'].hist()
    CHK


    """
    from res.flows.api.rrm import ResilientResonanceMap

    if df is None:
        rrm_orders = ResilientResonanceMap("sell", "order_items")
        df = rrm_orders.load_entities()

    if ss is None:
        # this could also be loaded from the rrm
        ss = res.connectors.load("s3").read(
            "s3://res-data-development/data-lake/rmm/styles/meta/partition_0.parquet"
        )
        ss = (
            ss.sort_values("number_of_times_in_queue")
            .set_index("style_code")
            .drop(
                [
                    "flag_for_review",
                    "body_version",
                    "available_size",
                    "request_auto_id",
                ],
                1,
            )
        )

    def style_status(style_sku):
        d = {
            "number_of_times_in_queue": 0,
            "created_at": None,
            "last_updated_at": None,
            "is_flagged": None,
            "is_done": 0,
            "is_cancelled": 0,
            "style_sku": style_sku,
        }

        try:
            d = dict(ss.loc[style_sku])
            d["style_sku"] = style_sku
            return d
        except:
            pass
        return d

    def sp(sl):
        def f(s):
            return " ".join(s.split(" ")[:3]) if pd.notnull(s) else None

        return set([f(s) for s in sl if s != "None" and s != None])

    def sku_status_counts(skus):
        def sp(sl):
            return " ".join(sl.split(" ")[:3])

        styles = pd.DataFrame(
            [sp(s) for s in skus], columns=["index"]
        ).drop_duplicates()
        df = pd.DataFrame([d for d in styles["index"].map(style_status)])

        df["days_since_created"] = (
            res.utils.dates.utc_now() - pd.to_datetime(df["created_at"], utc=True)
        ).dt.days
        df["days_since_last_updated"] = (
            res.utils.dates.utc_now() - pd.to_datetime(df["last_updated_at"], utc=True)
        ).dt.days
        df["pending_sku"] = df.apply(
            lambda x: None if x["is_done"] == 1 else x["style_sku"], axis=1
        )
        df["indicator"] = 1
        # return df

        return df.groupby("indicator").agg(
            {
                "days_since_created": [min, max, np.mean],
                "days_since_last_updated": [min, max, np.mean],
                "number_of_times_in_queue": [min, max, np.mean],
                "is_flagged": [min, max, np.mean, sum],
                "is_done": [min, max, np.mean, sum],
                "is_cancelled": [min, max, sum],
                "pending_sku": set,
            }
        )

    # tester
    # skus =list(df[df['order_name']=='BG-2129233']['sku'])[0]
    # sku_status_counts(skus)

    A = df[
        [
            "order_name",
            "sales_channel",
            "brand_code",
            "ordered_at",
            "number_of_order_items",
            "active_one_number",
            "number_of_order_items_that_have_one_number",
            "sku",
            "is_sufficiently_fulfilled",
            "flag_for_review_tag",
            "is_awaiting_payment",
        ]
    ]
    # post process
    A["style_sku"] = A["sku"].map(sp)
    A["number_required_m1"] = A["style_sku"].map(lambda f: len([i for i in f]))

    A["ratio_having_one_numbers"] = (
        A["number_of_order_items_that_have_one_number"] / A["number_of_order_items"]
    )
    A["days_since_order"] = (
        res.utils.dates.utc_now() - pd.to_datetime(A["ordered_at"], utc=True)
    ).dt.days

    if filter_pending_fulfillment:
        A = A[A["is_sufficiently_fulfilled"] == 0].reset_index(drop=True)

    # merge
    data = A["sku"].map(sku_status_counts)
    B = pd.concat([d for d in data]).reset_index()
    B[("pending_sku", "set")] = B[("pending_sku", "set")].map(sp)
    B = B.fillna(0)

    def fix_tuple(a):
        return f"{a[0]}_{a[1]}_acq" if isinstance(a, tuple) else a

    B.columns = [fix_tuple(c) for c in B.columns]
    B["pending_sku_set_acq"] = B["pending_sku_set_acq"].map(list)

    # we could
    A = A.join(B)

    A["pending_styles"] = A["number_required_m1"] - A["is_done_sum_acq"]

    fields = fields or A.columns

    return A[fields]


def apply_swaps_to_orders(missing, plan=False, recache=False):
    """
    given a seed set of skus that are causing problems
    apply the swap and update the original order to point to it
    generally we should be able to generate the make order with any order that makes sense at time of order
    but if the swap happened after today we cannot recover the meta one
    """
    from res.flows.sell.orders.queries import update_swapped_product
    from res.flows.dxa.styles.helpers import merge_swaps_to_cache

    def trimit(s):
        s = s.split(" ")
        s = [i.strip() for i in s]
        s = " ".join(s[:3])

        return s

    if recache:
        merge_swaps_to_cache()

    missing["style_sku"] = missing["sku"].map(trimit)

    duck = res.connectors.load("duckdb")
    data = duck.execute(
        "SELECT * FROM 's3://res-data-production/data-lake/cache/swaps/swaps.parquet'"
    )

    missing_with_swaps = pd.merge(
        missing, data, left_on="style_sku", right_on="new_sku"
    )
    if plan:
        return missing_with_swaps

    # this works because record conforms to the schema expected by the function
    update_swapped_product(missing_with_swaps)

    return missing_with_swaps


def request_reimport_batch(keys):
    import requests

    from res.flows.FlowEventProcessor import FlowEventProcessor

    e = FlowEventProcessor().make_sample_flow_payload_for_function(
        "sell.orders.process.reimport_batch"
    )
    e["assets"] = [{"order_number": key} for key in keys]

    j = requests.post(
        "https://data.resmagic.io/res-connect/flows/res-flow-node", json=e
    )
    return j.json()


def reimport_batch(event, context):
    """
    usage

        def make_payload(keys):
        from res.flows.FlowEventProcessor import FlowEventProcessor

        e = FlowEventProcessor().make_sample_flow_payload_for_function(
            "sell.process.reimport_batch"
        )
        e["assets"] = [{"asset_key": key} for key in keys]
        return e

        import requests
        requests.post('https://data.resmagic.io/res-connect/flows/res-flow-node', data=e)

    """

    from res.flows.sell.orders.queries import reimport_order

    with res.flows.FlowContext(event, context) as fc:
        assets = []
        for asset in fc.assets:
            key = asset["order_number"]
            delete_existing = asset.get("delete_existing", False)
            asset["status"] = "failed"
            try:
                status = reimport_order(key, delete_existing=delete_existing)
                asset["status"] = "ok"
            except:
                """
                try to import this from airtable directly if we failed here??
                #the exercise is to create an adapter from the order/order item table to the platform order and save it
                """
                pass

            assets.append(asset)

        return assets


def get_missing_shopify_orders_for_brand_since_date(
    brand_code, date=None, preview_missing=False
):
    """
    its expected that this is a last resort fill gap
    we miss the occasional order and we can fetch it now and sync
    assumes that the date is recent as we would not use this route e.g. for weeks or months of orders
    this is because there is already a process that syncs orders from snowflake which are updated daily
    so this is more for very hot orders that we miss
    """

    # what we have by this name already

    from schemas.pydantic.sell import Order
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    hasura = res.connectors.load("hasura")

    def get_orders_by_order_numbers(order_names):
        """lookup hasura for the orders that might match the expected list"""
        Q = """query match_existing_orders($names: [String!] = "") {
          sell_orders(where: {name: {_in: $names}}) {
            name
            id
          }
        }
        """
        return hasura.execute_with_kwargs(Q, names=list(order_names))["sell_orders"]

    shop = res.connectors.load("shopify")[brand_code]
    if shop:
        order_list = shop.get_recent_orders(date=date)
        res.utils.logger.info(
            f"Loaded {len(order_list)} orders. Checking what we have in the database"
        )
        if len(order_list) == 0:
            res.utils.logger.info(f"Nothing to do - we have no new orders to check for")
            return
        matching = get_orders_by_order_numbers(order_list["order_number"])
        res.utils.logger.info(f"We have {len(matching)} already - saving the missing")
        matching_names = [o["name"] for o in matching]

        if len(matching) == len(order_list):
            res.utils.logger.info(f"Nothing left to do")
            return

        # filter if there is a match
        if len(matching):
            order_list = order_list[~order_list["order_number"].isin(matching_names)]

        if preview_missing:
            return order_list

        # if we get to here without filtering by matching that we are missing everything in shopify for the brand - rare but possible
        ids_to_fetch = list(order_list["id"])
        res.utils.logger.info(
            f"Fetching from shopify the full order detail: id list is {ids_to_fetch}"
        )
        """
        Here we needed to go to the order lookup to find what ids to fetch
        another use case is if the airtable create-one stuff has an id and we dont, we could have
        just use that to fetch the ones we are missing
        ignoring that use case for now - here we just see what order numbers like KT-12345 we are missing 
        and then we get the ids for those and then we fetch the full payload so we can map to our model
        """
        all_orders = shop.fetch_full_order_detail_for_order_ids(ids_to_fetch)
        # return all_orders
        res.utils.logger.info(f"Parsing orders to our schema for saving")
        orders = [
            Order.from_shopify_payload(
                o, brand_code=shop._brand_code, elevate_shipping_attributes=True
            )
            for o in all_orders
        ]
        res.utils.logger.info(f"Saving {len(orders)} orders")
        node = FulfillmentNode(Order)  # , postgres="dont_need"
        for o in orders:
            # TODO we may want to do this if we start using that airtable queue in earnest
            node._save(o, update_queue=False)
        res.utils.logger.info(f"Done")

    else:
        raise Exception(f"error loading shop for brand {brand_code}")


def pull_missing_order_if_not_exists(order_number, channel):
    """
    do a quick check in hasura for the order or remediate
    """

    from res.flows.sell.orders.queries import (
        get_create_one_order_from_kafka_messages,
        get_order_by_name,
    )
    from schemas.pydantic.sell import Order
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    try:
        o = get_order_by_name(order_number)
        if not o:
            res.utils.logger.info(
                f"Order not saved to postgres - looking for: {order_number} channel: {channel}"
            )
            # check order exists and return
            res.utils.logger.send_message_to_slack_channel(
                "sirsh-test",
                f"[Order Check] Checking for missing order [{order_number}] from channel {channel} as we make production request",
            )

            if channel.lower().strip() == "shopify":
                brand_code = order_number.split("-")[0]
                # checks all recent shopify orders and does a full sync of what is missing
                get_missing_shopify_orders_for_brand_since_date(brand_code)
            else:
                payloads = get_create_one_order_from_kafka_messages(order_number)
                if payloads:
                    payload = payloads[-1]
                    node = FulfillmentNode(Order, postgres="nope")
                    node._save(Order.from_create_one_payload(payload))
                else:
                    res.utils.logger.info("looked in snowflake and its not there")
        else:
            res.utils.logger.info(f"Order saved to postgres: {order_number}")
    except Exception as ex:
        msg = f"Failed to check for missing order {order_number} - {traceback.format_exc()}"
        res.utils.logger.warn(msg)
        res.utils.logger.send_message_to_slack_channel(
            "sirsh-test", "[Order Check] <@U01JDKSB196>" + msg
        )
