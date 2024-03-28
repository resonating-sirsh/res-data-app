import res

from schemas.pydantic.make import OneOrder
import pandas as pd
import json
from tqdm import tqdm
from tenacity import retry, stop_after_attempt, wait_fixed
import traceback
from res.flows.api import FlowAPI

"""
notes:

if we at some point get a list of registered orders with correct prefixes, we can re-populate the lookup table - we should pause the prod inbox process while we do this

#prefix must be the official style prefix without the make increment, and id should be a unique request id to rank over
for example we normally have one request per fulfillable item - actually not all requests need to be made today so in future we can curb increments based on what we actually make

for record in tqdm(rankable.to_dict('records')):
    g = record['prefix']
    if g:
        rrt = increment_instance_for_group_at_time(g, record['id'])

"""


def try_slack_event_for_ppp_request(asset, message):
    try:
        rid = asset.get("id")
        slack = res.connectors.load("slack")
        body_code = asset.get("body_code")
        body_version = asset.get("body_version")
        color_code = asset.get("color_code")
        material_code = asset.get("material_code")
        size_code = asset.get("size_code")
        one_number = asset.get("one_number")

        sku = f"{body_code} {material_code} {color_code} {size_code}".upper()

        owner = "Followers <@U01JDKSB196> "

        q_url = f"https://airtable.com/apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viw76IYUsx5RKjMTG/{rid}"

        s = f"<{q_url}|{sku}-V{body_version}> {message} - {one_number}\n"

        p = {
            "slack_channels": ["autobots"],
            "message": s,
            "attachments": [],
        }
        slack(p)
    except Exception as ex:
        res.utils.logger.warn(f"Failing to slack - {ex}")


def migrate_ids(change_set):
    """
    the new id scheme for make orders is based on fulfillment items and the rank of creating in order
    there is some slight leeway identical items
    """
    MIGRATE_IDS = """mutation MyMutation($new_id: uuid = "",  $old_id: uuid = "",  $order_item_id: uuid = "") {
        update_make_one_orders(where: {id: {_eq: $old_id}}, _set: {id: $new_id, order_item_id: $order_item_id}) {
            returning {
            id
            }
            }
        }

        """

    hasura = res.connectors.load("hasura")
    for record in tqdm(change_set.reset_index().to_dict("records")):
        old_id = record["id"]
        new_id = record["new_id"]
        order_item_id = record["order_item_id"]
        try:
            hasura.execute_with_kwargs(
                MIGRATE_IDS, old_id=old_id, new_id=new_id, order_item_id=order_item_id
            )

        except Exception as ex:
            pass


def migrate_ids_and_one_number(change_set):
    """
    the new id scheme for make orders is based on fulfillment items and the rank of creating in order
    there is some slight leeway identical items
    """
    MIGRATE_IDS_FULL = """mutation MyMutation($new_id: uuid = "",  $old_id: uuid = "",  $order_item_id: uuid = "", $oid: uuid = "", $one_number: Int = "") {
        update_make_one_orders(where: {id: {_eq: $old_id}}, _set: {id: $new_id, order_item_id: $order_item_id, oid: $oid, one_number: $one_number}) {
            returning {
            id
            }
            }
        }
        """
    hasura = res.connectors.load("hasura")
    for record in tqdm(change_set.reset_index().to_dict("records")):
        old_id = record["id"]
        new_id = record["new_id"]
        one_number = record["one_number_proposed"]
        oid = res.utils.uuid_str_from_dict({"id": one_number})
        order_item_id = record["order_item_id"]
        try:
            hasura.execute_with_kwargs(
                MIGRATE_IDS_FULL,
                old_id=old_id,
                new_id=new_id,
                order_item_id=order_item_id,
                oid=oid,
                one_number=one_number,
            )

        except Exception as ex:
            print(ex)


def repopulate_redis_cache():
    def get_size_norm_lookup():
        """
        the size lookup maps aliases we use in processing
        """
        airtable = res.connectors.load("airtable")
        sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
        # predicate on sizes that we have
        _slu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Normalized"])
        _slu["Size Normalized"] = _slu["Size Normalized"].map(
            lambda x: x.replace("-", "")
        )
        slu = dict(_slu[["_accountingsku", "Size Normalized"]].values)
        slu_inv = dict(_slu[["Size Normalized", "_accountingsku"]].values)

        return slu

    mapping = get_size_norm_lookup()

    redis = res.connectors.load("redis")
    cache = redis["CACHE"]["SIZES"]
    for k, v in mapping.items():
        cache[k] = v

    res.utils.logger.info(f"added mapping to redis")
    return mapping


def fully_process_order(
    data, order_to_process, include_one_number_migration=True, plan=True
):
    """
    this is a WIP "safe" way to migrate ids of a make order using the new scheme
    IF we migrate the ids, later we can rewrite the order with the one number association
    by lookup up the saved order mapping, we can also map the correct one numbers in order
    if we find constraint violations we may disable the constraint and re-map and remove duplicate older associations
    We may also need to add new order requests that were not made in the passed

    """
    from res.flows.make.production.queries import get_order_map_by_order_key
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    # we can get existing one mappings to do our maint.
    order_map = get_order_map_by_order_key(order_to_process)
    order_map = order_map[0]["sku_map"] if len(order_map) else {}
    print(order_map)

    def make_id(row):
        return res.utils.uuid_str_from_dict(
            {"order_line_item_id": row["order_item_id"], "instance": int(row["rank"])}
        )

    def get_one_number(row):
        mp = order_map.get(row["sku"])
        if mp:
            rank = row["rank"] - 1
            if rank < len(mp):
                return mp[rank]

    eg = FulfillmentNode.read_by_name(order_to_process)
    eg = pd.DataFrame(eg.dict()["order_items"])
    # lookup the correct order item because in old processes we linked a dummy one
    sku_order_item_map = dict(eg[["sku", "id"]].values)
    qty = dict(eg[["sku", "quantity"]].values)
    example = data[data["order_number"] == order_to_process]
    # confirm the order item id is correct
    example["order_item_id"] = example["sku"].map(lambda x: sku_order_item_map[x])
    example["qty"] = example["sku"].map(lambda x: qty[x])

    # now make the id - repeast the logic of above to make sure it works each time
    if "index" not in example.columns:
        example = example.sort_values("created_at").reset_index(drop=True).reset_index()
    example["rank"] = (
        example.groupby(["sku"])["index"].rank(ascending=True).map(int)
    )  # .map(str)
    example["new_id"] = example.apply(make_id, axis=1)
    example["new_id"] = example.apply(make_id, axis=1)
    res.utils.logger.info(
        f"filter applied for rank less than qty in case we have dups in make"
    )
    example = example[example["rank"].astype(float) <= example["qty"].astype(float)]
    res.utils.logger.info(f"applying the one number if it exists for this rank")
    example["one_number_proposed"] = example.apply(get_one_number, axis=1)

    if not plan:
        if include_one_number_migration:
            migrate_ids_and_one_number(example)
        migrate_ids(example)

    # determine and queue missing requests with/out one numbers - they can just be posted

    return example


def update_order_map_from_df(orders, hasura=None):
    hasura = res.connectors.load("hasura")
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

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def update(r):
        return hasura.execute_with_kwargs(UPDATE_ORDER_MAP, objects=[r])

    failures = []
    from tqdm import tqdm

    for record in tqdm(orders.reset_index().to_dict("records")):
        key = record["order_number"]
        order_size = record["order_size"]
        skus = record["sku_map"]

        d = {
            "id": res.utils.uuid_str_from_dict({"order_key": str(key)}),
            "order_key": key,
            "sku_map": skus,
            "order_size": order_size,
        }
        # print(d)
        try:
            r = update(d)
        except:
            failures.append(d)
        print(r)
        # break

    return failures


def get_all_ones(in_make_only=True, mones=None):
    from res.connectors.airtable import AirtableConnector
    from ast import literal_eval

    def clean_list(s):
        if isinstance(s, list):
            return s
        if isinstance(s, str):
            try:
                s = literal_eval(s)
                return s
            except:
                pass
            return [s]
        return []

    snowflake = res.connectors.load("snowflake")
    res.utils.logger.info(f"hitting snowflake...")
    data = snowflake.execute(
        f"""select a.ONE_NUMBER as "one_number", c.ORDER_KEY as "order_number", a.ONE_SKU as "sku", a.CANCELLED_AT, a.PRODUCTION_REQUEST_ID, a.BODY_VERSION,
             a.ONE_CANCELLATION_ID, a.REASON_TAGS_FLAGGED, a.CREATED_AT as CREATED_AT 
            from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_PRODUCTION_REQUESTS"  a
            JOIN "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS" b on a.ORDER_LINE_ITEM_ID = b.ORDER_LINE_ITEM_ID and b.VALID_TO IS NULL
            JOIN "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" c on c.ORDER_ID = b.ORDER_ID and c.VALID_TO IS NULL
            AND a.VALID_TO IS NULL ;"""
    )
    data["is_cancelled"] = data["ONE_CANCELLATION_ID"].notnull()
    data["body_version"] = data["BODY_VERSION"]  # a.BODY_VERSION,
    data["contracts_failed"] = data["REASON_TAGS_FLAGGED"]
    data["created_at"] = data["CREATED_AT"]
    data["cancelled_at"] = data["CANCELLED_AT"]
    data["record_id"] = data["PRODUCTION_REQUEST_ID"]

    res.utils.logger.info(f"Loading mones from airtable")
    # we can pass it from another process
    if mones is None:
        mones = AirtableConnector.get_airtable_table_for_schema_by_name(
            "make.production_requests"
        )
    mones["is_cancelled"] = mones["is_cancelled"].notnull()
    mones["one_number"] = mones["key"]

    mones["created_at"] = mones["createdtime"]
    mones["cancelled_at"] = mones["cancel_date"]
    mones["contracts_failed"] = mones["defects"]
    res.utils.logger.info(f"Loaded {len(mones)} mones from airtable")

    # join out anything not currently in make
    # this way we keep anything cancelled and archived
    if in_make_only:
        res.utils.logger.info(
            f"Checking the {len(data)} orders to see if currently in make"
        )
        data = pd.merge(data, mones[["one_number"]], on="one_number")
        res.utils.logger.info(f" {len(data)} orders in make")

    fields = [
        "one_number",
        "order_number",
        "sku",
        "is_cancelled",
        "cancelled_at",
        "created_at",
        "contracts_failed",
        "body_version",
        "record_id",
    ]
    df = pd.concat([data[fields], mones[fields]]).drop_duplicates(
        subset=["one_number"], keep="last"
    )
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["cancelled_at"] = pd.to_datetime(df["cancelled_at"], utc=True)

    df["sku"] = df["sku"].map(lambda x: f"{x[:2]}-{x[2:]}" if "-" not in x else x)

    df["contracts_failed"] = df["contracts_failed"].map(clean_list)
    df["is_cancelled"] = df["is_cancelled"].map(int)
    df["body_version"] = df["body_version"].fillna(0).map(int)
    df["one_number"] = df["one_number"].fillna(-1).map(int)

    return df

    ##TODO defects
    # airtable = res.connectors.load('airtable')
    # fail = airtable.get_table_data('appH5S4hIuz99tAjm/tbl8L3lrXxG6lnBnJ')
    # defects = dict(fail[['record_id', 'Name']].values)
    # def map_recs(x):
    #     from ast import literal_eval
    #     try:
    #         if isinstance(x,str):
    #             x = literal_eval(x)
    #         return [item.replace('\n','') if 'rec' not in item else defects.get(item,item) for item in x ]
    #     except:
    #         pass
    #     return x
    # ones['contracts_failed'] = ones['contracts_failed'].map(map_recs)
    # ones['contracts_failed'] = ones['contracts_failed'].map(str)
    # ones[ones['contracts_failed'].notnull()]


def bulk_load_map_from_data(data, preview=False, remove_cancelled_ones=True):
    """
    supply mones contracts as cached
    - one_number
    - is_cancelled
    - order_number
    - sku
    """
    res.utils.logger.info(f"Fetching orders from make one production")

    open_ones = data[data["is_cancelled"] == False]

    orders = {}

    res.utils.logger.info("aggregating")
    open_ones["one_number"] = open_ones["one_number"].map(int)
    for k, v in open_ones.groupby("order_number"):
        skus = {}
        order_size = 0
        for sku, data in v[["sku", "one_number"]].groupby("sku"):
            data = data.sort_values("one_number")
            skus[sku] = list(data["one_number"])
            order_size += len(set(data["one_number"]))
        orders[k] = skus

    orders = pd.DataFrame(
        [
            {
                "order_key": str(k).strip(),
                "order_number": str(k).strip(),
                "sku_map": v,
                "order_size": sum([len(_v) for _v in v.values()]),
            }
            for k, v in orders.items()
        ]
    )

    if preview:
        return orders
    res.utils.logger.info("Updating the order map")
    return update_order_map_from_df(orders)


def bulk_load_map_from_mones(preview=False, remove_cancelled_ones=True):
    res.utils.logger.info(f"Fetching orders from make one production")
    orders = get_all_ones()

    open_ones = orders[orders["is_cancelled"] == False]

    orders = {}

    res.utils.logger.info("aggregating")
    open_ones["one_number"] = open_ones["one_number"].map(int)
    for k, v in open_ones.groupby("order_number"):
        skus = {}
        order_size = 0
        for sku, data in v[["sku", "one_number"]].groupby("sku"):
            data = data.sort_values("one_number")
            skus[sku] = list(data["one_number"])
            order_size += len(set(data["one_number"]))
        orders[k] = skus

    orders = pd.DataFrame(
        [
            {
                "order_key": str(k).strip(),
                "order_number": str(k).strip(),
                "sku_map": v,
                "order_size": sum([len(_v) for _v in v.values()]),
            }
            for k, v in orders.items()
        ]
    )

    if preview:
        return orders
    res.utils.logger.info("Updating the order map")

    return update_order_map_from_df(orders)


def reconcile_ones_map(event, context={}):
    """
    maint task for now to maintain a list of the old systems one for quickly merging with the new system

    """

    from res.flows.make.production.utils import bulk_load_map_from_mones

    bulk_load_map_from_mones()

    # res.utils.logger.info("reconcile cancelled ones")
    # reconcile_cancelled_ones()
    return {}


def reconcile_cancelled_ones(hasura=None, orders=None):
    """
    purge the one map - this should be done as we go so this is just a util cleanup
    """
    from res.flows.make.production.queries import record_cancelled_one
    from tqdm import tqdm

    Q = """query get_one_bridge {
    infraestructure_bridge_order_skus_to_ones {
        id
        order_key
        order_size
        sku_map
        id
        created_at
        updated_at
    }
    }
    """
    hasura = hasura or res.connectors.load("hasura")

    bridge = pd.DataFrame(
        hasura.execute_with_kwargs(Q)["infraestructure_bridge_order_skus_to_ones"]
    ).sort_values("created_at")
    bridge["expected_key"] = bridge["order_key"].map(
        lambda x: res.utils.uuid_str_from_dict({"order_key": str(x)})
    )

    snowflake = res.connectors.load("snowflake")
    data = snowflake.execute(
        f"""select a.ONE_NUMBER as "one_number", c.CHANNEL_ORDER_ID as "order_number", a.ONE_SKU as "sku", a.ONE_CANCELLATION_ID
    from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_PRODUCTION_REQUESTS"  a
    JOIN "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDER_LINE_ITEMS" b on a.ORDER_LINE_ITEM_ID = b.ORDER_LINE_ITEM_ID and b.VALID_TO IS NULL
    JOIN "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_ORDERS" c on c.ORDER_ID = b.ORDER_ID and c.VALID_TO IS NULL
    AND a.VALID_TO IS NULL ;"""
    )
    data["is_cancelled"] = data["ONE_CANCELLATION_ID"].notnull()

    f = data[data["is_cancelled"]]
    f = f[f["order_number"].isin(bridge["order_key"])]
    f["sku"] = f["sku"].map(lambda x: f"{x[:2]}-{x[2:]}" if "-" not in x else x)

    failed = []

    if orders is not None:
        res.utils.logger.info(f"Filtering for supplied orders")
        f = f[f["order_number"].isin(orders)]

    for record in tqdm(f.to_dict("records")):
        try:
            record_cancelled_one(
                record["order_number"], record["sku"], record["one_number"]
            )
        except:
            failed.append(record)
            pass


def get_bridged_one_map():
    hasura = res.connectors.load("hasura")
    Q = """query get_one_bridge {
    infraestructure_bridge_order_skus_to_ones {
        id
        order_key
        order_size
        sku_map
        id
        created_at
        updated_at
    }
    }
    """
    bridge = pd.DataFrame(
        hasura.execute_with_kwargs(Q)["infraestructure_bridge_order_skus_to_ones"]
    ).sort_values("created_at")
    bridge["expected_key"] = bridge["order_key"].map(
        lambda x: res.utils.uuid_str_from_dict({"order_key": str(x)})
    )
    return bridge


def remove_bad_mapped_ones():
    """
    if the id mapped is wrong we remove
    but why is it wrong

    """
    Q = """mutation MyMutation($id: uuid = "") {
        delete_infraestructure_bridge_order_skus_to_ones_by_pk(id: $id) {
            id
        }
        }
        """

    bridge = get_bridged_one_map()
    remove = bridge[bridge["id"] != bridge["expected_key"]]

    hasura = res.connectors.load("hasura")

    from tqdm import tqdm

    for record in tqdm(remove["id"]):
        try:
            r = hasura.execute_with_kwargs(Q, id=record)
        except:
            pass

    return remove


def bulk_load_map_from_kafka(preview=False, remove_cancelled_ones=False):
    """
    if we go to the create one requests, we can rebuild map of ones to order's skus and save them
    below we use a join instead but populating this is useful for processes

    we are carefully to deup and removed cancelled items when we run this - in future other process should maintain this map
    """

    def build_sku_map(row):
        d = {}

        for t in row["s-tuple"]:
            sku = t[0]
            if sku not in d:
                d[sku] = []
            try:
                d[sku].append(int(float((t[1]))))
            except:
                pass
        return d

    def as_snowlfake_table_select(topic):
        return f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_{topic.upper().replace('.','_')}" """

    snowflake = res.connectors.load("snowflake")

    req_data = snowflake.execute(
        as_snowlfake_table_select("res_make.make_one_request.create_response")
    )
    req_data["rc"] = req_data["RECORD_CONTENT"].map(json.loads)
    req_data["rc"] = req_data["RECORD_CONTENT"].map(json.loads)

    req_data = pd.DataFrame([d for d in req_data["rc"]])
    req_data["one_number"] = req_data["one_number"].map(int)
    res.utils.logger.info(f"Loaded {len(req_data)} ONEs from requests")

    # actually no we need rank awareness
    # # remove cancelled ones, one per line item
    # req_data = req_data.sort_values("one_number").drop_duplicates(
    #     subset=["order_line_item_id"]
    # )

    if remove_cancelled_ones:
        res.utils.logger.info(f"Loading cancelled ONEs")
        Q = """select DISTINCT "ONE_CANCELLATION_ID", "ONE_NUMBER", "ORIGINAL_PRODUCTION_REQUEST_IDS", "ONE_SKU", "CANCELLED_AT" from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION_MODELS"."FACT_HISTORICAL_PRODUCTION_REQUESTS" WHERE "CANCELLED_AT" IS NOT NULL """
        d = snowflake.execute(Q)
        d["ONE_NUMBER"] = d["ONE_NUMBER"].map(int)
        d = d[d["ONE_NUMBER"].isin(req_data["one_number"])].reset_index()
        cancelled_ones = list(d["ONE_NUMBER"].map(int).unique())
        res.utils.logger.info(f"removing {len(cancelled_ones)} cancelled ONEs")
        req_data["is_cancelled"] = req_data["one_number"].map(
            lambda x: x in cancelled_ones
        )
        req_data = req_data[~req_data["is_cancelled"]].reset_index(drop=True)

    res.utils.logger.info(f"Building the map")
    req_data["sku"] = req_data["sku"].map(lambda x: f"{x[:2]}-{x[2:]}")

    req_data["s-tuple"] = req_data.apply(
        lambda row: (row["sku"], row["one_number"]), axis=1
    )

    orders = req_data.groupby("order_number").agg(
        {
            "one_number": list,
            "sku": list,
            "s-tuple": list,
            "line_item_creation_date": min,
        }
    )
    orders["sku_map"] = orders.apply(build_sku_map, axis=1)
    orders["order_size"] = orders["sku_map"].map(lambda x: len(x.values()))
    orders[["sku_map", "order_size"]]

    if preview:
        return orders

    return update_order_map_from_df(orders)


def repair_mode():
    """
    at the moment this just provides a highlevel script that needs some work to be more useful

    care:
    - we wnt to make sure we have at least as many make orders as orders
    - id collision is one bad reason why we will loose some
    - so this repair can allow logic change and recreate while working out the kinks

    eventually we can save we have all historic orders and a ranking over them of one codes
    and we can have one numbers associated with all
    """

    def make_order(metadata):
        brand_code = metadata.get("brand_code")

        order_number = metadata.get("order_name").strip("#")
        if f"{brand_code}-" in order_number:
            return order_number
        elif "#" in metadata.get("order_name"):
            return f"{brand_code}-{order_number}"
        try_this = f"{brand_code}-{metadata.get('source_order_id')}"

        return try_this

    """
    load all make requests from kafka, a great source of truth of what is relevant in production
    """
    Q = f"""select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_ORDERS_ONE_REQUESTS"  """
    res.utils.logger.info(Q)
    snowflake = res.connectors.load("snowflake")
    original_one_order_requests = snowflake.execute(Q)
    original_one_order_requests["payload"] = original_one_order_requests[
        "RECORD_CONTENT"
    ].map(json.loads)
    ooor = pd.DataFrame([d for d in original_one_order_requests["payload"]])
    ooor = ooor.join(original_one_order_requests[["payload"]])
    ooor = (
        ooor.drop_duplicates(subset=["id"])
        .sort_values("created_at")
        .reset_index()
        .reset_index(drop=True)
        .reset_index()
    )
    # reindex one codes
    ooor["order_number"] = ooor["metadata"].map(make_order)
    ooor["RANK"] = (
        ooor.groupby(["order_number", "sku"])["index"].rank(ascending=True).astype(int)
    )

    """
    now get the make order requests we sent up to now, this may not be everything but a fast way to retrieve some
    we can also go back to order source
    """

    def as_snowlfake_table_select(topic):
        return f""" SELECT * FROM "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_{topic.upper().replace('.','_')}" """

    req_data = snowflake.execute(
        as_snowlfake_table_select("res_make.make_one_request.create_response")
    )
    # use the history of body versions for what we have made
    req_data["rc"] = req_data["RECORD_CONTENT"].map(json.loads)
    req_data = pd.DataFrame([d for d in req_data["rc"]])

    req_data["sku"] = req_data["sku"].map(lambda x: f"{x[:2]}-{x[2:]}")
    # careful to rank on an index sorted in time, drop and recreate
    rl = (
        req_data[
            [
                "line_item_creation_date",
                "sku",
                "one_number",
                "order_number",
                "body_version",
            ]
        ]
        .sort_values("line_item_creation_date")
        .reset_index(drop=True)
        .reset_index()
    )
    rl["RANK"] = (
        rl.groupby(["order_number", "sku"])["index"].rank(ascending=True).astype(int)
    )

    """
    Merge these together to create a new payload that we can send
    - we have merely correct the order and added a one number from a mapping
    - rather than use a sku map above, we have joined on the one numbers known to be associated with orders
    """

    def fix_up_payload(x):
        p = x["payload"]
        p["metadata"]["order_name"] = x["order_number"]
        p["one_number"] = int(float(x["one_number_y"]))
        p["metadata"]["body_version"] = (
            int(x["body_version"]) if pd.notnull(x["body_version"]) else None
        )
        return p

    test = pd.merge(ooor, rl, how="left", on=["order_number", "sku", "RANK"])
    test = test[test["index_y"].notnull()]
    test["payload"] = test.apply(fix_up_payload, axis=1)
    # PP = dict(test.iloc[1]["payload"])

    """
    we can send payloads again after doing some filtering on what we have in make already
    this last part is understanding if we have all the make one requests per order or not
    """
    kafka = res.connectors.load("kafka")
    K = kafka["res_make.orders.one_requests"]
    for PP in test["payload"][40:]:
        K.publish(PP, use_kgateway=True)


def _get_make_one_production_requests_not_in_one_make():
    """
    check what we are missing regularly and cache  "s3://res-data-platform/cache/pending_make_one.parquet"
    """
    from res.flows.make.production.utils import get_all_ones

    Q = """query MyQuery {
      make_one_orders {
        id
        order_number
        one_number
        created_at
      }
    }
    """
    hasura = res.connectors.load("hasura")
    res.utils.logger.info("fetching make orders that we have")
    odf = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_orders"]).sort_values(
        "created_at"
    )

    of = get_all_ones()
    of["one_number"] = of["one_number"].map(int)

    stuff = of[~of["one_number"].isin(odf["one_number"])]
    stuff = stuff[stuff["is_cancelled"] == False].reset_index(drop=True)

    res.utils.logger.info(
        f"saving the cached record set of size {len(stuff)} to s3://res-data-platform/cache/pending_make_one.parquet"
    )
    res.connectors.load("s3").save(
        "s3://res-data-platform/cache/pending_make_one.parquet", stuff
    )

    return stuff


@res.flows.flow_node_attributes(
    memory="12Gi",
)
def fill_make_orders_onenum(event, context=None):
    """
    among other things this

    A job that checks what make orders we have and fills up
    it uses the known one numbers
    the gap here is if the one is not yet assigned we dont try to find it which could be a separate process
    this one is easier to focus on for now
    """

    from res.flows.make.production.inbox import process_requests
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    df = res.connectors.load("s3").read(
        "s3://res-data-platform/cache/pending_make_one.parquet"
    )
    Q = """query MyQuery {
      make_one_orders {
        id
        order_number
        one_number
      }
    }
    """
    hasura = res.connectors.load("hasura")
    res.utils.logger.info(f"Loading context")
    odf = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_orders"])
    mos = list(odf["order_number"])
    res.utils.logger.info(f"We have {len(mos)}")

    odf["one_number"] = odf["one_number"].map(int)
    df["one_number"] = df["one_number"].map(int)

    # if we dont have the item in our ones which includes not having it even if there is no one, we will try to remake it
    work = df[~df["one_number"].isin(mos)]
    res.utils.logger.info(f"We have {len(work)} work todo")

    R = None
    for order_number in work["order_number"].unique():
        # order = get_order_by_name(order_number)
        # generate the request payload for the make order
        try:
            # a re pair function must check what one numbers are mapped lest it replace things that are not yet mapped
            make_requests = FulfillmentNode.get_make_order_request_for_order(
                order_number, map_one_numbers=True
            )
            # the production inbox does this - you can post to kafka or just do this
            R = process_requests(make_requests)
        except:
            # fail and try again later
            res.utils.logger.warn(traceback.format_exc())
    return R


def update_existing_one_piece_ids_to_expected(existing):
    """
    in some rare cases the one piece id was genearted with an incorrect id
    the id is supposed to be bound to the ranked order item
    when it is not we can get the parent id again and regenerate the id and replace the one in the database
    """
    from tqdm import tqdm

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def migrate_one_piece_id(record, hasura=None):
        M = """mutation MyMutation($id: uuid = "", $nid: uuid = "") {
          update_make_one_pieces(where: {id: {_eq: $id}}, _set: {id: $nid}) {
            returning {
              id
            }
          }
        }
        """
        hasura = hasura or res.connectors.load("hasura")

        return hasura.execute_with_kwargs(M, id=record["id"], nid=record["expected_id"])

    violations = existing["one_pieces"]
    violations = pd.DataFrame(violations)
    v1 = violations[violations["id"] != violations["expected_id"]]
    if len(v1):
        res.utils.logger.info(f"There are vioations")
        for record in tqdm(v1.to_dict("records")):
            a = migrate_one_piece_id(record)
    return violations


def reconcile_order_on_failure(order_number, plan=False, hasura=None):
    """
    the oid is already in use because
    - a different ranked order has it
      - this causes problems because an existing piece (rank) is using the oid
    - a different request id was used to save the order/orderitem
      - this leads to a conflict in the incoming ids that need to be saved because a new piece (id) is trying to use an existing oid

    RULES:
    - the make order MUST match the most recent
    """

    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode
    from res.flows.make.production.inbox import respond_to
    from res.flows.make.production.queries import (
        get_make_order_with_pieces_by_order_number,
    )

    r = FulfillmentNode.get_make_order_request_for_order(
        order_number, map_one_numbers=True
    )
    requests = [respond_to(req) for req in r]
    for r in requests:
        r["expected_id"] = r["request_id"]
        for p in r["one_pieces"]:
            p["id"] = res.utils.uuid_str_from_dict(
                {"request_id": r["request_id"], "piece_code": p["code"]}
            )
            p["oid"] = res.utils.uuid_str_from_dict(
                {"one_number": int(float(r["one_number"])), "piece_code": p["code"]}
            )

    existing = get_make_order_with_pieces_by_order_number(order_number)
    for r in existing:
        # r['expected_id'] = r['request_id'] -> map by ordinal alater

        for p in r["one_pieces"]:
            p["expected_id"] = res.utils.uuid_str_from_dict(
                {"request_id": r["id"], "piece_code": p["code"]}
            )
            p["expected_oid"] = res.utils.uuid_str_from_dict(
                {"one_number": int(float(r["one_number"])), "piece_code": p["code"]}
            )

        if not plan:
            update_existing_one_piece_ids_to_expected(r)

    return requests, existing


# reconcile_cancelled_ones()
def full_repair_order(order_number, try_reconcile=False):
    """

    util to hard repair from the sell order right up to the make order with the known one numbers
    may still need t reconcile the one numbers + check we are idempotetnt and dont steal one numbers on the make order


    #reasons for failure:
    1
        material swap not properly managed. resolution is some kind of product transfer and then call this method again to use the new product link

        #example with past swap
        from res.flows.sell.orders.queries import transfer_product_to_new_style
        transfer_product_to_new_style('0da097fd-44a1-c539-390b-3bd5e938ddc5',  new_style_id=m1.style_id, metadata={"old_sku": "CC-8081 CTPKT LILANL 5ZZXL"})
    2
       the order items are not properly saved in sell e..g bad data so that we cannot create the proper order ids
       we need to validate old

    """

    from schemas.pydantic.sell import Order
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode
    from res.flows.make.production.inbox import process_requests

    from res.flows.sell.orders.queries import (
        get_warehouse_airtable_orders_by_source_order_key,
        get_order_by_name,
    )

    chk = get_order_by_name(order_number)
    if not chk:
        res.utils.logger.info(
            "We do not have the order so fetching from warehouse and saving"
        )
        O = Order.from_create_one_warehouse_payload(
            get_warehouse_airtable_orders_by_source_order_key(order_number)
        )
        result = FulfillmentNode.run_request_collector(O)
    res.utils.logger.info(f"Getting the make request for {order_number}")
    a = FulfillmentNode.get_make_order_request_for_order(
        order_number, map_one_numbers=True
    )

    # hand hold for now - some times we can fix like this
    # if try_reconcile:
    #     # this is a dangerous retry because we dont understand how the ids on the piece got out of whack
    #     # the theory is we dont care what the ids are (noting joins that cannot be cascaded) but we do need them correct for upserts
    #     res.utils.logger.info("trying to reconcile ids")
    #     a, b = reconcile_order_on_failure(order_number)
    #     IF the problem is in sell we can re-import old orders - in the limit we should always have the correct ones so probably not worth building in here as could cause confusion
    #     IF sell is correct we can always remake the order here once there is (a) an order map and (b) no oid conflicts
    #     reconciliation would be a way to pair up the one numbers in that context if we had to but that depends if there is no other way

    R = process_requests(a, on_error="raise")

    # we should get the reasons for failure in the order
    # -style not registered
    # -sell order ids?
    # -constrain violation on oid ad piece or order level-> former impliess bad id in database
    #

    return R


def test_get_illegal_pieces():
    """
    one of our tests is to make sure there is never a piece that has an oid when the parent does not
    this screws up future batch assignments
    """
    ILLEGALS = """query MyQuery {
    make_one_orders(where: {oid: {_is_null: true}}) {
        oid
        id
        one_number
        created_at
        updated_at
        order_number
        one_pieces(offset: 1, where: {oid: {_is_null: false}}) {
        id
        }
    }
    }
    """
    hasura = res.connectors.load("hasura")

    di = pd.DataFrame(hasura.execute(ILLEGALS)["make_one_orders"])
    todo = di[di["one_pieces"].map(lambda x: len(x) > 0)]
    return todo


def sync_make_piece_increments_with_requests_table(plan=False):
    """
    given a known healing request table, seed the ones pieces make increment counter to be 1 + the healing requests
    in future any process should only update this value in the context of the healing requests: `update_healing_status`
    """

    from res.flows.make.production.queries import update_make_one_pieces_increment

    # load the pieces
    Q = """query MyQuery {
      make_one_pieces {
        code
        id
        oid
        one_order_id
        make_instance
      }
    }
    """
    from tqdm import tqdm

    hasura = res.connectors.load("hasura")

    pieces = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_pieces"])
    pieces = pieces[pieces["oid"].notnull()]
    pieces["make_instance"] = pieces["make_instance"].fillna(1).map(int)
    # load the make increments
    RQ = """query MyQuery {
      make_one_piece_healing_requests {
        request_id
        piece_id
      }
    }
    """
    r = pd.DataFrame(hasura.execute_with_kwargs(RQ)["make_one_piece_healing_requests"])
    r = r.groupby("piece_id").count().reset_index()

    #################
    r["make_instance"] = r["request_id"] + 1
    ##################

    # join out stuff that has no matching id
    # AND we can also join out anything that DOES have a matching count already and tehrefore needs no upadte
    r = pd.merge(
        r, pieces, left_on=["piece_id"], right_on=["oid"], suffixes=["", "_make"]
    )
    r = r[r["make_instance"] != r["make_instance_make"]]

    if plan:
        return r

    # start the value updates
    for record in tqdm(r.to_dict("records")):
        a = update_make_one_pieces_increment(
            oid=record["piece_id"], make_instance=record["make_instance"]
        )


@res.flows.flow_node_attributes(
    memory="10Gi",
)
def cache_mone_state(event, context={}, hours_ago=4):
    """
    deprecated for hourly scheduler
    """
    from res.flows.make.production.queue_update import ingest_queue_data

    """
    get any changes modified today and write the bridge table updates too
    """
    # we run this usually on a 1 hourly schedule anyway
    # see the apps/ infra / task scheduler - calls cache mone

    ingest_queue_data(
        since_date=res.utils.dates.utc_hours_ago(hours_ago), write_bridge_table=True
    )

    return {}


def save_cancelled_make_orders(order_number, sku_map, plan=True):
    """
    example:

    O = save_cancelled_make_orders(order_number='BG-2129233', sku_map={10300629 : 'CC-2099 CUTWL RAINQH Z2900'}, plan=False)

    """
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode
    from res.flows.make.production.inbox import respond_to

    # get a subset of the marker order for the skus we have
    make_request_for_order = FulfillmentNode.get_make_order_request_for_order(
        order_number, sku_filter=list(sku_map.values())
    )

    # go through all the pars of one numbers and skus and take a delegate request for that squ to generate the cancellation data
    for one_number, sku in sku_map.items():
        make_request_for_sku = [f for f in make_request_for_order if f["sku"] == sku][0]
        make_request_for_sku = respond_to(make_request_for_sku, one_number=one_number)

        # transfer ids
        # the id we are going to choose matches the oid - it may be that we already saved with a different id in which case we will just fail
        # but in future when we try to save we will use this scheme
        make_request_for_sku["request_id"] = res.utils.uuid_str_from_dict(
            {"id": one_number}
        )
        make_request_for_sku["status"] = "CANCELLED"
        for p in make_request_for_sku["one_pieces"]:
            # same for pieces - they are unique ids that are the same as the oid
            p["id"] = p["oid"]

        O = OneOrder(**make_request_for_sku)
        if not plan:
            res.utils.logger.info("saving")
            assert O.id == O.oid, "Illegal ids for cancelled orders"
            assert (
                O.one_pieces[0].id == O.one_pieces[0].oid
            ), "Illegal piece ids for cancelled orders"
            return FlowAPI(OneOrder).update_hasura(O)

    # just a sample
    return O


@res.flows.flow_node_attributes(
    memory="10Gi",
)
def process_cancellations(event, context={}):
    """ """

    from tqdm import tqdm

    QO = """query MyQuery {
      make_one_orders {
        id
        order_number
        one_number
        order_item_id
        sku
        order_item_rank
      }
    }
    """

    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")

    LU = pd.DataFrame(hasura.execute_with_kwargs(QO)["make_one_orders"])

    uri = "s3://res-data-platform/samples/data/snapshot-mones/mones_all.parquet"
    data = s3.read(uri)
    data["one_number"] = data["one_number"].fillna(0).map(int)
    # cancelled
    print("total orders", len(data))
    data = data[data["is_cancelled"] == 1]
    print("cancelled orders", len(data))
    data = data[data["order_number"].isin(LU["order_number"])]
    print("orders we know about in hasura", len(data))
    data = data[~data["one_number"].isin(LU["one_number"])]
    print("where we dont already have the cancelled ones", len(data))

    for order_number, records in tqdm(data.groupby("order_number")):
        print(f'<<<<<<<<<<< {order_number, records.to_dict("records")} >>>>>>>>>>>')
        sku_map = dict(records[["one_number", "sku"]].values)

        try:
            save_cancelled_make_orders(
                order_number=order_number, sku_map=sku_map, plan=False
            )
        except Exception as ex:
            print(ex)
            pass  # handle me


def bootstrap_bridge_ones(plan=False, in_make_only=False):
    """
    these needs to be run once of if we think we have dropped records of known ones
    interesting to run it every now and again and see what did not get logged as it may suggest a need to repair other things
    """
    res.utils.logger.info(f"getting all ones")
    data = get_all_ones(in_make_only=in_make_only)
    res.utils.logger.info(f"getting all existing bridge records")
    data["one_number"] = data["one_number"].map(int)
    Q = """query MyQuery {
      infraestructure_bridge_sku_one_counter {
        sku
        one_number
        created_at
        style_sku
      }
    }
    """

    hasura = res.connectors.load("hasura")

    ndata = pd.DataFrame(
        hasura.execute_with_kwargs(Q)["infraestructure_bridge_sku_one_counter"]
    )
    fdata = data[~data["one_number"].isin(ndata["one_number"])]

    res.utils.logger.info(f"thie diff is length {len(fdata)}")

    if plan:
        return fdata

    M = """mutation MyMutation($objects: [infraestructure_bridge_sku_one_counter_insert_input!] = {}) {
      insert_infraestructure_bridge_sku_one_counter(objects: $objects, on_conflict: {constraint: bridge_sku_one_counter_pkey, 
        update_columns: [airtable_rid, sku, style_sku, one_number, order_number, created_at]}) {
        returning {
          id
          order_number
          one_number
          sku
        }
      }
    }
    """

    def fix_sku(s):
        if s[2] != "-":
            return f"{s[:2]}-{s[2:]}"
        return s

    # data is the filtered set
    data = res.utils.dataframes.replace_nan_with_none(fdata)
    data["one_number"] = data["one_number"].map(int)
    data["created_at"] = data["created_at"].map(
        lambda x: str(x) if x is not None else None
    )
    data["cancelled_at"] = data["cancelled_at"].map(
        lambda x: str(x) if x is not None else None
    )

    data["style_sku"] = data["sku"].map(
        lambda x: f" ".join([a for a in x.split(" ")[:3]])
    )

    data["sku"] = data["sku"].map(fix_sku)
    from tqdm import tqdm

    def update(record):
        return hasura.execute_with_kwargs(M, objects=[record])

    for record in tqdm(
        data.rename(columns={"record_id": "airtable_rid"})
        .drop(["contracts_failed", "is_cancelled"], 1)
        .to_dict("records")
    ):
        record["id"] = res.utils.uuid_str_from_dict(
            {"one_number": int(record["one_number"])}
        )

        try:
            r = update(record)
        except Exception as ex:
            print(ex, "failure in saving to bridge")

    # return what we need to add
    return fdata


def what_are_we_missing():
    Q = """query MyQuery {
      make_one_orders {
        id
        order_number
        one_number
        order_item_id
        sku
        order_item_rank
      }
    }
    """

    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")
    snowflake = res.connectors.load("snowflake")

    # deterine what we have that is cancelled
    df = get_all_ones(in_make_only=False)
    cancelled_ones = df[df["is_cancelled"] == 1]["one_number"]
    len(cancelled_ones)
    cancelled_one_numbers = list(cancelled_ones.map(int))

    # some examples of missing
    missing = s3.read(
        "s3://res-data-platform/res-make-missing-orders/missing_ones_v2.csv"
    )
    missing["one_number"] = missing["one_number"].map(int)
    print(len(missing))
    # but not cacnelled
    missing = missing[~missing["one_number"].isin(cancelled_one_numbers)]
    # what do we have in make now
    LU = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_orders"])
    still_missing = missing[~missing["one_number"].isin(LU["one_number"])].reset_index(
        drop=True
    )
    # still missing
    s3.write(
        "s3://res-data-platform/res-make-missing-orders/still_missing_orders.csv",
        still_missing,
    )

    # filter by things that were requested
    # load anything that was ever requested
    data = snowflake.execute(
        f""" select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_MAKE_ONE_REQUEST_CREATE_RESPONSE"   """
    )
    data["payload"] = data["RECORD_CONTENT"].map(json.loads)
    payload = data.iloc[0]["payload"]
    order_map = dict(
        pd.DataFrame([d for d in data["payload"]])[
            ["one_number", "order_number"]
        ].values
    )
    all_prod_requests = pd.DataFrame([d for d in data["payload"]])

    all_prod_requests["one_number"] = all_prod_requests["one_number"].map(int)
    all_prod_requests["sku"] = all_prod_requests["sku"].map(
        lambda x: f"{x[:2]}-{x[2:]}"
    )
    all_prod_requests.head()
    md = pd.merge(still_missing, all_prod_requests, on="one_number")

    return md.sort_values("order_number")


def restore_make_orders_from_reference_mappings(m=None, eraise=False):
    """
    for simplicity here we assume the sell order exists
    if it does, we assume the order map has been reconciled / and assume we are not asking for anything that has been cancelled - the thing fixes itself anyway because we can overwrite the one number ?? check oid
    if we can pair the one we do so
    if we cannot it will be because the make order does not exist
    - we try to make the request from the order item - if that fails we flag the missing order
    if we can make the request, we can pin the one number onto the request and save it

    #get missing items which come from a file of missing one number orders
    #filter out anything that does not have sell orders
    #we could reimport sell orders with
    ## md[md['order'].map(len)==0]
    ## for order in md[md['order'].map(len)==0]['order_number']:
    ##     reimport_order(order)

    md = what_are_we_missing()
    md = md[md['sales_channel'] != 'OPTIMUS']
    md['order'] = md['order_number'].map(get_order_by_name)
    orders_exist = md[md['order'].map(len)>0]
    #you could save orders somewhere to work on them
    ##orders_exist.to_pickle("/Users/sirsh/Downloads/bad_orders_sample.pkl")

    """
    # lookup make orders -> generate ranks and verify ids
    # if we have the order (a) and if we have the one number (b) then skip
    # if we dont have the order and we have it in sell, save it now with the one number
    # if we dont have it in sell, request it and save it with full recovery and the one number
    # if we have the order but it has no one number, pair it now

    from res.flows.make.production.inbox import pair_make_request_to_one_order

    fail = []
    missing = []
    d = {}
    Q = """query MyQuery {
      make_one_orders {
        id
        order_number
        one_number
        order_item_id
        sku
        order_item_rank
      }
    }
    """
    hasura = res.connectors.load("hasura")
    LU = pd.DataFrame(hasura.execute_with_kwargs(Q)["make_one_orders"])
    print(len(LU))
    LU.head(1)
    for record in LU.to_dict("records"):
        d[int(record["one_number"])] = record

    full_repairs = ["DJ-2131783"]
    for record in m.to_dict("records")[0:]:
        one_number = int(record["one_number"])
        # print(one_number)
        if one_number in d:
            chk = d[one_number]
            res.utils.logger.info(f"we already have {chk}")
            continue
        # break

        try:
            # its important that this function does not raise but always return a result or NONE
            # if we have no result we try a full repair
            R = pair_make_request_to_one_order(
                sku=record["sku"],
                order_number=record["order_number"],
                one_number=int(record["one_number"]),
            )
            if R is None:
                # if the make order does not exist (assuming sell exists here) then make it right now using the original informatio of the one
                # we do this by looking up the one map again and then doing the thing
                on = record["order_number"]
                try:
                    if on not in full_repairs:
                        full_repair_order(on)
                        full_repairs.append(on)
                except:
                    print("COULD NOT REPAIR THIS ORDER", on)
                    pass  #
                missing.append(record)
        except Exception as ex:
            fail.append(one_number)
            print("Failed in processing pairing", one_number, ex)
            if eraise:
                raise
            # raise
    return fail, missing


@res.flows.flow_node_attributes(memory="10Gi", mapped=False)
def handle_rebuild_from_bridge(event={}, context={}):
    postgres = res.connectors.load("postgres")

    res.utils.logger.info(f"Fetch data from bridge")
    Q = f"""SELECT created_at, order_number, one_number,ordered_at, sku from infraestructure.bridge_sku_one_counter order by created_at desc"""
    pdata = postgres.run_query(Q)

    res.utils.logger.info(f"Fetch data from orders")
    Q = f"""SELECT id, sku, order_number, one_number from make.one_orders order by updated_at desc"""
    modata = postgres.run_query(Q)

    missing = pdata[~pdata["order_number"].isin(modata["order_number"])]

    res.utils.logger.info(f"Begin ingest")
    failed = []
    for o in missing["order_number"][1:]:
        try:
            full_repair_order(o)
        except KeyboardInterrupt:
            break
        except Exception as ex:
            print("failed", o, ex)
            failed.append(o)


if __name__ == "__main__":
    handle_rebuild_from_bridge()
