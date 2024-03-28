"""
This file contains multiple flows not all of which will be maintained. 
The primary flow is to pull orders from shopify, cache them in Dynamo and write them to S3->Druid
Much of this functionality could be refactored out of here into schema managers and connectors so that this should become much simpler




Some notes on counts
- TUCKER orders seem to behave well i.e. valid SKUs, numbers on snow similar to fetched
- RM seems to have mostly non "valid" skus - double check product id matches. RM is slow and we can probably leave that aside for testing for now
- check that we have all the orders in dynamo for everyone else (run last month first including RB)
- if there are no bugs for anyone else we can re-rnu RB sometime
- move all data from dynamo into graph and switch over to use that as primary (make fulfillments a child)
- KT (the kit) seems to have about 40k shopify orders but only about half that were pulled from shopify 
- start looking for specific keys we are missing but for that reginest with ignore dimensions on the id 
- lots of null brand codes in shopify!?
- we need to get orders from non shopify locations and ingest them into one_orders
- check RM just for a period e.g. the month of April 2021

#these are the sort of numbers that snowflak had as of mid-may for order items
Renowned	        1569
Dressed in Joy	    2216
JungleGurl	        2324
SUPERVSN	        2853
JCRT-ONE	        4046
JCRT	            4533
Pyer Moss	        5224
Reebok by Pyer Moss	5381
Tucker	            13684
Rebecca Minkoff	    16666
jcrt	            24758
thekitnyc	        41859
tuckernyc	        45267
Rebecca-Minkoff	    767792
---------------------------
[[NaN	            587388]]
"""

from res.connectors.dynamo import ShopifyOrdersDynamoTable
from . import FlowContext
from res.utils import logger, dataframes, safe_http
from . import dataframes

import pandas as pd
from datetime import datetime, timedelta
from ast import literal_eval
from res.connectors.airtable.formatting import remove_pseudo_lists, strip_emoji
import json
from pathlib import Path
import dateparser
import os
from res.connectors.airtable import AirtableConnector
from res.connectors.dgraph import DgraphConnector
from res.flows import relay

ORDERS_S3_PATH = "s3://res-data-platform/temp/orders.parquet"
# https://airtable.com/tblSA4TlAVbiQM5hj/viwgYFcU79YpR3ojQ?blocks=hide
SHOPIFY_CREDS_TABLE = "tblSA4TlAVbiQM5hj"
SHOPIFY_CREDS_BASE = "appc7VJXjoGsSOjmw"
# used to join the shopify line item record to our style/product record
SHOPIFY_PRODUCT_ID_COLUMN = "product_id"
RES_PRODUCT_ID_COLUMN = "product_id"
RAW_ORDER_TOPIC = "shopifyOrders"
PROCESSED_ORDER_TOPIC = "res_sell.one_orders.one_orders"
ORDER_STATUS_CHANGED = "orderStatusChanges"
DISTINCT_PROCESS_ORDER_TOPIC = "res_sell.one_orders.one_orders_distinct"
DEFAULT_WATERMARK_DELTA = 2
# TODO: probably do not need this if we use an active field
# TODO - temp removing RB to test
_EXCLUDE_SHOPIFY_USERS = ["techpirates@resonance.nyc"]
SELECT_STYLE_FIELDS = [
    # "Brand_Name",
    "Body Code",
    "Key",
    "__colorcode",
    "__materialcode",
    "__materialname",
]
STYLE_FIELD_RENAME = {
    "_id": "style_id",
    "__colorcode": "color_code",
    # "Brand_Name": "brand_name",
    "__materialname": "material_name",
    "__materialcode": "material_code",
    "Body Code": "body_code",
    "Key": "key",
}
METRIC_KEY = "M_SHOPIFY"
DGRAPH_ORDER_TABLE_NAME = "test_order"
SHOPIFY_API_REQ_LIMIT = 100


CREATE_ONE_TO_MAKE_ORDERS_MAPPING = {}

#### This section is for fleshing out some use of s3 partitioning and druid
#### this logic should move into other places once fleshed out


def get_batch_submit_order_items_spec():
    d = {
        "type": "index_parallel",
        "spec": {
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": {
                    "type": "s3",
                },
                "inputFormat": {"type": "parquet", "binaryAsString": True},
                "appendToExisting": False,
            },
            "tuningConfig": {
                "type": "index_parallel",
                "partitionsSpec": {"type": "dynamic"},
            },
            "dataSchema": {
                "dataSource": "batch_one_orders",
                "granularitySpec": {
                    "type": "uniform",
                    "queryGranularity": "HOUR",
                    "rollup": False,
                    "segmentGranularity": "DAY",
                },
                "timestampSpec": {"column": "created_at", "format": "iso"},
                "dimensionsSpec": {
                    "dimensions": [
                        "body_code",
                        "brand_code",
                        "brand_cohort",
                        "brand_name",
                        "color_code",
                        "fulfillment_status",
                        "is_regular_order",
                        "is_repeat_order",
                        "is_valid_one",
                        "key",
                        "material_code",
                        "name",
                        "name_order",
                        "one_order_key",
                        "sku",
                        "style_name",
                        "title",
                        "variant_title",
                        "order_channel",
                    ]
                },
                "metricsSpec": [
                    {"name": "count", "type": "count"},
                    {"name": "sum_grams", "type": "longSum", "fieldName": "grams"},
                    {
                        "name": "sum_order_number",
                        "type": "longSum",
                        "fieldName": "order_number",
                    },
                    {"name": "sum_price", "type": "doubleSum", "fieldName": "price"},
                    {
                        "name": "sum_quantity",
                        "type": "doubleSum",
                        "fieldName": "quantity",
                    },
                    {
                        "name": "sum_total_discount",
                        "type": "doubleSum",
                        "fieldName": "total_discount",
                    },
                    {
                        "name": "sum_total_tax",
                        "type": "doubleSum",
                        "fieldName": "total_tax",
                    },
                    # {
                    #     "name": "sum_total_shipping",
                    #     "type": "longSum",
                    #     "fieldName": "total_shipping",
                    # },
                ],
            },
        },
    }
    return d


def get_recent_partitions(
    fc,
    watermark=7,
    search_root="s3://res-data-platform/entities/order_items/ALL_GROUPS",
):
    s3 = fc.connectors["s3"]

    since = (datetime.utcnow() - timedelta(days=watermark)).date()

    recs = list(s3.ls_info(search_root, modified_after=since))

    changed_partitions = [p["path"] for p in recs]

    def is_on_or_after(p, since):
        try:
            return dateparser.parse(Path(p).stem).date() >= since
        except:
            return False

    return [p for p in changed_partitions if is_on_or_after(p, since)]


def get_batch_submit_template_by_brands(
    brand_codes, root="s3://res-data-platform/entities/order_items/"
):
    """
    DEPRECATE

    tried using brand folders but this causes issues in druid for segment replacement
    use ALL_GROUPS instead and partition by date in here with merge brands
    """
    if not isinstance(brand_codes, list):
        brand_codes = [brand_codes]

    d = get_batch_submit_order_items_spec()

    d["spec"]["ioConfig"]["inputSource"]["prefixes"] = [
        f"{root}/{bc}/" for bc in brand_codes
    ]

    return d


def get_batch_submit_template_by_paths(paths):
    if not isinstance(paths, list):
        paths = [paths]
    d = get_batch_submit_order_items_spec()

    d["spec"]["ioConfig"]["inputSource"]["uris"] = paths

    return d


def rebuild_initial_order_item_partitions(fc):
    # dc = fc["druid"]
    brand_codes = [d["args"]["brand_code"] for d in list_brands({}, {})]
    logger.info(
        f"updating partitions for brands {brand_codes} - and checking that the partition folders exist"
    )
    spec = get_batch_submit_template_by_brands(brand_codes=brand_codes)
    # dc.submit(spec)
    DRUID_URL = os.getenv(
        "DRUID_URL", "http://druid-router.druid.svc.cluster.local:8888"
    )  # "http://localhost:8005"
    return safe_http.request_post(f"{DRUID_URL}/druid/indexer/v1/task", json=spec)


def merge_recent_brand_partitions(event, context):
    """
    Unfortunate that we need to do this. We will need to refactor
    Annoyingly we par fetch brands and write to their own partitions but Druid needs segments per file
    But if we write to the same segments in parallel in fetching orders we would have concurrency issues
    Hence we need a merge step to merge partitions from multiple files based on dates into single partitions
    """
    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]

        search_root = "s3://res-data-platform/entities/order_items/"

        parts = [
            p
            for p in get_recent_partitions(fc=fc, search_root=search_root)
            if "ALL_GROUPS" not in p
        ]

        orders = pd.concat([pd.read_parquet(p) for p in parts])

        # temp the types are a bit messed up with history
        orders["id_order"] = orders["id_order"].map(int).astype(int)
        orders["id"] = orders["id"].map(int).astype(int)
        orders["is_regular_order"] = orders["is_regular_order"].map(as_bool)
        orders["is_repeat_order"] = orders["is_repeat_order"].map(as_bool)
        orders["is_valid_one"] = orders["is_valid_one"].map(as_bool)

        logger.info("repartitioning order item data for all groups")

        s3.write_date_partition(
            orders,
            group_partition_key=None,
            date_partition_key="created_at",
            dedup_key="one_order_key",
            entity="order_items",
        )


def update_druid_partitions(event, context):
    with FlowContext(event, context) as fc:
        # first we will merge brand partitions into ALL_GROUPS for Druid

        merge_recent_brand_partitions(event, context)

        response = rebuild_changed_order_item_partitions(fc=fc)

        if response.status_code != 200:
            raise Exception(
                "The druid task did not return a 200 status code - check ingestion tasks"
            )


def rebuild_changed_order_item_partitions(fc, watermark=30):
    # dc = fc["druid"]
    # dc.submit(spec)
    # any updated partitions with keys for the last month
    logger.info(
        f"submitting task for all partitions since watermark backfill {watermark}"
    )

    partitions = get_recent_partitions(fc=fc, watermark=watermark)
    # make a spec for this partitions
    spec = get_batch_submit_template_by_paths(partitions)
    logger.debug(f"submitting spec for recent partitions - {spec}")
    DRUID_URL = os.getenv(
        "DRUID_URL", "http://druid-router.druid.svc.cluster.local:8888"
    )  # "http://localhost:8005"
    return safe_http.request_post(f"{DRUID_URL}/druid/indexer/v1/task", json=spec)


def merge_partitions(event, context):
    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]
        brand_codes = [d["args"]["brand_code"] for d in list_brands({}, {})]
        data = []

        logger.info("reading data...")

        for b in brand_codes:
            for file in s3.ls(f"s3://res-data-platform/entities/order_items/{b}/"):
                bdata = pd.read_parquet(file)
                logger.info(f"Read {len(bdata)} rows from brand {b}")
                data.append(bdata)

        data = pd.concat(data)

        logger.info(f"writing partitions for {len(data)} rows with brand data")

        s3.write_date_partition(
            data,
            group_partition_key=None,
            date_partition_key="created_at",
            dedup_key="one_order_key",
            entity="order_items",
        )


def _recreate_index():
    dfs = []
    so = ShopifyOrdersDynamoTable()
    table = so._table_ref

    logger.info("building index to date for all orders")
    response = table.scan(
        ProjectionExpression="brand,#n,created_at",
        ExpressionAttributeNames={"#n": "name"},
    )
    dfs.append(pd.DataFrame(response["Items"]))
    c = 0
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="brand,#n,created_at",
            ExpressionAttributeNames={"#n": "name"},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        dfs.append(pd.DataFrame(response["Items"]))
        c += 1

    dfs = pd.concat(dfs).reset_index(drop=True)
    pth = "s3://res-data-platform/entities/order_items/.meta/index_cache.parquet"
    logger.info(f"saving index to {pth}")
    dfs.to_parquet(pth)

    print("done")


def rebuild_partitions(event, context):
    """

    dfs = []
    response =  table.scan(ProjectionExpression='brand,#n,created_at',ExpressionAttributeNames = {'#n': 'name'})
    dfs.append(pd.DataFrame(response['Items']))
    c = 0
    while 'LastEvaluatedKey' in response:
        response = table.scan(ProjectionExpression='brand,#n,created_at',ExpressionAttributeNames = {'#n': 'name'},ExclusiveStartKey=response['LastEvaluatedKey'])
        dfs.append(pd.DataFrame(response['Items']))
        c+=1

    dfs = pd.concat(dfs).reset_index().drop('index',1)
    print(c)
    dfs.to_parquet("s3://res-data-platform/entities/order_items/.meta/index_cache.parquet")

    """
    with FlowContext(event, context) as fc:
        so = ShopifyOrdersDynamoTable()
        kc = fc.connectors["kafka"]
        s3 = fc.connectors["s3"]
        processed_topic = "resSellOneOrders"
        topic_info = kc[processed_topic]
        schema = topic_info.topic_schema

        # this takes time so we dont always have to do it
        _recreate_index()

        # we may rebuild this with new indexes
        idx = pd.read_parquet(
            "s3://res-data-platform/entities/order_items/.meta/index_cache.parquet"
        )

        idx["key"] = idx.apply(lambda x: f"{x['brand']}/{x['name']}", axis=1)
        # we have some that dont have the fields we need for the new version. e.g. the correct keys and brands etc
        idx = idx.dropna()
        keys = idx["key"]

        logger.info(f"Batch keys length {len(keys)}")
        # on a big machine
        logger.info("reading data")
        orders = so.read_by_keys(keys)

        logger.info("parsing data")
        # for data in so._yield_by_keys(keys):

        # its worrying that this can happen for any case

        def try_parse(d):
            try:
                return json.loads(d)
            except Exception as ex:
                print("fail", ex)

        data = []
        failures = []
        counter = 0
        for _, row in orders[["_id", "brand", "json"]].iterrows():
            d = try_parse(row["json"])
            key = row["_id"]
            brand = row["brand"]
            if d is not None:
                # im not sure if there are case where we knew what brand we fetched from shopify but the data did not have it
                d["brand"] = brand
                d["brand_code"] = brand
                data.append(d)
            else:
                counter += 1
                failures.append(key)
                print("failed to parse...", key, counter)

        if len(failures) > 0:
            pd.DataFrame(failures, columns=["failures"]).to_csv(
                "s3://res-data-platform/entities/order_items/.meta/failed_keys.parquet"
            )

        orders = pd.DataFrame(data)

        # brand_orders = dict(orders[["id", "brand_code"]].astype(str).values)

        # orders.to_csv(
        #     "s3://res-data-platform/entities/order_items/.meta/temp_orders.csv"
        # )

        print("ORDERS")
        print(orders[["id", "brand_code"]].groupby("brand_code").count())

        logger.info("lu for conversions")

        orders = _try_coerce_schema(fc, orders)
        orders = _merge_orders_with_style_attributes(fc, orders)

        # orders.to_csv(
        #     "s3://res-data-platform/entities/order_items/.meta/temp_orders_items.csv"
        # )

        # the brand code is not always filled when we dont have ONEs for this brand e.g. RB
        # because we resolve the brand code eventually by the product

        logger.info(f"Checking types for order items length {len(orders)}")
        orders = dataframes.coerce_to_avro_schema(
            orders,
            avro_schema=schema[processed_topic],
        )

        print(orders.iloc[0])

        orders["id_order"] = orders["id_order"].map(int).astype(int)
        orders["id"] = orders["id"].map(int).astype(int)
        orders["is_regular_order"] = orders["is_regular_order"].map(as_bool)
        orders["is_repeat_order"] = orders["is_repeat_order"].map(as_bool)
        orders["is_valid_one"] = orders["is_valid_one"].map(as_bool)

        print("ORDER ITEMS")
        print(orders[["id", "brand_code"]].groupby("brand_code").count())

        logger.info(f"saving partitions for orders of size {len(orders)}")

        result = s3.write_date_partition(
            orders,
            entity="order_items",
            group_partition_key=None,  # "brand_code",
            date_partition_key="created_at",
            dedup_key="one_order_key",
            # we replace because we are doing a full sync
            replace=True,
        )

        # if we check sum of a full batch it should match what we put in from bootsrap

        # return len(orders), result
        # pd.DataFrame(result).to_csv(
        #     "s3://res-data-platform/entities/order_items/.meta/updated_batch_partition.parquet"
        # )

        logger.info(
            "Instructing druid to update the data segments for all partitions..."
        )
        rebuild_changed_order_item_partitions(fc, watermark=10000)

        logger.info("Done")


#####################


def _iterate_shopify_orders_for_shop(
    shop, auth, watermark_days=None, brand_code=None, endpoint="create"
):
    """
    For a given shop, return all dataframe chunks for pages of orders
    """

    if not pd.isnull(shop) and shop not in _EXCLUDE_SHOPIFY_USERS:
        url = f"https://{shop}.myshopify.com/admin/api/2022-04/orders.json"

        if watermark_days != None:
            url += f"?created_at_min={(datetime.utcnow() + timedelta(-1*watermark_days)).strftime('%Y-%m-%d')}&"
        else:
            url += "?"
        #
        # adding a bigger limit size
        url += f"limit={SHOPIFY_API_REQ_LIMIT}"
        url += f"&status=any"

        while url:
            logger.debug(f"Making shopify request for brand {brand_code}: {url}")
            try:
                response = safe_http.request_get(url, auth=auth)
                url = response.links.get("next", {}).get("url")
                # not sure if this is needed

                result = response.json().get("orders")
                if result:
                    df = pd.DataFrame(result)
                    if len(df) > 0:  # must be true
                        logger.info(
                            f"Fetched {len(df)} orders. The `created_at` date of the first one is {df.iloc[0]['created_at']}"
                        )
                        # these are control fields that we add on outside of the shopify payload
                        df["brand"] = brand_code
                        df["type"] = endpoint
                        yield df
            except Exception as ex:
                if brand_code != "LH":
                    raise ex
                else:
                    logger.warn("LH brand not paid skipping as it is Lawrence's")
                    break


def _parse_code(c):
    """
    the brand code needs to be cleaned from airtable

    """
    try:
        return literal_eval(str(c))[0] if not isinstance(c, list) else c[0]
    except Exception as ex:
        print(ex)  # TODO remove this when we understand the cases
        return str(c)


# set brand code to something specifically to trigger
def _get_shopify_orders(fc, brand_code=None, watermark_days=None):
    """
    For all our brands, go and get all the historical orders since watermark
    Return them in a large dataframe of orders and order items
    """

    logger.info(
        f"Requesting shopify orders for brands with watermark delta {watermark_days}"
    )

    filter = (
        "AND( {brand_code}='"
        + brand_code
        + "', {data_source_name}='Shopify', {active}='✅')"
    )
    # TODO: filter by an active field
    credentials = fc.connectors["airtable"][SHOPIFY_CREDS_BASE][
        SHOPIFY_CREDS_TABLE
    ].to_dataframe(filters=None if brand_code is None else filter)

    for record in credentials.to_dict("records"):
        for order_set in _iterate_shopify_orders_for_shop(
            record["account_id"],
            (record["api_key"], record["api_secret"]),
            brand_code=_parse_code(record["brand_code"]),
            watermark_days=watermark_days,
        ):
            if len(order_set) > 0:
                yield order_set


def _get_products(fc, keys=None):
    """
    Get our product/style lookup to join with shopify orders for creating wide tables for reports
    The source schema are a little messy and there is reasonable way to hide the messy field mappings
    """
    db = fc.connectors["mongo"]["resmagic"]

    prods = (
        pd.DataFrame(
            db["products"].find(
                {}, {"brandCode": 1, "name": 1, "ecommerceId": 1, "styleId": 1}
            )
        )
        .drop("_id", 1)
        .rename(
            columns={
                "brandCode": "brand_code",
                "ecommerceId": "product_id",
                "name": "style_name",
                "styleId": "style_id",
            }
        )
    )

    select_fields = [f"legacyAttributes.{f}" for f in SELECT_STYLE_FIELDS]
    styles = pd.DataFrame(db["styles"].find({}, {k: 1 for k in select_fields}))
    styles = (
        styles[["_id"]]
        .join([pd.DataFrame([r for r in styles["legacyAttributes"]])])
        .rename(columns=STYLE_FIELD_RENAME)
    )

    return pd.merge(prods, styles, how="left", on="style_id")


def _get_brand_attributes(fc):
    brands = fc.connectors["mongo"]["resmagic"]["brands"]
    df = pd.DataFrame(brands.find())
    brands = pd.DataFrame([d for d in df["legacyAttributes"].values])
    return brands[["Name", "Code", "Brand Cohort"]].rename(
        columns={
            "Name": "brand_name",
            "Code": "brand_code",
            "Brand Cohort": "brand_cohort",
        }
    )


def _flatten_orders(orders, keys=None):
    """
    Flatten to order item granularity with necessary joins
    The schema will evolve - we could replace these field lists with a schema registry query
    This would tidy up the code and ensure schema consistency but we would need to handle drift
    """

    order_cols = [
        "id",
        "brand_code",
        "order_number",
        "name",
        "currency",
        "tags",
        "created_at",
        "customer_total_spent",
        "customer_orders_count",
        "customer_last_order_name",
        "email",
        "total_tax",
        "total_discounts",
    ]
    item_cols = [
        "id",
        "product_id",
        "name",
        "title",
        "variant_title",
        "sku",
        "total_discount",
        "price",
        "quantity",
        "grams",
        "fulfillment_status",
    ]

    #####

    items = orders[["line_items"]].explode("line_items")
    items = pd.DataFrame(
        [d for d in items["line_items"]], index=items.index
    ).sort_index()

    for c in [
        "customer_total_spent",
        "customer_orders_count",
        "customer_last_order_name",
    ]:
        if c not in orders.columns:
            logger.warn(f"Column {c} not on dataframe - adding it")
            orders[c] = None

    try:
        # the left suffix is _order and we add id as order_item_id
        res = orders[order_cols].join(items[item_cols], lsuffix="_order")

        # add a shopify unique key to the output stream
        res["one_order_key"] = res["id"].map(lambda x: str(x) + "-SHOPIFY")

        order_item_counts = (
            res[["name_order", "id"]]
            .groupby("name_order")
            .count()
            .rename(columns={"id": "item_count"})
        )

        res = pd.merge(res, order_item_counts, on="name_order")

        logger.info(f"distributing discounts, tax and shipping over orders items")

        res["total_discount"] = res.apply(
            lambda row: row["total_discounts"] / min(1, row["item_count"]), axis=1
        )
        res["total_tax"] = res.apply(
            lambda row: row["total_tax"] / min(1, row["item_count"]), axis=1
        )

    except Exception as ex:
        logger.warn(f"Failed to resolve the order - {repr(ex)}")
        if len(orders) > 0:
            logger.warn(f"sample order record - {dict(orders.iloc[0])}")
        else:
            logger.warn("There are not orders to join")
        raise ex

    return res


def _try_coerce_schema(fc, orders):
    """
    When publishing a dataframe to a topic, we can filter the fields by what the schema allows
    This has the potential to drop fields but if we trust the schema version then this should be fine
    We should however make it easy to track down why fields would be dropped
    """

    def _first_title(x):
        try:
            return x[0]["title"]
        except:
            return ""

    schema = fc.connectors["kafka"][RAW_ORDER_TOPIC].topic_schema
    lineitem_fields = [d["name"] for d in schema["LineItem"]["fields"]]
    lineitem_type_maps = dataframes.casting_functions_for_avro_types(schema["LineItem"])

    # ShopifyOrder is the header part in the schema
    order_header_fields = [f["name"] for f in schema["ShopifyOrder"]["fields"]]
    # TODO: is this correct?dataframes.array_as_comma_seperated_list

    if "discount_codes" not in orders.columns:
        orders["discount_codes"] = None

    orders["discount_codes"] = orders["discount_codes"].map(str)
    orders["shipping_title"] = orders["shipping_lines"].map(_first_title)
    orders = dataframes.expand_column(orders, "customer")
    # using the fields from the schema and there types, filter and cast the nested dict
    orders["line_items"] = orders["line_items"].map(
        lambda x: dataframes.filter_by_dict_field_keys(
            x, lineitem_fields, lineitem_type_maps
        )
    )

    # coerce the dataframe types to match the avro schema
    orders = dataframes.coerce_to_avro_schema(
        orders, avro_schema=schema["ShopifyOrder"]
    )

    # default empty
    for c in order_header_fields:
        if c not in orders.columns:
            orders[c] = None

    return orders[order_header_fields + ["brand_code"]]


def _try_read(path):
    try:
        logger.debug(f"reading {path}")
        return pd.read_parquet(path)
    except:
        return pd.DataFrame()


def join_first_customer_orders(df):
    """
    This is a dynamic way to check a batch of orders against stored customer first order dates
    We read and write to S3 rather than create a new data store for this yet
    Any time we need to process a match we must load the customers for the brand and get/update the min dates
    When any orders is equal to the min date it is a first order
    The current set is added to the lookup "before" the check
    """

    if len(df) == 0:
        return df

    brands = list(df["brand_code"].unique())

    logger.info(
        f"Loading and refreshing customer orders for brands {brands} - this requires S3 reads so use sparingly"
    )

    data = pd.concat(
        [
            # loads email | created_at | brand_code
            # created_at is the first created_at lookup and brand code is a check_field
            _try_read(
                f"s3://res-data-platform/entities/order_items/{b}/.customer_history.parquet"
            )
            for b in brands
        ]
    )

    logger.debug(
        f"loaded {len(data)} customer lookup records for these brands. merging any incoming data..."
    )

    JOIN_KEY = ["email", "brand_code"]

    # get the first orders for each brand and customer
    current = (
        df[["brand_code", "email", "created_at"]]
        .sort_values("created_at")
        .drop_duplicates(subset=JOIN_KEY, keep="first")
    )

    # merge and resave to lookup: the data has the old orders and the current set has the latest
    data = pd.concat([data, current]).drop_duplicates(subset=JOIN_KEY, keep="first")
    for b, brand_lu in data.groupby("brand_code"):
        logger.info(
            f"Updating brand {b} lookup for customer min orders with {len(brand_lu)} records"
        )
        brand_lu.to_parquet(
            f"s3://res-data-platform/entities/order_items/{b}/.customer_history.parquet"
        )

    # merge customer orders that we have into the dataset
    # use the suffix _min for the first customer order date
    # this will be used for comparisons later (can drop brand_code_min)
    # if the data has duplicates in terms of email this would multiple orders - joining by the key
    return pd.merge(df, data, on=JOIN_KEY, how="left", suffixes=["", "_min"])


def _add_dervied_fields(df, products):
    # this should be loaded from some config system
    if len(df) == 0:
        return df

    non_regular_order_tags = [
        "Wholesale",
        "wholesale",
        "Employee",
        "EMPLOYEE",
        "gift",
        "PR",
        "Press",
        # only the exchange is verified
        "returnly_exchange",
        "returnly-refunded",
        # "returnly_repurchase",
    ]

    known_sku = products["key"].unique()

    logger.info(
        f"Loaded {len(known_sku)} known skus for this brand such as this one {known_sku[0]}"
    )

    def not_contains_exclusion_tag(x):
        if x != None:
            x = str(x)
            for t in non_regular_order_tags:
                if t in x:
                    return False
        return True

    def is_valid_one(row):
        # if pd.notnull(row["product_id"]):
        #     return True
        if not pd.isna(row["sku"]):
            # the first parts without the size are the BMC key used as the style key
            key = " ".join(row["sku"].split(" ")[:3])
            if len(row["sku"].split(" ")) == 4 and key in known_sku:
                return True
        return False

    def is_repeat_customer(row):
        try:
            # these two values need to exist. the created_at is the order date and the first customer order date is derived
            return row["created_at"] > row["created_at_min"]
        except:
            return False

    def lookup_order_rank(row):
        # lookup key value for brand_customer: {orders:order_item_counts}
        # if there is more than one order key, get all gets in a sorted list and get the index of our value
        # this rank tells us how many orders there for this shop up until our order
        return None

    # we load lookups in the etl here - we need to be careful about patterns for this but this works for now
    df = join_first_customer_orders(df)

    df["is_regular_order"] = df["tags"].map(not_contains_exclusion_tag)
    # if there is no price then its not a regular order
    df.loc[df["price"].fillna(0) == 0, "is_regular_order"] = False
    # if its returned cancelled or exchanged it is not

    # determine how many orders this custom has made with this brand
    # for now using a fuzzy
    df["is_repeat_order"] = df.apply(is_repeat_customer, axis=1)
    df["order_rank"] = df.apply(lookup_order_rank, axis=1)
    # try to determine if the one is valid based on the product id (required for new) or something that 'looks like' a proper sku
    df["is_valid_one"] = df.apply(is_valid_one, axis=1)
    # df["one_order_key"] = df.apply(make_key, axis=1)
    return df


def _infer_product_match_fill_missing(orders, products):
    def part(s, i):
        if pd.notnull(s):
            parts = s.split(" ")
            if len(parts) == 4:
                return parts[i]
        return None

    orders["body_code"] = orders["body_code"].fillna(
        orders["sku"].map(lambda x: part(x, 0))
    )
    orders["material_code"] = orders["material_code"].fillna(
        orders["sku"].map(lambda x: part(x, 1))
    )
    orders["color_code"] = orders["color_code"].fillna(
        orders["sku"].map(lambda x: part(x, 2))
    )
    orders["brand_code"] = orders["brand_code"].fillna(
        orders["sku"].map(lambda x: part(x, 0)[:2] if part(x, 0) != None else None)
    )

    return orders


def _merge_orders_with_style_attributes(fc, orders):
    """
    perform the merge to append our product attributes to orders
    the FlowContext fc is passed in to give access to connectors
    """
    orders = _flatten_orders(orders)

    products = _get_products(fc, keys=None)

    brand_metadata = _get_brand_attributes(fc)

    def int_as_string(x):
        try:
            return str(int(float(x)))
        except:
            return x

    # join as string. they are actually integers but we cannot cast if there are missing values
    products["product_id"] = products["product_id"].map(int_as_string)
    orders["product_id"] = orders["product_id"].map(int_as_string)

    orders = pd.merge(
        orders, products, on="product_id", how="left", suffixes=["", "_products"]
    )
    orders = _infer_product_match_fill_missing(orders, products)
    orders = _add_dervied_fields(orders, products)

    assert "brand_code" in list(orders.columns)

    # look up brand metadata
    orders = pd.merge(orders, brand_metadata, on="brand_code", how="left")

    return orders


def try_write_to_dgraph(df):
    from res.connectors.dgraph import DgraphConnectorTable

    dg = DgraphConnectorTable(DGRAPH_ORDER_TABLE_NAME)
    # TODO make sure dgraph updates are tenacious
    res = dg.update_dataframe(df)
    logger.info(f"Wrote {len(df)} records to dgraph.")

    return res


######      ###############################################
#     FLOWS                                               #
######      ###############################################


def list_brands(event, context):
    """
    This is invoked from the argo template to generate the list of brands
    """
    with FlowContext(event, context) as fc:
        filters = "AND({data_source_name}='Shopify', {active}='✅')"
        credentials = fc.connectors["airtable"][SHOPIFY_CREDS_BASE][
            SHOPIFY_CREDS_TABLE
        ].to_dataframe(filters="{data_source_name}='Shopify'")

        brands = [
            {"args": {"brand_code": _parse_code(record["brand_code"])}}
            for record in credentials.to_dict("records")
        ]

        # TEMP to process RB on her own
        # return [d for d in brands if d["args"]["brand_code"] == "RB"]

        return brands


def migrate_create_one_orders_to_s3(event, context):
    load_history = False

    with FlowContext(event, context) as fc:
        # we wont do a watermark, instead just do history or just reload the current table
        s3 = fc.connectors["s3"]
        schema = DgraphConnector.try_load_meta_schema("resSell.one_orders")
        att = _get_brand_attributes(fc)

        # MAKE a filter predicate for create one orders only (use in historic differently)
        # test with just current first and then get all historic
        predicate = AirtableConnector.make_key_lookup_predicate(
            ["resmagic.io"], "Order Channel"
        )
        orders = AirtableConnector.get_airtable_table_for_schema(
            schema, load_history=False, filters=predicate
        )

        logger.info(
            f"Loaded {len(orders)} for the resmagic.io order channel. Brand data, resSell.one_rders schema loaded."
        )

        # generate a create one key
        orders["one_order_key"] = orders.apply(
            lambda row: f"{row['order_number']}-{str(row['sku']).lower().replace(' ','-')}-CREATE.ONE",
            axis=1,
        )
        orders["id_order"] = 0
        orders["id"] = 0
        orders["price"] = 0
        orders["is_regular_order"] = False
        orders["is_repeat_order"] = False
        orders["is_valid_one"] = True
        orders["body_code"] = orders["sku"].map(
            lambda x: None if x is None else str(x).split(" ")[0]
        )
        orders["brand_name"] = orders["brand_code"].map(
            lambda x: dict(att[["brand_code", "brand_name"]].values).get(x)
        )
        orders["brand_cohort"] = orders["brand_code"].map(
            lambda x: dict(att[["brand_code", "brand_cohort"]].values).get(x)
        )

        # todo - in future we should coerce to the res.schema
        # orders = dataframes.coerce_to_avro_schema(
        #     orders,
        #     avro_schema=schema["resSell.one_orders"],
        # )

        logger.info(f"saving partitions for orders of size {len(orders)}")

        # merge these with the rest
        result = s3.write_date_partition(
            orders,
            entity="order_items",
            group_partition_key=None,
            date_partition_key="created_at",
            dedup_key="one_order_key",
            # we replace because we are doing a full sync
            replace=True,
        )

        logger.info(f"Updated {len(result)} partitions")

    # todo - update order channel on batch and rebuild but that is a low priority
    # many thing is to make sure we dont corrupt the partitions


# def get_non_shopify_orders(event, context):
#     args = event.get("args", {})
#     brand_code = args.get("brand_code", None)

#     if brand_code is None:
#         raise Exception(
#             "Expected a brand code to be specified for op get_non_shopify_orders. but alas, no."
#         )

#     # airtable order fullfillments - using our schema to query what we want
#     with FlowContext(event, context) as fc:
#         at = fc.connectors["airtable"]
#         # legacy loader needs to be merged in here maybe
#         # mongo may need to be used because we will not have the history in airtable
#         # todo create a filter
#         orders = None
#         orders["one_order_key"] = orders["key"].map(lambda x: str(x) + "-CREATE.ONE")
#         fc.connectors["kafka"][DISTINCT_PROCESS_ORDER_TOPIC].publish(
#             orders,
#             coerce=True,
#             use_kgateway=False,
#             dedup_on_key="one_order_key",
#         )


def migrate_to_s3_by_month(watermark):
    """
    This is a once off and does the same thing that the nightly save to partition does

    """
    with FlowContext({}, {}) as fc:
        s3 = fc.connectors["s3"]

        so = ShopifyOrdersDynamoTable()
        idx = so.load_index_cache(watermark)
        keys = idx["key"].values
        orders = so.read_by_keys(keys)

        s3.write_date_partition(
            orders,
            entity="order_items",
            group_partition_key="brand_code",
            date_partition_key="created_at",
        )


def write_dynamo_chunks(event, context):
    relay_to_kafka = event.get("relay_to_kafka", False)
    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]
        kc = fc.connectors["kafka"]
        processed_topic = "resSellOneOrders"
        topic_info = kc[processed_topic]
        schema = topic_info.topic_schema

        for orders_chunk in ShopifyOrdersDynamoTable().read_chunks():
            logger.info(f"loaded a chunk of length {len(orders_chunk)}")
            orders = pd.DataFrame([json.loads(d) for d in orders_chunk["json"].values])
            # try_write_to_dgraph(orders_chunk)
            # writing to a new topic with histroy

            if len(orders) > 0:
                orders = _try_coerce_schema(fc, orders)
                orders = _merge_orders_with_style_attributes(fc, orders)
                print(orders.groupby("is_repeat_order").count()[["id"]].T)

                # coerce to what the avro schema would be - we also save this to parquet (lesson)
                orders = dataframes.coerce_to_avro_schema(
                    orders,
                    avro_schema=schema[processed_topic],
                )

                logger.info(f"generated order items of length {len(orders)}")

                check_sums = s3.write_date_partition(
                    orders,
                    entity="order_items",
                    group_partition_key="brand_code",
                    date_partition_key="created_at",
                )

                # check that the number of partitions written on these dates ??

                logger.info(f"wrote order items of length {len(orders)} to s3")

                if relay_to_kafka:
                    fc.connectors["kafka"][processed_topic].publish(
                        orders,
                        coerce=True,
                        use_kgateway=True,
                        dedup_on_key="one_order_key",
                    )


def _relay_orders(fc, orders, brand_code, is_batch=True, topic=PROCESSED_ORDER_TOPIC):
    """
    Used to process incoming orders and send to one_orders
    If we are streaming real-time changes (normal node) we update order statuses
    We save in the store (dgraph) and also send events e.g. orderStatusChanged
    """
    sd = ShopifyOrdersDynamoTable()

    orders["brand_code"] = brand_code

    orders_chunk = _try_coerce_schema(fc, orders)
    logger.incr(f"{METRIC_KEY}_TEMP_COERCED_ORDERS", len(orders_chunk))
    orders_chunk = _merge_orders_with_style_attributes(fc, orders_chunk)
    logger.incr(
        f"{METRIC_KEY}_TEMP_STYLED_MERGED_FLAT_ORDER_ITEMS",
        len(orders_chunk),
    )
    orders_chunk = orders_chunk.drop_duplicates(subset=["one_order_key"])

    # use  requested brand code rather than trust what was read form the data
    # shopify is giving us data per brand

    logger.incr(f"{METRIC_KEY}_ingest_order_items", len(orders_chunk))
    logger.incr(
        f"{METRIC_KEY}_ingest_order_items_brand_{brand_code}",
        len(orders_chunk),
    )

    if not is_batch:
        # relay these events to a topic or take some action
        cancellations = orders[orders["type"] == "cancel"]
        # not sure if anything non null is something that should be handled or subset
        fulfillments = orders[orders["fulfillment_status"].notnull()]

        ##
        if len(cancellations) > 0:
            logger.incr(f"{METRIC_KEY}_ingest_order_cancelled", len(cancellations))
            logger.incr(
                f"{METRIC_KEY}_ingest_order_cancelled_brand_{brand_code}",
                len(cancellations),
            )

            for record in cancellations:
                sd.update_order_status(
                    record["brand"], record["name"], "cancelled", ignore_missing=True
                )
            # update what? this should be a merge of what is there and not update schema of incoming
            # we may need to wait until the full order is on the topic or we will corrupt what we have
            # for now maybe a partial mutation might be better or slow but we can read what is there and update the dict
            # try_write_to_dgraph(cancellations)
            # fc.connectors["kafka"][ORDER_STATUS_CHANGED].publish(cancellations)
            # DO WE NEED TO DO WITH PARTIAL CANCELLATIONS HERE - STATE WILL ALWAYS BE UPDATED SEP.

            # use the druid connector to update the LOOKUP on order status

        if len(fulfillments) > 0:
            logger.incr(f"{METRIC_KEY}_ingest_order_fulfilled", len(fulfillments))
            logger.incr(
                f"{METRIC_KEY}_ingest_order_fulfilled_brand_{brand_code}",
                len(fulfillments),
            )

            for record in cancellations:
                sd.update_order_status(
                    record["brand"], record["name"], "fulfilled", ignore_missing=True
                )

            # update what? this should be a merge of what is there and not update schema of incoming
            # try_write_to_dgraph(fulfillments)
            # fc.connectors["kafka"][ORDER_STATUS_CHANGED].publish(fulfillments)
            # TODO: IS PARTIAL FULFILLMENT A THING?

            # use the druid connector to update the LOOKUP on order status

    # TODO - return what we published and count and send those to grafana
    try:
        fc.connectors["kafka"][topic].publish(
            orders_chunk,
            coerce=True,
            use_kgateway=True,
            dedup_on_key="one_order_key",
        )
    except Exception as ex:
        logger.warn("Failing in kafka relay")

    return orders_chunk


def as_bool(s):
    if pd.isnull(s):
        return False
    if isinstance(s, int):
        return bool(s)
    if isinstance(s, bool):
        return s
    if isinstance(s, str):
        return s.lower() == "true"
    return False


def ingest_historic_shopify_orders(event, context):
    """
    pull all shopify orders since date and push them onto the dynamo db table
    """
    #######
    DEFAULT_LOOKBACK = 7
    #######
    args = event.get("args", {})
    watermark = args.get("order_ingest_watermark", DEFAULT_LOOKBACK)
    logger.info(f"Order fetch historic watermark set to -{watermark}")
    brand_code = args.get("brand_code", None)
    sd = ShopifyOrdersDynamoTable()
    with FlowContext(event, context, data_group=brand_code) as fc:
        s3 = fc.connectors["s3"]

        for orders in _get_shopify_orders(
            fc, watermark_days=watermark, brand_code=brand_code
        ):
            logger.info(f"writing {len(orders)} to dynamo.")
            sd.write(orders)

            # in future we will do this but we need to rebuild dgraph and add fulfillments as child
            # try_write_to_dgraph(orders)
            logger.incr(f"{METRIC_KEY}_ingest_query_all", len(orders))
            if len(orders) > 0:
                # brand code is added in this function as sometimes the data may not have even though we request from that shop??
                orders = _relay_orders(fc, orders, brand_code)

                orders["id_order"] = orders["id_order"].map(as_bool)
                orders["id"] = orders["id"].map(as_bool)
                orders["is_regular_order"] = orders["is_regular_order"].map(as_bool)
                orders["is_repeat_order"] = orders["is_repeat_order"].map(as_bool)
                orders["is_valid_one"] = orders["is_valid_one"].map(as_bool)

                # update the S3 partitions which is what we will use for druid ingestion
                s3.write_date_partition(
                    orders,
                    entity="order_items",
                    group_partition_key="brand_code",
                    dedup_key="one_order_key",
                    date_partition_key="created_at",
                    replace=True,
                )


def transform_order_items(event, context):
    """
    pull orders from the shopify ingestion topic and process them for pushing to the processed topic
    because we are getting updates we should push to dynamo even if exists this time and add status
    """
    sd = ShopifyOrdersDynamoTable()
    with FlowContext(event, context) as fc:
        orders = fc.connectors["kafka"][RAW_ORDER_TOPIC].consume()
        logger.info(
            f"Consumed {len(orders)} from {RAW_ORDER_TOPIC} and merging attributes"
        )
        if len(orders) > 0:
            for brand_code, brand_orders in orders.groupby("brand"):
                # for now the schema may not be the same but we can update it
                # when the schema is updated we can do the full but for now just update the status of existing orders
                # sd.write(brand_orders, upsert=True)
                if len(brand_orders) > 0:
                    _relay_orders(
                        fc, brand_orders, brand_code=brand_code, is_batch=False
                    )
        else:
            logger.info("Did not find any new orders on the stream")


def one_order_populate(event, context):
    """
    This is a repair flow that went from dynamo and will be updated for DGRAPH
    """
    with FlowContext(event, context) as fc:
        for orders_chunk in ShopifyOrdersDynamoTable().read_chunks():
            logger.info(f"loaded a chunk of length {len(orders_chunk)}")
            orders_chunk = pd.DataFrame(
                [json.loads(d) for d in orders_chunk["json"].values]
            )
            orders_chunk = _try_coerce_schema(fc, orders_chunk)
            orders_chunk = _merge_orders_with_style_attributes(fc, orders_chunk)
            orders_chunk = orders_chunk.drop_duplicates(subset=["one_order_key"])
            fc.connectors["kafka"][DISTINCT_PROCESS_ORDER_TOPIC].publish(
                orders_chunk,
                coerce=True,
                use_kgateway=False,
                dedup_on_key="one_order_key",
            )


def upsert_historic_shopify_orders(event, context):
    """
    pull all shopify orders since date and push them onto the kafka topic

    DEPRECATE: this is the same as ingest from historic now but we need to decide
    if we want to push onto shopifyTopic. It seems we cannot because we do not have
    a uniqueness check
    """
    args = event.get("args", {})
    watermark = args.get("order_ingest_watermark", 7)
    logger.info(f"Order fetch historic watermark set to -{watermark}")
    brand_code = args.get("brand_code")
    with FlowContext(event, context) as fc:
        for orders in _get_shopify_orders(
            fc, watermark_days=watermark, brand_code=brand_code
        ):
            # raw orders to dynamo db
            logger.info("Trying to coerce schema based on latest schema version...")
            orders = _try_coerce_schema(fc, orders)
            fc.connectors["kafka"][RAW_ORDER_TOPIC].publish(orders)


def process_single(key):
    """
    this is just a convenience test method as we always process batches
    """
    brand_code = key.split("/")[0]
    logger.info(
        f"Loading a single record for key {key} and processing it - this still requires batch loading of brand data for brand {brand_code}"
    )
    with FlowContext({}, {}) as fc:
        sd = ShopifyOrdersDynamoTable()
        json_data = json.loads(sd._get_record(key)["json"])
        orders = pd.DataFrame([json_data])
        orders["brand_code"] = brand_code
        orders = _try_coerce_schema(fc, orders)
        orders = _merge_orders_with_style_attributes(fc, orders)
        return orders


"""

Implement streams (normed - add what is in the source and requires no heavy lookups - we can lookup from cache if required)
[resMake.orders]
key
name
created_at
fulfillment_status
make_status
brand_code
quantity
price
total_discount
total_tax
order_channel

"""


@relay("shopifyOrder", {"resMake.orders": "resMake.orders"})
def shopify_to_make_orders(
    df, event=None, context=None, use_kgateway=True, coerce=True
):
    """
    data is passed as a dataframe - even for single records that are mapped to a dataframe
    This is inefficient. however the inefficiency is "written off" as the batch gets bigger

    ...
    some logic to transform a shopify order to a make.order
    in future we could do lookups but this is assumed stateless and simple
    If lookups are required we need to have a streaming context that can load lookups to a cache and save them in the context
    This function would only interact through the FlowContext - by passing in an event, context, this construct the flow context
    - If the event os populated it is possible to set assets for testing and override the stream construction
    - if a cache is added to the context, this can be used to lookup data
    """

    cols = [
        "sku",
        "id",
        "name",
        "variant_title",
        "price",
        "quantity",
        "brand",
        "id_order",
        "name_order",
        "created_at",
        "updated_at",
        "fulfillment_status",
        "one_order_key",
        "flow_event_type",
    ]

    df = dataframes.flatten(df, "line_items", lsuffix="_order")
    df["one_order_key"] = df["id"].map(lambda x: str(x) + "-SHOPIFY")
    df = df.rename(columns={"type": "flow_event_type"})
    for c in ["created_at", "updated_at"]:
        df[c] = pd.to_datetime(df[c], utc=True).map(str)
    return df[cols].reset_index().drop("index", 1)


def create_one_to_make_orders(df):
    """
    WIP logic for parsing create-one orders when we want to merge them into our MAKE orders topic
    """
    cols = [
        "sku",
        "id",
        "name",
        "variant_title",
        "price",
        "quantity",
        "brand",
        "id_order",
        "name_order",
        "created_at",
        "updated_at",
        "fulfillment_status",
        "one_order_key",
        "flow_event_type",
    ]

    df["lineItemsInfo"] = df["lineItemsInfo"].map(
        lambda x: x if isinstance(x, list) else literal_eval(x)
    )
    df = dataframes.flatten(df, "lineItemsInfo", lsuffix="_order")

    df["brandCode"] = df["brandCode"].map(remove_pseudo_lists)
    df["orderStatus"] = df["orderStatus"].map(strip_emoji)
    df["one_order_key"] = df["id"].map(lambda x: str(x) + "-CREATE-ONE")

    df = dataframes.rename_and_whitelist(
        df,
        columns={
            "one_order_key": "one_order_key",
            "id": "id",
            "id_order": "id_order",
            "sku": "sku",
            "friendlyName": "name",
            "quantity": "quantity",
            "createdAt": "created_at",
            "type": "flow_event_type",
            "number": "name_order",
            "brandCode": "brand",
            "orderStatus": "fulfillment_status",
        },
    )

    # ?
    df["flow_event_type"] = "created"
    # for now
    df["updated_at"] = df["created_at"]
    df["price"] = 0
    df["variant_title"] = ""
    # it seems we lack an id sometimes
    df["id"] = df["id"].fillna("NA")
    df["price"] = df["price"].astype(float)
    df["name_order"] = df["name_order"].astype(str)

    for c in ["created_at", "updated_at"]:
        df[c] = pd.to_datetime(df[c], utc=True).map(str)

    return df[cols]


"""
Implement the flow node interface for map reduce
"""


def generator(event, context):
    return list_brands(event, context)


def handler(event, context):
    return ingest_historic_shopify_orders(event, context)


def load_kafka_make_to_res_meta(event, context):
    def style_resolver_factory():
        def f(sku):
            if len(str(sku).split(" ")) == 4:
                return sku
            return None

        return f

    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]
        dgraph = fc.connectors["dgraph"]

        path = f"s3://res-kafka-out-production/topics/resMake.orders"

        logger.info(f"processing {path}")
        for f in s3.ls(path):
            try:
                data = s3.read(f)

                def style_resolver_factory():
                    def f(sku):
                        if len(str(sku).split(" ")) == 4:
                            return sku
                        return None

                    return f

                for col in ["created_at", "updated_at"]:
                    data[col] = pd.to_datetime(data[col])

                data["key"] = data.apply(
                    lambda row: f"{row['brand']}-{row['name_order'].replace('#','')}",
                    axis=1,
                )
                data["res_sku"] = data["sku"].map(style_resolver_factory())

                chk = data[data["res_sku"].notnull()].reset_index().drop("index", 1)

                chk["style_code"] = chk["res_sku"].map(
                    lambda x: " ".join(x.split(" ")[:3])
                )
                chk["style_code"] = chk["res_sku"].map(
                    lambda x: " ".join(x.split(" ")[:3])
                )
                chk["size_code"] = chk["res_sku"].map(
                    lambda x: " ".join(x.split(" ")[3:])
                )

                dgraph["make.orders"].update(chk)

            except Exception as ex:
                logger.debug(f"Failures in {f} - {repr(ex)}")

        logger.info("done")
