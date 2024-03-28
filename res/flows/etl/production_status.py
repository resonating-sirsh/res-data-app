from botocore.utils import instance_cache
import pandas as pd
from ast import literal_eval

from sentry_sdk.api import start_span
from res import utils
from res.connectors.dynamo import ProductionStatusDynamoTable
import res
from pydruid.db import connect
import re
from res.utils import safe_http
import os
from res.flows import FlowContext
from urllib.parse import urlparse
from datetime import datetime


def is_super_status_type(s):
    if "FULFILLMENT_STATUS" in s.upper() or "ASSEMBLY_NODE_STATUS" in s.upper():
        return True
    return False


# this NAME list is important as we need to find this to appear in queue ordinals
# the name must match exactly as this is a registration of a time point
# in future when creating dynamic flows we should add these to events so that all TPs are valid
# flows will be dynamic based on rules about how assets flow - the flow routers will group by discriminators and send to different work queues

# think about asset type and overlapping time points

ORDINAL = [
    "CURRENT_ASSEMBLY_NODE - MATERIAL",
    "CURRENT_ASSEMBLY_NODE - PRINT",
    "PRINT_QUEUE - SKIP",
    "PRINT_QUEUE - PREPAREMATERIALS",
    "PRINT_QUEUE - TODO",
    "PRINT_QUEUE - PROCESSING",
    ## ROLLS
    "ROLL_STATUS - PRETREATMENT",
    "SCOUR_WASH_QUEUE - TODO",
    "SCOUR_WASH_QUEUE - PROCESSING",
    "SCOUR_WASH_QUEUE - DONE",
    "SCOUR_DRY_QUEUE - TODO",
    "SCOUR_DRY_QUEUE - PROCESSING",
    "SCOUR_DRY_QUEUE - DONE",
    "PRETREATMENT_QUEUE - TODO",
    "PRETREATMENT_QUEUE - PROCESSING",
    "PRETREATMENT_QUEUE - DONE",
    # "ROLL_STATUS - UNASSIGNED",
    "STRAIGHTEN_QUEUE - TODO",
    "STRAIGHTEN_QUEUE - PROCESSING",
    "STRAIGHTEN_QUEUE - DONE",
    #### ^ ROLLLS
    "FULFILLMENT_STATUS - PENDINGINVENTORY",
    "ASSEMBLY_NODE_STATUS - TODO",
    "NESTED_ASSET_SETS",
    "PRINT_FLOW - LOCKED",
    "PRINT_QUEUE - RIPPED",
    "PRINT_QUEUE - PRINTED",
    "ROLL_STATUS - DONE",
    "PRINT_QUEUE - CANCELED",
    "STEAM_QUEUE - TODO",
    "STEAM_QUEUE - PROCESSING",
    "STEAM_QUEUE - DONE",
    "WASH_QUEUE - TODO",
    "WASH_QUEUE - PROCESSING",
    "WASH_QUEUE - DONE",
    "DRY_QUEUE - TODO",
    "DRY_QUEUE - PROCESSING",
    "DRY_QUEUE - DONE",
    # for some materials soften does not occur
    "CURRENT_ASSEMBLY_NODE - CUT",
    "SOFTEN_QUEUE - TODO",
    "SOFTEN_QUEUE - PROCESSING",
    "SOFTEN_QUEUE - DONE",
    "CUT_EXIT - TODO",
    "CUT_EXIT - NOTREADY",
    "CUT_EXIT - EXITCUT",
    # more granular here TODO - IVAN
    "TRIM_SHEET - TRUE",
    "PREP_EXIT - EXITSTATION",
    "HASENTEREDSEW - TRUE",
    "SEWQUEUE - TODO",
    "SEWQUEUE - SEWING",
    "SEWQUEUE - SPEC-ISSUE",
    "SEWQUEUE - SPEC",
    "SEWQUEUE - DONE",
    "DR_WAREHOUSE - TOCHECKIN",
    # "FULFILLMENT_STATUS - PROCESSING",
    "DR_WAREHOUSE - CHECKINCOMPLETE",
    "UNIT_PICKED - TODO",
    "UNIT_PICKED - TRUE",
    "CHECKOUT - VALUE",
    "SHIPPING_LABEL - TOPRINT",
    "SHIPPING_LABEL - PRINTED",
    "PACK_QUEUE - PREPAREDOCUMENTS",
    "PACK_QUEUE - PRINTED",
    "PACK_QUEUE - PACKED",
    "FULFILLMENT_STATUS - FULFILLED",
    "FULFILLMENT_STATUS - CANCELED",
    "FULFILLMENT_STATUS - REQUESTKILLEDINMAKE",
]

ORDER_ITEM_COLUMNS = [
    "Active ONE Number",
    "KEY",
    "ORDER_NAME",
    "order_date",
    "FULFILLMENT_STATUS",
]

STATE_COLS = [
    "one_number",
    "fulfillment_status",
    "order_key",
    "order_name",
    "brand",
    "body",
    "material",
    "color",
    "size",
]

DRUID_UPDATE_SPEC = {
    "type": "index_parallel",
    "spec": {
        "ioConfig": {
            "type": "index_parallel",
            "inputSource": {
                "type": "s3",
                "prefixes": ["s3://res-data-platform/entities/order_item_statuses/"],
            },
            "inputFormat": {"type": "parquet"},
        },
        "tuningConfig": {
            "type": "index_parallel",
            "partitionsSpec": {"type": "dynamic"},
        },
        "dataSchema": {
            "dataSource": "order_item_statuses_test",
            "granularitySpec": {
                "type": "uniform",
                "queryGranularity": "SECOND",
                "rollup": False,
                "segmentGranularity": "DAY",
            },
            "timestampSpec": {"column": "timestamp", "format": "iso"},
            "dimensionsSpec": {
                "dimensions": [
                    "body",
                    "brand",
                    "color",
                    "event_key",
                    "fulfillment_status",
                    "material",
                    "order_key",
                    "size",
                    {"type": "string", "name": "one_number"},
                ]
            },
            "metricsSpec": [{"name": "count", "type": "count"}],
        },
    },
}

DRUID_UPDATE_SPEC_LATEST = {
    "type": "index_parallel",
    "spec": {
        "ioConfig": {
            "type": "index_parallel",
            "inputSource": {
                "type": "s3",
                "uris": [
                    "s3://res-data-platform/entities/order_item_statuses/.queue_snapshot"
                ],
            },
            "inputFormat": {"type": "parquet", "binaryAsString": True},
        },
        "tuningConfig": {
            "type": "index_parallel",
            "partitionsSpec": {"type": "dynamic"},
        },
        "dataSchema": {
            "dataSource": "queue_snapshot",
            "granularitySpec": {
                "type": "uniform",
                "queryGranularity": "HOUR",
                "rollup": True,
                "segmentGranularity": "HOUR",
            },
            "timestampSpec": {"column": "timestamp", "format": "iso"},
            "dimensionsSpec": {
                "dimensions": [
                    "body",
                    "brand",
                    "color",
                    "event_key",
                    "key",
                    "material",
                    "ordered_event_key",
                    "size",
                    "type",
                    {"type": "string", "name": "one_number"},
                ]
            },
            "metricsSpec": [
                {"name": "count", "type": "count"},
                {
                    "name": "sum_approx_hours",
                    "type": "longSum",
                    "fieldName": "approx_hours",
                },
            ],
        },
    },
}

precedence = [
    "Material",
    "Print",
    "Cut",
    "Fusing Paper Marker",
    "Trims",
    "Paper Marker",
    "Prep for Sew",
    "Assembly Exit",
]


def check_null(s):
    if pd.isnull(s):
        return None
    if str(s).lower() in ["nan", "none", "null"]:
        return None
    return s


def props_from_sku(df):
    def part(i):
        def _r(s):
            try:
                return s.split(" ")[i]
            except Exception as ex:

                return None

        return _r

    try:
        for c in ["brand", "body", "material", "color", "size"]:
            df[c] = df[c].map(check_null)

        if "sku" in df.columns:
            df["body"] = df["body"].fillna(df["sku"].map(part(0)))
            df["material"] = df["material"].fillna(df["sku"].map(part(1)))
            df["color"] = df["color"].fillna(df["sku"].map(part(2)))
            df["size"] = df["size"].fillna(df["sku"].map(part(3)))

        # YIKES
        df["brand"] = df["brand"].fillna("MF")

    except Exception as ex:

        pass
    return df


def resolve_precedence(s):
    try:
        if pd.isnull(s):
            return s
        s = str(s)

        if isinstance(s, list):
            l = [i.upper() for i in s]
        else:
            try:
                l = literal_eval(s)
            except:
                l = [i.upper() for i in s.split(",")]
        for i in precedence:
            if i.upper() in str(l):
                return i.upper()
        return s
    except Exception as ex:
        raise ex


def make_type(s):
    if pd.isnull(s):
        return None
    s = str(s)
    if s[0] == "R" and ":" in s:
        return "ROLL"
    if "-PROD" in s or "-SMPL" in s:
        return "ONE"
    if len(s.split(" ")) == 4:
        return "SKU"
    # this is a final effort for 123123231_A_V
    if "_" in s:
        return "ONE"


def try_from_key(row):
    if row["type"] == "ONE":
        key = row["key"]
        # sometimes we have this format 123123213-PROD or sometimes we have some roll like ones 12323234_MATERIAL
        return key.split("-")[0] if "-" in key else key.split("_")[0]


def make_event(row):
    state = row["cell_value"].upper().replace("NONE", "TODO")

    # trim the value from these
    if row["column_name"] == "NESTED_ASSET_SETS":
        return "NESTED_ASSET_SETS"

    # can we do this - sometimes it might be set back to something. if we can ignore anything out of order this is safe and we can drop the later (KEEP FIRST OCCURENCE)
    result = f"{row['column_name']} - {state}"
    return result


def smart_list_eval_list(s):
    """
    We assume a list type. if its a string that cannot be evaluated as a list we treat it as one
    if its null we return null (not empty list)
    If its a list we just return it.
    """
    if isinstance(s, list):
        return s
    if pd.isnull(s):
        return None
    try:
        return literal_eval(s)
    except:
        return [s]


def get_ordinal(s):
    s = str(s).upper()
    key = s.replace("-ISSUE", "")
    if key in ORDINAL:

        return (
            f"{ORDINAL.index(key)}"
            if "-ISSUE" not in s
            else f"{ORDINAL.index(key)}-issue"
        )
    return None


def get_events(druid_host="localhost", port=8005):

    res.utils.logger.info("Loading production status")

    ps = ProductionStatusDynamoTable()
    # TODO choose a filter for this in future e.g. orders that are not fullfilled or are recently fulfilled - mayb make an archived field in future
    df = ps.read()

    # MISFIT thing where we have no style lookup for the brand?????
    res.utils.logger.info(f"Updating production asset attributes from SKU")
    df = props_from_sku(df)

    # make sure the rolls are lists - they should be lists or null of roll ids
    df["roll_id"] = df["roll_id"].map(smart_list_eval_list)

    # this is the many many graph of rolls and ones

    roll_lu = {}
    # explode the list and make sure everything is a string
    for roll, ones in (
        df[["roll_id", "one_number"]].explode("roll_id").astype(str).groupby("roll_id")
    ):
        roll_lu[roll] = list(ones["one_number"])

    order_item_lu = dict(df[["order_key", "one_number"]].astype(str).dropna().values)

    def try_from_roll(row):
        if row["type"] == "ROLL":
            return roll_lu.get(row["key"])

    def try_from_order_item(row):
        if row["type"] == "SKU":
            return order_item_lu.get(row["key"])

    res.utils.logger.info("Loading node_states since (some recent date)...")
    conn = connect(
        host=druid_host,
        port=port,
        path="/druid/v2/sql/",
        scheme="http",
        context={"timeout": 1000},
    )
    curs = conn.cursor()
    curs.execute(
        """SELECT "__time", "cell_value", "column_name", "count", "key", "node", "old_cell_value", "old_timestamp", "state_type", "sum_time_delta_minutes", "table_id", "timestamp" FROM "node_states" """
    )

    res.utils.logger.info("processing event data...")

    # TODO some where clause because we should not be dealing with really state stuff
    node_states = pd.DataFrame(curs)
    node_states["type"] = node_states["key"].map(make_type)
    N = node_states[
        ["key", "type", "column_name", "cell_value", "state_type", "timestamp"]
    ]
    # N = N[N['type'].notnull() ]
    N["cell_value"] = N["cell_value"].map(lambda x: str(x).upper())
    N = N[N["state_type"].isin(["Kanban - Leaf", "Kanban - Leaf - Assembly"])][
        ["column_name", "cell_value", "type", "state_type", "key", "timestamp"]
    ]  # .drop_duplicates()
    N = N[
        ~N["column_name"].isin(
            [
                "Priority v3",
                "_roll_locked_v3",
                "Material Pretreatment Priority",
                "Printer",
                "Pattern Work Status",
                "Feedback Status",
                "Parent Body ONE Ready Status",
                "Body ONE Ready Status",
                "Sample In Sew Status",
                "Digitize Status",
                "Construct Sample Status",
                "Grade Status",
                "Designer Review Status",
                "Status (from Scanned Pattern)",
            ]
        )
    ]

    N.loc[
        (N["column_name"] == "CHECKOUT") & (N["cell_value"].notnull()), "cell_value"
    ] = "VALUE"

    N.loc[
        N["column_name"].map(lambda x: x.upper()) == "CURRENT ASSEMBLY NODE",
        "cell_value",
    ] = N["cell_value"].map(resolve_precedence)

    for c in ["cell_value", "column_name"]:
        N[c] = N[c].map(lambda x: str(x).upper())

    N["cell_value"] = N["cell_value"].map(lambda x: re.sub(r"[^A-Za-z-]+", "", x))
    N["column_name"] = N["column_name"].map(
        lambda x: re.sub(r"[^A-Za-z-_ ]+", "", x.upper().replace(" ", "_"))
    )

    N["one_number"] = N.apply(try_from_key, axis=1)
    # in the case of rolls this is a 1-* relationship - we explode below
    N["one_number"] = N["one_number"].fillna(N.apply(try_from_roll, axis=1))
    N["one_number"] = N["one_number"].fillna(N.apply(try_from_order_item, axis=1))

    N["event_key"] = N.apply(make_event, axis=1)
    N["ordinal_hint"] = N["event_key"].map(get_ordinal)

    N = N.explode("one_number")
    N = N.sort_values(["one_number", "timestamp"])
    # treat empty string is null
    N.loc[N["one_number"] == "", "one_number"] = None

    events = pd.merge(
        N,
        df[
            [
                "one_number",
                "order_key",
                "body",
                "brand",
                "material",
                "color",
                "size",
                "key",
            ]
        ].drop_duplicates(),
        left_on="one_number",
        right_on="one_number",
        how="left",
        suffixes=["", "_item"],
    )

    return events[
        [
            "timestamp",
            "event_key",
            "one_number",
            "body",
            "brand",
            "material",
            "color",
            "size",
            "key",
            "ordinal_hint",
        ]
    ].sort_values(["one_number", "timestamp"])


def events_to_latest(ev):
    now = datetime.utcnow()
    E = (
        ev[ev["one_number"].notnull()]
        .sort_values(["one_number", "timestamp"])
        .drop_duplicates(subset=["one_number", "event_key"], keep="first")
    )
    E = E[E["ordinal_hint"].notnull()]
    E["ordinal_hint"] = E["ordinal_hint"].map(lambda x: f"{x:>03}")
    E["type"] = "A"
    E.loc[E["event_key"].map(is_super_status_type), "type"] = "B"
    E["ordered_event_key"] = E.apply(
        lambda row: f"{row['ordinal_hint']} {row['event_key']}", axis=1
    )
    E["approx_hours"] = pd.to_datetime(E["timestamp"]).map(
        lambda x: int((now - x).total_seconds() // 360)
    )

    # the latest event whatever it is shown
    return E.sort_values("timestamp").drop_duplicates(  # [E["type"] == "A"]
        subset="one_number", keep="last"
    )


def remove_brackets(s):
    if not pd.isnull(s):
        return str(s).replace("[", "").replace("]", "")


def get_brand(s):
    if not pd.isnull(s):
        return s.split("-")[0]


def get_body(s):
    if not pd.isnull(s):
        parts = s.split(" ")
        if len(parts) == 4:
            return parts[0].split("_")[-1]


def get_material(s):
    if not pd.isnull(s):
        parts = s.split(" ")
        if len(parts) == 4:
            return parts[1]


def get_color(s):
    if not pd.isnull(s):
        parts = s.split(" ")
        if len(parts) == 4:
            return parts[2]


def get_size(s):
    if not pd.isnull(s):
        parts = s.split(" ")
        if len(parts) == 4:
            return parts[-1]


def get_asset_one(s):
    if not pd.isnull(s):
        parts = s.split("_")
        return parts[0]


def get_print_assets(fc):
    ac = fc.connectors["airtable"]

    assets = ac["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"].to_dataframe(
        ["Key", "Roll Number V3"]
    )
    assets["ONE"] = assets["Key"].map(get_asset_one)

    return assets


def try_get_order(s):
    try:
        return literal_eval(str(s))[0]
    except:
        return None


def get_order_items(fc):
    # read prod requests
    ac = fc.connectors["airtable"]

    # get the ONE assets
    ful_order_items = ac["appfaTObyfrmPHvHc"]["tblUcI0VyLs7070yI"].to_dataframe(
        fields=ORDER_ITEM_COLUMNS
    )

    return process_order_items(ful_order_items)


def process_order_items(ful_order_items):
    ful_order_items["order_name"] = ful_order_items["ORDER_NAME"].map(try_get_order)
    ful_order_items["brand"] = ful_order_items["KEY"].map(get_brand)
    ful_order_items["body"] = ful_order_items["KEY"].map(get_body)
    ful_order_items["material"] = ful_order_items["KEY"].map(get_material)
    ful_order_items["color"] = ful_order_items["KEY"].map(get_color)
    ful_order_items["size"] = ful_order_items["KEY"].map(get_size)

    return (
        ful_order_items.rename(
            columns={
                "Active ONE Number": "one_number",
                "FULFILLMENT_STATUS": "fulfillment_status",
                "KEY": "order_key",
            }
        )
        .reset_index()
        .drop("ORDER_NAME", 1)
    )


def from_changes(changes):

    C = changes[changes["column_name"].isin(ORDER_ITEM_COLUMNS)].astype(str)
    C = C.drop_duplicates(subset=["record_id", "column_name"], keep="last").pivot(
        "record_id", "column_name", "cell_value"
    )
    return process_order_items(C)


def update_status_table(fc, data):
    data = data[STATE_COLS]
    if "one_number" not in data.columns:
        data["one_number"] = None
    data = data[data["one_number"].notnull()]
    ps = ProductionStatusDynamoTable()
    ps.update(data)


def update_ones_on_rolls(asset_changes):
    data = (
        asset_changes.astype(str)
        .sort_values("timestamp")
        .drop_duplicates(subset=["record_id", "column_name"])
        .pivot("record_id", "column_name", "cell_value")
        .rename(columns={"Order Number": "one_number", "Roll Number V3": "roll_id"})[
            ["one_number", "roll_id"]
        ]
        .dropna()
    )

    # the one number is in there but with the material - the one can actually be on multiple roles
    data["one_number"] = data["one_number"].map(
        lambda x: None if pd.isnull(x) else x.split("_")[0]
    )

    ps = ProductionStatusDynamoTable()
    ps.update_roll_assignments(data)


def update_fulfillment_status(event, context):
    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]

        DRUID_URL = os.getenv(
            "DRUID_URL", "http://druid-router.druid.svc.cluster.local:8888"
        )

        u = urlparse(DRUID_URL)
        events = get_events(druid_host=u.hostname, port=u.port)

        s3.write_date_partition(
            events,
            entity="order_item_statuses",
            group_partition_key=None,
            # the timestamp must be the target end date of the event
            date_partition_key="timestamp",
            # for now REPLACE but only until we add watermarks so we can keep rechecking types
            replace=True,
        )

        res.utils.logger.info("Updating druid partitions with events")

        # TODO we - should only use recent partitions for performance

        r = safe_http.request_post(
            f"{DRUID_URL}/druid/indexer/v1/task", json=DRUID_UPDATE_SPEC
        )

        if r.status_code != 200:
            raise Exception("Failed to run druid ingestion task for prod status")

        latest = events_to_latest(events)
        latest.to_parquet(
            f"s3://res-data-platform/entities/order_item_statuses/.queue_snapshot"
        )

        res.utils.logger.info("Updating druid partitions with events for latest queue")

        r = safe_http.request_post(
            f"{DRUID_URL}/druid/indexer/v1/task", json=DRUID_UPDATE_SPEC_LATEST
        )

        if r.status_code != 200:
            raise

        res.utils.logger.info("Done")
        return events


def _get_one_production_data_from_airtable(keys):
    mapping = {
        "Factory Request Name": "key",
        "SKU": "sku",
        "Order Number v3": "one_number",
        "Current Assembly Node": "current_status",
        "body_version": "body_version",
        "Body Code": "body_code",
        "SIZE": "size_code",
    }

    ac = res.connectors.load("airtable")
    key_predicate = res.connectors.airtable.AirtableConnector.make_key_lookup_predicate(
        keys, "Order Number v3"
    )
    return ac["Make ONE Production"]["Production Request"].read_and_map(
        mapping, filters=key_predicate
    )


def add_product_instance_info_for_one_numbers(assets):
    def lookup_sizes():
        ac = res.connectors.load("airtable")
        sz_lookup = ac["appjmzNPXOuynj6xP"]["Sizes"].to_dataframe()
        return dict(
            sz_lookup[["_accountingsku", "Size Normalized"]]
            .rename(
                columns={"_accountingsku": "size_name", "Size Normalized": "size_code"}
            )
            .values
        )

    # check dynamo if it does not exist go to airtable
    keys = list(assets["key"].unique())
    ps = ProductionStatusDynamoTable()
    data = ps.get(keys)[["one_number", "size_code", "body_version", "sku"]]

    # defensive
    fetched_one_numbers = list(data["one_number"].unique())
    if len(fetched_one_numbers) != len(keys):
        res.utils.logger.debug(f"Missing one numbers in cache - checking airtable...")
        data = _get_one_production_data_from_airtable(keys)

    # defensive
    fetched_one_numbers = list(data["one_number"].unique())
    if len(fetched_one_numbers) != len(keys):
        missing = set(keys) - set(data[keys])
        raise Exception(
            f"There are ONEs in the asset request that could not be found in any production order list {missing}"
        )

    # the code is formed of the number and a v but we do not assume the input is one or the other
    # we assume its 1 if its null but this is actually not safe

    data["body_version"] = (
        data["body_version"].map(res.utils.dataframes.coerce_null).fillna(1)
    )
    data["body_version_code"] = data["body_version"].map(
        lambda v: v.lower() if str(v).lower()[0] == "v" else f"v{v}"
    )

    assets = pd.merge(assets, data, left_on="key", right_on="one_number", how="left")

    # defensive
    if (len(assets["size_code"].isnull())) > 0:
        # in the terrible situtation where the production status does not record the size properly - look it up using the sku
        lu_size = lookup_sizes()

        def _size(row):
            # lookup sizes
            size = lu_size.get(row["sku"].split(" ")[-1])
            # for now do this but we REALLY need to find out where we get the correct sizes that are used in the DXF file
            # CC8074, IZ2002, DJ2000
            size = (
                size.replace("One Size Fits All", "1")
                .replace("Size ", "")
                .replace("T", "")
            )
            return size

        def _body_code(row):
            return row["sku"].split(" ")[0]

        for c in ["size_code", "body_code"]:
            if c not in assets:
                assets[c] = None

        assets["size_code"] = assets["size_code"].fillna(assets.apply(_size, axis=1))
        assets["body_code"] = assets["body_code"].fillna(
            assets.apply(_body_code, axis=1)
        )
    # TODO: - if we dont have the size for any record, lookup the values
    # now that we know the body code, size code and body version we can add the outlines to the data
    return assets
