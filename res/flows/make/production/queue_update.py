import res
import pandas as pd
from res.flows.meta.ONE.contract.ContractVariables import ContractVariables
from res.observability.io import VectorDataStore, ColumnarDataStore, EntityDataStore
from res.observability.queues import *
from res.observability.io.EntityDataStore import Caches
import json
from tqdm import tqdm
from res.flows.make.healing.piece_healing_monitor import (
    get_healing_pieces_per_one_from_data,
)


def get_anode(a):
    if isinstance(a, list):
        return list(set(a) - {"Trim"})[0]
    return None


def make_refs(row):
    sku = row["sku"]
    return {
        "has_body": sku.split(" ")[0].strip(),
        "has_style_code": " ".join(sku.split(" ")[:3]),
        "troubleshooting": "If request is stuck in DXA lookup the body and style code 'entities' to learn more about what is happening WITHIN the DXA nodes",
        "has_brand": (
            row["brand_name"].strip() if pd.notnull(row["brand_name"]) else None
        ),
    }


def fix_sku(sku):
    """
    this guy knows the difference between a sku and a style sku
    """
    if sku[2] != "-":
        sku = f"{sku[:2]}-{sku[2:]}"
    return sku


def get_pas():
    airtable = res.connectors.load("airtable")
    map_cols = {
        "Prep Pieces Spec Json": "piece_info",
        "Order Number": "one_number",
        "Print Queue": "status",
        "Rank": "rank",
    }
    pas = airtable["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"].to_dataframe(
        fields=list(map_cols.keys()),
        filters="AND({Rank}='Healing', {Print Queue})!='Done'",
    )
    return pas


def load_queue_data(since_date=None):
    def get_cv_map():
        from res.flows.meta.ONE.contract.ContractVariables import ContractVariables

        df = ContractVariables.load(base_id="appH5S4hIuz99tAjm")
        cvs = dict(df[["record_id", "name"]].dropna().values)
        return cvs

    columns = {
        "Order Number v3": "name",
        "Order Priority Type": "order_priority_type",
        "_original_request_placed_at": "customer_ordered_at",
        "Brand": "brand_name",
        "_cancel_date": "cancelled_at",
        "Belongs to Order": "customer_order_number",
        "Current Make Node": "node",
        "Current Assembly Node": "node_status",
        "SKU": "sku",
        "Trim Status Last Modified": "trim_picked_at",
        "CUT Date": "exited_cut_at",
        "sew_exit_at": "exited_sew_at",
        "PRINTQUEUE": "print_queue",
        "Last Updated At": "last_updated_at",
        "Fabric Print Date": "exited_print_at",
        "DxA Exit Date": "dxa_assets_ready_at",
        "Checked Into Warehouse At": "checked_into_warehouse_at",
        #'Sew Assignment: Sewing Node': 'sewing_node_assignment',
        "Contract Variables": "contracts_failing_list",
        "PPU Creation Status": "ppu_status",
        "Flag for Review Reason": "flag_reason",
        "Sales Channel": "sales_channel",
        "Flag for Review Reason": "comments",
    }

    cvs = get_cv_map()

    def lu_contract_vars(xl):
        if isinstance(xl, list):
            return [cvs.get(x) for x in xl if cvs.get(x)]
        return None

    airtable = res.connectors.load("airtable")

    """
    load data
    """
    res.utils.logger.info(f"Loading healing data...")

    hdata = get_healing_pieces_per_one_from_data(None)
    hlookup = dict(hdata[["one_number", "pending_healing_pieces"]].values)

    res.utils.logger.info(
        f"Requesting airtable data from the mone base. Will take a moment..."
    )
    if since_date:
        mod_field = "Last Updated At"
        res.utils.logger.info(f"Using filter {since_date=} - {mod_field=}")
        # if we have a change data we use that

        data = airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"].updated_rows(
            since_date=since_date,
            # This date I am assuming is updated when one of the fields we care about changes
            last_modified_field=mod_field,
            fields=list(columns.keys()),
        )

    else:
        data = airtable.get_table_data(
            "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk", fields=list(columns.keys())
        )
    res.utils.logger.info(f"Retrieved {len(data)} rows")

    res.utils.logger.info(f"Loaded healing data for {len(hlookup)} ONEs")

    for k in columns.keys():
        # stupid airtable BS
        if k not in data.columns:
            data[k] = None
    data = data.rename(columns=columns).rename(columns={"__timestamp__": "entered_at"})
    data["brand_name"] = data["brand_name"].fillna("UNKNOWN")

    """
    process columns
    """
    data["airtable_link"] = data["record_id"].map(
        lambda x: f"https://airtable.com/appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwUFV9tG1fj3atRr/{x}"
    )

    for d in [
        "last_updated_at",
        "exited_print_at",
        "dxa_assets_ready_at",
        "exited_sew_at",
        "exited_cut_at",
        "entered_at",
    ]:
        data[d] = pd.to_datetime(data[d], utc=True)
    data["sku"] = data["sku"].map(fix_sku)

    # hard code the exit for now
    data["scheduled_exit_at"] = data["entered_at"] + pd.Timedelta(days=10)
    data["refs"] = data.apply(make_refs, axis=1)
    data["contracts_failing_list"] = data["contracts_failing_list"].map(
        lu_contract_vars
    )
    data["number_of_failing_contracts"] = data["contracts_failing_list"].map(
        lambda x: len(x) if isinstance(x, list) else 0
    )
    data["node_status"] = data["node_status"].map(get_anode)
    data["name"] = data["name"].map(int)

    # using the integer one number, lookup the pieces that are still healing on that one
    data["healing_piece_list"] = data["name"].map(lambda x: hlookup.get(x) or [])

    data = res.utils.dataframes.replace_nan_with_none(data)
    res.utils.logger.info(f"Loaded and processed {len(data)} records")
    return data


def write_records_to_bridge(data):
    from tqdm import tqdm
    from res.flows.make.production.queries import update_one_status

    res.utils.logger.info(f"Writing state to the bridge table with {len(data)} records")

    for record in tqdm(data.to_dict("records")):
        # print(record)
        r = update_one_status(record, hasura=None)
        # break

    res.utils.logger.info(f"Done")


def ingest_queue_data(since_date=None, write_bridge_table=False):
    """
    Load the production requests queue
    we load 3 stores effectively
    - the bridge table is our write back from airtable so we know the dates for node transitions
    - the entity store is part of the ask-one framework for key-val looksups
    - the columnar store
    """
    hasura = res.connectors.load("hasura")

    res.utils.logger.info(f"Fetching production queue")
    data = load_queue_data(since_date=since_date)

    data = data.dropna(subset=["node", "sku", "name"])
    data["node_status"] = data["node_status"].fillna("NA")
    # load healing context

    if write_bridge_table:
        # we have mapped to something that matches the prod request below or both this and the next fail
        # it should be possible to re-read the parquet into this thing too
        write_records_to_bridge(data)

    records = [ProductionRequest(**d) for d in data.to_dict("records")]

    res.utils.logger.info(
        f"Populating the queue - columnar and entity stores with {len(data)} records..."
    )

    publish_queue_update(
        records,
        description="For make one production requests and the processing of orders i.e. ONEs",
    )
    return data
