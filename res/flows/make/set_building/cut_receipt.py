import res
from res.flows.meta.ONE.meta_one import MetaOne
import os
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed


def _id(row):
    return res.utils.uuid_str_from_dict(
        {
            # a handle on the source ticket
            "record_id": row["record_id"],
            "make_queue": row["current_make_node"],
            "make_status": row["cut_queue"],
            "event_timestamp": row["cut_queue_modified_at"],
        }
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_cut_inspection_queue_from_prod_requests():
    """
    pull everything in the cut queue - we could add a change window logic too but the queue is small enough
    - send events for things just arriving beyond cut
    - send events for thinks that pass the piece check inspection at the completer

    If we generate a unique id for the event we can dedupe when sending to kafka on this key so we only send one observation for piece
    If the pieces are stored in make one production we dont need to look them up

    AND(OR(AND(CUT_EXIT='EXIT CUT',{Station Inspection Status}='Approved'),AND({CUTQUEUE}='SKIP', {Cut Target Station Exit Date}!="")),AND(FIND("Cut", {Current Assembly Node})>0),{Assembly Flow}!='', {Flag For Review}=FALSE())

    """
    airtable = res.connectors.load("airtable")

    def fix_sku(s):
        if s[2] != "-":
            return f"{s[:2]}-{s[2:]}"
        return s

    make_columns = {
        "Order Number v3": "one_number",
        # "Defects" : "defects",
        "Contract Variables": "contract_variables",
        "Flag For Review": "is_flagged",
        "SKU": "sku",
        "Cut Queue Last Modified Time": "cut_queue_modified_at",
        "Belongs to Order": "order_number",
        "Body Version": "body_version",
        "Current Make Node": "current_make_node",
        "CUTQUEUE": "cut_queue",
        "Station Inspection Status": "station_inspection_status",
        "Number of Printable Body Pieces": "num_body_pieces_mone_prod",
        # Piece Count Complete
    }

    contract_variables_lookup = airtable["appH5S4hIuz99tAjm"][
        "tbllZTfwC1yMLWQat"
    ].to_dataframe(fields=["Name", "Variable Name"])
    cv = dict(contract_variables_lookup[["record_id", "Name"]].values)
    data = (
        airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"]
        .to_dataframe(
            fields=list(make_columns.keys()),
            filters="AND(CUT_EXIT='EXIT CUT',{Station Inspection Status}='Approved', {Current Make Node}='Assemble')",
        )
        .rename(columns=make_columns)
    )

    data["one_number"] = data["one_number"].map(int)
    data["body_version"] = data["body_version"].fillna(0).map(int)
    if "is_flagged" not in data.columns:
        data["is_flagged"] = False
    else:
        data["is_flagged"] = data["is_flagged"].fillna(0).map(int)
    data["sku"] = data["sku"].map(fix_sku)

    data["contract_variables"] = data["contract_variables"].map(
        lambda x: [cv.get(i) for i in x] if isinstance(x, list) else None
    )

    # Piece Count Complete

    data = res.utils.dataframes.replace_nan_with_none(data)

    return data.sort_values("cut_queue_modified_at").rename(
        columns={"__timestamp__": "created_at"}
    )


def process_changes(since_date=None):
    data = get_cut_inspection_queue_from_prod_requests()

    # determine some things about defects and also send events to prom

    # understand what we are puling from this queue - maybe understand what we are healing to i.e. lags

    # do one of these for sew as well so we can see who is waiting for healing pieces

    # do i know what my overall demand is for skus

    # we are going to get everything and push it to the set building queue to prepare them - they will not be active until inspection is passed
    for record in data.to_dict("records"):
        record["id"] = _id(record)
        # m1 = MetaOne(record["sku"])

        # piece observation build set and send to kafka with deduplicate on key=id

        # flow api update the good stuff


@res.flows.flow_node_attributes(
    memory="4Gi",
)
def handler(event, context={}):
    # window logic when run
    process_changes(None)

    return {}
