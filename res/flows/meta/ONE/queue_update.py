import res
import pandas as pd
import numpy as np
import json
from stringcase import snakecase
import traceback


def ensure_fields(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df


def treat_list(l):
    if isinstance(l, str):
        l = l.split(",")
        l = [a.strip() for a in l]
    if isinstance(l, list) or isinstance(l, np.ndarray):
        return list(l)

    return []


def treat_list_as_string(l):
    if isinstance(l, str):
        l = l.split(",")
        l = [a.strip() for a in l]
    if isinstance(l, list) or isinstance(l, np.ndarray):
        return ",".join(list(l))

    return None


def get_orders_map(ids):
    """
    for the first one orders we can map them to order numbers for what is in fulf right now adn return a lookup
    """
    from res.connectors.airtable import AirtableConnector

    if not len(ids):
        return {}

    res.utils.logger.info(f"Looking up {len(ids)} ids ")
    tab = res.connectors.load("airtable")["appfaTObyfrmPHvHc"]["tblhtedTR2AFCpd8A"]
    data = tab.to_dataframe(
        fields=["KEY", "Active ONE Numbers"],
        filters=AirtableConnector.make_key_lookup_predicate(ids, "_record_id"),
    )

    if not len(data):
        return {}

    def safe_list(l):
        try:
            return "(ONE Numbers: " + ",".join(l) + ")"
        except:
            return ""

    data["key"] = data.apply(
        lambda row: f"Order {row['KEY']} {safe_list(row['Active ONE Numbers'])}",
        axis=1,
    )

    return dict(data[["record_id", "key"]].values)


def get_body_data(days_back=None):
    res.utils.logger.info(f"fetching body data...")
    airtable = res.connectors.load("airtable")
    cols = {
        "Body Number": "name",
        "Pattern Version Number": "body_version",
        "Brand Name": "brand_name",
        "Body Name": "body_name",
        "Category Name": "category_name",
        "Available Sizes": "sizes",
        "Body ONE Ready Requests": "borr_id",
    }
    bodies = airtable.get_table_data(
        "appa7Sw0ML47cA8D1/tblXXuR9kBZvbRqoU",
        fields=list(cols.keys()),
        last_update_since_hours=24 * days_back if days_back else None,
    ).rename(columns=cols)

    bodies = bodies.explode("borr_id").reset_index(drop=True)
    bodies = ensure_fields(bodies, cols=cols.values())
    return bodies


def get_body_request_data(days_back=None, model=None):
    """
    the body queue is captured here for observability
    """

    def treat_contracts(s):
        if isinstance(s, str):
            s = s.split(",")
        if isinstance(s, list) or isinstance(s, np.ndarray):
            s = list(s)
        elif pd.isnull(s):
            return s
        s = [a for a in s if not pd.isnull(a)]
        if s:
            return s
        return []

    def try_parse_json(x):
        try:
            return json.loads(x)
        except:
            return {}

    airtable = res.connectors.load("airtable")
    bodies = get_body_data()
    body_lu = dict(bodies[["record_id", "name"]].values)
    cols = {
        # "Active Body Version" : "body_version",
        "Body Pieces Count": "body_pieces_count",
        "Body ONE Ready Request Type": "node",
        "Parent Body": "parent_body",
        "Body Meta One Response Contracts Failed": "contracts_failing_list",
        "Body ONE Ready Request Flow JSON": "sub_statuses",
        "Body ONE Ready Request Flow JSON Last Updated At": "status_last_updated_at",
        "Last Updated At": "last_updated_at",
        "Owner_Current Status": "owner",
        "Status": "sub_node_status",
        "Body Design Notes Onboarding Form": "first_one_body_design_notes",
        "Rejection Reasons": "first_one_rejection_reason",
        "Ready for Sample Status": "first_one_ready_for_sample_status",
        "Order First ONE Mode": "first_one_order_mode",
        "First ONE Order ID": "first_one_order_number",
    }

    if not days_back:
        body_requests = airtable.get_table_data(
            "appa7Sw0ML47cA8D1/tblrq1XfkbuElPh0l", fields=list(cols.keys())
        ).rename(columns=cols)
    else:
        body_requests = (
            airtable["appa7Sw0ML47cA8D1"]["tblrq1XfkbuElPh0l"]
            .updated_rows(
                fields=list(cols.keys()),
                hours_ago=24 * days_back,
                last_modified_field="Body ONE Ready Request Flow JSON Last Updated At",
            )
            .rename(columns=cols)
        )
    body_requests = ensure_fields(body_requests, cols=cols.values())
    # todo resolve from order
    body_requests["first_one_one_number"] = None
    body_requests["first_one_ready_for_sample_status"] = body_requests[
        "first_one_ready_for_sample_status"
    ].map(treat_list_as_string)
    res.utils.logger.info(
        f"Requested {len(body_requests)} records since {days_back} days back"
    )
    body_requests["contracts_failing_list"] = body_requests[
        "contracts_failing_list"
    ].map(treat_contracts)
    body_requests["entered_at"] = pd.to_datetime(
        body_requests["__timestamp__"], utc=True
    )
    body_requests["last_updated_at"] = pd.to_datetime(
        body_requests["last_updated_at"], utc=True
    )
    # use status instead if we have it
    body_requests["last_updated_at"] = pd.to_datetime(
        body_requests["status_last_updated_at"], utc=True
    ).fillna(body_requests["last_updated_at"])
    body_requests["node"] = body_requests["node"].fillna("Body Onboarding Node")
    body_requests["parent_body"] = body_requests["parent_body"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    body_requests["body_pieces_count"] = body_requests["body_pieces_count"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    body_requests["first_one_rejection_reason"] = body_requests[
        "first_one_rejection_reason"
    ].map(lambda x: x[0] if isinstance(x, list) else None)
    body_requests["parent_body"] = body_requests["parent_body"].map(
        lambda x: body_lu.get(x)
    )

    body_requests["cancelled_at"] = None
    body_requests.loc[
        body_requests["sub_node_status"] == "Cancelled", "cancelled_at"
    ] = body_requests["last_updated_at"]
    body_requests["exited_at"] = None
    body_requests.loc[
        body_requests["sub_node_status"] == "Done", "exited_at"
    ] = body_requests["last_updated_at"]
    body_requests["scheduled_exit_at"] = body_requests["entered_at"] + pd.Timedelta(14)
    body_requests["sub_statuses"] = body_requests["sub_statuses"].map(try_parse_json)

    merged = pd.merge(
        bodies,
        body_requests,
        left_on="borr_id",
        right_on="record_id",
        suffixes=["_bodies", ""],
    )
    merged = merged.sort_values("last_updated_at").drop_duplicates(
        subset=["name"], keep="last"
    )
    merged["airtable_link"] = merged["record_id"].map(
        lambda x: f"https://airtable.com/appa7Sw0ML47cA8D1/tblrq1XfkbuElPh0l/{x}"
        if x
        else None
    )

    # load the order so we can provide a link but also a low a key that we could lookup for information on the ONE
    order_map = get_orders_map(list(merged["first_one_order_number"].dropna()))
    merged["airtable_link_first_one_order"] = merged["first_one_order_number"].map(
        lambda x: f"The first ONE order [{order_map.get(x)}] -  https://airtable.com/appfaTObyfrmPHvHc/tblhtedTR2AFCpd8A/{x}"
        if pd.notnull(x)
        else None
    )
    # make one production link

    merged["refs"] = merged.apply(
        lambda row: {
            "has-parent-body": row.get("parent_body"),
            "has_brand": row.get("brand_name"),
        },
        axis=1,
    )

    if model:
        try:
            # dealing with bad data
            merged["body_name"] = merged["body_name"].fillna(merged["name"])
            merged = res.utils.dataframes.replace_nan_with_none(merged)

            records = [model(**s) for s in merged.to_dict("records")]
            return records
        except:
            res.utils.logger.warn(
                f"failed to parse into model - returning raw {traceback.format_exc()}"
            )

    return merged


def get_style_request_data(days_back=None, model=None):
    res.utils.logger.info(f"fetching apply color queue (acq) data...")
    fields = [
        "Created At",
        "Color Type",
        "Style Code",
        "Request Auto Number",
        "Flag for Review",
        "Active Flag for Review Tags",
        "Assignee Email",
        "Meta ONE Contracts Failing",
        "Body Version",
        # "Body Version",
        "Apply Color Flow Status",
        "Last Updated At",
        # "Meta.ONE Flags",
        "Meta ONE Sizes Required",
    ]
    data = res.connectors.load("airtable").get_table_data(
        "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w",
        fields=fields,
        last_update_since_hours=24 * days_back if days_back else None,
    )
    data = ensure_fields(data, fields)
    res.utils.logger.info(
        f"cleaning data with row count{len(data)} from {days_back=} in preparation for saving map base"
    )
    a = data.sort_values("Created At")
    a["Request Auto Number"] = a["Request Auto Number"].map(int)

    # remove cancelled - treat as not on queue
    # this is because sometimes after doneness a bogus request is added and cancelled - maybe we need an ever better way to confirm the existence of done
    a = a[a["Apply Color Flow Status"] != "Cancelled"]
    ###########
    a = a.sort_values("Request Auto Number")
    counter = dict(a.groupby("Style Code").count()["Created At"].reset_index().values)
    a = a.drop_duplicates(subset=["Style Code"], keep="last")[fields + ["record_id"]]

    a["flag_for_review_tag"] = a["Active Flag for Review Tags"].map(str)
    a["apply_color_flow_status"] = a["Apply Color Flow Status"].map(lambda a: a[0])
    a["contracts_failing_list"] = a["Meta ONE Contracts Failing"].map(treat_list)
    a["is_flagged"] = a["Flag for Review"].fillna(0).map(int)

    a["is_cancelled"] = 0
    a["is_done"] = 0
    a["number_of_times_in_queue"] = a["Style Code"].map(lambda x: counter.get(x))
    a["body_code"] = a["Style Code"].map(lambda x: x.split(" ")[0])
    a["color_code"] = a["Style Code"].map(lambda x: x.split(" ")[2])
    a["material_code"] = a["Style Code"].map(lambda x: x.split(" ")[1])

    a = a.drop(
        ["Meta ONE Contracts Failing", "Apply Color Flow Status", "Flag for Review"], 1
    )

    a.columns = [snakecase(c.lower()).replace("__", "_") for c in a.columns]

    for c in ["created_at", "last_updated_at"]:
        a[c] = pd.to_datetime(a[c], utc=True)

    a.loc[a["apply_color_flow_status"] == "Cancelled", "is_cancelled"] = 1
    a.loc[a["apply_color_flow_status"] == "Done", "is_done"] = 1
    a.loc[a["color_type"] != "Custom", "is_custom_placement"] = 0
    a.loc[a["color_type"] == "Custom", "is_custom_placement"] = 1
    a["body_version"] = a["body_version"].fillna(0).map(int)
    a["scheduled_exit_at"] = a["created_at"] + pd.Timedelta(1)
    a["exited_at"] = None

    a = a.rename(columns={"meta_one_sizes_required": "sizes"})

    res.utils.logger.info(f"Preview row {dict(a.iloc[0])}")

    styles = a.rename(
        columns={
            "style_code": "name",
            "assignee_email": "owner",
            "created_at": "entered_at",
            "apply_color_flow_status": "node",
        }
    )

    # refs - add has style code and has body code
    styles["refs"] = styles.apply(
        lambda row: {
            "has_body": row["body_code"],
            "has_style_code": row["name"],
            "has_material": row["material_code"],
        },
        axis=1,
    )
    styles.loc[styles["node"] == "Done", "exited_at"] = a["last_updated_at"]
    styles.loc[styles["node"] == "Cancelled", "cancelled_at"] = a["last_updated_at"]
    styles["airtable_link"] = styles["record_id"].map(
        lambda x: f"https://airtable.com/appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/{x}?blocks=hide"
    )
    styles = res.utils.dataframes.replace_nan_with_none(styles)

    if model:
        try:
            records = [model(**s) for s in styles.to_dict("records")]
            return records
        except:
            res.utils.loggers.warn(f"failed to parse into model - returning raw")

    return styles


def populate_dxa_queues(days_back=None, purge=False):
    # load styles and bodies into the two stores
    from res.observability.queues import Body, Style
    from res.observability.io import ColumnarDataStore, EntityDataStore
    from res.flows.meta.ONE.queue_update import (
        get_body_request_data,
        get_style_request_data,
    )

    for model, op in {
        Body: get_body_request_data,
        Style: get_style_request_data,
    }.items():
        try:
            res.utils.logger.info(f"Processing the {model} queue")

            cstore = ColumnarDataStore(model)
            if purge:
                res.connectors.load("s3")._delete_object(cstore._table_path)
            estore = EntityDataStore(model)

            data = op(days_back=days_back, model=model)

            cstore.add(data, key_field="name")
            estore.add(data, key_field="name")
        except:
            res.utils.logger.warn(
                f"Failed to process the {model} queue {traceback.format_exc()}"
            )
