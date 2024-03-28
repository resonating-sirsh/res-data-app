from res.flows import FlowContext
import res
from tenacity import retry, wait_fixed, stop_after_attempt
from stringcase import titlecase, snakecase
import pandas as pd
from functools import reduce
from operator import or_
import traceback
import os


def get_print_cohorts():
    res.utils.logger.info(f"Loading assets")
    airtable = res.connectors.load("airtable")
    pas = airtable.get_table_data(
        "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w",
        fields=[
            "SKU",
            "Last Optimus Status",
            "Original Order Place At",
            "Last Updated At",
            "Created At",
            "Rank",
            "Prepared Pieces Count",
            "Print Queue",
            "Material Code",
            "Assigned Rolls",
            "Rolls Assignment v3",
            "Order Number",
            "Flag For Review",
            "Flag For Review Reason",
            "Print Flag for Review: Tag",
        ],
    )

    # select some fields
    res.utils.logger.info(f"Transforming assets")
    chk = pas
    chk["pending_digital_assets"] = 0
    chk.loc[
        (chk["Prepared Pieces Count"].isnull())
        & (chk["Print Queue"] == "TO DO")
        & (chk["Rank"] != "Healing"),
        "pending_digital_assets",
    ] = 1
    chk = res.utils.dataframes.replace_nan_with_none(chk)
    chk["Print Flag for Review: Tag"] = chk["Print Flag for Review: Tag"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    for c in ["Original Order Place At", "Last Updated At", "Created At"]:
        chk[c] = pd.to_datetime(chk[c], utc=True)
    chk["days_to_queue"] = (chk["Created At"] - chk["Original Order Place At"]).dt.days
    chk["Original Order Place At"] = chk["Original Order Place At"].dt.date
    # aggregate by one
    res.utils.logger.info(f"Aggregating assets")
    GO = (
        chk.groupby(["Order Number", "SKU"])
        .agg(
            {
                "record_id": len,
                "Last Optimus Status": set,
                "Original Order Place At": min,
                "Last Updated At": max,
                "Created At": min,
                "Rank": set,
                "Material Code": set,
                "pending_digital_assets": max,
                "Prepared Pieces Count": sum,
                "Print Queue": set,
                "Flag For Review": set,
                "Print Flag for Review: Tag": set,
                "days_to_queue": max,
                "Flag For Review Reason": set,
            }
        )
        .reset_index()
    )

    # quick functions to make it easier to pick some states of interest - may be wrong
    def all_printed_function(row):
        pa = list(row["Print Queue"])
        return pa[0] == "PRINTED" and len(pa) == 1

    def any_healing_function(row):
        pa = list(row["Rank"])
        return "Healing" in pa

    def waiting_to_fill(row):
        pa = list(row["Last Optimus Status"])
        return "INSUFFICIENT_ASSET_LENGTH" in pa

    def waiting_for_roll(row):
        pa = list(row["Last Optimus Status"])
        return "NO_ROLLS" in pa

    def has_all_valid(row):
        pa = list(row["Last Optimus Status"])
        return pa[0] == "VALID" and len(pa) == 1

    def is_flagged(row):
        pa = list(row["Flag For Review"])
        return "True" in str(pa[0])

    def has_cancelled_asset(row):
        pa = list(row["Print Queue"])
        return "CANCELLED" in pa

    def cuttable_width_issue(row):
        return "into roll with width" in str(row["Flag For Review Reason"])

    res.utils.logger.info(f"Setting states")
    GO["all_printed"] = GO.apply(all_printed_function, axis=1).map(int)
    GO["has_healing"] = GO.apply(any_healing_function, axis=1).map(int)
    GO["waiting_to_fill_roll"] = GO.apply(waiting_to_fill, axis=1).map(int)
    GO["has_all_valid_rolls"] = GO.apply(has_all_valid, axis=1).map(int)
    GO["is_flagged"] = GO.apply(is_flagged, axis=1).map(int)
    GO["has_cancelled_assets"] = GO.apply(has_cancelled_asset, axis=1).map(int)
    GO["waiting_for_roll"] = GO.apply(waiting_for_roll, axis=1).map(int)

    def flat_set(s):
        return reduce(or_, s, set())

    columns = [
        "pending_digital_assets",
        "days_to_queue",
        "all_printed",
        "has_healing",
        "waiting_to_fill_roll",
        "has_all_valid_rolls",
        "is_flagged",
        "has_cancelled_assets",
        "waiting_for_roll",
    ]
    sums = dict(zip(columns, [sum for i in range(len(columns))]))
    sums.update({"Order Number": set})
    sums.update({"Material Code": flat_set})
    sums.update({"record_id": sum})

    try:
        res.utils.logger.info(f"Re-aggregating assets")
        PRINT = (
            GO[GO["all_printed"] == 0].groupby(["Original Order Place At"]).agg(sums)
        )
    except:
        return GO
    # GO[GO['all_printed']==0]

    PRINT["open_ones"] = PRINT["Order Number"].map(len)
    PRINT = PRINT.rename(
        columns={"Order Number": "one_numbers", "Material Code": "material_codes"}
    )
    PRINT.columns = [f"in_print_{c}" for c in PRINT.columns]

    for c in ["in_print_one_numbers", "in_print_material_codes"]:
        PRINT[c] = PRINT[c].map(list)

    return PRINT


def template_records(views, counters, channel=None):
    def add_owner(t):
        if t in [
            "Stuck In Prep Print Pieces",
        ]:
            return f" (<@U01JDKSB196>)"
        if t in [
            "Apply Color Awaiting M1 Previews",
            "Apply Color Stuck In Generate M1",
            "Stuck Unpacking in Apply Color Queue",
        ]:
            return f" (<@U03Q8PPFARG>, <@U01JDKSB196>)"
        if t in ["Make failed to validate request pending assets"]:
            return f" (<@U018U6ATXPV>)"

        if t in ["Rolls without Pdf"]:
            return f" (<@U01JDKSB196>, <U01GD33PRS8>)"

        if t in ["Tagged With Defects But Not Contract Variables"]:
            return f" (<@U494P3QJE>)"
        if t in [
            "Outstanding Line Item, No Active Production Request",
            "Completed in Make, Not Checked Into Warehouse",
            "Rejected at Sew Inspection, Open Line Item, Not Reproduced",
            "Unflagged, No Production Request, Not in Inventory",
        ]:
            return f" (<@U02AA6M66UF>)"
        return ""

    records = [
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"<https://airtable.com/{v}|{t}> {add_owner(t) if counters[t]>0 else ''}",
                },
                {"type": "mrkdwn", "text": f"{counters[t]} records"},
            ],
        }
        for t, v in views.items()
    ]
    records

    temp = {
        "slack_channels": [channel or "one-platform-x"],
        "message": "These are the current counters on support views that should typically be empty",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "Airtable views that have records but should not",
                },
            },
            *records,
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"Here is the overview <https://grafana.resmagic.io/d/Wttd_ekIk/exception-reporting?orgId=1|Dashboard>",
                    },
                ],
            },
        ],
    }
    return temp


def handler(event, context=None, plan=False, channel=None):
    """
    currently this is a pretty naff scheme
    obvious way to improve is to add an admin later to views and owners and read from some database

    test this with

    handler({}, channel='test-channel')

    """
    with FlowContext(event, context) as fc:
        airtable = res.connectors.load("airtable")
        slack = res.connectors.load("slack")

        @retry(wait=wait_fixed(3), stop=stop_after_attempt(2))
        def safe_fetch_counter(view):
            view = view.split("/")
            tab = airtable[view[0]][view[1]]
            return len(tab.to_dataframe(view=view[-1]))

        views = {
            "Print_investigations": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwVPJbbnbbhClqup",
            "Ones in Done state but likely stuck": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwxdFp5cspzNdJKX",
            "Stuck in prep print pieces": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwVreapFyS2GYTUw",
            # "should be nesting ready but not": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwAtHmFxyKnG1Kwy",
            # "reproduced requests that haven't been cancelled": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viw8f90sKZH7LwG98",
            "Rolls not migrated to print app": "apprcULXTWu33KFsh/tblJAhXttUzwxdaw5/viwAUvbjjTSvHAcQG",
            "Request not properly exiting Dxa": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwPKbW72i4ykinBp",
            # "print asset state TODO but roll out of print": "apprcULXTWu33KFsh/tblAQcPuKUDVfU7Fx/viwv7VOESbfJdccuC",
            "Make production request time outs": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwrKSWl3etArvaUi",
            "Production requests without print asset requests": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viw94ljVdPkVDl9zU",
            "Cut and Sew escalations": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwPndLDSeMXLCOdg",
            "Days since order for Dxa queue": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwdyXjBqO9duPVb6",
            # "Stuck in roll assignment": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwRcz1HvHkUpJVpz",
            # "Stuck in nesting queue": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwiZufZE46mqyg7k",
            # "Stuck in Printer Assignment": "apprcULXTWu33KFsh/tblwDQtDckvHKXO4w/viwCwAdeL6YVVt0B5",
            # "Apply Color Awaiting M1 Previews": "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwskJ7Blw0gT11Rl",
            # "Apply Color needing intervention": "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viw3XPNXrGa9BSImP",
            "Apply Color Stuck In Generate M1": "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwiu6edbsVEd5b4H",
            "Make failed to validate request pending assets": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwsop6DaKAggORL0",
            "Stuck Unpacking in Apply Color Queue": "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwwbs56OwDi1QLbq",
            # "Sew Awaiting Meta ONEs": "appa7Sw0ML47cA8D1/tblZrP2omIoDE7fsL/viw3uhK5iRjFFdt9E",
            "Rolls without Pdf": "apprcULXTWu33KFsh/tblSYU8a71UgbQve4/viwMcp6FnzeaQiBKX",
            "Tagged With Defects But Not Contract Variables": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viwDOsYcrxHWjh1zt",  # (Lily)
            "Outstanding Line Item, No Active Production Request": "appfaTObyfrmPHvHc/tblUcI0VyLs7070yI/viwr3EZMDfDLpBRrt",  # (Armi)
            "Completed in Make, Not Checked Into Warehouse": "appfaTObyfrmPHvHc/tblUcI0VyLs7070yI/viw3aAYy2dekJltp8",  #  (Armi)
            "Rejected at Sew Inspection, Open Line Item, Not Reproduced": "appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/viw0iu5bQzl7uKqtM",  # (Armi)
            "Body Meta One Response Failing Contracts": "appa7Sw0ML47cA8D1/tbl0ZcEIUzSLCGG64/viwXK49Evl0YE1oMJ",
            "Unflagged, No Production Request, Not in Inventory": "appfaTObyfrmPHvHc/tblUcI0VyLs7070yI/viwb07HMkwJ9DwGXj",  # (armi)
        }

        counters = {}
        for k, view in views.items():
            res.utils.logger.debug(f"Fetching {k}")
            k = k
            try:
                c = safe_fetch_counter(view)
                counters[k] = c

                if c > 0:
                    mt = res.utils.logger.metric_node_state_transition_incr(
                        node="general",
                        asset_key=k.replace(",", ""),
                        status="VALIDATION_ERRORS",
                        process="are-we-flowing",
                        inc_value=c,
                    )
                    res.utils.logger.info(f"publish metric {mt}")

            except:
                res.utils.logger.warn(f"Failed stats")
                res.utils.logger.warn(traceback.format_exc())
                counters[k] = -1

        if not plan:
            slack(template_records(views, counters, channel=channel))

        return counters


def get_m1_cohorts():
    from res.flows.make.analysis import get_prod_requests

    airtable = res.connectors.load("airtable")
    s3 = res.connectors.load("s3")
    # select order dates (try to use them by fallback)
    # this is a test for having all orders
    mones = get_prod_requests(open_only=False)
    dxa = mones[mones["current_make_node"] == "DxA Graphics"]
    dxa["style_sku"] = dxa["sku"].map(
        lambda x: f" ".join([a for a in x.split(" ")[:3]])
    )
    print(len(dxa.groupby("style_sku").count()), len(dxa))

    data = airtable.get_table_data(
        "appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w",
        fields=[
            "Apply Color Flow Status",
            "Export Asset Bundle Job Posted At",
            "Meta.ONE Flags",
            "Style Code",
            "Priority",
            "Priority",
            "Assignee Email",
            "Meta ONE Piece Preview Rejection Reason",
            "Requests IDs",
            "Flag for Review",
            "Apply Color Flow Status Last Updated At",
            "Root Cause Fix Owner",
            "Active Issue IDs",
            "Active Flag For Review Owners Emails",
        ],
    )
    data["Apply Color Flow Status"] = data["Apply Color Flow Status"].map(
        lambda x: x[0]
    )

    # need to know if one of them where cancelled
    # data[data['Apply Color Flow Status']!= 'Cancelled']

    a = data.drop_duplicates(subset=["Style Code"], keep="last")
    a = (
        a[a["Style Code"].isin(dxa["style_sku"])]
        .reset_index()
        .sort_values("Apply Color Flow Status")
    )
    a["days_in_queue"] = (
        (
            res.utils.dates.utc_now() - pd.to_datetime(a["__timestamp__"], utc=True)
        ).dt.total_seconds()
        / 3600
        / 24
    )
    a["last_moved_hours_ago"] = (
        res.utils.dates.utc_now()
        - pd.to_datetime(a["Apply Color Flow Status Last Updated At"], utc=True)
    ).dt.total_seconds() / 3600
    a["has_issue"] = 0
    a.loc[a["Active Issue IDs"].notnull(), "has_issue"] = 1
    #####################

    a["Assignee Email Temp"] = a.apply(
        lambda row: "No Person Assigned"
        if "Review" in str(row["Apply Color Flow Status"])
        or "Place Color" in str(row["Apply Color Flow Status"])
        else "Tech",
        axis=1,
    )
    a["Assignee Email"] = a["Assignee Email"].fillna(a["Assignee Email Temp"])
    a["Issue Assignee Email"] = a["Active Flag For Review Owners Emails"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    a[
        (a["has_issue"] == 1) & (a["Active Flag For Review Owners Emails"].isnull())
    ] = "No Person Assigned"

    # User issue alert on open ones
    # User qty alert on open ones
    a["num_requests"] = a["Requests IDs"].map(
        lambda x: len(x.split(",")) if pd.notnull(x) else 0
    )
    chk = a[a["Apply Color Flow Status"] != "Done"]
    #####
    create = chk.groupby(["Assignee Email", "Apply Color Flow Status"]).agg(
        {"index": len, "num_requests": sum, "has_issue": sum}
    )
    create["type"] = "Create"
    ######
    a["num_requests"] = a["Requests IDs"].map(
        lambda x: len(x.split(",")) if pd.notnull(x) else 0
    )
    chk = a[a["Apply Color Flow Status"] != "Done"]
    repair = chk.groupby(["Issue Assignee Email", "Apply Color Flow Status"]).agg(
        {"index": len, "num_requests": sum, "has_issue": sum}
    )
    repair["type"] = "Repair"

    try:
        merged_stats = (
            pd.concat([create, repair])
            .fillna(0)
            .rename(
                columns={
                    "index": "count",
                    "num_requests": "open orders",
                    "has_issue": "has problems",
                }
            )
            .sort_values("has problems")
        )

        metrics_and_slack("the m1 queue owners are", merged_stats)
    except:
        res.utils.logger.warn(f"Failed on the merged stats compute and send")
        res.utils.logger.warn(traceback.format_exc())
    # we could slack this merged state by user

    ####################
    ACQ = a[
        [
            "Style Code",
            "Apply Color Flow Status",
            "has_issue",
            "Apply Color Flow Status Last Updated At",
            "__timestamp__",
            "Assignee Email",
            "Active Flag For Review Owners Emails",
        ]
    ].rename(
        columns={
            "Style Code": "style_sku",
            "Apply Color Flow Status Last Updated At": "status_changed_at",
            "__timestamp__": "arrived_in_queue",
            "Apply Color Flow Status": "status",
        }
    )
    R = pd.merge(
        dxa[["style_sku", "one_number", "created_at", "sku"]], ACQ, on="style_sku"
    )
    R["date"] = pd.to_datetime(R["created_at"], utc=True).dt.date
    R["Node"] = "M1"
    cohort = (
        R.groupby(["date", "status"])
        .agg({"one_number": [len, list]})[["one_number"]]
        .reset_index()
        .fillna(0)
    )  # .astype(int)
    cohort.columns = [c[0] + c[1].strip(",") for c in cohort.columns.to_flat_index()]
    cohort = cohort.rename(
        columns={"one_numberlen": "request counts", "one_numberset": "onenumbers"}
    )
    cohort = cohort.pivot("date", "status").fillna(0).T
    cohort = cohort.T

    cohort["request counts"] = cohort["request counts"].astype(int)
    cohort["total_in_m1"] = cohort["request counts"].sum(axis=1).astype(int)

    def uc(c):
        return f"{c[0]}_{c[1]}" if isinstance(c, tuple) else c

    cohort.columns = [
        uc(c)
        .replace(" ", "_")
        .lower()
        .replace("numberlist", "")
        .rstrip("_")
        .replace("__", "_")
        for c in cohort.columns.to_flat_index()
    ]

    return cohort


def get_and_aggregate(event={}, context={}):
    res.utils.logger.info(f"Getting cohorts for m1...")
    expect_cols = [
        "request_counts_cancelled",
        "request_counts_done",
        "request_counts_export_colored_pieces",
        "request_counts_place_color_elements",
        "request_counts_review_brand_turntable",
    ]

    try:
        cohort = get_m1_cohorts()
        for c in expect_cols:
            if c not in cohort:
                cohort[c] = 0
        TEST = cohort[expect_cols].copy()
        TEST.columns = [c.replace("request_counts_", "") for c in TEST.columns]
        TEST = TEST.reset_index().rename(columns={"date": "entered production"})
        TEST.columns = [titlecase(c) for c in TEST.columns]
        total = pd.DataFrame(TEST[:-14].sum(axis=0)).T.reset_index(drop=True)
        total["Entered Production"] = "Previously"
        records = len(TEST)
        records = -1 * max(records, 14)
        m1_cohorts = pd.concat([total, TEST[records:]])

        metrics_and_slack("M1 cohort report", m1_cohorts)
    except:
        res.utils.logger.warn("failed")
        res.utils.logger.warn(traceback.format_exc())
    res.utils.logger.info(f"Getting cohorts for print...")
    P = get_print_cohorts()

    TEST = P.copy()[
        [
            "in_print_pending_digital_assets",
            "in_print_days_to_queue",
            "in_print_has_healing",
            "in_print_waiting_to_fill_roll",
            "in_print_has_all_valid_rolls",
            "in_print_is_flagged",
            "in_print_waiting_for_roll",
            "in_print_open_ones",
        ]
    ]
    TEST.columns = [
        titlecase(c.replace("in_print", "")).lstrip().rstrip() for c in TEST.columns
    ]
    total = pd.DataFrame(TEST[:-14].sum(axis=0)).T.reset_index()
    total["index"] = "Previously"

    prev = total.rename(columns={"index": "Original Order Place At"})
    prev["Days To Queue"] = -1
    print_cohorts = pd.concat([prev, TEST[-14:].reset_index()]).reset_index(drop=True)

    # dont add the last column which is aready a total and we can aggregate
    metrics_and_slack(
        "Print cohort report",
        print_cohorts,
        exlude_cols_from_metrics=[print_cohorts.columns[-1]],
    )

    return {}


def for_node_gauge(node_name, data):
    res.utils.logger.info("Writing metrics")
    node_name = node_name.lower().replace(" ", "_")
    # ASSSUME first column is a index text field in this mode - can generlize and also just show numberic data
    df = data.set_index(data.columns[0])
    for c in df.columns:
        for i, record in enumerate(df.to_dict("records")):
            if i == 0:
                dateref = "previously"
            else:
                dateref = f"count_{len(data)-i}_days_ago"

            metric_name = f"flow_state.cohort.{node_name}.{dateref}.{c.lower().replace(' ','_')}.infra"

            try:
                val = int(float(record[c]))

                res.utils.logger.metrics.incr(metric_name, val)
            except:
                # assume numberic iteger data
                pass


def metrics_and_slack(title, data, exlude_cols_from_metrics=None):
    """
    makes some assumotions about the shape of the data from here on
    - the gague should be data that is counter data and a first column is the index
      - we try to see the index on the first column
    - for slack same
    """

    # for each column, send a metric

    # make markdown message
    gauge_data = (
        data.drop(exlude_cols_from_metrics, 1) if exlude_cols_from_metrics else data
    )
    for_node_gauge(title, gauge_data)

    if not os.environ.get("DISABLE_SLACK", False):
        slack = res.connectors.load("slack")
        slack(
            {
                "slack_channels": ["one-platform-x"],
                "message": f"""{title}
                        ```{data.to_markdown()}```
                """,
            }
        )
