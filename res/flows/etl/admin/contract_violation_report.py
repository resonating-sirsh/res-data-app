import res
from warnings import filterwarnings
from datetime import datetime
from matplotlib import pyplot as plt
import pytz
from res.utils import dataframes
from collections import OrderedDict
from res.utils.dates import utc_now
from res.utils import dates
from res.flows import FlowContext

filterwarnings("ignore")
import pandas as pd
import matplotlib.pyplot as plt

SMALL_SIZE = 8
MEDIUM_SIZE = 10
BIGGER_SIZE = 16

plt.rc("font", size=BIGGER_SIZE)  # controls default text sizes
plt.rc("axes", titlesize=BIGGER_SIZE)  # fontsize of the axes title
plt.rc("axes", labelsize=BIGGER_SIZE)  # fontsize of the x and y labels
plt.rc("xtick", labelsize=MEDIUM_SIZE)  # fontsize of the tick labels
plt.rc("ytick", labelsize=MEDIUM_SIZE)  # fontsize of the tick labels
plt.rc("legend", fontsize=SMALL_SIZE)  # legend fontsize
plt.rc("figure", titlesize=BIGGER_SIZE)  # fontsize of the figure title

# we snake case columns at use
EMAIL_WATCHLIST = [
    {
        "key": "num_late",
        "query": """healing_count >= 0""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwdhn5aAIxLv2tjT?blocks=hide",
        "description": "The number of late ONES",
    },
    {
        "key": "num_healing",
        "query": """healing_count > 0""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwGpkBtqda1hiMDr?blocks=hide",
        "description": "The number of late ONES that are awaiting healing",
    },
    {
        "key": "excessive_reproduces",
        "query": """reproduce_count > 2""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwY9c8MxzCNi5ws8?blocks=hide",
        "description": "The number of late ONES with excessive reproduce (>2)",
    },
    {
        "key": "num_exceeding_two_weeks_in_node",
        "query": """days_in_node > 14""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwU7gZY7jMax1Qr9?blocks=hide",
        "description": "The number of late ONES more than 2 weeks in a node",
    },
    {
        "key": "num_awaiting_dxa_exit",
        "query": """is_exited_dxa == False""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viw8opowepP90MwI1?blocks=hide",
        "description": "The number of late ONES stuck in DXA now",
    },
    {
        "key": "num_awaiting_print_exit",
        "query": """is_exited_print == False""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwLkyceRCxTieNRd?blocks=hide",
        "description": "The number of late ONES stuck in Print now",
    },
    {
        "key": "num_awaiting_print_healing_pieces",
        "query": """is_awaiting_printed_healing_assets == True""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwsWLoUt4HEfwjMa?blocks=hide",
        "description": "The number of late ONES  waiting for healings pieces to print",
    },
    {
        "key": "num_awaiting_nest",
        "query": """is_awaiting_nest_assignment == True""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwjSuyoKNaK5Wldh?blocks=hide",
        "description": "The number of late ONES  waiting to be nested",
    },
    {
        "key": "num_awaiting_roll",
        "query": "is_awaiting_roll_assignment == True",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwScfM9qyPaxWARK?blocks=hide",
        "description": "The number of late ONES waiting to be assigned a roll",
    },
    {
        "key": "num_awaiting_printer_reassignment",
        "query": """is_awaiting_printer_reassignment == True""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwvNh9BYnoJHboch?blocks=hide",
        "description": "The number of late ONES  waiting to be RE-assigned a roll again",
    },
    {
        "key": "num_awaiting_printer_assignment",
        "query": """is_awaiting_printer_assignment == True and is_awaiting_roll_assignment == False""",
        "view": "https://airtable.com/appUFmlaynyTcQ5wm/tblNgI4b73UWObVXI/viwsy9587HCzcV1fu?blocks=hide",
        "description": "The number of late ONE waiting on rolls waiting to be printed",
    },
]


def plot_and_encode(df, **plot_kwargs):
    """
    Given a dataframe/grouped call its plot with passed kwargs and return the encoded dataframe
    TODO
    """
    import matplotlib.pyplot as plt
    import base64
    from io import BytesIO

    fig = plt.figure(figsize=plot_kwargs.get("figsize", (10, 10)))
    ax = df.plot(**plot_kwargs)
    plt.axis("off")
    tmpfile = BytesIO()
    fig.savefig(tmpfile, format="png", bbox_inches="tight", pad_inches=0)
    encoded = base64.b64encode(tmpfile.getvalue()).decode("utf-8")
    plt.close(fig)
    return encoded


def get_email_content(list_of_violations, encoded_images=None, **kwargs):
    """
    we have a named value set of descriptions and links of violations
    we can add any images that are self descriptive

    kwargs are used to add other variables
    """
    encoded_images = encoded_images or []
    encoded_images = "\n".join(
        [
            f"""<div> <img src=\'data:image/png;base64,{encoded}\'>' </div> </br>"""
            for encoded in encoded_images
        ]
    )

    list_of_violations = "\n".join([f"<li>{d}</li>" for d in list_of_violations])
    H = f"""
    <html>
        <body>
            <div class="a"><span class="b">There are <b>{kwargs['num_late']} late ONEs</b> violating contracts</span></div>
            <br/>
            <div class="a">
              <ul>
              {list_of_violations}
            </ul> 
            </div>
             <br/>
            <div>
              {encoded_images}
            </div>
        </body>
    </html>
    """

    return H


def try_int(d):
    try:
        return int(float(d))
    except:
        return -1


def datediff(d):
    if "2021" in str(d):
        return (utc_now().replace() - d.to_pydatetime()).days
    return None


def clean(s):
    if isinstance(s, dict):
        return None
    return s


def get_healing():
    cols = [
        "Reporting Node",
        "__one_code",
        "_make_one_request_id",
        "Piece Healing Statuses",
        "Created At",
        "Pieces Count",
        "Current Make Node (from ONE)",
        "Current Assembly Node (from ONE)",
    ]
    ren_cols = [
        "reporting_node",
        "one_number",
        "_make_one_request_id",
        "status",
        "created_at",
        "pieces_count",
        "one_make_node",
        "current_assembly_node",
    ]
    airtable = res.connectors.load("airtable")

    healing_app = airtable["appWxYpvDIs8JGzzr"]["tblgVQe404mNwsTDs"].to_dataframe()

    happ = healing_app[cols].rename(columns=dict(zip(cols, ren_cols)))
    h = []
    for key, healings in happ.groupby("one_number"):
        h.append({"key": str(key), "data": healings.to_dict("records")})

    h = pd.DataFrame(h)
    h["key"] = h["key"].astype(str)
    h["count"] = h["data"].map(len)

    h.sort_values("count")
    return h


def add_material_order_statistics(f):
    """
    merge statistics for one materials etc.
    """
    f["material_code"] = f["LINEITEM  SKU"].map(
        lambda x: x.split(" ")[1].lstrip().rstrip()
    )
    f["order_date"] = pd.to_datetime(f["order_date"])
    recent_orders = f[f["order_date"].dt.date > res.utils.dates.relative_to_now(40)]
    stat = (
        recent_orders.groupby("material_code")
        .count()[["__ordernumber"]]
        .sort_values("__ordernumber")
        .rename(columns={"__ordernumber": "last_40_day_ONEs_in_material_ordered"})
    )
    f = pd.merge(f, stat, on="material_code", suffixes=["", "_stats"])
    return f


def get_fulfillments(pending=True):
    airtable = res.connectors.load("airtable")
    fulf_order_items = "appfaTObyfrmPHvHc", "tblUcI0VyLs7070yI"
    fulf_order_items = airtable[fulf_order_items[0]][fulf_order_items[1]]

    fulf_order_items_cols = [
        "KEY",
        "Days Since Order",
        "Reservation Status",
        "order_date",
        "__ordernumber",
        "Active ONE NumberLast Modified",
        "Active ONE Number",
        "Sales Channel",
        "quantity",
        "LINEITEM  SKU",
        "FULFILLMENT_STATUS",
        "styleId",
        "Line Item Cancel Reason",
        "Lineitem price",
        "Canceled Reason",
        "__unit_cancelled",
        "Cancelled at",
    ]

    ful_oitems = fulf_order_items.to_dataframe(fulf_order_items_cols)
    ful_oitems["Sales Channel"] = ful_oitems["Sales Channel"].map(
        lambda x: x[0] if isinstance(x, list) and len(x) else None
    )
    ful_oitems["has_active_one"] = False
    ful_oitems.loc[ful_oitems["Active ONE Number"].notnull(), "has_active_one"] = True

    def try_week(x):
        try:
            return x // 7
        except:
            return None

    ful_oitems["weeks_since_order"] = ful_oitems["Days Since Order"].map(try_week)
    ful_oitems["order_date"] = ful_oitems["order_date"].map(
        lambda x: x[0] if isinstance(x, list) else x
    )

    ful_oitems = add_material_order_statistics(ful_oitems)

    # add stats at the material level and use in the late ones data

    if pending:
        pending_no_cancelled = ful_oitems[
            ~ful_oitems["FULFILLMENT_STATUS"].isin(
                ["FULFILLED", "CANCELED", "HAS_CANCEL_DATE"]
            )
        ]
        pending_no_cancelled = pending_no_cancelled[
            pending_no_cancelled["has_active_one"]
        ]
        return pending_no_cancelled

    return ful_oitems


def get_make_one_productions():
    make_one_prod = "appH5S4hIuz99tAjm", "tblptyuWfGEUJWwKk"
    airtable = res.connectors.load("airtable")
    make_one_prod = airtable[make_one_prod[0]][make_one_prod[1]]
    make_one_prod_cols = [
        "Factory Request Name",
        "Make ONE Status",
        "Assembly Node Status",
        "SKU (View)",
        "PRINTQUEUE",
        "CUTQUEUE",
        "SEWQUEUE",
        "Original Request",
        "PREP_PAPERMAKER_QUEUE",
        "__brandcode",
        "Delay Reasons: Tag",
        "Cut Flags",
        "Flag For Review",
        "Flag for Review Reason",
        "Age in Current Make Node (In Hours)",
        "Print Date",
        "Make Flow Last Updated At",
        "CreatedTime",
        "Assemble Return Tag",
        "Cancel Reason Tag",
        "Utilized Rolls",
        "Warning_Owner",
        "CUT Date",
        "Cut Assignment",
        "Flag for Review: Tag",
        "Healing Created?",
        "Age in Current Assembly Node",
        "Sew Assignment: Sewing Node",
        "Sew Assignment",
        "Current Make Node",
        "Reproduce Count",
        "Current Assembly Node",
        "Is this a Reproduce?",
        "DxA Exit Date",
        "Exit Cut Date",
        "Print Exit Date",
        "Healing Date",
        "Cancel ONE last updated",
        "Reproduced ONE last update",
        "Paper Marker Exit Date",
        "Reproduced ONEs Ids",
        "sew_exit_at",
        "Checked Into Warehouse At",
    ]

    mone_prod = make_one_prod.to_dataframe(make_one_prod_cols)

    for i, k in enumerate(["body", "material", "colour", "size"]):
        mone_prod[k] = mone_prod["SKU (View)"].map(
            lambda x: x.split(" ")[i] if pd.notnull(x) else None
        )

    mone_prod["Age in Current Assembly Node"] = (
        mone_prod["Age in Current Assembly Node"].map(try_int).astype(int)
    )

    for k, v in {
        "since_print": "Print Exit Date",
        "since_print_enter": "Print Date",
        "since_dxa": "DxA Exit Date",
        "since_cut": "Exit Cut Date",
        "since_cut_enter": "CUT Date",
    }.items():
        mone_prod[v] = mone_prod[v].map(clean)
        mone_prod[v] = pd.to_datetime(mone_prod[v], utc=True)
        mone_prod[k] = mone_prod[v].map(datediff)

    mone_prod["key"] = mone_prod["Factory Request Name"].map(lambda x: x.split("-")[0])
    mone_prod["original_one_at_reproduce"] = mone_prod["Is this a Reproduce?"].map(
        lambda x: str(x).split(" ")[-1].replace("-PROD", "") if pd.notnull(x) else None
    )

    make_ones = mone_prod
    # make_ones['key'] = make_ones['Factory Request Name'].map(lambda x: x.split('-')[0])
    make_ones["style_code"] = make_ones["SKU (View)"].map(
        lambda x: " ".join(x.split(" ")[:3]) if pd.notnull(x) else x
    )
    make_ones["sku"] = make_ones["SKU (View)"]
    make_ones["brand_code"] = make_ones["__brandcode"]
    make_ones["reproduced_key"] = make_ones["original_one_at_reproduce"].map(
        lambda x: x if x != "No" else None
    )
    make_ones["reproduced_keys_all"] = make_ones["Reproduced ONEs Ids"].map(
        lambda x: x if x != "No" else None
    )
    make_ones["size_code"] = make_ones["size"]
    make_ones["node_key"] = make_ones["Current Make Node"]
    make_ones["style_key"] = make_ones["style_code"]
    make_ones["created_at"] = make_ones["CreatedTime"]
    make_ones["flag_tag"] = make_ones["Flag for Review: Tag"]
    make_ones["delay_tag"] = make_ones["Delay Reasons: Tag"]
    make_ones["reproduces"] = make_ones["Reproduce Count"]
    make_ones["days_in_node"] = make_ones["Age in Current Assembly Node"]
    make_ones["healing_date"] = make_ones["Healing Date"]
    make_ones["now"] = datetime.utcnow().replace(tzinfo=pytz.utc)
    make_ones["created_at"] = pd.to_datetime(make_ones["created_at"])
    make_ones["days_as_one"] = (make_ones["now"] - make_ones["created_at"]).dt.days
    make_ones["is_reproduced"] = make_ones["reproduced_key"].map(pd.notnull)
    make_ones["is_healing"] = make_ones["healing_date"].map(pd.notnull)
    make_ones["reproduce_count"] = make_ones["Reproduce Count"]
    make_ones["key"] = make_ones["key"].map(str)

    MAKE_ONES = make_ones[
        [
            "key",
            "style_key",
            "sku",
            "brand_code",
            "reproduced_key",
            "size_code",
            "node_key",
            "reproduces",
            "flag_tag",
            "is_reproduced",
            "is_healing",
            "reproduce_count",
            "material",
            "body",
            "Print Exit Date",
            "Flag For Review",
            "Flag for Review Reason",
            "delay_tag",
            "days_in_node",
            "days_as_one",
            "healing_date",
            "Current Assembly Node",
            "original_one_at_reproduce",
            "created_at",
            "now",
            "sew_exit_at",
            "Checked Into Warehouse At",
        ]
    ]

    MAKE_ONES = MAKE_ONES[
        (MAKE_ONES["brand_code"].notnull()) & (MAKE_ONES["style_key"].notnull())
    ]
    MAKE_ONES["key"] = MAKE_ONES["key"].astype(str)

    return make_ones


def get_late_ones(
    late_one_threshold=6, fulfillments=None, make_one_productions=None, healing=None
):
    res.utils.logger.info("loading healings...")
    h = healing or get_healing()
    res.utils.logger.info("loading fulfillments...")
    f = fulfillments or get_fulfillments()
    res.utils.logger.info("loading make one productions")
    m = make_one_productions or get_make_one_productions()

    late = pd.merge(
        f[
            [
                "Active ONE Number",
                "FULFILLMENT_STATUS",
                "Cancelled at",
                "order_date",
                "weeks_since_order",
                "__ordernumber",
                "last_40_day_ONEs_in_material_ordered",
            ]
        ],
        m,
        left_on="Active ONE Number",
        right_on="key",
        suffixes=["_fulf", ""],
    )
    late = pd.merge(
        late,
        h,
        how="left",
        left_on=["Active ONE Number"],
        right_on="key",
        suffixes=["", "_healing"],
    ).rename(columns={"count": "healing_count"})

    ######
    late["healing_count"] = late["healing_count"].fillna(0)
    late["is_flagged_any"] = False
    late.loc[
        (late["Flag For Review"].notnull())
        | (late["Flag for Review Reason"].notnull())
        | (late["flag_tag"].notnull()),
        "is_flagged_any",
    ] = True
    late["weeks_as_one"] = late["days_as_one"].map(lambda x: x // 7)
    late = late[~late["node_key"].isin(["Done", "Cancelled"])]
    late["reproduce_count"] = late["reproduce_count"].fillna(0)
    late["node_string"] = late["Current Assembly Node"].map(str)
    late["reached_cut"] = late["CUT Date"].notnull()
    late["not_printed"] = late["Print Exit Date"].isnull()
    late["is_printed"] = late["Print Exit Date"].notnull()
    late["is_exited_dxa"] = True
    late.loc[late["Current Make Node"] == "DxA Graphics", "is_exited_dxa"] = False
    late["is_multiple_days_in_node"] = False
    late.loc[late["days_in_node"] > 3, "is_multiple_days_in_node"] = True

    late["is_awaiting_printed_assets"] = False
    late["is_awaiting_printed_healing_assets"] = False
    late["is_awaiting_nest_assignment"] = False
    late["is_awaiting_roll_assignment"] = False
    late["is_awaiting_printer_assignment"] = False
    late["is_awaiting_printer_reassignment"] = False

    ######################if we want to ignore things thare just ordered ###################
    late = late[late["weeks_since_order"] > 0]
    late["tags_any"] = False
    late.loc[
        (late["healing_count"] > 0)
        | (late["is_flagged_any"])
        | (late["delay_tag"].notnull()),
        "tags_any",
    ] = True
    ########################################################################################

    ######

    if late_one_threshold:
        late = late[late["days_as_one"] > late_one_threshold]

    late = late.reset_index(drop=True)
    return late


def add_contract_violations(late_as_one, print_watch_list=None):
    ones_violating = {}

    contracts = {
        "LATE": 7,
        "LATE_WHILE_HEALING": 7,
        "LATE_WITHOUT_FLAGS": 7,
        "LATE_WITH_FLAGS": 7,
        "LATE_DXA": 7,
        "LATE_NOT_PRINTED": 7,
        "LATE_ASSEMBLY": 7,
        "LATE_WAREHOUSE": 7,
        "WAS_UNASSINGED_PRINTER": 2,
        "HOURS_IN_SUBNODE": 24,
    }

    def add_contract_violation(one, contract_key, days):
        if one not in ones_violating:
            ones_violating[one] = []

        # check days for multiples
        s = list(set(ones_violating[one] + [contract_key]))
        ones_violating[one] = s

        return ones_violating

    queries = {
        "LATE": "days_as_one > 6",
        "LATE_WHILE_HEALING": "days_as_one > 6 and is_healing == True",
        "LATE_WITH_FLAGS": "days_as_one > 6 and is_flagged_any == True",
        "LATE_WITHOUT_FLAGS": "days_as_one > 6 and is_flagged_any == False",
        "LATE_DXA": "days_as_one > 6 and  `Current Make Node` == 'DxA Graphics'",
        "LATE_ASSEMBLY": "days_as_one > 6 and  `Current Make Node` == 'Assembly'",
        "LATE_SEW": "days_as_one > 6 and  `Current Make Node` == 'Sew'",
        "LATE_WAREHOUSE": "days_as_one > 6 and  `Current Make Node` == 'Warehouse'",
        "LATE_NOT_PRINTED": "not_printed == True",
        "HOURS_IN_SUBNODE": "`Age in Current Make Node (In Hours)` > 72",
        # for print watch list below
        "WAS_UNASSINGED_PRINTER": """print_queue=='TO DO' and was_assigned_printer == True """,
        "NOT_ASSINGED_PRINTER": """print_queue=='TO DO' and was_assigned_printer == False """,
        "NOT_ASSINGED_ROLL": """print_queue=='TO DO' and was_asigned_roll == False """,
        "NOT_ASSINGED_NEST": """print_queue=='TO DO' and was_asigned_nest == False """,
    }

    for row in late_as_one.query(queries["LATE"], engine="python")[
        ["Active ONE Number", "days_as_one"]
    ].to_dict("records"):
        add_contract_violation(row["Active ONE Number"], "LATE", row["days_as_one"])

    for row in late_as_one.query(queries["LATE_WHILE_HEALING"], engine="python")[
        ["Active ONE Number", "days_as_one"]
    ].to_dict("records"):
        add_contract_violation(
            row["Active ONE Number"], "LATE_WHILE_HEALING", row["days_as_one"]
        )

    for row in late_as_one.query(queries["LATE_WITH_FLAGS"], engine="python")[
        ["Active ONE Number", "days_as_one"]
    ].to_dict("records"):
        add_contract_violation(
            row["Active ONE Number"], "LATE_WITH_FLAGS", row["days_as_one"]
        )

    for row in late_as_one.query(queries["LATE_WITHOUT_FLAGS"], engine="python")[
        ["Active ONE Number", "days_as_one"]
    ].to_dict("records"):
        add_contract_violation(
            row["Active ONE Number"], "LATE_WITHOUT_FLAGS", row["days_as_one"]
        )

    for row in late_as_one.query(queries["LATE_NOT_PRINTED"], engine="python")[
        ["Active ONE Number", "days_as_one"]
    ].to_dict("records"):
        add_contract_violation(
            row["Active ONE Number"], "LATE_NOT_PRINTED", row["days_as_one"]
        )

    for node in ["LATE_DXA", "LATE_SEW", "LATE_ASSEMBLY", "LATE_WAREHOUSE"]:
        for row in late_as_one.query(queries[node], engine="python")[
            ["Active ONE Number", "days_as_one"]
        ].to_dict("records"):
            add_contract_violation(row["Active ONE Number"], node, row["days_as_one"])

    for row in print_watch_list.query(
        queries["WAS_UNASSINGED_PRINTER"], engine="python"
    ).to_dict("records"):
        add_contract_violation(
            row["order_number"],
            "WAS_UNASSINGED_PRINTER",
            row["days_since_printer_assigned"],
        )

    for assignment in [
        "NOT_ASSINGED_PRINTER",
        "NOT_ASSINGED_ROLL",
        "NOT_ASSINGED_NEST",
    ]:
        for row in print_watch_list.query(queries[assignment], engine="python").to_dict(
            "records"
        ):
            add_contract_violation(row["order_number"], assignment, row["age"])


# def get_nests():
#     nests = airtable.get_airtable_table_for_schema_by_name("make.nest_assets")


def get_rolls(rolls=None):
    """
    Load rolls with some avail. metrics with material aggregates
    """
    airtable = res.connectors.load("airtable")
    rolls = (
        rolls
        if rolls is not None
        else airtable.get_airtable_table_for_schema_by_name("make.rolls")
    )
    rolls["flags_any"] = (
        (rolls["warning"].notnull())
        | (rolls["flag_for_review"].notnull())
        | (rolls["flag_for_review_reason"].notnull())
    )
    rolls["material_code"] = rolls["material_code"].astype(str)
    # materail props
    R = rolls[
        [
            "material_code",
            "pretreated_length",
            "nested_length",
            "key",
            "warning",
            "flag_for_review",
            "flag_for_review_reason",
            "print_one_ready",
        ]
    ]
    R["unallocated_pretreated_material"] = 0
    R["unallocated_pretreated_material"] = R.apply(
        lambda row: 0
        if row["nested_length"] > 0 or not row["print_one_ready"]
        else row["pretreated_length"],
        axis=1,
    )

    # because data is shit
    def remove_lists(v):
        if isinstance(v, list) or isinstance(v, dict):
            return None
        return v

    R = R.applymap(remove_lists)

    R = (
        R.groupby("material_code")
        .agg(
            {
                "key": "count",
                "pretreated_length": "sum",
                "nested_length": "sum",
                "warning": "count",
                "print_one_ready": "sum",
                "unallocated_pretreated_material": "sum",
            }
        )
        .reset_index()
    )

    rolls = pd.merge(rolls, R, on="material_code", suffixes=["", "_material"]).rename(
        columns={
            "key_material": "roll_in_material_count",
        }
    )

    rolls["pretreated_length"] = rolls["pretreated_length"].fillna(0)
    return rolls[
        [
            "key",
            "material_code",
            "print_one_ready",
            "pretreated_length",
            "nested_length",
            "flags_any",
            "roll_in_material_count",
            "pretreated_length_material",
            "nested_length_material",
            "warning_material",
            "print_one_ready_material",
            "unallocated_pretreated_material",
        ]
    ]


def get_print_assets(join_roll_metrics=False):
    res.utils.logger.info("loading print assets...")
    airtable = res.connectors.load("airtable")
    pa_data = airtable.get_airtable_table_for_schema_by_name("make.print_assets")

    pa_data["roll_assigned_date"] = pd.to_datetime(pa_data["roll_assigned_date"])

    # pa_data = pa_data[pa_data["assigned_printer_date"].notnull()]
    pa_data["_now"] = dates.utc_now()

    # what happened
    # assigned printer date but no assigned printed
    # healings currently do not have

    pa_data["assigned_printer_date"] = pd.to_datetime(pa_data["assigned_printer_date"])
    pa_data["created_at"] = pd.to_datetime(pa_data["created_at"])
    pa_data["days_since_printer_assigned"] = (
        pa_data["_now"] - pa_data["assigned_printer_date"]
    ).dt.days
    pa_data["age"] = (pa_data["_now"] - pa_data["created_at"]).dt.days
    pa_data["roll_assigned_date"] = pd.to_datetime(pa_data["roll_assigned_date"])
    pa_data["days_since_roll_assigned"] = (
        pa_data["_now"] - pa_data["roll_assigned_date"]
    ).dt.days
    pa_data["was_assigned_roll"] = pa_data["roll_assigned_date"].notnull()
    pa_data["was_assigned_printer"] = pa_data["assigned_printer_date"].notnull()
    pa_data["was_unassigned_from_printer"] = (
        pa_data["assigned_printer_date"].notnull()
        & pa_data["assigned_printer_v3"].isnull()
    )
    pa_data["was_assigned_nest"] = pa_data["nested_asset_sets"].notnull()
    pa_data["is_assigned_printer"] = pa_data["assigned_printer_v3"].notnull()

    if join_roll_metrics:
        res.utils.logger.info(f"adding roll metrics")
        pa_data["roll_key"] = pa_data["assigned_rolls"].map(
            lambda x: None if not isinstance(x, list) or not x or not len(x) else x[0]
        )

        rolls = get_rolls()
        pa_data = pd.merge(
            pa_data, rolls, left_on="roll_key", right_on="key", suffixes=["", "_roll"]
        )

    return pa_data


def print_file_states():
    snowflake = res.connectors.load("snowflake")
    chk = snowflake.execute(
        """select * from IAMCURIOUS_PRODUCTION_MODELS.f_print_job_states """
    )
    fchk = chk.sort_values("UPDATED_AT").drop_duplicates(
        subset=["PRINT_FILE_NAME"], keep="last"
    )


def update_airtable(df, plan=False):
    res.utils.logger.info("updating airtable...")

    def update_table(tab, data):
        if len(data) == 0:
            res.utils.logger.info(f"Noting to update - empty set")
            return

        res.utils.logger.info(f"Checking for ids at origin table: {tab.table_name}")
        ids = tab.to_dataframe(fields=["ONE Number", "Name"])

        columns = list(data.columns)

        if len(ids) and "ONE Number" in ids.columns:
            data = pd.merge(
                data,
                ids,
                how="outer",
                left_on=["ONE Number"],
                right_on=["ONE Number"],
                suffixes=["", "_ORIGIN"],
            )

            # anything that is not in the local set should be deleted from the remote
            plist = data[data["Name"].isnull()]["record_id"]
            res.utils.logger.info(f"purging {len(plist)} records")
            for record_id in plist:
                res.utils.logger.info(
                    f"Purging record {record_id} from {tab.table_name}"
                )
                c = tab._base._get_client().delete(tab.table_id, record_id)
        else:
            data["record_id"] = None

            # now update the rest

        data = (
            data[~data["Name"].isnull()]
            .sort_values("Exceptions Reported At")
            .drop_duplicates(subset=["ONE Number"], keep="last")
        )

        res.utils.logger.info(f"Updating {len(data)} records in {tab.table_name}")

        copy_data = data[columns + ["record_id"]].copy()
        for k, v in data.dtypes.items():
            if "datetime" in str(v):
                copy_data[k] = copy_data[k].map(
                    lambda x: x.isoformat() if pd.notnull(x) else None
                )
        copy_data = copy_data.where(pd.notnull(copy_data), None)
        for record in copy_data.to_dict("records"):
            try:
                tab.update_record(record)
            except Exception as ex:
                print(record)
                raise ex

    cols = {
        "active_one_number": "ONE Number",
        "__ordernumber": "Order Number",
        "sku": "Name",
        "style_code": "Style Code",
        "body": "Body Code",
        "brand_code": "Brand",
        "material": "Material",
        "reproduced_key": "Reproduced ONE Number",
        "reproduce_count": "Reproduce Count",
        "healing_count": "Healing Count",
        "node_string": "Current Assembly Node",
        "node_key": "Current Make Node",
        "order_date": "Order Created At",
        "created_at": "ONE Created At",
        "DxA Exit Date": "DXA Exit Date",
        "Print Exit Date": "Exit Print At",
        "Exit Cut Date": "Exit Cut At",
        "healing_date": "Healing At",
        "now": "Exceptions Reported At",
        "days_as_one": "Days In ONE",
        "days_in_node": "Days In Node",
        "Flag for Review: Tag": "Issue Tags",
        "Cancel Reason Tag": "Cancel Tags",
        "Delay Reasons: Tag": "Delay Tags",
        "is_flagged_any": "Is Flagged For Review",
        "is_flagged_any": "Is Tagged",
        "is_reproduced": "Is Reproduce",
        "is_exited_dxa": "Is Exited DXA",
        "is_printed": "Is Exited Print",
        "reached_cut": "Is Reached Cut",
        "is_multiple_days_in_node": "Is Many Days In Node",
        "is_awaiting_printed_assets": "Is Awaiting Printed Assets",
        "is_awaiting_printed_healing_assets": "Is Awaiting Printed Healing Assets",
        "is_awaiting_nest_assignment": "Is Awaiting Nest Assignment",
        "is_awaiting_roll_assignment": "Is Awaiting Roll Assignment",
        "is_awaiting_printer_assignment": "Is Awaiting Printer Assignment",
        "is_awaiting_printer_reassignment": "Is Awaiting Printer Reassignment",
        "free_material": "Unallocated Material",
        "last_40_day_ONEs_in_material_ordered": "last_40_day_ONEs_in_material_ordered",
        # weeks or days since order
        "reproduced_keys_all": "reproduced_keys_all",
    }

    cols = OrderedDict(cols)

    df = df.loc[:, ~df.columns.duplicated()].copy().reset_index().drop("index", 1)

    df = dataframes.rename_and_whitelist(df, cols)[list(cols.values())]

    # ss = airtable_schema_from_df(df,'Late ONEs', multi_select_fields=['Issue Tags','Cancel Tags',  'Delay Tags'])
    base_id = "appUFmlaynyTcQ5wm"
    table_id = "tblNgI4b73UWObVXI"
    airtable = res.connectors.load("airtable")
    tab = airtable[base_id][table_id]

    # fetch what exists, join ids to upsert and purge

    if not plan:
        update_table(tab, df)

    return df


def send_summary_email(df):
    res.utils.logger.info("sending summary email...")
    try:
        emailer = res.connectors.load("email")

        def full_description(row):
            return f"""<b>{row['length']}</b>: {row['description']} - see <a href="{row['view']}">this view</a> for details"""

        summaries = pd.DataFrame(EMAIL_WATCHLIST)

        temp = dataframes.snake_case_columns(df)
        print(list(temp.columns))
        summaries["length"] = summaries["query"].map(
            lambda q: len(temp.query(q, engine="numexpr"))
        )
        summaries["full_description"] = summaries.apply(full_description, axis=1)

        options = {
            record["key"]: len(df.query(record["query"]))
            for record in summaries.to_dict("records")
        }
        list_of_violations = list(summaries["full_description"])

        # TODO images from df

        content = get_email_content(list_of_violations, encoded_images=None, **options)

        recipients = "sirsh@resonance.nyc"
        emailer.send_email(
            recipients,
            "Late ONES violating contracts",
            content,
            content_type="html",
        )
        res.utils.logger.info(f"sent email to {recipients}")
    except Exception as ex:
        res.utils.logger.warn(f"Failed to send summary email {res.utils.ex_repr(ex)}")


def handler(event, context={}, plan=False, dump=False):
    """
    this is a nightly processes that builds exception reports for all support issues relating to late ones
    - what is healing or reproducing
    - what is stuck in a node / by node (tns)
    - what has lost assignments (priority issues) :> see also the graph of assignments <- regressions

    Kafka processes will in future update "graphs" if assignments to states
    - we check those states at all times: and pair with some existing airtable field
    -
    """

    with FlowContext(event, context) as fc:
        # s3 = res.connectors.load("s3")
        late = get_late_ones()
        # s3.write partition (one_data)
        res.utils.logger.info("loading rolls to determine unallocated material")
        rolls = get_rolls()
        free_roll = dict(
            rolls.groupby("material_code")
            .mean()[["unallocated_pretreated_material"]]
            .reset_index()
            .values
        )

        pa_data = get_print_assets()
        # s3.write partition (pdata)
        # ndata = get_nests()
        # s3.write partition (ndata)

        pending = pa_data[pa_data["print_queue"] != "PRINTED"]
        todo_ones = list(pending[pending["print_queue"] == "TO DO"]["order_number"])
        ones_not_assigned_roll = list(
            pending[~pending["was_assigned_roll"]]["order_number"]
        )
        ones_unassigned_printer = list(
            pending[pending["was_unassigned_from_printer"]]["order_number"]
        )
        ones_not_assigned_printer = list(
            pending[~pending["is_assigned_printer"]]["order_number"]
        )
        ones_not_assigned_nest = list(
            pending[~pending["was_assigned_nest"]]["order_number"]
        )
        ones_healing_in_print = list(
            pending[pending["rank"] == "Healing"]["order_number"]
        )

        late["is_awaiting_printed_assets"] = late["Active ONE Number"].map(
            lambda x: x in todo_ones
        )
        late["is_awaiting_printed_healing_assets"] = late["Active ONE Number"].map(
            lambda x: x in ones_healing_in_print
        )
        late["is_awaiting_nest_assignment"] = late["Active ONE Number"].map(
            lambda x: x in ones_not_assigned_nest
        )
        late["is_awaiting_roll_assignment"] = late["Active ONE Number"].map(
            lambda x: x in ones_not_assigned_roll
        )
        late["is_awaiting_printer_assignment"] = late["Active ONE Number"].map(
            lambda x: x in ones_not_assigned_printer
        )
        late["is_awaiting_printer_reassignment"] = late["Active ONE Number"].map(
            lambda x: x in ones_unassigned_printer
        )

        # this determines approx if ONEs can be assigned to rolls becasue there is pretreated stuff that is not nested and no warnings
        late["free_material"] = late["material"].map(lambda x: free_roll.get(x))
        # do add the contract violation kafka some here
        # send email here

        send_summary_email(late)

        if dump:
            late.to_pickle("/Users/sirsh/Downloads/temp_data.pkl")

        df = update_airtable(late, plan)

        if dump:
            df.to_pickle("/Users/sirsh/Downloads/temp_data_airtable.pkl")

        res.utils.logger.info("Done")

        if plan:
            return df

        return {}
