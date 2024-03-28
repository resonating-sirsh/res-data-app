import res

# from res.learn.agents.data.EntityDataStore import EntityDataStoreAny as EntityDataStore
from res.observability.io import EntityDataStore, ColumnarDataStore
from res.observability.entity import AbstractEntity
from stringcase import snakecase
from tqdm import tqdm
from res.utils.env import RES_DATA_BUCKET
import pandas as pd
import numpy as np
from res.connectors.airtable import formatting
from res.connectors.airtable import AirtableConnector


def san_list(s):
    try:
        return s[0] if isinstance(s, list) else s
    except:
        return s


s3 = res.connectors.load("s3")


def ensure_fields(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None
    return df


def merge_data(uri, data, key):
    """
    dedupe and merge on key
    """
    if s3.exists(uri):
        existing = s3.read(uri)
        res.utils.logger.info(
            f"Existing data with {len(existing)} rows - checking if we have anything new in {len(data)} incoming records using key {key}"
        )
        data = pd.concat([existing, data]).drop_duplicates(subset=[key], keep="last")
    res.utils.logger.info(
        f"Complete data with {len(data)} rows - writing to path {uri}"
    )
    s3.write(uri, data)
    return data


def load_ones(entity_name="one", namespace="make", days_back=None):
    mones = AirtableConnector.get_airtable_table_for_schema_by_name(
        "make.production_requests",
        last_update_since_hours=24 * days_back if days_back else None,
    )

    mones["print_exit_date"] = pd.to_datetime(mones["print_exit_date"], errors="coerce")
    mones["cut_exit_date"] = pd.to_datetime(mones["cut_exit_date"], errors="coerce")

    mones["is_printed"] = mones["print_exit_date"].notnull().map(int)
    mones["is_cut"] = mones["cut_exit_date"].notnull().map(int)
    mones["is_cancelled"] = mones["cancel_date"].notnull().map(int)

    mones["order_number"] = mones["order_number"].map(lambda x: f"#{x}")
    mones["sku"] = mones["sku"].map(lambda x: f"{x[:2]}-{x[2:]}")
    mones = res.utils.dataframes.replace_nan_with_none(mones).drop(
        columns=["is_cancelled", "defects", "style_id"], axis=1
    )
    mones["material_code"] = mones["sku"].map(lambda x: x.split(" ")[1])

    mones["one_number"] = mones["key"]
    for col in []:
        mones[col] = mones[col].map(san_list).map(str)

    mones["current_assembly_node"] = mones["current_assembly_node"].map(str)
    # TODO

    mones = mones.drop(columns=["request_type", "cut_exit_date"], axis=1)

    # resolve contract variable text
    cvs = res.connectors.load("airtable").get_table_data(
        "appH5S4hIuz99tAjm/tbllZTfwC1yMLWQat/viwZvo8LgU6VqKF4u",
        fields=["Name", "Commercial Acceptability Evaluation"],
    )
    lu_cvs = dict(cvs[["record_id", "Name"]].values)

    """
    Block for adding some contract variables
    """

    def _cvs(x):
        if isinstance(x, list):
            return [lu_cvs.get(item) for item in x]
        if isinstance(x, str):
            return [lu_cvs.get(x)]

        return None

    for col in ["contract_variables", "sew_contract_variables"]:
        mones[col] = mones[col].map(_cvs)

    mones["has_shade_differences"] = (
        mones["contract_variables"].map(lambda x: "Shade Diff" in str(x)).map(int)
    )
    mones["has_lines"] = (
        mones["contract_variables"]
        .map(lambda x: "Verticle Lines" in str(x) or "Horizontal Lines" in str(x))
        .map(int)
    )
    mones["has_distortion"] = (
        mones["contract_variables"].map(lambda x: "Distortion Lines" in str(x)).map(int)
    )
    mones["has_white_dots"] = (
        mones["contract_variables"].map(lambda x: "White Dots" in str(x)).map(int)
    )
    mones["has_chemical_drops"] = (
        mones["contract_variables"].map(lambda x: "Chemical Drops" in str(x)).map(int)
    )
    mones["has_snagged_threads"] = (
        mones["contract_variables"].map(lambda x: "Snagged Thread" in str(x)).map(int)
    )
    mones["has_pattern_failure"] = (
        mones["sew_contract_variables"]
        .map(lambda x: "Pattern Failure" in str(x))
        .map(int)
    )

    # make a csv field now that we extracted what we wanted
    mones["contract_variables"] = mones["contract_variables"].map(
        lambda x: ",".join(x) if isinstance(x, list) else None
    )
    mones["sew_contract_variables"] = mones["contract_variables"].map(
        lambda x: ",".join(x) if isinstance(x, list) else None
    )
    mones = mones.rename(
        columns={
            "contract_variables": "contract_violations",
            "sew_contract_variables": "sew_contract_violations",
        }
    )

    mones["createdtime"] = pd.to_datetime(mones["createdtime"])

    sample = dict(mones[mones["contract_violations"].notnull()].iloc[0])
    res.utils.logger.info(f"Saving records like {sample}")

    # table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{entity_name}/partition_0.parquet"
    table_path = f"s3://res-data-platform/stores/columnar-store/{namespace}/{entity_name}/parts/0/data.parquet"
    res.utils.logger.info(f"Writing {len(mones)} rows to table path {table_path}")
    (
        s3.write(table_path, mones)
        if days_back is None
        else merge_data(table_path, mones, key="one_number")
    )

    cstore = ColumnarDataStore.open(name=entity_name, namespace=namespace)
    s = EntityDataStore(cstore.entity)
    for record in tqdm(mones.to_dict("records")):
        # print(record)
        s.insert_row(record, key="key")

    return mones


def load_style_queue(
    entity_name="style", namespace="meta", days_back=None, skip_redis=False
):
    res.utils.logger.info(f"fetching apply color queue (acq) data...")
    fields = [
        "Created At",
        "Color Type",
        "Style Code",
        "Request Auto Number",
        "Flag for Review",
        "Assignee Email",
        "Meta ONE Contracts Failing",
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

    a = a.drop_duplicates(subset=["Style Code"], keep="last")[fields]

    a["apply_color_flow_status"] = a["Apply Color Flow Status"].map(lambda a: a[0])

    a["contract_failure_names"] = a["Meta ONE Contracts Failing"]  # .map(str)
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

    a.loc[a["apply_color_flow_status"] == "Cancelled", "is_cancelled"] = 1
    a.loc[a["apply_color_flow_status"] == "Done", "is_done"] = 1

    a = res.utils.dataframes.replace_nan_with_none(a)

    a = a.rename(columns={"meta_one_sizes_required": "list_of_sizes"})

    res.utils.logger.info(f"Preview row {dict(a.iloc[0])}")

    # TODO come up with a partial loader strategy
    # table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{entity_name}/partition_0.parquet"
    table_path = f"s3://res-data-platform/stores/columnar-store/{namespace}/{entity_name}/parts/0/data.parquet"
    res.utils.logger.info(f"Writing table path {table_path}")
    (
        s3.write(table_path, a)
        if days_back is None
        else merge_data(table_path, a, key="style_code")
    )

    if not skip_redis:
        cstore = ColumnarDataStore.open(name=entity_name, namespace=namespace)

        s = EntityDataStore(cstore.entity)

        for record in tqdm(a.to_dict("records")):
            # print(record)
            s.insert_row(record, key="style_code", etype="style")

    return a


def load_bodies(entity_name="body", namespace="meta", days_back=None):
    res.utils.logger.info(f"fetching body data...")
    airtable = res.connectors.load("airtable")
    cols = [
        "Body Number",
        "Brand Name",
        "Pattern Version Number",
        "Body Name",
        "Category Name",
        "Body Pieces Count",
        "Body ONE Ready Request Status",
        "Available Sizes",
    ]
    bodies = airtable.get_table_data(
        "appa7Sw0ML47cA8D1/tblXXuR9kBZvbRqoU",
        fields=cols,
        last_update_since_hours=24 * days_back if days_back else None,
    )
    bodies = ensure_fields(bodies, cols)
    bodies["Body ONE Ready Request Status"] = bodies[
        "Body ONE Ready Request Status"
    ].map(lambda x: x[0] if isinstance(x, list) and len(x) else None)
    bodies = bodies.rename(
        columns={
            "Available Sizes": "size list",
            "Body ONE Ready Request Status": "status",
            "Pattern Version Number": "body_version",
            "Body Number": "body_code",
            "__timestamp__": "created_at",
        }
    )
    bodies.columns = [snakecase(b).replace("__", "_") for b in bodies.columns]
    bodies = res.utils.dataframes.replace_nan_with_none(bodies)

    # table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{entity_name}/partition_0.parquet"
    table_path = f"s3://res-data-platform/stores/columnar-store/{namespace}/{entity_name}/parts/0/data.parquet"
    res.utils.logger.info(f"Writing table path {table_path}")
    (
        s3.write(table_path, bodies)
        if days_back is None
        else merge_data(table_path, bodies, key="body_code")
    )

    cstore = ColumnarDataStore.open(name=entity_name, namespace=namespace)
    s = EntityDataStore(cstore.entity)

    # TODO make this version aware
    for record in tqdm(bodies.to_dict("records")):
        # print(record)
        s.insert_row(record, key="body_code", etype="body")

    return bodies


def load_materials(
    entity_name="material_properties_lookup", namespace="meta", days_back=None
):
    """
    Load some information about materials in a batch mode
    in future we should have ingestion via pydantic types to control what happens
    """

    res.utils.logger.info(f"fetching material data...")

    """
    Load two different material sources to join on material code
    """
    airtable = res.connectors.load("airtable")
    cols = [
        "Material Code",
        "Fabric Type",
        "Material Category",
        "Wash Care Instructions",
        "Weight Category",
        "Material Stability",
        "Material Name",
        "Specified Width (in.)",
        "Station / Category",
    ]
    mats = airtable.get_table_data(
        "appoaCebaYWsdqB30/tblJOLE1yur3YhF0K",
        fields=cols,
        last_update_since_hours=24 * days_back if days_back else None,
    )
    cols2 = [
        "Material Code",
        "Locked Width Digital Compensation (Scale)",
        "Locked Length Digital Compensation (Scale)",
        "Offset Size (Inches)",
        "Pretreatment Type",
        "Material Taxonomy",
    ]
    mats_2 = airtable.get_table_data(
        "app1FBXxTRoicCW8k/tblD1kPG5jpf6GCQl",
        fields=cols2,
        last_update_since_hours=24 * days_back if days_back else None,
    )

    mats = ensure_fields(mats, cols)
    mats_2 = ensure_fields(mats_2, cols2)
    mats = mats[mats["Station / Category"] == "Fabric"]
    fabrics = pd.merge(
        mats,
        mats_2,
        left_on="Material Code",
        right_on="Material Code",
        suffixes=["", "_make"],
    )

    """
    CHOOSE COLS
    """
    fabrics = fabrics[list(set(cols + cols2))]
    fabrics["Material Stability"] = fabrics["Material Stability"].fillna("Stable")
    fabrics = res.utils.dataframes.replace_nan_with_none(fabrics).rename(
        columns={"Specified Width (in.)": "width"}
    )
    fabrics.columns = [
        snakecase(s.strip())
        .replace("__", "_")
        .replace("_/_", "_")
        .replace("_(_scale)", "")
        .replace("_(_inches)", "")
        for s in fabrics.columns
    ]

    fabrics["name"] = fabrics["material_code"]

    # table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{entity_name}/partition_0.parquet"
    table_path = f"s3://res-data-platform/stores/columnar-store/{namespace}/{entity_name}/parts/0/data.parquet"
    res.utils.logger.info(f"Writing table path {table_path}")
    (
        s3.write(table_path, fabrics)
        if days_back is None
        else merge_data(table_path, fabrics, key="material_code")
    )
    # uses default store name here can change

    """
    get a model for the entity store - better had we made a pydantic from scratch
    """

    cstore = ColumnarDataStore.open(name=entity_name, namespace=namespace)

    cstore.description = "Get information on materials used in make and their various physical properties - this will NOT provide information about bodies or styles that use these materials"
    cstore.update_index()

    s = EntityDataStore(cstore.entity)
    records = [cstore.entity(**record) for record in fabrics.to_dict("records")]
    res.utils.logger.info(f"Writing entities such as {records[0]}")

    s.add(records, key_field="material_code")

    return fabrics


def load_orders(entity_name="orders", namespace="sell", days_back=None, route=None):
    from res.observability.queues import Order, publish_queue_update

    def _validate_ones(s):
        def _valid(x):
            try:
                return int(float(x)) > 1000
            except Exception as ex:
                return False

        if isinstance(s, list) or isinstance(s, np.ndarray):
            return [i for i in s if _valid(i)]

    def okey(row):
        k = row["key"].split("_")[0]
        return f"{k}"

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
    cols = [
        "KEY",
        "Shipping Tracking Number Line Item",
        "SKU",
        "SKU Mismatch",
        "BRAND_CODE",
        "BRAND",
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
        "__timestamp_fulfillment",
        "__order_record_id",
    ]
    order_item_data = airtable.get_table_data(
        "appfaTObyfrmPHvHc/tblUcI0VyLs7070yI",
        last_update_since_hours=24 * days_back if days_back else None,
        fields=cols,
    )

    order_item_data = ensure_fields(order_item_data, cols)

    res.utils.logger.info(f"transforming order items")
    try:
        OI = order_item_data.copy()
        OI.columns = [clean(s) for s in order_item_data.columns]

        for col in [
            "brand_code",
            "brand",
            "flag_for_review_tag",
            "order_channel",
            "sales_channel",
            "email",
            "order_record_id",
        ]:
            OI[col] = OI[col].map(san_list).map(str)

        for col in ["reservation_status", "sku_mismatch"]:
            OI[col] = OI[col].map(lambda x: formatting.strip_emoji(str(x)))

        OI["order_name"] = OI.apply(okey, axis=1)
        OI = res.utils.dataframes.replace_nan_with_none(OI)
        # OI = remove_object_types(OI)
        OI["sku"] = OI["sku"].map(lambda x: f"{x[:2]}-{x[2:]}")
        OI["style_code"] = OI["sku"].map(lambda x: " ".join(x.split(" ")[:3]))
        OI["cancelled_at"] = pd.to_datetime(OI["cancelled_at"], utc=True)
        OI["timestamp"] = pd.to_datetime(OI["timestamp"], utc=True)
        OI["created_at"] = pd.to_datetime(OI["created_at"], utc=True)
        OI["ordered_at"] = OI["created_at"]
        OI["fulfilled_at"] = pd.to_datetime(OI["timestamp_fulfillment"], utc=True)

        OI["is_cancelled"] = 0
        OI["is_pending"] = 1
        OI["is_fulfilled"] = 0
        OI.loc[OI["fulfillment_status"] == "FULFILLED", "is_fulfilled"] = 1
        OI.loc[OI["fulfillment_status"] == "CANCELED", "is_cancelled"] = 1
        OI.loc[OI["fulfillment_status"] == "FULFILLED", "is_pending"] = 0
        OI.loc[OI["fulfillment_status"] == "CANCELED", "is_pending"] = 0
        OI = OI.drop(
            columns=[
                "key",
                "lineitem_id",
                "flag_for_review",
                "record_id",
                "sku_mismatch",
                "reservation_status",
            ],
            axis=1,
        )

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

        my_data = OI.groupby(
            [
                "order_name",
                "sales_channel",
                "order_channel",
                "brand_code",
                "brand",
                "order_record_id",
            ]
        ).agg(
            {
                **{
                    a: len
                    for a in [
                        "number_of_order_items",
                    ]
                },
                **{
                    a: sum
                    for a in [
                        "is_fulfilled",
                        "is_cancelled",
                        "has_one_number",
                        "has_flag",
                        "has_hold_flag",
                        "is_pending",
                    ]
                },
                "ordered_at": max,
                "fulfilled_at": max,
                "cancelled_at": max,
                "last_updated_at": max,
                "active_one_number": list,
                "sku": list,
                "style_code": list,
                "flag_for_review_tag": list,
            }
        )

    except Exception as ex:
        print(ex)
        print(OI.columns)
        return OI

    my_data["is_completely_fulfilled"] = (
        my_data["number_of_order_items"] == my_data["is_fulfilled"]
    ).map(int)
    my_data["is_completely_fulfilled"] = (
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

    my_data["num_m1"] = my_data["style_code"].map(lambda f: len([i for i in f]))
    # my_data = my_data[my_data['is_completely_fulfilled']==0]
    my_data["ratio_having_one_numbers"] = (
        my_data["has_one_number"] / my_data["number_of_order_items"]
    )
    my_data["days_since_order"] = (
        res.utils.dates.utc_now() - pd.to_datetime(my_data["ordered_at"], utc=True)
    ).dt.days

    my_data["active_one_number"] = my_data["active_one_number"].map(_validate_ones)

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
    ).reset_index()

    uri = "s3://res-data-platform/samples/data/order_item_stats.parquet"
    res.utils.logger.info(f"caching order items to {uri}")

    s3.write(uri, OI)

    subset_cols = [
        "order_record_id",
        "sku",
        "order_name",
        "style_code",
        "brand_code",
        "brand",
        "number_of_pending_order_items",
        "number_of_fulfilled_order_items",
        "number_of_cancelled_order_items",
        "number_of_order_items_that_have_one_number",
        "number_of_order_items_that_have_hold_flags",
        "number_of_order_items_that_have_flags",
        "order_channel",
        "sales_channel",
        "is_completely_fulfilled",
        "is_completely_cancelled",
    ]

    # TODO come up with a partial loader strategy
    # table_path = f"s3://{RES_DATA_BUCKET}/data-lake/rmm/{namespace}/{entity_name}/partition_0.parquet"
    table_path = f"s3://res-data-platform/stores/columnar-store/{namespace}/{entity_name}/parts/0/data.parquet"
    res.utils.logger.info(f"Writing table path {table_path}")
    (
        s3.write(table_path, my_data)
        if days_back is None
        else merge_data(table_path, my_data, key="order_name")
    )

    # this is all temporary sourcing data - pulls things from airtable and sends to caches for testing agents
    if route != "queue":
        cstore = ColumnarDataStore.open(name=entity_name, namespace=namespace)
        s = EntityDataStore(cstore.entity)

        for record in tqdm(my_data[subset_cols].to_dict("records")):
            # print(record)
            s.insert_row(record, key="order_name", etype=entity_name)
    else:
        my_data = my_data.rename(
            columns={
                "brand": "brand_name",
                "order_name": "name",
                "sku": "skus",
                "active_one_number": "one_numbers",
                "flag_for_review_tag": "contracts_failing_list",
                "number_of_order_items_that_have_one_number": "number_of_production_requests_total",
                "number_of_pending_order_items": "number_of_production_requests_delayed_in_nodes",
                "number_of_order_items_that_have_hold_flags": "number_of_production_requests_with_failing_contracts",
                "ordered_at": "entered_at",
            }
        )
        my_data["node"] = "Sell"
        my_data["airtable_link"] = my_data["order_record_id"].map(
            lambda x: f"https://airtable.com/appfaTObyfrmPHvHc/tblhtedTR2AFCpd8A/{x}"
        )
        my_data["scheduled_exit_at"] = my_data["entered_at"] + pd.Timedelta(days=10)
        for c in ["contracts_failing_list", "skus", "one_numbers"]:
            my_data[c] = my_data[c].fillna(lambda x: []).map(list)

        my_data = res.utils.dataframes.replace_nan_with_none(my_data)

        res.utils.logger.info(f"Publishing queue")
        records = [Order(**o) for o in my_data.to_dict("records")]
        publish_queue_update(records)

    return OI


def reload_contract_variables(days_back=7, **kwargs):
    """ """

    from res.observability.entity import AbstractEntity, AbstractVectorStoreEntry
    from res.observability.io import ColumnarDataStore, VectorDataStore
    import json

    airtable = res.connectors.load("airtable")
    data = airtable.get_table_data("app6tdm0kPbc24p1c/tblVsZYRk4MzKAgd5")

    data = airtable.get_table_data(
        "app6tdm0kPbc24p1c/tblVsZYRk4MzKAgd5",
        last_update_since_hours=24 * days_back if days_back else None,
    )

    if len(data) == 0:
        res.utils.logger.info("noting to update")
        return
    cols = [
        "Name",
        "Variable Name",
        "Variable Name_Spanish",
        "Active/Inactive",
        "Source",
        "Commercial Acceptability Review Status",
        "Parent Node Name",
        "Commercial Acceptability Contract_English",
        "Commercial Acceptability Specification_Proposal",
        "Contract Variable Owner Name",
        "dxa_node_name",
        "Version",
        "ONE Impact",
    ]
    for c in cols:
        if c not in data.columns:
            data[c] = None

    data = data[cols]

    from stringcase import snakecase

    def cl(s):
        return snakecase(s.replace(" ", "").replace("/", "")).replace("__", "_")

    data.columns = [cl(c) for c in data.columns]
    data = data.rename(
        columns={
            "variable_name": "contract_variable_name",
            "active_inactive": "is_active",
            "commercial_acceptability_review_status": "com_accept_review_status",
            "commercial_acceptability_specification_proposal": "commercial_acceptability_proposal",
            "commercial_acceptability_contract_english": "commercial_acceptability_terms",
        }
    )

    data["parent_node_name"] = data["parent_node_name"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )
    data["source"] = data["source"].map(lambda x: x[0] if isinstance(x, list) else None)
    data["text"] = data.apply(lambda row: json.dumps(dict(row)), axis=1)

    M = AbstractVectorStoreEntry.create_model_from_data(
        "contract_variables_text", namespace="contracts", data=data
    )

    store = VectorDataStore(
        M,
        create_if_not_found=True,
        description="Glossary of contract variables with text for Resonance with owners, nodes, commercial acceptability terms",
    )
    records = [M(**r) for r in data.to_dict("records")]
    store.update_index()
    store.add(records)

    return data


def reload_all(days_back=1):
    for f in (
        load_style_queue,
        load_orders,
        load_materials,
        load_bodies,
        load_ones,
        reload_contract_variables,
    ):
        try:
            f(days_back=days_back)
        except Exception as ex:
            res.utils.logger.info(f"Failing to run {f} - {ex}")
