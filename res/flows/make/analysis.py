import res
import pandas as pd
import numpy as np
import json
from tenacity import retry, stop_after_attempt, wait_fixed


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def get_prod_requests(open_only=False):
    """
    pull everything in the cut queue - we could add a change window logic too but the queue is small enough
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
        "Current Assembly Node": "current_assembly_node",
        "CUTQUEUE": "cut_queue",
        "Number of Printable Body Pieces": "num_body_pieces_mone_prod",
    }

    contract_variables_lookup = airtable["appH5S4hIuz99tAjm"][
        "tbllZTfwC1yMLWQat"
    ].to_dataframe(fields=["Name", "Variable Name"])
    cv = dict(contract_variables_lookup[["record_id", "Name"]].values)
    data = (
        airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"]
        .to_dataframe(
            fields=list(make_columns.keys()),
            # filters="AND(Search('Cut', {Current Assembly Node}),Search('INSPECTION', {CUTQUEUE}))",
        )
        .rename(columns=make_columns)
    )

    data["one_number"] = data["one_number"].map(int)
    data["body_version"] = data["body_version"].fillna(0).map(int)
    data["is_flagged"] = data["is_flagged"].fillna(0).map(int)
    data["sku"] = data["sku"].map(fix_sku)

    data["contract_variables"] = data["contract_variables"].map(
        lambda x: [cv.get(i) for i in x] if isinstance(x, list) else None
    )
    data["current_assembly_node"] = data["current_assembly_node"].map(
        lambda x: x[0] if isinstance(x, list) else None
    )

    data = res.utils.dataframes.replace_nan_with_none(data)

    open_ones = data[data["current_make_node"] != "Cancelled"]
    demand = dict(open_ones.groupby("sku").count()["record_id"].reset_index().values)
    in_sew = dict(
        open_ones[open_ones["current_make_node"] == "Sew"]
        .groupby("sku")
        .count()["record_id"]
        .reset_index()
        .values
    )

    unflagged_in_sew = dict(
        open_ones[
            (open_ones["current_make_node"] == "Sew") & (open_ones["is_flagged"] == 0)
        ]
        .groupby("sku")
        .count()["record_id"]
        .reset_index()
        .values
    )
    flagged = dict(
        open_ones[(open_ones["is_flagged"] == 1)]
        .groupby("sku")
        .count()["record_id"]
        .reset_index()
        .values
    )
    data["number_open_for_sku"] = data["sku"].map(lambda x: demand.get(x, 0))
    data["number_in_sew_for_sku"] = data["sku"].map(lambda x: in_sew.get(x, 0))
    data["number_unflagged_in_sew_for_sku"] = data["sku"].map(
        lambda x: unflagged_in_sew.get(x, 0)
    )
    data["number_flagged_per_sku"] = data["sku"].map(lambda x: flagged.get(x, 0))
    data["flagged_percentile"] = data["number_flagged_per_sku"].rank(pct=True)

    if open_only:
        data = data[data["current_make_node"] != "Cancelled"].reset_index(drop=True)

    return data.sort_values("cut_queue_modified_at").rename(
        columns={"__timestamp__": "created_at"}
    )


@retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
def cache_healing_data():
    snowflake = res.connectors.load("snowflake")

    r = snowflake.execute(
        f"""SELECT * FROM IAMCURIOUS_PRODUCTION.PREP_PIECES_RESPONSES_QUEUE  """
    )  #   "
    r["rc"] = r["RECORD_CONTENT"].map(json.loads)
    r["metadata"] = r["rc"].map(lambda x: x.get("metadata"))
    r["rank"] = r["metadata"].map(lambda x: x.get("rank"))
    H = r[r["rank"] == "Healing"]
    H = pd.DataFrame([_r for _r in H["rc"]])
    HH = H[
        [
            "id",
            "one_number",
            "pieces",
            "area_nest_utilized_pct_physical",
            "area_pieces_yds_physical",
            "area_nest_yds_physical",
            "width_nest_yds_physical",
            "height_nest_yds_physical",
            "size_code",
            "material_code",
            "color_code",
            "created_at",
        ]
    ]
    res.utils.logger.info(f"loaded {len(HH)}")

    HH = HH.drop_duplicates(subset=["id"])
    res.utils.logger.info(f"unique {len(HH)}")

    HH = HH[HH["pieces"].apply(len) > 0].reset_index(drop=True)

    HH["piece_code"] = HH["pieces"].map(lambda x: list(x.keys())[0])
    HH["one_number"] = HH["one_number"].map(int)
    HH = HH[
        [
            "id",
            "one_number",
            "pieces",
            "area_nest_utilized_pct_physical",
            "area_pieces_yds_physical",
            "area_nest_yds_physical",
            "width_nest_yds_physical",
            "height_nest_yds_physical",
            "size_code",
            "material_code",
            "color_code",
            "created_at",
        ]
    ]
    HH["piece_code"] = HH["pieces"].map(lambda x: list(x.keys())[0])

    HH["one_number"] = HH["one_number"].map(int)
    # s3://res-data-platform/samples/data/snapshot-mones/
    uri = "s3://res-data-platform/samples/data/snapshot-mones/healing_pieces.parquet"
    res.utils.logger.info(f"Saving {len(HH)} healing piece records to {uri}")
    HH[["one_number", "created_at", "piece_code", "id"]].to_parquet(uri)
    HH.sort_values("created_at")
    return HH


def add_healing_to_ones(ones, refresh_cache=False, save_stats=True):
    """
    computing interesting healing statistics for any set of ONEs
    can seed with all known ones or use the function above

    TODO:
    - We should add the normalization of counts e.g. healing count / instance of making -> finding how many times we have made a piece require some work
      (table how many times have we made sku as a factor)
    - we need to do more work to find high value features
    - we need to have some specific questions (LLM MLL)
      - what pieces or skus are healing often and at risk
      - are there are any anomalies in changes in certain values over time
    """
    s3 = res.connectors.load("s3")
    hasura = res.connectors.load("hasura")

    Q = """query MyQuery {
    infraestructure_bridge_sku_one_counter {
        sku
        style_sku
        one_number
        created_at
    }
    }
    """

    res.utils.logger.info("Counting skus")
    # TODO: this is inefficient we should execute the query maybe group by in SQL but its the same DB scan anyway so we can live with it
    data = pd.DataFrame(
        hasura.execute_with_kwargs(Q)["infraestructure_bridge_sku_one_counter"]
    )
    sku_counter = dict(data.groupby("sku").count()[["created_at"]].reset_index().values)

    if ones is None:
        res.utils.logger.info(
            "no ones passed in - using cached. This is a test mode - it is recommended to bring the data from production latest"
        )
        ones = s3.read(
            "s3://res-data-platform/samples/data/snapshot-mones/mones_all.parquet"
        )

    if refresh_cache:
        res.utils.logger.info("Caching healing data")
        cache_healing_data()

    # depends on this running - could do it hourly as part of the job that is compiling this
    healings = s3.read(
        "s3://res-data-platform/samples/data/snapshot-mones/healing_pieces.parquet"
    )

    # just in case
    healings = healings.drop_duplicates(subset=["id"])

    # are these pieces healing RIGHT now in print!!

    # now merge what is in mone
    healings["one_number"] = healings["one_number"].map(int)
    # filter out old 2d codes
    healings = healings[healings["piece_code"].map(lambda x: len(str(x)) > 5)]
    # rate is based on how many times the sku itself is ordered -fill na on the column

    piece_counter = (
        healings.groupby("piece_code")
        .count()["id"]
        .reset_index()
        .rename(columns={"id": "healed_count"})
    )
    healings = pd.merge(healings, piece_counter, on="piece_code")

    healings["piece_type_heal_percentile"] = healings["healed_count"].rank(pct=True)

    ones["one_number"] = ones["one_number"].map(int)
    # we can do an inner join
    healings = pd.merge(
        healings, ones, on="one_number", suffixes=["", "_request"], how="inner"
    )

    healings["number_of_skus_ordered_approx"] = (
        healings["sku"].map(lambda x: sku_counter.get(x)).fillna(1)
    )

    res.utils.logger.info(f"{len(healings)} records of healings joined to ones")

    def sized_code(row):
        return f"{row['piece_code']} {row['sku'].split(' ')[-1].lstrip()}"

    healings["sized_piece_code"] = healings.apply(sized_code, axis=1)
    healings["physical_piece"] = healings.apply(
        lambda row: f"{row['one_number']}-{row['piece_code']}-{row['created_at']}",
        axis=1,
    )

    for c in [
        "sku",
        "one_number",
        "sized_piece_code",
        "order_number",
        "physical_piece",
    ]:
        h = (
            healings.groupby(c)
            .count()["created_at"]
            .reset_index()
            .rename(columns={"created_at": "healed"})
        )
        h["percentile"] = h["healed"].rank(pct=True)
        heal_counter = dict(h[[c, "healed"]].values)
        rank = dict(h[[c, "percentile"]].values)
        # create a rank too for all counters
        healings[f"per_{c}_heal"] = healings[c].map(lambda x: heal_counter.get(x))

        healings[f"per_{c}_percentile"] = healings[c].map(lambda x: rank.get(x))

    # here we norm the piece in the sku by how many times we ordered that piece
    healings["piece_sku_heal_normed"] = (
        healings["per_sku_heal"] / healings["number_of_skus_ordered_approx"]
    )
    # we then get the rank of all pieces - this is one of the better measures of the susceptibility to need heal
    healings["sku_piece_heal_percentile"] = healings["piece_sku_heal_normed"].rank(
        pct=True
    )

    for c in ["created_at", "created_at_request"]:
        healings[c] = pd.to_datetime(healings[c], utc=True)

    # how significant is the size of the piece
    healings["size_wrt_pieces_healing_influence"] = (
        healings["per_sized_piece_code_heal"] / healings["healed_count"]
    )
    # how significant is the particular piece out of all ones in the order
    healings["piece_wrt_product_healing_influence"] = (
        healings["per_physical_piece_heal"] / healings["per_one_number_heal"]
    )
    # how significant is the one out of all the order
    healings["piece_wrt_order_healing_influence"] = (
        healings["per_one_number_heal"] / healings["per_order_number_heal"]
    )
    #
    healings["hours_between_heal_and_request"] = (
        healings["created_at"] - healings["created_at_request"]
    ).dt.total_seconds() / 3600
    healings["hours_since_healing"] = (
        res.utils.dates.utc_now() - healings["created_at"]
    ).dt.total_seconds() / 3600
    healings["hours_since_request"] = (
        res.utils.dates.utc_now() - healings["created_at_request"]
    ).dt.total_seconds() / 3600

    healings["is_piece_recently_healed"] = 0
    healings.loc[healings["hours_since_healing"] <= 24, "is_piece_recently_healed"] = 1

    for c in [
        "sku",
        "one_number",
        "sized_piece_code",
        "order_number",
        "physical_piece",
    ]:
        h = (
            healings.groupby(c)
            .sum()["is_piece_recently_healed"]
            .reset_index()
            .rename(columns={"is_piece_recently_healed": "healed_recently"})
        )
        heal_counter = dict(h[[c, "healed_recently"]].values)
        healings[f"is_{c}_healing"] = healings[c].map(lambda x: heal_counter.get(x))

    healings = healings.round(2)

    if save_stats:
        uri = "s3://res-data-platform/samples/data/snapshot-mones/healing_statistics.parquet"
        res.utils.logger.info(uri)
        healings.to_parquet(uri)
        # healings.set_index('pid').to_csv("/Users/sirsh/Downloads/wip_healing_piece_stats.csv")

    return healings


def get_one_and_order_aggregates(healings, save_stats=True):
    """
    aggregate order and one level stats based on the previous schema
    """
    s3 = res.connectors.load("s3")

    agg_one = {
        "is_one_number_healing": min,
        "is_order_number_healing": min,
        # how many have healed of our pieces
        "per_physical_piece_heal": sum,
        # how many are healing now approx - the first of these
        "is_physical_piece_healing": sum,
        # "_sized_piece_code_hsum": sum,
        # how many time pieces like this or skus are heaked
        "per_sku_percentile": np.mean,
        "piece_type_heal_percentile": np.mean,
        # our healing state
        "hours_since_healing": min,
        "hours_since_request": max,
        "hours_between_heal_and_request": max,
        # these are all the pieces belonging to us that healed
        "piece_code": set,
    }

    agg_order = dict(agg_one)
    agg_order["is_one_number_healing"] = sum

    agg_sku = dict(agg_one)
    agg_sku["is_one_number_healing"] = sum
    agg_sku["per_sku_percentile"] = min
    agg_sku["hours_since_request"] = min

    as_counts = {"is_physical_piece_healing": "num_physical_pieces_healing"}

    OO = healings.groupby("one_number").agg(agg_one).rename(columns=as_counts)
    OOO = healings.groupby("order_number").agg(agg_order).rename(columns=as_counts)
    SKU = healings.groupby("sku").agg(agg_sku).rename(columns=as_counts)

    if save_stats:
        s3.write(
            "s3://res-data-platform/samples/data/snapshot-mones/one_level_healing_statistics.parquet",
            OO.reset_index(),
        )
        s3.write(
            "s3://res-data-platform/samples/data/snapshot-mones/order_level_healing_statistics.parquet",
            OOO.reset_index(),
        )
        s3.write(
            "s3://res-data-platform/samples/data/snapshot-mones/sku_level_healing_statistics.parquet",
            SKU.reset_index(),
        )

        # OO.to_csv("/Users/sirsh/Downloads/wip_healing_piece_stats_by_ONE.csv")
        # OOO.to_csv("/Users/sirsh/Downloads/wip_healing_piece_stats_by_order.csv")

    return OO, OOO, SKU


@res.flows.flow_node_attributes(
    memory="4Gi",
)
def handler(event, context={}):
    """
    every hour or so we can cache these
    """
    # retry logic to functions

    mones = get_prod_requests(open_only=False)
    healings = add_healing_to_ones(mones, refresh_cache=True)
    get_one_and_order_aggregates(healings)

    return {}
