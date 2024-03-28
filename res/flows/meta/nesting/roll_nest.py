from numpy.core.numeric import roll
from res.flows import flow_node_attributes, FlowContext
from res.utils import logger
from res.connectors import load
from res.media.images.providers import DxfFile
from res.learn.optimization.nest import (
    nest_dataframe_by_chunks,
    evaluate_nest_v1,
    update_shape_props,
)
from res.flows.dxa.printfile import ResNestV1, renest_roll_v1
import pandas as pd
from res.utils import logger
from ast import literal_eval


def nest_ranked_body(body, size, ranked_piece_names, rank, material_width):
    results = []
    print("mat", material_width)
    result = DxfFile.nest_body(
        body,
        size,
        ranked_piece_names=ranked_piece_names,
        rank=rank,
        material_width=material_width,
    )
    result = evaluate_nest_v1(result)
    results.append(result)

    return pd.DataFrame(results)


def nest_and_evaluate_styles(styles, material_width):
    """
    sum all the products split by all their piece types
    "[{'body': 'KT2017', 'normed_size': '8', 'rank': 1, 'ranked_pieces': ['PKTBAGL', 'PKTBAGR']}]"
    """
    data = pd.concat(
        [
            nest_ranked_body(
                p["body"],
                p["normed_size"],
                ranked_piece_names=p["ranked_pieces"],
                rank=p["rank"],
                material_width=material_width,
            )
            for p in styles
        ]
    )

    sum_cols = [
        "total_nested_height_yards",
        "total_nest_area_yards",
        "total_shape_area_yards",
        "pieces_per_nest",
    ]
    metrics = dict(data[sum_cols].sum())
    metrics.update(dict(data[["total_packing_factor"]].mean()))
    return metrics


def eval_roll(df, aggregate=True, plot_roll=False):
    """
    Each roll is evaluated on a node
    We should save the entire nest roll which takes some time
    WE also take a dataframe of all nesting statisics for the roll
    So we can save roll.nest and roll.evaluation
    """

    nests = []
    for roll, grp in df.groupby("roll"):
        logger.info(f"nesting roll {roll}")
        nested_roll = renest_roll_v1(grp, plot=plot_roll)
        nested_roll = update_shape_props(nested_roll)

        path = f"s3://res-data-platform/samples/data/rolls/{roll}/nested_roll.csv"
        logger.info(f"saving the nested roll to {path}")
        nested_roll.to_csv(path)

        # this is row level and now we can add it back
        # rename all these to NP-PCB
        roll_evaluation = evaluate_nest_v1(nested_roll)
        roll_evaluation = {f"RP-PCB-{k}": v for k, v in roll_evaluation.items()}

        # here we do not need to do the nest just evaluate it at roll level
        for _, row in grp.iterrows():
            _nest = row["nest_data"]
            logger.info(
                f"------------evaluating nest {_nest}, {row['Key']}------------ "
            )
            _nest = evaluate_nest_v1(_nest)
            _nest = {f"NP-PCB-{k}": v for k, v in _nest.items()}
            _nest.update(roll_evaluation)
            logger.info("-----DONE---")

            # todo get all the one nests in the roll
            # check a cache to see if the body-combo has been nested already for the right sizes
            # for this entire nest we can go and resolve all nests and then aggregate their stats

            try:
                logger.info("************BODY LEVEL (no material props)**************")
                # add body level we need to do the nest
                body_evaluation = nest_and_evaluate_styles(
                    row["product"], material_width=row["material_width"]
                )
                body_evaluation = {f"1P-P00-{k}": v for k, v in body_evaluation.items()}
                _nest.update(body_evaluation)
            except Exception as ex:
                logger.warn(ex)
                logger.warn(
                    f"************************{row['Key']} FAILED******************"
                )

            _nest["roll"] = roll
            _nest["key"] = row["Key"]
            # _nest['material_code'] = row['Material Code']
            _nest["one_count"] = row["one_count"]
            _nest["nest_count"] = 1

            nests.append(_nest)

    nests = pd.DataFrame(nests)

    if aggregate:

        # save pre-aggregated data on s3 and then we will also add that to snowflake??

        for roll, data in nests.groupby("roll"):
            # for parquet to-string some data
            for c in ["product", "Material_set"]:
                if c in data.columns:
                    data[c] = data[c].map(str)
            path = f"s3://res-data-platform/samples/data/rolls/{roll}/nest-level-evaluation.parquet"
            logger.info(f"saving the nest level not aggregated roll data to {path}")
            data.to_parquet(path)

        nests = nests.set_index(["key", "roll"]).astype(float)
        aggregations = {k: "sum" for k in list(nests.columns)}
        aggregations.update(
            {k: "mean" for k in list(nests.columns) if "packing_factor" in str(k)}
        )
        aggregations.update(
            {k: "max" for k in list(nests.columns) if "width_yards" in str(k)}
        )
        aggregations.update({k: "min" for k in list(nests.columns) if "RP-" in str(k)})
        nests = nests.groupby("roll").agg(aggregations)

        # later we will add on printer datea for rolls above and these we will be aggregated too

    return nests.reset_index()


def generator(event, context=None):
    nest_asset = pd.read_csv("s3://res-data-platform/samples/data/nests.csv")
    rolls = nest_asset["roll"].unique()
    assets = [
        {
            "apiVersion": "v0",
            "metadata": {"name": "meta.nesting.roll_nest"},
            "assets": [{"roll": roll}],
        }
        for roll in rolls
    ]
    return assets


@flow_node_attributes(
    memory="2Gi",
)
def handler(event, context=None):

    nest_asset = pd.read_csv("s3://res-data-platform/samples/data/nests.csv")
    nest_asset["product"] = nest_asset["product"].map(literal_eval)
    event["metadata"] = {"name": "meta_nesting_rolls", "node": "eval_rolls"}
    event["task"] = {}
    event["task"]["key"] = "test"

    with FlowContext(event, context) as fc:
        for rec in fc.assets:
            roll_key = rec["roll"]
            data = (
                nest_asset[nest_asset["roll"] == roll_key]
                .reset_index()
                .drop("index", 1)
            )
            key = roll_key.replace(": ", "-")
            fc.apply("evaluate_rolls", eval_roll, data, key=key)


# reducer should join on things about the roll itself and save some data on s3 and sink to snowpipe
def reducer(event, context=None):
    def try_read(f, s3):
        try:
            return s3.read(f)
        except:
            return pd.DataFrame()

    def ingest_entity(data, e, dedup_key, date_range=None):
        s3.write_date_partition(
            data, dedup_key=dedup_key, date_partition_key="created_at", entity=e
        )
        S3_ENTITY_ROOT = f"s3://res-data-platform/entities/{e}/"
        snowflake.ingest_from_s3_path(S3_ENTITY_ROOT, e)

    with FlowContext(event, context) as fc:
        snowflake = fc.connectors["snowflake"]
        s3 = fc.connectors["snowflake"]
        data = pd.concat(
            [
                try_read(f, s3)
                for f in s3.ls(
                    "s3://res-data-development/flows/v1/meta_nesting_rolls/v0/evaluate_rolls/"
                )
            ]
        )

        ingest_entity(data, "roll_nest_evaluations", "roll")

        files = [
            f
            for f in s3.ls(f"s3://res-data-platform/samples/data/rolls/")
            if "nest-level-evaluation.parquet" in f
        ]
        data = pd.concat([s3.read(f) for f in files])

        ingest_entity(data, "nest_evaluations", "key")
