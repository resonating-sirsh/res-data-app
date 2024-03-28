# progressive nesting approach where we just take pieces off kafka as they come along and try to nest
# into blocks of some fixed size for each type of material (TODO -- figure out the sizes depending on demand rate)
import res
import json
import pandas as pd
import numpy as np
import datetime
from res.flows import flow_node_attributes, FlowContext
from res.flows.event_compression import compress_event
from res.learn.optimization.packing.annealed.progressive import (
    ProgressiveNode,
)
from res.utils import logger
from shapely.geometry import Polygon
from shapely.wkt import dumps, loads
from matplotlib import pyplot as plt
from .utils import transform_pieces_for_nesting, validate_shapes
from .pack_pieces import DPI_SETTING

INPUT_KAFKA_TOPIC = "res_meta.dxa.prep_pieces_responses"
OUTPUT_KAFKA_TOPIC = "res_make.res_nest.progressive_nests"
S3_URL = f"s3://{res.RES_DATA_BUCKET}/progressive_nests"
LEFTOVER_DF = f"{S3_URL}/pieces/leftover.feather"
KAFKA_CONSUMER_NAME = "pack_progressive"
PIECE_DF_COLUMNS = [
    "id",
    "one_number",
    "material_code",
    "piece_count",
    "piece_set_key",
    "piece_id",
    "piece_name",
    "piece_type",
    "s3_image_path",
    "s3_shape_path",
    "buffer",
    "cuttable_width",
    "stretch_x",
    "stretch_y",
    "created_at",
]

s3 = res.connectors.load("s3")


def _kafka_df_to_piece_df(df):
    if df.shape[0] == 0:
        return None
    # make the df be about pieces instead of ones with nested piece info.
    df = (
        df.drop(columns=["make_pieces", "pieces"])
        .join(df.make_pieces.explode())
        .reset_index(drop=True)
    )
    df = df.join(pd.DataFrame.from_records(df.make_pieces.values))
    df = df.join(pd.DataFrame.from_records(df.metadata.values))
    # kind of hackneyed but it is what it is: assuming parallel directory structure for image and shapes.
    df["s3_image_path"] = df.filename
    df["s3_shape_path"] = df.filename.apply(
        lambda f: f.replace("extract_parts", "extract_outlines").replace(
            ".png", ".feather"
        )
    )
    # vague priority ordering
    df.sort_values(["one_number", "piece_id"], inplace=True)
    logger.info(f"exploded df for {df.shape[0]} pieces")
    return df[PIECE_DF_COLUMNS]


def _get_kafka_pieces():
    kafka = res.connectors.load("kafka", group_id=KAFKA_CONSUMER_NAME)[
        INPUT_KAFKA_TOPIC
    ]
    kafka_df = kafka.consume(give_up_after_records=100)
    logger.info(f"got {kafka_df.shape[0]} ones from kafka")
    return _kafka_df_to_piece_df(kafka_df)


def _get_snowflake_pieces():
    # this is just for dev purposes so we dont have to care about kafka.
    snowflake = res.connectors.load("snowflake")
    data = snowflake.execute(
        f"""select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."PREP_PIECES_RESPONSES_QUEUE" where "created_at" > '2022-08-21';"""
    )
    msgs = data["RECORD_CONTENT"].apply(json.loads).values
    df = pd.DataFrame.from_records(msgs)
    logger.info(f"got {df.shape[0]} ones from snowflake")
    return _kafka_df_to_piece_df(df)


def _get_piece_df(source):
    """
    Gets all the pieces that we will try to nest on this iteration of nesting.
    The source is the kafka queue as well as whatever pieces are left over from last time.
    """
    if source == "snowflake":
        return _get_snowflake_pieces()
    else:
        return _get_kafka_pieces()


def _plot_nest(df, path=None):
    geoms = df.nested_geometry.apply(loads)
    x0, y0, x1, y1 = (
        df.min_nested_x.min(),
        df.min_nested_y.min(),
        df.max_nested_x.max(),
        df.max_nested_y.max(),
    )
    width = 5
    height = int(width * (y1 - y0) / (x1 - x0))
    plt.figure(figsize=(width, height))
    plt.ylim(y0, y1)
    plt.xlim(x0, x1)
    for g in geoms:
        plt.fill(*g.boundary.xy, color=(0.6, 0.6, 0.9))
        plt.plot(*g.boundary.xy, color=(0, 0, 0))
    if path:
        plt.savefig("nest.png", bbox_inches="tight")
        s3.upload("nest.png", path)
    else:
        plt.show()


def _output_nest(material, run_key, material_df, level, packing):
    pieces_shapes = packing.packed_shapes()
    df = (
        material_df[material_df.piece_id.isin(pieces_shapes.keys())]
        .copy()
        .reset_index()
    )
    df["packed_shape"] = df["piece_id"].apply(lambda i: pieces_shapes[i])
    df["geometry"] = df["shape"].apply(dumps)
    df["bounds"] = df["shape"].apply(lambda p: np.array(p.bounds))
    df["original_centroid"] = df["shape"].apply(lambda p: np.array(p.centroid))
    df["nested_centroid"] = df["packed_shape"].apply(lambda p: np.array(p.centroid))
    df["nested_geometry"] = df["packed_shape"].apply(dumps)
    df["nested_bounds"] = df["packed_shape"].apply(lambda p: np.array(p.bounds))
    df["nested_area"] = df["packed_shape"].apply(lambda p: p.area)
    df["translation"] = df.nested_centroid - df.original_centroid
    df = df.join(
        pd.DataFrame(
            df["nested_bounds"].to_list(),
            columns=[
                "min_nested_x",
                "min_nested_y",
                "max_nested_x",
                "max_nested_y",
            ],
        )
    )
    df["composition_x"] = df.min_nested_x + df.buffer
    df["composition_y"] = df.max_nested_y - df.buffer
    df = df.drop(columns=["shape", "packed_shape", "transformed_shape"])
    key = f"{run_key}-{material}-{level}"
    path = f"{S3_URL}/nests/{key}/nest.feather"
    s3.write(path, df)
    _plot_nest(df, f"{S3_URL}/nests/{key}/nest.png")
    logger.info(f"Wrote nest {key}")
    # also write to kafka
    msg = {
        "nest_key": key,
        "roll_key": "unkn",
        "roll_rank": 0,
        "nest_file_path": path,
        "material_code": material,
        "asset_count": df.one_number.nunique(),
        "piece_count": df.shape[0],
        "width_nest_yds": material_df.iloc[0].cuttable_width / 36.0,
        "asset_keys": df.one_number.unique().tolist(),
        "piece_ids": df.piece_id.unique().tolist(),
        "height_nest_yds": df.max_nested_y.max() / (36.0 * DPI_SETTING),
        "utilization": df.nested_area.sum()
        / (material_df.iloc[0].cuttable_width * DPI_SETTING * df.max_nested_y.max()),
    }
    res.connectors.load("kafka")[OUTPUT_KAFKA_TOPIC].publish(msg, use_kgateway=True)


def _prepare_for_nesting(material_df):
    n = material_df.shape[0]
    logger.info(f"Pieces: {n}")
    max_width = DPI_SETTING * material_df.iloc[0].cuttable_width
    logger.info(f"Nesting with max width {max_width}")
    logger.info(f"Loading geometry")
    material_df["shape"] = material_df.s3_shape_path.apply(
        lambda p: Polygon(s3.read(p)[["column_1", "column_0"]].values).convex_hull
    )
    material_df["transformed_shape"] = material_df.apply(
        lambda r: transform_pieces_for_nesting(
            [r["shape"]],
            r.stretch_x,
            r.stretch_y,
            r.buffer,
            max_width * DPI_SETTING,
        )[0],
        axis=1,
    )
    material_df["transformed_area"] = material_df["transformed_shape"].apply(
        lambda p: p.area
    )
    material_one_df = (
        material_df.groupby("one_number")
        .agg({"transformed_shape": list, "piece_id": list, "transformed_area": sum})
        .reset_index()
    )
    return material_one_df, max_width


def _build_forest(layers, max_height=100000, max_depth=3):
    logger.info(f"building forest from {len(layers[0])} trees")
    res = []
    i = 0
    leaves = layers[0]
    while i < len(leaves):
        if leaves[i].packing.height() < max_height and i < len(leaves) - 1:
            res.append(ProgressiveNode.from_nodes(leaves[i : (i + 2)]))
            i += 2
        else:
            res.append(leaves[i])
            i += 1
    if len(res) < len(leaves) and len(res) > 1 and max_depth > 0:
        return _build_forest([res] + layers, max_height, max_depth - 1)
    else:
        return layers


def _pack_material(material, material_df):
    run_key = datetime.datetime.now().strftime("%Y-%m-%d-%H-%m-%S")
    logger.info(f"Nesting {material}")
    material_one_df, max_width = _prepare_for_nesting(material_df)
    # TODO priority ordering.
    material_one_df.sort_values("one_number", inplace=True)
    leaf_nests = []
    logger.info(f"Building leaf nests for {material_one_df.shape[0]} ones")
    for _, r in material_one_df.iterrows():
        try:
            validate_shapes(r.transformed_shape, max_width)
            shapes = dict(zip(r.piece_id, r.transformed_shape))
            leaf_nest = ProgressiveNode.from_pieces(shapes, max_width)
            leaf_nest.packing.compress()
            leaf_nests.append(leaf_nest)
        except ValueError as ex:
            # TODO: kick back to piece extraction
            logger.info(f"ONE is screwed {r.one_number} {repr(ex)}")
            continue
    # TODO: logic for knits so we dont nest stuff together.
    forest = _build_forest([leaf_nests])
    # TODO: output the whole forest so that we can choose what length we want.
    for height, level in enumerate(reversed(forest)):
        for node in level:
            _output_nest(material, run_key, material_df, height, node.packing)


@flow_node_attributes(
    "pack_progressive.generator",
)
def generator(event, context={}):
    with FlowContext(event, context) as fc:
        piece_df = _get_piece_df(fc.args.get("source", "kafka"))
        if piece_df is not None:
            logger.info(f"got {piece_df.shape[0]} pieces")
            materials = piece_df.material_code.unique()
            output = []
            for material in materials:
                assets_path = f"{S3_URL}/mapred/{material}.feather"
                s3.write(
                    assets_path,
                    piece_df[piece_df.material_code == material].reset_index(drop=True),
                )
                payload_i = dict(event)
                payload_i["assets"] = []
                payload_i["args"]["assets_path"] = assets_path
                payload_i["args"]["material_code"] = material
                output.append(payload_i)
            logger.info(f"nesting for {len(output)} materials")
            return output
        else:
            logger.info(f"No pieces to nest")
            return []


@flow_node_attributes("pack_progressive.handler", memory="8G", cpu="7500m")
def handler(event, context):
    # we will just kick this thing off periodically with a null payload for now - the actual data comes from kafka.
    # the basic idea is that pieces can be in 3 places:
    # - the kafka queue coming out of piece preparation
    # - in a partial nest thats not big enough yet
    # - in a nest which has been emitted
    # to make this robust we can have a proper db for piece location but for now the interesting part is just the act of nesting progressively
    # rather than hardening of the data storage.
    with FlowContext(event, context) as fc:
        material = fc.args.get("material_code")
        piece_df = s3.read(fc.args["assets_path"])
        _pack_material(material, piece_df)


@flow_node_attributes("pack_progressive.reducer")
def reducer(event, context={}):
    return {}
