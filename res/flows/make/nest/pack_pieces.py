import res
import os
import pandas as pd
import numpy as np
import tempfile
from res.flows import flow_node_attributes, FlowContext
from res.utils import logger
from res.learn.optimization.packing.annealed import pack_annealed
from res.learn.optimization.packing.annealed.nn import load_model as load_nesting_nn
from shapely.wkt import dumps
from shapely.ops import unary_union
from .utils import (
    transform_pieces_for_nesting,
    validate_nesting,
    validate_shapes,
    notify_callback,
    image_offset,
    flag_asset,
    piece_label_angle,
    upload_qr_pieces,
    piece_df_to_polygon,
)

DPI_SETTING = 300
DEFAULT_WIDTH_INCHES = 55
DEFAULT_HEIGHT = 48000 * 3
DEFAULT_BUFFER = 75  # In pixels (i.e., inches / DPI) -> SA changing to match the nesting default used today
DEFAULT_STRETCH_X = 1.0
DEFAULT_STRETCH_Y = 1.0
METHOD_HEURISTIC = "heuristic"
METHOD_ANNEALED = "annealed"

s3 = res.connectors.load("s3")


def _pack_pieces(shapes, groups=None, **kwargs):

    width = kwargs.get("output_bounds_width", DPI_SETTING * DEFAULT_WIDTH_INCHES)
    height = kwargs.get("output_bounds_length", DEFAULT_HEIGHT)
    max_vertical_strip_width = kwargs.get("max_vertical_strip_width", None)
    max_packing_iters = kwargs.get("max_packing_iters", 50000)
    threads = kwargs.get("threads", 8)

    stretch_x = kwargs.get("stretch_x", DEFAULT_STRETCH_X)
    stretch_y = kwargs.get("stretch_y", DEFAULT_STRETCH_Y)
    buffer = kwargs.get("buffer", DEFAULT_BUFFER)

    method = kwargs.get("packing_method", METHOD_HEURISTIC)

    logger.debug(f"Nesting {len(shapes)} shapes")
    logger.debug(f"Width: {width}")
    logger.debug(f"Max vertical strip width: {max_vertical_strip_width}")
    logger.debug(f"Height: {height}")
    logger.debug(f"Buffer: {buffer}")
    logger.debug(f"Method: {method}")
    logger.debug(f"stretch_x: {stretch_x}")
    logger.debug(f"stretch_y: {stretch_y}")
    logger.debug(f"max_packing_iters: {max_packing_iters}")

    shapes = transform_pieces_for_nesting(shapes, stretch_x, stretch_y, buffer, width)

    max_piece_width = max((s.bounds[2] - s.bounds[0] for s in shapes))
    if max_piece_width > width:
        raise ValueError(
            f"Trying to nest piece with width {max_piece_width} into roll with width {width}"
        )

    if max_vertical_strip_width is not None:
        max_vertical_strip_width *= stretch_x
        if max_piece_width > max_vertical_strip_width:
            logger.warn(
                f"Fusing piece with width {max_piece_width} exceeds block fuser width {max_vertical_strip_width} -- abandoning vertical strip plan"
            )
            max_vertical_strip_width = None

    validate_shapes(shapes, width, height)

    if method == "NN":
        packed_shapes = load_nesting_nn().nest_boxes_legacy(shapes, width)
    else:
        packed_shapes = pack_annealed(
            shapes,
            groups if groups is not None else [None] * len(shapes),
            width,
            height,
            max_vertical_strip=max_vertical_strip_width,
            max_iters=max_packing_iters,
            threads=threads,
        )

    nested_shapes = [n.translated for n in packed_shapes]

    validate_nesting(shapes, nested_shapes, width, height)

    return nested_shapes


def _publish_evaluation(nested_shapes, job_key, fc):
    try:

        TOPIC = "res_make.res_nest.nest_evaluations"
        logger.info(f"publishing nest evaluation to {TOPIC}")

        PIXELS_TO_YARDS = 1 / (DPI_SETTING * 36)

        max_height = max(s.bounds[3] for s in nested_shapes)
        max_width = fc.args.get("output_bounds_width")
        shape_area = sum(s.area for s in nested_shapes)

        result = {
            "job_key": job_key,
            "asset_count": len(fc.assets),
            "created_at": res.utils.dates.utc_now_iso_string(),
            "height_nest_yds": max_height * PIXELS_TO_YARDS,
            "width_nest_yds": max_width * PIXELS_TO_YARDS,
            "area_nest_yds": max_height * max_width * PIXELS_TO_YARDS**2,
            "area_pieces_yds": shape_area * PIXELS_TO_YARDS**2,
            "area_nest_utilized_pct": shape_area / (max_height * max_width),
            "packing_method": fc.args.get("packing_method", METHOD_HEURISTIC),
            "piece_count": len(fc.assets),
            "material": fc.args.get("material", ""),
        }

        fc.connectors["kafka"][TOPIC].publish(result, coerce=True, use_kgateway=True)

        return result
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to evaluate and publish nest (to the kafka topic) {repr(ex)}"
        )


def _build_asset_df(fc):
    """
    Load the passed in assets and add some additional info to the dataframe for ourselves
    and for our posterity.
    """
    df = fc.assets_dataframe
    if "piece_group" not in df.columns:
        df["piece_group"] = None
    # hack in some lame pieces to try to identify the thing.
    job_key = fc.args.get("job_key")
    qr_code_image_path = fc.get_node_path_for_key("nest", job_key) + "/qr_code.png"
    qr_code_shape_path = fc.get_node_path_for_key("nest", job_key) + "/qr_code.feather"
    upload_qr_pieces(job_key, qr_code_image_path, qr_code_shape_path, 5 * DPI_SETTING)
    df["one_index"] = df["asset_key"]
    identification_pieces = {
        "asset_key": [""] * 2,
        "one_index": ["0", str(int(df["one_index"].max()) + 1)],
        "piece_group": ["anchor_bottom", "anchor_top"],
        "piece_type": [df.iloc[0].piece_type] * 2,
        "s3_shape_path": [qr_code_shape_path] * 2,
        "s3_image_path": [qr_code_image_path] * 2,
    }
    df = pd.concat(
        [
            pd.DataFrame(identification_pieces),
            df,
        ]
    ).reset_index(drop=True)
    # kind of lame to dupe this info but it lets us have a completely self contained nesting description.
    df["stretch_x"] = fc.args.get("stretch_x", DEFAULT_STRETCH_X)
    df["stretch_y"] = fc.args.get("stretch_y", DEFAULT_STRETCH_Y)
    df["buffer"] = fc.args.get("buffer", DEFAULT_BUFFER)
    df["output_bounds_width"] = fc.args.get(
        "output_bounds_width", DPI_SETTING * DEFAULT_WIDTH_INCHES
    )
    df["output_bounds_length"] = fc.args.get("output_bounds_length", DEFAULT_HEIGHT)
    df["material"] = fc.args.get("material", "")
    return df


def _get_image_bounds(s3_path):
    """
    Mirror what we get if we just read this thing into an np array and ask for the shape.
    Which is: height, width, channels
    """
    from pyvips import Image as vips_image

    with tempfile.NamedTemporaryFile(suffix=".png") as temp:
        s3._download_to_file(s3_path, temp)
        temp.flush()
        image = vips_image.pngload(temp.name, access="sequential")
        return np.array([image.height, image.width, image.bands])


def _rectify_labels(df):
    """
    Takes ones corresponding to labels and make all their pieces the same shape -- assuming they are close enough.
    """
    piece_counts = df.asset_key.value_counts()
    ones_to_rectify = piece_counts[piece_counts == 50].index
    logger.info(f"Adjusting shapes for ones which are labels: {ones_to_rectify}")
    hulls = (
        df[df.asset_key.isin(ones_to_rectify)]
        .groupby(["asset_key"])["shape"]
        .apply(lambda g: unary_union(g).convex_hull)
        .reset_index()
        .rename(columns={"shape": "hull"})
        .set_index("asset_key")
    )
    df = df.set_index("asset_key").join(hulls, how="left").reset_index()
    ind = df["hull"].notnull()
    df[ind]["shape"] = df[ind].apply(
        lambda r: r["hull"]
        if (r["hull"] - r["shape"]).area / r["shape"].area < 0.05
        else r["shape"],
        axis=1,
    )
    return df.drop(columns=["hull"])


@flow_node_attributes(
    "pack_pieces",
    description="Pack geometry in a way which tries to maximize utilization of the roll.",
    memory="12G",  # TODO: make memory usage variable depending on payload size.
    cpu="7500m",
    mapped=False,
    allow24xlg=True,
)
def handler(event, context):
    """
    Performs nesting.
    The event should have:
    "assets": [
        # each entry represents a piece which needs nestiong -- there could be duplicates if the nest has multiple of the same body etc.
        {
            "s3_image_path": fully fledged s3 path where the image file is.
            "s3_shape_path": fully fledged s3 path where the geometry file is -- the geometry should be a data frame with x,y coordinates in
                       column_1, column_0 respectively
        }, ...
    ]
    "args": {
        "stretch_x": how much to stretch the shapes along the x axis
        "stretch_y": how much to stretch the shapes along the x axis
        "output_bounds_width": max width of the roll in pixels (i.e., 300ths of an inch)
        "output_bounds_height": max height of the roll in pixels (i.e., 300ths of an inch)
        "buffer": how much of a buffer to put around the pieces (in terms of 300ths of an inch)
        "job_key": a name for this job that we can use to refer to this nest when its time to actually composite the nest.
    }
    """
    with FlowContext(event, context) as fc:
        job_key = fc.args.get("job_key")
        df = _build_asset_df(fc)
        logger.info("Loading Geometry")
        df["shape"] = df.s3_shape_path.apply(lambda p: piece_df_to_polygon(s3.read(p)))
        max_packing_iters = 50000
        # deal with labels going into the block fuser -- rectify the geometry so things nest easier and also
        # do a special nesting alg.
        block_fusing_labels = (
            df.iloc[0].piece_type == "block_fuse"
            and df.asset_key.value_counts().max() >= 50
        )
        if block_fusing_labels:
            # the labels themselves are usually all slightly different shapes in the geometry files
            # so replace them all with some bounding box that will nest easily.
            df = _rectify_labels(df)
            # in addition - just add pieces to the roll in asset id order (arbitrary) since they will form a nice grid
            # this is what max_packing_iters = 0 will do -- just preserve the input order rather than doing any annealing.
            df = df.sort_values("one_index").reset_index(drop=True)
            max_packing_iters = 0
        logger.info("Packing Pieces")
        df["packed_shape"] = _pack_pieces(
            df["shape"],
            df["piece_group"].where(pd.notnull(df["piece_group"]), None),
            **fc.args,
            max_packing_iters=max_packing_iters,
        )
        logger.info("Checking image bounds")
        # load the images to (a) ensure that the geometry is valid and (b) figure out what the offsets should be.
        df["image_bounds"] = df.s3_image_path.apply(_get_image_bounds)
        df["compensated_image_bounds"] = df.image_bounds.apply(
            lambda b: b
            * np.array(
                [
                    fc.args.get("stretch_y", DEFAULT_STRETCH_Y),
                    fc.args.get("stretch_x", DEFAULT_STRETCH_X),
                    1,
                ]
            )
        )
        asset_errors = []
        for idx, row in df.iterrows():
            ny, nx, _ = row.compensated_image_bounds
            x0, y0, x1, y1 = row.packed_shape.bounds
            height = y1 - y0
            width = x1 - x0
            if height < ny or width < nx:
                flag_reason = f"Invalid asset {idx} with compensated shape size {width, height} and image size {nx, ny} {row.s3_shape_path, row.s3_image_path}"
                asset_errors.append(flag_reason)
                if os.environ["RES_ENV"] == "production":
                    flag_asset(
                        row.asset_key,
                        row.asset_id,
                        row.s3_image_path,
                        fc._task_key,
                        flag_reason,
                    )
        if len(asset_errors) > 0:
            raise ValueError(f"There were asset errors: {asset_errors}")

        logger.info("Dumping nest info")
        df["compositing_offset"] = df[
            ["packed_shape", "compensated_image_bounds"]
        ].apply(
            lambda r: image_offset(r.packed_shape, r.compensated_image_bounds), axis=1
        )
        df["geometry"] = df["shape"].apply(dumps)
        df["bounds"] = df["shape"].apply(lambda p: np.array(p.bounds))
        df["original_centroid"] = df["shape"].apply(lambda p: np.array(p.centroid))
        df["nested_centroid"] = df["packed_shape"].apply(lambda p: np.array(p.centroid))
        df["nested_label_angle"] = df["packed_shape"].apply(piece_label_angle)
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
        df["composition_x"] = df.min_nested_x + df.compositing_offset.apply(
            lambda x: x[0]
        )
        df["composition_y"] = df.max_nested_y - df.compositing_offset.apply(
            lambda x: x[1]
        )
        roll_length = max(shape.bounds[3] for shape in df["packed_shape"])
        logger.info(f"Nested {df.shape[0]} shapes into a roll of length {roll_length}")
        _publish_evaluation(df["packed_shape"], job_key, fc)
        df = df.drop(columns=["shape", "packed_shape"])
        fc._write_output(df, "nest", job_key, output_filename="output_verbose.feather")
        # only what is needed for compositing - to save on memory usage.
        df = df[
            [
                "s3_image_path",
                "composition_x",
                "composition_y",
                "stretch_x",
                "stretch_y",
                "output_bounds_width",
                "output_bounds_length",
                "material",
                "asset_id",
                "asset_key",
                "sku",
                "piece_type",
                "min_nested_y",
                "max_nested_y",
                "nested_area",
                "nested_centroid",
                "nested_label_angle",
            ]
        ]
        fc._write_output(df, "nest", job_key, output_filename="output.feather")
        nesting_callback = fc.args.get("on_nesting_solution_notify")
        if nesting_callback is not None:
            logger.info("Finished nesting -- notifying callback")
            # note that we are lying to the bot and saying that a printfile was created which will no longer happen until the stitching phase.
            notify_callback(
                df,
                fc.args.get("job_key"),
                nesting_callback,
                succeeded=True,
                printfile_path="TODO",
                nest_df_path=fc.get_node_path_for_key("nest", job_key),
            )


def on_failure(event, context):
    with FlowContext(event, context) as fc:
        nesting_callback = fc.args.get("on_nesting_solution_notify")
        job_key = fc.args.get("job_key")
        if nesting_callback is not None:
            logger.info("Failed nesting -- notifying callback")
            notify_callback(
                _build_asset_df(fc),
                job_key,
                nesting_callback,
                succeeded=False,
            )


def on_success(event, context):
    pass
