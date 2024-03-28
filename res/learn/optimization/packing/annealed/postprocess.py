# things we can do to the nest after optimus is happy with it without affecting the constraints of the solution.
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
from res.utils import logger
from shapely.affinity import translate
from shapely.wkt import loads, dumps
from shapely.ops import unary_union
from shapely.geometry import box, Point
from ..rasterized.canvas import Canvas, Piece
from ...packing.packing import lower_left


def postprocess_nest_df(nest_df):
    nest_df = duplicate_small_pieces(nest_df)
    return nest_df


def make_row(row, x, y, piece_id):
    d = dict(row)
    d["piece_id"] = piece_id
    p = loads(d["nestable_wkt"])
    x0, y0, _, _ = p.bounds
    n = translate(p, x - x0, y - y0)
    d["nested_geometry"] = dumps(n)
    d["nested_bounds"] = np.array(n.bounds)
    (
        d["min_nested_x"],
        d["min_nested_y"],
        d["max_nested_x"],
        d["max_nested_y"],
    ) = n.bounds
    d["nested_centroid"] = np.array(n.centroid)
    d["composition_x"] = d["min_nested_x"] + d["buffer"]
    d["composition_y"] = d["max_nested_y"] - d["buffer"]
    return pd.DataFrame.from_records([d])


def duplicate_small_pieces(nest_df):
    """
    Try to fill in gaps in the nest by just duplicating whatever the smallest pieces are - since they have a tendency
    to get lost when cutting.
    """
    candidates = (
        nest_df[
            pd.notna(nest_df.asset_id)
            & nest_df.piece_id.apply(lambda p: "duplicate" not in p)
        ]
        .sort_values("nested_area")
        .reset_index()
    )

    logger.info(
        f"Attempting to duplicate small pieces for nest with {candidates.shape[0]} candidates."
    )

    canvas = Canvas.from_nest_df(nest_df)

    for i in range(min(candidates.shape[0], 15)):
        cid = candidates.iloc[i]["index"]
        canvas.try_add_piece_inplace(
            Piece.from_wkt(cid, nest_df.iloc[cid].nestable_wkt)
        )

    if len(canvas.pieces) > 0:
        duplicated = pd.concat(
            [
                make_row(
                    nest_df.iloc[piece.id],
                    x * canvas.cell_size,
                    y * canvas.cell_size,
                    nest_df.iloc[piece.id]["piece_id"] + "::duplicate",
                )
                for piece, x, y in canvas.pieces
            ]
        )
        logger.info(f"Duplicated {duplicated.shape[0]} pieces.")
        return pd.concat([nest_df, duplicated]).reset_index(drop=True)
    else:
        logger.info(f"Failed to duplicate pieces.")
        return nest_df


def add_patches(nest_df):
    patch_row = dict(nest_df.iloc[0])
    sx = nest_df.iloc[0].stretch_x
    sy = nest_df.iloc[0].stretch_y
    buf = nest_df.iloc[0].buffer
    nested_geom = unary_union(nest_df["nested_geometry"].apply(loads).values)
    patch_height = 900 * sy  # +2*buf
    patch_width = 900 * sx  # +2*buf
    patch_row["buffer"] = 0
    nest_bounds = box(
        0,
        0,
        nest_df.iloc[0].output_bounds_width - patch_width,
        nest_df.max_nested_y.max() - patch_height,
    )
    patch = box(0, 0, patch_width, patch_height)
    patch_row["nestable_wkt"] = dumps(patch)
    maybe_feasible = nest_bounds - unary_union(
        [
            translate(nested_geom, -i * patch_width / 10, -j * patch_height / 10)
            for i in range(11)
            for j in range(11)
        ]
    )
    positions = []
    while maybe_feasible.area > 0:
        ll = lower_left(maybe_feasible)
        tp = translate(patch, *ll)
        if tp.intersection(nested_geom).area == 0:
            nested_geom = unary_union([nested_geom, tp])
            positions.append(ll)
            maybe_feasible -= unary_union(
                [
                    translate(tp, -i * patch_width, -j * patch_height)
                    for i in range(2)
                    for j in range(2)
                ]
            )
        else:
            maybe_feasible -= Point(*ll).buffer(150)
    logger.info(f"Added {len(positions)} patches. {positions}")
    if len(positions) > 0:
        patch_rows = pd.concat(
            [
                make_row(
                    patch_row,
                    x,
                    y,
                    f"patch_{i}",
                )
                for i, (x, y) in enumerate(positions)
            ]
        )
        # un set some stuff.
        for field in [
            "node_key",
            "asset_id",
            "asset_key",
            "piece_name",
            "piece_description",
            "zone",
            "fusing_type",
            "cutline_uri",
            "wrapped_piece_ids",
            "wrapped_piece_codes",
            "artwork_uri",
            "piece_info",
            "thread_colors",
            "rank",
            "is_healing",
            "sku",
            "geometry",
            "body_code",
        ]:
            patch_rows[field] = None
        # randomize artwork
        patch_rows["s3_image_path"] = (
            nest_df[~nest_df["asset_id"].isna()][["s3_image_path"]]
            .sample(len(positions), replace=True)
            .values
        )
        return pd.concat([nest_df, patch_rows]).reset_index(drop=True)
    else:
        return nest_df


def plot_postprocessed(nest_df):
    width = nest_df.iloc[0].output_bounds_width
    height = nest_df.iloc[0].total_nest_height
    plt.figure(figsize=(3, 3 * height / width))
    plt.ylim(0, height)
    plt.xlim(0, width)
    for _, r in nest_df.iterrows():
        poly = loads(r.nested_geometry)
        plt.fill(
            *poly.boundary.xy,
            color=(0.6, 0.9, 0.6) if "duplicate" in r.piece_id else (0.6, 0.6, 0.9),
        )
        plt.plot(
            *poly.boundary.xy,
            color=(0.3, 0.45, 0.3) if "duplicate" in r.piece_id else (0.3, 0.3, 0.45),
        )
