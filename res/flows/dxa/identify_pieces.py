import res
from res.utils import logger
from res.media.images.providers import dxf as res_dxf
from res.media.images import geometry
from res.media.images import outlines

from dataclasses import dataclass
import json
from operator import itemgetter
import os

import pandas as pd
from shapely.geometry import Polygon
from shapely import affinity, wkb
from shapely.affinity import translate
import numpy as np
from numpy import asarray
from scipy import ndimage
from PIL import Image, ImageDraw, ImageFilter
import cv2 as cv

# from mpl_toolkits.axes_grid1 import ImageGrid
import matplotlib.pyplot as plt


S3_BUCKET = "meta-one-assets-prod"
S3_BUCKET_PREFIX = "color_on_shape"
s3 = res.connectors.load("s3")


@dataclass
class PieceComparison:
    s3_path: str
    dxf_unique_key: str
    metric: str
    metric_val: float

    def __lt__(self, other):
        return self.metric_val < other.metric_val


##### <>Experimental Matching Via Buffering (and helpers)<>
# Note: These aren't used for hausdorff matching. They're used for
# a method of matching that showed some promise for pieces with buffer.


def shift_to_match_centroids(p_from: Polygon, p_to: Polygon):
    xoff = p_to.centroid.coords[0][0] - p_from.centroid.coords[0][0]
    yoff = p_to.centroid.coords[0][1] - p_from.centroid.coords[0][1]
    return translate(p_from, xoff=xoff, yoff=yoff)


def poly_can_contain(outer: Polygon, inner: Polygon):
    # Determines whether the outer poly can contain the inner,
    # when the inner is shifted inside outer

    return (
        not outer.is_empty
        and not inner.is_empty
        and outer.contains(shift_to_match_centroids(inner, outer))
    )


def buffer_poly(p, dist):
    """Adds buffer to a polygon"""
    # cap_style and join_style 1 produce rounded (bevelled) edges. We could consider using jagged
    return p.buffer(dist, cap_style=1, join_style=1, mitre_limit=5.0)


def calc_min_containing_buffer(
    unbuffered: Polygon, buffered: Polygon, max_calc_iterations=10, max_buffer=1024
):
    """
    Calculates the minimum containing buffer such that `unbuffered` contains `buffered`
    """
    #
    trans_unbuff = shift_to_match_centroids(unbuffered, buffered)

    # binary search to find min buffer so that unbuffered contains buffered
    floor = 0
    ceil = max_buffer
    for i in range(max_calc_iterations):
        test_buffer = (ceil + floor) // 2
        if buffer_poly(trans_unbuff, test_buffer).contains(buffered):
            ceil = test_buffer
        else:
            floor = test_buffer
    return ceil


def match_by_containing_buffer(imgs, dxf_pieces, matches):
    imgs_unmatched = imgs[~imgs["s3_path"].isin(matches["s3_path"])].sort_values(
        by="poly_area"
    )
    dxfs_unmatched = dxf_pieces[
        ~dxf_pieces["unique_key"].isin(matches["dxf_unique_key"])
    ].sort_values(by="poly_area")

    # Iter through all imgs, (start with smallest img) find those dxfs that it could contain
    # (based on area, and translated contains), and for those, find min shrink-wrap distance
    shrinkwrap_comps = []
    for img in imgs_unmatched[["s3_path", "poly", "poly_area"]].values:
        img_s3_path, img_poly, img_poly_area = img
        for dxf in dxfs_unmatched[["unique_key", "poly", "poly_area"]].values:
            dxf_key, dxf_poly, dxf_poly_area = dxf
            if img_poly_area > dxf_poly_area and poly_can_contain(img_poly, dxf_poly):
                shrinkwrap_dist = calc_min_containing_buffer(dxf_poly, img_poly)
                if shrinkwrap_dist is not None:
                    comp = PieceComparison(
                        s3_path=img_s3_path,
                        dxf_unique_key=dxf_key,
                        metric="shrinkwrap",
                        metric_val=shrinkwrap_dist,
                    )
                    shrinkwrap_comps.append(comp)

    # Now assign them
    MAX_SHRINKWRAP_DIST = 500

    matched_dxf_piece_keys = set()
    matched_s3_paths = set()

    accepted_matches = []

    shrinkwrap_comps.sort()  # lowest metric_vals first
    for comp in shrinkwrap_comps:
        if comp.s3_path not in matched_s3_paths and (
            comp.metric_val < MAX_SHRINKWRAP_DIST
        ):
            matched_s3_paths.add(comp.s3_path)
            accepted_matches.append(comp)

    return pd.DataFrame(accepted_matches).drop_duplicates("s3_path", keep="first")


##### </> Experimental Matching Via Buffering


def _plot_shapes(shapes):
    for i, shape in enumerate(shapes):
        x, y = shape.exterior.xy
        plt.fill(x, y, alpha=0.5, fc="blue", ec="none")
        plt.axis("equal")
        plt.plot(
            x,
            y,
            color="blue",
            linewidth=0.5,
        )
        plt.title("Detected shapes")
    plt.show()


def get_s3_image_outlines(s3_path: str) -> pd.DataFrame:
    s3 = res.connectors.load("s3")
    logger.info(f"Retrieving images from {s3_path}")
    polys = []
    s3_paths = [p["path"] for p in s3.ls_info(s3_path) if p["path"].endswith(".png")]
    for p in s3_paths:
        im_array = s3.read(p)
        logger.info(f"Retrieved {p}")
        outline_parts = list(outlines.part_outlines([im_array]))
        polys.append(Polygon(outline_parts[0]))
    imgs = pd.DataFrame()

    return imgs.assign(s3_path=s3_paths, poly=polys)


def s3_body_path(body, color, size_code, version):
    return f"s3://{S3_BUCKET}/{S3_BUCKET_PREFIX}/{body}/{version}/{color}/{size_code}/pieces/"


def get_s3_dxf(s3_path, size) -> res_dxf.DxfFile:
    logger.info(f"Retrieving dxf from {s3_path}")
    dxf = s3.read(s3_path)

    dxf_pieces = dxf.generate_piece_image_outlines(size=size)
    dxf_pieces["poly"] = dxf_pieces.apply(
        lambda r: Polygon(r["outline_dpi_300"] if r["outline_dpi_300"].any() else []),
        axis="columns",
    )
    dxf_pieces["poly_area"] = dxf_pieces.apply(lambda r: r["poly"].area, axis="columns")

    # TODO: Figure out a scheme for naming these dxf piece keys.
    # Also, this doesn't really work in the case when dxf pieces are duped multiple times
    dxf_pieces["unique_key"] = dxf_pieces.apply(
        lambda r: r["key"] + ("__M" if r["is_mirror"] else ""), axis="columns"
    )
    return dxf_pieces


def match_by_area(imgs: pd.DataFrame, dxf_pieces: pd.DataFrame) -> pd.DataFrame:
    from sklearn.neighbors import BallTree

    tree = BallTree(dxf_pieces[["poly_area"]].values)

    imgs["poly_area"] = imgs.apply(lambda r: r["poly"].area, axis="columns")

    AREA_MATCH_THRESHOLD = (
        imgs[["poly_area"]].mean() * 0.05
    )  # half a percent difference in area

    # Query for similar areas
    imgs["dxf_area_matches"] = tree.query_radius(
        imgs[["poly_area"]], r=AREA_MATCH_THRESHOLD
    )

    # Build a list of hausdorff comparisons between dxf and imgs
    comparisons = []
    for img in imgs[["s3_path", "dxf_area_matches", "poly"]].values:
        s3_path, dxf_match_indexes, poly = img
        for dxf_i in dxf_match_indexes:
            dxf_piece = dxf_pieces.iloc[int(dxf_i)]
            h_dist = dxf_piece["poly"].hausdorff_distance(poly)
            # NOTE: we need to use the unique key for the dxf here (which includes mirrored)
            comparisons.append(
                PieceComparison(
                    s3_path=s3_path,
                    dxf_unique_key=dxf_piece["unique_key"],
                    metric="hausdorff_distance",
                    metric_val=h_dist,
                )
            )

    ### Compute Unique Matches for s3_img -> dxf comparisons that are similar enough in area
    # Anecdotally, good matches are < 50, even in the case where one piece has notches
    # and the other doesn't
    HAUSDORFF_MATCH_THRESHOLD = 50

    matched_dxf_piece_keys = set()
    matched_s3_paths = set()

    accepted_matches = []
    duplicated_matches = []

    comparisons.sort()  # lowest (ie closest match) scores first
    for comp in comparisons:
        if (comp.metric_val < HAUSDORFF_MATCH_THRESHOLD) and (
            comp.s3_path not in matched_s3_paths
        ):
            matched_s3_paths.add(comp.s3_path)
            if comp.dxf_unique_key not in matched_dxf_piece_keys:
                matched_dxf_piece_keys.add(comp.dxf_unique_key)
                accepted_matches.append(comp)
            else:
                duplicated_matches.append(comp)

    logger.info(
        f"Finished Hausdorff Match. Found {len(accepted_matches)} unique matches."
    )
    if len(duplicated_matches) > 0:
        logger.warn(
            f"Found duplicate matches. Tried to match s3 -> dxf piece, but dxf piece was already taken: {duplicated_matches}"
        )
    matches = pd.DataFrame(accepted_matches)

    # add in the non-unique dxf piece key
    return matches.join(
        dxf_pieces.set_index("unique_key")[["key"]].rename(columns={"key": "dxf_key"}),
        on="dxf_unique_key",
    ).drop_duplicates()


def gen_artifacts(
    matches: pd.DataFrame, imgs: pd.DataFrame, dxf_pieces, output_dir="./artifacts"
):
    """
    Generates output files for eventual upload to s3.

    returns: List of paths for artifacts to upload, including
        `piece_mappings.json` : json dictionary mapping from s3 path to dxf piece name
        `full_matches_output.feather`: dataframe with matches and polygons to help debugging
    """
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)

    matches_mapping = {
        s3_path: dxf_key for s3_path, dxf_key in matches[["s3_path", "dxf_key"]].values
    }
    matches_output_path = os.path.join(output_dir, "piece_mappings.json")
    with open(matches_output_path, "a") as f:
        json.dump(matches_mapping, f)
        # f.write(json.dump(matches_mapping), matches_output_path)

    # Make one big outer joined dataframe to determine what's been matched,
    # as well as dxf and s3 images that haven't been matched.
    full_outer_matches = matches.join(
        imgs.set_index("s3_path")[["poly"]].rename(columns={"poly": "img_poly"}),
        on="s3_path",
        how="outer",
    ).join(
        dxf_pieces.set_index("unique_key")[["poly"]].rename(
            columns={"poly": "dxf_poly"}
        ),
        on="dxf_unique_key",
        how="outer",
    )

    # Convert polys to wkb so we can featherize them
    # TODO: maybe this should live in the s3 connector?
    full_outer_matches = full_outer_matches.assign(
        img_poly=full_outer_matches.apply(
            lambda r: r["img_poly"].wkb if r[["img_poly"]].any() else None,
            axis="columns",
        ),
        dxf_poly=full_outer_matches.apply(
            lambda r: r["dxf_poly"].wkb if r[["dxf_poly"]].any() else None,
            axis="columns",
        ),
    ).reset_index()

    full_matches_output_path = os.path.join(output_dir, "full_matches_output.feather")
    full_outer_matches.to_feather(full_matches_output_path)

    return [matches_output_path, full_matches_output_path]


def hydrate_matched_df(path: str):
    matches = s3.read(path)
    return matches.assign(
        dxf_poly=matches.apply(
            lambda r: wkb.loads(r["dxf_poly"]) if r["dxf_poly"] else None,
            axis="columns",
        ),
        img_poly=matches.apply(
            lambda r: wkb.loads(r["img_poly"]) if r["img_poly"] else None,
            axis="columns",
        ),
    )


def handler(event, context):
    args = event["args"]
    body, dxf_file_s3_path, dxf_size, version, color, size_code = itemgetter(
        "body", "dxf_file_s3_path", "dxf_size", "version", "color", "size_code"
    )(args)

    s3_dir = s3_body_path(body, color, size_code, version or "v1")

    s3_imgs: pd.DataFrame = get_s3_image_outlines(s3_dir)
    if len(s3_imgs) == 0:
        logger.error("No body pieces found for {body}")
        return

    dxf_pieces: pd.DataFrame = get_s3_dxf(dxf_file_s3_path, dxf_size)

    area_matches = match_by_area(s3_imgs, dxf_pieces)

    # FYI, I think we may still want this matching for buffered or (eventually) warped pieces
    # So I'll leave it commented for now.
    #
    # buffer_matches = match_by_containing_buffer(s3_imgs, dxf_pieces, area_matches)
    # matches = area_matches.append(buffer_matches)

    paths = gen_artifacts(area_matches, s3_imgs, dxf_pieces, output_dir="./artifacts")

    logger.info("Uploading Matching Artifacts to s3")
    for p in paths:
        file_name = os.path.basename(p)
        logger.info(f"Uploading {file_name}")
        s3.upload(p, os.path.join(s3_dir, file_name))


if __name__ == "__main__":
    BODY = "cc-4047"
    DXF_FILE = "body_cc_4047_v2_pattern.dxf"

    DXF_BODY = "cc_4047"

    event = {
        "args": {
            "body": BODY,
            "size_code": "2zzsm",
            "color": "perpit",
            "version": "v2",
            "dxf_size": "SM",
            "dxf_file_s3_path": f"s3://meta-one-assets-prod/bodies/{DXF_BODY}/pattern_files/{DXF_FILE}",
        }
    }

    handler(event, context=None)
