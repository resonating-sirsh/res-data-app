# Utility methods for nesting / compositing jobs..
import cv2
import qrcode
import numpy as np
import pandas as pd
import os
import res
import random
from res.airtable.print import ASSETS
from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.media.images.providers.dxf import DxfFile
from res.utils import logger, post_to_lambda
from shapely.affinity import affine_transform, scale, translate
from shapely.geometry import Point, Polygon
from shapely.ops import clip_by_rect


BASE_RES_MAGIC_PRINT = "apprcULXTWu33KFsh"
BASE_DXA_ONE_MARKERS = "appqtN4USHTmyC6Dv"
TABLE_NESTS = "tbl7n4mKIXpjMnJ3i"
TABLE_ASSEMBLY_FAILURE_REVIEW = "tblpDzTdXTJFFx9zP"
TABLE_PRINT_ASSETS_FLOW = "tblwDQtDckvHKXO4w"
MM_PER_INCH = 25.4

s3 = res.connectors.load("s3")


def add_qr_code_to_header(header_base_image, qr_code_data):
    """
    Add a qr code to the header image.
    The QR code is square and sized so that the edge length is equal to the minimum of the headers height/width.
    This means that the QR code takes up the full height or full width of the header, whichever is smaller.
    It is placed at the top left of the header.
    """
    qr_code_size = min(header_base_image.shape[:2])
    # move the qr code inwards on the header.
    qr_code_offset = int(0.8 * qr_code_size)
    qr_code = np.asarray(qrcode.make(qr_code_data).convert("RGB"))
    qr_scaled = cv2.resize(qr_code, (qr_code_size, qr_code_size))
    combined_header = header_base_image.copy()
    combined_header[
        0:qr_code_size, qr_code_offset : (qr_code_offset + qr_code_size)
    ] = qr_scaled
    return combined_header


def validate_shapes(shapes, output_width, output_height=None, flag_asset=None):
    """
    Ensure that shapes can actually fit on the roll we are dealing with.
    This should be done after all the stretching etc to the pieces.
    """
    for i, shape in enumerate(shapes):
        x0, y0, x1, y1 = shape.bounds
        width = x1 - x0
        height = y1 - y0
        if width >= output_width:
            if flag_asset is not None:
                ASSETS.update(
                    flag_asset, {"Material - Contract Variable": ["recNOvZ6gPLhxqvIS"]}
                )
            raise ValueError(
                f"Input Shape index {i}. Width:{width} is greater than output bound width:{output_width}"
            )
        if output_height is not None and height >= output_height:
            raise ValueError(
                f"Input Shape index {i}. Height:{height} is greater than output bound height:{output_height}"
            )


def image_offset(shape, image_bounds):
    """
    Add an offset to the nested position so that when compositing the image to the offset position,
    the center of the image would coincide with the center of the nested shape.
    """
    ny, nx, _ = image_bounds
    x0, y0, x1, y1 = shape.bounds
    height = y1 - y0
    width = x1 - x0
    return np.array([int(width / 2 - nx // 2), int(height / 2 - ny // 2)])


def shape_image_slop(shape, image, image_offset, grid_resolution=30):
    """
    After transforming the image and the shape so that the centers overlap -
    what is the largest distance between a non-transparent image point, and the polygon
    boundary.
    This should theoretically be zero or maybe 1 in the case of rounding errors, but we
    can tolerate some slop coming from the upstream processes if its sufficiently small.
    """
    ny, nx, _ = image.shape
    max_dist = 0
    for x in range(0, nx, nx // grid_resolution + 1):
        for y in range(0, ny, ny // grid_resolution + 1):
            if image[y][x][3] > 0:
                point = Point(np.array([x, y]) + image_offset)
                if not shape.contains(point):
                    dist = point.distance(shape)
                    if dist > max_dist:
                        max_dist = dist
    return max_dist


def transform_piece_for_nesting(shape, stretch_x, stretch_y, buffer, max_width):
    return transform_pieces_for_nesting(
        [shape], stretch_x, stretch_y, buffer, max_width
    )[0]


def transform_pieces_for_nesting(
    shapes, stretch_x, stretch_y, buffer, max_width, clip_to_width=False
):
    """
    scale, invert, and buffer the pieces.
    """

    def _optional_buffer(s):
        s = affine_transform(s, [stretch_x, 0, 0, -stretch_y, 0, 0])
        if clip_to_width:
            # just ensure the thing fits.
            sx1, sy0, sx2, sy1 = s.bounds
            return clip_by_rect(s, sx1, sy0, sx1 + max_width - 2, sy1)
        if buffer:
            buffered = s.buffer(buffer)
            x0, y0, x1, y1 = buffered.bounds
            if x1 - x0 <= max_width:
                return buffered
            else:
                # the piece is so wide it would take up the whole roll, so try to just buffer it vertically
                sx1, _, sx2, _ = s.bounds
                # hack to deal with rounding errors.
                if sx2 - sx1 > max_width:
                    sx1 += 1
                    sx2 -= 1
                return clip_by_rect(buffered, sx1, y0, sx2, y1)
        else:
            return s

    return [_optional_buffer(s) for s in shapes]


def validate_nesting(
    input_shapes, output_shapes, output_width, output_height, tol=1e-6
):
    """
    ensure that we have a valid packing containing all the pieces.
    """
    # since we nest with integer lattice points its possible for shapes to share e.g., a vertical edge
    # meaning they have non zero intersection but the nesting is valid.  just shrink each piece by 1 to deal with this.
    output_shapes_inner = [s.buffer(-2) for s in output_shapes]
    if len(input_shapes) != len(output_shapes_inner):
        raise ValueError(
            f"Invalid nesting: input shapes length {len(input_shapes)} but nesting result has {len(output_shapes)} shapes"
        )
    for i, output_shape in enumerate(output_shapes_inner):
        x0, y0, x1, y1 = output_shape.bounds
        if (
            x0 < -tol
            or y0 < -tol
            or x1 > output_width + tol
            or y1 > output_height + tol
        ):
            raise ValueError(
                f"Invalid nesting: piece {i} is out of bounds {output_shape.bounds} for max width {output_width} and height {output_height}"
            )
        # this is possibly slow we can remove it after sufficient time of not encountering the error.
        for j in range(i + 1, len(output_shapes_inner)):
            # sometimes we get a non zero area seemingly just due to floating point rounding.
            intersection_area = output_shapes_inner[j].intersection(output_shape).area
            if intersection_area > tol:
                raise ValueError(
                    f"Invalid nesting: pieces {i, j} intersect with non zero area {intersection_area}"
                )


def get_nest_key_from_argo_job_key(job_key):
    try:
        return list(
            ResAirtableClient().get_records(
                BASE_RES_MAGIC_PRINT,
                TABLE_NESTS,
                filter_by_formula=f"AND({{Argo Jobkey}}='{job_key}')",
                log_errors=False,
            )
        )[0]["fields"]["record_id"]
    except Exception as ex:
        logger.warn(f"Failed to find nest record id for job {job_key}, {repr(ex)}")
        return job_key


# TODO: move this over to kafka
def notify_callback(
    df, job_key, lambda_name, nest_df_path=None, printfile_path=None, succeeded=False
):
    nested_length = int(df.max_nested_y.max() - df.min_nested_y.min())
    df = df[pd.notna(df.asset_id)].reset_index(drop=True)
    all_assets = [
        {"asset_id": asset_id, "material_code": df.material[0]}
        for asset_id in df.asset_id.unique()
        if asset_id is not None
    ]
    payload = {
        "nested_assets": all_assets if succeeded else [],
        "omitted_assets": [] if succeeded else all_assets,
        "print_file": printfile_path,
        "nest_df_path": nest_df_path,
        "print_file_ready": printfile_path is not None,
        "evaluation": {
            "nested_area": int(df.nested_area.sum()) if succeeded else 0,
            "request_width": int(df["output_bounds_width"][0]),
            "request_length": int(df["output_bounds_length"][0]),
            "total_area": int(
                df["output_bounds_width"][0] * df["output_bounds_length"][0]
            ),
            "nested_length": nested_length if succeeded else 0,
            "num_pieces_requested": int(df.shape[0]),
            "num_pieces_nested": int(df.shape[0]) if succeeded else None,
        },
        "user_data": {
            "jobkey": job_key,
        },
        "status_code": 200 if succeeded else 500,
    }
    env = os.getenv("RES_ENV")
    if env == "production":
        logger.info(f"Posting to callback {lambda_name}: {payload}")
        post_to_lambda(lambda_name, payload)
    else:
        logger.info(
            f"Not posting to callback {lambda_name} in env {env} payload was: {payload}"
        )


def flag_asset(one_number, asset_record_id, asset_path, task_key, flag_reason):
    logger.info(f"Flagging asset {asset_record_id} for reason {flag_reason}")
    airtable = res.connectors.load("airtable")
    airtable[BASE_DXA_ONE_MARKERS][TABLE_ASSEMBLY_FAILURE_REVIEW].update_record(
        {
            "record_id": None,
            "Errors": flag_reason,
            "Name": one_number,
            "Filename": asset_path,
            "Task": task_key,
            "AssetId": asset_record_id,
        },
        use_kafka_to_update=False,
    )
    asset_update = {
        "record_id": asset_record_id,
        "Flag For Review": "true",
        "Flag For Review Reason": flag_reason,
    }
    if "width" in flag_reason and "nest" in flag_reason:
        asset_update["Failure Tags"] = "OVER_MAX_WIDTH"
    airtable[BASE_RES_MAGIC_PRINT][TABLE_PRINT_ASSETS_FLOW].update_record(
        asset_update,
        use_kafka_to_update=False,
    )


def unflag_asset(asset_record_id, flag_reason_predicate=None):
    airtable = res.connectors.load("airtable")
    asset_record = airtable[BASE_RES_MAGIC_PRINT][TABLE_PRINT_ASSETS_FLOW].get_record(
        asset_record_id
    )
    if not asset_record["fields"].get("Flag For Review", False):
        return
    if flag_reason_predicate is not None:
        flag_reason = asset_record["fields"].get("Flag For Review Reason")
        if not flag_reason_predicate(flag_reason):
            logger.info(
                f"Not unflagging since flag reason does not match predicate {flag_reason}"
            )
            return
    logger.info(f"Un-flagging asset {asset_record_id}")
    airtable[BASE_RES_MAGIC_PRINT][TABLE_PRINT_ASSETS_FLOW].update_record(
        {
            "record_id": asset_record_id,
            "Flag For Review": "false",
        },
        use_kafka_to_update=False,
    )


def piece_label_angle(shape):
    """
    Just figure out a good angle for the labels mainly to get the 45 degree pieces right.
    """
    center = shape.centroid.coords[0]
    centered_shape = affine_transform(shape, (1, 0, 0, 1, -center[0], -center[1]))
    shape_matrix = np.array(centered_shape.exterior.coords.xy).T[
        :-1,
    ]
    angles = [
        (0, np.array([1, 0])),
        (-45, np.array([1, -1])),
        (45, np.array([1, 1])),
        (90, np.array([0, 1])),
    ]
    norms = [
        (a, np.linalg.norm(np.matmul(shape_matrix, v / np.linalg.norm(v))))
        for a, v in angles
    ]
    # only do a vertical label if the piece is really skinny
    if norms[3][0] < norms[0][0] * 5:
        norms = norms[:-1]
    return max(norms, key=lambda t: t[1])[0]


def make_qr_image(qr_data, qr_code_size):
    qr_code = np.asarray(qrcode.make(qr_data).convert("RGB"))
    qr_code = cv2.cvtColor(qr_code, cv2.COLOR_RGB2RGBA)
    qr_code[:, :, 3] = 255
    return cv2.resize(qr_code, (qr_code_size, qr_code_size))


def write_qr_image_to_s3(qr_data, qr_code_size, s3_path):
    qr_code = make_qr_image(qr_data, qr_code_size)
    filename = f"qr_code_{hash(qr_data)}.png"
    cv2.imwrite(filename, qr_code)
    s3.upload(filename, s3_path)


def upload_qr_pieces(job_key, qr_code_image_path, qr_code_shape_path, qr_code_size):
    write_qr_image_to_s3(job_key, qr_code_size, qr_code_image_path)
    upload_rectangle_piece(qr_code_shape_path, qr_code_size, qr_code_size)
    logger.info(
        f"Wrote qr code for value {job_key} to {qr_code_image_path}, {qr_code_shape_path}"
    )


def write_text_label_to_s3(text, height_px, width_px, s3_path):
    img = np.zeros((height_px, width_px, 4), np.uint8)
    text_scale = 1
    while text_scale < 10:
        tw, th = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, text_scale, 20)[0]
        if tw >= width_px or th >= height_px:
            text_scale = max(1, text_scale - 1)
            break
        text_scale += 1
    cv2.putText(
        img,
        text,
        (0, height_px - 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        text_scale,
        (0, 0, 0, 255),
        20,
        cv2.LINE_AA,
    )
    filename = "text_label_" + str(abs(hash(text))) + ".png"
    cv2.imwrite(filename, img)
    s3.upload(filename, s3_path)


def write_guillotine_seperator_to_file(
    top_text, bottom_text, height_px, width_px, local_path
):
    height_px = int(height_px)
    width_px = int(width_px)
    img = np.ones((height_px, width_px, 4), np.uint8) * 255
    text = top_text + " " + (bottom_text or "")
    text_scale = 1
    text_height = int(height_px / (3 if bottom_text is not None else 1.8))
    while text_scale < 10:
        _, th = cv2.getTextSize(text, cv2.FONT_HERSHEY_SIMPLEX, text_scale, 20)[0]
        if th >= text_height:
            text_scale = max(1, text_scale - 1)
            break
        text_scale += 1
    cv2.putText(
        img,
        top_text,
        (int(text_height * 1.1), text_height),
        cv2.FONT_HERSHEY_SIMPLEX,
        text_scale,
        (0, 0, 0, 255),
        20,
        cv2.LINE_AA,
    )
    qr_code_size = text_height
    qr_x = int(text_height * 0.05)
    qr_y = int(text_height * 0.1)
    img[qr_y : (qr_y + qr_code_size), qr_x : (qr_x + qr_code_size)] = cv2.resize(
        np.asarray(qrcode.make(top_text).convert("RGBA")), (qr_code_size, qr_code_size)
    )

    if bottom_text is not None:
        cv2.putText(
            img,
            bottom_text,
            (int(text_height * 1.1), int(height_px * 5 / 6)),
            cv2.FONT_HERSHEY_SIMPLEX,
            text_scale,
            (0, 0, 0, 255),
            20,
            cv2.LINE_AA,
        )
        qr_y = int(height_px * 5 / 6) - text_height + int(text_height * 0.1)
        img[qr_y : (qr_y + qr_code_size), qr_x : (qr_x + qr_code_size)] = cv2.resize(
            np.asarray(qrcode.make(bottom_text).convert("RGBA")),
            (qr_code_size, qr_code_size),
        )
        cv2.line(
            img,
            (0, int(height_px / 2)),
            (width_px, int(height_px / 2)),
            (0, 0, 0, 255),
            35,
        )
    cv2.imwrite(local_path, img)


def write_guillotine_seperator_to_s3(
    top_text, bottom_text, height_px, width_px, s3_path
):
    filename = (
        "guillotine_seperator_" + str(abs(hash(top_text + " " + bottom_text))) + ".png"
    )
    write_guillotine_seperator_to_file(
        top_text, bottom_text, height_px, width_px, filename
    )
    s3.upload(filename, s3_path)


def upload_text_label_piece(image_path, shape_path, height_px, width_px, text):
    write_text_label_to_s3(text, height_px, width_px, image_path)
    upload_rectangle_piece(shape_path, height_px, width_px)
    logger.info(f"Wrote text label {text} to {image_path}, {shape_path}")


def qr_piece_df(qr_code_size):
    return rectangle_piece_df(qr_code_size, qr_code_size)


def rectangle_piece_df(height, width):
    return pd.DataFrame(
        {
            "column_0": [0, 0, height, height, 0],
            "column_1": [0, width, width, 0, 0],
        }
    )


def upload_rectangle_piece(s3_path, height, width):
    s3.write(s3_path, rectangle_piece_df(height, width))


def piece_df_to_polygon(df):
    return Polygon(df[["column_1", "column_0"]].values).convex_hull


def qr_code_polygon(qr_code_size):
    return piece_df_to_polygon(qr_piece_df(qr_code_size))


def s3_path_to_image_supplier(path):
    def g():
        logger.info(f"Downloading s3 image {path}")
        return s3.read(path)

    return g


def zero_waste_patch_image_supplier(artwork_uri):
    def g():
        logger.info(f"Downloading s3 image {artwork_uri}")
        img = s3.read(artwork_uri)
        h, w, c = img.shape
        if h > 900 and w > 900:
            for t in range(10):
                y = random.randint(0, h - 900)
                x = random.randint(0, w - 900)
                s = img[y : (y + 900), x : (x + 900), :]
                if s[:, :, 3].sum() == 900 * 900 * 255:
                    return np.ascontiguousarray(s)
        mean_color = (
            (img[:, :, 0:3] * np.repeat(img[:, :, 3:4] / 255.0, 3, axis=2))
            .mean(axis=(0, 1))
            .astype("uint8")
        )
        pp = np.ones((900, 900, 4), dtype="uint8") * 255
        pp[:, :, 0:3] = mean_color
        return np.ascontiguousarray(pp)

    return g


def pixel_space_to_dxf(shape, paper_scale_x, paper_scale_y):
    return Polygon(
        (x * paper_scale_x * MM_PER_INCH / 300, y * paper_scale_y * MM_PER_INCH / 300)
        for x, y in shape.boundary.coords
    )


def cutline_from_path(path):
    # note that the nest is made with inverted pieces.
    # the dxf is meant to be off by a 180 degree rotation from those inverted pieces
    # the end result of this is that the dxf piece is off by a x-axis reflection from
    # the original piece.
    try:
        return scale(Polygon(s3.read(path)[["column_1", "column_0"]].values), -1, 1)
    except:
        return None


def midpoint(polygon):
    x0, y0, x1, y1 = polygon.bounds
    return np.array(
        [
            (x1 + x0) / 2.0,
            (y1 + y0) / 2.0,
        ]
    )


def nest_to_dxf(nest_df, output_s3_path, one_number=None):
    logger.info("making dxf")
    cutline_df = nest_df.copy()
    cutline_df["cutline_poly"] = cutline_df["cutline_uri"].apply(cutline_from_path)
    cutline_df = cutline_df[~cutline_df.cutline_poly.isnull()].reset_index(drop=True)
    if cutline_df.empty:
        return False
    cutline_df["cutline_centroid"] = cutline_df["cutline_poly"].apply(midpoint)
    # the dxf is upside down hence negating the direction of the translation.
    cutline_df["cutline_nested_centroid"] = cutline_df.apply(
        lambda r: np.array(
            [
                -(r.max_nested_x + r.min_nested_x) / (2.0 * r.stretch_x),
                -(r.max_nested_y + r.min_nested_y) / (2.0 * r.stretch_y),
            ]
        ),
        axis=1,
    )
    cutline_df["nested_cutline"] = cutline_df.apply(
        lambda r: pixel_space_to_dxf(
            translate(
                r.cutline_poly,
                *(r.cutline_nested_centroid - r.cutline_centroid),
            ),
            r.paper_marker_stretch_x,
            r.paper_marker_stretch_y,
        ),
        axis=1,
    )
    DxfFile.export_dataframe(
        pd.DataFrame.from_dict({"geometry": cutline_df["nested_cutline"].values}),
        output_s3_path,
        point_conversion_op=lambda p: p.boundary.coords,
    )
    img_path = plot_dxf(output_s3_path)
    if one_number is not None:
        s3.copy_to_make_one_production(one_number, output_s3_path)
        s3.copy_to_make_one_production(one_number, img_path)
    return True


def plot_dxf(dxf_path):
    from matplotlib import pyplot as plt

    img_path = dxf_path.replace(".dxf", "_dxf.png")
    plt.switch_backend("Agg")
    DxfFile(dxf_path).plot()
    plt.savefig("dxf.png", bbox_inches="tight")
    s3.upload("dxf.png", img_path)
    plt.close()
    return img_path
