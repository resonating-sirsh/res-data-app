from scipy.signal import find_peaks
from res.utils import logger
import pandas as pd
import numpy as np
import boto3
from pathlib import Path
import cv2
from PIL import Image
from res.connectors.graphql import ResGraphQL, Queries
import json

GET_MATERIAL_QUERY = """
query getMaterial($code:String) {
    material(code:$code) {
        id
        code
        printFileCompensationLength
        printFileCompensationWidth
        cuttableWidth
    }
}
"""


def _find_missing_pixels_in_dataframe(axis_dataframe):
    values = axis_dataframe.diff()[0]
    positive_peaks, _ = find_peaks(values, prominence=100, height=100)
    negative_peaks, _ = find_peaks(-values, prominence=100, height=100)
    peaks = np.concatenate((positive_peaks, negative_peaks), axis=None)
    peaks_dataframe = pd.DataFrame(peaks)

    # to detect missing pixels we will just account for the pixels after the 50th pixel around the border of the piece since
    # some pieces have notches or parts of the image that are supposed to be transparent
    T = round(
        len(values) * 0.10
    )  # 10% of the initial and last part of the image its ignored.
    # T = 100
    filter_arr = peaks_dataframe[0].between(
        T,
        len(values) - T,
        inclusive=False,
    )

    peak_points = np.array(peaks_dataframe[0])[filter_arr]

    # uncomment to plot
    # plt.gcf().set_size_inches((30, 15))
    # plt.plot(values)
    # plt.plot(peak_points, values[peak_points], "x")
    # plt.show()

    return np.any(peak_points)


def look_for_missing_pixels(s3_file_uri):
    s3_client = boto3.client("s3")
    bucket, input_file_key = s3_file_uri.split("/", 2)[-1].split("/", 1)

    # Download file
    logger.info(f"Downloading {input_file_key}...")
    input_key_path = Path(input_file_key)
    input_file_path = f"/tmp/{input_key_path.name}"
    s3_client.download_file(bucket, input_file_key, input_file_path)
    logger.info(f"File downloaded. Checking for missing pixels...")

    piece = cv2.imread(input_file_path, cv2.IMREAD_UNCHANGED)
    piece_alpha_channel = piece[:, :, 3]

    piece_df_x_axis = pd.DataFrame(piece_alpha_channel.mean(axis=0))
    piece_df_y_axis = pd.DataFrame(piece_alpha_channel.mean(axis=1))

    # look for vertical lines of missing pixels.
    has_vertical_missing_pixels = _find_missing_pixels_in_dataframe(piece_df_x_axis)

    # look for horizontal lines of missing pixels.
    has_horizontal_missing_pixels = _find_missing_pixels_in_dataframe(piece_df_y_axis)

    if has_vertical_missing_pixels or has_horizontal_missing_pixels:
        logger.warning(f"File '{s3_file_uri}' may have missing pixels")

    return has_vertical_missing_pixels or has_horizontal_missing_pixels


def validate_piece_exceeds_material_cuttable_width(
    s3_file_uri, material_code, s3_file_uri_includes_offset_buffer=True
):
    s3_client = boto3.client("s3")
    gql = ResGraphQL()

    bucket, input_file_key = s3_file_uri.split("/", 2)[-1].split("/", 1)

    logger.info(f"Downloading {input_file_key}...")
    input_key_path = Path(input_file_key)
    input_file_path = f"/tmp/width_check_{input_key_path.name}"
    s3_client.download_file(bucket, input_file_key, input_file_path)
    logger.info(
        f"File downloaded. Checking if piece will exceed cuttable with after compensation..."
    )

    piece_image = Image.open(input_file_path)
    dpi_x, _ = piece_image.info.get("dpi", (300, 300))
    width, _ = piece_image.size
    piece_width_in_inches = width / dpi_x

    material = gql.query(Queries.GET_MATERIAL_QUERY, {"code": material_code})["data"][
        "material"
    ]
    material_cuttable_width = (
        material["cuttableWidth"] if material["cuttableWidth"] else 0
    )
    material_offset_size_inches = (
        material["offsetSizeInches"] if material["offsetSizeInches"] else 0
    )
    material_compensation_width_scale = (
        material["printFileCompensationWidth"]
        if material["printFileCompensationWidth"]
        else 0
    )

    if not s3_file_uri_includes_offset_buffer:
        # Add buffer width
        piece_width_in_inches = piece_width_in_inches + material_offset_size_inches * 2

    compensated_piece_width = (
        piece_width_in_inches * material_compensation_width_scale / 100
    )

    does_piece_exceed_cuttable_width = compensated_piece_width > material_cuttable_width

    piece_exceeds_cuttable_width_details = None

    if does_piece_exceed_cuttable_width:
        piece_exceeds_cuttable_width_details = {
            "pieceS3FileUri": s3_file_uri,
            "piece": s3_file_uri.split(".")[0].split("/")[-1],
            "materialCode": material_code,
            "materialCompensationWidthPercent": material_compensation_width_scale,
            "materialCuttableWidth": material_cuttable_width,
            "materialOffsetSize": material_offset_size_inches,
            "netPieceWidth": piece_width_in_inches - material_offset_size_inches * 2
            if s3_file_uri_includes_offset_buffer
            else piece_width_in_inches + material_offset_size_inches * 2,
            "compensatedPieceWidth": compensated_piece_width,
            "unit": "Inches",
        }

        logger.warning(json.dumps(piece_exceeds_cuttable_width_details, indent=2))

    return does_piece_exceed_cuttable_width, piece_exceeds_cuttable_width_details
