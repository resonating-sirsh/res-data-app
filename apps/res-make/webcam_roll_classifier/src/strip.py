import res
import cv2
import numpy as np
from datetime import date
from functools import lru_cache
from res.utils import secrets, logger
from pyairtable import Table

BASE_MATERIALS = "app1FBXxTRoicCW8k"
TABLE_MATERIALS = "tblD1kPG5jpf6GCQl"
BASE_CUT = "appyIrUOJf8KiXD1D"
TABLE_CUT = "tblXHLe3lm4zGvfZX"
BASE_PRINT = "apprcULXTWu33KFsh"
TABLE_PRINTFILES = "tblAQcPuKUDVfU7Fx"

s3 = res.connectors.load("s3")


@lru_cache(maxsize=1)
def _get_compensation_data():
    logger.info("Requesting material info")
    compensation_res = Table(
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
        BASE_MATERIALS,
        TABLE_MATERIALS,
    ).all(
        fields=[
            "Material Code",
            "Locked Length Digital Compensation (Scale)",
            "Locked Width Digital Compensation (Scale)",
        ],
    )
    logger.info(f"Got material info for {len(compensation_res)} materials")
    return {
        r["fields"]["Material Code"]: (
            r["fields"]["Locked Length Digital Compensation (Scale)"],
            r["fields"]["Locked Width Digital Compensation (Scale)"],
        )
        for r in compensation_res
        if "Material Code" in r["fields"]  # WTF
    }


def _get_recent_printfile_records(days=14):
    # lame heuristic strat to get the candidate set of printfiles -- just everything thats been printed in the last week.
    today = date.today().strftime("%Y-%m-%d")
    logger.info(f"Fetching last {days} days of printfile records for date {today}")
    recent_printfiles = Table(
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
        BASE_PRINT,
        TABLE_PRINTFILES,
    ).all(
        fields=[
            "Key",
            "Asset Path",
            "Roll Name",
            "Printed_ts",
            "Material Code",
            "__roll_record_id",
        ],
        formula=f"AND(DATETIME_DIFF('{today}', {{Printed_ts}}, 'd') <= {days}, {{Print Queue}}='PRINTED')",
    )
    logger.info(f"Got {len(recent_printfiles)} printfile records")
    return recent_printfiles


@lru_cache(maxsize=10)
def _get_strip(s3_strip_path, compensation_x, compensation_y):
    strip = s3.read(s3_strip_path)
    # munge the image to make it white where the things transparent.
    background = np.ones_like(strip) * 255.0
    alpha = np.stack([strip[:, :, 3] / 255.0 for _ in range(3)], axis=2)
    strip = np.multiply(strip[:, :, 0:3], alpha) + np.multiply(
        background[:, :, 0:3], 1 - alpha
    )
    strip = strip.astype(np.ubyte)
    # undo the compensation -- since we see the roll after all the physical processes.
    strip = cv2.resize(
        strip,
        (0, 0),
        fx=1.0 / compensation_x,
        fy=1.0 / compensation_y,
        interpolation=cv2.INTER_AREA,
    )
    # make sure we have at least 2x as much height as we have width - so that we can make
    # all the sliding windows which potentially have any of the roll in the view.
    if 2 * strip.shape[1] >= strip.shape[0]:
        strip = cv2.copyMakeBorder(
            strip,
            0,
            2 * strip.shape[1] - strip.shape[0] + 1,
            0,
            0,
            cv2.BORDER_CONSTANT,
            value=[255, 255, 255],
        )
    return strip


# this is mainly for debugging purposes.
def get_strips_for_paths(paths):
    printfiles = Table(
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
        BASE_PRINT,
        TABLE_PRINTFILES,
    ).all(
        fields=[
            "Key",
            "Asset Path",
            "Roll Name",
            "Material Code",
        ],
        formula=f"AND({{Asset Path}}!=BLANK(), FIND({{Asset Path}}, '{','.join(paths)}'))",
    )
    printfiles = [r for r in printfiles if "Asset Path" in r["fields"]]  # WTF airtable.
    return get_strips_for_printfiles(printfiles)


def get_strip_for_path(path):
    return next(get_strips_for_paths([path]))


def get_strips(days=14):
    return get_strips_for_printfiles(_get_recent_printfile_records(days=days))


def get_strips_for_printfiles(printfiles):
    compensation_data = _get_compensation_data()
    for i, r in enumerate(printfiles):
        compensation_y, compensation_x = compensation_data.get(
            r["fields"]["Material Code"][0], (1.0, 1.0)
        )
        logger.info(
            f"Loading strip {i}: {r}, compensation {compensation_y, compensation_x}"
        )
        strip = _get_strip(
            r["fields"]["Asset Path"] + "/printfile_composite_thumbnail.png",
            compensation_x,
            compensation_y,
        )
        yield {
            **r["fields"],
            "image": strip,
            "id": r["id"],
            "compensation_x": compensation_x,
            "compensation_y": compensation_y,
        }


def _manifest_row_to_kafka_message(row):
    return {
        "pieces": [
            {
                "code": "-".join(row.piece_name.split("-")[-2:]),
            }
        ],
        "id": f"laser_cutter_webcam_{res.utils.res_hash()}",
        "one_number": int(str(row.asset_key).split("_")[0]),
        "observed_at": res.utils.dates.utc_now_iso_string(),
        "node": "Make.Cut.LaserCutter",
        "status": "Enter",
    }


def get_kafka_messages_for_pieces(asset_path, normalized_offset, normalized_height):
    try:
        manifest = s3.read(f"{asset_path}/manifest.feather")
        if manifest is None:
            return None
        # note - min_y and max_y on the pieces are measuring from the bottom of the printfile.
        # want pieces with min_y_inches < offset + height, and max_y_inches > offset
        y_min = manifest.printfile_height_inches[0] * (
            1 - normalized_offset - normalized_height
        )
        y_max = manifest.printfile_height_inches[0] * (1 - normalized_offset)
        pieces = manifest[
            (
                manifest.inches_from_printfile_top
                < manifest.printfile_height_inches[0]
                * (normalized_offset + normalized_height)
            )
            & (
                (
                    manifest.printfile_height_inches
                    - manifest.inches_from_printfile_bottom
                )
                > manifest.printfile_height_inches[0] * normalized_offset
            )
        ]
        # munge these into kafka messages for make_asset_status
        return pieces.apply(_manifest_row_to_kafka_message, axis=1).tolist()
    except Exception as ex:
        logger.info(f"Failed to generate kafka messages for {asset_path}: {repr(ex)}")
        return None
