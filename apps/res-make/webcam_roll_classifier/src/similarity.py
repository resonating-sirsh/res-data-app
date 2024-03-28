import cv2
import res
import numpy as np
from dataclasses import dataclass
from res.utils import logger
from strip import get_strips, get_kafka_messages_for_pieces, get_strip_for_path
from webcam import get_target_region

# the basic approach here is to take patches of the roll along with the target image from the webcam
# then to downsample both aggressively and just look at the euclidean distance in RGB space between
# the downsampled images.  PATCH_RESOLUTION controls the final resolution after downsampling.
PATCH_RESOLUTION = 16

OUTPUT_BUCKET = "s3://res-data-development/image_recognition/laser_cutter"

KAFKA_TOPIC = "res_make.piece_tracking.make_piece_observation_request"

MAX_DISTANCE_THRESHOLD = 100.0

weaviate_client = res.connectors.load("weaviate")._client

# a patch with some metadata.
@dataclass
class Patch:
    offset: int
    normalized_offset: float
    normalized_height: float
    flipped: bool
    pixels: np.array


def _get_strip_patches(strip_image):
    # massively downsample and make 16*16 vectors from sliding windows.
    width = strip_image.shape[1]
    strip_scale = float(PATCH_RESOLUTION) / width
    strip_downsampled = (
        cv2.resize(
            strip_image,
            (0, 0),
            fx=strip_scale,
            fy=strip_scale,
            interpolation=cv2.INTER_AREA,
        )
        / 255.0
    )
    height_d = strip_downsampled.shape[0]
    for i in range(height_d - PATCH_RESOLUTION + 1):
        offset = int(i / strip_scale)
        patch = strip_downsampled[i : (i + PATCH_RESOLUTION), :, :]
        # emit the patch and the flipped version of hte patch too -- since sometimes
        # the roll is fed into the machine in the opposite direction (?)
        for flipped in [True, False]:
            yield Patch(
                offset,
                float(i) / height_d,
                float(PATCH_RESOLUTION) / height_d,
                flipped,
                cv2.flip(patch, -1) if flipped else patch,
            )


def _get_strip_patch(strip, offset, flipped):
    width = strip.shape[1]
    patch = strip[offset : (offset + width), :, :]
    return patch if not flipped else cv2.flip(patch, -1)


def patch_to_vec(pixels):
    n, m, _ = pixels.shape
    px_good = pixels[2 : (n - 2), 2 : (m - 2), :]
    cv2.GaussianBlur(px_good, (5, 5), 0)
    return px_good.flatten()


def reset_weaviate():
    weaviate_client.schema.delete_class("Printfile_tile")

    pf_schema = {
        "classes": [
            {
                "class": "Printfile_tile",
                "properties": [
                    {
                        "name": "printfile_name",
                        "dataType": ["string"],
                    },
                    {
                        "name": "offset",
                        "dataType": ["int"],
                    },
                    {
                        "name": "normalized_offset",
                        "dataType": ["number"],
                    },
                    {
                        "name": "normalized_height",
                        "dataType": ["number"],
                    },
                    {
                        "name": "flipped",
                        "dataType": ["boolean"],
                    },
                ],
                "vectorIndexConfig": {
                    "distance": "l2-squared",
                },
            }
        ],
    }
    weaviate_client.schema.create(pf_schema)


def update_weaviate(days=8):
    try:
        reset_weaviate()
    except:
        pass
    logger.info(f"Updating weaviate for the last {days} days")
    # ensure weaviate has all the recent printfiles -- TODO == just index the printfile as its composited (?)
    for strip in get_strips(days=days):
        # check if its indexed already
        asset_path = strip["Asset Path"]
        r = (
            weaviate_client.query.aggregate("Printfile_tile")
            .with_where(
                {
                    "path": ["printfile_name"],
                    "operator": "Equal",
                    "valueString": asset_path,
                }
            )
            .with_group_by_filter(["printfile_name"])
            .with_meta_count()
            .do()
        )
        if len(r["data"]["Aggregate"]["Printfile_tile"]) == 0:
            logger.info(f"Writing printfile {asset_path} to weaviate")
            patches = 0
            for patch in _get_strip_patches(strip["image"]):
                weaviate_client.data_object.create(
                    data_object={
                        "printfile_name": asset_path,
                        "offset": patch.offset,
                        "normalized_offset": patch.normalized_offset,
                        "normalized_height": patch.normalized_height,
                        "flipped": patch.flipped,
                    },
                    class_name="Printfile_tile",
                    vector=patch_to_vec(patch.pixels),
                )
                patches += 1
            logger.info(f"Wrote {patches} patches for printfile {asset_path}")


def _output_path(webcam_path):
    file_name = webcam_path.split("/")[-1]
    return f"{OUTPUT_BUCKET}/{file_name.replace('.', '_')}"


def _downsample_target(target_region):
    return (
        cv2.resize(
            target_region,
            (PATCH_RESOLUTION, PATCH_RESOLUTION),
            interpolation=cv2.INTER_AREA,
        )
        / 255.0
    )


def get_matches_for_target(target_path, target_downsampled, roll_name=None):
    try:
        resp = (
            weaviate_client.query.get(
                "Printfile_tile",
                [
                    "printfile_name",
                    "offset",
                    "flipped",
                    "normalized_offset",
                    "normalized_height",
                ],
            )
            .with_near_vector(
                {
                    "vector": patch_to_vec(target_downsampled),
                    "distance": MAX_DISTANCE_THRESHOLD,
                }
            )
            .with_additional(["distance"])
            .with_limit(15)
            .do()
        )
        resp = resp["data"]["Get"]["Printfile_tile"]
        if len(resp) < 1:
            return None, False
        matches_on_roll = (
            []
            if roll_name is None
            else [r for r in resp if roll_name.lower() in r["printfile_name"]]
        )
        best_match = matches_on_roll[0] if len(matches_on_roll) > 0 else resp[0]
        return best_match, True
    except Exception as ex:
        logger.error(f"Failed to get matches for {target_path}: {repr(ex)}")
        return None, False


def get_patch_for_match(match):
    strip = get_strip_for_path(match["printfile_name"])
    return _get_strip_patch(strip["image"], match["offset"], match["flipped"])


def get_best_matches(webcam_paths, write_kafka=True):
    s3 = res.connectors.load("s3")
    kafka = res.connectors.load("kafka") if write_kafka else None
    # get webcam targets.
    for p in webcam_paths:
        try:
            output_path = _output_path(p)
            logger.info(f"Loading target info for {p}.")
            input = s3.read(p)
            target_region = get_target_region(p)
            target_downsampled = _downsample_target(target_region)
            s3.write(output_path + "/input.png", input)
            s3.write(output_path + "/target.png", target_region)
            best_match, match_good = get_matches_for_target(
                p,
                target_downsampled,
            )
            if best_match is not None:
                s3.write(output_path + "/best_match.json", best_match)
                s3.write(
                    output_path + "/best_match.png", get_patch_for_match(best_match)
                )
                if match_good:
                    kafka_messages = get_kafka_messages_for_pieces(
                        best_match["printfile_name"],
                        best_match["normalized_offset"],
                        best_match["normalized_height"],
                    )
                    if kafka_messages is not None:
                        for msg in kafka_messages:
                            msg["metadata"] = {
                                "camera_image": p,
                                "dist": best_match["_additional"]["distance"],
                                "printfile_name": best_match["printfile_name"],
                                "normalized_offset": best_match["normalized_offset"],
                                "normalized_height": best_match["normalized_height"],
                            }
                        if write_kafka:
                            kafka[KAFKA_TOPIC].publish(
                                kafka_messages, use_kgateway=True
                            )
        except Exception as ex:
            logger.error(f"Failed to handle webcam image {p}: {repr(ex)}")
