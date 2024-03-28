import os

# os.environ["OPENCV_IO_MAX_IMAGE_PIXELS"] = pow(2, 40).__str__()
import cv2
import uuid
import boto3
import numpy as np
import argparse, re
from PIL import Image
from pathlib import Path
from res.utils import logger
from shapely.geometry import *
import matplotlib.pyplot as plt


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--dry-run",
        action="store_true",
        default=False,
        help="Use a dry run, which will not upload the output file",
    )
    parser.add_argument(
        "-i", "--s3-input-key", required=True, help="S3 key of the pdf to be rasterized"
    )
    parser.add_argument(
        "-o",
        "--s3-output-key",
        required=True,
        help="S3 key where to store the rasterized output",
    )

    return parser.parse_args(args)


def extract_pieces(file_path):
    img = cv2.imread(file_path, -1)
    rows, cols, channels = img.shape

    if channels == 3:  # add alpha channel
        img = cv2.cvtColor(img, cv2.COLOR_RGB2RGBA)
        img[:, :, 3] = 0

    mask = cv2.inRange(img, (0, 0, 0, 0), (255, 255, 255, 0))
    contours, hierarchy = cv2.findContours(mask, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)
    rows, cols, _ = img.shape
    hier = -1

    for cont in contours:
        area = cv2.contourArea(cont)  # Eliminate contour surrounding entire image
        if area > 0.9 * rows * cols:
            hier = 0
            break

    tol = 100
    arr = np.empty((0, 2))
    approxs = []
    for i, cnt in enumerate(contours):
        approx = cv2.approxPolyDP(cnt, 2, True)
        perimeter = cv2.arcLength(approx, True)
        if perimeter > tol:
            if hierarchy[0, i, 3] == hier:
                x, y, w, h = cv2.boundingRect(cnt)
                cnt_coords = approx.reshape((approx.shape[0], 2))
                mask_copy = mask.copy()
                mask_copy = cv2.fillPoly(mask_copy, [cnt], (127, 127, 127, 255))
                crop_mask = mask_copy[y : y + h, x : x + w]
                correct = cv2.inRange(crop_mask, np.array([127]), np.array([127]))
                crop_img = img[y : y + h, x : x + w]
                iso = cv2.bitwise_and(crop_img, crop_img, mask=correct)
                trans_mask = iso[:, :, 3] == 0
                iso[trans_mask] = [255, 255, 255, 0]
                if (
                    iso.shape[0] == cnt_coords.shape[0]
                ):  # make sure coords and rows are different sizes
                    last = cnt_coords[-1, :]
                    first = cnt_coords[0, :]
                    avg = np.array(np.mean([last, first], axis=0)).reshape((1, 2))
                    cnt_coords = np.append(cnt_coords, avg, axis=0)
                approxs.append(cnt_coords)
                add = np.array([[cnt_coords, iso]], dtype=object)
                arr = np.append(arr, add, axis=0)
    return arr


def file_to_pieces(file_path):
    arr = extract_pieces(file_path)
    arr = np.vstack(arr)
    shapes = arr[:, 0].tolist()
    images = arr[:, 1].tolist()
    return shapes, images


def split_style_size_pieces(event):
    s3_client = boto3.client("s3")
    input_file_uri = event.get("input_file_uri")
    bucket, input_file_key = input_file_uri.split("/", 2)[-1].split("/", 1)
    # Download file
    logger.info(f"(1/4) Downloading {input_file_key}...")
    input_key_path = Path(input_file_key)
    input_file_path = f"/tmp/{input_key_path.name}"
    s3_client.download_file(bucket, input_file_key, input_file_path)
    logger.info(f"(2/4) File downloaded. Splitting Pieces...")

    _, images = file_to_pieces(input_file_path)
    logger.info(f"(3/4) Pieces splitted. Uploading pieces to S3...")

    # for shape in _:
    #     shape_cpy = np.copy(shape)
    #     shape_cpy[:, 1] *= -1
    #     x, y = Polygon(shape_cpy).exterior.xy
    #     plt.fill(x, y, alpha=0.5, fc='blue', ec='none')
    #     plt.plot(x, y, color='blue', linewidth=.5)
    #     plt.show()

    color_on_shape_path = input_file_key.split("/")
    color_on_shape_path = "/".join(color_on_shape_path[:-2])
    base_pieces_key = f"{color_on_shape_path}/pieces"

    all_pieces_keys = []
    # s3 = res.connectors.load('s3)
    for image in images:
        piece_file_name = f"{str(uuid.uuid4())}.png"
        local_path = f"/tmp/{piece_file_name}"
        # Convert BGR OpenCV image into RGB to match PIL's channel ordering
        RGBimage = cv2.cvtColor(image, cv2.COLOR_BGRA2RGBA)
        # Convert OpenCV Numpy array onto PIL Image
        PILimage = Image.fromarray(RGBimage)
        # s3.write("s3://.png", im, dpi=(300,300))
        PILimage.save(local_path, dpi=(300, 300))

        output_file_s3_key = f"{base_pieces_key}/{piece_file_name}"

        s3_client.upload_file(local_path, bucket, output_file_s3_key)
        all_pieces_keys.append(output_file_s3_key)

    logger.info("(4/4) Done")

    return {
        "pieces_folder_uri": f"s3://{bucket}/{base_pieces_key}",
        "pieces_keys": all_pieces_keys,
    }


if __name__ == "__main__":

    # example payload
    data = {
        "input_file_uri": "s3://meta-one-assets-prod/color_on_shape/mr_6002/v3/leopaq/3zzmd/rasterized_files/b73f1fa6-e6e3-4cef-9400-c4e0d642b327.png"
    }

    split_style_size_pieces(data)
