import res
import os
import numpy as np
from datetime import datetime, timedelta
import cv2

# how far back to go
WEBCAM_TIME_WINDOW_MINUTES = int(os.environ.get("WEBCAM_TIME_WINDOW_MINUTES", 60 * 12))


def get_latest_webcam_paths():
    # the s3 connector deliberately drops the time out of the timestamp for reasons i dont want to figure out.
    modified_after = datetime.now() - timedelta(minutes=WEBCAM_TIME_WINDOW_MINUTES)
    recent_paths = res.connectors.load("s3").ls_info(
        "s3://dr-factory-resource-images-prod/laser-cutters/",
        suffixes=[".png"],
        modified_after=modified_after,
    )
    for file_info in recent_paths:
        if file_info["last_modified"] > modified_after:
            yield file_info["path"]


def get_target_region(path):
    """
    Retrieves a square section of the image thats likely to contain the roll we are trying to classify.
    The technique is to just find a square section of the image which is brighter on the inside than on the outside
    since the roll is mainly white or some colors, whereas the machine and belt are mostly black.
    """
    target = res.connectors.load("s3").read(path)
    xbounds = (
        (1500, 4500) if "cutter-4" not in path else (900, 4500)
    )  # todo = check laser 1
    cropped = target[:, xbounds[0] : xbounds[1], :]
    gray = cv2.cvtColor(cropped, cv2.COLOR_BGR2GRAY) / 255.0
    column_intensities = np.mean(gray, axis=0)
    n = len(column_intensities)
    normsq = np.linalg.norm(column_intensities) ** 2
    best = None
    best_score = 0
    for x0 in range(0, n, 50):
        for x1 in range(x0 + 1, n, 50):
            contained = sum(column_intensities[x0:x1])
            sim = contained / np.sqrt((x1 - x0) * normsq)
            if best is None or sim > best_score:
                best = (x0, x1)
                best_score = sim
    x0, x1 = best
    xdim = x1 - x0
    best = None
    best_score = 0
    for y0 in range(0, cropped.shape[0] - xdim, 50):
        brightness = np.mean(gray[y0 : (y0 + xdim), x0:x1], axis=(0, 1))
        if best is None or brightness > best_score:
            best = y0
            best_score = brightness
    cropped = cropped[best : (best + xdim), x0:x1, :]
    return cropped
