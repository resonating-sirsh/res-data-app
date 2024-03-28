from PIL import Image
import numpy as np
import os
from datetime import datetime

from .. import logger

try:
    pass
except Exception as ex:
    # SA: library conflict and need to unblock
    logger.warn(f"Load error on mile marker (vips): {repr(ex)}")
    if not os.environ.get("IGNORE_VIPS_LOAD_ERROR"):
        raise

from . import text
from . import qr
from . import icons


# conventions assumed
DPI = 300
# e.g. see https://www.pixelto.net/px-to-mm-converter Dots per millimeter
DPMM = 1 / 0.84667


def plot_geometry(g, figsize=(20, 20)):
    """
    convenience
    """
    from geopandas import GeoDataFrame

    GeoDataFrame(g, columns=["geometry"]).plot(figsize=figsize)


def sample_near_point(im, xc, yc, radius=10):
    import cv2

    im = np.asarray(im)
    mask = np.zeros(im.shape[:2], dtype=np.uint8)
    mask = cv2.circle(mask, (xc, yc), radius, 255, -1)

    return im[np.ix_(mask.any(1), mask.any(0))]


def get_perceptive_luminance_at_point(pt):
    coef = np.array([0.2126, 0.7152, 0.0722])
    return np.dot(np.array(pt[:3]), coef) / 255.0


def get_perceptive_luminance(im, points):
    """
    wip sampler of images at points
    for example we can sample all the notch locations of an image and get the average luminance to decide about symbol placing

    https://stackoverflow.com/questions/596216/formula-to-determine-perceived-brightness-of-rgb-color

    """
    import cv2
    import pandas as pd

    def sample_near(im, xc, yc, radius=10):
        im = np.asarray(im)
        mask = np.zeros(im.shape[:2], dtype=np.uint8)
        mask = cv2.circle(mask, (xc, yc), radius, 255, -1)

        return im[np.ix_(mask.any(1), mask.any(0))]

    def perceptive_luminance(row):
        rgb = np.array(row)[:3]
        coef = np.array([0.2126, 0.7152, 0.0722])

        return np.dot(rgb, coef) / 255.0

    samples = [sample_near(im, int(pt.y), int(pt.x)) for pt in points]

    samples = pd.concat(
        pd.DataFrame(np.asarray(sample)[:, :, :3].mean(axis=1), columns=["R", "G", "B"])
        for sample in samples
    )
    samples["s"] = samples.sum(axis=1)
    samples = samples[samples["s"] > 0]
    samples["l"] = samples.apply(perceptive_luminance, axis=1)

    # samples['l'].hist()
    l = np.round(samples["l"].mean(), 2)

    return 0 if np.isnan(l) else l


def make_square_thumbnail(uri_or_img, min_size=128, fill_color=(0, 0, 0, 0)):
    from PIL import Image
    import res

    s3 = res.connectors.load("s3")
    im = s3.read_image(uri_or_img) if isinstance(uri_or_img, str) else uri_or_img
    im.thumbnail(size=(min_size, min_size))
    x, y = im.size
    size = max(min_size, x, y)
    new_im = Image.new("RGBA", (size, size), fill_color)
    new_im.paste(im, (int((size - x) / 2), int((size - y) / 2)))
    return new_im


def combine_vertical(
    images, padding=None, max_width=None, mode="RGB", back_color="white"
):
    """
    image combiner - works on pil images but can generalize
    This requires a little worked to be generalized - for now white background icons and labels are built
    """
    s = np.stack([np.array(im.size) for im in images])
    max_w = s.max(axis=0)[0]
    sum_h = s.sum(axis=0)[-1]
    padding = padding or 0

    canvas = Image.new(mode, (max_w, sum_h + padding * len(images)), back_color)
    y = 0
    for i, im in enumerate(images):
        canvas.paste(im, (0, y))
        y += im.size[1] + padding

    return canvas


def combine_horizontal(
    images, padding=None, max_width=None, mode="RGB", back_color="white"
):
    """
    image combiner - works on pil images but can generalize
        This requires a little worked to be generalized - for now white background icons and labels are built
    """
    s = np.stack([np.array(im.size) for im in images])
    max_h = s.max(axis=0)[-1]
    sum_w = s.sum(axis=0)[0]
    padding = padding or 0

    canvas = Image.new(mode, (sum_w + padding * len(images), max_h), back_color)
    x = 0
    for i, im in enumerate(images):
        canvas.paste(im, (x, 0))
        x += im.size[0] + padding

    return canvas


def read(filename):
    from skimage.io import imread

    return imread(filename)


def make_header(key, **kwargs):
    """
    We combine images to make a header - return 4 channel even if three channel used upstream
    Generally in deployed code we default to alpha for compositing etc.
    """
    # git hash comes from the environment or can be passed in if not
    git_hash = os.environ.get("GIT_HASH", kwargs.get("git_hash", "abc123"))
    ts = datetime.utcnow().isoformat(" ", "seconds")
    qr_code = qr.encode(key, scale=6)
    symbol = icons.get_svg("transformer-2", scale=0.07).resize(qr_code.size)
    text_labels = combine_vertical(
        [text.label(" "), text.label(f"Build: {git_hash}"), text.label(ts)], padding=0
    )
    header = combine_horizontal([symbol, qr_code, text_labels], padding=0)

    im = Image.new("RGBA", (header.size[0] + 10, header.size[1] + 20), "white")
    im.paste(header, (0, 0))

    return im


def PIL_from_float_image(im):
    return Image.fromarray((im * 255).astype("uint8"))


def pyvips_init():
    import pyvips

    PIXEL_PER_MILLIMETER_SETTING = 300 / 25.4
    MAX_MEM = 4 * 1048576000
    pyvips.cache_set_max_mem(MAX_MEM)
