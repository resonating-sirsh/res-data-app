import res
from PIL import Image, ImageFont, ImageDraw
import os

# something should fetch fonts - if the s3 location is used for fonts then we
S3_FONT_LOCS = "s3://res-data-platform/fonts/"
FONT_LOCS = os.environ.get("FONT_LOCATION", ".")
PROPERTIES = {
    "dxa.labels.font": "Helvetica",
    "dxa.labels.font_size": 40,
    "dxa.labels.height": 50,
}

PAL1 = [
    (255, 0, 0),  # Red
    (0, 255, 0),  # Green
    (0, 0, 255),  # Blue
    (128, 0, 128),  # Purple
    (255, 165, 0),  # Orange
    (255, 0, 255),  # Magenta
    (255, 255, 0),  # Yellow
    (139, 69, 19),  # Brown
    (0, 255, 255),  # Cyan
    (0, 128, 128),  # teal
]


def label(text, **kwargs):
    """
    basic text drawing - TODO - add features to make this more useful
    """
    fill_color = (42, 42, 42, 255)
    size = 16
    padding = 10
    font = ImageFont.truetype(font=f"{FONT_LOCS}/Helvetica.ttf", size=size)
    (font_width, font_baseline), (offset_x, offset_y) = font.font.getsize(text)
    ascent, descent = font.getmetrics()
    font_height = ascent + descent
    font_size = (
        font_width + 2 * padding,
        font_height + 2 * padding,
    )  # (font_width, size)

    im = Image.new("RGBA", font_size, "white")
    drawer = ImageDraw.Draw(im)

    xy = (padding, padding)
    # generate three tiers: we also add a transparent padding, we optionally add a second color and we always add a border
    drawer.text(xy, text, font=font, fill=fill_color)

    return im


def ensure_s3_fonts(to=None):
    FONT_LOC = os.environ.get("FONT_LOCATION", to or ".")
    probe_font_name = None
    try:
        res.utils.logger.debug("probing for fonts")
        # this is a temporary solution - we have no font strategy and we need to think about cluster/non cluster and good locations etc.
        # this is a way to check if we have one expected font or we download from the s3 folder for the font set
        probe_font_name = "DejaVuSans"
        _ = ImageFont.truetype(font=f"{FONT_LOCS}/{probe_font_name}.ttf", size=12)
        probe_font_name = "Helvetica"
        _ = ImageFont.truetype(font=f"{FONT_LOCS}/{probe_font_name}.ttf", size=12)
        res.utils.logger.debug("probing for fonts")
        probe_font_name = "Archivo Black"
        _ = ImageFont.truetype(font=f"{FONT_LOCS}/{probe_font_name}.ttf", size=12)
    except:
        res.utils.logger.info(
            f"Failed to fetch the sample font {probe_font_name} - downloading font pack from S3 and using font location {FONT_LOCS} - set the Font location to avoid this in future"
        )
        s3 = res.connectors.load("s3")
        for f in s3.ls(S3_FONT_LOCS):
            s3._download(f, target_path=FONT_LOC)


def pull_config():
    # go to the config manager and get the properties that can be changed globally
    # override the defaults here
    PROPERTIES = {}


def get_text_image(
    text,
    size=None,
    height=None,
    font_name=None,
    fill_color=(100, 100, 100, 255),
    # stroke color - off
    stroke_color=(0, 0, 0, 0),
    # default to transparent but keep the size for consistency - this is the outer stroke
    stroke_color2=(0, 0, 0, 0),
    auto_clip=False,
):
    """
    Get text used in dxa labels etc. apply some conventions for styles
    """

    # get the properties
    font_name = font_name or PROPERTIES.get("dxa.labels.font")
    size = size or PROPERTIES.get("dxa.labels.font_size")
    height = height or PROPERTIES.get("dxa.labels.height")

    font = ImageFont.truetype(font=f"{FONT_LOCS}/{font_name}.ttf", size=size)

    (font_width, font_baseline), (offset_x, offset_y) = font.font.getsize(text)
    ascent, descent = font.getmetrics()
    font_height = ascent + descent
    font_size = (font_width, font_height)  # (font_width, size)

    # ^ im not using font size here but really we need to use the biggest of a set - so instead we are picking some sort of max size
    # we need to concat labels hence the need to consist

    HPAD = 2
    im = Image.new("RGBA", font_size, (255, 255, 255, 0))
    drawer = ImageDraw.Draw(im)
    # determine so as to center

    # convoluted way to adjust for the centroid of the font
    factor = 0  # size - (descent + font_baseline)
    # use the padding we are adding the to font and the adjustment factor to pick a standard location as a function of size
    xy = (0, -(HPAD * 5) + factor)
    xy = (0, 0)
    # generate three tiers: we also add a transparent padding, we optionally add a second color and we always add a border
    drawer.text(
        xy,
        text,
        font=font,
        fill=fill_color,
        stroke_width=HPAD * 3,
        stroke_fill=(0, 0, 0, 0),
    )
    drawer.text(
        xy,
        text,
        font=font,
        fill=fill_color,
        stroke_width=HPAD * 2,
        stroke_fill=stroke_color2,
    )
    drawer.text(
        xy,
        text,
        font=font,
        fill=fill_color,
        stroke_width=HPAD,
        stroke_fill=stroke_color,
    )

    stretch = ((height * 100) / font_height) / 100.0

    # the same size image is always returned
    im = im.resize((int(stretch + font_width), height))

    if auto_clip:
        # remove useless parts of the image to make it as short as possible
        im = np.asarray(im)
        v = im.sum(axis=1).sum(axis=1)
        im = im[v > 0, :, :]
        im = Image.fromarray(im)

    return im


import numpy as np


def small_circle(color, outline=None, size=50, pad=10):
    s = size
    im = Image.new("RGBA", (s, s), color=(255, 255, 255, 1))
    draw = ImageDraw.Draw(im)

    # draw.ellipse((pad, pad, s-pad, s-pad), fill='black', outline="black")
    draw.ellipse((2, 2, s - pad, s - pad), fill=color, outline=outline)
    return im


def small_square(color, outline=None, size=50, pad=10):
    s = size
    im = Image.new("RGBA", (s, s), color=(255, 255, 255, 1))
    draw = ImageDraw.Draw(im)

    # draw.ellipse((pad, pad, s-pad, s-pad), fill='black', outline="black")
    draw.rectangle((0, 2, s - pad, s - pad), fill=color, outline=outline)
    return im


# (150,150,150,255) this color is a dark shaded the looks ok on white


def get_piece_type_image(piece_type):
    if piece_type == "BF":
        return small_square((150, 150, 150, 255))
    return small_square("white", (150, 150, 150, 255))


def get_piece_make_type_image(piece_type):
    # precedence
    if piece_type == "healing":
        return small_circle((240, 10, 10, 200))
    if piece_type == "extra":
        return small_circle((150, 150, 150, 255))

    return small_circle("white", (150, 150, 150, 255))


def get_one_label(
    one_number, piece_key=None, projection=None, as_numpy=False, **kwargs
):
    """
    Generate a label for this order, for this particular piece
    this is a first approximation of what we want
    we will generalized this to produce some funcion of a one over a symbol alphabet
    we auto clip the image to save space in the y axis. remove useless parts of the label that have no information e.g. all transparent rows

    for our chosen font these are the symbols we can use
    https://www.fileformat.info/info/unicode/font/dejavu_sans_mono_bold/blockview.htm?block=miscellaneous_symbols
    lookup some unicode values to copy paste symbols
    https://www.compart.com/en/unicode/U+2638

    """

    one_number = str(one_number)

    shapes = ["♜", "♚", "♝", "⚛", "♥", "★", "♛", "✪", "⚂", "☸"]
    shapes2 = ["✖", "✤", "✦", "✿", "☊", "❖", "☀", "☂", "☯", "⚘"]  # "⬢"

    colors = PAL1

    one_number_projection = one_number

    if projection:
        # assume a function that maps string one number of string code of length 4 or more
        one_number_projection = projection(one_number)

    auto_clip = kwargs.get("one_label_autoclip", True)
    color = kwargs.get("color", (0, 0, 0, 255))
    one_text_color = kwargs.get("one_label_text_color", color)
    # choose a narrow font
    one_text_font = kwargs.get("one_label_font", "Helvetica")

    texts = [
        # {
        #     "text": shapes2[int(one_number_projection[-1])],
        #     "font_name": "DejaVuSans",
        #     "fill_color": colors[int(one_number_projection[-2])],
        # },
        # {
        #     "text": shapes[int(one_number_projection[-3])],
        #     "font_name": "DejaVuSans",
        #     "fill_color": colors[int(one_number_projection[-4])],
        # },
        {
            "text": one_number,
            "font_name": one_text_font,
            "fill_color": one_text_color,
        },
        {
            "text": kwargs.get("my_part"),
            "font_name": one_text_font,
            "fill_color": one_text_color,
        },
    ]

    im = np.concatenate(
        [
            get_piece_make_type_image(kwargs.get("piece_make_type")),
            get_piece_type_image(kwargs.get("piece_type")),
        ]
        + [get_text_image(**t) for t in texts],
        axis=1,
    )

    if auto_clip:
        # remove useless parts of the image to make it as short as possible
        v = im.sum(axis=1).sum(axis=1)
        im = im[v > 0, :, :]

    return im if as_numpy else Image.fromarray(im)
