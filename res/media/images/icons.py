"""
Wll be adapting from https://github.com/Pythonity/icon-font-to-png/blob/master/icon_font_to_png/icon_font.py
Add icons to the S3 font location which is fetch locally in k8s for now
"""
import os
import re
from collections import OrderedDict
import tinycss
from PIL import Image, ImageFont, ImageDraw, ImageOps
from six import unichr
import io
import numpy as np
import res
import pandas as pd
import xml.dom.minidom as md

FONT_LOCS = os.environ.get("FONT_LOCATION", ".")


def extract_icons(path="s3://res-data-platform/icons/pack.zip", **kwargs):
    import s3fs
    from zipfile import ZipFile
    import io

    # "s3://meta-one-assets-prod/color_on_shape/cc_6001/v10/somelu/zipped_pieces.zip"

    res.utils.logger.debug(f"Extracting icons from {path} to . ")
    fs = s3fs.S3FileSystem()
    with ZipFile(io.BytesIO(fs.cat(path))) as input_zip:
        input_zip.extractall(".")


def rgb_to_hex(r, g, b):
    return ("#{:X}{:X}{:X}").format(r, g, b)


def get_svg(
    name,
    root=None,
    # this is a simple temp thing for now until we add more icon utils - this is essentially for shading in gray scale
    # so supply a number for 0 - 255
    color=None,
    mirror=False,
    flip=False,
    rotate=None,
    if_missing_key=None,
    opacity=None,
    color_rgba=None,
    resource_path=None,
    **kwargs,
):
    """
    for example download font awesome fonts https://fontawesome.com/download
    install cairosvg not adding as requirement for sure...

    rotates or done first on the source icon followed by others
    """
    from cairosvg import svg2png

    light_mode = kwargs.get("light_mode")

    # TODO we should root this as RES_FONT
    root = root or os.environ.get(
        "RES_FONT_SVG", "./svg"
    )  # s3://res-data-platform/icons/svg
    file = f"{root}/{name}.svg"

    if "s3://" in root:
        s3 = res.connectors.load("s3")
        try:
            f = s3.read(file)
        except:
            raise FileNotFoundError(f"Unable to read icon file {file}")
    else:
        try:
            with open(file) as f:
                f = f.read()
        except FileNotFoundError as fex:
            res.utils.logger.warn(
                f"The icon requested {name} could not be loaded - {file}"
            )
            # local debug helper - we could support this in prod but not sure sure
            mfile = f"{root}/{if_missing_key}.svg"
            if if_missing_key and mfile != if_missing_key:
                with open(mfile) as f:
                    f = f.read()
            else:
                raise fex

    col = kwargs.get("currentColor")
    if color_rgba:
        # use the 4 channel to get the hex color and opacity
        col = rgb_to_hex(*list(color_rgba)[:3])
        if len(color_rgba) > 3:
            opacity = color_rgba[-1] / 255.0
    if col and not pd.isnull(col):
        f = f.replace("currentColor", f"rgba{col}").replace("black", f"rgba{col}")
    if light_mode:
        # https://johndecember.com/html/spec/colorsvg.html
        # SA careful here we need something more generic here - this is a convenience and it assumes the svg conventions
        f = f.replace("currentColor", "silver").replace("black", "silver")

    if opacity:
        d = md.parseString(f)
        d.documentElement.setAttribute("opacity", str(opacity))
        f = d.toxml()

    options = {
        k: v for k, v in kwargs.items() if k not in ["currentColor", "light_mode"]
    }

    try:
        im = Image.open(io.BytesIO(svg2png(f, **options)))
    except Exception as ex:
        raise Exception(f"Failed top open icon file {file}: {repr(ex)}")

    if color:
        arr = np.array(im)
        arr[arr > 0] = color
        im = Image.fromarray(arr)
    if rotate:
        im = im.rotate(rotate, expand=True)
    if mirror:
        im = ImageOps.mirror(im)
    if flip:
        im = ImageOps.flip(im)

    return im


def get_number_as_dice(i, **kwargs):
    """
    supports up to 6*3 codes for seams
    """
    nums = ["one", "two", "three", "four", "five", "six"]
    shade = 200
    i = int(i)
    if i < 6:
        return get_svg(f"solid/dice-{nums[i]}", scale=0.1, color=shade)
    elif i < 12:
        ims = [
            get_svg(f"solid/dice-six", scale=0.1, color=shade),
            get_svg(f"solid/dice-{nums[i%6]}", scale=0.1, color=shade + 10),
        ]
        return Image.fromarray(np.concatenate([np.asarray(i) for i in ims], axis=1))
    else:
        ims = [
            get_svg(f"solid/dice-six", scale=0.1, color=shade),
            get_svg(f"solid/dice-six", scale=0.1, color=shade + 10),
            get_svg(f"solid/dice-{nums[i%6]}", scale=0.1, color=shade + 20),
        ]
        return Image.fromarray(np.concatenate([np.asarray(i) for i in ims], axis=1))


class IconFont(object):
    """Base class that represents web icon font"""

    def __init__(
        self,
        # these are assumed to be downloaded by a font downloader to local or somehow mapped
        css_file="font-awesome.css",
        ttf_file="fontawesome-webfont.ttf",
        keep_prefix=False,
    ):
        """
        :param css_file: path to icon font CSS file
        :param ttf_file: path to icon font TTF file
        :param keep_prefix: whether to keep common icon prefix
        """

        RES_FONTS_FOLDER = os.environ.get("RES_FONT_FOLDER", ".")
        # temp as we work out or font strategy
        self.css_file = f"{RES_FONTS_FOLDER}/{css_file}"
        self.ttf_file = f"{RES_FONTS_FOLDER}/{ttf_file}"

        self.keep_prefix = keep_prefix
        self.css_icons, self.common_prefix = self.load_css()

    def load_css(self):
        """
        Creates a dict of all icons available in CSS file, and finds out
        what's their common prefix.
        :returns sorted icons dict, common icon prefix
        """
        icons = dict()
        common_prefix = None
        parser = tinycss.make_parser("page3")
        stylesheet = parser.parse_stylesheet_file(self.css_file)

        is_icon = re.compile("\.(.*):before,?")

        for rule in stylesheet.rules:
            selector = rule.selector.as_css()

            # Skip CSS classes that are not icons
            if not is_icon.match(selector):
                continue

            # Find out what the common prefix is
            if common_prefix is None:
                common_prefix = selector[1:]
            else:
                common_prefix = os.path.commonprefix((common_prefix, selector[1:]))

            for match in is_icon.finditer(selector):
                name = match.groups()[0]
                for declaration in rule.declarations:
                    if declaration.name == "content":
                        val = declaration.value.as_css()
                        # Strip quotation marks
                        if re.match("^['\"].*['\"]$", val):
                            val = val[1:-1]
                        icons[name] = unichr(int(val[1:], 16))

        common_prefix = common_prefix or ""

        # Remove common prefix
        if not self.keep_prefix and len(common_prefix) > 0:
            non_prefixed_icons = {}
            for name in icons.keys():
                non_prefixed_icons[name[len(common_prefix) :]] = icons[name]
            icons = non_prefixed_icons

        sorted_icons = OrderedDict(sorted(icons.items(), key=lambda t: t[0]))

        return sorted_icons, common_prefix

    def get_icon(
        self,
        icon,
        size=50,
        color="black",
        scale="auto",
        mirror=False,
        flip=False,
    ):
        """
        Gets given icon with provided parameters.
        If the desired icon size is less than 150x150 pixels, we will first
        create a 150x150 pixels image and then scale it down, so that
        it's much less likely that the edges of the icon end up cropped.
        :param icon: valid icon name
        :param size: icon size in pixels
        :param color: color name or hex value
        :param scale: scaling factor between 0 and 1,
                      or 'auto' for automatic scaling
        """
        org_size = size
        size = max(150, size)

        image = Image.new("RGBA", (size, size), color=(0, 0, 0, 0))
        draw = ImageDraw.Draw(image)

        if scale == "auto":
            scale_factor = 1
        else:
            scale_factor = float(scale)

        font = ImageFont.truetype(self.ttf_file, int(size * scale_factor))
        width, height = draw.textsize(self.css_icons[icon], font=font)

        # If auto-scaling is enabled, we need to make sure the resulting
        # graphic fits inside the boundary. The values are rounded and may be
        # off by a pixel or two, so we may need to do a few iterations.
        # The use of a decrementing multiplication factor protects us from
        # getting into an infinite loop.
        if scale == "auto":
            iteration = 0
            factor = 1

            while True:
                width, height = draw.textsize(self.css_icons[icon], font=font)

                # Check if the image fits
                dim = max(width, height)
                if dim > size:
                    font = ImageFont.truetype(
                        self.ttf_file, int(size * size / dim * factor)
                    )
                else:
                    break

                # Adjust the factor every two iterations
                iteration += 1
                if iteration % 2 == 0:
                    factor *= 0.99

        draw.text(
            (float(size - width) / 2, float(size - height) / 2),
            self.css_icons[icon],
            font=font,
            fill=color,
        )

        # Get bounding box
        bbox = image.getbbox()

        # Create an alpha mask
        image_mask = Image.new("L", (size, size), 0)
        draw_mask = ImageDraw.Draw(image_mask)

        # Draw the icon on the mask
        draw_mask.text(
            (float(size - width) / 2, float(size - height) / 2),
            self.css_icons[icon],
            font=font,
            fill=255,
        )

        # Create a solid color image and apply the mask
        icon_image = Image.new("RGBA", (size, size), color)
        icon_image.putalpha(image_mask)

        if bbox:
            icon_image = icon_image.crop(bbox)

        border_w = int((size - (bbox[2] - bbox[0])) / 2)
        border_h = int((size - (bbox[3] - bbox[1])) / 2)

        # Create output image
        out_image = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        out_image.paste(icon_image, (border_w, border_h))

        # If necessary, scale the image to the target size
        if org_size != size:
            out_image = out_image.resize((org_size, org_size), Image.ANTIALIAS)

        if mirror:
            out_image = ImageOps.mirror(out_image)
        if flip:
            out_image = ImageOps.flip(out_image)

        return out_image


def get_limited_edition_placeholder(
    text, size, buffer=0, max_factor_base_10=3, font="BebasNeue.otf"
):
    """
    assumes a font in font location - you can set your env and copy the font in there
    on cluster we load it in from s3 fonts

    """
    font_name = f"{FONT_LOCS}/{font}"

    text = str(text).zfill(max_factor_base_10)
    # create image
    img = Image.new("RGBA", size, color=(255, 255, 255, 0))
    # measure the size of the text

    def font_dims(font_size):
        font = ImageFont.truetype(font=font_name, size=font_size)
        ascent, descent = font.getmetrics()
        bbox = font.getmask(text).getbbox()
        text_width = bbox[2]
        text_height = bbox[3] + descent

        return text_width, text_height, font

    font_size = 0
    text_height = 0
    text_width = 0
    while text_width < size[0] - buffer:  # and text_height < size[1] - buffer:
        font_size += 1
        text_width, text_height, font = font_dims(font_size)
    text_width, text_height, font = font_dims(font_size - 1)

    d = ImageDraw.Draw(img)
    # debug d.rectangle([(0, 0), size], fill=(200, 200, 255, 255))
    d.text(
        (size[0] / 2, 0),
        align="center",
        anchor="mt",
        text=text,
        font=font,
        fill=(0, 0, 0, 255),
    )

    return img.convert("RGBA")


def get_size_label_centered_circle(text, size, font="Chapman-Medium.otf", **kwargs):
    """
    get a size label with hard coded sizes and styles

    :param text: the text you want to write - it is assumed the text will be no more than three characters
    :param size:: the size tuple of the image - the font will be sized proportionally
    """

    # remove the size
    text = text.replace("Size ", "")

    im = Image.new("RGBA", size, color=(0, 0, 0, 0))
    draw = ImageDraw.Draw(im)

    center_x, center_y = size[0] // 2, size[1] // 2
    # a little padding to shrink the circle
    circle_radius = (min(size) // 2) - 2
    draw.ellipse(
        [
            (center_x - circle_radius, center_y - circle_radius),
            (center_x + circle_radius, center_y + circle_radius),
        ],
        outline="black",
    )

    x, y = im.size[0] // 2, im.size[1] // 2
    font = ImageFont.truetype(
        font=f"{FONT_LOCS}/{font}",
        size=int(size[1] / 3),
    )
    (font_width, font_baseline), (offset_x, offset_y) = font.font.getsize(text)
    drawer = ImageDraw.Draw(im)
    (width, height) = drawer.textsize(text, font=font)
    offset_x, offset_y = font.getoffset(text)
    width += offset_x
    height += offset_y
    x -= width // 2
    y -= height // 2
    drawer.text((x, y), text, font=font, fill="black")

    return im


def get_size_label(text, size, font="ArchivoBlack-Regular.ttf", **kwargs):
    """
    get a size label with hard coded sizes and styles
    """

    from cairosvg import svg2png

    font = kwargs.get("font", "Helvetica")

    collar = f"""
    <svg width="160" height="160" xmlns="http://www.w3.org/2000/svg">

     <g>
      <circle cx="80" cy="80" r="75" fill="currentColor" stroke="currentColor" /> 
      
     </g>
    </svg>"""

    im = Image.open(io.BytesIO(svg2png(collar))).resize(size)
    s = int(size[1] * 0.5)

    font = ImageFont.truetype(
        font=f"{FONT_LOCS}/{font}",
        size=s,
    )
    (font_width, font_baseline), (offset_x, offset_y) = font.font.getsize(text)

    s = int(s - s / 2)

    drawer = ImageDraw.Draw(im)
    drawer.text((2 * s - int(font_width / 2.0), s), text, font=font, fill="white")

    return im


#####
##  certain types of ops
#####

SYMBOL_SIZE = (50, 50)


# =["bodice", "bodice", "sleeve", "bodice", "collar", "bodice", "wildcard"],
def construction_label(
    sequence,
    ic=None,
):
    """
    The construction label function provide a method to retrieve special symbols
    bodies have a construction order and on any seam we can place a sequence of symbols
    the  "order" of construction is shaded on the symbols to show where one is in the process

    each seam can also have specific operations on a seam; these are added as extra symbols after the construction symbol
    some seams do not show a construction order including those with construction order 0
    """

    try:
        # protect against other types of enums
        sequence = list(sequence)
    except:
        pass

    # safety - we should refactor
    if not isinstance(sequence, list):
        sequence = []

    # strict mode would not allow this TODO:
    # if not sequence:
    #     # this can happen sometimes if we send something as a seam allowance op that should instead be a notch label
    #     # for example the main labels are added as edge templates that get mapped to center notches so they are not really seam ops even though they are on specific edges
    #     raise ValueError(
    #         """A construction sequence is required to create a construction label.
    #            - Check of we have one in our lookups for this construction type.
    #            - For example Short-3-3 should be in the construction lookups `get_construction_sequence`"""
    #     )

    def load_icon(im, order, i, pad=2, size=SYMBOL_SIZE):
        temp = im
        o = int((size[1] - im.size[1]) / 2.0)
        # right pad by pasing in a slightly larger image
        im = Image.new(temp.mode, (size[0] + pad, size[1]), 0)
        im.paste(temp, (0, o))
        # if i >= order:
        #     arr = np.array(im)
        #     arr[arr > 0] = 50
        #     im = Image.fromarray(arr)
        return im

    ic = ic or IconFont()

    def _f(order, op_name=None):
        if pd.isnull(order):
            order = 0

        mapping = lambda n, i: get_svg(
            f"sew-{n}", currentColor="white" if i >= order else "black"
        ).resize(SYMBOL_SIZE)
        # this images should be padded and colors according to their order
        ims = [load_icon(mapping(k, i), order, i) for i, k in enumerate(sequence)]

        im = None
        if len(ims):
            im = Image.fromarray(np.concatenate(ims, axis=1))
            # draw a dotted line along the length of the image
            d = ImageDraw.Draw(im)

            # draw a line a little smaller than the length along the middle
            l = im.size[0] - 5
            d.line(
                [(2, int(im.size[1] / 2.0)), (l, int(im.size[1] / 2.0))],
                fill="gray",
                width=1,
            )

        if op_name is not None and op_name not in ["void", "res.void"]:
            _op = op_symbol(op_name)

            if im is not None:
                if _op.size[1] != im.size[1]:
                    # res.utils.logger.debug(
                    #     f"Warning: op icon {op_name}  if size {_op.size} needs to be resized because it has a different height to the construction symbols of sie {im.size}"
                    # )
                    _op = _op.resize((_op.size[0], SYMBOL_SIZE[-1]))
            if not order:
                return _op

            try:
                # combine the op after some space (note the blank space) and its dims
                im = Image.fromarray(
                    np.concatenate(
                        [
                            np.asarray(im),
                            # symbol size is in XY and we reverse here for numpy and added a channel
                            # 30 is the padding for now
                            np.zeros((SYMBOL_SIZE[-1], 30, 4), dtype=np.uint8),
                            np.asarray(_op),
                        ],
                        axis=1,
                    )
                )
            except Exception as ex:
                res.utils.logger.info(
                    f"Failing to process icons with op {op_name} and sequence {sequence}"
                )
                raise ex

        return im

    return _f


def get_binding_op_pair():
    a1 = get_svg("i-cursor", scale=0.1)
    a2 = get_svg("won-sign", scale=0.1, rotate=-90)
    im = Image.new(a1.mode, (140, 60), 0)
    im.paste(a2, (0, 2))
    im.paste(a1, (80, 2))
    return im.rotate(90, expand=True)


def op_symbol(name, ic=None, **kwargs):
    ic = ic or IconFont()
    # if name == "french-seam":
    #     return ic.get_icon("shekel-sign", size=60).resize((60, 60))
    # if name == "five-inch-over":
    #     return get_svg("won-sign", scale=0.1, rotate=90).resize((60, 60))
    # todo create something special for this one
    if name == "five-inch-over_and_binding":
        return get_binding_op_pair()
    if name == "cut":
        return ic.get_icon("cut", size=60, mirror=True).resize(SYMBOL_SIZE)

    name = "sew-" + name
    # check what kwargs we can send to svg - we are mostly using flips and rotates to reuse symbols - we may also change colors etc
    return get_svg(name, if_missing_key="sew-missing", **kwargs)  # .resize(SYMBOL_SIZE)

    raise Exception(f"unknown operation {name}")


def notch_symbol(name, position=None, ic=None, pose="right", **kwargs):
    """
    actual size notch symbols
    """
    flip = kwargs.get("flip")
    S = 40
    NOTCH_SYMBOL_SIZE = (S, S)
    if name in ["snip_notch", "snip_fuse_notch"]:
        # no need to flip these, they are not really directional
        return IconFont().get_icon(
            "cut", size=S, mirror=False, flip=False
        )  # .resize(NOTCH_SYMBOL_SIZE)

    # TODO - still working on icon pol
    if "sa_piece_fuse" in name:
        flip = position == "after"
        return get_svg("solid/check-double", mirror=True, flip=flip, rotate=-45).resize(
            NOTCH_SYMBOL_SIZE
        )

    if "sa_directed_arrow" in name:
        # buying time with this logic - we should really have a convention for placement lines
        # and the arrow or any directed object is assumed to flow along the placement line
        flip = False
        if pose == "left":
            flip = True

        return get_svg(
            "solid/up-long",
            # mirror=mirror, #it doesnt make a difference this is symmetric in this dir i think
            flip=flip,
        ).resize(SYMBOL_SIZE)

    return get_svg(f"sew-{name}", **kwargs)  # .resize(NOTCH_SYMBOL_SIZE)


def notch_annotation(name, ic=None, **kwargs):
    """
    All symbols are defines for a default sense e.g. -> right
    if a relative placement is records as left, then we should flip them around for certain cases
     eg. not before and after which is just clockwise but for those relative to an avatar keypoint
     TODO: think about a conversion from sense to relative clockwise
    """
    if "sa_bidirectional" in name:
        return get_svg("solid/right-left").resize(SYMBOL_SIZE)

    # TBD: these annotations are design to be pointing left/right so we mirror as required
    # flipping is not needed in this case
    mirror = kwargs.get("mirror", kwargs.get("pose", "right") == "left")
    # get from args but default based on pose
    flip = kwargs.get("flip", False)

    if "one_piece_clean_close" in name:
        if "outside" in name:  # toggle what we have for whatever pose
            flip = not flip
        return get_svg(
            # "solid/right-from-bracket",
            "sew-one_piece_clean_close",
            mirror=mirror,
            flip=flip,
        ).resize(SYMBOL_SIZE)

    if "sa_tack_directional" in name:
        if "outside" in name:  # toggle what we have for whatever pose
            flip = not flip
            mirror = not mirror
            # this may depend on the side we are on
        mirror = False  # <-choosing this because the right pieces were right
        if "before" in name:  # toggle what we have for whatever pose
            flip = not flip
            mirror = not mirror
        return get_svg(
            # "solid/right-from-bracket",
            "sew-sa_tack_directional",
            mirror=mirror,
            flip=flip,
        ).resize(SYMBOL_SIZE)

    if "sa_directional" in name:
        # for now if we always mirror with the line it might works for the sleeves
        # mirror = False  # kwargs.get("pose", "right")
        mirror = True
        if kwargs.get("pose") == "left":
            mirror = False

        if "outside" in name:  # toggle what we have for whatever pose
            mirror = not mirror

        if "reversed" in name:
            mirror = not mirror

        return get_svg(
            "solid/right-long",
            mirror=mirror,
            flip=flip,
        ).resize(SYMBOL_SIZE)

    if "sa_piece_fuse" in name:
        # not sure if this is true - we go clockwise so this is outside and following the notch??

        mirror = "after" in name
        return get_svg(
            "solid/check-double", mirror=mirror, flip=flip, rotate=-45
        ).resize(SYMBOL_SIZE)

    if name == "compose-edge_stitched_seam-binding":
        # this one is a hack for now because we dont know enough to describe this
        im = Image.fromarray(
            np.concatenate(
                [
                    np.asarray(op_symbol("edge_stitched_seam")),
                    # symbol size is in XY and we reverse here for numpy and added a channel
                    np.zeros((SYMBOL_SIZE[-1], 20, 4), dtype=np.uint8),
                    np.asarray(op_symbol("binding")),
                ],
                axis=1,
            )
        )

        im = im.rotate(-90, expand=True)

        return im


def get_identity_label(key, symbol_size=(52, 52), **kwargs):
    if "identity-" not in key:
        key = f"identity-{key}"

    is_interior = kwargs.get("is_interior")
    identity_qualifier = kwargs.get("identity_qualifier")
    pairing_qualifier = kwargs.get("pairing_qualifier")

    if key == "" or key == "identity-":
        key = "identity-neutral"
    if is_interior:
        key += "-interior"
    if identity_qualifier:
        key += identity_qualifier
    if pairing_qualifier:
        key = f"{pairing_qualifier}-{key}"

    res.utils.logger.debug(f"Using identity icon: {key} from kwargs {kwargs}")
    ic = get_svg(f"sew-{key}")

    if symbol_size:
        return ic.resize(symbol_size)
    return ic


# deprecate
def identity_label(piece_code, pose="right", is_inside=False, **kwargs):
    if pd.isnull(piece_code):
        piece_code = ""

    symbol = "sew-identity_headless"

    flip = False

    if "TOP" in piece_code[-4:]:
        symbol = "sew-identity"
    elif "U" in piece_code[-2:]:
        symbol = "sew-identity"
        flip = True

    if pose == "center":
        symbol += "_centered"

    if is_inside:
        symbol += "_inside"
    # if the piece is under use the dotted line etc.

    # the default identity is this none
    return get_svg(symbol, mirror=(pose != "right"), flip=flip).resize(SYMBOL_SIZE)


def image_props_at_point(im, pt, radius=100, plots=False, **kwargs):
    """
    sample an image and get some color props for label placement e.g. best colors
    """
    import cv2

    def sample_near(im, xc, yc):
        im = np.asarray(im)
        mask = np.zeros(im.shape[:2], dtype=np.uint8)
        mask = cv2.circle(mask, (xc, yc), radius, 255, -1)

        return im[np.ix_(mask.any(1), mask.any(0))]

    def perceptive_luminance(row):
        rgb = np.array(row)[:3]
        coef = np.array([0.2126, 0.7152, 0.0722])

        return np.dot(rgb, coef) / 255.0

    sample = sample_near(im, int(pt.x), int(pt.y))

    chk = pd.DataFrame(
        np.asarray(sample)[:, :, :3].mean(axis=1), columns=["R", "G", "B"]
    )
    mean_color = chk.mean(axis=0).astype(int)
    chk["average_color"] = chk["R"].map(lambda x: tuple(mean_color))
    chk["perceptive_luminance"] = chk.apply(perceptive_luminance, axis=1)
    median_l, mean_l = (
        chk["perceptive_luminance"].median(),
        chk["perceptive_luminance"].mean(),
    )

    chk["mean_lum"] = mean_l
    chk["median_lum"] = median_l

    # default middle
    col = (255 * mean_l * np.array([1, 1, 1, 1])).astype(int)
    col[-1] = 200
    # todo we may need a better function for this
    # we are controlling
    threshold = 0.85
    if median_l > threshold:
        col = (50, 50, 50, 130)
    if median_l <= threshold:
        col = (250, 250, 250, 250)

    chk["suggested_foreground_color"] = chk["R"].map(lambda x: tuple(col))

    if kwargs.get("spoof_white_background"):
        chk["average_color"] = chk["R"].map(lambda x: (255, 255, 255, 255))
        # below is using the same color as we do in high lim settings
        # we also turn down the opacity when we have dark backgrounds as we dont need it
        chk["suggested_foreground_color"] = chk["R"].map(
            lambda x: (50, 50, 50, 130 if median_l > threshold else 240)
        )

    if plots:
        chk[["perceptive_luminance"]].hist()

    return chk


def get_color_text_label(text, font_color, back_color, **kwargs):
    """
    get text with proper colors and backgrounds
    color of outlines and other things can be added
    """
    from res.media.images.text import get_text_image

    outlines = (0, 0, 0, 0)

    if kwargs.get("outline_in_foreground_color"):
        outlines = font_color

    t = get_text_image(
        text,
        fill_color=font_color,
        stroke_color=(0, 0, 0, 0),
        stroke_color2=(0, 0, 0, 0),
    )

    tim = np.asarray(t)
    canvas_size = list(tim.shape)
    square_ratio = canvas_size[0] / canvas_size[1]

    # some padding
    canvas_size[0]
    canvas_size[1] += 2  # += 2

    # create back canvas
    canvas = Image.fromarray(np.zeros(canvas_size, dtype=np.uint8))
    draw = ImageDraw.Draw(canvas)

    draw.rectangle(
        ((0, 0), canvas.size[0] - 1, canvas.size[1] - 1),
        outline=outlines,
        fill=back_color,
    )

    canvas.paste(t, (1, 1), mask=t)
    return canvas


def get_color_label(label_name, label_color, back_color=(0, 0, 0, 0), **kwargs):
    """
    as for the text, we can load labels e.f. svg labels
    """
    l = get_svg(label_name, color_rgba=label_color)

    outlines = (0, 0, 0, 0)

    if kwargs.get("outline_in_foreground_color"):
        outlines = label_color

    tim = np.asarray(l)
    canvas_size = list(tim.shape)

    square_ratio = canvas_size[0] / canvas_size[1]

    # some padding
    canvas_size[0]
    canvas_size[1] += 2  # += 2

    # create back canvas - we can create circular ones for smaller labels or ones that are squareish in shape
    canvas = Image.fromarray(np.zeros(canvas_size, dtype=np.uint8))
    draw = ImageDraw.Draw(canvas)
    if square_ratio < 0.75:
        draw.rectangle(
            ((0, 0), canvas.size[0] - 1, canvas.size[1] - 1),
            outline=outlines,
            fill=back_color,
        )
    else:
        draw.ellipse(
            ((0, 0), canvas.size[0] - 1, canvas.size[1] - 1),
            outline=outlines,
            fill=back_color,
        )

    canvas.paste(l, (1, 1), mask=l)

    return canvas


def get_color_text_label_for_image_site(im, pt, text, **kwargs):
    """
    Create a blended background using the average sample color at label site
    And determines the foregroupd using the luminence of the image + some heuristics
    """
    props = dict(image_props_at_point(im, pt, **kwargs).iloc[-1])
    mean_color = props["average_color"]
    col = props["suggested_foreground_color"]

    return get_color_text_label(text, font_color=col, back_color=mean_color, **kwargs)


def get_color_label_for_image_site(im, pt, label_name, **kwargs):
    """
    Create a blended background using the average sample color at label site
    And determines the foregroupd using the luminence of the image + some heuristics
    a secondary color could be determined for averages in background but we do not support that yet but assume labels are designed with one color
    """
    props = dict(image_props_at_point(im, pt, **kwargs).iloc[-1])
    mean_color = props["average_color"]
    col = props["suggested_foreground_color"]

    return get_color_label(label_name, col, back_color=mean_color, **kwargs)
