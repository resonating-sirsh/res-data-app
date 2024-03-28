"""
All public functions return images or image collections for image processing pipelines
"""
from numpy.core.defchararray import translate
from scipy.ndimage.filters import gaussian_filter
from . import logger
import numpy as np
from PIL import Image, ImageOps
from skimage.segmentation import find_boundaries
from skimage.measure import find_contours, label, regionprops
from scipy.ndimage.morphology import binary_fill_holes
from skimage.draw import polygon_perimeter, polygon2mask, circle_perimeter, line
from scipy.signal import convolve2d
from PIL import Image, ImageDraw
from sklearn.neighbors import BallTree, KDTree
from .keypoints import _harris_corners
import res
import pandas as pd
from shapely.geometry import Polygon
import cv2
from res.media.images.geometry import (
    shift_geometry_to_origin,
    name_part,
    swap_points,
    translate,
    remove_notch_from_outline,
    nearest_points,
    unary_union,
)
from skimage.morphology import binary_erosion
from res.utils.error_codes import ErrorCodes


Image.MAX_IMAGE_PIXELS = None
DEFAULT_OUTLINE_THICKNESS = 10
DEFAULT_OUTLINE_COLOR = (0, 0, 0)
# for now we adda default buffer on all clipped pieces around the separated pieces for the image pipeline
# this is super important as it provides the inverse mask for finding outlines making the pipline continuous
PIECE_PADDING = 10


class MaxWidthExceededError(ValueError):
    ...


def non_transparent_mask(image, **kwargs):
    # TODO: what we really want to do is find the full transparent parts and create a silhouette which is cheaper than grayscale which we dont care abut but is used in image ops
    mask = image[:, :, -1]
    return mask > 0


def non_white_mask(image, **kwargs):
    mask = image < 255
    return mask[:, :, 0]


def _mask_region_bbox(im, region):
    miny, minx, maxy, maxx = region.bbox
    mask = np.zeros((im.shape[0], im.shape[1]), dtype=np.bool)
    mask[miny:maxy, minx:maxx] = True
    return mask


def mask_marker(im):
    if im.shape[-1] == 4:
        return non_transparent_mask(im)
    return non_white_mask(im)


def get_piece_outline(im, pad=1):
    mim = mask_marker(im)
    shape = tuple(np.array(mim.shape) + 2 * pad)
    canvas = np.zeros(shape)
    canvas[pad:-pad, pad:-pad] = mim

    g = get_piece_outlines_from_mask(canvas)[0]

    return shift_geometry_to_origin(g)


def get_piece_outline_dataframe(im, pad=1):
    o = get_piece_outline(im)
    # we dont trust to say what is x or y - just what you save is what you get back as an outline
    return pd.DataFrame(o.coords, columns=["column_0", "column_1"])


def repeat_distance(im):
    """
    given a rectangular image, compare the wrapped end pixels on each axis
    return the mean absolute euclidean distance in color space of the pixels
    for perfectly wrapped tiles we would see no difference on the ends in x and y
    the caller can determine a threshold e.g. a tolerance away from zero in each axis
    """

    try:

        def d(row):
            dist = np.linalg.norm(
                np.array([row[0], row[1], row["2"]])
                - np.array([row["0bla"], row["1bla"], row["2bla"]])
            )
            return dist

        def g(row):
            return np.linalg.norm(np.array([row[0]]) - np.array([row["0bla"]]))

        f = d
        # if it's greyscale we need to add a channel
        if len(im.shape) == 2:
            im = im.reshape(im.shape[0], im.shape[1], 1)
            f = g

        a = pd.DataFrame(im[0, :, :])
        b = pd.DataFrame(im[-1, :, :])
        x = a.join(b, rsuffix="bla")
        x["dist"] = x.apply(f, axis=1)
        x = x["dist"].map(abs).mean()
        a = pd.DataFrame(im[:, 0, :])
        b = pd.DataFrame(im[:, -1, :])
        y = a.join(b, rsuffix="bla")
        y["dist"] = y.apply(f, axis=1)
        y = y["dist"].map(abs).mean()

        # x y inverted for image space

        return (x, y)
    except Exception as e:
        logger.info(f"repeat distance failed {e}")
        return (0, 0)


def tile_yoke(data_or_uri, thumbnail_size=None, return_properties=False):
    """
    utility to see how something tiles on a sample piece

    base examples:
     s3://resmagic/uploads/78ecfde3-5f4b-40ad-a818-fa3f216f1e9d.tif

    """
    WKT = """LINESTRING (2689.073 765.92, 2917.176 755.282, 3143.298 722.719, 3248.585 685.272, 3348.318 634.928, 3444.761 563.614, 3527.387 477.899, 3650.231 273.379, 3733.863 0, 5378.214 355.214, 5344.734 652.285, 5325.343 898.727, 5314.521 1152.552, 5313.395 1406.461, 5318.415 1660.463, 5326.028 1914.567, 5331.004 2064.269, 5099.489 2089.441, 4867.974 2114.683, 4636.357 2138.867, 4404.53 2160.863, 4051.659 2189.349, 3698.539 2211.783, 3345.07 2227.282, 2991.154 2234.966, 2689.107 2234.966, 2387.055 2234.966, 2033.145 2227.269, 1679.681 2211.758, 1326.564 2189.327, 973.698 2160.865, 47.211 2064.26, 52.186 1914.587, 59.806 1660.477, 64.826 1406.469, 63.697 1152.556, 52.871 898.726, 0 355.213, 1644.335 0.004, 1681.493 138.259, 1728.009 273.284, 1782.356 380.175, 1851.001 477.694, 1933.676 563.352, 2030.115 634.665, 2129.841 685.122, 2235.103 722.765, 2461.084 755.42, 2689.073 765.92)"""
    from res.media.images.outlines import place_artwork_in_shape
    from shapely.wkt import loads

    s3 = res.connectors.load("s3")
    artwork = (
        np.array(s3.read_image(data_or_uri))
        if isinstance(data_or_uri, str)
        else data_or_uri
    )
    r = loads(WKT)

    im = place_artwork_in_shape(r, artwork)

    if thumbnail_size:
        im.thumbnail(thumbnail_size)

    return im


def save_image_to_string(im, format="PNG"):
    import base64
    from io import BytesIO

    buffered = BytesIO()
    if isinstance(im, np.ndarray):
        im = Image.fromarray(im)
    im.save(buffered, format=format)

    return base64.b64encode(buffered.getvalue()).decode()


def load_image_from_string(s):
    import base64
    import io

    if not isinstance(s, bytes):
        s = s.encode()
    decoded_string = io.BytesIO(base64.b64decode(s))
    return Image.open(decoded_string)


def is_png_with_no_dpi(file):
    # PNG signature is 8 bytes: b'\x89PNG\r\n\x1a\n'
    png_signature = file.read(8)
    if png_signature != b"\x89PNG\r\n\x1a\n":
        return False  # Not a PNG file

    while True:
        length_bytes = file.read(4)
        if not length_bytes:
            break  # Reached end of file

        length = int.from_bytes(length_bytes, byteorder="big")
        chunk_type = file.read(4)
        if chunk_type == b"pHYs":
            # Found pHYs chunk
            return False

        # Move to next chunk (4 bytes for chunk type, 4 bytes for CRC)
        file.seek(length + 4, 1)

    return True  # pHYs chunk not found


def analyze_artwork(
    uri,
    include_piece_preview=False,
    include_artwork_tile=False,
    try_denoise=True,
    repeat_threshold=0.075,
    include_tile_preview=False,
):
    res.utils.logger.info("  analyzing artwork")
    """
    get a base 64 encoded thumbnail and send it along with some tests

    examples:

    s3://resmagic/uploads/f0309cb2-ff47-4a3f-b294-a7f890527ced.tif

    in the result we can look at the images if we include them e.g. if we returned data

    load_image_from_string(data['artwork'])
        load_image_from_string(data['artwork'])
        load_image_from_string(data['piece_preview'])
    """
    from skimage.restoration import denoise_wavelet
    import pyvips

    # convert to bool in case strings sent to us e.g. "true"
    include_piece_preview = str(include_piece_preview).lower() == "true"
    include_artwork_tile = str(include_artwork_tile).lower() == "true"
    include_tile_preview = str(include_tile_preview).lower() == "true"

    errors = []
    res.utils.logger.info(f"  loading {uri} (pyvips {pyvips.__version__})...")
    # artwork = res.connectors.load("s3").read_image(uri)
    # get the dimensions etc using pyvips
    s3 = res.connectors.load("s3")
    if not uri.startswith("s3://") or not s3.exists(uri):
        res.utils.logger.info(f"  image not found: {uri}")
        errors.append(ErrorCodes.IMAGE_NOT_FOUND)
        return {"errors": errors}

    stream = s3.file_object(uri)
    res.utils.logger.info(f"  creating pyvips image from stream...")
    image = pyvips.Image.new_from_buffer(stream.read(), "")
    # downsample so we don't OOM
    stream.seek(0)
    thumbnail = pyvips.Image.thumbnail_buffer(stream.read(), 1024).numpy()
    res.utils.logger.info("  loaded.")

    # im_dpi = artwork.info.get("dpi")
    TO_DPI = 25.4  # pyvips uses dots per mm
    im_dpi = (round(image.xres * TO_DPI), round(image.yres * TO_DPI))
    im_size = (image.width, image.height)

    # libvips defaults sometimes on a png if it has no dpi information, oddly
    # it'll default to 300dpi on mac and 72dpi on linux. let's check the raw
    # png stuff just to be sure
    # e.g. s3://resmagic/uploads/924150c3-842c-4ed5-95b2-581c0e02da54.png
    stream.seek(0)
    if is_png_with_no_dpi(stream):
        res.utils.logger.info("  image is png with no dpi")
        im_dpi = None

    res.utils.logger.info(f"  got dpi {im_dpi} and size {im_size}")

    def _get_props(artwork, dpi):
        res.utils.logger.info(f"get props image dpi {dpi}")
        dpi = (int(dpi[0]), int(dpi[1])) if dpi else None
        _repeat_distance = repeat_distance(artwork)

        if not dpi or not (dpi[0] == 300 and dpi[1] == 300):
            errors.append(ErrorCodes.IMAGE_DPI_NOT_SUPPORTED)
        if not "rgb" in image.interpretation:
            errors.append(ErrorCodes.COLOR_TYPE_NOT_SUPPORTED)

        props = {
            "dpi": dpi,
            "was_noise_removed": False,
            # i want this to be
            "repeat_distance": list(reversed(_repeat_distance)),
            # repeat distance is scaled for the yx numpy coords
            "normed_repeat_distance": (
                _repeat_distance[1] / artwork.shape[1],
                _repeat_distance[0] / artwork.shape[0],
            ),
            "artwork_size": im_size,
            "color_type": image.interpretation,
        }

        return props

    # artwork = np.asarray(artwork)

    res.utils.logger.info("  getting properties.")
    tests = []
    props = _get_props(thumbnail, dpi=im_dpi)

    # some tolerance on the repeat pattern and we also try the image with noise removed if we have to
    if (
        props["normed_repeat_distance"][0] > repeat_threshold
        or props["normed_repeat_distance"][1] > repeat_threshold
    ):
        if try_denoise:
            res.utils.logger.info("Trying with denoise as threshold exceeded")
            denoised_artwork = denoise_wavelet(
                thumbnail, channel_axis=-1, rescale_sigma=True
            )
            props = _get_props(denoised_artwork, dpi=im_dpi)
            props["was_noise_removed"] = True
            if (
                props["normed_repeat_distance"][0] > repeat_threshold
                or props["normed_repeat_distance"][1] > repeat_threshold
            ):
                tests.append("REPEAT_DISTANCE_EXCEEDS_THRESHOLD")

    tile_preview = None
    if include_tile_preview:
        res.utils.logger.info("  getting tile preview.")
        rolled = np.roll(thumbnail, thumbnail.shape[1] // 2, axis=1)
        rolled = np.roll(rolled, thumbnail.shape[0] // 2, axis=0)
        tile_preview = save_image_to_string(Image.fromarray(rolled))

    props.update(
        {
            "tests_failed": tests,
            "piece_preview": save_image_to_string(
                tile_yoke(thumbnail, thumbnail_size=(1000, 1000))
            )
            if include_piece_preview == True
            else None,
            "artwork": save_image_to_string(thumbnail)
            if include_artwork_tile == True
            else None,
            "tile_preview": tile_preview,
        }
    )
    if errors:
        props["errors"] = ErrorCodes.remove_duplicates(errors)

    res.utils.logger.info(f"  got properties {props}")

    if "repeat_distance" in props:
        x = np.array(props["repeat_distance"])
        props["repeat_distance"] = np.where(np.isnan(x), None, x).tolist()

    if "normed_repeat_distance" in props:
        x = np.array(props["normed_repeat_distance"])
        props["normed_repeat_distance"] = np.where(np.isnan(x), None, x).tolist()

    return props


def draw_polygon_shape_as_image(g, color="whitesmoke"):
    bounds = g.bounds[-2:]
    bounds = (int(bounds[0]), int(bounds[1]))
    image = Image.new("RGBA", bounds, color=color)
    draw = ImageDraw.Draw(image)
    draw.polygon([(int(p[0]), int(p[1])) for p in list(g.coords)], fill=color)
    return image


def place_artwork_in_shape(
    shape, artwork, offset_x=0, offset_y=0, simple_rectangle=False
):
    """
    Tile the artwork and clip the piece
    if the offset is given, we can assume the tiling starts somewhere away from the origin of the piece
    If the offset was absolute in the placement file for example (top left and right of the piece as it was originally), we could see how the tile tiles the entire placement,
      and then see where the grid lines fall and then relatively offset the origin of the piece to clip to the grid.
      In this case for example, offset_x could be the distance to the nearest gridline to the right
    """
    if isinstance(artwork, str):
        artwork = res.connectors.load("s3").read(artwork)

    tW = artwork.shape[1]
    tH = artwork.shape[0]
    bounds = shape.bounds
    shapeH = bounds[-1]
    shapeW = bounds[-2]
    # is it safe to round this-> the grid can only be an integer value but what is the true offset on the piece
    shapeW, shapeH = int(np.ceil(shapeW)), int(np.ceil(shapeH))
    cols, rows = int(np.ceil(shapeW / tW)), int(np.ceil(shapeH / tH))
    res.utils.logger.info(
        f"-Tiling a tile of H={tH},W={tW} across a shape of H={shapeH}, W={shapeW} requiring cols={cols} and rows {rows} with offset {offset_x, offset_y} - grid size XY {cols*tW, rows*tH} must be larger than the shape allowing for offset"
    )

    res.utils.logger.info("ensure artwork channels")
    artwork = ensure_4_channels(artwork)
    res.utils.logger.info(f"make {cols} cols")
    col = np.concatenate([artwork.copy() for i in range(cols)], axis=1)
    res.utils.logger.info(f"make grid with {rows} rows")
    grid = np.concatenate([col.copy() for i in range(rows)], axis=0)

    # we dont need a special mask
    if simple_rectangle:
        res.utils.logger.info(f"Using rectangle tiling mode ({shapeH},{shapeW})....")
        piece = grid[:shapeH, :shapeW, :]
        return Image.fromarray(piece)

    # clip a region of the grid correctly offset and just the same size as the shape
    res.utils.logger.info("make mask")
    m, b = get_mask(shape)

    maskH, maskW = m.shape[0], m.shape[1]
    grid = grid[offset_y : offset_y + maskH, offset_x : offset_x + maskW, :]

    try:
        res.utils.logger.info("apply the mask")
        piece = cv2.bitwise_and(grid, grid, mask=m)
    except Exception as ex:
        res.utils.logger.info(f"hit the edge case {ex}")
        # edge case - issue sometimes with drawing
        return draw_polygon_shape_as_image(shape)

    return Image.fromarray(piece)


def place_directional_color_in_outline(
    g, stripe, plaid, tile, add_notches=False, buffer_offset=None
):
    """
    you can pass None for stipe and plaid to assume a grid center in the centroid of the piece
    DEPRECATE in favour of `place_artwork_in_shape`
    """

    res.utils.logger.debug(f"tiling with tile of size {tile.shape}")
    s = int(tile.shape[0])

    # TODO assertions
    if stripe is not None and plaid is not None:
        grid_center = nearest_points(stripe, plaid)[0]
    else:
        grid_center = g.centroid

    def _scale(x):
        return int(np.ceil(x / s))

    l = _scale(grid_center.x)
    r = _scale(g.bounds[2] - grid_center.x)
    d = _scale(grid_center.y)
    u = _scale(g.bounds[3] - grid_center.y)

    # make an offset from the full integer repeat
    ox = int(s - (grid_center.x % s))
    oy = int(s - (grid_center.y % s))
    # this is the offset within the generated grid we pre-clip
    col = np.concatenate([tile.copy() for i in range(u + d)], axis=0)
    grid = np.concatenate([col.copy() for i in range(l + r)], axis=1)

    grid = grid[oy:, ox:, :]

    m, b = get_mask(g)
    part = grid[b[1] : b[3], b[0] : b[2], :]
    part = ensure_4_channels(part)

    return Image.fromarray(cv2.bitwise_and(part, part, mask=m))


def get_piece_outlines_from_mask(image_mask, **kwargs):
    """
    Return the outlines from the mask as shapely geometries
    This can be useful to plot

     #for plotting we swap points and invert axis for image space
     df = GeoDataFrame([swap_points(p) for p in polygons], columns=['geometry'])
     ax = gdf.plot(figsize=(10,10))
     ax.invert_yaxis()

    Or to generate image masks to slice out pieces from the origin image

     slice_pieces_using_piece_outlines(im, outlines)

    TODO this can fail when piece are too near the edge but we have a padding solution that resolves
    We dont want to blindly pad thoguh because some really large images we often see are far from outside
    #so we should test and pad

    """
    polygons = []
    mask_pad = kwargs.get("region_mask_padding", 1)

    def any_contains(p):
        for p_ in polygons:
            if p_.intersects(p):
                # try:
                #     inter = p_.intersection(p).area / p.area
                #     logger.debug(
                #         f"Found something that intersected with a ratio {inter}"
                #     )
                # except:
                #     pass
                return True
        return False

    if kwargs.get("erode"):
        logger.debug("eroding mask when detecting outlines")
        # this handles a case of low quality thumbnail approximate piece counting when there is distoration at the edges conencting islands
        image_mask = binary_erosion(image_mask)
        # also blur a little
        image_mask = gaussian_filter(image_mask, sigma=2)

    min_area = kwargs.get("min_area", 500)
    check_contains = kwargs.get("check_contains", True)

    boundaries = find_boundaries(image_mask)
    components = label(boundaries)
    props = regionprops(components, boundaries)

    for _, region in enumerate(props):
        miny, minx, maxy, maxx = region.bbox
        if min_area is not None and region.area < min_area:
            continue

        # use the region image which is a better partial mask that excludes nghs and avoids conflicts on biggest ply below
        # the countours are a better polygon to use
        # add the biggest contour as a polygon using the offset for the actual polygon which is needed both for ht tests and creating masks
        # mask padding is a safety for contour detection with boundary conditions
        P = biggest_contour_polygon(
            find_contours(pad_mask(region.image, mask_pad), 0),
            offset=[miny - mask_pad, minx - mask_pad],
        )
        if P is None:
            continue
        if check_contains and any_contains(P):
            continue
        if P.convex_hull.area < min_area:
            logger.debug(
                f"skipping an outline that is less than area threshold {min_area}"
            )
            continue
        # accepted polygons are not in other polygons and should be the biggest outer polygon
        polygons.append(P)

    # careful with convention - do we want to swap the points or not yx or xy
    outlines = [swap_points(p.boundary) for p in polygons]

    return outlines


def try_pad_image(im, pad=1):
    """
    not sure if this padding makes sense in all cases
    """
    try:
        pim = Image.fromarray(im)
        col = (0, 0, 0, 0) if im.shape[-1] == 4 else (255, 255, 255)
        padded = Image.new(
            pim.mode,
            (pim.size[0] + 2 * pad, pim.size[1] + 2 * pad),
        )
        padded.paste(pim, (pad, pad))
        return np.array(padded)
    except Exception as ex:
        return im


def biggest_contour_polygon(contours, offset, use=3):
    all_pols = []
    for c in contours:
        try:
            # try add a valid polygon of the contour
            all_pols.append(Polygon(c + offset))
        except:
            pass

    # logger.debug(f"Unioning {len(all_pols)} polygons from the image regions")
    # # testing try a unary union of the n biggest ones
    # p = cascaded_union(all_pols)
    # print("TYPE", p.type)
    # if p.type == "Polygon":
    #     return p

    if len(all_pols) == 0:
        return None

    # return the one with the biggest area
    return all_pols[np.argmax([p.area for p in all_pols])]


def get_padded_image_mask(
    g: Polygon, padding=20, make_square=False, fill_color="black"
):
    """
    return the shape as a padding PIL image RGB format
    this can be used to train shape classifiers on body parts
    """
    # from PIL import ImageOps

    m, b = get_mask(g)
    im = ImageOps.expand(Image.fromarray(m), border=padding, fill=fill_color).convert(
        "RGB"
    )

    if make_square:
        square_side = max(im.width, im.height)
        canvas = Image.new("RGB", (square_side, square_side), fill_color)
        canvas.paste(
            im, ((square_side - im.width) // 2, (square_side - im.height) // 2)
        )
        im = canvas

    return im


def get_mask(g: Polygon, **kwargs):
    """
    Get mask for some polygon returning a mask at-the-origin and the original bounds
    This can be used for example
        mask,b = get_mask(g)
        part = im[b[1]:b[3],b[0]:b[2],:].copy()
        part[mask] = 0

    This is an example of mixing geometries and numpy - need to clean up this interface in future
    This will actually be better with np outlines instead of geometry but need to refactor

    Note here the g will be consistent in image space

    The masked pieces should look the same as they do in the original image
    """
    pad = kwargs.get("pad", 1)
    round_up_bounds = kwargs.get("round_up_bounds", False)
    b = (
        np.array(g.bounds).astype(int)
        if not round_up_bounds
        else np.ceil(np.array(g.bounds)).astype(int)
    )
    # actually pad e.g 1 pixel around the piece so that it is away from the boundary - important for later clipping
    b = [max(0, b[0] - pad), max(0, b[1] - pad), b[-2] + pad, b[-1]]

    y, x = int(b[2] - b[0]), int(b[-1] - b[1])
    g = shift_geometry_to_origin(g)
    # as well as padding shift the polygon so it is just off the origin as per padding
    g = translate(g, xoff=pad, yoff=pad)
    binary_image = np.zeros((x, y), dtype=np.uint8)
    m = cv2.fillPoly(binary_image, np.array([np.array(g).astype(int)], "int32"), 255)
    return m, b


def enumerate_pieces(im, named_piece_outlines=None, **kwargs):
    """
    High level piece splitting workflow
    - we create a mask for 3 (white background) or 4 (alpha-transparent) channel image
    - we determine polygon outlines for the masked pieces
    - We create individual piece masks to clip pieces
    - pieces can be named in different ways
      - if not piece names or index passed in we generate a uuid
      - if an index is passed in but not named pieces we generate piece_{idx}
      - if piece names are supplied we try to lookup the piece match and use the piece key or fallback to the other methods
    - this function can be called to split an image and save piece names on s3 for example
    - piece names can be lookup from shapefiles eg. ASTM/DXF and use our resonance piece naming format res.key

    -ad the end of enumeration the caller should check that named pieces becomes and empty dict otherwise things have not been assigned
    """

    # special case prepare an image that is right on the borders so it can be clipped
    valid_pad = (im.shape[-1] == 4 and im[0, 0, 0] == 0) or (
        im.shape[-1] == 3 and im[0, 0, 0] == 255
    )
    if not valid_pad:
        im = try_pad_image(im)

    # padding  - see use below - could make this a settinv
    pad = 0
    confidence_bounds = kwargs.get("confidence_bounds")
    invert_named_pieces = kwargs.get("invert_named_pieces", False)
    log_file_fn = kwargs.get("log_file_fn")
    confidences_out = kwargs.get("confidences_out", {})
    logger.debug(f"Masking image of size {im.shape}")
    mask = mask_marker(im)
    if log_file_fn:
        log_file_fn("mask.png", mask)
    logger.debug(f"Creating a piece outline set from the image mask")
    piece_outlines = get_piece_outlines_from_mask(mask)
    image_piece_count = len(piece_outlines)
    logger.debug(f"We detected {image_piece_count} pieces in the mask")
    # we cannot assume this in the sense that the raster images might have a partial mapping to rasters- we just need to name the sub pieces
    # if named_piece_outlines and len(named_piece_outlines) != image_piece_count:
    #     raise ValueError(
    #         "The number of pieces generated from the ASTM file does not equal the number of pieces in the rasterized marker image"
    #     )
    if log_file_fn:
        # add pandas dataframe is a borken abstraction TODO:
        # these shape outlines are exactly the ones found in the image and we can shift them to the origin
        df = pd.DataFrame(piece_outlines, columns=["original"])
        df["geometry"] = df["original"].map(lambda g: str(shift_geometry_to_origin(g)))
        df["original"] = df["original"].map(str)
        log_file_fn("piece_outlines.feather", df)

    for idx, g in enumerate(piece_outlines):
        try:
            name, confidence = name_part(
                g,
                named_piece_outlines,
                idx=idx,
                invert_named_pieces=invert_named_pieces,
                piece_material_buffers=kwargs.get("piece_material_buffers"),
            )
        except Exception as ex:
            res.utils.logger.warn(f"Name part failed - bad geometry probably {ex}")
            name = idx
            confidence = -1

        confidences_out[name] = confidence

        mask, b = get_mask(g, pad=pad)
        logger.info(
            f"Masking piece {idx}:{name} with bounds {b} padded with {pad} and matched with confidence {np.round(confidence,3)}"
        )
        if confidence > 100:
            logger.warn(f"The confidence value is outside the threshold")
        if confidence_bounds:
            if confidence < confidence_bounds[0] or confidence > confidence_bounds[-1]:
                logger.debug(
                    f"Skipping piece outside confidence bounds {confidence_bounds}"
                )
                continue
        # clip the image to the bounds - the padding is so later when we get al outline on the mask we always have a complete contour

        part = im[b[1] : b[3], b[0] : b[2], :].copy()
        part = ensure_4_channels(part)
        # slice it
        part = cv2.bitwise_and(part, part, mask=mask)
        # return the named piece - always use the alpha channel
        yield name, part


def outline_bounds(outline):
    """
    assumes an outline at the origin and provides the bounds
    """
    return int(np.ceil(outline[:, 0].max())), int(np.ceil(outline[:, 1].max()))


def mask_from_outline(outline):
    """
    Construct a mask from an outline
    TODO: check faster ways e.g. binary fill holes on a contour
    """

    return polygon2mask(outline_bounds(outline), outline)


def polygon_image_from_outline(outline, fill_color_rgb=(157, 193, 131)):
    """
    Create a polygon image from an outline - this is just a convenience method
    TODO: there might be faster ways to construct this
    """

    mask = mask_from_outline(outline)
    canvas = np.ones((*outline_bounds(outline), 4))
    rgb = tuple(np.array(fill_color_rgb) / 255.0)
    canvas[:, :, :] = (*rgb, 1.0)
    canvas[~mask] = 0
    return canvas


def transparent_thumbnail(im, **kwargs):
    # make a thumbail of the large image if we can
    thumbnail_factor = kwargs.get("thumbnail_scale_factor", (6, 6))
    # never pad for these small images
    kwargs["piece_padding"] = 0
    # thumbnail = resize(
    #     im,
    #     (im.shape[0] // thumbnail_factor[0], im.shape[1] // thumbnail_factor[1]),
    #     anti_aliasing=True,
    # )
    thumbnail = ImageOps.fit(
        Image.fromarray(im),
        (im.shape[1] // thumbnail_factor[1], im.shape[0] // thumbnail_factor[0]),
        Image.ANTIALIAS,
    )
    thumbnail = np.array(thumbnail)

    # return thumbnail
    # make transparent cancas
    canvas = np.zeros((thumbnail.shape[0], thumbnail.shape[1], 4), np.uint8)
    bboxes = []
    # extract the transparent pieces and place them back where there bounding boxes were on the thumbail
    # pieces are always returned transparency - should be a unit test for this
    for i, part in enumerate(
        get_major_part_borders_or_parts(thumbnail, parts_bbox=bboxes, **kwargs)
    ):
        miny, minx, maxy, maxx = bboxes[i]
        canvas[miny:maxy, minx:maxx, :] = part

    # doing a two pass because we lose too much in the accuracy of the piece extraction
    thumbnail = ImageOps.fit(
        Image.fromarray(canvas),
        (
            canvas.shape[1] // thumbnail_factor[1],
            canvas.shape[0] // thumbnail_factor[0],
        ),
        Image.ANTIALIAS,
    )
    thumbnail = np.array(thumbnail)

    # TODO: i did this because bug in flow where it doesnt write the numpy image as a single png
    return [thumbnail]


def _is_probably_notch(im, size_hint=1000):
    """
    10k is a hint area for a  bounding box - it doesnt matter too much it just means we avoid checking many shapes
    """
    return np.prod(im.shape[:2]) < size_hint and has_transparent_center(im)


def ensure_4_channels(part):
    if not np.any(np.array(part.shape) < 0):
        # ensure rgba on the piece
        if part.shape[-1] < 4:
            logger.debug("extending RGB to RGBA by adding an alpha channel")
            part = np.dstack((part, np.zeros(part.shape[:-1], dtype=np.uint8) + 255))
    return part


def get_major_part_borders_or_parts(im, **kwargs):
    """
    A way of returning parts that are not contained in other parts
    A point in polygon check is used to see if the found part is within an existing part
    The assumption is that we scan the image left right and top down therefore
    We encouter bigger shapes first

    The part border is masked to clip the entire image and we create a part at the origin

    IT would be nice to avoid the copy which we could maybe do if we knew that
    other shapes would not be in the bonding box of another shape - that way we could just clip and save portions
    of the original image
    As this is done in a generator however, we should only have a percentage increase in image size e.g. full image + part

    validation is optionally applied if we know the material properties
    """

    max_allowed_width = kwargs.get("output_bounds_width")
    stretch_x = kwargs.get("stretch_x", 1)
    min_area = kwargs.get("min_area", 500)
    for _, part in enumerate_pieces(im, **kwargs):
        # validate - first check non non in case there is some void masking or somehing but dont see how that could happen
        if part is not None:
            if (part.shape[0] * part.shape[1]) < min_area:
                logger.debug(
                    f"Skipping a piece whose area is smaller than the threshold {min_area}"
                )
                continue

            compensated_x = part.shape[1] * stretch_x
            if max_allowed_width:
                if compensated_x > max_allowed_width:
                    raise MaxWidthExceededError(
                        f"The width of the shape with bounds {part.shape} and compensated width ({part.shape[1]}*{stretch_x:.2f}={compensated_x:.2f}) exceeds the max cuttable width {max_allowed_width}"
                    )

            # consistent channels
            if not np.any(np.array(part.shape) < 0):
                # ensure rgba on the piece
                if part.shape[-1] < 4:
                    logger.debug("extending RGB to RGBA by adding an alpha channel")
                    part = np.dstack(
                        (part, np.zeros(part.shape[:-1], dtype=np.uint8) + 255)
                    )

            yield part

    # mask_to_use = non_white_mask if im.shape[-1] == 3 else non_transparent_mask

    # # can force it too
    # if kwargs.get("mask_to_use") is not None:
    #     mask_to_use = kwargs["mask_to_use"]

    # max_allowed_width = kwargs.get("output_bounds_width")
    # stretch_x = kwargs.get("stretch_x", 1)
    # min_area = kwargs.get("min_part_area", 500)

    # # if we are using a non transparent mask we fill try the boundary detection
    # check_contains = mask_to_use == non_white_mask

    # if "check_piece_contains" in kwargs:
    #     check_contains = kwargs.get("check_piece_contains")

    # logger.debug(
    #     f"Using the mask {mask_to_use} - check contains is {check_contains} and min area is {min_area}"
    # )

    # avoid_copy = kwargs.get(
    #     "avoid_copy", True
    # )  # TODO: this is a low mem assumption - make sure it is clear

    # assume_non_intersecting_bbox = kwargs.get("assume_non_intersecting_bbox", False)

    # try:
    #     mask_output_path = kwargs.get("mask_output_path")
    #     if mask_output_path:
    #         s3 = res.connectors.load("s3")
    #         s3.write(mask_output_path, mask_to_use(im))
    #         logger.debug(f"wrote mask to {mask_output_path} ")
    # except:
    #     pass

    # boundaries = find_boundaries(mask_to_use(im))
    # components = label(boundaries)

    # logger.debug(f"Found {len(components)} components....")

    # props = regionprops(components, boundaries)
    # existing_bounding_boxes = kwargs.get("parts_bbox", [])

    # # we are assuming bonding boxes which allows for faster and lower memory collision tests
    # # if we need to be more accurate e.g. if input images are packed more tightly, we would need to revise
    # # but if input images were packed tighter the input images should also be smaller
    # # def _contained_in_existing(miny, minx, maxy, maxx):
    # #     for e in existing_bounding_boxes:
    # #         eminy, eminx, emaxy, emaxx = e
    # #         if miny >= eminy and miny <= emaxy and maxy >= eminy and maxy <= emaxy:
    # #             if minx >= eminx and minx <= emaxx and maxx >= eminx and maxx <= emaxx:
    # #                 return True

    # #     return False

    # polygons = []

    # def any_contains(p):
    #     for p_ in polygons:
    #         if p_.intersects(p):
    #             return True
    #     return False

    # for index, region in enumerate(props):
    #     if min_area is not None and region.area < min_area:
    #         continue
    #     try:
    #         P = Polygon(region.coords)
    #         if any_contains(P):
    #             continue
    #     except:
    #         pass
    #     polygons.append(P)

    #     _label = props[index].label
    #     miny, minx, maxy, maxx = region.bbox
    #     compensated_x = (maxx - minx) * stretch_x

    #     if max_allowed_width:
    #         if compensated_x > max_allowed_width:
    #             raise ValueError(
    #                 f"The width of the shape with bounds {(miny, minx, maxy, maxx)} and compensated width ({(maxx-minx)}*{stretch_x}={compensated_x}) exceededs the max cuttable width {max_allowed_width}"
    #             )
    #     # Following are cases for invalid regions

    #     # if check_contains and _contained_in_existing(miny, minx, maxy, maxx):
    #     #     continue

    #     if _is_probably_notch(im[miny:maxy, minx:maxx, :]):
    #         continue
    #     existing_bounding_boxes.append(region.bbox)

    #     # minimal padding parameter
    #     def chk_bound(v):
    #         return v if v > 0 else 0

    #     padding = kwargs.get("piece_padding", PIECE_PADDING)
    #     miny, minx, maxy, maxx = (
    #         chk_bound(miny - padding),
    #         chk_bound(minx - padding),
    #         maxy + padding,
    #         maxx + padding,
    #     )
    #     logger.debug(
    #         f"binary filling: {not assume_non_intersecting_bbox}, avoid_copy: {avoid_copy} and padding {padding}, bounds {(miny, minx, maxy, maxx)}"
    #     )

    #     part = im[miny:maxy, minx:maxx, :].copy()
    #     partial_component = components[miny:maxy, minx:maxx]
    #     r_mask = (
    #         binary_fill_holes(partial_component == _label)
    #         if not assume_non_intersecting_bbox
    #         else _mask_region_bbox(im, region)
    #     )

    #     logger.info(
    #         f"got a part of shape {part.shape} and a mask of shape {r_mask.shape}. The region area is {region.area}"
    #     )

    #     if not np.any(np.array(r_mask.shape) < 0):
    #         # ensure rgba on the piece
    #         if part.shape[-1] < 4:
    #             logger.debug("extending RGB to RGBA by adding an alpha channel")
    #             part = np.dstack(
    #                 (part, np.zeros(part.shape[:-1], dtype=np.uint8) + 255)
    #             )
    #         # consider copy case later part = part.copy() where there is a risk of overlaps
    #         # we now apply the mask to the copied part to remove anything outside of the part that may be in its bounding box
    #         # this is done regardless of whether we copy or not but would inadvertently corrupt later parts if we did not copy
    #         # and the parts where overlapping bounding boxes
    #         # part[~r_mask[miny:maxy, minx:maxx]] = 0
    #         part[~r_mask] = 0
    #         yield part

    #         # if not is_low_contrast(part):
    #         #     yield part
    #         # else:
    #         #     logger.warn("Skipped low contrast image that we dont trust")
    #     else:
    #         logger.debug("skipping data with invalid bounds")


def parts(image, **kwargs):
    for part in get_major_part_borders_or_parts(image, return_parts=True, **kwargs):
        yield part


def outline_at_edge_peaks(outline, **kwargs):
    """
    We can use keypoints to find the parts of the outline/contours that are at edges
    The peak detection is approx on the contour and we just correct it here
    """
    keypoints = kwargs.get("keypoints")
    if keypoints is not None:
        peaks = keypoints.get("peaks", None)
        if peaks is not None and len(peaks) > 0 and len(outline) > 0:
            dist, idx = KDTree(outline).query(peaks, k=1)
            return outline[idx[:, 0]]
    return None


def get_transparency_contours(image, **kwargs):
    """
    This is basically a wrapper around the skimage get contours except we can also do some custom simplify
    For example notch removal means filling out the conours so that notices are ignore
    This is not affecting things like hull computation but only when we want to draw contour cutlines "accross" the notches
    See also `draw_part_outlines`

    Note this function assumes transparency when constructing the mask

    """
    notch_threshold = kwargs.get("max_notch_width", None)  # e.g 50
    NOTCH_POINT_THRESHOLD = 4
    mask = non_transparent_mask(image)
    res = find_contours(mask, 0)
    if notch_threshold:
        logger.info(
            f"Using keypoints and notch treshold {notch_threshold} to remove notches from the outline..."
        )

        # it can be passed in from globals or computed on demand- may be more keypoints in general
        keypoints = (
            kwargs["keypoints"] if "keypoints" in kwargs else _harris_corners(mask)
        )

        filtered_outline = outline_at_edge_peaks(res[0], keypoints=keypoints)
        if filtered_outline is not None:
            logger.info(f"Using {len(filtered_outline)} contour points at edges...")
            # find clusters of edge peaks within the characteristic notch distance
            idx = BallTree(filtered_outline).query_radius(
                filtered_outline, r=notch_threshold
            )
            # there must be at least 4 items in the cluster for it to be a notch
            idx = [i for i in idx if len(i) >= NOTCH_POINT_THRESHOLD]
            # for each cluster get the points
            for i, row in enumerate(idx):
                # map the index to sets of points
                pts = set([tuple(filtered_outline[i]) for i in row])
                # determine which notches are on the outside and "close" them on the mask
                # we dont do anything with "split" for now but could try another strategy for cleaning the contour (the mask is updated inline)
                split = split_notch_points(pts, mask, epsilon=4)
                # refetch the mask
            # possibly costly fill because i want to make sure that we are creating a sold silhouette before getting the contours
            mask = binary_fill_holes(mask)
            res = find_contours(mask, 0)
        else:
            logger.warn(
                "did not find peaks or keypoints to filter the outline to edge points"
            )
    return res


def draw_part_outlines(part_collection, **kwargs):
    """
    This is an aesthetic function to mark cutlines etc.
    """

    def _thick_line_mask(outline, thickness, size):
        return _thick_outline(outline, thickness) != 0

    thickness = kwargs.get("thickness", DEFAULT_OUTLINE_THICKNESS)
    outline_color = kwargs.get("outline_color", DEFAULT_OUTLINE_COLOR)
    for part in part_collection:
        res = get_transparency_contours(part, **kwargs)[0]
        # if len(res) > 0:
        pim = Image.fromarray(part)
        ImageDraw.Draw(pim).line(
            [(p[1], p[0]) for p in res], fill="black", width=thickness
        )
        part = np.asarray(pim)
        yield part


def bound_size(outline_at_the_origin):
    """
    Assume outlines at the origin or fails assertion
    """
    min_point = np.array(
        [outline_at_the_origin[:, 1].min(), outline_at_the_origin[:, 0].min()]
    )
    assert (
        np.sum(min_point) == 0
    ), "The bounds check assumes that the polygon is at the origin but the min bound is not 0,0"

    return np.array(
        [outline_at_the_origin[:, 1].max(), outline_at_the_origin[:, 0].max()]
    )


def pad_mask(im, pad=1):
    if pad == 0:
        return im
    # todo test this - its used so we always have "something" around the edge but assumed for transparency
    # but then we need to test that the actual outlines is moved back to the origin or whatever so it makes sense in general
    shp = tuple(np.array(im.shape) + 2 * pad)
    canvas = np.zeros(shp)
    canvas[pad:-pad, pad:-pad] = im
    return canvas


def draw_lines(im, lines, **kwargs):
    """

    draw lines on images
    if `min_line_length` is not set, we do not draw lines that are small than 10px
    """

    def dotted_lines(ring, sep=20, space=5):
        points = []
        for a in ring:
            for b in np.arange(sep, a.length, sep):
                b = a.interpolate(b)
                points.append(b.buffer(space))
        result = unary_union(ring) - unary_union(points)
        if not isinstance(result, list):
            if result.type == "LineString":
                result = [result]
        return result

    if lines is None:
        return im

    color = kwargs.get("color", (10, 10, 10, 255))
    thickness = kwargs.get("thickness", 1)
    im = np.array(im)
    # expect an iterable geometry

    if not isinstance(lines, list):
        if lines.type == "LineString":
            lines = [lines]

    # TODO: support vairable lengths and spaces for dash strings
    if kwargs.get("dotted"):
        lines = dotted_lines(lines)

    for l in lines:
        # try simplify as there can be some tony little irrelevant parts
        l = l.simplify(5)

        try:
            if len(l.coords) == 2:
                pts = [tuple(p) for p in np.array(l.coords)[:, :2].astype(int)]

                im = cv2.line(
                    im,
                    *pts,
                    color=color,
                    thickness=thickness,
                )
            elif int(l.length) > kwargs.get("min_line_length", 10):
                im = cv2.polylines(
                    im,
                    # we take the first two components of any line vector supporting LineSrringZ
                    [np.array(l.coords)[:, :-1].astype(int)],
                    # closed
                    False,
                    # black
                    color=color,
                    thickness=thickness,
                )
        except Exception as ex:
            logger.debug(
                f"Failed when drawing the line {l} of length {l.length} on the image - is the line string valid?: {repr(ex)}"
            )

    return im


def draw_outline(
    im,
    outline_image_space=None,
    center=True,
    fill="black",
    # 2mm in pixels is about this 300 dpi and ~.08 incs/mm
    dxa_piece_outline_thickness=24,
    remove_notch=True,
    pad_image=True,
    return_pil=False,
):
    """
    Draw an outline over an image - few things to be careful about
    - we draw line segments and this may not be smooth enough
    - the line outline for different reasons may not be precisely an overlay of some generate image - we can try to center and then cover up with the given thickness
    Will need to watch this function in practice

    - a number of conventions are used here based on how things display and map between shapely and image space throughout
    - still working through the interface e.g. if numpy or geometries are used.

    pad image we take ther responsability in image space of expanding the image
    """

    if outline_image_space is not None:
        ol = outline_image_space
        sim = (im.shape[1], im.shape[0])
        sol = (ol.bounds[3] - ol.bounds[1], ol.bounds[2] - ol.bounds[0])

        print("im shape and outline bounds", im.shape, ol.bounds)

        dx = sol[0] - sim[0]
        dy = sol[1] - sim[1]

        res.utils.logger.debug(
            f"dx={dx}, dy={dy} - will offset/pad the image and we are also offseting the outline delta below"
        )

        im = image_padding(im, dx * 2, dy * 2)

        res.utils.logger.debug(f"padded na image to shape {im.shape}")

        # always make sure we are at origin ? actually no - is the following a no opp ?
        # we dont actually want to draw at the origin, we want to draw at the origin of the image
        # outline_image_space = shift_geometry_to_origin(outline_image_space)
        # outline_image_space = translate(outline_image_space, xoff=dx, yoff=-1 * dy)

        # use numpy - also, shift for any dx dy here so the outlines is overlayed correctly in the padded image
        outline_image_space = np.array(outline_image_space.coords) + np.array(
            [int(dy), int(dx)]
        )

        # because of how PIL will draw onto the image in this Y order
        # outline_image_space = invert_y_axis(outline_image_space, order="yx")

        # add half the distance in the bounds
        # if center:
        #     correction = (
        #         np.array(im.shape[:2]) - np.array(outline_bounds(outline_image_space))
        #     ) / 2.0
        #     logger.debug(
        #         f"{correction} is the bounds correction yx because the outline is either smaller or bigger than the image (not sure if we need to re-pad when it is smaller). We are correcting because [center]==true",
        #     )
        #     outline_image_space += correction

        # closed_poly = [tuple(reversed(l)) for l in outline_image_space]
        closed_poly = [tuple(reversed(l)) for l in outline_image_space]
    else:
        # apply padding for safety -> move this into outline func
        # this is done if an outline is sitting right on the border so the outer contour is "broken"
        closed_poly = get_piece_outline(np.asarray(im))
        if remove_notch:
            closed_poly = remove_notch_from_outline(closed_poly)
        closed_poly = [tuple(l) for l in closed_poly.coords]

    pim = Image.fromarray(im) if isinstance(im, np.ndarray) else im
    # get the outline segments and draw them

    # padding
    # logger.info("padding")
    pad = dxa_piece_outline_thickness
    padded = Image.new(
        pim.mode, (pim.size[0] + 2 * pad, pim.size[1] + 2 * pad), (0, 0, 0, 0)
    )
    padded.paste(pim, (pad, pad))
    pim = padded
    # pad the polygon too
    closed_poly = [tuple([t[0] + pad, t[1] + pad]) for t in closed_poly]
    # should use numpy - this offsets the outline by padding so we can fill the line back out

    pim = Image.fromarray(
        cv2.polylines(
            np.array(pim),
            [np.array(closed_poly).astype(int)],
            # closed
            True,
            # black
            color=(0, 0, 0, 255),
            thickness=dxa_piece_outline_thickness,
        )
    )

    # PIL can be used but does not look great
    # closed_poly = [tuple([t[0] + pad, t[1] + pad]) for t in closed_poly]
    # logger.info("done")

    # ImageDraw.Draw(pim).line(
    #     closed_poly, fill="black", width=dxa_piece_outline_thickness, joint="curve"
    # )

    return np.asarray(pim) if not return_pil else pim


def image_padding(im, dx, dy):
    # todo test this - its used so we always have "something" around the edge but assumed for transparency
    # but then we need to test that the actual outlines is moved back to the origin or whatever so it makes sense in general
    shp = (im.shape[0] + int(dy), im.shape[1] + int(dx))
    padx = int(dx / 2)
    pady = int(dy / 2)
    canvas = np.zeros((*shp, im.shape[-1]), dtype=im.dtype)
    canvas[pady : im.shape[0] + pady, padx : im.shape[1] + padx] = im
    return canvas


def part_outlines(image_collection, **kwargs):
    """
    We get outlines of a part which may have some extra elements draw onto it in the flow e.g cutlines already
    This should be called when we know what we want to nest with so it should contain everything withing the image we care about including padding
    """

    padding = kwargs.get("padding", 2)

    def pad_image(im, pad=1):
        # todo test this - its used so we always have "something" around the edge but assumed for transparency
        # but then we need to test that the actual outlines is moved back to the origin or whatever so it makes sense in general
        shp = tuple(np.array(im.shape[:2]) + 2 * pad)
        canvas = np.zeros((*shp, im.shape[-1]))
        canvas[pad:-pad, pad:-pad] = im
        return canvas

    for i, part in enumerate(image_collection):
        res = find_contours(non_transparent_mask(part), 0)

        if len(res) > 0:
            # this is an assumption that the contours are on the boundary for accurate masks- we could  np.concatenate(res) but its dangerous
            res = res[0]
            res = res if padding == 0 else _pad_outline(res, padding)
            yield res
        else:
            raise Exception(f"Failed to extract outlines for part {i}")


def has_transparent_center(im):
    """
    check if the shape has a transparent center which should be illegal..g this could be just some artifact boundary that is not a solid shape
    """
    return np.sum(im[int(im.shape[0] / 2.0), int(im.shape[1] / 2.0), :]) == 0


def _thick_outline(outline, thickness):
    """
    using a convolution on a polygon embedded in a grid, construct a thick line = 1 on a grid of 0s
    This can be used as a mask to draw a line (we might want to consider ways to do this e.g. anti-aliasing concerns)
    """
    if thickness <= 1:
        return outline

    outline += thickness
    y, x = outline[:, 1], outline[:, 0]
    maxy = int(np.ceil(y.max()) + thickness)
    maxx = int(np.ceil(x.max()) + thickness)
    canvas = np.zeros((maxx, maxy))
    canvas[polygon_perimeter(x, y, shape=canvas.shape)] = 1
    kernel = np.ones((thickness, thickness))
    result = np.int64(convolve2d(canvas, kernel, mode="same") > 0)
    return result


def _pad_outline(outline, padding):
    """
    Given an outline, add thickness and get the outline or contour of that thick outline
    Slight misnomer as assuming a symmetrical line that we want to pad on each side so we are really expanding or thickening.
    But when we get the outline it on the outside so if you want to enlarge by 5 set padding to 10
    """
    if padding == 0:
        return outline

    result = _thick_outline(outline, padding)
    result = find_contours(result, 0.5)[0]
    return result


def padded_outlines(polygons, buffer):
    for outline in polygons:
        yield _pad_outline(outline)


def clockwise_yx(pts):
    pts = np.array([p for p in pts])
    center = pts.mean(axis=0)
    angles = np.arctan2((pts - center)[:, 1], (pts - center)[:, 0])
    angles[angles < 0] = angles[angles < 0] + 2 * np.pi
    return pts[np.argsort(angles)]


def line_spans_transparent_region(points, mask, epsilon=4, debug_mark_circle=False):
    ordered_points = clockwise_yx(points)
    for i in range(len(ordered_points)):
        p1 = ordered_points[i - 1]
        p2 = ordered_points[i]
        mp = int((p1[0] + p2[0]) / 2.0), int((p1[1] + p2[1]) / 2.0)

        is_non_transparent = np.sum(mask[circle_perimeter(*mp, epsilon)])

        # for testing draw the cicle that is used to sample for transparent space
        if debug_mark_circle:
            mask[circle_perimeter(*mp, epsilon)] = 1

        if is_non_transparent == 0:
            # actually draw the line
            mask[line(int(p1[0]), int(p1[1]), int(p2[0]), int(p2[1]))] = 1
            yield p1
            yield p2


def split_notch_points(points, mask, epsilon):
    res = list(line_spans_transparent_region(points, mask, epsilon))
    keep_points = set([tuple(p) for p in res])
    remove_points = points - keep_points
    return {"keep": keep_points, "remove": remove_points}


def invert_y_axis(outline, order="xy", bounds=None):
    """
    give a polygon outline (or any vector) invert the y axis
    this is commonly done for mapping between image space cartesian where the y axis is inverted
    """
    if bounds is not None:
        # assume a vector but a tuple shape can be passed
        if not isinstance(bounds, list):
            bounds = [bounds]

    index = 1 if order == "xy" else 0

    # if the outline is "complete" otherwise we use a reference bounds
    # for example if the polygon is a sub polygon, we offset it using the larger bounds
    maxy = outline[:, index].max() if not bounds else np.array(bounds)[:, index].max()

    outline[:, index] = (outline[:, index] - maxy) * -1

    return outline


# def _boundaries(image, **kwargs):
#     boundaries = find_boundaries(image)
#     components = label(boundaries)
#     props = regionprops(components, boundaries)

#     for index, region in enumerate(props):

#         label = props[index].label

#         yield {
#             "index": index,
#             "bbox": region.bbox,
#             "label": props["index"].label,
#             "area": region.area,
#             # TODO: an there be multiple contours?
#             "contour": find_contours(components == label, 0.5)[0],
#         }
