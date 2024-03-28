import numpy as np
from shapely.geometry import Polygon
import pandas as pd
import os

from res.flows.make.nest.pack_pieces import (
    _pack_pieces,
    DEFAULT_HEIGHT,
    DEFAULT_WIDTH_INCHES,
    DPI_SETTING,
    DEFAULT_BUFFER,
)  # TODO: move everything over to this other flow.

from res.utils import logger, ping_slack
import res

from res.media.images.geometry import (
    get_geo_add_y_fn,
    get_shape_bound_fn,
    invert_axis,
    Point,
    translate,
    scale,
)


PIXEL_PER_MILLIMETER_SETTING = DPI_SETTING / 25.4
DEFAULT_WIDTH = DPI_SETTING * DEFAULT_WIDTH_INCHES
# 2000 for testing
DEFAULT_APPROX_TILE_SIZE = 50000

# mile marker goings on.
MILE_MARKER_SPACING_PX = DPI_SETTING * 10  # one every 10"
MILE_MARKER_LINE_WIDTH_PX = 10
MILE_MARKER_LINE_LENGTH_PX = 4 * DPI_SETTING
MILE_MARKER_LABEL_HEIGHT_PX = 150
MILE_MARKER_LABEL_WIDTH_PX = 400

ROLL_INSPECTION_FILE_DIM = 4000


def plot_remote_nest(path, geometry_column="nested.geometry", figsize=(7, 21)):
    """ """
    from geopandas import GeoDataFrame
    from shapely.wkt import loads
    from matplotlib import pyplot as plt

    s3 = res.connectors.load("s3")
    df = s3.read(path)

    def _try_load(g):
        if isinstance(g, str):
            return loads(g)
        return g

    try:
        df[geometry_column] = df[geometry_column].map(_try_load)
        df = GeoDataFrame(df, geometry=geometry_column)
        df.plot(figsize=figsize)
        plt.gca().invert_yaxis()
    except Exception as ex:
        logger.warn(
            f"Could not plot due to {repr(ex)} - check the dataframe and try again"
        )
    return df


def nest(shapes, **kwargs):
    """Nests Shapes for use in production.
    Positional arguments:
    shapes is enumerable set of 2d numpy_arrays
    Keyword arguments:
    output_bounds_width, output_bounds_height -- printable inches at 300DPI
    stretch_x, stretch_y -- how much to stretch the pieces along each axis
    buffer -- an int describing buffer space (in 300ths of an inch)
    """
    max_iters = kwargs.get("max_packing_iters", 1000)
    nested_shapes = _pack_pieces(
        [Polygon(s) for s in shapes], max_packing_iters=max_iters, threads=1, **kwargs
    )
    for nested_shape in nested_shapes:
        yield np.array(nested_shape.exterior.coords)


def get_transforms(unnested, nested, **kwargs):
    nested = list(nested)
    transforms = []
    trans_dic = []
    for input, output in zip(unnested, nested):
        translation = [output[0][0] - input[0][0], output[0][1] - input[0][1]]
        miny = np.array(output)[:, 1].min()
        minx = np.array(output)[:, 0].min()
        maxy = np.array(output)[:, 1].max()
        maxx = np.array(output)[:, 0].max()
        trans_dic.append(
            {
                "nested": output.tolist(),
                "translation": translation,
                # "affine": transforms.tolist(),
                "min_nested_x": int(minx),
                "min_nested_y": int(miny),
                "max_nested_x": int(maxx),
                "max_nested_y": int(maxy),
            }
        )
    return pd.DataFrame(trans_dic)


def nest_transforms(shapes, **kwargs):
    nested = nest(shapes, **kwargs)
    df = get_transforms(shapes, nested, **kwargs)

    if "one_number" in kwargs:
        df["one_number"] = kwargs.get("one_number")

    return df


def add_background(roll, width, height, add_grid):
    from pyvips import Image as vips_image

    divider = (
        (vips_image.black(width, MILE_MARKER_LINE_WIDTH_PX) + (0, 0, 0, 0))
        .copy(
            interpretation="srgb",
            xres=PIXEL_PER_MILLIMETER_SETTING,
            yres=PIXEL_PER_MILLIMETER_SETTING,
        )
        .draw_rect(
            [0, 0, 0, 255],
            0,
            0,
            MILE_MARKER_LINE_LENGTH_PX,
            MILE_MARKER_LINE_WIDTH_PX,
            fill=True,
        )
        .draw_rect(
            [0, 0, 0, 255],
            width - MILE_MARKER_LINE_LENGTH_PX,
            0,
            MILE_MARKER_LINE_LENGTH_PX,
            MILE_MARKER_LINE_WIDTH_PX,
            fill=True,
        )
    )

    if add_grid:
        logger.info("Adding grid")
        w = int(width / 2) - int(width / 2) % 600
        logger.info((width, w))
        for x in range(0, int(width / 2), 600):
            lw = 20 if x == 0 or x + 600 >= int(width / 2) else 10
            lc = (
                [0, 0, 0, 255]
                if x == 0 or x + 600 >= int(width / 2)
                else [180, 180, 180, 255]
            )
            roll = roll.draw_rect(
                lc,
                x,
                0,
                lw,
                height,
                fill=True,
            )
            roll = roll.draw_rect(
                lc,
                width - x - lw,
                0,
                lw,
                height,
                fill=True,
            )
        for y in range(0, int(height), 600):
            roll = roll.draw_rect(
                [180, 180, 180, 255],
                0,
                y,
                w,
                20,
                fill=True,
            )
            roll = roll.draw_rect(
                [180, 180, 180, 255],
                width - w,
                y,
                w,
                20,
                fill=True,
            )

    for y in range(
        0,
        int(height) - MILE_MARKER_LABEL_HEIGHT_PX - MILE_MARKER_LINE_WIDTH_PX - 5,
        MILE_MARKER_SPACING_PX,
    ):
        label_image = vips_image.text(
            f'{int(y/DPI_SETTING)}"',
            width=MILE_MARKER_LABEL_WIDTH_PX,
            height=MILE_MARKER_LABEL_HEIGHT_PX,
            autofit_dpi=True,
        )[0]
        label_image = (
            label_image.new_from_image([0, 0, 0])
            .bandjoin(label_image)
            .copy(interpretation="srgb")
        )
        roll = roll.composite(divider, "over", x=0, y=y)
        roll = roll.composite(
            label_image, "over", x=0, y=y + MILE_MARKER_LINE_WIDTH_PX + 5
        )
        roll = roll.composite(
            label_image,
            "over",
            x=int(width) - MILE_MARKER_LABEL_WIDTH_PX,
            y=y + MILE_MARKER_LINE_WIDTH_PX + 5,
        )

    return roll


def add_background_uniform_mode(roll, height, width, artwork_uri, stretch_x, stretch_y):
    from pyvips import Image as vips_image

    logger.info(f"Constructing uniforms background with {artwork_uri}")
    background = res.connectors.load("s3").read(artwork_uri)
    background_compensated = (
        vips_image.new_from_memory(
            background.data,
            background.shape[1],
            background.shape[0],
            bands=background.shape[2],
            format="uchar",
        )
        .affine((stretch_x, 0, 0, stretch_y))
        .copy(interpretation="srgb")
    )
    for i in range(int(np.ceil(width / background_compensated.width))):
        for j in range(int(np.ceil(height / background_compensated.height))):
            roll = roll.composite(
                background_compensated,
                "over",
                x=i * background_compensated.width,
                y=j * background_compensated.height,
            )
    return roll


def nest_transformations_apply(transformations, image_suppliers, **kwargs):
    if len(image_suppliers) != len(transformations):
        message = f"Different numbers of piece images ({len(image_suppliers)}) and transformations ({len(transformations)})"
        logger.error(message)
        raise ValueError(message)

    # this cuts the memory usage approximately in half but doubles the running time.
    # the normal amount of threads that pyvips will use if we dont set this is 64.
    # os.environ["VIPS_CONCURRENCY"] = "32"
    import pyvips
    from pyvips import Image as vips_image

    output_path = kwargs.get("output_path")
    x_coords = list(transformations[kwargs.get("x_coord_field", "min_nested_x")])
    y_coords = list(transformations[kwargs.get("y_coord_field", "max_nested_y")])
    buffers = kwargs.get("buffers", [DEFAULT_BUFFER] * len(image_suppliers))
    stretch_x = kwargs.get("stretch_x", 1.0)
    stretch_y = kwargs.get("stretch_y", 1.0)

    logger.info(f"Compensating with x:{stretch_x:.3f} y:{stretch_y:.3f}")

    image_labels = kwargs.get("image_labels")

    width = kwargs.get("output_bounds_width", DPI_SETTING * DEFAULT_WIDTH_INCHES)
    height = kwargs.get("output_bounds_length", DEFAULT_HEIGHT)
    print_bounds = (width, height)

    logger.info(f"Using output bounds (x,y) {print_bounds}")
    logger.debug(f"stretch_x: {stretch_x}")
    logger.debug(f"stretch_y: {stretch_y}")

    if os.getenv("RES_ENV") == "development":
        pyvips.leak_set(True)

    # output image canvas limited to bounds of the nesting
    roll = (vips_image.black(*print_bounds) + (0, 0, 0, 0)).copy(
        interpretation="srgb",
        xres=PIXEL_PER_MILLIMETER_SETTING,
        yres=PIXEL_PER_MILLIMETER_SETTING,
    )

    logger.info("Constructing background")

    uniform_mode = kwargs.get("uniform_mode", False)

    if kwargs.get("background_artwork_uri") is not None:
        roll = add_background_uniform_mode(
            roll,
            height,
            width,
            kwargs.get("background_artwork_uri"),
            stretch_x if kwargs.get("compensate_background", True) else 1.0,
            stretch_y if kwargs.get("compensate_background", True) else 1.0,
        )
    else:
        roll = add_background(roll, width, height, kwargs.get("grid", False))

    s3 = res.connectors.load("s3")
    total_image_bytes = 0

    noheader_file = "roll_noheader.png"
    noheader_output_path = f"{output_path}/printfile_composite_noheader.png"
    logger.info(f"Loading {len(image_suppliers)} images")
    for i, image_supplier in enumerate(image_suppliers):
        # the act of giving vips the png data as a buffer and letting vips save the image means
        # its going to take less ram when it loads the thing up again.
        piece_file = f"piece_{i}.png"
        image = image_supplier()
        vimage = (
            vips_image.new_from_memory(
                image.data, image.shape[1], image.shape[0], bands=4, format="uchar"
            )
            .affine((stretch_x, 0, 0, stretch_y))
            .copy(interpretation="srgb")
        )
        # mask a region for each piece -- corresponding to the buffer around it.
        # this ensures that the laser cutter has enough clearance irrespective of whatever
        # background stuff we put on the roll.

        if uniform_mode or buffers[i] == 0:
            vimage.pngsave(piece_file)
        else:
            # vips is so utterly gimped.
            # this is just trying to make an image 2*buffer bigger than the piece, dilate the alpha channel, then composite the original over it.
            # leading to a buffer of size buffer[i] of opaque white around the piece.
            b = buffers[i]
            alpha = (
                vips_image.black(vimage.width + 2 * b, vimage.height + 2 * b, bands=1)
                .bandsplit()[0]
                .composite(vimage.bandsplit()[3], "over", x=b, y=b)
            )
            # makes perfect sense thanks vips.
            # the mask can be 255 for "stuff" 0 for "empty" and 128 for dont care.
            mask = alpha.bandsplit()[0].dilate(
                vips_image.mask_ideal(
                    2 * b + 1, 2 * b + 1, 1, uchar=True, reject=True, optical=True
                )
                * 127
                / 255
                + 128
            )
            mask = (
                mask.new_from_image([255, 255, 255])
                .bandjoin(mask)
                .copy(interpretation="srgb")
            )
            mask.composite(vimage, "over", x=b, y=b).pngsave(piece_file)
        image_bytes = os.path.getsize(piece_file)
        total_image_bytes += image_bytes

    logger.info(f"Total piece image size on disk {total_image_bytes}")

    for i in range(len(image_suppliers)):
        vimage = vips_image.pngload(f"piece_{i}.png", access="sequential")
        x = int(x_coords[i]) - buffers[i]
        y = int(height - y_coords[i]) - buffers[i]

        comp_df_row = transformations.iloc[i]
        nested_height = comp_df_row.max_nested_y - comp_df_row.min_nested_y
        nested_width = comp_df_row.max_nested_x - comp_df_row.min_nested_x
        if vimage.height > nested_height + 2 * buffers[i] + 100:
            ping_slack(
                f"<@U0361U4B84X> Piece {comp_df_row.asset_id} {comp_df_row.piece_code} too tall {vimage.height} for nested bounds {nested_height}",
                "autobots",
            )
            raise Exception(
                f"Piece {comp_df_row.asset_id} {comp_df_row.piece_code} too tall {vimage.height} for nested bounds {nested_height}"
            )
        if vimage.width > nested_width + 2 * buffers[i] + 100:
            ping_slack(
                f"<@U0361U4B84X> Piece {comp_df_row.asset_id} {comp_df_row.piece_code} too wide {vimage.width} for nested bounds {nested_width}",
                "autobots",
            )
            raise Exception(
                f"Piece {comp_df_row.asset_id} {comp_df_row.piece_code} too wide {vimage.width} for nested bounds {nested_width}"
            )

        logger.info(
            f"Compositing image {i} of size {vimage.height, vimage.width} at x={x}, y={y}.  with buffer {buffers[i]}"
        )
        roll = roll.composite(vimage, "over", x=x, y=y)

    roll.pngsave(noheader_file)

    logger.info(f"Transferring to {noheader_output_path}")
    s3.upload(noheader_file, noheader_output_path)

    if image_labels:
        SCALE = 0.05 if height > 50000 else 0.1
        TEXT_DPI = int(3000 * SCALE) if height * SCALE > 1000 else int(1000 * SCALE)
        logger.info(f"Writing labeled image {image_labels}")
        labeled = vips_image.thumbnail(noheader_file, int(height * SCALE))
        logger.info(
            f"Creating labeled image with dimensions {labeled.width, labeled.height}"
        )
        for label, coords, angle in image_labels:
            # jfc vips
            label_image = (
                (vips_image.text(label, dpi=TEXT_DPI, width=200) + (0, 0, 0, 255))
                .copy(interpretation="srgb")
                .rotate(-angle)
            )
            cx, cy = coords
            cx = int(cx * SCALE - label_image.width / 2)
            cy = int((height - cy) * SCALE - label_image.height / 2)
            logger.info(
                f"Adding label {label} with dimension {label_image.width, label_image.height} at coords {cx, cy}, with angle {angle}"
            )
            try:
                labeled = labeled.composite(label_image, "over", x=cx, y=cy)
            except Exception as e:
                logger.warn(f"Failed to add label {repr(e)}")
        labeled_file_name = "printfile_labeled.png"
        labeled.pngsave(labeled_file_name)
        labeled_output_path = f"{output_path}/printfile_labeled.png"
        logger.info(f"Transferring to {labeled_output_path}")
        s3.upload(labeled_file_name, labeled_output_path)

    # generate an even smaller preview image for roll inspection
    vips_image.thumbnail(noheader_file, ROLL_INSPECTION_FILE_DIM).pngsave(
        "roll_inspection.png"
    )
    s3.upload("roll_inspection.png", f"{output_path}/roll_inspection.png")

    thumb_h = min(int(height / 5), 15000)
    logger.info(f"Writing thumbnail data for image of height {thumb_h}")

    output_path_full = f"{output_path}/printfile_composite_thumbnail.png"
    logger.info(
        f"Writing thumbnail data for image of height {thumb_h} to {output_path_full}"
    )
    thumb = vips_image.thumbnail(noheader_file, thumb_h)
    len_pcs = write_thumbnail(
        thumb,
        output_path,
        check_count=len(transformations),
        raise_error=kwargs.get("raise_composite_count_error"),
    )

    logger.debug(f"The composite appears to have {len_pcs} pieces")

    return transformations


def write_thumbnail(img, output_path, check_count=None, raise_error=False):
    """
    Write a thumbnail and optionally do some validations on the smaller image
    """
    from res.media.images.outlines import get_piece_outlines_from_mask, mask_marker

    s3 = res.connectors.load("s3")

    def logging_function(path, data):
        logger.debug(f"Logging {path}")
        s3.write(f"{output_path}/mask/{path}", data)

    im = np.ndarray(
        buffer=img.write_to_memory(),
        dtype=np.uint8,  # float32
        shape=[img.height, img.width, img.bands],
    )

    logger.debug("writing to s3...")
    s3.write(f"{output_path}/printfile_composite_thumbnail.png", im, dpi=(300, 300))

    if check_count:
        logger.debug(
            f"Checking to see if the piece count {check_count} is correct within mask of shape {im.shape}"
        )
        mim = mask_marker(im)
        logging_function("thumbnail_mask.png", mim)
        pcs = len(get_piece_outlines_from_mask(mim, erode=True, convex_area_filter=500))
        logger.debug(f"Counted {pcs} pieces in the thumbnail mask")
        if pcs != check_count:
            logger.warn(
                f"The expected number of pieces was {check_count} but got {pcs}"
            )
            # we are raising the value error so we do not need to log it
            # ^ we are currently not failing nesting when this happens but lets keep an eye in pager duty

            if raise_error:
                raise ValueError("Incorrect piece count in print file composite")
            else:
                res.utils.logger.warning(
                    "Found an incorrect piece count but we are not killing the argo task"
                )
            # or just send to sentry ?

        return pcs


def _add_labels_to_vips_image(vips_image, labels, transformations, key):
    import pyvips
    from pyvips import Image as vips_image

    if labels == None:
        return
    # assert labels have the same size as transformations of labels

    transformations = list(transformations)
    locations = []
    stretches = []
    rotations = []
    for i, image in enumerate(labels):
        vips_image = pyvips.Image.new_from_memory(
            image.data, image.shape[1], image.shape[0], bands=4, format="uchar"
        ).copy(interpretation="srgb")

        logger.debug(
            f"Adding label {i} of size ({image.shape[0]}, {image.shape[1]}) at {locations[i]}, {locations[i]} scaled by {stretches[i]}, {stretches[i]}"
        )

        roll = roll.composite(vips_image, "over", x=locations[i], y=locations[i])


def find_transform(X, Y):
    """
    Given two matrices, try to compute the transformation
    """
    pad = lambda x: np.hstack([x, np.ones((x.shape[0], 1))])
    unpad = lambda x: x[:, :-1]
    A, res, rank, s = np.linalg.lstsq(pad(X), pad(Y), rcond=None)
    # tr = lambda x: unpad(np.dot(pad(x), A))

    return A


def _new_locations_for_nesting(shape_collection, output_bounds):
    pass


def _transformations_from_locations(a, b):
    # linalg
    pass


# Testing Only
def fixnest(nested):
    nested = list(nested)
    lopped = []
    for each in nested:
        lopped.append(each[:-1])
    return lopped


def nesting_check_kwargs(
    output_bounds_length_pixels, output_bounds_width_pixels, **kwargs
):
    # TODO
    return kwargs


def nest_dataframe_by_chunks(df, chunk_size=180, **kwargs):
    """
    use nest by set but the sets are ordinal pages
    this is a time optimization and reduce packing factor but in a fair way
    the nesting time becomes linear time in the chunk size

    Examples:
     - 250 pieces can take ~ 25 minute to nest
     - 180 pieces ~ 13 minutes
    """

    if len(df) < chunk_size:
        return nest_dataframe(df, **kwargs)

    # else do chunking
    if "index" in df.columns:
        df = df.drop("index", 1)
    # df = res.utils.dataframes.paginate(df, chunk_size)
    df = df.reset_index().rename(columns={"index": "page"})
    df["page"] = df["page"].map(lambda i: i // chunk_size)
    pages = list(df["page"].unique())
    pages = [[p] for p in pages]

    logger.info(
        f"nesting: chunking dataframe of len {len(df)} into chunks of {chunk_size}"
    )

    return nest_dataframe_by_sets(df, pages, "page", **kwargs)


def nest_dataframe_by_sets(
    df,
    sets,
    filter_column="piece_type",
    inter_set_padding=1,
    **kwargs,
):
    """
    This is convenient way to do split nests just at the dataframe level
    Combine shapes with offset so it feels as though we are nesting in groups
    [
        pieces in set A
    ]
    thin buffer
    [
        offset pieces in set B
    ]
    etc.

    """
    # defaults if not overridden by special known piece type sizes for now hard coded
    width = kwargs.get("output_bounds_width", DPI_SETTING * DEFAULT_WIDTH_INCHES)
    Y_MAX_IDX = -1
    ndf = None
    total_height = 0
    for s in sets:
        options = dict(kwargs)
        options["output_bounds_width"] = 14000 if "block_fuse" in s else width

        _df = df[df[filter_column].isin(s)].reset_index().drop("index", 1)

        _df = nest_dataframe(_df, **options)
        # might be that we need to shit +- this thing
        _df["nested.geometry"] = _df["nested.geometry"].map(
            get_geo_add_y_fn(total_height)
        )

        # return _df
        # from the shapely bounds we need to get the max y and we are assuming a certain orientation in nesting
        total_height = (
            _df["nested.geometry"].map(get_shape_bound_fn(Y_MAX_IDX)).max()
            + inter_set_padding
        )
        logger.debug(f"adding set {s} to group nest")
        ndf = (
            pd.concat([ndf, _df]).reset_index().drop("index", 1)
            if ndf is not None
            else _df
        )
    return ndf


def translate_nested_geom(col="geometry"):
    """
    apply a simple transformation on g as a function of coff
    this seems weird its because we usually buffer the original shape to nest it so you want to back-apply the transformation on the nest
    """

    def f(row):
        offset = row["coff_inv"]
        g = row[col]
        g = translate(g, xoff=offset[0], yoff=offset[1])
        return g

    return f


def centroid_offset_inv(geometry_column="geometry"):
    """
    a little confusing but we nest in image space and its confusing to know intent for 0 index
    here we invert as though we nested from 0 down
    """

    def f(row):
        a = row[geometry_column].centroid
        b = row[f"nested.{geometry_column}"].centroid
        y_max = row["max_y"]
        b = Point(b.x, y_max - b.y)
        return (b.x - a.x, b.y - a.y)

    return f


def nest_dataframe(
    df,
    geometry_column="geometry",
    as_geo_pandas=False,
    reverse_points=False,
    grouping_column=None,
    return_plot=False,
    **kwargs,
):
    """

    This generates a dataframe for nesting and nests its returning the nested result as a dataframe

    this is a convenience method that could be improved
    we can automatically map between polygons and numpy arrays and be more intelligent about
    what we are allowed to pass in
    Example improvement would be to work with shapely types and covert them to numpy arrays under the hood and allow 'geometry' columns to be nested

    We manage outlines according to conventions; one strategy is if we ready outlines straight off an image

    #we can set the geometry like this for undoing the image space mapping assumptions
    data['geometry'] = data['geometry'].map(lambda g : swap_points(invert_axis(g)).convex_hull)
    For example we could read any folder of images and collect their outlines, and keeping by the file and the outline as a pair, we nest
    Note this recipe also needs to take hulls or find some other low res rep of the outline or this will take forever

    for f in glob("../**/*.png", recursive=True):
        im = imread(f)
        ol = get_piece_outline(im)

    Then the compositor can just combine the outline with the image
        assets = paginate_nested_dataframe(nested)
        from res.media.images import compositor
        from res.flows.dxa import res_color
        assets['key'] = "1010101"
        im = list(compositor.composite_images(tile_page(assets,0), res_color_fn_factory=res_color.get_outline_overlay_function) )[0]



    """

    def _plotter(ndf):
        from geopandas import GeoDataFrame

        # custom plot column can be passed
        df = GeoDataFrame(
            ndf,
            geometry=kwargs.get("plot_column") or f"nested.original.{geometry_column}",
        )
        title = "nest"
        if "category" in ndf.columns:
            title = ndf["category"][0]

        print(kwargs.get("figsize", (16, 8)))
        ax = df.plot(figsize=kwargs.get("figsize", (16, 8)))
        ax.set_title(title)
        df["bounds"] = df[f"nested.{geometry_column}"].map(lambda x: x.bounds).values
        if "res.key" not in df.columns:
            if "key" in df.columns:
                df["res.key"] = df["key"]
            else:
                df["res.key"] = None
        mapping = dict(df[["res.key", "bounds"]].values)
        for k, v in mapping.items():
            w = v[-2] - v[0]
            pts = int((v[-2] + v[0]) / 2.0), int((v[-1] + v[1]) / 2.0)
            ax.annotate(
                k,
                xy=pts,
                size=8,
                color="k",
                rotation=0 if w > 3000 else 90,
                ha="center",
                va="center",
            )  # weight="bold",

        return ax

    """
    grouping plotting block
    """
    if grouping_column:
        res.utils.logger.info(f"nesting in groups {grouping_column}")
        groups = []
        offset = 0
        for gr, data in df.groupby(grouping_column):
            res.utils.logger.info(f"nesting in group {gr}- {len(data)} items")

            gd = nest_dataframe(
                data.reset_index(drop=True),
                geometry_column=geometry_column,
                delay_plot=True,
                **kwargs,
            )
            # translate offset
            for col in [
                f"nested.original.{geometry_column}",
                f"nested.{geometry_column}",
            ]:
                gd[col] = gd[col].map(lambda g: translate(g, yoff=offset))
            # special treatment of geometry
            if (
                "geometry" != geometry_column
                and "nested.original.geometry" in gd.columns
            ):
                gd["nested.original.geometry"] = gd["nested.original.geometry"].map(
                    lambda g: translate(g, yoff=offset)
                )

            groups.append(gd)
            offset += gd["max_y"].max()

        df = pd.concat(groups).reset_index(drop=True)

        if kwargs.get("plot"):
            pl = _plotter(df)
            if return_plot:
                return pl

        return df

    """
    """
    # plt.gca().invert_yaxis()

    def should_reverse_points(v):
        # also make it a list
        v = np.array(v).astype(int)
        if reverse_points:
            return v[:, [1, 0]]
        return v

    def centroid_offset(row):
        # we offset by what the convex hull of the object would move since the nested geometry is always a nested convex hull
        a = row[geometry_column].convex_hull.centroid
        b = row[f"nested.{geometry_column}"].convex_hull.centroid
        return (b.x - a.x, b.y - a.y)

    def iterate_shapes(d):
        """
        Separating concerns, nesting dataframe only nests what it is given now
        """
        for _, row in df.iterrows():
            g = row[geometry_column]

            # we may want to insist on linear rings in future but coercing for now
            if g.type == "Polygon":
                g = g.exterior
            elif hasattr(g, "geoms"):
                # automatically take a convex hull if this geom type and we will then take the exterior
                g = g.convex_hull.exterior

            yield np.array(g).astype(int)

    logger.debug(
        f"Nesting a dataframe of length {len(df)} using column [{geometry_column}]"
    )
    # iterate the shapes applying logic such as mirroring and duplication
    input_vectors = [should_reverse_points(a) for a in list(iterate_shapes(df))]

    ndf = pd.DataFrame(
        [Polygon(p).exterior for p in list(nest(input_vectors, **kwargs))],
        columns=[f"nested.{geometry_column}"],
    )

    ndf = df.join(ndf)
    # this metadata us useful so we know the input nest constraints
    ndf["tile_width"] = kwargs.get(
        "output_bounds_width", DPI_SETTING * DEFAULT_WIDTH_INCHES
    )

    ndf["coff"] = ndf.apply(centroid_offset, axis=1)
    # min_off = ndf.apply(centroid_offset, axis=1)

    # ndf["coff_origin"] = min_bounds(df)
    ndf["max_y"] = ndf[f"nested.{geometry_column}"].map(lambda g: g.bounds[-1]).max()
    # ndf["coff_inv"] = ndf.apply(centroid_offset_inv(geometry_column), axis=1)
    # first invert so it matches how we nested
    ndf[geometry_column] = ndf[geometry_column].map(lambda x: scale(x, yfact=-1))
    # then get the translation
    ndf["coff_inv"] = ndf.apply(centroid_offset, axis=1)
    # then apply the translation
    ndf[f"nested.original.{geometry_column}"] = ndf.apply(
        translate_nested_geom(geometry_column), axis=1
    )

    # we can nest anything called a geometry
    if "geometry" != geometry_column and "geometry" in ndf.columns:
        ndf[f"geometry"] = ndf["geometry"].map(lambda x: scale(x, yfact=-1))
        ndf[f"nested.original.geometry"] = ndf.apply(
            translate_nested_geom("geometry"), axis=1
        )
    logger.debug(f"Nested a dataframe of length {len(df)}")

    if as_geo_pandas:
        from geopandas import GeoDataFrame

        return GeoDataFrame(ndf, geometry=f"nested.original.{geometry_column}")

    if kwargs.get("plot") and not kwargs.get("delay_plot"):
        ax = _plotter(ndf)
        if return_plot:
            return ax

    return ndf


# this could move to the nesting class as its just a datframe wrapper with some simple workflow
def nest_asset_set(
    assets,
    output_bounds_length_pixels=None,
    output_bounds_width_pixels=None,
    geometry_column="outline",
    reverse_points=False,
    **kwargs,
):
    """
    DEPRECATE in place of nest data frame????

    assume assets with the correct schema and a regular index
    the nesting length is the primary parameter not taken from material physical properties
    the kwargs need to be validated including mapping inches to pixel values in places
    if the output_bounds_length inches is passed is converted to pixels

    kwargs:
     output_bounds_length_inches: used instead if pixel values
     output_bounds_width_inches: used instead if pixesl values
     stretch_x: compensation width/x
     stretch_y: compensation length/y
     buffer: default 60

    """
    kwargs = nesting_check_kwargs(
        output_bounds_length_pixels, output_bounds_width_pixels, **kwargs
    )

    def should_reverse_points(v):
        # also make it a list
        v = np.array(v)
        if reverse_points:
            return v[:, [1, 0]]
        return v

    shapes = [should_reverse_points(a) for a in list(assets[geometry_column].values)]

    result = nest(shapes, **kwargs)

    return assets.join(result)
    # this is cached by the flow node but we will be writing the events we care about to kafka with xy, area later. it will have a simple nested structure; nesting with some point data


def evaluate_nesting(
    result_dataframe,
    geometry_column="nested.geometry",
    request_payload=None,
    should_update_shape_props=True,
    **kwargs,
):
    if should_update_shape_props:
        result_dataframe = update_shape_props(
            result_dataframe, geometry_column=geometry_column
        )

    n = result_dataframe

    n["shape_area"] = n[geometry_column].map(lambda g: Polygon(g).area)

    max_y = int(n["max_nested_y"].max())
    min_y = int(n["min_nested_y"].min())
    max_x = int(n["max_nested_x"].max())
    min_x = int(n["min_nested_x"].min())
    nested_height = max_y - min_y
    nested_width = max_x - min_x

    n["total_nested_height"] = nested_height
    n["total_nested_width"] = nested_width
    n["total_nested_area"] = int(n["shape_area"].sum())
    n["total_area"] = nested_height * nested_width
    n["total_packing_factor"] = n["total_nested_area"] / n["total_area"]

    return n


def summarize_nest_evaluation(n):
    def pixels_to_yards(p, i=1):
        return (p / 300**i) * 0.0277778**i

    n["total_nested_shape_area"] = int(n["shape_area"].sum())

    for k in ["total_nested_height", "total_nested_width"]:
        n[f"{k}_yards"] = n[k].map(pixels_to_yards)

    n["total_nest_area_yards"] = (
        n["total_nested_width_yards"] * n["total_nested_height_yards"]
    )
    n["total_shape_area_yards"] = n["total_nested_shape_area"].map(
        lambda x: pixels_to_yards(x, 2)
    )
    n["pieces_per_nest"] = len(n)

    cols = [
        "total_nested_height_yards",
        "total_nested_width_yards",
        "total_nest_area_yards",
        "total_shape_area_yards",
        "total_packing_factor",
        "pieces_per_nest",
    ]
    n = n[cols].rename(
        columns=dict(
            zip(
                cols,
                [
                    "height_nest_yds",
                    "width_nest_yds",
                    "area_nest_yds",
                    "area_pieces_yds",
                    "area_nest_utilized_pct",
                    "piece_count",
                ],
            )
        )
    )

    return dict(n.min())


def make_geometry(a):
    """
    nesting result helper - array to polygon exterior
    """

    def _flat_array(v):
        return np.stack(list(v))

    return Polygon(_flat_array(a)).exterior


def make_geo_dataframe_from_nest_result(df, as_geo_pandas=True):
    """
    as the name says - this is convenient to take the numpy arrays and turn them to a shape for plotting
    the as_geo_pandas is there because if you dont have geopandas installed you might still want the shaely objects
    """
    df["geometry"] = df["nested"].map(make_geometry)
    if as_geo_pandas:
        from geopandas import GeoDataFrame

        return GeoDataFrame(df)
    return df


def make_geometry(a):
    """
    We take the nested stored array of arrays and return the shapely geometry (for display)
    """

    def _flat_array(v):
        return np.stack(list(v))

    return Polygon(_flat_array(a)).exterior


def invert_to_image_space(H):
    """
    return a function that maps a vector to "image space"
    image space reverses the polygons so they are tiled from top to bottom so the 0th location becomes the location at H
    when we load images and place them in the NESTED locations from vector space, they will sit in the correct locations
    when places at there minY location
    """

    def f(v):
        def _flat_array(v):
            return np.stack(list(v))

        v = _flat_array(v)
        v = np.abs(np.array([0, H]) - v)
        return v

    return f


def tile_nesting(data, approx_tile_size=None, eps=10):
    """
    Determines what the tiling should be assigning shapes to pages
    """
    approx_tile_size = approx_tile_size or DEFAULT_APPROX_TILE_SIZE
    maxy = int(data["max_nested_y"].max())
    # print(approx_tile_size, maxy)
    chunks = int(np.ceil(maxy / approx_tile_size))
    chunk_size = int(np.ceil((maxy + eps) / chunks))
    bound = int(chunk_size * chunks)
    res.utils.logger.debug(
        f"Dividing region into {chunks} of size {chunk_size}. The max coord {maxy} fits in {bound}"
    )
    # add page flags

    def is_page(i):
        chunk_lower = (i) * chunk_size
        # make the chunk 1 less for integer based boundary condition
        chunk_upper = (i + 1) * chunk_size - 1

        # print(chunk_lower,chunk_upper)
        def f(row):
            miny, maxy = row["min_nested_y"], row["max_nested_y"]
            return int(not (miny > chunk_upper or maxy < chunk_lower))

        return f

    data["shape_height"] = data["max_nested_y"] - data["min_nested_y"]
    for i in range(chunks):
        data[f"on_page_{i}"] = None
        data[f"on_page_{i}"] = data.apply(is_page(i), axis=1)
        page_boundary = chunk_size * i
        # data[f'y_rel_to_{i}'] = data['min_nested_y'] - page_boundary
        # data[f'y_rel_to_{i}'] = data[f'y_rel_to_{i}'].map(lambda x : x if abs(x) < chunk_size else None)

    data["is_on_a_page"] = data[[f"on_page_{i}" for i in range(chunks)]].sum(axis=1)
    data["tile_height"] = chunk_size
    data["chunks"] = chunks

    if len(data[data["is_on_a_page"] == 0]) > 0:
        raise Exception("One of the shapes is not mapped to a page")

    # if its on one page, the offset is just the mod of the chunk size
    return data


def tile_page(df, page, geometry_column="nested.geometry", **kwargs):
    """
    Given metadata about what the pages should contain, generate the page data
    """
    data = df[df[f"on_page_{page}"] == 1]
    chunk_size = None
    # filter data for this page and check if only one one page or more than one
    chunk_size = int(data["tile_height"].min())
    lower = (page) * chunk_size
    upper = lower + chunk_size - 1

    # may make this contract an assertion instead of a restriction[
    cols = [
        "key",
        "file",
        "shape_height",
        "min_nested_y",
        "max_nested_y",
        "min_nested_x",
        "tile_height",
        "tile_width",
        # this can be removed as it is fetch downstream
        "resonance_color_inches",
        # adding this but we should make this contract more sensible i.e. add everyting to res color or put the contract somewhere else (central)
        # this can be remoed from some reason as res color
        "outline_prescale_factor",
        # one reason why we need this is to provide outlines - the nested geometry is different because it is buffered
        "geometry",
        geometry_column,
        "one_specification_key",
        "body_piece_key",
    ]
    # TODO: smarter contracts
    data = data[[c for c in cols if c in data.columns]]

    data["lower_bound"] = lower
    data["upper_bound"] = upper
    data["offset"] = data["min_nested_y"] - data["lower_bound"]
    data["y"] = data["offset"].map(lambda x: (max(x, 0)))
    data["clip_lower"] = data["offset"].map(lambda x: abs(x) if x < 0 else 0)
    data["clip_upper"] = data.apply(
        lambda row: row["upper_bound"] - row["max_nested_y"], axis=1
    )
    data["clip_upper"] = data.apply(
        lambda row: (
            row["shape_height"] + row["clip_upper"] if row["clip_upper"] < 0 else 0
        ),
        axis=1,
    )

    # Contract on flows: should always have non default index
    return data.reset_index().drop("index", 1)


def get_shape_props(v):
    def _flat_array(v):
        return np.stack(list(v))

    v = _flat_array(v)
    YC = 1
    XC = 0
    return {
        "min_nested_y": int(v[:, YC].min()),
        "min_nested_x": int(v[:, XC].min()),
        "max_nested_y": int(v[:, YC].max()),
        "max_nested_x": int(v[:, XC].max()),
        "shape_height": int(v[:, YC].max()) - int(v[:, YC].min()),
        "shape_width": int(v[:, XC].max()) - int(v[:, XC].min()),
    }


def update_shape_props(df, geometry_column="nested.geometry", **kwargs):
    props = pd.DataFrame(
        [d for d in df[geometry_column].map(lambda g: get_shape_props(np.array(g)))]
    )
    for c in props.columns:
        df[c] = props[c]

    return df


def get_geometry_bounds(df, geometry_column="nested.geometry", **kwargs):
    props = pd.DataFrame(
        [d for d in df[geometry_column].map(lambda g: get_shape_props(np.array(g)))]
    )
    return props["max_nested_y"].max() - props["min_nested_y"].min()


def paginate_nested_dataframe(data, geometry_column="nested.geometry", **kwargs):
    # TODO determine shape bounds as an op in the shapely format
    nest_height = get_geometry_bounds(data)
    logger.debug(
        f"The total nest height is {nest_height} - inverting the layout to the image space in preperation for compositing..."
    )

    if kwargs.get("invert_y"):
        data[geometry_column] = data[geometry_column].map(
            lambda g: invert_axis(g, [nest_height, nest_height])
        )
    data = update_shape_props(data)

    logger.debug(f"tiling nesting using shape statistics")
    return tile_nesting(data)


def nest_asset_make_page(data, **kwargs):
    """
    Higher level function to generate the tiling in two steps
    """
    data["total_nested_height"] = (
        data["max_nested_y"].max() - data["min_nested_y"].min()
    )
    H = data["max_nested_y"].max() - data["min_nested_y"].min()
    # TODO boundary check - do we need to do the inversion or not here
    data["nested"] = data["nested"].map(invert_to_image_space(H))
    data = data[["nested", "files"]]
    data = data.join(
        pd.DataFrame([d for d in data["nested"].map(get_shape_props).values])
    )
    data = tile_nesting(data)
    # if there are no pages assume one page
    page = kwargs.get("page", 0)
    return tile_page(data, page)


def pixels_to_yards(p, i=1):
    return (p / 300**i) * 0.0277778**i


def evaluate_nest_v1(n, should_update_shape_props=False, geometry_column="geometry"):
    """
    This is a legacy nesting evaluation - have not tested all IO but works for reading from the persisted feather
    Assumes n is a part to a saved file for now and then we can find other ways to just pass in dataframes later
    but there is already something that evaluates nests inline so this just needs to be merged when we have a final contract
    for all nest types

    """

    if isinstance(n, str):
        # from the legacy format
        s3 = res.connectors.load("s3")
        n = s3.read(n)
        n[f"nested.{geometry_column}"] = n["nested"].map(lambda g: Polygon(np.stack(g)))

    # it si important to use the polygon of the shape which may be a linear ring (for area)
    n["shape_area"] = n[f"nested.{geometry_column}"].map(lambda g: Polygon(g).area)

    # sometimes we need to add on extra stats
    if update_shape_props:
        n = update_shape_props(n, geometry_column=f"nested.{geometry_column}")

    max_y = int(n["max_nested_y"].max())
    min_y = int(n["min_nested_y"].min())
    max_x = int(n["max_nested_x"].max())
    min_x = int(n["min_nested_x"].min())

    nested_height = max_y - min_y
    nested_width = max_x - min_x

    n["total_nested_height"] = nested_height
    n["total_nested_width"] = nested_width
    n["total_nested_shape_area"] = int(n["shape_area"].sum())
    n["total_area"] = nested_height * nested_width
    n["total_packing_factor"] = n["total_nested_shape_area"] / n["total_area"]

    for k in ["total_nested_height", "total_nested_width"]:
        n[f"{k}_yards"] = n[k].map(pixels_to_yards)

    n["total_nest_area_yards"] = (
        n["total_nested_width_yards"] * n["total_nested_height_yards"]
    )
    n["total_shape_area_yards"] = n["total_nested_shape_area"].map(
        lambda x: pixels_to_yards(x, 2)
    )
    n["pieces_per_nest"] = len(n)

    cols = [
        "total_nested_height_yards",
        "total_nested_width_yards",
        "total_nest_area_yards",
        "total_shape_area_yards",
        "total_packing_factor",
        "pieces_per_nest",
    ]

    # keep the internal separate from contract for now - extra rename
    n = n[cols].rename(
        columns=dict(
            zip(
                cols,
                [
                    "height_nest_yds",
                    "width_nest_yds",
                    "area_nest_yds",
                    "area_pieces_yds",
                    "area_nest_utilized_pct",
                    "piece_count",
                ],
            )
        )
    )

    return dict(n.min())


def plot_comparison(data):
    """
    for showing how the different shapes nest using the concention of column names
    """
    from geopandas import GeoDataFrame
    from res.media.images.geometry import translate

    def center_on(g, h):
        ct = h.centroid
        my_ct = g.centroid
        return translate(g, xoff=ct.x - my_ct.x, yoff=ct.y - my_ct.y)

    renders = []

    for key, df in data.groupby("key"):
        pg = df[df["geometry_column"] == "physical_geometry"].iloc[0][
            "nested.physical_geometry"
        ]
        cg = df[df["geometry_column"] == "compensated_geometry"].iloc[0][
            "nested.compensated_geometry"
        ]
        rg = df[df["geometry_column"] == "raw_geometry"].iloc[0]["nested.raw_geometry"]
        cg = center_on(cg, pg)
        rg = center_on(rg, pg)

        renders.append(
            {
                "key": key,
                "color": "black",
                "geometry_column": "physical",
                "geometry": pg,
            }
        )

        renders.append(
            {
                "key": key,
                "color": "orange",
                "geometry_column": "compensated",
                "geometry": cg,
            }
        )

        renders.append(
            {
                "key": key,
                "color": "blue",
                "geometry_column": "raw",
                "geometry": rg,
            }
        )

    df = GeoDataFrame(renders)
    df.plot(color=df["color"], figsize=(15, 18))

    return df


def nest_from_saved_ppp_pieces(
    file="/Users/sirsh/Downloads/pieces_job.feather",
    outpath="s3://res-data-platform/samples/comct_test2",
):
    """
    emergency nest things again function - take care with output bound length + how many thigns fit in memory to make print files
    """
    import res
    import pyvips
    import pandas as pd
    from pyvips import Image as vips_image
    from res.learn.optimization.nest import nest_transformations_apply, nest_transforms
    from res.flows.dxa.printfile import _try_get_material_comp

    s3 = res.connectors.load("s3")
    pcsin = pd.read_feather(file)
    # if they dont fit
    pcs = pcsin  # [150:]
    all_bounds = [
        s3.read(uri)[["column_1", "column_0"]].values for uri in pcs["outline_uri"]
    ]

    # nest

    fc = res.flows.FlowContext({})

    marker_params = _try_get_material_comp(fc, "COMCT")

    buffer = 70

    output_bounds_length = 100000

    job_key = "sample_job2_123"
    one_keys = []

    nest_transforms = nest_transforms(
        all_bounds,
        key=job_key,
        **marker_params,
        buffer=buffer,
        output_bounds_length=output_bounds_length,
    )

    # todo get the nesting out bounds

    image_paths = list(pcs["filename"])

    def get_image(uri):
        image = s3.read(uri)

        def f():
            return image

        return f

    nest_transformations_apply(
        nest_transforms,
        image_paths=image_paths,
        image_suppliers=[get_image(uri) for uri in image_paths],
        output_path=outpath,
        key=job_key,
        **marker_params,
        buffer=buffer,
        output_bounds_length=9000,
    )
