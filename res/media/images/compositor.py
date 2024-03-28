#! wget https://i.etsystatic.com/13820514/r/il/ebac57/1056350238/il_794xN.1056350238_qtlt.jpg

# import pyvips
# %matplotlib inline
# from matplotlib import pyplot as plt
# from skimage.io import imread

# tile_count = 10
# tiles = [pyvips.Image.new_from_file(f"il_794xN.1056350238_qtlt.jpg",
#                                     access="sequential") for x in range(tile_count)]
# im = pyvips.Image.arrayjoin(tiles, across=tile_count)
# im.write_to_file("out.png")

# plt.imshow(imread('out.png'))

# https://github.com/libvips/libvips/issues/59

# https://stackoverflow.com/questions/33195055/how-to-perform-logical-operation-and-logical-indexing-using-vips-in-python


from uuid import uuid1

from numpy.testing._private.utils import KnownFailureException
from res.utils import logger
from PIL import Image
from skimage.io import imread
import res
from skimage.transform import resize
import numpy as np

DPI_SETTING = 300
PIXEL_PER_MILLIMETER_SETTING = DPI_SETTING / 25.4


def composite_images(
    page_df,
    clip_lower_field="clip_lower",
    clip_upper_field="clip_upper",
    x_field="min_nested_x",
    y_field="y",
    res_color_fn_factory=lambda row: lambda im: im,
    **kwargs,
):
    """
    Compsite images on the page - images are clipped
    The resonce color function provides an overlay image that is generated dynamically from page information like body/one etc.
    """

    def read_image_for_row_with_offset(row):
        s3 = res.connectors.load("s3")
        res.utils.logger.debug(f"Reading {row['file']}")
        im = s3.read(row["file"]) if row["file"][:5] == "s3://" else imread(row["file"])
        # provide somethinf that genererates an overlay function as a function of the data in the row
        # the function should always resolve f(im) -> im or None ... the default factory just returns the original image
        overlay_fn = res_color_fn_factory(row)
        # add the resonance color to the piece image here if a function is supplied - for example the caller can pass a closure wrapping res_color.add_overlay(im, dxf, one, piece ....)
        im = overlay_fn(im) if overlay_fn else im
        res.utils.logger.debug(f"Shape: {im.shape}")
        lb = row[clip_lower_field]
        ub = row[clip_upper_field] if row[clip_upper_field] != 0 else im.shape[0]
        return im[lb:ub, :, :], row.get(y_field), row.get(x_field)

    # use the max values for variable sized pages - at least on the width
    page_height = int(page_df["tile_height"].max())
    page_width = int(page_df["tile_width"].max())
    stretch_x = kwargs.get("stretch_x")
    stretch_y = kwargs.get("stretch_y")

    res.utils.logger.debug(f"Creating a canvas with dims {(page_width,page_height)}")

    canvas = Image.new("RGBA", (page_width, page_height), (0, 0, 0, 0))

    for row in page_df.to_dict("records"):
        im, y, x = read_image_for_row_with_offset(row)
        # contract - should be known if this is numpy or PIL
        im_shape = im.shape

        new_pil_size = None

        if stretch_y and stretch_x:
            logger.debug(
                f"Will resize image from numpy with shape {im_shape} by x={stretch_x}, y={stretch_y}"
            )
            new_pil_size = (int(stretch_x * im.shape[1]), int(stretch_y * im.shape[0]))

        im = Image.fromarray(im)

        if new_pil_size:
            size = im.size
            im = im.resize(new_pil_size)
            logger.debug(
                f"Image in PIL resized from {size} to {im.size} - PIL uses XY dims"
            )

        canvas.paste(im, (x, y), im)

    # convention is always to yield even if one item ..
    yield canvas
    # TODO: - test boundaries and trim last page
    # find the height that fits for a width of e.g. 18k material and 4 channels ( we can remove a channel when saving the page if we want)


def join_images(parts, output_path_full, header_image=None, footer_image=None):
    """
    Merge images from file name parts into a composite
    Optionally add header and footers from memory e.g. barcodes and timestamps
    - could support reading these from file but they are typically dynamically generated
    """
    from pyvips import Image as PVImage
    from res.media.images import pyvips_init

    pyvips_init()

    def vertical_joiner(images, base=None):
        for i, im in enumerate(images):
            base = im if i == 0 else base.join(im, "vertical", expand=True)
        return base

    def make_vips_image(image):
        image = np.array(image)
        return PVImage.new_from_memory(
            image.data,
            image.shape[1],
            image.shape[0],
            bands=image.shape[-1],
            format="uchar",
        ).copy(
            interpretation="srgb",
            xres=PIXEL_PER_MILLIMETER_SETTING,
            yres=PIXEL_PER_MILLIMETER_SETTING,
        )

    s3 = res.connectors.load("s3")

    downloaded = []

    for file in parts:
        # make sure the downloaded files is unique
        fname = f"{str(uuid1())}.png"
        res.utils.logger.info(f"Downloading {file} - >{fname}")
        downloaded.append(s3._download(file, "/tmp/", filename=fname))

    images = [
        PVImage.new_from_file(filename, access="sequential") for filename in downloaded
    ]

    if images:
        # TODO could probe the image lengths and use this if there is any advantage
        # joined = PVImage.arrayjoin(images, across=1)
        joined = vertical_joiner(images)

        if header_image is not None:
            joined = make_vips_image(header_image).join(joined, "vertical", expand=True)
        # TODO: maybe footer too

        res.utils.logger.info(f"writing out, preparing to upload")
        joined.set("xres", PIXEL_PER_MILLIMETER_SETTING)
        joined.set("yres", PIXEL_PER_MILLIMETER_SETTING)
        joined.write_to_file("/tmp/all.png")
        res.utils.logger.info(f"uploading to {output_path_full}")
        s3.upload("/tmp/all.png", output_path_full)
    else:
        res.utils.logger.warn(
            f"There were no images to join possibly because we have only run nesting in mode [plan]"
        )


# This version will be neater and does not need download and upload explicitly
def _join_images(images, filename):
    """
    https://stackoverflow.com/questions/58874087/how-to-convert-dzi-files-to-multi-tile-pyramidal-tiff-format/58927878#58927878
    This should be tested for S3 files and can be warpped in a handler for use as a service
    The service will pass event, context

    assets : [
        {key, value}
    ]
    args:[
        output
    ]
    """
    try:
        from pyvips import Image as PVImage
        from res.media.images import pyvips_init

        pyvips_init()

        s3 = res.connectors.load("s3")
        logger.info(f"Merging {len(images)} to {filename}")
        images = [
            PVImage.new_from_file(file_or_buffer, access="sequential")
            for file_or_buffer in s3.get_file_buffers(images)
        ]
        im = PVImage.arrayjoin(images, across=len(images))
        # this will return a temp file context that is uploaded to S3 on exit
        with s3.get_file_buffer(filename, "wb") as file:
            im.write_to_file(file.name)

    except Exception as ex:
        logger.error(f"Failed to join images {repr(ex)}")
