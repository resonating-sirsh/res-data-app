import res
import numpy as np
from PIL import Image
from res.media.images.outlines import try_pad_image


def extract_images(
    file="/Users/sirsh/Downloads/CC-6001-V10-DRESS_reorgd-0-SM.pdf",
    rotate=90,
    pad=1,
    draw_outline=False,
):
    """
    extracts all images from pdf although for our use case assumed one image in a pdf

    NOTE: the alpha channel is just tested for something that does not have internal transparencies here
    and in general we will likely want to draw the outline for better separation

    - sometimes a file does not have an image - we should raise an error that is meaningful
    - the page.ressources show if we have any stream objects but have seen cases where there us some
    weird color and know raster layer. not sure what this means
    """
    from pikepdf import Pdf, PdfImage

    # filename = ""
    f = Pdf.open(file) if not isinstance(file, Pdf) else file
    for i, page in enumerate(f.pages):
        for j, (name, raw_image) in enumerate(page.images.items()):
            image = PdfImage(raw_image).as_pil_image()

            if image.mode == "RGB" and hasattr(raw_image, "SMask"):
                mask = PdfImage(raw_image.SMask).as_pil_image()
                # return mask, image
                image.putalpha(mask)
                # piece_outline = get_piece_outlines_from_mask(try_pad_image(np.asarray(mask)))[0]
            else:
                # one reason to force alpha is for latter padding but we may still need to do some understading of the image in here
                # for example add other outlines
                image = image.convert("RGBA")

            if rotate:
                image = image.rotate(rotate, expand=True)

            if pad:
                image = Image.fromarray(try_pad_image(np.asarray(image)))

            yield image


def _iterate_s3_pdf_archive(filename, raise_reader_error=False):
    """
    creating a function here in advance of a better archive interface on the s3 connector
    """
    import s3fs
    from zipfile import ZipFile
    import io
    from pikepdf import Pdf, PdfImage

    # "s3://meta-one-assets-prod/color_on_shape/cc_6001/v10/somelu/zipped_pieces.zip"

    fs = s3fs.S3FileSystem()
    input_zip = ZipFile(io.BytesIO(fs.cat(filename)))
    for name in input_zip.namelist():
        fname = name.split("/")[-1]
        if fname[-4:] == ".pdf":
            try:
                yield fname, Pdf.open(io.BytesIO(input_zip.read(name)))
            except Exception as ex:
                if raise_reader_error:
                    raise ex


def iterate_s3_pdf_archive_images(filename):
    """
    get numpy images tensors and file name from the zipped pdf
    """
    for fname, file in _iterate_s3_pdf_archive(filename):
        for image in extract_images(file):
            yield fname, np.asarray(image)
