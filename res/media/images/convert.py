import subprocess
from pathlib import Path
import tempfile
from res.connectors.s3 import S3Connector
from res.utils import logger
import os

# TODO: check what styles do not have thumbnails e.g. : recqmCNvOAYmaTfmb


def rasterize_png(url_in, url_out, **kwargs):
    """
    Given a file on S3 and a target location on S3
    use ghostscript to rasterize the file with options and upload
    """
    s3 = S3Connector()
    dpi = kwargs.get("dpi", 300)
    ext = None
    bucket, in_file = s3.split_bucket_and_blob_from_path(url_in)
    ext = Path(in_file).suffix
    filename = Path(in_file).stem
    outfile = f"/tmp/{filename}.png"
    s3file = f"{bucket}/{in_file}"
    if not s3.exists(url_in):
        logger.info(f"{s3file} does not exist. Skipping conversion on this one")
        # logger.incr("missing_thumbnail_files",1)
        return

    with s3._s3fs.open(s3file, "rb") as s3f:
        with tempfile.NamedTemporaryFile(
            suffix=ext, prefix=filename, mode="wb"
        ) as input:
            input.write(s3f.read())
            input.flush()

            if os.stat(input.name).st_size == 0:
                logger.info(
                    f"{s3file} exists but has zero bytes in it. Skipping conversion of this one."
                )
                # logger.incr("missing_thumbnail_files",1)
                return
            channels = "pngalpha"  # or png16m
            try:
                subprocess.check_output(
                    [
                        "gs",
                        f"-sDEVICE={channels}",
                        f"-r{dpi}",
                        "-o",
                        outfile,
                        "-dBufferSpace=2000000000",
                        "-dNumRenderingThreads=4",
                        "-f",
                        input.name,
                    ]
                )

                # url_out may need to be
                bucket, out_file = s3.split_bucket_and_blob_from_path(url_out)
                destination = f"{bucket}/{out_file}"
                s3._s3fs.put(outfile, destination)

            except subprocess.CalledProcessError as error:
                raise error
