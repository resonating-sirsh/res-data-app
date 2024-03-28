"""
TODO: add some tools for working with qr codes  
#https://blog.jonasneubert.com/2019/01/23/barcode-generation-python/
#https://boofcv.org/index.php?title=Performance:QrCode
#https://boofcv.org/index.php?title=Performance:QrCode
import segno
from IPython.core.display import display, HTML
qr = segno.make('https://datadev.resmagic.io/res-connect/qr/instructions/1000000/piece1', micro=False)
#qr = segno.make('10045110900/100', micro=True)
display(HTML(qr.svg_inline()))

qr.save("/Users/sirsh/Downloads/qr.png", scale=2)

from matplotlib import pyplot as plt
from skimage.io import imread
plt.imshow(imread("/Users/sirsh/Downloads/qr.png"),'gray')
a = plt.axis('off')
"""

import segno
import io
from PIL import Image, ImageFont, ImageDraw
import numpy as np
import cv2
import qrcode

import res
import os

FONT_LOCS = os.environ.get("FONT_LOCATION", ".")


def get_one_label_qr_code(
    data, uri="https://www.oneis.one", size=(350, 350), include_the_01=True
):
    """
    stub if needed https://data.resmagic.io/res-connect/one_code

    get the ONE label with fix size hard coded

    #WAS asked to do a hack which could lead to issues
    #we need a new future coding system for these and then migrate the existing mappings to it

    from res.media.images.qr import get_one_label_qr_code
    hack_id = 1
    for i in range(100):
        code = f"9{hack_id}{str(i).zfill(3)}10101"
        print(code)
        im = get_one_label_qr_code(code, size= (350,350) )
        im.save(f"/Users/sirsh/Downloads/MR_LabelPremade/{code}.png")

    """
    data = f"{uri}/{data}"
    qr_code = np.asarray(qrcode.make(data).convert("RGBA"))
    size_code = size  # (size[0], size[1] - 40)
    qr_scaled = cv2.resize(qr_code, size_code)
    font_size = 40
    QR = Image.fromarray(qr_scaled)

    try:
        font = ImageFont.truetype(font=f"{FONT_LOCS}/Helvetica.ttf", size=font_size)
        # get a drawing context
        d = ImageDraw.Draw(QR)
        if include_the_01:
            d.text(
                (font_size - 5, size_code[-1] - (font_size)),
                "0",
                font=font,
                fill=(0, 0, 0),
            )
            d.text(
                (size_code[-1] - (font_size + 15), size_code[-1] - font_size),
                "1",
                font=font,
                fill=(0, 0, 0),
            )
    except Exception as ex:
        res.utils.logger.info(f"Failed to add text on QR: {ex}")
    return QR


def encode(data, micro=False, **kwargs):
    """
    REturn a QR encoding in a specific format SVG is the vector format but we could return png too
    """
    if "scale" not in kwargs:
        kwargs["scale"] = 6

    qr = segno.make(str(data), micro=micro)
    b = io.BytesIO()
    qr.save(b, "png", border=0, **kwargs)
    return Image.open(b).convert("RGBA")


def decode(file_or_buffer, **kwargs):
    pass
