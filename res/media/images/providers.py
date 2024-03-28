import ezdxf
from ezdxf.addons import geo
import numpy as np
from shapely.geometry import shape
from pdfreader import PDFDocument
from pdfreader import SimplePDFViewer
import pikepdf
import imagehash
from PIL import Image

# TODO: Make .ai reading time efficient

# generate polygon arrray from dxf's
def parse_dxf(filepath):
    doc = ezdxf.readfile(filepath)
    msp = doc.modelspace()
    poly_test = []
    for e in msp.query("LWPOLYLINE"):
        hold = geo.proxy(e)
        poly_test.append(shape(hold))

    # convert polygon set to np.array
    poly_array = [np.array(n.exterior.coords) for n in poly_test]
    return poly_array


def parse_illustrator(filepath):
    fd = open(filepath, "rb")
    doc = PDFDocument(fd)
    page = next(doc.pages())
    pagehold = next(doc.pages())
    gallery = []
    for each in page.Resources.Pattern.keys():
        pagehold = next(doc.pages())
        print("getting obj", each)
        obj = pagehold.Resources.Pattern[str(each)]
        print("making img", each)
        img = obj.to_Pillow()
        print("appending", each)
        gallery.append(obj)
    return gallery


# Takes several years to run currently ^


def parse_image(filepath):
    fd = open(filepath, "rb")
    viewer = SimplePDFViewer(fd)
    viewer.render()
    all_page_images = viewer.canvas.images
    all_page_inline_images = viewer.canvas.inline_images
    img = all_page_images["img0"]


def gen_hash(img):
    hsh = imagehash.average_hash(Image.open(img))
    print(hsh)
