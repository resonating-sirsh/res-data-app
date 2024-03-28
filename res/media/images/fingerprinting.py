import res.media.images.providers as providers
import res.media.images.outlines as outlines
import numpy as np
import pandas as pd
from PIL import Image
import imagehash
from shapely.geometry import Polygon
from shapely.geometry.polygon import orient


def compare_hash():
    img = np.asarray(
        Image.open(
            "/Users/chris/Documents/Imagehashtest/JR-3108-V3-CTJ95-XL-FEETUW.png"
        )
    ).copy()
    parts = list(outlines.parts(img))
    outs = outlines.part_outlines(parts)
    ripped_dxf = providers.parse_dxf("/Users/chris/Documents/JR-3108-V3-CTJ95-XL.dxf")
    piece_dict = []
    id = 0
    for each in outs:
        img_hash = imagehash.average_hash(Image.fromarray(each))
        piece_dict.append({"id": id, "hash": img_hash, "poly": each})
        id = id + 1
    id = 0
    dxf_dict = []
    for each in ripped_dxf:
        img_hash = imagehash.average_hash(Image.fromarray(each))
        dxf_dict.append({"id": id, "hash": img_hash, "poly": each})
        id = id + 1
    return piece_dict, dxf_dict


def shapely_to_png():
    img = Image.new("RGB", (2000, 2000))
    draw = ImageDraw.Draw(img)
    draw.polygon(Polygon(dfx.iloc[0].img))
    x = map(int, x)
    y = map(int, y)
    draw.polygon(zip(x, y), fill="wheat")


def normalize(polygon):
    def normalize_ring(ring):
        coords = ring.coords[:-1]
        start_index = min(range(len(coords)), key=coords.__getitem__)
        return coords[start_index:] + coords[:start_index]

    polygon = orient(polygon)
    normalized_exterior = normalize_ring(polygon.exterior)
    normalized_interiors = list(map(normalize_ring, polygon.interiors))
    return Polygon(normalized_exterior, normalized_interiors)
