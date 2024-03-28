from colorthief import ColorThief
import os
import boto3
import numpy as np
from sklearn.neighbors import KDTree

BASE_COLORS_RGB = np.array([
    [230, 25, 75], #Red 0
    [60, 180, 75], #Green 1
    [255, 255, 25], #Yellow 2
    [0, 130, 200], #Blue 3
    [245, 130, 48], #Orange 4
    [145, 30, 180], #Purple 5
    [70, 240, 240], #Cyan 6
    [140, 50, 230], #Magneta 7
    [210, 245, 60], #Lime 8
    [250, 190, 212], #Pink 9
    [0, 128, 128], #Teal 10
    [220, 190, 255], #Lavender 11
    [170, 170, 40], #Brown 12
    [255, 250, 200], #Beige 13
    [128, 0, 0], #Maroon 14
    [170, 255, 195], #Mint 15
    [128, 128, 0], #olive 16
    [255, 215, 180], #Apricot 17
    [0, 0, 128], #navy 18
    [128, 128, 128], #Gray 19
    [255, 255, 255], #White 20
    [0, 0, 0] #Black 21
])

BASE_COLORS_NAME = [
    "Red", "Green", "Yellow", "Blue", "Orange", "Purple", "Cyan", "Magneta", "Lime", "Pink", "Teal",
    "Lavender", "Brown", "Beige", "Maroon", "Mint", "Olive", "Apricot", "Navy", "Gray", "White", "Black"
]

TMP_FILE = "tb.png"

def _get_image(bucket, key):
    session = boto3.Session()
    s3 = session.resource('s3')
    s3.Bucket(bucket).download_file(key, TMP_FILE)

def _extract_dominant_rgb(dominant_count):
    color_thief = ColorThief(TMP_FILE)
    dominant_color_rgb = color_thief.get_color(quality=1)
    palette = color_thief.get_palette(color_count=dominant_count)
    return {"dominant_color": dominant_color_rgb, "dominant_palette": palette}

def _get_color_name(rgb_value):
    dist, idx = KDTree(BASE_COLORS_RGB).query([[rgb_value[0], rgb_value[1], rgb_value[2]]], k=1) 
    return BASE_COLORS_NAME[idx[0][0]]

def extract_colors(data):

    _get_image(data["s3_bucket"], data["s3_key"])

    dominant_colors_rgb = _extract_dominant_rgb(data["dominant_colors_count"])
    print(f"Dominant Color (RGB): {dominant_colors_rgb['dominant_color']}")
    print(f"Color Palette (RGB): {dominant_colors_rgb['dominant_palette']}")

    # Removing temp file after color is extracted
    os.remove(TMP_FILE)

    # Returning dominant colors based on the requested format
    palette_colors_names = []
    if data["color_format"] == "name":
        dominant_color_name = _get_color_name(dominant_colors_rgb['dominant_color'])
        for rgb_color in dominant_colors_rgb["dominant_palette"]:
            color_name = _get_color_name(rgb_color)
            if color_name not in palette_colors_names:
                palette_colors_names.append(color_name)
        #Making sure all colors are included in the palette
        if dominant_color_name not in palette_colors_names:
            palette_colors_names.append(dominant_color_name)
        return {
            "color_format": "name",
            "dominant_color": dominant_color_name,
            "palette_colors": palette_colors_names
        }

    else:
        return{
            "color_format": "rgb",
            "dominant_color": dominant_colors_rgb['dominant_color'],
            "palette_colors": dominant_colors_rgb['dominant_palette']
        }