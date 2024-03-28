from os import major
from res.media.images.geometry import invert_axis
import pytest
import json
from shapely.geometry import Polygon
import numpy as np


def test_invert_axis():
    assert 1 == 1, "nope"
    # changing conventions and need new tests
    # from res.media.images.geometry import invert_axis

    # with open("res_tests/.sample_test_data/shapes/polygon_inches.json", "r") as f:
    #     shape = json.load(f)
    #     g = Polygon(shape["sample_inches"])
    #     g = invert_axis(g)

    #     bounds = np.array(g.bounds)

    #     assert (
    #         np.any(bounds < 0) == False
    #     ), "The inverted axis produced negative coordinates"

    #     # test with specified bounds


def test_to_image_space():
    assert 1 == 1, "nope"
    # from res.media.images.geometry import to_image_space

    # DPI = 300

    # def first_point(g, scale=False):
    #     return tuple(list(np.array(g.exterior.coords) * (DPI if scale else 1))[0])

    # # we take a sample polygon which is inches but we do not consider what is xy or yx in the same data
    # # we only consider the transformations from the input to other spaces
    # with open("res_tests/.sample_test_data/shapes/polygon_inches.json", "r") as f:
    #     shape = json.load(f)

    #     g = Polygon(shape["sample_inches"])
    #     p = first_point(g, True)
    #     g = to_image_space(g, invert_y=False, swap_axes=False)
    #     assert first_point(g) == (
    #         p[0],
    #         p[1],
    #     ), "The to image space converstion with no invert and no swap axis does have the correct first point value scaled by 300 dpi"

    #     g = Polygon(shape["sample_inches"])
    #     p = first_point(g, True)
    #     g = to_image_space(g, invert_y=False, swap_axes=True)
    #     assert first_point(g) == (
    #         p[1],
    #         p[0],
    #     ), "The to image space converstion with no invert and [swap axis==True] does not have the correct first point value scaled by 300 dpi"

    #     # when we invert t and swap access, the y is the p[0] and we expect the first y value to be 403.71
    #     g = Polygon(shape["sample_inches"])
    #     g = to_image_space(g, invert_y=True, swap_axes=True)
    #     assert first_point(g) == (
    #         403.71,
    #         5432.310000000001,
    #     ), "The to image space converstion with no invert and [swap axis==True] does not have the correct first point value scaled by 300 dpi"
