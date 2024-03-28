from res.learn.optimization.packing.geometry import (
    count_vertices,
    simple_inscribed_polygon,
)
from shapely.geometry import Polygon, Point, box
import json


def test_count_vertices():
    assert count_vertices(box(0, 1, 0, 1)) == 4


def test_simple_inscribed_polygon_in_circle():
    circle = Point(0, 0).buffer(100)
    inscribed_polygon = simple_inscribed_polygon(circle, 6)
    assert count_vertices(inscribed_polygon) <= 6
    assert inscribed_polygon.within(circle)


def test_simple_inscribed_polygon_in_non_convex_shape():
    # build a box with a bite taken out
    non_convex_shape = box(0, 0, 100, 100).difference(Point(100, 50).buffer(25))
    inscribed_polygon = simple_inscribed_polygon(non_convex_shape, 10)
    # currently the method fails to simplify the shape since its concave.
    # but it should still return something which can be considered "inscribed"
    assert inscribed_polygon.within(non_convex_shape)

def test_simple_inscribed_polygon_non_convex_complicated():
    with open("res_tests/.sample_test_data/shapes/non_convex_piece.json", "r") as f:
        non_convex_shape = Polygon(json.load(f))
    inscribed_polygon = simple_inscribed_polygon(non_convex_shape, 10)
    # currently the method fails to simplify the shape since its concave.
    # but it should still return something which can be considered "inscribed"
    assert inscribed_polygon.within(non_convex_shape)