import numpy as np
import pytest
from numpy.testing import assert_array_equal
from shapely.geometry import Polygon, Point, box
from res.learn.optimization.packing.annealed.packing_geometry import (
    PackingGeometry,
    choose_geom,
    get_frontier_points,
    bounding_polygon,
)


def test_get_frontier_points_box():
    # should return just the coordinates of the top 2 vertices
    assert_array_equal(
        np.array([[0, 100], [100, 100]]), get_frontier_points(box(0, 0, 100, 100))
    )


def test_get_frontier_points_pentagon():
    assert_array_equal(
        np.array(
            [
                [0, 100],
                [50, 120],
                [100, 100],
            ]
        ),
        get_frontier_points(
            Polygon([[0, 0], [0, 100], [50, 120], [100, 100], [100, 0]])
        ),
    )


def test_bounding_polygon_box():
    shape = box(0, 0, 100, 100)
    bound = bounding_polygon(shape, 10)
    # shapely is not clear with how it handles open/closed sets.
    assert shape.within(bound.buffer(1e-10))
    assert len(list(bound.exterior.coords)) <= 11


def test_bounding_polygon_circle():
    shape = Point(0, 0).buffer(100)
    bound = bounding_polygon(shape, 10)
    assert shape.within(bound.buffer(1e-10))
    assert len(list(bound.exterior.coords)) <= 11


def test_bounding_polygon_concave():
    shape = box(0, 0, 100, 100) - Point(100, 50).buffer(50)
    bound = bounding_polygon(shape, 10)
    assert shape.within(bound.buffer(1e-10))
    assert len(list(bound.exterior.coords)) <= 11


def test_choose_geom():
    assert choose_geom(box(0, 0, 100, 100)).type_name == "axis_aligned_box"
    assert (
        choose_geom(Polygon([(0, 50), (50, 100), (100, 50), (50, 0)])).type_name
        == "oriented_box"
    )
    assert choose_geom(Point(0, 0).buffer(100)).type_name == "simplified_bound"


def test_packing_circles():
    # this is kind of integration testing everything related to the geometry of packing.
    # two circles touching on the x axis.
    c1 = Point(200, 0).buffer(200)
    c2 = Point(600, 0).buffer(200)
    points = get_frontier_points(c1) + get_frontier_points(c2)
    frontier = [
        (points[i - 1][0], points[i - 1][1], points[i][0], points[i][1])
        for i in range(1, len(points))
    ]
    # shape to place
    c3 = Point(0, 0).buffer(200)
    c3 = PackingGeometry(c3, [c3], "test")
    coords = c3.best_fit_coordinates(frontier, 800)
    full_translation = coords + c3.local_offset
    # the circle should be at (400, 200*sqrt(3)) -- since it should be between the two circles we placed, and high enough to not intersect them
    assert full_translation[0] == pytest.approx(400, 1)
    # theres evidently some slop in these polygonal representations of circles
    assert full_translation[1] == pytest.approx(200 * np.sqrt(3), 1)


def test_packing_boxes():
    # two boxes touching on the x axis.
    c1 = box(0, 0, 40, 20)
    c2 = box(40, 0, 80, 20)
    points = get_frontier_points(c1) + get_frontier_points(c2)
    frontier = [
        (points[i - 1][0], points[i - 1][1], points[i][0], points[i][1])
        for i in range(1, len(points))
    ]
    # shape to place
    c3 = box(0, 0, 20, 20)
    c3 = PackingGeometry(c3, [c3], "test")
    coords = c3.best_fit_coordinates(frontier, 80)
    full_translation = coords + c3.local_offset
    # the box should be at (0, 20)
    assert full_translation[0] == pytest.approx(0, 1e-3)
    assert full_translation[1] == pytest.approx(20, 1e-3)
