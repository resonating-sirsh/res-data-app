import pytest
from shapely.geometry import Point
from res.learn.optimization.packing.annealed.packing import Packing
from res.learn.optimization.packing.annealed.packing_geometry import (
    choose_geom,
)


def test_packing_circles_offset():
    # try to just pack a bunch of circles in a way where subsequent rows need to be offset.
    # radius 10 circles in a box thats wide enough for 3.5 across
    packing = Packing(max_width=360)
    n = 10
    for i in range(n):
        packing = packing.add(choose_geom(Point(0, 0).buffer(50)), i)
    centers = [b.translated.centroid.coords[0] for b in packing.to_packingshapes()]
    for i in range(n):
        row = i // 3
        pos = i % 3
        assert centers[i][0] == pytest.approx(50 * (1 + row % 2) + 100 * pos, 1)
        assert centers[i][1] == pytest.approx(50 + 100 * row, 1)


def test_margins():
    margin = 10
    max_width = 100
    packing = Packing(max_width=max_width, margin_x=margin)
    n = 10
    for i in range(n):
        packing = packing.add(choose_geom(Point(0, 0).buffer(30)), i)
    for p in packing.to_packingshapes():
        assert p.translated.bounds[0] >= margin
        assert p.translated.bounds[2] <= max_width - margin


def test_too_wide():
    packing = Packing(max_width=50)
    with pytest.raises(ValueError):
        packing.add(choose_geom(Point(0, 0).buffer(26)), 0)
