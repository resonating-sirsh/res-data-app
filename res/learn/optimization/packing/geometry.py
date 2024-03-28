import numpy as np
from shapely.affinity import affine_transform
from shapely.geometry import Polygon
from shapely.ops import orient


def translate(shape, vec):
    """
    Convenience function to translate a shape.
    """
    return affine_transform(shape, [1, 0, 0, 1, vec[0], vec[1]])


def shapely_to_skgeom(p):
    """
    Convert a shapely polygon to an skgeom polyon and make sure its "simple"
    meaning that it doesnt have the beginning and end vertices that co-incide like shapely does.
    """
    # return sg.Polygon(p.exterior.coords[:-1])
    raise Exception("Unused codepath")


def skgeom_to_shapely(p):
    """
    Convert a skgeom polygon back into a shapely one.
    """
    # return Polygon(p.outer_boundary().coords)
    raise Exception("Unused codepath")


def negate_shape(shape):
    """
    Compute -shape.  Its the set of all points p so that -p is in the shape.  Or equivalently scaling the
    shape by -1 on all axes.
    """
    return affine_transform(shape, [-1, 0, 0, -1, 0, 0])


def count_vertices(p: Polygon):
    """
    Number of vertices of a polygon
    """
    return len(p.exterior.coords)


def simple_inscribed_polygon(p: Polygon, max_vertices=10):
    """
    Generate an inscribed polygon in p with fewer vertices than p.
    If we test for interesection of two of these simple inscribing polygons then
    the test is going to be faster than if we tested the original polygons, but
    it might say they don't intersect when they actually do -- but never that they
    do intersect when the original polygons dont.
    Note that this method doesnt work for pieces which are concave -- should try to
    improve it.
    """
    if count_vertices(p) <= max_vertices:
        return p
    tolerance = 1
    simple = p.simplify(tolerance, preserve_topology=False)
    while count_vertices(simple) > max_vertices and simple.within(p):
        tolerance *= 2
        simple = p.simplify(tolerance, preserve_topology=False)
    if not simple.within(p):
        # see if we just over-simplified the polygon
        simple = p.simplify(tolerance / 2, preserve_topology=False)
    if not simple.within(p):
        # any simplifcation along these lines just leads to not being an inscribed polygon.
        return p
    return simple


def convex_minkowski(a, b):
    """
    Ad-hoc version of the minkowski sum operation which only works for convex polygons.
    """
    points = []

    def get_ordered_vertices(s):
        p = [np.array(c) for c in orient(s).boundary.coords][:-1]
        i = min(range(len(p)), key=lambda i: (p[i][1], p[i][0]))
        return p[i:] + p[:i]

    ap = get_ordered_vertices(a)
    bp = get_ordered_vertices(b)
    an = len(ap)
    bn = len(bp)
    i = 0
    j = 0
    while i < an or j < bn:
        points.append(ap[i % an] + bp[j % bn])
        cross = np.cross(ap[(i + 1) % an] - ap[i % an], bp[(j + 1) % bn] - bp[j % bn])
        if cross >= 0:
            i += 1
        if cross <= 1:
            j += 1
    return Polygon(points)
