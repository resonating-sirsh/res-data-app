"""
All functions return geometry collections even if cardinality 1
This is to support image processing pipelines
"""

import shapely
from shapely import geometry
from shapely.geometry import LineString, MultiPoint, Point, box, MultiLineString
from shapely.geometry.polygon import LinearRing, Polygon
from shapely.ops import (
    cascaded_union,
    orient,
    unary_union,
    polygonize,
    nearest_points,
    transform,
    substring,
)
from shapely.affinity import scale, translate, rotate
import numpy as np
import uuid

import pandas as pd
from typing import List
from shapely import affinity
from sklearn.neighbors import BallTree, KDTree
from scipy.spatial.distance import euclidean
import res
import numpy as np
from scipy.spatial import ConvexHull
from shapely.validation import make_valid
from collections.abc import Iterable
import json


types = [MultiLineString]


def geometry_svg_to_image(g, scale=1.0, color="blue"):
    """
    in case you want to save shapes as images
    scale it down if the target png will be bigger than allowed - you can resize precisely after

    """
    from PIL import Image
    from io import BytesIO
    from cairosvg import svg2png

    def styled_svg(shp):
        """
        shapely does
        fill="none" stroke="#66cc99"
        """
        svg = shp._repr_svg_()
        svg = svg.replace("#66cc99", color)
        return svg

    return Image.open(BytesIO(svg2png(styled_svg(g), scale=scale)))


def get_surface_normal_vector_at_point(pt, outline, explain=False, buffer=50):
    try:
        g = outline.intersection(pt.buffer(buffer))
        return normal_vector_via_chord(g=g, outline=outline, pt=pt, explain=explain)
    except:
        # print(traceback.format_exc())
        # trying to use the angle of the nearest line
        return None


def pair_nearest_points_as_lines(a, b, threshold):
    """
    takes nearby points and pairs them together
    points in a are matched to points in by if they are less than threshold distance from each other
    multi line strings are returned
    """

    # print(a)
    def pt_array(p):
        return [p.x, p.y]

    def as_ndarray(x):
        return [pt_array(p) for p in x]

    d, ids = KDTree(as_ndarray(b)).query(as_ndarray(a), k=1)
    d, ids = d, ids[:, 0]

    return MultiLineString(
        [
            [pt_array(a[i]), pt_array(b[ids[i]])]
            for i, d_i in enumerate(d)
            if d_i < threshold
        ]
    )


def line_at_point(pt, angle=0, length=50):
    """
    given a point use the disc trick to make a line at an angle of a certain length around a point
    """
    p = pt.buffer(length / 2.0).exterior
    a = p.interpolate(0)
    b = p.interpolate(0.5, normalized=True)

    return rotate(LineString([a, b]), angle)


def dots_to_discs(n, buffer):
    if not n:
        return n
    if n.type == "Point":
        return n.buffer(buffer).exterior
    if n.type == "MultiPoint":
        return unary_union([ni.buffer(buffer).exterior for ni in n])


def safe_unary_union(gc, buffer=None):
    if isinstance(gc, Iterable):
        u = unary_union([i for i in gc if i])
        return u.buffer(buffer) if buffer else u
    return gc


def is_polygon_rectangular(g, tolerance=1e-04):
    p = Polygon(g)
    cA = p.convex_hull.area
    A = p.area
    factor = (cA - A) / cA
    return factor < tolerance


def minimum_bounding_rectangle(points):
    """
    Find the smallest bounding rectangle for a set of points.
    Returns a set of points representing the corners of the bounding box.

    :param points: an nx2 matrix of coordinates
    :rval: an nx2 matrix of coordinates
    """

    pi2 = np.pi / 2.0

    # get the convex hull for the points
    hull_points = points[ConvexHull(points).vertices]

    # calculate edge angles
    edges = np.zeros((len(hull_points) - 1, 2))
    edges = hull_points[1:] - hull_points[:-1]

    angles = np.zeros((len(edges)))
    angles = np.arctan2(edges[:, 1], edges[:, 0])

    angles = np.abs(np.mod(angles, pi2))
    angles = np.unique(angles)

    # find rotation matrices
    # XXX both work
    rotations = np.vstack(
        [np.cos(angles), np.cos(angles - pi2), np.cos(angles + pi2), np.cos(angles)]
    ).T
    #     rotations = np.vstack([
    #         np.cos(angles),
    #         -np.sin(angles),
    #         np.sin(angles),
    #         np.cos(angles)]).T
    rotations = rotations.reshape((-1, 2, 2))

    # apply rotations to the hull
    rot_points = np.dot(rotations, hull_points.T)

    # find the bounding points
    min_x = np.nanmin(rot_points[:, 0], axis=1)
    max_x = np.nanmax(rot_points[:, 0], axis=1)
    min_y = np.nanmin(rot_points[:, 1], axis=1)
    max_y = np.nanmax(rot_points[:, 1], axis=1)

    # find the box with the best area
    areas = (max_x - min_x) * (max_y - min_y)
    best_idx = np.argmin(areas)

    # return the best box
    x1 = max_x[best_idx]
    x2 = min_x[best_idx]
    y1 = max_y[best_idx]
    y2 = min_y[best_idx]
    r = rotations[best_idx]

    rval = np.zeros((4, 2))
    rval[0] = np.dot([x1, y2], r)
    rval[1] = np.dot([x2, y2], r)
    rval[2] = np.dot([x2, y1], r)
    rval[3] = np.dot([x1, y1], r)

    return rval


def determine_outline_bias(g):
    """
    For a given shape, these provide major and minor angles of the bias for the min rectangle
    """
    p = Polygon(minimum_bounding_rectangle(np.array(g.coords))).exterior
    p = p - unary_union(magic_critical_point_filter(p)).buffer(5)
    return [line_angle(i) for i in list(p)]


def geom_iter(g):
    if isinstance(g, list):
        for item in g:
            yield item
    else:
        if not isinstance(g, float) and g:
            if g and g.type == "Point":
                return [g]
            elif "Multi" in g.type:
                for item in g.geoms:
                    yield item
            else:
                for item in g.coords:
                    yield item


def geom_list(g):
    return list(geom_iter(g))


def geom_len(g):
    return len(list(geom_iter(g)))


def geom_get(g, i):
    return list(geom_iter(g))[i]


# this is used in a lot of places so I don't want to rock the boat but I think this should be -1
# for the end of the line, not 1 for the second point in the line...
def line_angle(l, last=False):
    v = list(l.coords)

    index = -1 if last else 0
    x1, y1, x2, y2 = v[0][0], v[0][1], v[index][0], v[index][1]
    return np.rad2deg(np.arctan2(y2 - y1, x2 - x1))


def edge_curvature_at_point(edges, buffer=300):
    """
    computes an angle of curvature at a point on a surface for any point
    for example supply the surface and then a point on that surface by interpolation

        edges = "some linear ring"
        f = edge_curvature_at_point(edges)
        f(edges.interpolate(5500), plot=True)
    """
    edge_segments = edges - MultiPoint(magic_critical_point_filter(edges)).buffer(5)

    def f(pt, plot=False):
        L = pt.buffer(buffer).intersection(edge_segments)
        L = L[0] if L.type != "LineString" else L

        coords = list(L.coords)

        three_points = [
            Point(coords[0]),
            L.interpolate(L.length / 2),
            Point(coords[-1]),
        ]

        if plot:
            from geopandas import GeoDataFrame
            from matplotlib import pyplot as plt

            GeoDataFrame(list(edge_segments) + three_points, columns=["geometry"]).plot(
                figsize=(10, 10)
            )
            plt.gca().invert_yaxis()

        return round(three_point_angle(*three_points))

    return f


def bend_curvature(l):
    """
    an approximation of curvature - do not want to call it curvature per se
    """
    three_points = [
        l.interpolate(0),
        l.interpolate(l.length / 2),
        l.interpolate(1, normalized=True),
    ]
    return 180 - round(three_point_angle(*three_points))  # / 180


def edge_query(edges, threshold=0.1):
    """
    query edges with z components to find the point on the edges nearest to the points and its z component
    """

    if edges.type == "MultiLineString":
        # TODO: faster merge
        e = []
        for l in geom_iter(edges):
            e += geom_list(l)
        edges = e

    zs = np.array(geom_list(edges))[:, -1]
    tr = KDTree(np.array(geom_list(edges))[:, :2])

    def f(p):
        d, id = tr.query(np.array([p]))
        if threshold is None or d < threshold:
            return zs[id][0][0]
        return None

    return f


def point_clockwise_order(outline):
    """
    given a boundary outline, return a function that provides the clockwise angle of the point
    this is useful for sorting existing points srt the outline
    """
    center = np.array(geom_list(outline)).mean(axis=0)[:2]

    def angle(p):
        p = np.array(p)[:2]
        theta = np.arctan2((p - center)[1], (p - center)[0])
        if theta < 0:
            theta += 2 * np.pi
        return theta

    return angle


def to3d(g):
    """add a 3rd component so we can move"""
    try:
        return transform(lambda x, y: (x, y, 0), g)
    except:
        return transform(lambda x, y, z: (x, y, 0), g)


def to2d(g):
    if pd.isnull(g):
        return None
    if g.type.startswith("Multi") or g.type == "GeometryCollection":
        return unary_union([to2d(gi) for gi in g])

    """downsample"""
    try:
        return transform(lambda x, y, z: (x, y), g)
    except:
        return transform(lambda x, y: (x, y), g)


def to2dint(g):
    """downsample"""
    try:
        return transform(lambda x, y, z: (int(x), int(y)), g)
    except:
        return transform(lambda x, y: (int(x), int(y)), g)


def dotted_lines(ring, sep=20, space=5):
    points = []
    for a in ring:
        for b in np.arange(sep, a.length, sep):
            b = a.interpolate(b)
            points.append(b.buffer(space))
    result = cascaded_union(ring) - cascaded_union(points)
    if not isinstance(result, list):
        if result.type == "LineString":
            result = [result]
    return result


def zshift_geom(g, z_shift):
    """Move the edge in the z plane"""
    g = to3d(g)
    if z_shift:
        g = translate(g, zoff=z_shift)
    return g


def tr_edge(e, z_shift):
    """Move the edge in the z plane"""
    e = to3d(e)
    if z_shift:
        e = translate(e, zoff=z_shift)
    return e


def sort_points_wrt_outline(l, shape, sense=-1):
    # center = np.array(ref_center_point)[:2]
    # def angle(p):
    #     p = np.array(p)[:2]
    #     theta = np.arctan2((p - center)[1], (p - center)[0])
    #     if theta < 0:
    #         theta += 2 * np.pi
    #     return theta

    cs = pd.DataFrame(l.coords, columns=["x", "y", "z"])

    key = pd.DataFrame(
        shape.coords,
        columns=[
            "x",
            "y",
        ],
    ).reset_index()

    cs = pd.merge(cs, key, on=["x", "y"], suffixes=["", "k"])

    cs = cs.sort_values("index")

    return LineString(cs[["x", "y", "z"]].values)


def interpolate_away_from(line, c, d):
    """
    given a reference geometry, we can interpolrate along lines away from it
    this works by reversing the points in the line if the first point is not the closest one to the geometry
    """

    assert (
        line.type == "LineString"
    ), f"Can only interpolate away from a reference point along lines but the supplied geom is {line.type}"

    points = list(line.coords)

    if Point(points[0]).distance(c) > Point(points[-1]).distance(c):
        points = list(reversed(points))

    return LineString(points).interpolate(d)


def outside_offset(g, shape, r=35, sense=-1):
    """
    see inside offset but uses "NOT" contains as a ref
    """

    def _com(l):
        # center of mass of a line / points on average
        return Point(np.array(l.coords).mean(axis=0))

    def _midpoint(l):
        return l.interpolate(0.5, normalized=True)

    def offset_line(l):
        """
        offset a line strictly inside and preserve the z component
        """
        try:
            if int(l.length) == 0 or l is None:
                return None

            p1 = list(l.coords)[0]
            z = None if len(p1) < 3 else p1[-1]
            off = l.parallel_offset(r, "left")

            # NB::: This could be a bug if contains is to strict - we should check some of the lin nearits center maybe
            if not Polygon(shape).contains(_midpoint(off)):
                l = off
            else:
                l = LineString(reversed(list(l.parallel_offset(r, "right").coords)))
            if z is not None:  # keep the z - is there another way to do this
                l = tr_edge(l, z)

            # what we really want here is clockwise orientation- text the right way up etc.
            return l
        except Exception as ex:
            import traceback

            res.utils.logger.warn(f"offsetting failed - {traceback.format_exc()}")
            return None

    if g.type != "MultiLineString":
        return offset_line(g)
    else:
        E = [offset_line(l) for l in g if int(l.length) > 0]
        E = [e for e in E if e is not None]
        return unary_union(E)


def ensure_multi_point(g):
    if g:
        if g.type == "Point":
            return MultiPoint([g])
        return g


def coerce_linear_ring_fill_small_gaps(g, gap_threshold=1):
    """
    going around segments trying to connect little gaps and then polygonize and get the exterior
    g is probably a multiline string
    """

    def fill_ring(S):
        for s in g:
            t = g - s
            p1 = s.interpolate(0)
            p2 = s.interpolate(1, normalized=True)
            d = p1.distance(t)
            if d > 0 and d < gap_threshold:
                l = LineString([p1, nearest_points(p1, t)[-1]])
                yield l
            yield t
            d = p2.distance(t)
            if d > 0 and d < gap_threshold:
                l = LineString([p2, nearest_points(p2, t)[-1]])
                yield l

    ll = unary_union(list(fill_ring(g)))
    return list(polygonize(ll))[0].exterior


def chordify_line(g):
    return LineString([g.interpolate(0), g.interpolate(g.length)])


def inside_offset(g, shape, r=35, sense=-1):
    """
    orient lines by sense too

    test with : expect the red lines inside and all end points (blobs) at clockwise end
    (noticed a small bug for some lines that we can fix later

        g = geometry_to_image_space(row,'outline')
        vs = geometry_to_image_space(row,'viable_surface')
        ax = GeoDataFrame(vs,columns=['geometry']).plot(figsize=(40,40))
        center = np.array(g).mean(axis=0)
        vs_in = inside_offset(vs, center)
        ax = GeoDataFrame(vs_in,columns=['geometry']).plot(ax=ax,color='red')
        ax = GeoDataFrame([Point(list(l.coords)[-1]).buffer(50) for l in vs_in],columns=['geometry']).plot(ax=ax,color='red')
    """

    def _com(l):
        # center of mass of a line / points on average
        return Point(np.array(l.coords).mean(axis=0))

    def _midpoint(l):
        return l.interpolate(0.5, normalized=True)

    def offset_line(l):
        """
        offset a line strictly inside and preserve the z component
        """
        try:
            if int(l.length) == 0 or l is None:
                return None

            p1 = list(l.coords)[0]
            z = None if len(p1) < 3 else p1[-1]
            off = l.parallel_offset(r, "left")

            # NB::: This could be a bug if contains is to strict - we should check some of the lin nearits center maybe
            if Polygon(shape).contains(_midpoint(off)):
                l = off
            else:
                l = LineString(reversed(list(l.parallel_offset(r, "right").coords)))
            if z is not None:  # keep the z - is there another way to do this
                l = tr_edge(l, z)

            # what we really want here is clockwise orientation- text the right way up etc.
            return l
        except Exception as ex:
            import traceback

            res.utils.logger.warn(f"offsetting failed - {traceback.format_exc()}")
            return None

    if g.type != "MultiLineString":
        return offset_line(g)
    else:
        E = [offset_line(l) for l in g.geoms if int(l.length) > 0]
        E = [e for e in E if e is not None]
        return unary_union(E)


def dist_from(v):
    """
    helper - refactor for raw numpy - pandas easier fornow
    """

    def f(row):
        u = [row["x"], row["y"]]
        return euclidean(u, v)

    return f


# def _sort_geometry_points(pts, offset=0, sense=-1):
#     """ """
#     center = pts.mean(axis=0)
#     angles = np.arctan2((pts - center)[:, 1], (pts - center)[:, 0])
#     angles[angles < 0] = angles[angles < 0] + 2 * np.pi

#     # wrap around the offset
#     angles = (np.rad2deg(angles) + offset) % 360

#     return pts[np.argsort(angles)]


def remove_non_corners_on_small_chords(c, chord_length=300):
    """
    WIP - it seems a test for irrelevant corners on small chords could check the polygon from the simplification of "removing" such chords
    The polygon that remains would contain any interior points that on shapes such as TH-3002-V3-BLSSLPNLLF-S_XS might not be considered as corners
    """

    def choose_sacrificial_point(a, b, c):
        if LinearRing(c).simplify(chord_length).contains(b):
            res.utils.logger.debug(
                f"removing {b} in remove_non_corners_on_small_chords"
            )
            return b
        elif LinearRing(c).simplify(chord_length).contains(a):
            res.utils.logger.debug(
                f"removing {a} in remove_non_corners_on_small_chords"
            )
            return a

        res.utils.logger.warn(
            f"We did not find a point that can be removed from the small segment"
        )

    # BallTree
    v = np.array([[p.x, p.y] for p in c])
    results = BallTree(v).query_radius(v, r=300)
    pairs = set([tuple(r) for r in results if len(r) > 1])

    rem_p = []

    for p in pairs:
        a, b = p[0], p[1]
        a, b = Point(*list(v[a])), Point(*list(v[b]))
        rem_p.append(choose_sacrificial_point(a, b, c))

    return c - MultiPoint(rem_p)


def get_mesh(g, C):
    """
    we need to simplify the corners we see - its something like corners merged with midpoints between corners
    this is the final true test where we can check if the small angle on the corner is within threshold
    we can use the same 141 degree threshold as we use for corner selection normally
    """
    data = pd.DataFrame(C, columns=["geometry"])
    data["proj"] = data["geometry"].map(lambda pt: g.project(pt))

    def mp(row):
        return g.interpolate((row["proj"] + row["proj_shift"]) / 2)

    data["proj_shift"] = data["proj"].shift(-1).fillna(g.length)
    data["mid"] = data.apply(mp, axis=1)

    data["geometry_next"] = (
        data["geometry"]
        .shift(-1)
        .fillna(data["geometry"].map(lambda x: data.iloc[0]["geometry"]))
    )
    data["geometry_prev"] = (
        data["geometry"]
        .shift(1)
        .fillna(data["geometry"].map(lambda x: data.iloc[-1]["geometry"]))
    )
    data["prev_mid"] = (
        data["mid"].shift(1).fillna(data["mid"].map(lambda x: data.iloc[-1]["mid"]))
    )

    def small_adj_angle(row):
        a1 = three_point_angle(row["geometry_prev"], row["prev_mid"], row["geometry"])
        a2 = three_point_angle(row["geometry"], row["mid"], row["geometry_next"])
        return min(a1, a2)

    data["angle"] = data.apply(small_adj_angle, axis=1)

    return data


def filter_sensitive_corners(
    c,
    g,
    used_edges=None,
    trange=[0, 0.015],
    t_edge_length=0.5 * 300,
    small_segment=None,
    plot=False,
    min_corners=5,
    # if the angle is really acute then dont remove
    acute_angle_threshold=141,
    angles=[],
):
    """
    by using a parametric area reduction method check if we like the point
    interestingly some grow the shape and we can keep them but others shrink by some amount if they can possibly be removed

    t_edge_length is absolute in pixel reference system for now - could be some ratio of the size but its tricky

    if there are fewer corners than min corners we do not do this check
    """

    ref = Polygon(c)
    # must pass in the original list because unary union can do bad things
    points = geom_list(c)

    # we only filter if there are a large enough number of corners
    if len(points) < min_corners:
        return c

    def is_in_smoothable_triplet(idx):
        """
        using a sliding window take three points
        if the first and last point are very close, remove all three from candidates - this is for when we insert a cut into a piece that is not a true edge
        CASE: TK-3087-V2-TOPFTPNL-S_2X
        we could also check that no two points are close by but we should leave this for now

        the triplets are any pairs that are too close that also contain another point.
        here we just look at pairwise neighbours but we need to test boundary conditions on wrapped lists

        """
        ar = np.array(MultiPoint(c))
        # use the quarter edge segment length as a very small number
        ids = KDTree(ar).query_radius(ar, r=t_edge_length / 4)

        def is_between(k, pair):
            """
            wrapped adjacency check
            this is s a bit weird. for cycles, we just want to see if a point is a triple
            [A] [B] [C] with wrapping lists
            the idea is that for very close by points in pairs, A,C we can check if there is another point between them
            if [A] [B] are close we can also match on that
            in conclusion, any sets of 2 or thee points that are too close to each other in the circuit are removed
            this is to deal with cases where we have weird cutlines added to shapes
            """
            # reverse the pair because the assumption is we are separated by 1 only
            # for wrapped lists we need wrap in this direction ->
            if abs(pair[1] - pair[0]) > 2:
                pair = (pair[1], pair[0])
            l = len(ids)
            if k == pair[0] or k == pair[1]:
                return True
            if (k + 1) % l == pair[-1] or (k + l - 1) % l == pair[0]:
                return True
            return False

        groups = []
        for result in ids:
            t = tuple(result)
            if len(t) == 2 and t not in groups:
                # #print(
                #     idx, t[0], t[1], len(ids)
                # )  # - > until boundary condition case, check that there is only a difference of one in case we clear a bunch of things
                if is_between(idx, t):
                    # print("match")
                    return True
                groups.append(t)
        return False

    def is_pair_exterior(idx):
        """
        determine nearest ngh in a radius
        if we are below some distance threshold e.g. small edge, we cannot remove both of these
        so we check which one in such a pair is furthest from center of mass and we lock it
        Example body piece where this is needed JR-3114-V2-SHTBKPNL-S_M

        """
        pts = MultiPoint(c)
        center = pts.centroid
        ar = np.array(pts)
        d, ids = KDTree(ar).query(ar, k=2)

        # distances for the second ngh that is not self
        # if there is a neighbour to close then we are a pair
        if d[:, 1][idx] < t_edge_length:
            # this returns a pair of point indexes for the current pioint index e.g. [3,4]
            # these points[3] and points[4] can be compared. "I" am always the first e.g. ids[3] will be [3,4] and not [4,3]
            # and this is because KNN will return ordered distances. so i am 0 distance from myself and some distance from the next guy
            pids = ids[idx]
            # if i am the furthest from the centroid in the pair return true so as to keep this point
            return points[pids[0]].distance(center) > points[pids[1]].distance(center)
        return False

    rem = points

    diffs = []
    chash = []

    if g:
        mesh = get_mesh(g, c)
        angles = list(mesh["angle"])
        # print("mesh angles", angles)

    for idx in range(len(c)):
        # we add the line mid points for a better approx of the shape
        P = Polygon([p for i, p in enumerate(c) if i != idx])

        diff = (ref - P).area
        scaled_diff = diff / ref.area

        # print(idx, scaled_diff)

        diffs.append(diff)

        protected_corner = angles[idx] < acute_angle_threshold

        # print("protected corner", protected_corner, angles[idx])

        if not protected_corner:
            # skip points that are weirdly close as these cut be inserted cuts and not really corners :(
            if is_in_smoothable_triplet(idx):
                # print("skipping smoothed", idx)
                continue

            # if its not a paired exterior its a candidate to remove if small area difference

            if scaled_diff >= trange[0] and trange[1] >= scaled_diff:
                if not is_pair_exterior(idx):
                    # print("removing corner for area diff", idx, scaled_diff)
                    continue
        else:
            pass
            # print("is a protected corner", idx)

        # we are add
        chash.append(points[idx])

    # now if there are still some and small segment
    # test the points at ends of small segments -> if we can remove a point and create a polygon, reject the one as corner that remains inside the closure

    if plot:
        from geopandas import GeoDataFrame

        df = GeoDataFrame(points, columns=["geometry"])
        df["diffs"] = diffs

        ax = df["diffs"].plot(figsize=(20, 10))

        ax.axhspan(trange[0], trange[1], facecolor="b", alpha=0.5)
        return df

    return MultiPoint(chash)


def magic_critical_point_filter(
    g,
    critical_angle_threshold=141,
    # inches are default thresholds
    hull_distance_threshold=5 * 300,
    # this parameter could be determine from the seam allowance on the edge
    min_segment_inches=0.5,
    # min_segment_inches=0,
    known_keypoints=None,
    known_keypoint_dist=None,
    simplify=0,
    angles=[],
    **kwargs,
):
    """
    Somehow encode all logic for getting critical points
    these are corners but we remove corners following small segments or well sindie the entire (far from hull boundary)
    This is parametric so needs calibration and may never really work without users providing keypoints - lets see what we learn
    """
    # We always orient polygons this way - clockwise - for now orient on the way in... or just check here
    ch = g.convex_hull.boundary

    if simplify:
        g = g.simplify(3)

    # critical_angle_threshold = 141  # <- fixing this for singleton values

    corners = [
        Point(a[0])
        for a in polygon_vertex_angles(
            Polygon(g.simplify(1)), less_than=critical_angle_threshold, angles=angles
        )
    ]

    # playing: "simple" shapes may be fine
    if len(corners) < 7:
        return corners

    d = []
    for i, c in enumerate(corners):
        if i > 0:
            d.append(c.distance(corners[i - 1]))
        else:
            d.append(c.distance(corners[-1]))

    def near_known_keypoints(g):
        if known_keypoints and known_keypoint_dist:
            # return g.distance(known_keypoints)
            if g.distance(known_keypoints) < known_keypoint_dist:
                print("finding a point that is too near a keypoint like a notch")
                return True

        return False

    df = pd.DataFrame([p for p in corners], columns=["geometry"])
    df["d"] = d
    df["cd"] = df["geometry"].apply(lambda g: g.distance(ch))
    df["near_kps"] = df["geometry"].map(near_known_keypoints)

    return list(df["geometry"].values)


def edge_corners_controlled(edges, notches):
    """
    a parametric stricter corner tool
    maybe can reduce to one but not sure about small segments - this does two passes to find something like corners and
    then zooms out and checks the line angles
    maybe can be done in one pass
    """

    def angle_at_corner(edges, buffer=300):
        def f(c):
            # get segment approx nearby
            k = c.buffer(buffer).intersection(edges)
            a = three_point_angle(
                list(np.array(k.interpolate(0))),
                list(np.array(k.interpolate(k.length / 2))),
                list(np.array(k.interpolate(k.length))),
            )

            return a

        return f

    angle_at_corner_fn = angle_at_corner(edges)
    crns = magic_critical_point_filter(edges, critical_angle_threshold=150)

    # we can actually exclude points near notches as candidate corners
    # this is not something we are going to do any more because notches can be in the wrong place
    # crns = [c for c in crns if c.distance(notches) > 10]
    # crns = [c for c in crns if 45 <= angle_at_corner_fn(c) <= 160 ]

    return crns


def polygon_keypoints(g, angle_treshold=150):
    # supports only polygons for now
    return MultiPoint(
        [list(p[0]) for p in polygon_vertex_angles(g, less_than=angle_treshold)]
    )


def sort_edges(edges):
    """
    not the most efficient but needed quickly
    some simple sort logic for edges - the one starting at the bottom in image space
    """
    df = pd.DataFrame(edges, columns=["geometry"])
    df["com"] = df["geometry"].map(
        lambda x: np.array(x.coords)[:, :2].mean(axis=0).astype(int)
    )
    df["x"] = df["com"].map(lambda x: x[0])
    df["y"] = df["com"].map(lambda x: x[1])
    df = df.sort_values(["y", "x"])
    df["eid"] = df["geometry"].map(lambda x: list(x.coords)[0][-1])
    first = df.iloc[-1]["eid"]
    max_edge = df["eid"].max() + 1
    df["eid_mod"] = df["eid"].map(lambda e: (e - first) % max_edge)

    edges_sorted = []
    for _, r in df.iterrows():
        edges_sorted.append(tr_edge(r["geometry"], r["eid_mod"]))
    return edges_sorted


def points_from_bounds(pts, side=None):
    """
    helper to add point form from bounds e.g. corners and retangle center points from bounds
    """
    data = {
        "tr": Point(pts[0], pts[1]),
        "br": Point(pts[0], pts[3]),
        "tl": Point(pts[2], pts[3]),
        "bl": Point(pts[2], pts[1]),
        "mb": Point(0.5 * (pts[2] + pts[0]), pts[3]),
        "mt": Point(0.5 * (pts[2] + pts[0]), pts[1]),
    }

    if side == "right":
        return {k: v for k, v in data.items() if k in ["mb", "mt", "tr", "br"]}
    if side == "left":
        return {k: v for k, v in data.items() if k in ["mb", "mt", "tl", "bl"]}
    return data


def sort_as_line(multiline, edge_order_fn):
    """
    Sometimes we have distjoint lines and we want to interpolate (for example) on a single object
    we can try this function. example case
    M = MultiLineString([ [[34.2344974197167, 1720.006539291799, 1], [0, 862.4977854417903, 1]], [ [0, 862.4977854417903, 1], [34.23449714743319, 4.990212676116509, 1]] ])
    """
    pts = []
    for l in multiline:
        for p in list(l.coords):
            pts.append(p)

    pts = [Point(p) for p in set(pts)]
    pts = sorted(pts, key=lambda x: edge_order_fn(x))
    return LineString(pts)


def sorted_corners_conventional(o, corners=None, plot=False, factor=None):
    """
    a particular logic to choose a conventional first corner
    we want the corner that (going clockwise) starts on the edge that first ends on or crosses the center point
    """

    def re_order_from_index(l, i):
        return l[i:] + l[0:i]

    # some small offset away from corner
    factor = factor or o.bounds[2] * 0.01

    corners = geom_list(corners or magic_critical_point_filter(o))
    el = o - unary_union(corners).buffer(5)
    pa = Point(o.bounds[2] / 2 - factor, o.bounds[3])
    pb = Point(o.bounds[2] / 2 - factor, o.bounds[3] / 2)
    ll = LineString([pa, pb])

    candidate = []
    for e in el:
        if e.intersects(ll):
            candidate.append([e, e.distance(pa)])

    candidate = sorted(candidate, key=lambda x: x[1])
    assert (
        len(candidate) > 0
    ), "did not match any edge that crosses near the center line which is weird"

    candidate = candidate[0][0]
    pa, pb = candidate.interpolate(0), candidate.interpolate(1, normalized=True)

    # get the start of the vector, the one furthest from the sample point
    npt = pa if pa.distance(pa) > pb.distance(pa) else pb

    distance_min = o.bounds[-1] * o.bounds[-2]
    idx = -1
    for i, c in enumerate(geom_list(corners)):
        d = c.distance(npt)
        if d < distance_min:
            distance_min = d
            idx = i

    corner = corners[idx]

    if plot:
        from geopandas import GeoDataFrame

        GeoDataFrame([candidate.buffer(10), o, corner], columns=["geometry"]).plot()

    if idx > 0:
        return re_order_from_index(corners, idx)

    return corners

    raise Exception("Failed to sort corners")


def label_edge_segments(
    g,
    fn,
    corners=None,
    plot=False,
    debug=False,
    buffer=10,
    should_filter_critical_corners=False,
    **kwargs,
):
    # fn = dxf_notches.projection_offset_op(p._row)
    # corners from some sort of critical filter and can be sorted
    if corners is None:
        corners = magic_critical_point_filter(g)
        if should_filter_critical_corners:
            corners = filter_sensitive_corners(MultiPoint(corners))

    corners = sorted(list(geom_iter(corners)), key=lambda g: fn(g))

    if len(corners) == 0:
        res.utils.logger.warn(f"No corners, adding dummy at start")
        corners = [g.interpolate(0), g.interpolate(0.5, normalized=True)]

    # corners = sorted_corners_conventional(g, corners=corners)

    # buffer could be a function of the shape
    parts = g - unary_union(corners).buffer(buffer)

    segments = []
    seg_offsets = []
    for i in range(len(corners)):
        t = (corners[i], corners[(i + 1) % len(corners)])
        segments.append(t)
        seg_offsets.append((fn(t[0]), fn(t[1])))

    data = []
    edges = []
    assignments = []
    for l in parts.geoms:
        assert (
            l.type == "LineString"
        ), "The outline parts did not form linestrings maybe because when subtracting buffered corners we did not get lines"
        centroid = fn(l.interpolate(l.length / 2))

        # adding .geoms for deprecation warning
        for i, pair in enumerate(segments):
            if debug:
                print(centroid, fn(pair[0]), fn(pair[1]))
            if fn(pair[0]) <= centroid <= fn(pair[1]):
                data.append(
                    {
                        "geometry": l,
                        "pt1": pair[0],
                        "pt2": pair[1],
                        "idx": i,
                        "center": l.interpolate(l.length / 2),
                        "range": (fn(pair[0]), fn(pair[1])),
                        "distance1": l.distance(pair[0]),
                        "distance2": l.distance(pair[1]),
                    }
                )
                l = tr_edge(l, i)
                assignments.append(i)

                break

        # boundary case
        idx = len(segments) - 1
        if centroid > fn(pair[1]):
            data.append(
                {
                    "geometry": l,
                    "pt1": pair[0],
                    "pt2": pair[1],
                    "center": l.interpolate(l.length / 2),
                    "idx": idx,
                    "is_edge_case": True,
                }
            )
            l = tr_edge(l, idx)
            assignments.append(i)

        if not l.has_z:
            res.utils.logger.debug(
                f"Encountered a line that is not 3D in edge naming - this means we could not find boundaries to name it: we assigned segments {assignments} from {idx} - centroid index {centroid}"
            )
            l = tr_edge(l, idx)

        edges.append(l)

    # we have ordered the edges and placed an id from some corners, if we know the correct corners  great, buts its easier to sort by edges after
    # so we tag intervals and then "rotate" based on edge sorting
    edges = sort_edges(edges)

    if plot:
        from geopandas import GeoDataFrame
        from matplotlib import pyplot as plt

        df = GeoDataFrame(data)
        colors = ["k", "b", "y", "g", "r", "orange", "gray", "yellow"]
        ax = df.plot(color=df["idx"].map(lambda i: colors[int(i)]), figsize=(20, 10))
        GeoDataFrame(list(corners), columns=["geometry"]).plot(ax=ax)
        GeoDataFrame(list(df["center"]), columns=["geometry"]).plot(ax=ax)
        plt.gca().invert_yaxis()

        for record in df.to_dict("records"):
            t = record["pt1"].x, record["pt1"].y
            ax.annotate(
                str(record["idx"]),
                xy=t,
                arrowprops=dict(facecolor="black", shrink=0.005),
            )
        for record in df.to_dict("records"):
            t = record["center"].x, record["center"].y
            ax.annotate(
                str(record["idx"]),
                xy=t,
                arrowprops=dict(facecolor="red", shrink=0.005),
            )

    if debug:
        return edges, data, corners

    return unary_union(edges)


def old_label_edge_segments(g: LinearRing, **kwargs):
    """
    find the key points, and walk clockwise from one of them and increment a counter
    add the index as the z coordinate of the outline geometry - this should come true edges
    can we add or maintain these on the viable surface so when we have segments they are indexed in some order - and we can still re-index once they are ordered and partioned
    if the keypoints that delimit segments is known use that other wise try to detect from critical angles

    We sort the clockwise (-1 or TODO 1 counter) by going around the orientated polygon
    We can leave the default sort or we can shift by some angle and taking points at angles from the center of mass

    #TODO: unit tests on sense and applied edge id conventions

    #these can be mapped to a dict using (dataframe of the 3 points, group by z and return line strings)

        arr = pd.DataFrame( np.array(row['edges'].coords), columns=['x','y','z'])
        { int(key) :  LineString(value.values) for key, value in arr.groupby('z')}


    A validation (ensure all edges point on clockwise end and tat par offsite right is inside the shape)

        edges= swap_points(label_edge_segments(row['outline']))
        df = GeoDataFrame(edges, columns =['geometry'])
        df['last_point'] = df['geometry'].map(lambda l : Point(list(l.coords)[-1]).buffer(.3))
        ax = df.plot(cmap='Spectral',figsize=(20,20))
        ax = GeoDataFrame(df['last_point'].values,columns= ['geometry']).plot(ax=ax,cmap='Spectral')
        # g = df.iloc[2]['geometry'].parallel_offset(0.5,'right')
        GeoDataFrame([g.parallel_offset(.5, 'right') for g in edges],columns=['geometry']).plot(ax=ax)
    """

    g = orient(g, sign=-1)

    corners = magic_critical_point_filter(g)
    corners = [(p.x, p.y) for p in corners]

    def is_corner(row):
        for c in corners:
            if row["y"] == c[1] and row["x"] == c[0]:
                return True
        return False

    data = pd.DataFrame([p for p in g.coords], columns=["x", "y"]).reset_index()
    com = Point(data[["x", "y"]].mean(axis=0).values)
    data["is_first"] = data["index"].map(lambda x: True if x == 0 else False)
    data["is_second"] = data["index"].map(lambda x: True if x == 1 else False)
    data["is_corner"] = data.apply(is_corner, axis=1)
    data["corner_expansion"] = data["is_corner"].map(lambda x: 0 if not x else [0, 1])
    data = data.explode("corner_expansion")
    data["idx"] = data["corner_expansion"].cumsum()  # % len(corners)
    lines = []

    # this is not perfect - as we have an extra line to avoid the wrapping problem
    # but it doesnt matter because the region is numbered with the same seam id which is all we care about
    for _, v in data.groupby("idx"):
        v["idx"] = v["idx"].map(lambda i: i % len(corners))
        if len(v) > 1:
            l = LineString(v[["x", "y", "idx"]].values)
            lines.append(l)

    return unary_union(lines)


def geo_line_segments(g):
    pts = list(g)
    # create a generic geo point iterator that is type agnostic
    for i, p in enumerate(pts):
        yield LineString([pts[i - 1], p])


def compute_scale_factor(a, b):
    ab = np.array(a.bounds)
    bb = np.array(b.bounds)
    # al = len(list(a))
    # bl = len(list(b))
    a_w, a_h = np.abs(ab[2] - ab[0]), np.abs(ab[3] - ab[1])
    b_w, b_h = np.abs(bb[2] - bb[0]), np.abs(bb[3] - bb[1])
    w_factor = (a_w or 1) / (b_w or 1)
    h_factor = (a_h or 1) / (b_h or 1)

    return w_factor, h_factor


def scale_proportionally(a, b):
    w_factor, h_factor = compute_scale_factor(a, b)
    return affinity.scale(b, xfact=w_factor, yfact=h_factor)


def find_congruence(a, b, returns="geometry", r=0.8, plot=False):
    """
    basic test on multipoints and returns a congruency subset of points
    the A->B set are returned e,g, a sample sized transformed onto another size
    We return (A,B) in that order so a transformation Gamma(A,B) can then be found
    """
    cols = ["0", "1"]
    bdash = scale_proportionally(a, b)
    B = pd.DataFrame(np.array(b), columns=cols).join(
        pd.DataFrame(np.array(bdash), columns=["scaled_0", "scaled_1"])
    )
    A = pd.DataFrame(np.array(a), columns=["sample_0", "sample_1"])
    A["match"] = [
        p[0] if len(p) else None
        for p in BallTree(B[["scaled_0", "scaled_1"]].values).query_radius(
            np.array(a), r=r
        )
    ]
    A = A[A["match"].notnull()]
    A["match"] = A["match"].astype(int)
    A = A.set_index("match").join(B, lsuffix="_sample")
    a, b = MultiPoint(A[["sample_0", "sample_1"]].values), MultiPoint(A[cols].values)

    if plot:
        from geopandas import GeoDataFrame

        GeoDataFrame([{"geometry": a}, {"geometry": b}]).plot(
            figsize=(20, 20), cmap="Spectral"
        )

    return a, b


def get_geo_add_y_fn(y_offset):
    """
    closeure op- for dataframes get the thing that adds height to each geom
    """

    def f(g):
        try:
            return translate(g, yoff=y_offset)
        except:
            return g

    return f


def get_shape_bound_fn(idx):
    """
    Optionally bounds can be passed in if the points in g do not represent the universe of points i.e. true bound
    shapely bounds are (minx, miny, maxx, maxy)
    """

    def f(g):
        return g.bounds[idx]

    return f


def geo_max_y(g):
    # return the top y bound of g
    pass


def name_part(
    g, named_parts, idx=None, invert_named_pieces=False, min_dist=2000, **kwargs
):
    """
    Is interface function== true (when we do things that mix DXF, image space etc we should focus on it)
    Part naming by passing in named outlines in the DXF format (300dpi) which should be thought of as a typical polygon layout
    Note on CRS:
        We normally have to be careful with CRS: xy->yx, invert y-axis for numpy comparison, and shift to the origin to ignore absolute locations
        The y- inversion can be confusing but it is not always obvious from any picture which orientation we are in
    CRS aside, we are simply comparing all named shapes (popped on match) such that there is a rough area intersection and then getting the closest by hausdorff
    -
    """

    min_dist_init = min_dist
    debug = kwargs.get("debug")

    def approx(a, b):
        try:
            # the pieces at the origin should essentially overlap
            intersection_ratio = a.intersection(b).area / a.area
            return intersection_ratio > 0.85
        except Exception as ex:
            res.utils.logger.warn(
                f"Failing to get an approx intersection due to bad shapes probably {ex}"
            )
            return False

    if named_parts:
        # set tolerance for each piece at start
        min_dist = min_dist_init
        min_key = None
        dist = None
        # for all the parts, using an approx to skip, check the hausdorff
        keys = named_parts.keys()
        for k in keys:
            # do a comparison of shapes only - ignore any offsets
            # by applying Polygon we can deal with LinearRings, numpy arrays etc and assume a 2d object - outlines are assumed though
            p = shift_geometry_to_origin(Polygon(named_parts[k]))
            # the outlines we have are currently in the DXF orientation so image space inverts the y
            if invert_named_pieces:
                p = invert_axis(p)

            # for fuzzier matches we apply a buffer to the ASTM shape to match a physical shape
            # in general a function(g) can be added rather than just a shapely buffer
            # if piece_material_buffers and k in piece_material_buffers:
            #     p = p.buffer(piece_material_buffers[k])

            g = shift_geometry_to_origin(Polygon(g))
            if approx(g, p):
                dist = g.hausdorff_distance(p)
                #'small' disambiguation factor
                # i add this because for symmetrical pieces we can not tell the difference even though the notch distribution differs
                # so if we add notched outlines we can see there is a slight different in polygon area overlaps ..
                factor = 1 - g.intersection(p).area / g.area
                dist += factor

                if dist < min_dist:
                    min_key = k
                    min_dist = dist
                    if debug:
                        res.utils.logger.debug(f"found dist {k}: {min_dist}")

        # based on some check and if not with replacement
        if min_key:
            named_parts.pop(min_key)
            return min_key, min_dist
        else:
            res.utils.logger.debug(
                f"could not determine a match - the smallest distance was {dist}"
            )

    return uuid.uuid1() if idx is None else idx, -1


def _ensure_polygon(g_or_ndarray):
    """
    extend for many types but we can  handle geometry or ndarray in the same way
    """
    if isinstance(g_or_ndarray, geometry):
        return True, g_or_ndarray
    else:
        return False, Polygon(g_or_ndarray)


def _ensure_polygon_return_type(shapely_in, g):
    return g if shapely_in else g.exterior.coords


def ndarray_area(a):
    if a is not None:
        return geometry.Polygon(a).area
    return 0


def detect_outline_castle_notches(
    g, simplification=1, min_area_filter=600, return_surface_notch_point=True
):
    """
    DEPRECATE in favour of the the other one and then make sure all scales work e.g. inches, mms, px

    By buffer expanding and collapsing we remove notches and detect notches from the difference if the original and new polygons
    there may be noise and we are looking for rectangular shapes of a certain area in the result

    #TODO choose which units the parameters are in and scale internally
    """
    # we need to simplify to remove small artifacts on the surface
    sg = Polygon(g).simplify(simplification)
    ssg = remove_notch_from_outline(sg)
    # take the differ - default notch buffer size is 50
    ns = Polygon(ssg) - sg

    ns = pd.DataFrame(ns, columns=["geometry"])
    ns["area"] = ns["geometry"].map(lambda g: g.area)
    # simple test i can check the form as rectangular too
    ns = ns[ns["area"] > min_area_filter]
    ns["surface_point"] = ns["geometry"].map(
        lambda n: nearest_points(ssg, n.centroid)[0]
    )

    # create a single multi polygon object from the dataframe that we filtered
    return (
        unary_union(ns["geometry"].values)
        if not return_surface_notch_point
        else unary_union(ns["surface_point"].values)
    )


def try_ensure_valid(ol):
    """
    when things are not valid it gets a bit weird
    it seems we get some spurious polygons that we should remove and take the one that is most likely the composite
    """
    if ol.is_valid:
        return ol

    P = make_valid(Polygon(ol))
    if P.type != "Polygon":
        areas = [abs(P.area - p.area) for p in P]
        P = P[np.argmin(areas)]
    if P.type != "Polygon":
        return P.boundary
    return P.exterior


def detect_castle_notches_from_outline(
    g,
    simp=0.05,
    buffer=0.2,
    plot=False,
    threshold=[0.005, 0.03],
    units="inches",
    add_z_component=None,
):
    """
    tested for NET files in inches -> add z comp for

    hdf = GeoDataFrame(list(holes),columns=['geometry'])
    hdf['area'] = hdf['geometry'].map(lambda g :g .area)
    hdf['area'].hist()

    """

    # todo if scale not in inches then do something but this is tested for inches scale net file

    sa = Polygon(g).simplify(simp)
    saa = Polygon(sa.buffer(buffer).buffer(-1 * buffer).exterior)
    holes = saa - sa

    if holes.is_empty:
        return None

    cns = [c for c in holes if c.area < threshold[-1] and c.area > threshold[0]]
    # project sites onto outline
    projected = lambda n: g.interpolate(g.project(n))
    cns = [projected(c.centroid) for c in cns]

    # if add z -> add the special indicator e.g 0.18

    if plot:
        from geopandas import GeoDataFrame

        cns = [p.buffer(0.3).exterior for p in cns]
        GeoDataFrame(list(cns) + [g], columns=["geometry"]).plot(figsize=(20, 20))

    return unary_union(cns)


def detect_v_notches_from_outline(
    g,
    display=False,
    sample_increments=0.1,
    r1=0.25,
    r2=0.95,
    thresholds=[0.12, 0.14],
    outline_smoothing=0.5,
    units="inches",
    add_z_component=None,
):
    """

    tested for NET files in inches -> add z comp for

    we can use the outline with some size parameters - here theyare tested for inches scale
    -  can display the sampling data with display=True

    Example call:

        df = detect_v_notches_from_outline(net_outline)
        #get the ones that are given a  true value from the sampled subset of corner objects
        #display it over A
        vs = list(df[df['is_v_notch']]['pt'])
        vs = [p.buffer(.7).exterior for p in unary_union(vs)]
        GeoDataFrame(list(vs)+[A],columns=['geometry']).plot(figsize=(20,20))

    """
    data = []

    # smoothing operation
    P = Polygon(g).buffer(outline_smoothing).buffer(-1 * outline_smoothing)

    outline = P.exterior
    for i in np.arange(0, outline.length, sample_increments):
        p = outline.interpolate(i)

        pt = p.buffer(r1)
        pt_large = p.buffer(r2)

        area_1 = pt.area
        area_2 = pt_large.area

        pt = pt - P
        pt_large = pt_large - P

        surface_small = outline.intersection(pt)
        surface_large = outline.intersection(pt_large)

        v = pt.area

        if v > thresholds[0] and v < thresholds[1]:
            d = {
                "i": i,
                "pt": p,
                "idisc_small": pt,
                "idisc_large": pt_large,
                "area_ratio_small": pt.area / area_1,
                "area_ratio_large": pt_large.area / area_2,
                "surface_small": surface_small,
                "surface_large": surface_large,
                "angle_1": three_point_angle(
                    surface_small.interpolate(0),
                    p,
                    surface_small.interpolate(1.0, normalized=True),
                ),
                "angle_2": three_point_angle(
                    surface_large.interpolate(0),
                    p,
                    surface_large.interpolate(1.0, normalized=True),
                ),
            }
            data.append(d)

    df = pd.DataFrame(data)

    df["R1"] = (df["area_ratio_large"] / df["area_ratio_small"]).fillna(0)
    df["R2"] = (df["angle_1"] / df["angle_2"]).fillna(0)
    df["is_v_notch"] = df.apply(lambda row: row["R1"] < 1 and row["R2"] < 1, axis=1)

    if display:
        from res.utils.dataframes import dataframe_with_shape_images

        return dataframe_with_shape_images(df, ["idisc_small", "idisc_large"])

    return df


def remove_notch_from_outline(g, buffer=50, simplify=0):
    """
    A simple method to remove small notch detail given the characteristic notch height/hole size of 50
    """
    if simplify:
        g = simplify(g)
    g = Polygon(g.buffer(buffer).exterior).buffer(-1 * buffer).exterior
    return g


def get_polygon_segments(p):
    """
    Get the line segments of the polygons i.e. join consecutive points
    """

    def _iter(coords, all_points=[]):
        coords = list(coords)
        for i, p in enumerate(coords):
            if i > 0:
                yield LineString([coords[i - 1], p])

    all_points = []
    if p.type == "Polygon":
        return list(_iter(p.exterior, all_points))
    if p.type == "LineString":
        return list(_iter(p.coords, all_points))
    if p.type == "LinearRing":
        return list(_iter(p, all_points))

    raise Exception(f"Type {p.type} not handled")


def polygon_vertex_angles(g, less_than=360, debug=False, angles=[]):
    """
    returns the angles at each point in the polygon
    returns as a tuple pt, angle

    Example (right angle triangle):

        g = Polygon([[0,0], [np.sqrt(3),0], [np.sqrt(3),1]])
        angles = polygon_vertex_angles(g)
        #returns angles rounded to 30, 90, 60

    A filter is supplied in cases where we want significant angles
    because these are intern angles, close to 180 degrees is a straight line
    and acute angles are sharp e.g. < e.g. 150 are sharper angles
    """

    if g.type != "Polygon":
        g = Polygon(g)

    def _iter(g):
        # remove the closing point
        v = np.array(g.exterior.coords)[1:]
        # iterate in an order of points that puts [1,1] as b followed by the other points in order
        # because a tuple pt,angle is returned the order is not assumed however
        for i, c in enumerate(v):
            a, b = v[i - 2], v[i - 1]
            theta = three_point_angle(a, b, c)
            if theta <= less_than:
                angles.append(theta)
                if debug:
                    print(theta)
                yield b,

    return list(_iter(g))


def find_transform(X, Y):
    """
    Given two matrices, try to compute the transformation
    """
    pad = lambda x: np.hstack([x, np.ones((x.shape[0], 1))])
    unpad = lambda x: x[:, :-1]
    A, res, rank, s = np.linalg.lstsq(pad(X), pad(Y), rcond=None)
    tr = lambda x: unpad(np.dot(pad(x), A))

    return tr


def _polygon_area(points):
    # TODO: is there a cheaper way in numpy
    return geometry.MultiPoint(list(points)).area


def _convex_hull(boundary, **kwargs):
    hull = geometry.MultiPoint(list(boundary)).convex_hull
    try:
        return np.array(hull.exterior.coords)
    except:
        print("Temp: FAILED TO GET A PROPER POLYGON - WHY ??")
        return []


def convex_hulls(boundary_collection, **kwargs):
    """
    Special case of the alpha hull where \alpha=0
    Return part index and areas along with the hull. Keep a parameter in alpha for consistency
    *see also alpha_hulls

    #we can create functions that extract geometry attributes but this can be computed anytime
    yield {"index": index, "alpha": 0, "hull": hull, "area": _polygon_area(hull)}
    """
    for index, boundary in enumerate(boundary_collection):
        yield _convex_hull(boundary, **kwargs)


def alpha_hulls(boundary_collection, **kwargs):
    """
    https://en.wikipedia.org/wiki/Alpha_shape
    https://github.com/bellockk/alphashape
    Using parameter alpha try to go from convex idealization to concave
    Return index and areas as part of a general contract in a collection of results
    alpha MUST be set to None to not use convex hull and do exhaustive iterations

    """
    from alphashape import alphashape

    # make sure we have a list of polygons - normally we do
    bc = np.array(boundary_collection)
    dims = len(bc.shape)
    if dims == 2:
        boundary_collection = [boundary_collection]

    alpha = 0 if "alpha" not in kwargs else kwargs["alpha"]
    for index, boundary in enumerate(boundary_collection):
        # if the alpha parameter is 0 just return the convex hull
        alpha_hull = (
            alphashape(boundary, alpha)
            if alpha is None or alpha > 0
            else _convex_hull(boundary)
        )
        yield alpha_hull


def _numpy_polygon_from_shapely_series(geometry_series):
    """
    this is a dataframe helper for working with polygons directly in this module
    to help callers. it is arguably the wrong abstraction
    """
    return [[p.x, p.y] for p in unary_union(geometry_series.values)]


def alpha_hull(pts, **kwargs):
    # unpacking from generators
    return list(alpha_hulls(pts, **kwargs))[0]


def shape_size(g):
    b = g.bounds
    return (b[2] - b[0], b[3] - b[1])


def place_structure_element_castle(pt, angle=90, stype="v", factor=0):
    """

    #test location
    testa = LineString([[600,600], [800,200]])
    testb = rotate(LineString([[600,600], testa.interpolate(.3, normalized=True)]), 90)
    setup = cascaded_union([testa,testb])
    pt = testa.intersection(testb)
    setup = cascaded_union([setup, pt.buffer(10)])

    #add a structure element with no rotate or translation to test

    mdash = place_structure_element(pt,line_angle(testb))
    viz = cascaded_union([m,mdash])

    we can support half shapes so the base point might be somewhere other than center bottom

    """
    # a reference based object
    v = Polygon([[0, 0], [75, 75], [150, 0]])
    # translate the base to the point
    if pt:
        tr = pt.x - 75, pt.y - 0
        v = translate(v, *tr)
    # rotate around the point
    return rotate(v, angle - factor, origin=pt or "center")
    # the corrected location depending on how we


def place_structure_element(pt, angle=90, stype="v", factor=0):
    """

    #test location
    testa = LineString([[600,600], [800,200]])
    testb = rotate(LineString([[600,600], testa.interpolate(.3, normalized=True)]), 90)
    setup = cascaded_union([testa,testb])
    pt = testa.intersection(testb)
    setup = cascaded_union([setup, pt.buffer(10)])

    #add a structure element with no rotate or translation to test

    mdash = place_structure_element(pt,line_angle(testb))
    viz = cascaded_union([m,mdash])

    we can support half shapes so the base point might be somewhere other than center bottom

    """
    # a reference based object
    scale = 50
    v = Polygon([[0, 0], [scale, scale], [2 * scale, 0]])
    # translate the base to the point
    if pt:
        tr = pt.x - scale, pt.y - 0
        v = translate(v, *tr)
    # rotate around the point
    return rotate(v, angle - factor, origin=pt or "center")
    # the corrected location depending on how we


def hulls_from_point_set_groups(
    dataframe, group_key_col="key", geometry_column="geometry", alpha=None
):
    """
    This is a pandas helper that arguably should not be here
    It is a convenience method to apply alpha hulls to groups of points and return the result of the group
    it is used for example in dxf layer handling

    Example:
        import res
        s3 = res.connectors.load('s3')
        dxf_data = s3.read('s3://meta-one-assets-prod/bodies/ll_2003/pattern_files/body_ll_2003_v1_pattern.dxf')

        hulls_from_point_set_groups(dxf_data.layers)

    """
    hulls = []
    for g, data in dataframe.groupby(group_key_col):
        geom = _numpy_polygon_from_shapely_series(data[geometry_column])
        geom = alpha_hull(geom, alpha=alpha)
        hulls.append(
            {
                group_key_col: g,
                "alpha_hull": geom,
                # "geometry": Polygon(geom)
            }
        )

    return pd.DataFrame([d for d in hulls])


def compensate_geometry(data, column, x, y, recompute_hull=True):
    # stretch the polygon by the amount and return the polygons
    # if recompute hull we "re-simplify" after the stretch op
    return data


# def outlines_from_point_set_groups(
#     dataframe, group_key_col="key", geometry_column="polylines"
def scale_shape_by(factor):
    def f(s):
        if s == None:
            return s
        s = scale(s, xfact=factor, yfact=factor, zfact=1.0, origin=(0, 0))
        # object bounds are minx, miny, maxx, maxy - we want to subtract the minus
        return s

    return f


def shift_geometry_to_origin(g, bounds=None):
    """
    Have any geometry oriented at the origin
    Optionally bounds can be passed in if the points in g do not represent the universe of points i.e. true bound
    shapely bounds are (minx, miny, maxx, maxy) so the first two is the shift we want
    """
    bounds = bounds or g.bounds
    shift = (-1 * bounds[0], -1 * bounds[1])
    return translate(g, *shift)


def swap_points(g: Polygon):
    """
    switch xy in the geometry
    """

    def f(x, y, z=None):
        return (y, x) if not z else (y, x, z)

    return shapely.ops.transform(f, g)

    try:
        return shapely.ops.transform(lambda x, y: (y, x), g)
    except:
        return shapely.ops.transform(lambda x, y, z: (y, x, z), g)


def component_distances(g, pt):
    """
    Create a new point that represents the vector distance from a given point
    """
    return shapely.ops.transform(lambda x, y: (abs(x - pt.x), abs(y - pt.y)), g)


def orient_counter_clockwise(g):
    return orient(g)


def mirror_on_y_axis(g, shift_to_origin=False):
    g = scale(g, yfact=-1)
    return g if not shift_to_origin else shift_geometry_to_origin(g)


def mirror_on_x_axis(g, shift_to_origin=False):
    g = scale(g, xfact=-1)
    return g if not shift_to_origin else shift_geometry_to_origin(g)


def has_x_symmetry(g, tolerance=1e-03):
    """
    determine if its symmetric around x axis
    """
    g = Polygon(g)
    h = g.intersection(mirror_on_x_axis(g))
    return (abs(h.area - g.area) / g.area) < tolerance


def has_y_symmetry(g, tolerance=1e-03):
    """
    determine if its symmetric around y axis
    """
    g = Polygon(g)
    h = g.intersection(mirror_on_y_axis(g))
    return (abs(h.area - g.area) / g.area) < tolerance


def buffer_expansion_trick(g, buffer=0.005):
    t = g.type

    if t in ["MultiPolygon"]:
        g = g.buffer(buffer).buffer(-1 * buffer)
        if g.type == "MultiPolygon":
            # remove irrelevant polygons
            g = cascaded_union([p for p in g if p.area > 0.01])

    return g


def reflect_geom(g, line):
    """
    reflect a geometry over a line
    """
    line = np.asarray(line)
    x1, y1 = line[0]
    x2, y2 = line[1]
    dx = x2 - x1
    dy = y2 - y1
    line_length_sq = dx**2 + dy**2

    def f(x, y, z=None):
        # Calculate the projection of the point onto the line
        u = ((x - x1) * dx + (y - y1) * dy) / line_length_sq
        projection_x = x1 + u * dx
        projection_y = y1 + u * dy

        # Calculate the reflected point
        reflection_x = 2 * projection_x - x
        reflection_y = 2 * projection_y - y

        return (
            (reflection_x, reflection_y)
            if z == None
            else (reflection_x, reflection_y, z)
        )

    return transform(f, g)


def unfold_geometry(g, line):
    """
    unfold over a line using the reflection on the line
    for polygons we can deal with small irregular angles on fold lines
    """
    if not g:
        return g
    g_type_in = g.type
    if g_type_in == "LinearRing":
        g = Polygon(g)
    g = unary_union([g, reflect_geom(g, line)])
    if g_type_in == "LinearRing":
        g = g.buffer(10).buffer(-10).exterior
    return g


def unfold_geometry_x(g, fold_line=None, ref_geom=None):
    """
    if we have stored only one half of a symmetric shape, we can merge its reflection onto it (y axis)
    We should check that xoff in the translation and the bound index is the one we wantWhen we reflect it seems we need to shift too
    """

    # for right folds we assumed we need to offset this way
    ref_geom = g  # ref_geom or g
    offset_width = ref_geom.bounds[2] - ref_geom.bounds[0]
    if fold_line:
        if ref_geom.centroid.x < fold_line[0][0]:
            offset_width *= -1

    g = cascaded_union([g, translate(scale(g, xfact=-1), xoff=offset_width)])
    # g = cascaded_union([g, translate(mirror_on_x_axis(g), xoff=offset_width)])

    return g


def unfold_geometry_y(g, fold_line=None, ref_geom=None):
    """
    if we have stored only one half of a symmetric shape, we can merge its reflection onto it (x axis)
    We should check that xoff in the translation and the bound index is the one we want
    When we reflect it seems we need to shift too
    """
    ref_geom = g  # ref_geom or g
    offset_length = ref_geom.bounds[-1] - ref_geom.bounds[1]
    if fold_line:
        if ref_geom.centroid.y < fold_line[0][1]:
            offset_length *= -1

    g = cascaded_union([g, translate(mirror_on_y_axis(g), yoff=offset_length)])

    return g  # buffer_expansion_trick(g)


def clip_to_ref_point_x(g, pt, eps=0.001):
    """
    This removed rounding errors. Best explained by a scenario
    We rotate a line/surface onto the Y axis. AFter the rotation we expect the line surface to be perfectly parallel with the y axis
    but due to rounding errors it will not be. If we the reflected a shape on this axis the union of the image and the original would not be a single polygon

    Clipping is a good way to say that we know what point we rotated around and we can use this as a true value for where the line or surface is regardless of rounding errors
    """

    def clip(x):
        return pt.x if abs(x - pt.x) <= eps else x

    try:
        return shapely.ops.transform(lambda x, y: (clip(x), y), g)
    except:
        return shapely.ops.transform(lambda x, y, z: (clip(x), y, z), g)


def clip_to_ref_point_y(g, pt, eps=0.001):
    """
    This removed rounding errors. Best explained by a scenario
    We rotate a line/surface onto the Y axis. AFter the rotation we expect the line surface to be perfectly parallel with the y axis
    but due to rounding errors it will not be. If we the reflected a shape on this axis the union of the image and the original would not be a single polygon

    Clipping is a good way to say that we know what point we rotated around and we can use this as a true value for where the line or surface is regardless of rounding errors
    """

    def clip(y):
        return pt.y if abs(y - pt.x) <= eps else y

    try:
        return shapely.ops.transform(lambda x, y: (x, clip(y)), g)
    except:
        return shapely.ops.transform(lambda x, y, z: (x, clip(y), z), g)


def three_point_angle(a, b, c):
    """
    returns the angle between three points
    """
    ba = np.array(a) - np.array(b)
    bc = np.array(c) - np.array(b)
    cosine_angle = np.dot(ba, bc) / (np.linalg.norm(ba) * np.linalg.norm(bc))

    # bounds check for rounding errors
    cosine_angle = max(cosine_angle, -1)
    cosine_angle = min(cosine_angle, 1)

    angle = np.arccos(cosine_angle)
    angle = np.degrees(angle)

    return angle if not np.isnan(angle) else 0


def line_angle_to_y_axis(l):
    """
    compute the angle between a line and the y axis
    take care with directions
    """
    a, b = tuple(l.coords)
    # make sure the higher point is a
    if a[0] < b[0]:
        a, b = b, a
    c = [b[0], a[1]]

    return three_point_angle(a, b, c)


def unfold_on_line(g, linestring, eps=0.001, refine=False, ref_geom=None, **kwargs):
    """
    In general we can unfold a geometry around a line/fold line
    We do this by rotating to a reference angle and then using reflection functions
    and then rotating back
    """
    if g is None:
        return None
    # special cases - if x1~x2 unfold over the y axis... similar for x axis
    line = list(linestring.coords)
    type_in = g.type

    # if we need to refine its because this would not work as linear ring
    if refine and g.type == "LinearRing":
        g = Polygon(g)

    # correct to use the nearest points on the surface of g as we assume this is a better approx of how and where to rotate and mirror
    # line = [nearest_points(g, Point(_l))[0] for _l in line]
    # line = [[p.x, p.y] for p in line]

    # if there is no rotation to do, just infold
    # we could also check the other axis but trying this for now to pipe everything through one flow
    theta = 0
    if abs(line[0][1] - line[1][1]) == 0:
        theta = 90
        # need to think about more about what the reflection point is
        # if its  a simple reflection on that axis it might be work but we always fall back to reflecting on the fold line on the same axis
        try_g = unfold_geometry_y(g, line, ref_geom=ref_geom)
        if try_g.type == "Polygon":
            return try_g
    elif abs(line[0][0] - line[1][0]) == 0:
        try_g = unfold_geometry_x(g, line, ref_geom=ref_geom)
        if try_g.type == "Polygon":
            return try_g
    else:
        # get the lines angle to the vertical
        theta = line_angle_to_y_axis(LineString(line))

    # if the angle is different, rotate onto reference line and do the same thing

    # choose the point closest to the origin (at y axis, x=0)
    # this is the point of rotation on the geometry - an important reference point for further calcs
    ref_point = line[0] if line[0][0] < line[1][0] else line[1]
    # we are rounding the angle to be approx and then clipping - round 2?

    G = rotate(g, angle=round(theta, 2), origin=ref_point)  # if theta > eps else g
    G = clip_to_ref_point_x(G, Point(ref_point), eps=eps) if G.type == "Polygon" else G
    # G = clip_to_ref_point_y(G, Point(ref_point), eps=eps) if G.type == "Polygon" else G

    # assert something? we should now have a surface that is parallel to the Y axis so we can unfold along x

    G = unfold_geometry_x(G, line, ref_geom=ref_geom)

    g = rotate(G, angle=-1 * theta, origin=ref_point)

    if refine:
        # res.utils.logger.debug(f"refining type {g.type}")
        # this is done because sometimes the fold line is slightly off and we need to do this truck to make a whole unfolded linear run
        if g.type == "MultiPolygon":
            g = buffer_expansion_trick(g, buffer=0.005)

        if type_in == "LinearRing":
            g = LinearRing(g.exterior.coords)

        if g.type == "MultiPoint":
            # after unfolding we can have points that are slight too close to another point - an almost mirror
            pruned_points = []
            for i in g.geoms:
                d = i.distance(unary_union(pruned_points)) if len(pruned_points) else -1
                # print(d)
                if d < 0 or d > 0.05:
                    pruned_points.append(i)
            g = unary_union(pruned_points)

        # if multi something else we may need to prune e.g. notches that fold onto each other

    return g


def reflect_on_mirror_line(g, fold_line_points, reference_geometry=None):
    """
    a mirror line is assumed vertical or horizontal here
    we can use it to reflect any geometry over this line and merge into un unfolded geometry
    """
    mirror_point = fold_line_points[0]
    # assume nearly horizontal or vertical but see if its more H than V or vice versa
    use_y_reflection = abs(fold_line_points[0].y - fold_line_points[1].y) < abs(
        fold_line_points[0].x - fold_line_points[1].x
    )

    # get the nearest point on the mirror line to reflect over
    nrp = nearest_points(reference_geometry or g, mirror_point)[0]

    if not use_y_reflection:
        ref = scale(g, xfact=-1, origin=nrp)
    else:  # default
        ref = scale(g, yfact=-1, origin=nrp)

    return cascaded_union([g, ref])


def rotate_onto_inverted_x_axis(g):
    """
    A specific to-image space operation taking something in image space and flipping it so the y axis goes to the x axis and gets inverted so the origin is reflected
    its a 90 degree rotation anti clockwise and a normalization of the CRS
    """
    return mirror_on_x_axis(swap_points(g))


def rotate_onto_x_axis(g):
    """
    A specific to-image space operation taking something in image space and flipping it so the y axis goes to the x axis and gets inverted so the origin is reflected
    its a 90 degree rotation anti clockwise and a normalization of the CRS

    This method illustrates a pattern where we return what ever type we are given but mostly deal in polygons internally for transform ops
    """
    return swap_points(g)

    # todo would need to handle all the geometry types separately
    # shapely_in, g = _ensure_polygon(g)
    # return _ensure_polygon_return_type(shapely_in, swap_points(g))


def geometry_to_image_space(row, g, DPI=300, DPMM=(1 / 0.084667), **kwargs):
    """
    DPMM is
    The shapely shape when plotted should be upside down in the Y and then it can be overlayed in image space with 0 at top
    The piece outline should be at the origin and internal pieces should be offset correctly from that.

    composite operations by convention - used for astm files to take outlines in inches to the image space crs
    suppots passing a  geom directly or by string on the row
    the row is required for context on the piece e.g. offsets

    DPI and DPMM should not really be two parametesr as we need a source param that gets mapped to whatever units
    but for backwards compatability and to allow callers to just choose what they want, will leavel like this for now

    #example
        g = geometry_to_image_space(row, 'outline')
        v = geometry_to_image_space(row, 'viable_surface')
        ax =  GeoDataFrame([g.buffer(50)],columns=['geometry']).plot(figsize=(20,20))
        GeoDataFrame(v,columns=['geometry']).plot(ax=ax,color='red')
    """
    # CONTRACT:
    # a very special case - in image space we have the very messy problem where things that are folded out offset an image by some v-notch height
    # so far not sure how to deal with this systematically - we need some tighter alignments between shapefiles and images
    # manual parts of the proess that change image shapes are probematic
    # not NESTING also needs to know about these adornments but we can probably absorb it into nesting buffers. But NB

    # millimeters is the default scaler from DXF files to pixel space
    # but we can override and use inches instead
    units = kwargs.get("source_units", "mm")

    scaler = DPI if units == "inches" else DPMM

    scaler = scale_shape_by(scaler)

    if isinstance(g, str):
        g = row.get(g)

    if g is None or g.is_empty:
        return None

    bounds = row["outline"].bounds
    folds = row.get("fold_aware_edges")
    # if folds:
    #     # NB!!!!need to play with this - we do want to offset by with the fold would be but inversion and everything - not sure sure
    #     bounds = folds.bounds

    g = translate(g, xoff=-1 * bounds[0], yoff=-1 * bounds[1])

    # # we invert irst, then we change the bounds
    yx_bounds = [bounds[1], bounds[0], bounds[-1], bounds[2]]

    if kwargs.get("swap_points", True):
        g = swap_points(g)
    if kwargs.get("invert_axis", True):
        g = invert_axis(g, yx_bounds, axis_scheme="xy")

    # what should the default be - was False, trying True but need to match up with piece naming logic on ingestion
    # the thing just needs to be consistent but we can test that a piece named R or L matches what we expect in image space
    # test for cases where only an R or L exists e.g. FRONT left and RIGHT CC-6001 piece has this case
    if kwargs.get("mirror", True):
        # this is n astm thing - need to understand this but we dont always need
        g = invert_axis(g, yx_bounds, axis_scheme="yx")

    # if not kwargs.get("allow_out_of_bounds"):
    #     assert (
    #         round(g.bounds[0]) >= 0 and round(g.bounds[1]) >= 0
    #     ), f"the geometry having bounds {g.bounds} in image space landed in negative coordinate space - key: {row.get('key')}"

    return scaler(g)


def invert_axis(
    g: Polygon, bounds: List[int] = None, axis_scheme: str = "xy", **kwargs
):
    """

    inverting the axis is an image space thing: images start top left so the y axis is inverted
    the bounds are needed to determine the "universe" size that you want
    by default we just invert the polygon but you can invert shapes that are withing a large space in which case a different bounds should be passed
    bounds can be either [max,maxy] or [minx,miny, maxy,maxy]. the later of length 4 is what is usede by shapely and a general bounds

    the y_axis is assumed to be at 0 in image space but if we are not yet in image space we can set it to so that we are xy order
    """

    # either bounds with 4 or two can be passed in. the max are the last two
    # if y_axis==1 then y is b otherwise its a in the lambda below
    bounds = bounds or g.bounds
    max_value = bounds[-2:][1 if axis_scheme == "xy" else 0]
    min_value = bounds[:2][1 if axis_scheme == "xy" else 0]
    H = max_value - min_value

    def f(a, b, z=None):
        return (
            (a, -1 * (b - H))
            if axis_scheme == "xy"
            else (
                (-1 * (a - H), b)
                if not z
                else (
                    (a, -1 * (b - H), z)
                    if axis_scheme == "xy"
                    else (-1 * (a - H), b, z)
                )
            )
        )

    return shapely.ops.transform(f, g)

    # try:
    #     return shapely.ops.transform(
    #         lambda a, b: (
    #             (a, -1 * (b - H)) if axis_scheme == "xy" else (-1 * (a - H), b)
    #         ),
    #         g,
    #     )
    # except:
    #     # TODO: better way to make all tarnsforms z aware
    #     # print("inverting Z aware by ", H, "scheme", axis_scheme)
    #     return shapely.ops.transform(
    #         lambda a, b, c: (
    #             (a, -1 * (b - H), c) if axis_scheme == "xy" else (-1 * (a - H), b, c)
    #         ),
    #         g,
    #     )


def union_geometries(
    dataframe,
    group_key_col="key",
    # geometry_column="polylines",
    geometry_column="geometry",
    polygonize_union=False,
    pre_map_geometry=None,
    shift_to_origin=False,
):
    """
    Similar to hull extraction this gets outlines from the data
    These methods group multiple layer geometries into one union to work with dfx files etc.
    Example:
      dxf_data = s3.read('s3://meta-one-assets-prod/bodies/ll_2003/pattern_files/body_ll_2003_v1_pattern.dxf')
      L = dxf_data.layers
      #the entity layer 1 contains part bounds
      L1 = [L['entity_layer']==1]
      #using default column names on this method
      outlines = outlines_from_point_set_groups(L1)

    if a pre-map is supplied is used to map over and convert geometries
    """

    if pre_map_geometry:
        dataframe[geometry_column] = dataframe[geometry_column].map(pre_map_geometry)

    outlines = []
    for k, data in dataframe.groupby(group_key_col):
        assert (
            geometry_column in data.columns
        ), f"The geometry column {geometry_column} is not in the data frame columns {list(data.columns)}"

        geom = unary_union(data[geometry_column].values)

        outlines.append(
            {
                group_key_col: k,
                "geometry": list(polygonize(geom))[0] if polygonize_union else geom,
            }
        )

    df = pd.DataFrame([d for d in outlines])

    return df


def outlines_from_point_set_groups(
    dataframe,
    group_key_col="key",
    # geometry_column="polylines",
    geometry_column="geometry",
    shift_to_origin=False,
):
    """
    Similar to hull extraction this gets outlines from the data
    These methods group multiple layer geometries into one union to work with dfx files etc.
    Example:
      dxf_data = s3.read('s3://meta-one-assets-prod/bodies/ll_2003/pattern_files/body_ll_2003_v1_pattern.dxf')
      L = dxf_data.layers
      #the entity layer 1 contains part bounds
      L1 = [L['entity_layer']==1]
      #using default column names on this method
      outlines = outlines_from_point_set_groups(L1)

    """
    outlines = []
    for k, data in dataframe.groupby(group_key_col):
        assert (
            geometry_column in data.columns
        ), f"The geometry column {geometry_column} is not in the data frame columns {list(data.columns)}"

        geom = unary_union(
            data[data[geometry_column].notnull()][geometry_column].values
        )

        if shift_to_origin:
            geom = shift_geometry_to_origin(geom)

        outlines.append(
            {
                group_key_col: k,
                # "outline": [list(p) for l in geom for p in l.coords],
                # # its better to return a guaranteed polygon
                "geometry": list(polygonize(geom))[0],
            }
        )

    df = pd.DataFrame([d for d in outlines])

    # for dpi in add_dpis:
    #     df[f"geometry_dpi_{dpi}"] = df["geometry"].map(scale_shape_by(dpi))
    #     df[f"outline_dpi_{dpi}"] = df[f"geometry_dpi_{dpi}"].map(
    #         lambda g: [list(p) for p in g.exterior.coords]
    #     )

    return df


def find_2D_transform(X, Y):
    """
    Given two matrices, try to compute the transformation f(X)
    X must be something that returns an array of D dimensional points that are squeezed into 2D for our use cases
    """

    pad = lambda x: np.hstack([x, np.ones((x.shape[0], 1))])
    unpad = lambda x: x[:, :-1]
    A, res, rank, s = np.linalg.lstsq(pad(X), pad(Y), rcond=None)

    def prep_x(x):
        # hard code DIMS = 2 here!!!
        # prep is just to make sure we squeeze to 2 ds and dont try to use a larger size - can make this smarter
        return np.stack([list(p[:2]) for p in np.array(x)])

    tr = lambda x: unpad(np.dot(pad(prep_x(x)), A))

    return tr


def get_geometry_keypoints(g, pose="right"):
    """
    Take a geometry possibly after geometry_to_image_space(row,'field')
    and determine keypoints which are used as reference points
    A use for this is for example choosing points on specific edges of a viable surface that are "near" one of these keypoints

    The pose determines if inside and outside are the the left or right
    pose == right buts the outside on the left as you look at the image (on the right coming out of the page)
    """

    # default e.g. if centered treat as right for now
    if pose != "left":
        pose = "right"

    # adding coords for deprecation warning: coords works on any/either object e.g. polygon exterior or linear ring
    pts = np.array(g.coords) if g.type != "Polygon" else np.array(g.exterior.coords)

    # to do support general geometries - currently assume
    bounds = g.bounds
    width = bounds[2] - bounds[0]
    centroid = g.centroid
    tl = bounds[:2]
    br = bounds[-2:]
    tr = [bounds[2], bounds[1]]
    bl = [bounds[0], bounds[-1]]
    ct = [centroid.x, bounds[0]]
    cb = [centroid.x, bounds[2]]
    com = Point(pts.mean(axis=0))
    # pick some numper to place a point "far" from the geometry as a ref point
    infinity_factor = 1000  # relative to width which is already scaled- we should be further away than we are high
    epl = [-1 * infinity_factor * width, com.y]
    epr = [infinity_factor * width, com.y]

    def near_to(x):
        return nearest_points(g, Point(x))[0]

    d = {
        "outside_top": near_to(tl if pose == "right" else tr),
        "outside_tip": near_to(epl if pose == "right" else epr),
        "outside_bottom": near_to(bl if pose == "right" else br),
        "inside_top": near_to(tr if pose == "right" else tl),
        "inside_tip": near_to(epr if pose == "right" else epl),
        "inside_bottom": near_to(br if pose == "right" else bl),
        "center_top": near_to(ct),
        "center_bottom": near_to(cb),
        # "avatar_center_ref": Point(epr),
        # "avatar_outer_ref": Point(epr),
        # "center_of_mass_top":
        # "center_of_mass_bottom":
        "centroid": g.centroid,
        "center_of_mass": com,
    }

    d["outside_bottom-centroid"] = d["outside_bottom"], d["centroid"]
    d["inside_bottom-centroid"] = d["inside_bottom"], d["centroid"]
    d["outside_bottom-center_top"] = d["outside_bottom"], d["center_top"]
    d["inside_bottom-center_top"] = d["inside_bottom"], d["center_top"]

    return d


#######
#       relative geometry - finding points in geometries sorted by referenc to other points
#       they all take any geometry and return a Point


def relative_after(edges, pose="right", kps=None, sense=-1):
    # TODO:for sense -1 or do the opposite for 1 in future
    fn = point_clockwise_order(edges)

    def f(g):
        points = np.array(g)
        angles = map(fn, points)
        return Point(points[np.argmax(angles)])

    return f


def relative_before(edges, pose="right", kps=None, sense=-1):
    # TODO:for sense -1 or do the opposite for 1 in future
    fn = point_clockwise_order(edges)

    def f(g):
        points = np.array(g)
        angles = map(fn, points)
        return Point(points[np.argmin(angles)])

    return f


# refactor some of these into something nicer for all similar queries
def relative_inside(edges, pose="right", infinity_factor=5, kps=None):
    """
    given a geometry, find the inside point with respect to the edge passed in
    A pose gives the sense e.g. right pieces but the inside on the "right-of"
    """
    kps = kps or get_geometry_keypoints(edges, pose=pose)
    com = kps["center_of_mass"]
    bounds = edges.bounds
    width = bounds[2] - bounds[0]

    pose_factor = 1 if pose != "left" else -1
    pt = Point([pose_factor * infinity_factor * width, com.y])

    def f(g):
        return nearest_points(pt, g)[-1]

    return f


def relative_outside(edges, pose="right", infinity_factor=5, kps=None):
    """
    given a geometry, find the inside point with respect to the edge passed in
    A pose gives the sense e.g. right pieces but the inside on the "right-of"
    """
    kps = kps or get_geometry_keypoints(edges, pose=pose)
    com = kps["center_of_mass"]
    bounds = edges.bounds
    width = bounds[2] - bounds[0]
    pose_factor = -1 if pose != "left" else 1
    # this is just an outside infinity point that we could add to the kps themselves
    pt = Point([pose_factor * infinity_factor * width, com.y])

    def f(g):
        return nearest_points(pt, g)[-1]

    return f


def distance_to_relative_inside(edges, infinity_factor=5):
    """
    Proximity function using a "point at infinity" which factor * width of geometry
    """
    kps = get_geometry_keypoints(edges)
    com = kps["center_of_mass"]
    bounds = edges.bounds
    width = bounds[2] - bounds[0]

    def f(p):
        pt = Point([infinity_factor * width, com.y])
        return int(pt.distance(p))

    return f


def distance_to_relative_outside(edges, infinity_factor=5):
    """
    Proximity function using a "point at infinity" which factor * width of geometry
    """
    kps = get_geometry_keypoints(edges)
    com = kps["center_of_mass"]
    bounds = edges.bounds
    width = bounds[2] - bounds[0]

    def f(p):
        pt = Point([-1 * infinity_factor * width, com.y])
        return int(pt.distance(p))

    return f


def is_between_kps(pt, edges, kp1, kp2):
    """
    using a circtui from a reference key point going clockwise, determine if another pint lies on a segment defined by kps start and stop clockwise
    """
    # when treated as a right piece, inside is right and outside is left
    kps = get_geometry_keypoints(edges)
    kp1 = edges.project(kps[kp1])
    kp2 = edges.project(kps[kp2])
    pt = edges.project(pt)

    l = edges.length

    def _offset(p):
        ref = kp2
        ref = edges.project(kps["inside_tip"])
        return (p + (l - ref)) % l

    f, s = _offset(kp1), _offset(kp2)

    pt = _offset(pt)

    return f <= pt < s


def surface_side(pt, edges, line_rule=True):
    """
    surface side takes a point on a ring and decides if its top/left/right/bottom
    if the edges are in fact just a line and not a ring, we always return top
    the purpose of this function is just for the label orientation for legibility

    [use is between on the edge circuit for this]
    """

    # this is a funny rule - normal the edges are a ring but for knits for example we only consider one line at the top for the viable surface
    # so what this means is if the edges are not a ring but in fact a line, then the side is always on the top so the label sits facing up reading left to right
    if edges.type == "LineString" and line_rule:
        return "top"

    if is_between_kps(pt, edges, "inside_top", "inside_bottom"):
        return "right"
    elif is_between_kps(
        pt,
        edges,
        "outside_bottom",
        "outside_top",
    ):
        return "left"
    elif is_between_kps(pt, edges, "inside_bottom", "outside_bottom"):
        return "bottom"
    return "top"


def round_coordinates(coordinates, precision=3):
    if isinstance(coordinates, (list, tuple)):
        return type(coordinates)(map(round_coordinates, coordinates))
    return round(coordinates, ndigits=precision)


def geometry_to_geojson(geom, precision=3):
    # convert to geojson object
    geom = shapely.geometry.mapping(geom)

    # round the coordinates to the precision required
    geom["coordinates"] = round_coordinates(geom["coordinates"], precision)

    return geom


def dict_to_geojson(data, geometry_key="geometry", precision=3):
    properties = {}
    geom = data[geometry_key]
    geom_type = geom.geom_type
    if geom.is_empty:
        return {}

    # convert to geojson object & round
    geom = geometry_to_geojson(geom, precision)

    # there are some exceptions when converting shapely to geojson
    if geom_type == "LinearRing":
        properties["__is_linear_ring"] = True
        geom["type"] = "LineString"

    # get all the other properties
    for key, value in data.items():
        # make sure the value isn't None or NaN before adding
        if key != geometry_key and value is not None and value is not np.nan:
            if isinstance(value, np.generic):
                value = value.item()
            properties[key] = value

    return {"type": "Feature", "geometry": geom, "properties": properties}


def to_geojson(data, geometry_key="geometry", precision=3):
    # print(type(data))
    data_type = type(data)
    if data is None:
        return {}

    elif issubclass(data_type, shapely.geometry.base.BaseGeometry):
        return dict_to_geojson({"geometry": data}, geometry_key, precision)
    elif data_type is dict:
        return dict_to_geojson(data, geometry_key)
    elif data_type is list:
        if len(data) == 0:
            return {}

        first_type = type(data[0])
        geojson_list = []
        if issubclass(first_type, shapely.geometry.base.BaseGeometry):
            geojson_list = [geometry_to_geojson(d, precision) for d in data]
        else:
            geojson_list = [dict_to_geojson(d, geometry_key, precision) for d in data]

        return {"type": "FeatureCollection", "features": geojson_list}
    elif isinstance(data, pd.DataFrame):
        if len(data.index) == 0:
            return {}

        return {
            "type": "FeatureCollection",
            "features": [
                dict_to_geojson(d, geometry_key, precision)
                for d in data.to_dict("records")
            ],
        }
    else:
        raise Exception("Cannot convert to geojson")


def from_geojson(x):
    """
    simple version, not tested in general
    this weorks if the type string can be evaluated as a shapely object imported in this context
    """

    if isinstance(x, str):
        x = json.loads(x)

    if pd.isnull(x):
        res.utils.logger.warn(f"Missing geometry {x}")
        return None
    type = x.get("type")

    if type == "FeatureCollection":
        return [from_geojson(f) for f in x["features"]]
    if type == "Feature":
        x = x["geometry"]
    return eval(x["type"])(x["coordinates"]) if x and len(x) else None


def is_bias_piece(shape, min_aspect_ratio=4):
    polygon = Polygon(shape)
    # detect bias pieces (long diagonal strips)
    # essentially just see if the thing is a perfect axis aligned rectangle if we rotate it 45 degrees.
    bounds = rotate(polygon, -45).bounds
    if box(*bounds).area > polygon.area * 1.05:  # TODO: see if slop is necessary here
        return False
    # only care about stuff with a big enough aspect ratio.
    dx = bounds[2] - bounds[0]
    dy = bounds[3] - bounds[1]
    return dx > min_aspect_ratio * dy or dy > min_aspect_ratio * dx


def normal_vector_via_chord(
    g,
    pt,
    outline,
    factors=[20],
    simplify_factor=1,
    notch_length=75,
    dist_tolerance=5,
    interior_offset=35,
    explain=False,
):
    """
    For notches or generally we may want the normal vector on the outline surface pointing into the shape
    The effort here is more about approximation for complicated surfaces

    g is an edge i.e. a segment within a close ring outline

    factors are added for smooting but for now we will just support one chord. This accounts for surface irregularityand we can refine with good test cases
    """
    if simplify_factor:
        g = g.simplify(1)
    # inside offset into the region where we want notches to end

    prj = g.project(pt)
    chords = [
        LineString([g.interpolate(prj - f), g.interpolate(prj + f)]) for f in factors
    ]
    C = chords[0]
    # whats left into the shape on the notch when we move to the cord
    vl = notch_length - pt.distance(C)
    g_in = inside_offset(C, outline, r=vl)
    pt_proj = g_in.interpolate(g_in.project(pt))
    pt = Point(pt.x, pt.y)
    C = LineString([pt, pt_proj])

    if explain:
        from geopandas import GeoDataFrame

        GeoDataFrame(
            [
                {"key": "offsets", "geometry": unary_union([g, g_in])},
                {"key": "point", "geometry": pt.buffer(3)},
                {"key": "notch_line", "geometry": C.buffer(1)},
                {"chord": "notch_line", "geometry": chords[0]},
            ]
        ).plot(figsize=(15, 15))

    if C.distance(g) > dist_tolerance:
        res.utils.logger.warn(
            f"The notch vector at distance {C.distance(g) } is not on the surface"
        )
        return None

    # if not Polygon(g).contains(ep1) and not Polygon(g).contains(ep2):
    #     res.utils.logger.warn(f"one of the notche points is outside the shape")

    return C


def project_point_from_surface(pt, outline, offset):
    """
    given a point near a surface, project away from the surface up to 2 * distance of point to surface
    """
    pts = MultiPoint(nearest_points(pt, outline))
    line_point = pts - pt
    l = LineString([pt, line_point])
    l = rotate(l, 180, pt)
    extreme_point = l.interpolate(1, normalized=True)

    # new lien from surface to extreme points projected from surface
    l = LineString([line_point, extreme_point])

    placement_point = l.interpolate(offset)

    return placement_point


def normal_vector(
    g,
    pt,
    outline,
    factors=[20],
    simplify_factor=1,
    notch_length=75,
    dist_tolerance=5,
    explain=False,
):
    """
    For notches or generally we may want the normal vector on the outline surface pointing into the shape
    The effort here is more about approximation for complicated surfaces

    g is an edge i.e. a segment within a close ring outline

    factors are added for smooting but for now we will just support one chord. This accounts for surface irregularityand we can refine with good test cases
    """
    if simplify_factor:
        g = g.simplify(1)
    # inside offset into the region where we want notches to end
    g_in = inside_offset(g, outline, r=notch_length)
    prj = g.project(pt)
    chords = [
        LineString([g.interpolate(prj - f), g.interpolate(prj + f)]) for f in factors
    ]
    # tangent = np.mean([line_angle(c) for c in chords])
    C = rotate(chords[0], -90)
    # safety determine start point
    ep1 = C.interpolate(0)
    ep2 = C.interpolate(0.99, normalized=True)
    if ep1.distance(g_in) > ep2.distance(g_in):
        ep1 = ep2
    ep2 = C.interpolate(0.5, normalized=True)
    # this is a way to find a corrected point on the offset line
    # we do this so we can project the normal line a distance of our choosing
    ep1 = g_in.interpolate(g_in.project(ep1))

    C = LineString([ep2, ep1])

    if explain:
        from geopandas import GeoDataFrame

        GeoDataFrame(
            [
                {"key": "offsets", "geometry": unary_union([g, g_in])},
                {"key": "point", "geometry": pt.buffer(3)},
                {"key": "notch_line", "geometry": C.buffer(1)},
                {"chord": "notch_line", "geometry": chords[0]},
            ]
        ).plot(figsize=(15, 15))

    if C.distance(g) > dist_tolerance:
        res.utils.logger.warn(
            f"The notch vector at distance {C.distance(g) } is not on the surface"
        )
        return None

    # if not Polygon(g).contains(ep1) and not Polygon(g).contains(ep2):
    #     res.utils.logger.warn(f"one of the notche points is outside the shape")

    return C


def make_castle_structure_surface_element(ol, width=30, height=50):
    """
    the three sides coming in from the segment but without the segment
    """

    def f(pt):
        # take a small surface region segment around the notch point
        s = ol.intersection(pt.buffer(width))
        # get the opposite side
        part = inside_offset(s, ol, r=height)
        # complete the castle 4 sides and remove the surface side
        return unary_union([part, s]).convex_hull - s

    return f


def make_v_structure_element(ol, offset_angle_factor=270, half_side=300 / 4):
    def f(pt):
        s = ol.intersection(pt.buffer(half_side))

        # x marks the spot. we can place a diamond and the boundary of this and the original shape makes a v notch
        x = rotate(s, -90)
        return unary_union([x, s]).convex_hull

    return f


def prune_lines_by_starting_point(g, min_distance=50):
    """
    dont not allow features that are to close to each other
    """
    lines = [l for l in g]
    first_points = [l.interpolate(0) for l in lines]

    pts = []
    valid_lines = []
    for i, p in enumerate(first_points):
        if len(pts):
            if p.distance(unary_union(pts) < min_distance):
                continue
        pts.append(p)
        valid_lines.append(lines[i])
    return MultiLineString(lines)


def pil_image_alpha_centroid(pil_image):
    alpha = np.asarray(pil_image)[:, :, 3] / 255.0
    z = np.sum(alpha)
    cx = np.sum(alpha.sum(axis=0) * np.arange(alpha.shape[1])) / z
    cy = np.sum(alpha.sum(axis=1) * np.arange(alpha.shape[0])) / z
    return cx, cy


def move_polygon_over_alpha(pil_image, polygon):
    """
    lame method to try to align a polygon over a piece image by maximizing the overlap between the polygon
    and non-transparent pixles of the image.
    """
    img = np.asarray(pil_image)

    def alpha(x, y):
        if x < 0 or x >= img.shape[1] or y < 0 or y > img.shape[0]:
            return False
        return img[int(y), int(x), 3] != 0

    polygon = Polygon(polygon)  # ensure its filled in.
    icx, icy = pil_image_alpha_centroid(pil_image)
    pcx, pcy = polygon.centroid.xy
    dx = icx - pcx
    dy = icy - pcy
    q = translate(polygon, dx, dy)
    overlap = []
    for _ in range(10):
        qcx, qcy = q.centroid.xy
        gx, gy = 0, 0
        z = 0
        ov = 0
        x0, y0, x1, y1 = q.bounds
        for x in np.linspace(x0, x1, 100):
            for y in np.linspace(y0, y1, 100):
                if q.contains(Point(x, y)):
                    axy = alpha(x, y)
                    d = -1 if axy else 1
                    gx += (qcx - x) * d
                    gy += (qcy - y) * d
                    z += 1
                    ov += 1 if axy else 0
        overlap.append(ov)
        gx /= z
        gy /= z
        if abs(gx) < 2 and abs(gy) < 2:
            break
        dx += gx
        dy += gy
        q = translate(polygon, dx, dy)
    if overlap[-1] < overlap[0]:
        # just in case we somehow made things worse, return the centroid alignment.
        return icx - pcx, icy - pcy
    return dx, dy


def first_corner_projection_offset_function(g, from_pt):
    """
    there may be a smarter way but this create a measure from a reference point of choice e.g a corner point so all other points can be referenced in order or interpolated distance from there
    """

    def first_corner_projection():
        """
        how far is this from the end of the geometry. shift by that much to put it at the ref point
        """
        return g.length - g.project(from_pt)

    offset = first_corner_projection()

    l = g.length

    def f(pt):
        return (g.project(pt) + offset) % l

    return f
