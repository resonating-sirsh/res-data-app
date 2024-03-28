import numpy as np
from math import floor, ceil
from shapely.affinity import translate
from shapely.geometry import box, Polygon
from shapely.ops import orient, clip_by_rect, unary_union
from res.learn.optimization.packing.packing import lower_left
from res.learn.optimization.packing.geometry import negate_shape
from .segment_tree import SweeplineHelper


def slope(seg):
    return (seg[3] - seg[1]) / (seg[2] - seg[0])


def interpolate(seg, x):
    return round(seg[1] + (seg[3] - seg[1]) * (x - seg[0]) / (seg[2] - seg[0]))


def coords_to_latice(c):
    return [(round(x), round(y)) for x, y in c]


def translate_seg(seg, v):
    return (seg[0] + v[0], seg[1] + v[1], seg[2] + v[0], seg[3] + v[1])


def containing_lattice_polygon(p):
    dx, dy = p.centroid.coords[0]
    return Polygon(
        [
            (floor(x) if x - dx <= 0 else ceil(x), floor(y) if y - dy <= 0 else ceil(y))
            for x, y in p.boundary.coords
        ]
    )


def combine_frontier(segs, x0, x1, frontier):
    """
    Given a frontier of line segments which all cover disjoint parts of the x-axis,
    add in the new segments and remove what was in the extents (x0, x1)
    """
    new_frontier = []
    added = False
    for p in frontier:
        px0, py0, px1, py1 = p
        if px0 < x0 and px1 > x1:
            # seg splits p p
            if px0 != x0:
                new_frontier.append((px0, py0, x0, interpolate(p, x0)))
            new_frontier.extend(segs)
            if px1 != x1:
                new_frontier.append((x1, interpolate(p, x1), px1, py1))
            added = True
        elif px0 < x0 and px1 > x0:
            # seg covers right end of p
            if px0 != x0:
                new_frontier.append((px0, py0, x0, interpolate(p, x0)))
            if not added:
                new_frontier.extend(segs)
                added = True
        elif px0 < x1 and px1 > x1:
            # seg covers left end of p
            if not added:
                new_frontier.extend(segs)
                added = True
            if px1 != x1:
                new_frontier.append((x1, interpolate(p, x1), px1, py1))
        elif px0 >= x1 or px1 <= x0:
            new_frontier.append(p)
    if not added:
        # new segments must have exactly covered some set of existing segments
        new_frontier.extend(segs)
        new_frontier = sorted(new_frontier, key=lambda p: p[0])
    for i in range(1, len(new_frontier)):
        if new_frontier[i][0] > new_frontier[i - 1][2]:
            raise ValueError(f"Bad frontier {new_frontier}")
    return new_frontier


def get_frontier_points(shape):
    """
    Returns the set of vertices which are visible if youre looking straight down onto this shape.
    these are the segments which we need to use for collision detection when placing shapes on top of
    each other.
    """
    frontier = []
    # orient clockwise so we can go from left to right and over the top of the shape.
    coords = list(orient(shape, -1).exterior.coords[:-1])
    n = len(coords)
    i = 0
    # start at the leftmost point, breaking ties by taking the highest one.
    i0 = min(range(n), key=lambda i: [coords[i][0], -coords[i][1]])
    x1 = shape.bounds[2]
    while i < n:
        p = coords[(i + i0) % n]
        frontier.append(p)
        if p[0] >= x1:
            break
        i += 1
    return frontier


class ConvexPackingPiece:
    """
    A convex polygon to be packed - either a complete piece or a part of one.
    """

    def __init__(self, bound):
        self.bound = containing_lattice_polygon(bound)
        self.local_offset = -lower_left(self.bound)
        self.bound = translate(self.bound, *self.local_offset)
        self.frontier = self.get_frontier()
        self.negated_edge = self.get_negated_edge()

    def get_frontier(self):
        """
        Returns the set of line segments which are visible if youre looking straight down onto this box.
        these are the segments which we need to use for collision detection when placing shapes on top of
        each other.
        """
        frontier_points = get_frontier_points(self.bound)
        return [
            (
                frontier_points[i - 1][0],
                frontier_points[i - 1][1],
                frontier_points[i][0],
                frontier_points[i][1],
            )
            for i in range(1, len(frontier_points))
            if frontier_points[i][0] - frontier_points[i - 1][0]
            > 0  # dont bother with vertical segments.
        ]

    def get_negated_edge(self):
        """
        Returns the set of points along the frontier of -shape.  These are the points we need when computing
        the no-fit-region of this polygon with a line segment -- and when we only care about the polygon being
        somewhere above that segment (hence we don't need to care about what the top of the shape would collide with).
        """
        return get_frontier_points(negate_shape(self.bound))

    def no_fit_segs(self, seg, clip_left, clip_right):
        """
        Compute the top half of: seg + (-shape) where + means the minkowski sum.
        Imagine taking the shape and placing it upside down at each end of the line segment
        then taking the convex hull of what we have.  This is essentially what is going on here.
        The difference between this and the real minkowski sum is that we only care about half of
        the shape -- because we are going to insist that the shape is placed above the line segment anyway
        and so we dont have to worry about what the top half of the shape could intersect with.
        """
        sx, sy, tx, ty = seg
        stx, sty = tx - sx, ty - sy
        # normal vector to the segment
        nx, ny = -sty, stx
        # index where the segment fits into the convex hull
        max_t, max_dot = None, None
        for t, (u, v) in enumerate(self.negated_edge):
            dot = u * nx + v * ny
            if max_dot is None or dot >= max_dot:
                max_dot = dot
                max_t = t
        segs = []
        if sx != tx:
            segs = [
                (
                    sx + self.negated_edge[max_t][0],
                    sy + self.negated_edge[max_t][1],
                    tx + self.negated_edge[max_t][0],
                    ty + self.negated_edge[max_t][1],
                )
            ]
        if not clip_left:
            left = [
                (
                    sx + self.negated_edge[i - 1][0],
                    sy + self.negated_edge[i - 1][1],
                    sx + self.negated_edge[i][0],
                    sy + self.negated_edge[i][1],
                )
                for i in range(1, max_t + 1)
                if self.negated_edge[i - 1][0] != self.negated_edge[i][0]
            ]
            segs = left + segs
        if not clip_right:
            right = [
                (
                    tx + self.negated_edge[i - 1][0],
                    ty + self.negated_edge[i - 1][1],
                    tx + self.negated_edge[i][0],
                    ty + self.negated_edge[i][1],
                )
                for i in range(max_t + 1, len(self.negated_edge))
                if self.negated_edge[i - 1][0] != self.negated_edge[i][0]
            ]
            segs = segs + right
        return segs


class PackingGeometry:
    """
    Represents a shape to be packed - with a bounding box which *is convex* and might have simpler
    geometry (i.e., less vertices) than the real shape.
    The bounding box is translated so that its lower left point is at (0, 0).
    """

    def __init__(self, geom, bounds, type_name, piece_group=None):
        self.geom = geom
        self.piece_group = piece_group
        self.type_name = type_name
        self.pieces = [ConvexPackingPiece(p) for p in bounds]
        self.box = unary_union(
            [translate(b.bound, *-b.local_offset) for b in self.pieces]
        )
        self.local_offset = -lower_left(self.box)
        self.box = translate(self.box, *self.local_offset)
        self.bounds = self.box.bounds
        self.width = self.bounds[2] - self.bounds[0]
        self.frontier = [
            translate_seg(s, self.local_offset - p.local_offset)
            for p in self.pieces
            for s in p.get_frontier()
        ]

    def translated(self, translation):
        return translate(
            self.geom,
            translation[0] + self.local_offset[0],
            translation[1] + self.local_offset[1],
        )

    def no_fit_segs(self, frontier):
        # build up all the no-fit segments.  we can clip out a bunch of them that we know wont be
        # contained in the actual surface we care about (e.g., when we have a frontier that looks like a
        # staircase then we will never need the right hand ends of any of the no fit segments since they will
        # always be underneath the left hand edge of the next one).
        no_fit_segs = []
        for p in self.pieces:
            pt = -(self.local_offset - p.local_offset)
            need_translate = np.linalg.norm(pt) > 1
            for i, frontier_seg_i in enumerate(frontier):
                clip_left = i == 0 or frontier[i - 1][3] > frontier_seg_i[1]
                clip_right = (
                    i == len(frontier) - 1 or frontier[i + 1][1] > frontier_seg_i[3]
                )
                if need_translate:
                    no_fit_segs += [
                        translate_seg(s, pt)
                        for s in p.no_fit_segs(frontier_seg_i, clip_left, clip_right)
                    ]
                else:
                    no_fit_segs += p.no_fit_segs(frontier_seg_i, clip_left, clip_right)
        return no_fit_segs

    def best_fit_coordinates(self, frontier, max_width, margin=0, x_loss_val=0.05):
        """
        Build up the no-fit-polygon between the bottom half of this polygon and the current frontier.
        Then march along the thing and determine the lowest place where the polygon will fit.
        """
        loss_fn = lambda p: p[1] + x_loss_val * p[0]
        bx0, _, bx1, _ = self.bounds
        max_x0 = max_width - margin - bx1
        min_x0 = margin - bx0
        if min_x0 > max_x0:
            raise ValueError(
                f"Unable to nest box with width {bx1 - bx0} into nesting with max width {max_width} and margin {margin}"
            )
        sweepline = SweeplineHelper(self.no_fit_segs(frontier))
        x0 = min_x0  # x coordinate to check
        best_coordinates = None
        best_loss = None
        while True:
            x0 = round(x0)
            if x0 > max_x0:
                break
            sweepline.update(x0)
            max_y, max_seg, max_slope, y_coord = None, None, None, None
            # find the maximum y coordinate at x0, and the line segment with the greatest slope
            # which passes through that point
            for sx0, sy0, sx1, sy1 in sweepline.active:
                if sx0 <= x0 and sx1 > x0:
                    slope = (sy1 - sy0) / (sx1 - sx0)
                    ys = round(sy0 + (x0 - sx0) * slope)
                    if (
                        max_y is None
                        or max_y < ys
                        or (max_y == ys and slope > max_slope)
                    ):
                        max_y = ys
                        max_seg = (sx0, sy0, sx1, sy1)
                        max_slope = slope
                # also see what the height would have been immediately before this x coordinate
                # since we may have just gone up a discontinuity
                if sx0 < x0 and sx1 >= x0:
                    slope = (sy1 - sy0) / (sx1 - sx0)
                    ys = round(sy0 + (x0 - sx0) * slope)
                    if y_coord is None or ys > y_coord:
                        y_coord = ys
            if max_y is None:
                # we are at an x0 where there are no segments meaning we are done.
                break
            curr_coords = (
                (
                    x0,
                    max_y,
                )
                if y_coord is None or max_y < y_coord or x0 == min_x0
                else (x0 - 1, y_coord)
            )
            curr_loss = loss_fn(curr_coords)
            if best_coordinates is None or curr_loss < best_loss:
                best_coordinates = curr_coords
                best_loss = curr_loss
            # if the segment that defined max_y is sloping uphill or flat then theres no need to check anywhere else
            # along it because we know it can't possibly be any better than the current coordinates.
            # note: making an assumption that the objective function is monotonic along straight lines in the x,y plane
            if loss_fn(max_seg[2:]) >= curr_loss:
                x0 = max(round(max_seg[2]), x0 + 1)
            else:
                # the line segment is sloping down - so find out the next spot where it intersects something or
                # if it doesn't then just go to the end.
                min_delta = None
                sweepline.update(max_seg[2], drop=False)
                for sx0, sy0, sx1, sy1 in sweepline.active:
                    slope = (sy1 - sy0) / (sx1 - sx0)
                    # line segment overlaps x0
                    if sx0 <= x0 and sx1 >= x0 and abs(slope - max_slope) > 0:
                        ys = sy0 + slope * (x0 - sx0)
                        delta = round((ys - max_y) / (max_slope - slope))
                        if delta > 0 and (min_delta is None or delta < min_delta):
                            min_delta = delta
                    # line segment starts after x0
                    elif sx0 > x0:
                        ysx0 = max_y + max_slope * (
                            sx0 - x0
                        )  # y coordinate of max segment at sx0
                        if sy0 > ysx0:
                            delta = round(sx0 - x0)
                            if delta > 0 and (min_delta is None or delta < min_delta):
                                min_delta = delta
                        elif slope != max_slope and slope > 0:
                            delta = round(
                                (sx0 - x0) + (sy0 - ysx0) / (max_slope - slope)
                            )
                            if delta > 0 and (min_delta is None or delta < min_delta):
                                min_delta = delta
                if min_delta is None or x0 + min_delta + 0 >= max_seg[2]:
                    # skip to the end of the segment and recompute everything
                    x0 = max(round(max_seg[2]), x0 + 1)
                else:
                    x0 = x0 + min_delta
        return best_coordinates


def bounding_polygon(geom, max_vertices):
    """
    Build up a simple bounding polygon for the shape by starting with an axis aligned box
    and then iteratively cutting off the biggest piece that we can.
    """
    hull = geom.convex_hull
    if len(hull.exterior.coords) - 1 <= max_vertices:
        return hull
    b = hull.bounds
    max_t = b[2] - b[0] + b[3] - b[1]
    coords = [np.array(p) for p in orient(hull).exterior.coords]
    return _refine_bounding_polygon(coords, box(*b), max_vertices, max_t)


def _refine_bounding_polygon(coords, bound, max_vertices, max_t):
    best = None
    for i in range(1, len(coords)):
        d = coords[i] - coords[i - 1]
        d = d / np.sqrt(np.sum(d * d))
        u = np.array([d[1], -d[0]])
        # kind of lame shapely doesnt have a halfspace so here we are.
        bi = bound - Polygon(
            [
                coords[i] - max_t * d,
                coords[i] + max_t * d,
                coords[i] + max_t * d + max_t * u,
                coords[i] - max_t * d + max_t * u,
            ]
        )
        if bi.area > 0 and (best is None or bi.area < best.area):
            best = bi
    if len(best.exterior.coords) - 1 > max_vertices or best.area / bound.area > 0.99:
        return bound
    else:
        return _refine_bounding_polygon(coords, best, max_vertices, max_t)


def divider(max_width):
    """
    Construct a "piece" to go in the nesting that just acts as a horizontal divider.
    The point of these is to add some stability into the nesting and allow better convergence
    of the local search.  Note that in principle these don't make the nesting any worse as they
    could all be placed at the very top or bottom by the search algorithm - and they dont contribute any height.
    On the other hand, by placing these throughout the nesting what we do is essentially
    break the nesting into segments supported by these pieces, and when moving around pieces in the nest
    only two of the segments will actually change length - whereas without these moving any one piece can
    dramatically change the layout of all the pieces above it.
    """
    geom = box(0, 0, max_width, 0)
    return PackingGeometry(geom, [geom], "divider", None)


def guillotine_divider(width, height):
    geom = box(0, 0, width, height)
    return PackingGeometry(geom, [geom], "guillotine_spacer", None)


def vertical_spacer(width):
    """
    See notes in packing.get_packing
    """
    divider = box(0, 0, width, 100000000)
    return PackingGeometry(divider, [divider], "vertical_spacer", None)


def convex_approximation(geom, max_slop=0.05, max_depth=3):
    geom_area = geom.area
    axis_aligned_box = box(*geom.bounds)
    if axis_aligned_box.area < (1 + max_slop) * geom_area:
        return [axis_aligned_box], "axis_aligned_box"
    oriented_box = clip_by_rect(geom.minimum_rotated_rectangle, *geom.bounds)
    if oriented_box.area < (1 + max_slop) * geom_area:
        return [oriented_box], "oriented_box"
    # the simplified bound is guaranteed to be no worse than the axis aligned box at least.
    simplified_bound = clip_by_rect(bounding_polygon(geom, 10), *geom.bounds)
    if max_depth <= 1 or simplified_bound.area < (1 + max_slop) * geom_area:
        return [simplified_bound], "simplified_bound"
    # split the polygon along the middle and try to find good approximations to either side.
    if geom.area < geom.convex_hull.area * (1 - max_slop):
        cx = geom.centroid.xy[0][0]
        lhs = convex_approximation(
            geom - box(cx, *geom.bounds[1:]),
            max_slop=max_slop,
            max_depth=max_depth - 1,
        )
        rhs = convex_approximation(
            geom - box(*geom.bounds[0:2], cx, geom.bounds[3]),
            max_slop=max_slop,
            max_depth=max_depth - 1,
        )
        pieces = lhs[0] + rhs[0]
        total_area = sum(p.area for p in pieces)
        if total_area < simplified_bound.area * 0.99:
            return pieces, "union"
    return [simplified_bound], "simplified_bound"


def choose_geom(geom, piece_group=None, max_slop=0.05, max_width=None):
    """
    Represent a piece by an appropriate bounding box - attempting to find the simplest shape
    which encloses the box without using more than 1 + max_slop times the area of the original shape.
    """
    bound, typename = convex_approximation(geom, max_slop=max_slop)
    return PackingGeometry(geom, bound, typename, piece_group)
