import numpy as np
from shapely.geometry import box, LineString, Polygon, Point
from shapely.geometry.polygon import orient
from shapely.ops import unary_union
from res.utils import logger
from .geometry import (
    count_vertices,
    negate_shape,
    shapely_to_skgeom,
    skgeom_to_shapely,
    simple_inscribed_polygon,
    translate,
)

# choose the lowest point at all costs - tie breaking by left to right
FEASIBLE_SET_SORT_KEY_LOWER_LEFT = lambda p: [p[1], p[0]]
# also give some priority to getting the piece more to the left of the roll -- possibly trading off some height.
# this relieves some idiosyncrasies that happen with the above method.
FEASIBLE_SET_SORT_KEY_RELAXED = lambda p: p[1] + 0.05 * p[0]


def minkowski_sum(a, b):
    raise Exception("Unused codepath")


class PackingShape:
    """
    Represents a shape thats involved in the packing operation.
    Namely the shape is transformed so that its all in the positive quadrant.
    Then the resulting shape is eventually moved to its final location, so the resulting
    transform is the sum of these two.
    Also keep track of a simplified version of the geometry which is fully contained within
    the real one -- that we can use for a cheaper version of collision detection when
    generating a feasible set.
    """

    def __init__(self, shape: Polygon):
        self.shape = orient(shape, sign=1)
        try:
            self.simple_interior = orient(
                simple_inscribed_polygon(self.shape, 10), sign=1
            )
        except Exception as e:
            logger.info(f"Failed to simplify geometry {e}")
            self.simple_interior = self.shape
        self.is_simplified = count_vertices(self.shape) > count_vertices(
            self.simple_interior
        )
        self.area = shape.area
        self.centering = np.array([-shape.bounds[0], -shape.bounds[1]])
        self.translate(np.array([0.0, 0.0]))
        _, _, self.width, self.height = self.bounds

    def translate(self, translation):
        """
        Actually place the object -- this also generates the skgeom versions of the translated shapes.
        """
        self.translation = translation
        total_translation = self.get_total_translation()
        self.translated = translate(self.shape, total_translation)
        self.translated_simple_interior = translate(
            self.simple_interior, total_translation
        )
        self.translated_skgeom = None  # shapely_to_skgeom(self.translated)
        self.translated_simple_interior_skgeom = (
            None  # shapely_to_skgeom(self.translated_simple_interior)
        )
        self.bounds = self.translated.bounds

    def get_total_translation(self):
        """
        The total translation vector to apply to the original shape to get its nested location.
        """
        return self.centering + self.translation

    def bounds_intersect_box(self, x_min, y_min, x_max, y_max):
        # Really we should use some data structure to retrieve the shapes which intersect the box
        # but for now the complexity of this is miniscule compared to the rest of the computation.
        return not (
            self.bounds[3] < y_min
            or self.bounds[2] < x_min
            or self.bounds[1] > y_max
            or self.bounds[0] > x_max
        )


def lower_left(shape, key=FEASIBLE_SET_SORT_KEY_LOWER_LEFT):
    """
    Return the lower left point of a polygon
    """
    if shape.geom_type == "Point":
        return np.array(shape.coords[0])
    elif shape.geom_type == "Polygon":
        return lower_left(shape.boundary, key)
    elif shape.geom_type == "LineString":
        return np.array(min(shape.coords[:-1], key=key))
    else:
        # the shape is the union of a bunch of polygons.
        return min([lower_left(poly, key) for poly in list(shape)], key=key)


def get_feasible_set(feasible_width, feasible_height, no_fit_region):
    """
    Realistically this thing is just computing the set difference between the feasible region on the sheet
    which is determined by the shapes height and width, and the pre-computed no-fit region.
    However shapely is wishy-washy about closed/open sets and edge cases when the feasible region is a line
    segment or a single point.  Essentially - points on the boundary of the no fit polygon should be considered
    feasible -- it just means that the edges of the shapes will be touching.
    """
    if feasible_height == 0.0 and feasible_width == 0.0:
        if Point(0, 0).within(no_fit_region):
            return Polygon()
        else:
            return Point(0, 0)
    if feasible_height == 0.0:
        feasible_set = LineString([(0, 0), (feasible_width, 0)])
    elif feasible_width == 0.0:
        feasible_set = LineString([(0, 0), (0, feasible_height)])
    else:
        feasible_set = box(0, 0, feasible_width, feasible_height)
    fit_region = feasible_set - no_fit_region
    if not fit_region.is_empty:
        return fit_region
    fit_region_edge = feasible_set.intersection(no_fit_region.boundary)
    return fit_region_edge


def packing_position(
    shape_to_pack, packed_shapes, max_width, max_height, feasible_set_sort_key
):
    """
    Determine the coordinates to translate a shape by so that it won't intersect any of the currently packed shapes.
    Of all the possible places where it could go - we select the one on the bottom left.
    """
    feasible_width = max_width - shape_to_pack.width
    feasible_height = max_height - shape_to_pack.height
    if feasible_width < 0 or feasible_height < 0:
        raise Exception("Failed to pack shapes - insufficient space on the roll")
    # this technique is called the "no fit polygon"
    # see: https://dash.harvard.edu/bitstream/handle/1/25619464/tr-15-94.pdf section 3.3.1 (page 49)
    # The main thing to know is that if A, B are polygons, and A(x), B(y) are their respective translations by vectors x, y:
    # then A(x) and B(y) intersect iff (y - x) is contained in A(0) + (-B(0)) where + is the minkowski sum.
    # We are essentially trying to find the y vector for the current piece B -- so that it wont intersect with any of the currently
    # placed shapes A_1 ... A_i.  So we need to find a y so that for all i: (x_i - y) is not in A_i(0) + (-B(0))
    # or equivalently: y is not in A_i(x_i) + (-B(0))
    # this means that y can't be in anywhere in the union of the minkowski sums of -B with all the currently placed A_i's.
    negated_shape_to_pack = shapely_to_skgeom(negate_shape(shape_to_pack.translated))
    negated_simplified_shape_to_pack = shapely_to_skgeom(
        negate_shape(shape_to_pack.translated_simple_interior)
    )
    # first prune the candidate set using the simplified geometry.
    no_fit_region = unary_union(
        [
            skgeom_to_shapely(
                minkowski_sum(
                    packed_shape.translated_simple_interior_skgeom,
                    negated_simplified_shape_to_pack,
                )
            )
            for packed_shape in packed_shapes
        ]
    )
    feasible_region = get_feasible_set(feasible_width, feasible_height, no_fit_region)
    if feasible_region.is_empty:
        raise Exception("Failed to pack shapes - simplified feasible region is empty")
    # now incrementally build up the true feasible region by computing no fit polygons under the real geometry -- but
    # only in places which are feasible under the simplified geometry -- this way we can potentially avoid computing all the
    # no fit polygons.
    computed_minkowski_sums = set()
    while len(computed_minkowski_sums) <= len(packed_shapes):
        possible_feasible_point = lower_left(feasible_region, feasible_set_sort_key)
        # find all the already packed shapes which could potentially intersect the current shape if we placed it at this point -- and whos
        # true geometry has not already been accounted for in the feasible set.
        feasible_bounding_box = [
            possible_feasible_point[0],
            possible_feasible_point[1],
            possible_feasible_point[0] + shape_to_pack.width,
            possible_feasible_point[1] + shape_to_pack.height,
        ]
        possible_obstructions = [
            i
            for i, s in enumerate(packed_shapes)
            if s.bounds_intersect_box(*feasible_bounding_box)
            and i not in computed_minkowski_sums
        ]
        if len(possible_obstructions) > 0:
            # update the feasible set with the real geometry of those shapes.
            for obstructing_index in possible_obstructions:
                no_fit_region = no_fit_region.union(
                    skgeom_to_shapely(
                        minkowski_sum(
                            packed_shapes[obstructing_index].translated_skgeom,
                            negated_shape_to_pack,
                        )
                    )
                )
                computed_minkowski_sums.add(obstructing_index)
            feasible_region = get_feasible_set(
                feasible_width, feasible_height, no_fit_region
            )
            if feasible_region.is_empty:
                raise Exception("Failed to pack shapes - exact feasible set is empty")
        else:
            # the relaxed feasible point was a real feasible point.
            return possible_feasible_point
    return lower_left(feasible_region, feasible_set_sort_key)


def pack(
    shapes, max_width, max_height, feasible_set_sort_key=FEASIBLE_SET_SORT_KEY_RELAXED
):
    """
    This function just packs the list of shapes onto the area defined by the sheet - by using a greedy
    algorithm that places the shapes in order of decreasing area into the lowest position on the sheet where
    they fit.

    The shapes will not be adulterated by this routine -- meaning that any transformations that need to happen
    (scaling etc) should be done before calling this.

    The return value is a structure containing the input shapes along with their translations and some other info
    about the packing.
    """
    n = len(shapes)
    sheet_area = max_width * max_height
    # this packing is a heuristic where we insert shapes in decreasing order of height
    # breaking ties into decreasing order of area.
    areas = [shape.area for shape in shapes]
    heights = [shape.bounds[3] - shape.bounds[1] for shape in shapes]
    shape_order = sorted(range(n), key=lambda i: [heights[i], areas[i]], reverse=True)
    logger.info(
        f"Packing {n} shapes with total area {np.sum(areas)} into a roll of size {sheet_area}"
    )
    # translate each shape so its local origin is its lower left point.
    packing_shapes = [PackingShape(shape) for shape in shapes]
    packed_shapes = []
    # place each shape.
    for i in range(n):
        index_to_pack = shape_order[i]
        shape_to_pack = packing_shapes[index_to_pack]
        shapes_coordinates = packing_position(
            shape_to_pack, packed_shapes, max_width, max_height, feasible_set_sort_key
        )
        shape_to_pack.translate(shapes_coordinates)
        packed_shapes.append(shape_to_pack)
    # restore original shape order
    ordered_packed_shapes = [None] * n
    for i, j in enumerate(shape_order):
        ordered_packed_shapes[j] = packed_shapes[i]
    return ordered_packed_shapes
