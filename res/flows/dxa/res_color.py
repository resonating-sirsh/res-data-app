"""
Resonance color will be served from a K8s service so it can be previewed ?
res-color/<one>/<piece>

Improvements:
1. We still use the DXF behind the scenes. We need to think about how/when to process things 
because they flows would detect state. For example we have something called a viable surface that is pre computed
by the DXF and stored as data. But suppose we change edge numbering which is in the viable surface, this would be invalidated
this is just an example of how we need to think through what is computed where and knowing when processes need to run

"""

from ast import literal_eval
from numpy.core.defchararray import translate
from shapely.geometry import LineString, Point, MultiPoint, MultiLineString
from shapely.geometry.point import Point
from shapely.ops import nearest_points
from res.flows.meta.pieces import PieceName
from res.media.images.text import get_one_label, get_text_image
import numpy as np
from PIL import Image, ImageDraw, ImageOps
from res.media.images.geometry import (
    get_geometry_keypoints,
    relative_outside,
    scale_shape_by,
    surface_side,
    translate,
    rotate_onto_x_axis,
    geometry_to_image_space,
    get_geometry_keypoints,
    inside_offset,
    relative_inside,
    unary_union,
    edge_curvature_at_point,
    line_angle,
    three_point_angle,
    magic_critical_point_filter,
    determine_outline_bias,
    shift_geometry_to_origin,
)
from res.media.images.providers.dxf import dxf_notches
from res.media.images.outlines import draw_lines, draw_outline
from skimage.draw import polygon2mask
from res.utils import logger
import res
from shapely.wkt import loads as shapely_loads
from res.media.images.icons import *
from res.media.images.qr import encode
import pandas as pd

DEFAULT_DPI = 300
RES_COLOR_FUNCTION_FIELD = "res_color_fn"
OUTLINE_COLUMN = "geometry"


###############
##

# TYPES
NOTCH_SNIP = "snip_notch"
NOTCH_FOLD = "fold_notch"
NOTCH_TOPSTITCH = "top_stitch_notch"
# a conventional or designer driven offset from the corner 1.4 inch towards the COM
NOTCH_POINT_OFFSET = "top_stitch_notch-design_offset_4th"

# ANNOTATIONS - relative is important here as the left or right is ambig (for notches above there are on the notch line unless explicitly offset)
# inche either side depending on context - inch
# the hpyhen separates a position information if it can be determined
# before and after are clowise and inside and outside are relative to the avatar
NOTCH_FUSE_BEFORE = "sa_piece_fuse-before"
NOTCH_FUSE_OUTER_AFTER = "sa_piece_fuse-after"
NOTCH_DIR_INSIDE = "sa_directional-inside"
NOTCH_DIR_OUTSIDE = "sa_directional-outside"
NOTCH_TACK_DIR_INSIDE = "sa_tack_directional-inside"
NOTCH_TACK_DIR_OUTSIDE = "sa_tack_directional-outside"
NOTCH_PULL_TOGETHER = "sa_bidirectional"

# different conventions for sorting notches on edges etc.
NOTCH_ORDER = "edge_order"
# position tips
# place just above, place just above to the f(inside, outside, after, before, above, below)
# place on the viable surface on the inside outside


class PieceImageLabeller:
    """
    The labeller constructs label plans from the piece data
    The plan can be observed as just a collection of geometries and then finally applied to the image
    We place qr codes, one labels and identity symbols at speccpfic places but more fine grained info required for construciton info and notch annotations
    For these we construct a dataframe of objects that can be annotaed with one more more annotations. We use an expand on labels to do multilpes
    Each row of label info provides a viable surface line of some kind
    - we can choose a relative position e.g. before, after, left, inside, outside, etc.
    - there are always two options to choose from
    A label plan knows the label geometry we want to add so it can reserve space to avoid collisions
    When placing in image space we know everything we need e.g.

    The row can contain the notch info or construction info which is res color dervied from DXF + user data
    If the row does not contain it it can be set after

    The strategy for notch labels is to
    (a) pick a notch type which may or may not have an annotation
    (b) add an optional tag where the tag encodes a placement logic e.g. to the inside

    When constructing this for a piece it is assumed pieces already know their body construction and pose info from a dxf parse level + res-color
    Each piece then has dictionaries of data for seam and notch level read rom res color and materilized in the row
    """

    # reference symbol builders
    def __init__(
        self,
        row,
        piece_identity=None,
        one_key="100000",
        geoms_to_image_space=False,
        **options,
    ):
        self._label_plan = []
        self._row = row

        # read options from config
        self._piece_identity = row.get("piece_identity", piece_identity)
        self._piece_key = row.get("res.key")

        self._piece_code = row.get("piece_code")
        # this is important for managing relative symbols -> we pass it to keypoints query
        # symbols are designed to work for a default pose and may need to be flipped for alt pose - e.g. something pointing away from a notch in pose will point towards it in another
        self._piece_pose = row.get("side", "right")
        self._is_inside_piece = row.get("is_inside", False)

        self._edges = (
            geometry_to_image_space(row, "edges")
            if geoms_to_image_space
            else row.get("edges")
        )
        self._corners = MultiPoint(
            magic_critical_point_filter(self._edges)
        )  # .buffer(5)

        self._viable_surface = (
            geometry_to_image_space(row, "viable_surface")
            if geoms_to_image_space
            else row.get("viable_surface")
        )
        self._grain_line = (
            geometry_to_image_space(row, "grain_line")
            if geoms_to_image_space
            else row.get("grain_line")
        )
        self._internal_lines = (
            geometry_to_image_space(row, "internal_lines")
            if geoms_to_image_space
            else row.get("internal_lines")
        )

        self._grain_line_angle = (
            line_angle(self._grain_line) if self._grain_line is not None else 0
        )
        self._kps = get_geometry_keypoints(self._edges, pose=self._piece_pose)
        self._notches = dxf_notches.get_notch_frame(row)

        # some type of seam data structure that we can use to understand viable surfaces and seams where we can do collision tests
        self._internal_lines = self._try_get_relevant_internal_lines(row)

        self._set_notch_label_info(row.get("notch_info", {}))
        self._construction_label_info = row.get("construction_info", {}) or {}
        self._internal_line_label_info = row.get("internal_line_info", {}) or {}

        # bolt on for identity symbol location choice
        self._last_construction_edge = None
        for k, v in self._construction_label_info.items():
            if pd.notnull(v.get("is-last-op")):
                # here we take the edge of the one marked
                self._last_construction_edge = k  # v.get("is-last-op")

        # cook up some functions to sort lines by circuit order conventions
        # TODO: when settled refactor all relative geometric functions and abstraction for piece/rows
        edge_order_fn = dxf_notches.projection_offset_op(row)

        def circuit_relative_after(g):
            points = np.array(g)
            return Point(points[np.argmax(list(map(edge_order_fn, points)))])

        def circuit_relative_before(g):
            points = np.array(g)
            return Point(points[np.argmin(list(map(edge_order_fn, points)))])

        def circuit_relative_from_corner_min(g):
            pt = Point(nearest_points(g, self._corners)[0])
            return pt

        def circuit_relative_from_corner_max(g):
            pt = Point(nearest_points(g, self._corners)[0])
            points = np.array(g)
            # use the point that is closest and then get the point furthest from that point
            return Point(
                points[np.argmax(list(map(lambda p: Point(p).distance(pt), points)))]
            )

        # make relative location ops
        self._relative_location = {
            "inside": relative_inside(self._edges),
            "outside": relative_outside(self._edges),
            "before": circuit_relative_before,
            "after": circuit_relative_after,
            "near_edge_corner": circuit_relative_from_corner_min,
            "near_edge_center": circuit_relative_from_corner_max,
        }

        self._construction_sequence = row.get(
            "body_construction_sequence",
            ["bodice", "bodice", "sleeve", "collar", "bodice", "wildcard"],
        )
        self._construction_label_fn = construction_label(self._construction_sequence)

        self.curvature_fn = edge_curvature_at_point(self._edges)

        # revise contract - this is important object
        self._annotations = self._add_annotations(self._notches, row)

    def _repr_html_(self):
        return self.label_plan.to_html(
            formatters={"label": res.utils.dataframes.image_formatter}, escape=False
        )

    def towards_center_offset(self, g, offset=300 * 0.25):
        """
        experimental: this might not worked as desired for all shapes
        The use cases here is notches can have offsets away from the corner for a wider seam that we want to point to
        away from the corner here is towards the center of mass but it could also be away from the nearest corner if safer
        The distance is in image space the 1/4 inch that a designer might specify
        """
        center = Point(np.array(self._edges).mean(axis=0))

        a = g.parallel_offset(offset, "left")

        b = LineString(reversed(list(g.parallel_offset(offset, "right").coords)))

        return a if a.distance(center) < b.distance(center) else b

    def _try_get_relevant_internal_lines(self, row, min_internal_line_length=500):
        def orient_line(l):
            pts = list(l.coords)
            # we expect the last point to be nearer to the center of mass for internal lines
            if Point(pts[0]).distance(self._kps["center_of_mass"]) < Point(
                pts[-1]
            ).distance(self._kps["center_of_mass"]):
                l = LineString(list(reversed(pts)))
            return l

        try:
            il = row.get("internal_lines")

            if pd.notnull(il):
                # TODO: this is a contract thing that we need to push upstream probably
                if il.type == "LineString":
                    il = MultiLineString([il])

                df = pd.DataFrame(
                    list(self._internal_lines),
                    columns=["geometry"],
                )
                df["length"] = df["geometry"].map(lambda g: g.length)
                df = df[df["length"] >= min_internal_line_length]
                lines = list(df["geometry"].values)

                lines = [orient_line(l) for l in lines]

                return MultiLineString(lines)
        except Exception as ex:
            print("No internal lines loaded", repr(ex))

    def determine_placement(self, r):
        "DEPRECATED"
        try:

            def pd_get(r, x, default=None):
                val = r.get(x)
                if pd.notnull(val):
                    return val
                return default

            func = self._relative_location.get(r.get("position"))

            # we may take an inside offset if there is a dy

            # resolve to a primitive value
            dx = pd_get(r, "dx")
            dy = pd_get(r, "dy", 0.125 * 300)

            # if there is a dx what we do is interpolrate in the direction from the mid point to the target point up to the dx value e.g. a quarter inch in the direction of the target point along the placement line
            pline = r.get("placement_line")
            if not pline:
                # sometimes we know the position but not the line
                return r.get("placement_position")

            if pd.notnull(dy):
                pline = inside_offset(pline, self._row["geometry"], r=dy)

            ct = pline.interpolate(pline.length / 2.0)

            if pd.notnull(func) and pd.notnull(pline):
                pt = func(pline)
                # if there is a dx we interpolate in the direction of the point from center rather than use the point itself
                if dx:
                    l = LineString([ct, pt])
                    pt = l.interpolate(dx)

                return pt
            # by default use the center of the pline
            return ct
        except Exception as ex:
            print("error making placement", repr(ex))

    def _add_annotations(self, notches, row):
        symbols = row.get("annotations")
        if symbols is not None:

            # this is an expansion/projection on all symbols at the notch
            df = pd.merge(
                notches,
                pd.DataFrame(symbols),
                left_on=["nearest_edge_id", "edge_order"],
                right_on=["edge_id", "notch_id"],
            )
            # df["placement_angle"] = df["placement_line"].map(line_angle) + 90
            df["placement_angle"] = df["notch_line"].map(line_angle)
            df["placement_position"] = df.apply(self.determine_placement, axis=1)

            return df
        return None

    def _set_notch_label_info(self, notch_label_info):
        """
        augment notches with info supplied from data
        """
        self._notch_label_info = notch_label_info or {}

        if len(notch_label_info):

            def get_notch_type(row):
                if row["distance_to_edge"]:
                    return None
                key = int(row["nearest_edge_id"]), int(row[NOTCH_ORDER])
                info = self._notch_label_info.get(key)
                if info:
                    return info.get("notch_type")

            def get_notch_annotation(row):
                if row["distance_to_edge"]:
                    return None
                key = int(row["nearest_edge_id"]), int(row[NOTCH_ORDER])
                info = self._notch_label_info.get(key)
                if info:
                    return info.get("annotation")

            if len(self._notches) > 0:
                self._notches["notch_type"] = self._notches.apply(
                    get_notch_type, axis=1
                )
                self._notches["notch_annotation"] = self._notches.apply(
                    get_notch_annotation, axis=1
                )

    def set_construction_label_info(self, construction_label_info):
        self._construction_label_info = construction_label_info

    @property
    def surface_notches(self):
        if self._notches is None or len(self._notches) == 0:
            return pd.DataFrame()
        df = self._notches[self._notches["distance_to_edge"] == 0].reset_index()
        return df.drop("index", 1) if len(df) > 0 else df

    @property
    def internal_line_annotations(self):
        if self._internal_lines:
            for idx, l in enumerate(self._internal_lines):
                if self._internal_line_label_info.get(idx):
                    ilinfo = self._internal_line_label_info.get(idx)
                    # orient the line towards the center of mass
                    # add both the cut annotation or whatever it is and the additional internal line style orientations
                    itype = ilinfo.get("type")
                    if itype:
                        yield {
                            "edge_id": idx,
                            "notch_id": None,
                            "placement_position": l.interpolate(5),
                            "placement_angle": line_angle(l),
                            "category": "internal_line__type",
                            "label": notch_symbol(itype, pose=self._piece_pose),
                            "key": itype,
                        }
                    ilannot = ilinfo.get("annotation")
                    if ilannot:
                        yield {
                            "edge_id": idx,
                            "notch_id": None,
                            "placement_position": l.interpolate(150),
                            "placement_angle": line_angle(l),
                            "category": "internal_line_annotation",
                            "label": notch_annotation(ilannot, pose=self._piece_pose),
                            "key": ilannot,
                        }

    @property
    def construction_annotation(self):
        """
        construction is based on adding things to the viable surface which is a location inside the seam away from notches
        we determine the placement as a function of the size of the label being placed on a viable segment
        we interpolate along the viable surface which is assumed big enough to fit construction labels

        """

        center = self._kps["center_of_mass"]
        placed_construction = {}
        # iterate over the viable surface and look for seam construction info for that edge
        # we are taking the inside offset of the viable surface here - might not be the best place but need to think about what seam info we have when
        # going in an eith of an inch for now - this is a paramter dfault somewhere overriden at seam level or piece level
        offset_viable_surface = inside_offset(
            self._viable_surface, shape=self._edges, r=0.125 * 300
        )
        for i, g in enumerate(offset_viable_surface):

            edge_id = int(list(g.coords)[0][-1])
            for cinfo in self._construction_label_info:

                # this model loops through all edge annotations but assuming one for now
                # we try to place the full edge info on the edge which may be a composite
                if cinfo("edge_id") == edge_id and edge_id in placed_construction:
                    order = cinfo.get("body_construction_order")

                    if pd.isnull(order):
                        order = None

                    op_name = cinfo.get("operation")
                    is_last = cinfo.get("is_last")
                    offset = cinfo.get("offset", 0)
                    label = (
                        self._construction_label_fn(order, op_name=op_name)
                        if order
                        else op_symbol(op_name)
                    )
                    label_width = label.size[0]

                    if (
                        g.length < label_width
                    ):  #: label_width + 50: #hard coded buffer for now -> we can trick to preserve smaller spaces for other things by keeping bigger spaces for larger labels for now until collision detection is better
                        continue

                    pp = g.interpolate(offset)
                    ce = g.interpolate(offset + label_width / 2.0)
                    Fe = g.interpolate(offset + label_width)

                    three_points = [pp, ce, Fe]
                    curve_sign = 1
                    # if the curve is convex it bends out of the shape and we use the negative sign
                    # wew get the chord of the extremes of the three points and compare with e point interpolated around the curve itself
                    # the chord is the shortest distance between the points and will be inside or outside the shape
                    # add r-200 for a more exaggerated inside offset
                    chord = LineString([pp, Fe])
                    if chord.interpolate(chord.length / 2, r=200).distance(
                        center
                    ) > ce.distance(center):
                        curve_sign = -1

                    # best approx of chord angle
                    placement_angle = np.rad2deg(np.arctan2(Fe.y - pp.y, Fe.x - pp.x))

                    yield {
                        "edge_id": edge_id,
                        "notch_id": None,
                        "placement_position": ce,
                        "placement_angle": placement_angle,
                        "curvature": curve_sign
                        # small angle is signed for concavity or convexity with respect to the shape
                        * (180 - round(three_point_angle(*three_points))),
                        "category": "construction",
                        "label": label,
                        "key": None,
                    }

                    placed_construction[edge_id] = i

    @property
    def notch_annotations(self):
        """
        notch annotations are added to notches using info about notch locations and angles of nearby surfaces
        we return both type and annotations for notches which are labelled added to conventional offset locations
        """
        for _, row in self.surface_notches.iterrows():
            ntype = row.get("notch_type")
            nannotation = row.get("notch_annotation")
            edge_id = int(row["nearest_edge_id"])
            notch_id = row[NOTCH_ORDER]
            if ntype:
                l = row["notch_line"]

                pt = Point(list(l.coords)[-1])

                # TODO: temp exception: we just pick a fixed quart inch offset for this notch type
                # in practice there will be some kind of designer input data with this type of info
                # this shows we need find grained specs for some of these measurements upstream
                if ntype == NOTCH_POINT_OFFSET:
                    pt = Point(list(self.towards_center_offset(l).coords)[-1])

                yield {
                    "edge_id": edge_id,
                    "notch_id": notch_id,
                    "placement_position": pt,
                    "placement_angle": line_angle(l),
                    "category": "notch_type",
                    "label": notch_symbol(ntype, pose=self._piece_pose),
                    "key": ntype,
                }
            if nannotation:
                # adding a default inside to save typing
                position = (
                    nannotation.split("-")[-1]
                    if "-" in nannotation
                    else None or "inside"
                )
                l = row["placement_line"]
                yield {
                    "edge_id": edge_id,
                    "notch_id": notch_id,
                    "placement_position": self._relative_location[position](l),
                    "placement_angle": line_angle(l),
                    "category": "notch_annotation",
                    "label": notch_annotation(nannotation, pose=self._piece_pose),
                    "key": nannotation,
                }

    @property
    def label_plan(self):
        return pd.DataFrame([d for d in self._label_plan])

    @staticmethod
    def get_placement_point_and_angle(nrow, depth, position):
        """
        nrow is a dictionary of notch information e.g. the point, the surface. etc
        """
        # get the notches surface
        # create a buffer on the surface and create its inside offset at depth = this line goes either side of the notch
        # choose the point on that line described by position
        # get the angle of the placement line - for certain annotations we point e.g. up when we place the actual label but that is relative e.g. 90 to the placement location

    @property
    def identity_symbol(self):

        return identity_label(
            self._piece_code, pose=self._piece_pose, is_inside=self._is_inside_piece
        )

    @property
    def one_label(self):
        return Image.fromarray(get_one_label("1000000"))

    @property
    def qr_code(self):
        pass

    def _get_construction_label(self, **kwargs):
        pass

    def _get_notch_label(self, **kwargs):
        pass

    def _get_notch(
        self, edge_id, index, distance_from_outside=0, sort=NOTCH_ORDER, plot=True
    ):
        """
        Example:
            n = pm._get_notch( 0, 6)
            pl = n['placement_line']
            fn = relative_inside(pm._edges)
            GeoDataFrame([fn(pl)] + [n[g] for g in ['geometry', 'placement_line', 'notch_line' ] ],columns=['geometry']).plot()
            plt.gca().invert_yaxis()
            plt.axis("off")

        """
        N = self.surface_notches
        N = N[N["nearest_edge_id"] == edge_id]
        return dict(N[N[sort] == index].iloc[0])

    @staticmethod
    def curve_image(im, angle):
        """
        testing with small angle
        """

        is_big_angle = angle < 0
        from wand.image import Image as WImage

        # we flip e.g. text because we are going to flip it back
        # its a flat image so this does not change the view port but the curved image below does
        if is_big_angle:
            im = ImageOps.flip(im)

        # experiment
        factor = 3
        angle *= factor

        WI = WImage.from_array(np.asarray(im))
        WI.virtual_pixel = "transparent"
        WI.distort("arc", (abs(angle),))
        im = np.asarray(WI)
        # simple alpha recovery for test

        im = Image.fromarray(im)

        if is_big_angle:
            im = ImageOps.flip(im)

        # remove noise and respect alpha channels

        return im

    def _place_label(
        self, im, label, placement_position=None, directional_label=False, **options
    ):
        """
        Actual image space label placement from metadata
        """

        back_color = options.get("background_color", "white")
        label_width = label.size[0]
        offset = options.get("offset", 0)
        # placement_side = options.get("placement_side", 'center')
        placement_angle = options.get("placement_angle", 0)
        curvature = options.get("curvature")
        piece_side = options.get("piece_side")

        if not directional_label:
            # the placement angle is a bit arbitrary here - its like saying if we are in the bottom of the piece
            # or the label text is starting to look upside down
            if piece_side == "bottom" or placement_angle > 120:
                label = ImageOps.mirror(label)
                label = ImageOps.flip(label)

            # elif piece_side == "right":
            #     label = ImageOps.flip(label)

            # if abs(placement_angle) > 90:
            #     label = ImageOps.mirror(label)
            #     if placement_angle < -90:
            #

        # add a label background before rotate and curbe
        background = Image.new("RGBA", label.size, back_color)
        background.paste(label, (0, 0), mask=label)
        label = background

        CURVE_THRESHOLD = 3
        if curvature is not None:
            if abs(curvature) > CURVE_THRESHOLD:
                res.utils.logger.info(
                    f"LEGACY curving label outside curve threshold: {abs(curvature)} > {CURVE_THRESHOLD}"
                )
                label = PieceImageLabeller.curve_image(label, curvature)

        # PIL rotation is counter clockwise and we computed clockwise angles here
        label = label.rotate(
            -1 * placement_angle,
            expand=True,
        )

        pp = placement_position
        # important recalc
        label_width_offset = label.size[0] / 2.0
        label_height_offset = label.size[1] / 2.0
        pp = (int(pp.x - label_width_offset), int(pp.y - label_height_offset))

        im.paste(label, pp, mask=label)

        return im

    def _apply_internal_lines(self, pm, width=5, fill="gray"):

        # todo - make sure always multiple lines
        if self._internal_lines is not None:
            d = ImageDraw.Draw(pm)
            for l in self._internal_lines:
                if l.type == "LineString":
                    l = [tuple(l[:2]) for l in l.coords]
                    d.line(l, fill=fill, width=width)

        return pm

    def _apply_plan_to_image(self, im, plan=False):
        self._plan_placement()

        if plan == True:
            return self.label_plan

        convert_back = False
        if isinstance(im, np.ndarray):
            im = Image.fromarray(im)
            convert_back = True

        for p in self._label_plan:
            label = p.get("label")
            directional = p.get("category") == "notch_type"
            if label is not None:
                self._place_label(im, directional_label=directional, **p)

        self._apply_internal_lines(im)

        # TODO draw some sew lines

        return im if not convert_back else np.asarray(im)

    def __call__(self, im, plan=False):
        return self._apply_plan_to_image(im, plan=plan)

    def _plan_placement(self):
        # reset
        self._label_plan = []

        # for the angle to be 0 for identity symbols probably
        # TODO - when we know the identity key we could add it as the key here
        self.add(
            self.identity_symbol,
            edge_preference=self._last_construction_edge,
            position="outside_bottom",
            placement_angle=0,
            category="identity",
            key="TODO-identity-key",
        )
        # self.add(self.qr_code, position='inside_bottom', category="qr")
        for c in self.construction_annotation:
            self.add(**c)
        for n in self.notch_annotations:
            self.add(**n)
        for n in self.internal_line_annotations:
            self.add(**n)
        # this gets add in free space where other symbols are least likely e.g. outside middle
        # self.add(self.one_label, category="one_label", position="free_viable_surface")

    def add(self, label, **kwargs):
        """
        can simplify this as we are now just taking kwargs with defaults
        """
        # print("add kwargs", kwargs)
        edge_id = kwargs.get("edge_id")
        notch_id = kwargs.get("notch_id")
        # the relative position on the segment - can be an integer offset or textual describe relative to keypoints on the shape
        position = kwargs.get("position")
        placement_position = kwargs.get("placement_position")
        # the angle of a section of the viable segment
        placement_angle = kwargs.get("placement_angle")
        # the location of the label that is recorded for collision tests - takes into account label dimensions, position and angle
        edge_preference = kwargs.get("edge_preference")

        assert not (
            position is None and placement_position is None
        ), "either a position or a placement position is required"

        # both of these can probably be dine construction label
        # TODO - compute curvature for construction category because the labels are longer - maybe for the one number too
        # viable surface iterators can still be used here if position is "viable_surface" <- the edge id must also be known in this case

        if pd.notnull(edge_preference):

            corners = MultiPoint(magic_critical_point_filter(self._edges)).buffer(10)
            eds = self._edges - corners
            com = Point(np.array(self._edges).mean(axis=0))

            for e in eds:
                if list(e.coords)[0][2] == edge_preference:
                    placement_position = Point(list(e.coords)[0][:2])
                    # this only makes sense of we are in a sort of top and bottom mode TODO: temp
                    towards = com  # Point(placement_position.x, com.y)

                    # interpolate into the center a little
                    placement_position = LineString(
                        [placement_position, towards]
                    ).interpolate(40)

                    # some exceptions as im SICK of this
                    # the only way to fix this is to add a layer of abstraction to irregular piece shapes
                    if self._piece_key == "CC-6001-V9-DRSCLRTOP-BF_SM":
                        placement_position = Point(
                            placement_position.x, placement_position.y + 30
                        )

                    if "DRSCLRSTD" in self._piece_key:
                        placement_position = Point(
                            placement_position.x - 30, placement_position.y
                        )

                    if "DRSPKTBAGUR" in self._piece_key:
                        placement_position = Point(
                            placement_position.x + 20, placement_position.y + 20
                        )

                    if "DRSPKTBAGUL" in self._piece_key:
                        placement_position = Point(
                            placement_position.x - 20, placement_position.y
                        )

                    if "DRSPKTBAGTOP" in self._piece_key:
                        placement_position = Point(
                            placement_position.x - 20, placement_position.y
                        )

                    break

        # attempted default - with only piece level info
        # this determines identity image placement interpolated towards the center say
        if placement_position is None:
            pl = self.get_relative_placement_line(position, edge_preference)
            # this interpolation places symbols like identity and qr
            placement_position = pl.interpolate(1 * pl.length / 4.0)
            placement_angle = kwargs.get("placement_angle", line_angle(pl))

        self._label_plan.append(
            {
                "edge_id": edge_id,
                "notch_id": notch_id,
                "placement_position": placement_position,
                "placement_angle": placement_angle,
                "label": label,
                "category": kwargs.get("category"),
                "key": kwargs.get("key"),
                "curvature": kwargs.get("curvature"),
                "piece_side": surface_side(placement_position, self._edges),
            }
        )

    def get_relative_placement_line(
        self, position="outside_bottom", edge_preference=None
    ):
        RADIAL_BUFFER = 100
        pt = self._kps[position]
        corners = MultiPoint(magic_critical_point_filter(self._edges)).buffer(10)

        L = pt.buffer(RADIAL_BUFFER).intersection(self._edges) - corners
        L = list(L) if not isinstance(L, list) and not L.type == "LineString" else [L]
        if len(L) > 1:
            angles = list(map(line_angle, list(L)))
            # flatter angle
            idx = np.argmax([abs(90 - abs(a % 180)) for a in angles])
            L = L[idx]
        else:
            L = L[0]
        # get an inside offset
        L = inside_offset(L, self._edges, r=300 * 0.175)
        return L


##
############


def load_piece_dxf(piece_key, **kwargs):
    """
    get the piece DXF info at 300 dpi for this unique style piece
    """


def project_one_number_to_symbol_space(one_number, hypercube_volume=10**4, p=4231):
    """
    This is an example of mapping an integer string represention of a ONE number onto a 4 digit code in the space 10^4 so as to space out consecutive numbers in symbol space
    The intuition is for any number n, find a "permutation by power" in the space

    p is just "".join([str(i) for i in range(D,0,-1)]) for any D

    """
    f = lambda n: (p**n - n) % hypercube_volume
    n = int(one_number) % hypercube_volume
    return str(f(n)).zfill(4)


def _get_label_samples(**kwargs):
    for i in range(10):
        # for j in range(10):
        text = f"1000{i}{i}{i}{i}"
        yield get_one_label(text, **kwargs)


def _sample_color_rgb(im, sampling_polygon_inches, **kwargs):
    """
    Samples color in a region of the target image and generates a color that is some function of the colour in that region
    Currently this is a dumb inverted color which is not really what we want - we can configure any function to generate the new color
    The new color is used as a background color for a label placed on a piece
    """

    if kwargs.get("label_background_color"):
        return kwargs.get("label_background_color")

    bounds = (im.shape[0], im.shape[1])
    sampling_outline = np.array(sampling_polygon_inches.exterior.coords)
    assert not np.any(
        sampling_outline < 0
    ), "The sampling polygon has negative coordinates. Check that the geometries for the overlay are in the correct CRS"

    # create the mask from the outline - swap for image space
    sampling_outline = sampling_outline[:, [1, 0]]

    # do we need to invert this / what assetions are needed here on the geom

    # sampling_outline = invert_y_axis(sampling_outline, order="yx", bounds=bounds)

    # using the mask of the entire canvas, use the origin offset sampling polygon as a mask
    #  we have scaled it above to image space and mapped to x,y->y_inv,x
    mask = polygon2mask(bounds, sampling_outline)

    #    return (255, 255, 255, 255)

    scaling = 1 if str(im.dtype) == "uint8" else 255
    # can also return RGB scaled but just stay in whatever format we are in
    mean = (im[mask].mean(axis=0) * scaling).astype(int)

    # bounds check but this is a sign that we have gone somewhere illegal
    mean[mean < 0] = 255

    return tuple(np.append(255 - mean[:-1], 255))


# in res color
def overlay_factory(row):
    """
    a row dict is kwargs from an asset request - if we have the data in a row for this contract with can make the overlay function

    Usage

        row = ... load for an from a dxf for some res.key for a piece called key

        piece_image =  s3.read(f"{piece_image_root}/{key}.png")

        f = overlay_factory(row)
        im = f(piece_image)
        plt.imshow(im)

    """
    # support evolving contract
    res_color_info = row.get(
        "resonance_color_inches", row.get("resonance_color_inches")
    )

    outline_prescale_factor = row.get("outline_prescale_factor")

    label = get_one_label(str(row["key"]))
    # from the dxf this is what we nested - the nesting result must have a defined orientation TODO document this convention
    # it is setup so we can viz things in nested dataframes and may not have same orientation of what came out of DXF
    outline = row.get(OUTLINE_COLUMN)
    # outline = invert_axis(outline)
    # outline = swap_points(outline)

    def _f(im, use_images_outline=True):
        # pass None to use the image's outline
        im = draw_outline(im, outline if not use_images_outline else None)

        return place_label(
            im, label, res_color_info, outline_prescale_factor=outline_prescale_factor
        )

    return _f


def get_outline_overlay_function(row):
    def _f(im):
        return draw_outline(im)

    return _f


def get_overlay_function(row):
    """
    We use the metadata to create an overlay function for each piece row
    The overlay knows how to add labels to apiece using context from the dxf file
    """

    def _f(im):
        label = get_one_label(row["key"])
        # just a convenient polymorphic trick
        if isinstance(im, str):
            im = res.connectors.load("s3").read(im)
        # TODO: apply the negative overlay i.e. transparent masks if relevant
        im = draw_outline(im, row[OUTLINE_COLUMN])
        # TODO: if labels were a map we can add lots of them using the full resonance color
        return place_label(im, label, row["resonance_color"])

    return _f


def add_overlay_functions_to_data(data, geometry_column="outline_300_dpi"):
    """
    Input data contains things like
    {
        key (ne number)
        piece_key
        outline_image_space
        res_color_metadata
        label props
    }

    these are added by the DxfFile annotate asset sets function

    """
    s3 = res.connectors.load("s3")

    data[RES_COLOR_FUNCTION_FIELD] = data.apply(get_overlay_function, axis=1)

    return data


def place_label(
    im: np.ndarray, label: np.ndarray, location_info, **kwargs
) -> np.ndarray:
    """
    Given a generated label with transparent background
    Use its size when generating label info and sample a background color

    To place a label we use an edge vector (geometry) and angle.
    Because shapes are rotated around center we need a translation too which is the corner translation

    Location info should be heavily tested as the rest of this is just Image manip
    We also use the sample ROI which is a buffered region around the centerline where the image would be placed

    Experimenting with confining the image space transformations to here after getting origin-dxf data in inches
    - swap axis on the sampling polygon
    - add 90 degrees to the placement angle to rotate anti-clock onto the x-axis
    - swap the axis on the placement geometry

    res_color contract
    - color_sampling_mask_region
    - label_corners
    - angle
    """

    # for now supporting this so it does not matter how it is serialized but we will take more with the contract in future
    if isinstance(location_info, str):
        location_info = literal_eval(location_info)
    if isinstance(location_info["label_corners"], str):
        location_info["label_corners"] = shapely_loads(location_info["label_corners"])
    if isinstance(location_info["piece_bounds"], str):
        location_info["piece_bounds"] = literal_eval(location_info["piece_bounds"])

    # get location info including sample roi for the
    corners = location_info["label_corners"].convex_hull
    piece_bounds = location_info["piece_bounds"]
    # location info must be offset by the piece bounds for image space and scaled by DPI

    # move to the origin after scaling
    corners = translate(corners, xoff=-1 * piece_bounds[0], yoff=-1 * piece_bounds[1])

    fold_bounds = location_info.get("fold_aware_edge_bounds")
    if fold_bounds:
        # for now at least
        fold_bounds = literal_eval(fold_bounds)
        if fold_bounds[0] < 0:
            corners = translate(corners, xoff=-1 * fold_bounds[0])
        if fold_bounds[1] < 0:
            corners = translate(corners, yoff=-1 * fold_bounds[1])

    # scale to image space/rotate to image space
    corners = scale_shape_by(DEFAULT_DPI)(corners)
    corners = rotate_onto_x_axis(corners)

    # sample color in image space
    sample_color = _sample_color_rgb(im, corners, **kwargs)
    logger.debug(f"sampled a colour {sample_color} in {corners}")
    _ = kwargs.get("dxa_label_padding", 2)
    dxa_label_outline_width = kwargs.get("dxa_label_outline_width", 2)

    # the shape is designed to be the same as the transparent labels which are placed into the space
    shape = (label.shape[1], label.shape[0])
    IM = Image.new("RGBA", shape, sample_color)
    draw = ImageDraw.Draw(IM)
    if dxa_label_outline_width:
        draw.rectangle(
            ((0, 0), shape[0] - 1, shape[1] - 1),
            outline="black",
            width=dxa_label_outline_width,
        )

    label = Image.fromarray(label)

    # the placement point is just an array in the original CRS of inches, origin and xy
    # the corners are just the swapped points
    # corners = swap_points(location_info["label_corners"])
    corners = np.array([[p[0], p[1]] for p in list(corners.exterior.coords)])
    # we always choose the min corner in image space as the palcement, wherever that corner is
    placement_location = tuple(
        np.array([corners[:, 0].min(), corners[:, 1].max()]).astype(int)
    )

    pre_scale = kwargs.get("outline_prescale_factor")
    if pre_scale is not None and (pre_scale[0] != 1.0 or pre_scale[1] != 1.0):
        logger.debug(f"linearly scaling a buffered piece yx = {pre_scale}")
        placement_location = (
            int(placement_location[0] * pre_scale[1]),
            int(placement_location[1] * pre_scale[1]),
        )

    placement_location = (placement_location[0], im.shape[0] - placement_location[1])
    logger.debug(
        f"Scaled placement_location {placement_location} in topmost leftmost corner of image placement, im.shape {(im.shape[1], im.shape[0])}. We will offset for image space"
    )

    # image is yx and we are in xy (both CRS and how we place in PIL) - we trust our x as we look at the image and we invert the y
    # placement_location = (placement_location[0], placement_location[1])

    placement_angle = (90 - float(location_info["angle"])) % 180.0
    # TODO assert that the label corners area all contained in the piece - after rotation somethign can extend outside a corner for example

    logger.debug(
        f"placing label of size {shape} in image of shape {im.shape} at {placement_location} at angle {placement_angle}"
    )

    IM.paste(label, (0, 0), mask=label)
    IM = IM.rotate(
        placement_angle,
        expand=True,
    )

    # if its actually a float -im not sure what is the way to handle this but this cannot be necessary
    PIM = (
        Image.fromarray((im * 255).astype("uint8"))
        if not str(im.dtype) != "unit8"
        else Image.fromarray(im)
    )

    PIM.paste(IM, placement_location, mask=IM)

    # interface should generally be an array and not a PIL - ndarray in ndarray out
    return np.asarray(PIM)


def place_symbol_on_segment(
    im,
    label,
    g,
    offset=10,
    placement_angle=0,
    center=None,
    sense=None,
    metadata={},
):
    """
    - Need to test this for either sense - the sense will probably be inherited from the piece e.g. left and right pieces
    - we should abstract the piece pose understanding but fine here for this week
    - g is expected to tbe some center line for a seam and the label is replaced and rotated right on it and the interpolated offset

    The "right" pieces are in the sense such that traversal goes from outside in; we need to take parallel offset in the correct direction
    The "left" reflected pieces are in the opposite sense so that traversal is also from outside in and then we parallel offset on the other side reversed
    Either way, we can choose the first or last segment in the edge id on a partcular side according to this sense

    What it does not do:
    decide orientations of lines etc. assumes caller has id'd a line and knows its length and how far to offset placements
    For example if we have an edge with three segments and we want to place on the "outside" of a shape, we should
    identify the right segment g, and then if necessary offset so the label is placed on the right or left as required.
    The label manager needs to do this becasue there can be multiple lables with different lengths etc and this function should not worry about that
    """

    def quadrant(a):
        if abs(a) > 90:
            return 3 if a < 0 else 2
        else:
            return 0 if a < 0 else 1

    # todo
    label_width = label.size[0]
    label_height = label.size[1]

    PIM = (
        Image.fromarray((im * 255).astype("uint8"))
        if not str(im.dtype) != "unit8"
        else Image.fromarray(im)
    )

    # todo implement invert interpolrate on the reverse of the line
    pp = g.interpolate(offset)
    ce = g.interpolate(offset + label_width / 2.0)
    Fe = g.interpolate(offset + label_width)

    # best approx of chord angle
    placement_angle = np.rad2deg(np.arctan2(Fe.y - pp.y, Fe.x - pp.x))

    quad = quadrant(placement_angle)

    # piece orient angle
    angle = np.arctan2(pp.y - center[1], pp.x - center[0])
    if angle < 0:
        angle += 2 * np.pi
    angle = round(np.rad2deg(angle), 2)

    # PLACE AT CENTER!!!!!!!!!!!!!!!!!!!!!!!!!!!
    pp = ce

    # this is a stand in rule - basically we want to know if its on the baseline of the image roughly
    # if angle > 35 and angle < 135:
    #     label = ImageOps.flip(label)
    #     label = ImageOps.mirror(label)

    if abs(placement_angle) > 90:
        label = ImageOps.flip(label)
        label = ImageOps.mirror(label)

    # PIL rotation is counter clockwise and we computed clocwise angles here
    label = label.rotate(
        -1 * placement_angle,
        expand=True,
    )
    # recalc for offset
    # i spent whole morning realising i needed to move this down from where it was above
    # the expand on the rotate means the label offset after rotations around the center but placement at top left uses the expanded offset
    # obvious once you know!
    label_width_offset = label.size[0] / 2.0
    label_height_offset = label.size[1] / 2.0
    pp = (int(pp.x - label_width_offset), int(pp.y - label_height_offset))
    PIM.paste(label, pp, mask=label)

    if metadata:
        metadata[pp] = round(placement_angle, 2), f"Q{quad}", f" {angle}"

    return PIM


def res_plus_overlay_factory(row):
    def f(im):
        return label_viable_surface(im, row)

    return f


def unpack_res_color(data):
    """
    Fow now as we work through the interface the resonance color is in this format but eventually it will be a node on the graph
    """
    from res.flows.make.assets import bodies
    import pandas as pd

    k = data.iloc[0]["one_specification_key"]

    # for each body - just test one here but can easily do a multi key look up
    lu_data = bodies.get_one_spec_pieces(k, unmarshall_geometry=True)
    # take any new geometry cols from the lookup

    # TODO: contracts
    geoms = [
        c
        for c in ["geometry", "notches", "internal_lines", "viable_surface"]
        if c in lu_data.columns and c not in data.columns
    ]
    data = pd.merge(data, lu_data[["body_piece_key"] + geoms], on=["body_piece_key"])

    data["res.key"] = data["body_piece_key"]

    return data


def try_get_construction_symbol(row, seam_id):
    # hard code for cc-c6001 and then look for instructions on the edges of the viable surface
    # this think can be read from the body level and override
    construction_label_fn = construction_label(
        ["bodice", "bodice", "sleeve", "bodice", "collar", "bodice", "wildcard"]
    )

    info = row.get("seam-construction", {}).get(seam_id)

    if info:
        order = info.get("order")
        op_name = info.get("op")
        # if there is an order > 0 we show the full construction symbols otherwise just add the op
        return (
            construction_label_fn(order, op_name=op_name)
            if order
            else op_symbol(op_name)
        )

    return None


def label_viable_surface(image, row, plot=False, return_pil=False):
    """
    Add resonance color
    Here and in other functions we should have a strict mode setting for gated runs i.e. first time runs
    When strict we do collision tests on new bodies etc but when strict mode is off we do not

    Notes: adding constraints will be useful for testing eg.. dont add on a segment with certain length or curvature etc.
    That we can test placements and possibly patch things for problematic bodies

    """
    row["key"] = "1000000"
    temp_piece = row.get("res.key", row.get("body_piece_key"))

    g = row.get("geometry")
    com = np.array(g).mean(axis=0)
    vs = inside_offset(row.get("viable_surface"), shape=g)
    kp = get_geometry_keypoints(g)

    symbols_on_seams = {}
    # the seam ids are different to the index i of the surface
    for i, g in enumerate(vs):
        if "CC-6001-V8-BELT" in temp_piece and i != 1:
            continue

        seam_id = int(list(g.coords)[0][-1])

        # logger.debug(f"SEAM {seam_id}, segment {i} length {g.length} ")

        # temp while adding bigger sew instructions
        if g.length < 400:
            continue

        symbol = try_get_construction_symbol(row, seam_id) or get_number_as_dice(
            seam_id
        )

        if symbols_on_seams.get(seam_id) is None:
            pm = place_symbol_on_segment(image, symbol, g, center=com)
            symbols_on_seams[seam_id] = i
        # for completeness but we need to optimize this function
        image = np.asarray(pm)

    # one label
    one_label = Image.fromarray(get_one_label(row["key"]))
    one_placed = False
    for i, g in enumerate(vs):
        seam_id = int(list(g.coords)[0][-1])
        if g.length > 150 and not one_placed:  # seam_id in [0, 1] and
            # note doing this to keep ones away from sew symbols
            if symbols_on_seams.get(seam_id) != i:

                # choose an offset with respect
                pm = place_symbol_on_segment(image, one_label, g, offset=0, center=com)
                image = np.asarray(pm)
                one_placed = True
                # logger.info(f"placing on seam {seam_id}.{i}")
                break

    # qr code
    for i, g in enumerate(vs):
        if i == 1:  # and g.length > 150:
            qrc = encode(row["res.key"]).resize((80, 80))
            pm = place_symbol_on_segment(image, qrc, g, offset=80, center=com)
            image = np.asarray(pm)
            break

    # identity
    pt = tuple(
        np.array(LineString(kp["outside_bottom-centroid"]).interpolate(55)).astype(int)
    )

    # label = identity_label(dict(row).get("identity", {}))
    # pm.paste(label, (pt[0], pt[1]), mask=label)
    try:
        int_lines = geometry_to_image_space(row, "internal_lines")

        if int_lines is not None:

            logger.info(
                f"drawing internal lines - expect multi line string and type is {int_lines.type}"
            )
            d = ImageDraw.Draw(pm)
            # todo - make sure always multiple lines
            if int_lines.type == "LineString":
                int_lines = [int_lines]
            for l in int_lines:
                if l.type == "LineString":
                    l = [tuple(l[:2]) for l in l.coords]
                    d.line(l, fill="gray", width=5)
    except Exception as ex:
        logger.warn(f"Failed to draw internal lines {repr(ex)}")

    return draw_outline(pm, return_pil=return_pil)


"""
migrating to static label functions - may move these down to images
"""


def place_label_from_options(
    im, label, placement_position=None, directional_label=False, **options
):
    """
    Actual image space label placement from metadata
    """

    # print(options)
    # white or transparent
    SHIFT_MARGIN = 5  # options.get("shift_margin", 0)
    TRANSPARENT = (0, 0, 0, 0)
    WHITE = (255, 255, 255, 255)
    back_color = options.get("background_color", WHITE)
    label_width = label.size[0]
    offset = options.get("offset", 0)
    # placement_side = options.get("placement_side", 'center')
    placement_angle = options.get("placement_angle", 0)
    curvature = options.get("curvature")
    piece_side = options.get("piece_side")

    if not directional_label:
        # the placement angle is a bit arbitrary here - its like saying if we are in the bottom of the piece
        # or the label text is starting to look upside down

        if piece_side == "bottom" or placement_angle > 120:
            label = ImageOps.mirror(label)
            label = ImageOps.flip(label)

    # add a label background before rotate and curbe
    background = Image.new("RGBA", label.size, back_color)
    background.paste(label, (0, 0), mask=label)
    label = background

    piece_side = options.get("piece_side")

    CURVE_THRESHOLD = 2
    if curvature is not None:
        if abs(curvature) > CURVE_THRESHOLD:
            res.utils.logger.info(
                f"curving label outside curve threshold: {abs(curvature)} > {CURVE_THRESHOLD}"
            )
            pre_size = label.size
            label = PieceImageLabeller.curve_image(label, curvature)
            after_size = label.size

    if abs(curvature or 0) >= 7:
        factor = 0.6
        res.utils.logger.info(
            f"because the curvature is {curvature} we will shrink the image to {factor} its size"
        )
        new_size = (int(label.size[0] * factor), int(label.size[1] * factor))
        label = label.resize(new_size)
        # we can also do an outside offset or move closer to the surface

    # PIL rotation is counter clockwise and we computed clockwise angles here
    if placement_angle:
        label = label.rotate(
            -1 * placement_angle,
            expand=True,
        )

    pp = placement_position
    # important recalc
    # margin is for something related to seam labels but not sure what
    MARGIN = (
        SHIFT_MARGIN  # if not curvature or abs(curvature) < 15 else curvature * 1.2
    )

    label_width_offset = (label.size[0] / 2.0) - MARGIN
    label_height_offset = (label.size[1] / 2.0) - MARGIN
    # for excessive curvature we might want to do something here
    #

    if not pd.isnull(pp):
        pp = (int(pp.x - label_width_offset), int(pp.y - label_height_offset))
        res.utils.logger.debug(f"Placing label at {pp}")
        im.paste(label, pp, mask=label)
    else:
        res.utils.logger.warn(f"The placement position could not be determined!!!")

    return im


def place_label_on_image(
    im, label, placement_position=None, directional_label=False, **options
):
    """
    This is a streamline version of the above - we will probably improve this and deprecate the others
    """

    # white or transparent
    MARGIN = 0  # options.get("shift_margin", 0)
    TRANSPARENT = (0, 0, 0, 0)
    WHITE = (255, 255, 255, 255)
    back_color = options.get("background_color", WHITE)
    label_width = label.size[0]
    label_height = label.size[1]
    offset = options.get("offset", 0)
    # placement_side = options.get("placement_side", 'center')
    placement_angle = options.get("placement_angle", 0)
    curvature = options.get("curvature")
    piece_side = options.get("piece_side")

    if not directional_label:
        # the placement angle is a bit arbitrary here - its like saying if we are in the bottom of the piece
        # or the label text is starting to look upside down
        if piece_side == "bottom" or placement_angle > 120:
            label = ImageOps.mirror(label)
            label = ImageOps.flip(label)

            if piece_side == "bottom":
                # print("shifting up the placement position at the bottom")
                placement_position = Point(
                    placement_position.x, placement_position.y - label.size[1]
                )
        if piece_side == "top":
            # print("shifting up the placement position at the top")
            placement_position = Point(
                placement_position.x, placement_position.y - label.size[1]
            )

    # add a label background before rotate and curbe
    background = Image.new("RGBA", label.size, back_color)
    background.paste(label, (0, 0), mask=label)
    label = background
    piece_side = options.get("piece_side")

    #     # PIL rotation is counter clockwise and we computed clockwise angles here
    if placement_angle:
        label = label.rotate(
            -1 * placement_angle,
            expand=True,
        )

    pp = placement_position

    # this is an offset to allow rotate around the placement position rather than center - we translate
    # we use the original label width and height before the rotational expansion of the image
    label_width_offset = 0  # (label_width / 2.0) - MARGIN
    label_height_offset = 0  #  (label_height /2.0) - MARGIN

    if not pd.isnull(pp):
        pp = (int(pp.x - label_width_offset), int(pp.y - label_height_offset))
        res.utils.logger.debug(
            f"Placing label at {pp}. the angle is {placement_angle} and the size went from {label_width, label_height}->{label.size}"
        )
        im.paste(label, pp, mask=label)
    else:
        res.utils.logger.warn(f"The placement position could not be determined!!!")

    return im


def place_label_on_edge_segment(
    im,
    label,
    g,
    offset,
    fit_to_line=True,
    inside_offset_fn=None,
    piece_side_function=None,
    plan=False,
    show_metadata=False,
    offset_center=None,
    seam_allowance=None,
    surface_geometry_offset_function=None,
):
    """
    if we are fitting to the line we can use the angle and just place properly
    otherwise we need to make sure the label is sitting with gravity but totally inside the shape without angle or curvature
    we always interpolate along the line according to offset
    """

    if seam_allowance and seam_allowance <= 0.25:
        res.utils.logger.info("shrinking label for small seam")
        label = label.resize((int(label.size[0] * 0.75), int(label.size[1] * 0.75)))
    label_width = label.size[0]
    pp = g.interpolate(offset)
    ce = g.interpolate(offset + label_width / 2.0)
    Fe = g.interpolate(offset + label_width)

    if g.length < label_width + offset:
        if not show_metadata:
            res.utils.logger.warn(
                f"the label is too wide to fit on the geometry at offset {offset}"
            )

    curve_sign = 1 if fit_to_line else 0

    # if the curve is convex it bends out of the shape and a chord connecting endpoints will be inside the shape
    # so if we do a hit test and find the chord is inside the shape we use the outward negative sign
    chord = LineString([pp, Fe])
    # we can take a ling further in as a comparator for the chord i.e. is the center of the chord near than the center placement point on the surface
    center_proxy = inside_offset_fn(chord, r=200).interpolate(chord.length / 2.0)

    # #for large chords the following may be safe
    # # rotate the chord and get the side furtheset from the surface
    # center_proxy = rotate(chord, 90)
    # p1, p2 = center_proxy.interpolate(0), center_proxy.interpolate(center_proxy.length)
    # center_proxy = p1

    if fit_to_line:
        if chord.interpolate(chord.length / 2).distance(center_proxy) > ce.distance(
            center_proxy
        ):
            curve_sign = -1

    # best approx of chord angle
    placement_angle = (
        np.rad2deg(np.arctan2(Fe.y - pp.y, Fe.x - pp.x)) if fit_to_line else 0
    )

    curvature = curve_sign * (180 - round(three_point_angle(*[pp, ce, Fe]))) * 0.5

    if not seam_allowance:
        seam_allowance = 0

    # testing
    if abs(curvature) > 3:
        d = {
            "curvature": curvature,
            "pp": str(pp),
            "ce": str(ce),
            "Fe": str(Fe),
            "center_proxy": str(center_proxy),
            "three_point_angle": round(three_point_angle(*[pp, ce, Fe])),
            "chord_center": str(chord.interpolate(chord.length / 2)),
            "chord_offset": str(inside_offset_fn(chord)),
            # report seam allowance in the pixels
            "seam_allowance": seam_allowance * 300,
            "label_size": label.size,
        }

    if pd.isnull(ce):
        raise Exception("Computed null placement position from chord")

    if curvature > 10:
        g = inside_offset_fn(g, 0.065 * 300)
        ce = g.interpolate(offset + label_width / 2.0)

    if pd.isnull(ce):
        raise Exception("Interpolated null placement position from chord")

    # if there is a centroid offset we can translate ce
    if offset_center:
        res.utils.logger.debug(f"Offset centroid in label placement")
        ce = translate(ce, xoff=offset_center[0], yoff=offset_center[1])

    if pd.isnull(ce):
        raise Exception("Translated null placement position from chord")

    if surface_geometry_offset_function:
        # res.utils.logger.debug(f'using supplied offset function to fit to surface')
        ce = surface_geometry_offset_function(ce)

    options = {
        "segment": g,
        "placement_angle": placement_angle,
        "placement_position": ce,
        # smooth curvature
        "curvature": curvature,
        # is this the only place we control directional
        "directional_label": (not fit_to_line),
    }

    if show_metadata:
        o = {
            "placement_angle": 0,
            "planned_placement_angle": np.round(placement_angle),
            "placement_position": ce,
            "planned_curvature": curvature,
            "res.curve_sign": curve_sign,
            "fit_to_line": fit_to_line,
        }
        print("METADATA: ", o)
        return place_label_from_options(
            im,
            notch_symbol("top_stitch"),
            # is this always true
            directional_label=(not fit_to_line),
            # get_text_image(
            #     str({k: v for k, v in o.items() if k != "placement_position"})
            # ),
            # TEMP - trying to understand placements - this is realted to some of the seam placement logic which may be wrong for viable surfaces
            **o,
        )

    # horizon context
    if piece_side_function:
        options["piece_side"] = piece_side_function(ce)

    if plan:
        print("planned", options)
        return options

    return place_label_from_options(im, label, shift_margin=45, **options)


def get_viable_locations(
    ol,
    notches,
    label_width,
    min_segment_length=700,
    inside_offset_r=25,
    notch_buffer=50,
    allow_sides=["top", "bottom", "left", "right"],
):
    """
    For a given label width find locations where we can place labels and the geometric info about those locations
    """
    min_angle = 170
    ol = ol.simplify(1)
    vs = ol - notches.buffer(notch_buffer) if pd.notnull(notches) else ol
    vs = vs - unary_union(magic_critical_point_filter(ol)).buffer(notch_buffer)
    vs = shift_geometry_to_origin(vs)
    transformed_ol = shift_geometry_to_origin(ol)
    vs = inside_offset(vs, transformed_ol, r=inside_offset_r)
    for l in vs:
        if l.length > min_segment_length:
            # interpolate in small jumps along the segment
            # if the curvature is small enough, accept the coordinate
            # find plaes from half the label width which is the first palce we can center-place until near the end. We can do better
            for pt in np.arange(label_width / 2, l.length - label_width, 10):
                # check cuvature where the label would be placed using a,b,c as the end points and b the center of the label
                a, c = l.interpolate(pt), l.interpolate(pt + label_width)
                b = l.interpolate(pt + label_width / 2.0)
                # is abs ok here
                theta = abs(three_point_angle(a, b, c))
                pChord = LineString([a, c])
                side = surface_side(b, transformed_ol)
                if side in allow_sides:
                    if theta > min_angle:
                        d = {}
                        d["segment"] = l
                        d["placement_position"] = b
                        d["place_chord"] = pChord
                        d["placement_angle"] = line_angle(pChord)
                        d["directional_label"] = False
                        d["piece_side"] = side
                        yield d


def viable_surfaces_for_edge(
    row, e, offset=0.135 * 300, sort=True, min_length=80, notch_bounds=None
):
    """
    the viable surface offset can be changed for an edge label

    notch bounds are the original projection of the notches used to define boundaries of a sectional edge
    #maybe we can make this work with half edges
    """
    try:
        edge_id_fn = lambda g: int(np.array(g.coords)[:, -1].mean())
        vs = row.get("viable_surface")
        edge_order_fn = dxf_notches.projection_offset_op(row)
        if offset:
            vs = inside_offset(vs, row["geometry"], r=offset)
        if sort:
            # we can sort viable surfaces by edge order. we sort by edge and then within the edge
            vs = sorted(vs, key=lambda v: (edge_id_fn(v), edge_order_fn(v.centroid)))
            # skip the first and probably the last because we dont want to place near the corners

        for i, g in enumerate(vs):
            ref_point = Point(list(g.coords))
            if notch_bounds:
                if len(notch_bounds) == 1:
                    # hmmm: so we introduced a pseudo sectional edge just in and around the notch
                    notch_bounds = notch_bounds[0], notch_bounds[0] + 100
            if notch_bounds and len(notch_bounds) > 1:
                if not (
                    edge_order_fn(ref_point) > notch_bounds[0]
                    and edge_order_fn(ref_point) < notch_bounds[1]
                ):
                    # this is because we have defined a sectional edge on the edge and we only accept viable surface segments
                    # that are inside these locations
                    continue
            # small lines near the start or end are ignore?? the near thing os not working though

            if g.length > min_length:
                edge_id = edge_id_fn(g)
                if e == edge_id or e == -1:
                    # yield the geometry and the edge.vs address
                    yield g, edge_id, i
    except Exception as ex:
        res.utils.logger.warn(
            "Could not find any viable surface for this edge!!!!!!!!!!!"
        )
        raise ex


def label_seams(
    im,
    row,
    one_number,
    construction_sequences,
    default_construction_seq=None,
    small_buffer=20,
    default_init_offset=150,
    min_viable_surface_length=100,
    show_metadata=False,
    seam_allowances=None,
    offset_center=None,
    surface_geometry_offset_function=None,
    **sew_symbol_options,
):
    """
    we can control sew symbols on seams here
    - identity
    - construction order
    - ops
    """

    def make_rank(r):
        t = r.get("type")
        if t == "identity":
            return 0
        if t == "op":
            return 1
        return 2

    used = {}
    seam_allowances = seam_allowances or {}

    surface_line = row["geometry"]

    # this is used to move in off the surface for some placements
    # identity?
    SEAM_PLACEMENT_OFFSET = 12.75

    def inside_offset_fn(g, r=SEAM_PLACEMENT_OFFSET):
        l = inside_offset(g, row["geometry"], r=r)
        # if offset_center:
        #     l = translate(l, xoff=offset_center[0], yoff=offset_center[1])
        return l

    # context for a point on the surface
    piece_side_function = lambda pt: surface_side(pt, row["edges"])

    info = list(row.get("edge_annotations", []))

    if len(info) == 0:
        return im

    info = pd.DataFrame([d for d in info])
    info["rank"] = info.apply(make_rank, axis=1)
    info = info.sort_values(["edge_id", "rank"])

    identity_placed = False

    geom_attributes = row.get("sew_geometry_attributes", [])

    try:
        geom_attributes = list(geom_attributes)
    except:
        geom_attributes = []

    bias = 0
    if "Bias" in geom_attributes:
        bias = np.abs(np.array(determine_outline_bias(row["geometry"]))).min().round()
    if "45Bias" in geom_attributes:
        bias = 45
    if "Minus45Bias" in geom_attributes:
        bias = -45

    res.utils.logger.debug(
        f"Piece bias is {bias} - geometry attributes {geom_attributes}"
    )

    def edge_length(e):
        return np.sum([v.length for v, _, _ in viable_surfaces_for_edge(row, e)])

    # on every edge we only want one construction symbol - we could merge the operations into one icon or here just cheat
    # the cheat is to check if we have already seen the construction order on the edge and not show it the second time
    construction_label_placed_on_edge = {}

    for e, g in info.groupby("edge_id"):

        el = edge_length(e)

        # depending on how big the edges use we can choose an offset at start - e.g. be generous on larger edges
        suggested_init_offset = default_init_offset
        if el > 2 * 300:
            suggested_init_offset = 1 * 300
        if el < 1 * 300:
            suggested_init_offset = 0.125 * 300

        for seam_annotation in g.to_dict("records"):
            # see contract for edge_annotations
            seam_placed = False
            sa_type = seam_annotation.get("type")

            if pd.isnull(sa_type):
                sa_type = "op"
            edge_id = seam_annotation.get("edge_id")
            if pd.isnull(edge_id):
                edge_id = -1
            olabel = seam_annotation["operation"]
            # we can have a vertical and horizontal allowance so to speak
            seam_allowance = seam_allowances.get(edge_id, 0.25 * 300)

            if edge_id == e or edge_id == -1:

                # if you have space use it - but we will bound then on the seam allowance
                vs_bound = 0.16 * 300
                upper_bound = 0.25 * 300
                div_factor = 0.3
                vs_offset = max(seam_allowance * div_factor, vs_bound)
                vs_offset = min(upper_bound, vs_offset)
                if seam_allowance <= 0.25:
                    fixed_dist = 25
                    res.utils.logger.info(
                        f"Small seam allowance: offset shift {vs_offset} -> {fixed_dist}"
                    )
                    vs_offset = fixed_dist

                # here we can define a sectional edge and restrict the viable surface

                n1 = seam_annotation.get("notch_0_ordinal")
                n2 = seam_annotation.get("notch_1_ordinal")
                edge_restriction_notches = []
                if pd.notnull(n1):
                    edge_restriction_notches.append(n1)
                if pd.notnull(n2):
                    edge_restriction_notches.append(n2)

                # there should be 0 or two
                if len(edge_restriction_notches) == 1:
                    res.utils.logger.warn(
                        f"In invalid state was encountered  - there is only one mapped notche in the edge_restriction_notches in res_color"
                    )
                    # raise Exception(
                    #     "*********Failing due to a missing notch!!!!!!!*****"
                    # )

                # print("SEARCH SURFACES ::::>>>>>>>>>>>>>>>>>>>> ", olabel, sa_type)
                for v, actual_edge, vs_idx in viable_surfaces_for_edge(
                    row, e, offset=vs_offset, notch_bounds=edge_restriction_notches
                ):
                    # print("::::>>>>>>>>>>>>>>>>>>>> ", olabel)
                    if v.length < min_viable_surface_length:
                        logger.debug(f"skipping viable surface segment below threshold")
                    GEOM_COORD = (actual_edge, vs_idx)
                    if sa_type == "op" and sew_symbol_options.get("ops"):
                        # print("OPY", seam_annotation)
                        construction_seq = seam_annotation.get(
                            "body_construction_sequence", []
                        )
                        # if we had a key we could look it up but dont support

                        # TODO: this is not perfect - idea is we have operations that are not denoted as labels but as lines or something like that?
                        apply_op_label = (
                            olabel if olabel not in ["shaping_fold"] else None
                        )

                        corder = seam_annotation.get("body_construction_order")
                        # we can turn off the consturction order
                        # but must have ops if we include it for now
                        if not sew_symbol_options.get("construction_order"):
                            construction_seq = []
                            corder = 0

                        # if we have already placed it - treat the order as 0 the next time
                        # if construction_label_placed_on_edge:
                        #     corder = 0

                        try:
                            # spook not having a construction sequence if we have already put it on the edge
                            if construction_label_placed_on_edge.get(edge_id):
                                construction_seq = []
                                corder = 0
                            label = construction_label(construction_seq)(
                                corder,
                                apply_op_label,
                            )

                        except Exception as ex:
                            res.utils.logger.warn(
                                f"Failing to place seam annotation {seam_annotation} and seq {construction_seq}({corder})"
                            )
                            raise ex
                    elif sa_type == "text" and sew_symbol_options.get("piece_name"):
                        label = get_text_image(olabel, auto_clip=True)
                    elif sa_type == "add_one_label":
                        label = get_one_label(one_number)
                    elif sa_type == "identity" and sew_symbol_options.get("identity"):
                        # this is not very nice - need a new model
                        if identity_placed:
                            continue
                        # the row interior is determine from sew construction logic and added to the marker

                        pairing_qualifier = PieceName.get_pairing_component(row)
                        if pairing_qualifier:
                            res.utils.logger.debug(
                                f"Determined piece pairing qualifier"
                            )

                        label = get_identity_label(
                            olabel,
                            is_interior=row.get("is_interior"),
                            identity_qualifier=row.get("identity_qualifier"),
                            pairing_qualifier=pairing_qualifier,
                        )

                        # this is really a notation that we need to figure out
                        # but we can have the angle match edge 0 in any case:> Orientation Edge
                        or_offset = seam_annotation.get("orientation_edge")
                        if not pd.isnull(or_offset):
                            res.utils.logger.debug(
                                f"'rotating' offset identity -> todo find edge 0:> {or_offset} - bias is {bias}"
                            )
                            label = label.rotate(
                                (-90 * or_offset),
                                expand=True,
                            )

                        # always rotate identity by bias
                        label = label.rotate(
                            -1 * bias,
                            expand=True,
                        )

                    else:
                        if sa_type != "text":
                            logger.debug(
                                f"skipping unsupported or decativated label type: {sa_type}"
                            )
                        continue

                    if label is not None:

                        label_width = label.size[0]
                        l = v.length
                        # if the length is relatively small

                        # init_offset = (
                        #     0 if l < 1.5 * label_width else default_init_offset
                        # )
                        init_offset = suggested_init_offset

                        # for really small edges we should try to squeeze a little more
                        l_used = used.get(GEOM_COORD, init_offset)
                        avail = l - l_used
                        rem_length = used.get(GEOM_COORD, l)

                        logger.debug(
                            f"attempt placing {olabel}({sa_type}) of size {label.size} on subsegment ({vs_idx}) at offset {l_used} edge {actual_edge} of full length {el}- rem size {avail} out of {v.length}"
                        )

                        # test
                        if os.environ.get("RES_IMAGE_DEV_LINES"):
                            res.utils.logger.debug("drawing viable surface dev lines")
                            im = Image.fromarray(draw_lines(im, v, dotted=True))

                        if label.size[0] < avail:
                            im = place_label_on_edge_segment(
                                im,
                                label,
                                g=v,
                                offset=l_used,
                                fit_to_line=sa_type not in ["identity"],
                                inside_offset_fn=inside_offset_fn,
                                piece_side_function=piece_side_function,
                                show_metadata=show_metadata,
                                offset_center=offset_center,
                                seam_allowance=seam_allowance,
                                surface_geometry_offset_function=surface_geometry_offset_function,
                            )

                            used[GEOM_COORD] = l_used + label_width + small_buffer
                            seam_placed = True
                            if sa_type == "identity":
                                identity_placed = True
                            if sa_type == "op":
                                construction_label_placed_on_edge[edge_id] = True

                            break
                        else:
                            res.utils.logger.debug(
                                f"label too big to place {label.size[0]} >= {avail}"
                            )

    # validate symbol placement checksum
    return im
