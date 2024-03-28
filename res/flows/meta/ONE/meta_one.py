"""
This is the meta-one base class. It reads from the database to provide pieces of a style
It supports interfaces such as getting nesting statistics and validation

TODO:
1. can we track if each pieces is directional in the metadata; more a flow question
2. set active status properly in the insert pieces mutation
3. nesting stats /costs
4. trims
5. test combos

annotation models
- cutlines that are physical with structure elements for special notch types
- symbolized notches that are just typed notches from construction with directionality
- SA two side sets derived from seam types e.g. add annotations at "notch points" on opposite edges
- sectional edges and recurse on the above

https://coda.io/d/Platform-Documentation_dbtYplzht1S/The-Meta-one-api_suGF9#_luHbW

draft schema




"""

from res.airtable.print import (
    print_material_id,
    ASSETS as PRINT_ASSETS,
    REQUESTS as PRINT_REQUESTS,
)
from res.airtable.cut import REQUESTS as CUT_REQUESTS, PIECES as CUT_PIECES
from res.media.images.geometry import Polygon
from res.media.images.outlines import (
    place_artwork_in_shape,
    get_piece_outline,
)
from shapely.wkt import loads
from res.media.images.geometry import (
    normal_vector_via_chord,
    unary_union,
    make_v_structure_element,
    shift_geometry_to_origin,
    inside_offset,
    MultiPoint,
    LineString,
    Point,
    line_at_point,
    from_geojson,
    pil_image_alpha_centroid,
    move_polygon_over_alpha,
)

from res.media.images.icons import (
    get_size_label_centered_circle,
    get_limited_edition_placeholder,
)
from shapely.geometry import MultiLineString, shape
from shapely.affinity import rotate, translate

from PIL import Image
import pandas as pd
import res
import numpy as np
import json
from res.flows.dxa.styles import queries, helpers, asset_builder
from res.flows.meta.ONE import queries as meta_one_queries
from res.media.images.qr import get_one_label_qr_code
from res.flows.make.production.queries import set_one_status_fields
from res.utils.dates import utc_now_iso_string
from res.flows.meta.ONE.geometry_ex import (
    _geom_seam_guide_blocks,
    _remove_shapes_from_outline,
    get_viable_seam_locations,
    get_forbidden_seam_regions,
    geometry_data_to_hpgl,
)

from res.flows.meta.ONE.annotation import (
    draw_annotations,
    draw_outline_on_image,
    draw_outline_on_image_center_with_offset_ref,
    draw_positional_notches,
    Annotation,
    load_identity_symbol,
    _placement_line_knit_buffer,
    add_one_code_hack,
    add_material_wall_hack,
    draw_individual_lines,
    fill_in_placeholder,
    # debug_placeholder,
)
from res.flows.meta.pieces import PieceName
from schemas.pydantic.meta import MetaOneStatus
import traceback
from res.flows.api.FlowContracts import FlowContractValidator
from res.utils.env import RES_DATA_BUCKET
from tenacity import retry, wait_fixed, stop_after_attempt
from res.flows.meta.ONE.annotation import ViableSegments
from res.media.images import get_perceptive_luminance
from res.media.images.text import get_text_image
from res.connectors.s3 import S3Connector


EDGE_CORNER_DISTANCE_BUFFER = 50
PRINTABLE_PIECE_TYPES = ["self", "block_fuse", "lining", "combo"]
DXA_KNIT_CUTLINE_PREVIEW_THICKNESS = 7
MIN_INTER_NOTCH_DISTANCE = 300 / 16  # about 1/16 inches
# this is borrowned from TUCKER style WHITEK
WHITE_ARTWORK = "s3://resmagic/uploads/992dfc9a-5ef1-42bc-9310-cec424cb6b03.tif"  # "s3://resmagic/uploads/c6c0bf75-b6cc-4dca-847d-a85f9fd4541d.tif"


def safe_loads(g):
    try:
        if pd.notnull(g) and str(g).lower() not in ["nan", "none"]:
            return loads(g)
    except:
        return g


class meta_one_piece:
    def __init__(self, record, parent):
        try:
            self._row = record
            self._vs = None

            self._parent = parent
            # the garment piece meta data is the style size BMC piece
            self._metadata = record.get("piece_metadata")
            if pd.isnull(self._metadata):
                self._metadata = {}

            is_knit = (self._metadata.get("offset_size_inches", 0) or 0) > 0

            self._row["requires_block_buffer"] = is_knit and self._parent.is_directional
            # when we label things this is a naff we to remove segments from the viable surface
            self._chosen_segments = []
            self._ref_bounds = self._row["outline"].bounds
            self._edges = self.piece_outline - self.corners.buffer(
                EDGE_CORNER_DISTANCE_BUFFER
            )
            self._offset_buffer = (
                res.utils.dataframes.get_prop(self._metadata, "offset_size_inches", 0)
                * 300
            )

            self._is_stable_material = self._offset_buffer == 0
            self.__is_legacy_flow = parent._flow != "3d"
            self._healing_instance = 0
            self._im = None
        except:
            res.utils.logger.warn(f'Failing to load piece {record["key"]}')
            raise

    def __getitem__(self, key):
        return self._row.get(key)

    @property
    def key(self):
        return self._row["key"]

    @property
    def body_code(self):
        body_code = "-".join(self.key.split("-")[:2])
        return body_code

    @property
    def body_version(self):
        body_version = int(float(self.key.split("-")[2].replace("V", "")))
        return body_version

    @property
    def size_code(self):
        return self._row.get(
            "style_sizes_size_code", getattr(self._parent, "size_code")
        )

    @property
    def normed_size(self):
        normed_size = self._row.get("normed_size", getattr(self._parent, "normed_size"))
        if normed_size is None:
            normed_size = self.size_code[-2:]
            if normed_size == "00":
                # Z2300 - Z5400 (we should probably look up the sizes but need to investigate)
                normed_size = self.size_code.replace("Z", "").replace("0", "")
        if normed_size == "One Size Fits All":
            return "OS"
        return normed_size.replace("-", "")

    @property
    def piece_ordinal(self):
        return self._row.get("piece_ordinal")

    @property
    def one_code(self):
        # we could use the parent set size for this but normally we want a set size definition straight from the database
        my_part = f"{self.piece_ordinal}/{self._row.get('piece_set_size')}"
        one_code = self._row.get("one_code")
        if one_code is None:
            # try determine it from parent but this is not really a supported code path
            one_code = getattr(self._parent, "one_code", None)
        return f"{one_code} {my_part}"

    @property
    def one_number(self):
        one_number = self._row.get(
            "one_number", getattr(self._parent, "one_number", None)
        )
        return one_number

    @property
    def sku(self):
        # for a collect of styles, 1 or more this makes sense. a body sku would be different but dont usual ask for it
        sku = self._row.get("sku", getattr(self._parent, "sku", None))
        return sku

    @property
    def piece_name(self):
        return PieceName(self.key)

    @property
    def is_legacy_flow(self):
        return self.__is_legacy_flow

    @property
    def piece_outline(self):
        ol = self._row["outline"]
        ol = shift_geometry_to_origin(ol)
        return ol

    @property
    def buffered_piece_outline(self):
        return shift_geometry_to_origin(
            Polygon(self.piece_outline).buffer(self._offset_buffer).boundary
        )

    @property
    def piece_sewline(self):
        ol = self._row["inline"]
        ol = shift_geometry_to_origin(ol, bounds=self._ref_bounds)
        return ol

    @property
    def internal_lines(self):
        if self._row.get("internal_lines"):
            lines = unary_union(self._row.get("internal_lines"))
            lines = shift_geometry_to_origin(lines, bounds=self._ref_bounds)
            if lines.type == "LineString":
                lines = MultiLineString([lines])
            return lines
        return MultiLineString([])

    @property
    def stitched_lines(self):
        # none hack need to see where it comes from as a string
        if (
            self._row.get("stitched_lines")
            and str(self._row.get("stitched_lines")) != "None"
        ):
            lines = unary_union(self._row.get("stitched_lines"))
            lines = shift_geometry_to_origin(lines, bounds=self._ref_bounds)
            if lines.type == "LineString":
                lines = MultiLineString([lines])
            return lines
        return MultiLineString([])

    @property
    def buttons(self):
        f = self._row.get("buttons_geojson")
        if f and str(f) != "None":
            # todo there is a function to get the hole vector
            b = unary_union(
                [
                    shape(b["geometry"]).buffer(10).exterior
                    for b in f["features"]
                    if b["properties"]["type"] == "button"
                ]
            )
            b = shift_geometry_to_origin(b, bounds=self._ref_bounds)
            return MultiLineString([b]) if b.type == "LineString" else b
        return MultiLineString([])

    @property
    def pleats(self):
        def _pleat_vector(s):
            c = [Point(p) for p in list(s.coords)]
            O = self.piece_outline
            # two step, create a project onto the surface to make a new line and inside offset that from the surface to allow for any placement of the pleat
            c1, c2 = O.interpolate(O.project(c[1])), O.interpolate(O.project(c[3]))
            O = inside_offset(LineString([c1, c2]), O, r=35)
            # then project the points onto that new line
            c1, c2 = O.interpolate(O.project(c1)), O.interpolate(O.project(c2))
            # the last point is the arrow head showing which way we interpret direction
            return unary_union([LineString([c1, c2]), c2.buffer(10).exterior])

        f = self._row.get("pleats_geojson")
        if f and str(f) != "None":
            # todo there is a function to get the hole vector
            b = unary_union(
                [_pleat_vector(shape(b["geometry"])) for b in f["features"]]
            )
            b = shift_geometry_to_origin(b, bounds=self._ref_bounds)
            return MultiLineString([b]) if b.type == "LineString" else b
        return MultiLineString([])

    @property
    def button_holes(self):
        def make_button_hole(f):
            pt = shape(f["geometry"])
            angle = np.degrees(f["properties"].get("radians", 0))
            return line_at_point(pt, angle + 90, 50)

        f = self._row.get("buttons_geojson")
        if f and str(f) != "None":
            b = unary_union(
                [
                    make_button_hole(h)
                    for h in f["features"]
                    if h["properties"]["type"] == "hole"
                ]
            )
            b = shift_geometry_to_origin(b, bounds=self._ref_bounds)
            return MultiLineString([b]) if b.type == "LineString" else b

        return MultiLineString([])

    @property
    def is_stable_material(self):
        return self._is_stable_material

    @property
    def labelled_piece_image_outline(self):
        # do we swap the points or not
        return get_piece_outline(np.asarray(self.labelled_piece_image))

    @property
    def piece_image_outline(self):
        # do we swap the points or not
        return get_piece_outline(np.asarray(self.piece_image))

    @property
    def seam_guides(self):
        sa = res.utils.dataframes.get_prop(self._row, "seam_guides", [])
        sa = unary_union([s for s in sa if s.type != "Point"])
        # sa = sa.intersection(Polygon(self.piece_outline))
        sa = shift_geometry_to_origin(sa, bounds=self._ref_bounds)
        return sa
        # sa =  sa.intersection(Polygon(self.piece_outline))

    @property
    def _seam_guide_forbidden_regions(self):
        return get_forbidden_seam_regions(self)

    @property
    def material_offset(self):
        return 300 * self._row.get("piece_metadata", {}).get("offset_size_inches", 0)

    @property
    def typed_notches(self):
        """
        Here we classify notches - there are some physical ones that will be used in cutlines and the rest will be symbolized in some way
        physical notches are more versatile if we have them but each needs a special structure element

        for knits all notches should be v -notches (not the seam guides which are castles)
        for wovens we will be more specific but this needs to come from construction

        in 2d there should be no notch types and everything is already drawn on the color

        found type where seam guide is a point - need to check contract
        """

        _typed_notches = []

        # WARNING this is to calibrate physical notches
        # half_width = 12 if self._parent._fully_annotate == True else 3
        half_width = 3
        # get the seam guide vectors which we will turn into blocks
        seam_guides = _geom_seam_guide_blocks(self.seam_guides, half_width=half_width)
        for i, sg in enumerate(self.seam_guides):
            if sg.type != "Point":
                _typed_notches.append(
                    Annotation(
                        point=sg.interpolate(0),
                        segment=sg,
                        shape=seam_guides[i],
                        atype="castle_notch",
                    )
                )

        # we get the vector emanating from the surface at the notch location just look seam guides
        _notches = [
            self.get_surface_normal_vector_at_point(n)
            for n in self._surface_notches
            if n.distance(self.seam_guides) > 1
        ]
        # allow from some exceptions above but need to loop back to understand
        _notches = [n for n in _notches if n is not None]

        # these are turned into blocks that are removed from the shape later
        # for stable materials we default to v notches on all physical notches that are not seam guides
        notch_blocks = _geom_seam_guide_blocks(_notches, half_width=half_width)
        for i, n in enumerate(_notches):
            _typed_notches.append(
                Annotation(
                    point=n.interpolate(0),
                    segment=n,
                    shape=notch_blocks[i],
                    atype="castle_notch" if self.is_stable_material else "v-notch",
                )
            )

        # off the corners we can use different angles to make the surface elements if we want them for wovens but for now we wont
        # castles = make_castle_structure_surface_element(self.piece_image_outline)

        return _typed_notches

    @property
    def notches(self):
        """
        By contract, seam guides should not be in here - they are everything else
        """
        n = self._row["notches"]
        if n and hasattr(n, "type"):
            n = shift_geometry_to_origin(n, bounds=self._ref_bounds)
            if n.type == "Point":
                return MultiPoint([n])
            return n
        return []

    @property
    def _surface_notches(self):
        """
        Make sure its only the surface ones - we should not need this its just for testing
        """
        # surface notches are valid if they are near the surface but not near corners
        n = [n for n in self.notches if n.distance(self.piece_outline) < 0.001]

        # in the event that there are seam guides
        if not self.seam_guides.is_empty:
            n = [
                item
                for item in n
                if item.distance(self.seam_guides) > MIN_INTER_NOTCH_DISTANCE
            ]

        if not self.corners.is_empty:
            n = [item for item in n if item.distance(self.corners) > 5]

        # special but risky
        # r = self._seam_guide_forbidden_regions
        # n = [n for n in n if n.distance(r) > 5]

        if len(n) == 0:
            return MultiPoint([])
        n = unary_union(n)
        if n.type == "Point":
            return MultiPoint([n])
        return n

    @property
    def surface_notch_lines(self):
        vectors = [
            self.get_surface_normal_vector_at_point(p) for p in self._surface_notches
        ]
        lines = unary_union([v for v in vectors if v is not None])
        return lines if lines.type != "LineString" else MultiLineString([lines])

    def get_physical_cutline(self, outline=None):
        """
        we assume the piece image outline and the outline agree at this point

        the physical cutline may depend on the material so we trust the type notch compiler to give us what we want
        here we just need to subtract castle or other intruding elements and add extruding elements

        we trust the body outline from metadata
        the physical cutline will be draw for stable materials but not for unstable in the labelled

        we should
        """

        if self.__is_legacy_flow:
            # in the legacy 2d flow the notches are already as they should be in the outline and this cutline flows through unbuffered
            return self.piece_outline

        elements = []

        # we use the passed in outline or default. passed in outline is probably the computed piece image outline
        g = self.piece_outline if outline is None else outline

        def omit(n):
            """
            a special case omission for now on neck bindings after speaking to Xi to help sewers
            """
            k = PieceName(self.key)._key
            if "NBDG" in k:
                bnds = g.bounds[-2]
                return n.x > 50 and n.x < (bnds - 50)
            return False

        v_notches = make_v_structure_element(g)
        _castles_geom = None
        try:
            castle_notches = [
                n.shape for n in self.typed_notches if n.atype == "castle_notch"
            ]

            if len(castle_notches) > 0:
                castle_notches = [n for n in castle_notches if not omit(n.centroid)]
                g = _remove_shapes_from_outline(g, unary_union(castle_notches))

            _castles_geom = unary_union(castle_notches)

        except Exception as ex:
            # we only need this in legacy 2d flow anyway

            res.utils.logger.debug(
                f"Failed to remove castle notches experimental mode {ex}"
            )

        # look for structure elements
        for typed_notch in self.typed_notches:
            if typed_notch.atype == "v-notch":
                if (
                    _castles_geom
                    and _castles_geom.distance(typed_notch.point) < 300 / 4
                ):
                    # skip vs to near castles
                    continue
                # the structure element is constructed from then notch vector not the point
                elements.append(v_notches(typed_notch.segment))

        if len(elements) == 0:
            return g

        c = unary_union(elements)

        # return c
        g = unary_union([c, Polygon(g)]).boundary

        return g

    def get_viable_surface(
        self, notch_buffer=150, inside_offset_r=35, return_external=False
    ):
        # for a knit draw the things somewhere in the image region
        # might be smarter to get a large outside offset of the cutline instead (performance)
        if not self.is_stable_material:
            return MultiLineString(_placement_line_knit_buffer(self))

        ol = self.piece_outline.simplify(1)
        if self.corners and not self.corners.is_empty:
            ol = ol - self.corners.buffer(notch_buffer)
        if self.seam_guides and not self.seam_guides.is_empty:
            ol = ol - self.seam_guides.buffer(notch_buffer)
        if self.notches and not self.notches.is_empty:
            ol = ol - self.notches.buffer(notch_buffer)

        if return_external:
            return ol
        ol = inside_offset(ol, self.piece_outline, r=inside_offset_r)

        if ol.type == "LineString":
            ol = MultiLineString([ol - ol.interpolate(10).buffer(50)])

        if len(self._chosen_segments):
            res.utils.logger.debug("Removing segments from the viable surface")
            a = unary_union([a.buffer(50) for a in self._chosen_segments])
            ol = ol - a

        # remove invalid
        def is_valid(l):
            if l is None:
                return False
            if l.length == 0:
                return False
            l = rotate(l, 90)
            p1 = l.interpolate(0, normalized=True)
            p2 = l.interpolate(1, normalized=True)
            P = Polygon(self.piece_outline)
            return not (P.contains(p1) and P.contains(p2))

        ol = MultiLineString([l for l in ol if is_valid(l)])

        return ol

    def get_viable_surface_frame_for_label(self, label_size):
        vs = self.get_viable_surface()
        return pd.DataFrame(
            list(
                get_viable_seam_locations(
                    vs,
                    ref_outline=self.piece_outline,
                    label_width=label_size,
                    # this is sort of stupid. we have knits that take a one number possible should not on a horizontal line that screws up old logic
                    use_line_rule=(not self.is_legacy_flow),
                )
            )
        )

    @property
    def piece_image(self):
        """
        the image is either loaded from the uri
        """
        if self._im is not None:
            return self._im

        placed_color_path = self._row.get("uri")
        if placed_color_path:
            self._im = Image.fromarray(
                res.connectors.load("s3").read(placed_color_path)
            )
        else:
            artwork_uri = self._row.get("artwork_uri")

            if artwork_uri:
                ol = self.piece_outline
                # in all flows
                if self._offset_buffer and self._offset_buffer > 0:
                    res.utils.logger.info(
                        f"apply offset buffer {self._offset_buffer/300}"
                    )
                    ol = Polygon(ol).buffer(self._offset_buffer).exterior
                im = place_artwork_in_shape(ol, artwork_uri)
                im = draw_outline_on_image(im, ol=ol, thickness=2)
                # this is import to subsequently split it out or separate from background
                # im = draw_outline_on_image(im, ol, thickness=6) <- test the outline first for some color pieces especially knits like KT-3038 CTNBA BLACHC 4ZZLG' <-

            self._im = im
        return self._im

    @property
    def corners(self):
        """
        temporary - this will be in the database
        """

        # if its available in the database we use it e..g 3d otherwise we infer it

        corners = self._row["corners"]
        if corners:
            corners = unary_union(corners)
            corners = shift_geometry_to_origin(corners, bounds=self._ref_bounds)
            return corners

        from res.media.images.geometry import magic_critical_point_filter

        # for consistency with legacy we need to remove the notches to detect corners properly
        closed_outline = Polygon(self.piece_outline).buffer(50).buffer(-50).exterior
        return unary_union(magic_critical_point_filter(closed_outline))

    @property
    def annotations(self):
        return []

    @property
    def edges(self):
        """
        these are the segmented lines between corners with some space from corners
        """
        e = self._edges
        if e.type == "LineString":
            return MultiLineString([e])
        return e

    @property
    def size_as_label(self):
        from res.media.images.text import get_text_image

        return get_text_image(self.size_code)

    @property
    def identity_symbol(self):
        """
        this icon is usually placed on the last edge an orientated to sit with gravity even if the piece has bias
        """
        try:
            symbol = self._row.get("sew_identity_symbol")
            if symbol:
                symbol = load_identity_symbol(symbol, light=False)
                return symbol.resize((50, 50))
        except:
            return None

    def get_identity_symbol(self, light_mode=False):
        """
        this icon is usually placed on the last edge an orientated to sit with gravity even if the piece has bias
        """
        try:
            symbol = self._row.get("sew_identity_symbol")
            if symbol:
                symbol = load_identity_symbol(symbol, light_mode=light_mode)
                return symbol.resize((50, 50))
        except Exception as ex:
            print("failed to load identity", symbol, ex)
            return None

    @property
    def piece_type(self):
        return self._row.get("body_pieces_type")

    def edge_containing(self, pt, TINY=0.01):
        edges = list(self.edges)
        distances = [pt.distance(e) for e in edges]

        touching_edges = []
        for i, d in enumerate(distances):
            if d < TINY:
                touching_edges.append(edges[i])
        if touching_edges:
            e = unary_union(touching_edges)
        # if we cannot create a union of edges that it touches just return one that is close. but really it should be touching
        else:
            e = edges[np.argmin(distances)]
        return e

    def surface_containing(self, pt):
        return self.piece_outline.intersection(pt.buffer(50))

    def get_surface_normal_vector_at_point(self, pt, explain=False):
        try:
            # g = self.edge_containing(pt)
            g = self.surface_containing(pt)

            # assert the g is near the point / when we add it make sure the vector is somewhere sensible
            # temp = normal_vector(g=g, outline=self.piece_outline, pt=pt, explain=explain)
            return normal_vector_via_chord(
                g=g, outline=self.piece_outline, pt=pt, explain=explain
            )
        except:
            # print(traceback.format_exc())
            # trying to use the angle of the nearest line
            return None

    def get_one_number_and_group_symbol(self, one_number=None, light=False):
        # need to deprecate this import
        from res.flows.dxa.res_color import get_one_label

        one_number = self.one_number
        if not one_number:
            return None
        if "LBLBYPNL" in self.piece_name.name:
            # for the one label and maybe for any label in future we dont show a one number
            return None

        piece_make_type = "normal"
        if self._parent._meta_one_options.get("is_healing"):
            piece_make_type = "healing"
        if self._parent._meta_one_options.get("is_extra", False):
            piece_make_type = "extra"

        my_part = f" ({self.piece_ordinal}/{self._row.get('piece_set_size')})"

        color = (100, 100, 100, 200) if light else (20, 20, 20, 255)

        lbl = get_one_label(
            one_number,
            piece_type=self.piece_name.piece_type,
            piece_make_type=piece_make_type,
            my_part=my_part,
            color=color,
        )
        try:
            gl = (
                self.piece_name.grouping_symbol
                if not light
                else self.piece_name.grouping_symbol_light
            )
            fact = lbl.size[1] / gl.size[1]
            new_size = (int(gl.size[0] * fact), lbl.size[1])
            gl = gl.resize(new_size)
            labels = [lbl, np.zeros_like(gl), gl]

            return Image.fromarray(np.concatenate(labels, axis=1))
        except:
            res.utils.logger.warn(f"failed to get group icon for piece {self.key}")
            return lbl

    @property
    def one_code_label(self):
        try:
            return get_text_image(self.one_code, size=32)
        except:
            return None

    def __repr__(self):
        return self.key

    def get_annotated_piece_image(
        self, instance=1, draw_centered_cutline=False, **kwargs
    ):
        """
        this is the runtime instance where we can add additional context and overlays outside of meta one context
        - QR codes or customizations
        - annotations for previews or testing
        """
        self._healing_instance = kwargs.get("healing_instance", 0)
        im = self.labelled_piece_image

        # suppose the viable surface is updated from placed locations
        # now we can use the model to add stuff in other places here
        return im

    @property
    def geometries(self):
        return unary_union(
            [self.seam_guides, self.piece_outline, self.notches, self.internal_lines]
        )

    def draw_sew_internal_annotations(self, im):
        # if we want to use these in the meta one can move them to labelled piece images + test the different use cases that they are loaded form the body
        if self._row.get("stitched_lines"):
            im = draw_individual_lines(im, self.stitched_lines)

        try:
            im = draw_individual_lines(im, self.buttons)
            im = draw_individual_lines(im, self.button_holes, dotted=False)
            im = draw_individual_lines(im, self.pleats, thickness=4, dotted=True)
        except Exception as ex:
            print(ex, "failed on sew annotations")
        return im

    @property
    def dxa_preview(self):
        # create the annotated piece image as it would be applied in nesting
        im = self.get_annotated_piece_image()
        # for knits show the cutline
        if not self._is_stable_material:
            x = self.piece_image_outline.simplify(1)
            ref_outline = inside_offset(x, x, r=self.material_offset)
            if ref_outline:
                # you would think this cannot happen in this use case but for example some binding pieces are not buffered at all?
                im = draw_outline_on_image_center_with_offset_ref(
                    im,
                    self.get_physical_cutline(),
                    ref_outline,
                    thickness=DXA_KNIT_CUTLINE_PREVIEW_THICKNESS,
                )

        if not self.piece_name.is_printable and self._row.get("internal_lines"):
            im = draw_individual_lines(im, self.internal_lines)

        if isinstance(self._parent, BodyMetaOne):
            im = self.draw_sew_internal_annotations(im)

        return im

    @property
    def artwork_replacements(self):
        return (
            self._row.get(
                "artwork_replacements",
                getattr(self._parent, "_artwork_replacements", {}),
            )
            or {}
        )

    def fill_in_placeholders(self, im, offset_x=0, offset_y=0, **kwargs):
        """
        We use the body level anchors to add labels in place
        the replacement data can come from any level; body, style or order so we allow this to be passed in or taken from the piece
        """

        # the piece instance overrides the make instance
        # for example a make instance is something that tells us how many times we have made this style but a piece instance could override that i.e. if we have multiples of labels
        piece_instance = self._row.get("piece_instance") or self._row.get(
            "make_instance", 0
        )
        artwork_replacement_mapping = self.artwork_replacements

        def image_loader(name, size=None):
            uri = artwork_replacement_mapping.get(name)
            if uri:
                return res.connectors.load("s3").read_image(uri)
            # if qr code and we have piece instance
            if name in ["one_label_placeholder", "one_code_placeholder"]:
                return get_one_label_qr_code(
                    f"9{piece_instance}{self._parent._one_number}", size=size
                )

            if "limited_edition_index_placeholder" in name:
                _label = get_one_label_qr_code(
                    f"9{piece_instance}{self._parent._one_number}"
                )

                # size = int(_label.size[0] * 0.8), int(_label.size[1] * 0.8)
                # TODO need a way to determine a brands font but for now the default will be the first one we used
                return get_limited_edition_placeholder(piece_instance, size=size)

            if name in ["size_placeholder"]:
                font = "ArchivoBlack-Regular.ttf"
                if self._parent.body_code == "ME-9000":
                    font = "Chapman-Medium.otf"
                size = (125, 125)
                return get_size_label_centered_circle(
                    text=self.normed_size, size=size, font=font
                )

            # else:
            #     raise Exception(f"Have not handled for case {name} in image loader")

        # resolve artwork for special handlers and add to the mapping here -> qr code based on instance, material wall based on material code of the piece, normed size based on normed size of the piece
        try:
            # body_piece_placeholders_geojson
            placeholders = self._row.get(
                "body_piece_placeholders_geojson"
            ) or self._row.get("body_pieces_placeholders_geojson", {})

            ### temp hack - aren't they all!
            try:
                bags = [
                    "CC9023 HC293 MIDJCG 0ZZOS",  # -
                    "CC9023 HC293 MIDJDB 0ZZOS",  #
                    "CC9023 HC293 MIDJDI 0ZZOS",  #
                    "CC9023 HC293 MIDJEH 0ZZOS",  #
                    "CC9023 HC293 MIDJFC 0ZZOS",  #
                    "CC9023 HC293 MIDJKJ 0ZZOS",  #
                    "CC9023 HC293 MIDJQD 0ZZOS",  #
                    "CC9023 HC293 MIDJQT 0ZZOS",  #
                    "CC9023 HC293 MIDJWU 0ZZOS",  #
                    "CC9023 HC293 MIDJZL 0ZZOS",  #
                ]
                if (
                    "CC-9023-V1-BAGFTPKT-S" in self._row["body_piece_key"]
                    and self._parent.sku.replace("-", "") in bags
                ):
                    placeholders = {
                        "type": "FeatureCollection",
                        "features": [
                            {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": (489.737, 1920),
                                },
                                "properties": {
                                    "name": "one_code_placeholder",
                                    "width": 295,
                                    "height": 295,
                                    "radians": 0.0,
                                },
                            }
                        ],
                    }
            except Exception as ex:
                pass
            ### end temp hack

            for placeholder in placeholders.get("features") or {}:
                res.utils.logger.info(f"filling placeholders {placeholder}")
                # im = debug_placeholder(im, placeholder)
                im = fill_in_placeholder(
                    self, im, placeholder, image_loader, offset_x, offset_y
                )
            return im

        except Exception as ex:
            res.utils.logger.warn(
                f"failed to fill in placeholders {traceback.format_exc()}"
            )
        return im

    def try_add_further_annotations(self, im):
        # try catch adding s    ome more stuff
        try:
            if self._parent._fully_annotate == True:
                im = self.draw_sew_internal_annotations(im)

        except Exception as ex:
            res.utils.logger.warn(f"Failing to added extra annotations {ex}")

        return im

    @property
    def labelled_piece_image(self):
        # load some properties from the meta one e.g. to add some extra features

        im = self.get_labelled_piece_image()
        # im = self.try_add_further_annotations(im)

        return im

    @property
    def material_code(self):
        return self._row.get("garment_piece_material_code")

    def _is_no_symbol_mode(self, im, L):
        """
        no symbol mode has become a misnomer but its now a sensitivity to lum.
        for light colors we use light text
        """
        try:
            # rename this to light mode
            # for now only if its fully annotated

            # if self._parent._fully_annotate == False:
            #     return False

            # WE are enabling this on all styles now

            LTHRESHOLD = 0.9
            # if self.material_code in ["CDCBM"]:
            if L > LTHRESHOLD:
                res.utils.logger.info(
                    f"Using auto annotate mode to make some genomic decisions about what and when to label"
                )
                return True
        except Exception as ex:
            res.utils.logger.warn(
                f"Failing to detect symbol mode {res.utils.ex_repr(ex)}"
            )
        return False

    def get_image_offset(self, shape):
        res.utils.logger.info("Applying offset buffer")
        image_bounds = self.piece_image_outline.bounds

        width_diff = image_bounds[2] - shape.bounds[2]
        height_diff = image_bounds[3] - shape.bounds[3]

        offset_x = width_diff / 2
        offset_y = height_diff / 2

        return offset_x, offset_y

    def try_get_no_symbol_mode(self, im):
        try:
            points = [pt.point for pt in self.typed_notches]
            L = get_perceptive_luminance(im, points)
            res.utils.logger.info(f"Perceptive luminance {L}")
            return self._is_no_symbol_mode(im, L=L)
        except:
            res.utils.logger.warn(f"Failing to try_get_no_symbol_mode ")
            return False

    def get_buffered_piece_outline(self, im):
        ol = self.buffered_piece_outline
        offset_x, offset_y = self.get_image_offset(ol)

        # this is a safety for transparent art in a known knit case
        # if the piece is buffered, we may have to offset the outline

        ############ centroid offset
        # inline = self._row["inline"].centroid.coords[0]
        # outline = self._row["outline"].centroid.coords[0]

        # offset = (inline[0] - outline[0], inline[1] - outline[1])
        # offset_x += offset[0]
        # offset_y += offset[1]
        #################

        ############# bounds offset
        # inline = self._row["inline"].bounds
        # outline = self._row["outline"].bounds

        # res.utils.logger.info(f"inline {inline}")
        # res.utils.logger.info(f"outline {outline}")

        # offset = (
        #     ((inline[0] + inline[2]) - (outline[0] + outline[2])) / 2,
        #     ((inline[1] + inline[3]) - (outline[1] + outline[3])) / 2,
        # )
        # offset_x += offset[0]
        # offset_y += offset[1]
        #################

        ############# image diff offset
        im_size = im.size
        ol_size = ol.bounds[2], ol.bounds[3]

        offset = (
            (im_size[0] - ol_size[0]) / 2,
            (im_size[1] - ol_size[1]) / 2,
        )
        # offset_x += offset[0] # no idea why this x isn't needed but y is...
        offset_y += offset[1]
        ################

        ol = translate(ol, xoff=offset_x, yoff=offset_y)

        return ol

    def get_labelled_piece_image(self, **kwargs):
        self._chosen_segments = []
        # print("<<<<>>>>>")
        # print(self._parent._fully_annotate)
        res.utils.logger.debug(
            f"Fetching the labelled image for {self.key}. Please wait..."
        )

        im = self.piece_image

        if not self.is_stable_material:
            # if the piece is buffered, we may have to offset
            ol = self.get_buffered_piece_outline(im)
        else:
            # here we are using the image outline for solids for now
            # this is safer than changing for all cases as risk of mismatch
            # but we should at least measure difference to piece outline and use the piece outline if the image outline looks weird
            ol = get_piece_outline(np.asarray(im))

        # creating a special test mode to not do the other stuff and just add a cutline and some fainter images
        # based on testing we could merge this better with the other stuff
        # experimental physical cutlines
        label_placed = False
        no_symbol_mode = self.try_get_no_symbol_mode(im)

        # needs refactor above and here to choose label placements based on color alone
        if not label_placed:
            one_label = self.get_one_number_and_group_symbol(
                one_number=kwargs.get("one_number"), light=no_symbol_mode
            )

            ### deprecated block when we add annot. to BODY
            # here there is a rule that labels are a trim type and should not get one numbers!!

            # adding the identity symbol too
            labels = {
                "one_label": one_label,
                "identity": self.get_identity_symbol(light_mode=no_symbol_mode),
            }
            if self._parent._healz_mode:
                labels["size"] = self.size_as_label

            # if self._parent._fully_annotate == True:
            #     labels = {
            #         "one_label": one_label,
            #         "one_code": self.one_code_label,
            #         "identity": self.identity_symbol,
            #     }

            vs = ViableSegments(self)

            if one_label is not None and self.piece_name.name not in [
                "BDYONLBL-S",
                "LBLBYPNL-S",
            ]:
                for key, label in labels.items():
                    if label is None:
                        continue
                    try:
                        options = {}
                        if key == "identity":
                            options["directional_label"] = True
                        im = vs.place_label_with_reservation(im, label, **options)

                    except:
                        res.utils.logger.warn(
                            f"Unable to add label  on {self.key} {traceback.format_exc()} "
                        )

        im = add_one_code_hack(self, im, size=self.normed_size)
        im = add_material_wall_hack(
            self, im, material_code=self._row.get("garment_piece_material_code")
        )
        ####################################

        offset_x = offset_y = 0
        if not self.is_stable_material:
            offset_x, offset_y = self.get_image_offset(self.piece_outline)
        im = self.fill_in_placeholders(im, offset_x, offset_y, **kwargs)

        # we should do this always - for white things we need to see it
        dx, dy = move_polygon_over_alpha(im, ol)
        im = draw_outline_on_image(im, ol=ol, thickness=1, dx=dy, dy=dx)

        # for stable materials we can draw all sorts of things
        if self.is_stable_material or not self.piece_name.is_printable:
            # we can take the physical outline which is needed because of small differences

            #####CHANGE COLOR FOR LIGHTS
            color = (0, 0, 0, 255)
            points = [pt.point for pt in self.typed_notches]
            try:
                L = get_perceptive_luminance(im, points)
                if self._is_no_symbol_mode(im, L=L):
                    res.utils.logger.info(f"Using the lighter color")
                    color = (150, 150, 150, 255)
            except:
                res.utils.logger.info(f"Failing to try_get_no_symbol_mode ")

            #######################
            if not self.is_legacy_flow:
                im = draw_positional_notches(
                    im, self.seam_guides, outline_guide=ol, color=color
                )
                im = draw_positional_notches(
                    im,
                    self.surface_notch_lines,
                    distance_factor=0.9,
                    outline_guide=ol,
                    color=color,
                )
                im = draw_annotations(
                    im, self.annotations
                )  # void op for now until we generalize and subsume the rest
                # im = draw_dashed_line_on_image(im,self.piece_sewline,thickness=3)
            # for the legacy flow we still draw the cutline
            im = self.draw_cutline(im, outline=ol)
        # if not self.is_stable_material:
        #     res.utils.logger.info("drawing a thin line around the knit")
        #     im = draw_outline_on_image(im, ol=self.piece_image_outline, thickness=2)

        # we have a standard on the bias now
        return im

    def draw_cutline(self, im, outline=None, **kwargs):
        """
        We draw the cutline in different contexts
        for wovens we draw it but we close it
        for knits we only draw it for previews and center it but not by default in the labelled image
        """
        # it may be we can be more efficient and use the outline that we load but they are slightly different maybe with a little cut line
        # be careful if we get an outline from in image we may need to swap points?
        ol = self.get_physical_cutline(outline=outline)
        # remove castle notches from the outline for drawing around it - we keep the v notches
        if self._is_stable_material:
            ol = Polygon(ol).buffer(25).buffer(-25).exterior

        im = draw_outline_on_image(im, ol, **kwargs)
        return im


def _try_parse_version_from_row(row):
    try:
        # if we dont, a valid piece names should always have the body version
        k = row.get("key")
        return int(k.split("-")[2].upper().replace("V", ""))
    except:
        return "?"


def soft_swap(data, field="garment_piece_material_code", context=None):
    """
    https://github.com/resonance/res-data-platform/pull/3227
    """
    try:
        import os

        """1 of three places"""
        # in source code for now - could put this somewhere else e.g. in the graph - use controlled
        swaps = {"LSG19": "CHRST"}  # {"CTW70": "PIMA7"}

        materials = set(data[field].unique())

        # hack to allow the swap if it gets this far and its requested
        if "LSG19" in materials and "CHRST" not in materials:
            if context == "CHRST":
                swaps = {"LSG19": "CHRST"}

        swapping = set(swaps.keys())
        if (
            len(materials & swapping) > 0
            and os.environ.get("DISABLE_SOFT_SWAPS") == None
        ):
            res.utils.logger.warn("<<<<<<<<< APPLYING SOFT SWAP >>>>>>>>>>")
            data[field] = data[field].map(lambda x: swaps.get(x, x))

    except:
        pass
    return data


class MetaOne(FlowContractValidator):
    def __init__(
        self,
        sku,
        one_number=None,
        piece_type=None,
        material_code=None,
        multiplier=None,
        min_body_version=None,
        artwork_replacements=None,
        fully_annotate=False,
        make_instance=None,
        body_version=None,
        **kwargs,
    ):
        """
        the filter for piece type is current a single slicer because we want to use the meta one here for ppp use case
        in future we may select pieces in a different way. we will never need to filter sets probably
        """

        self._size_lu_data = {}
        self._healz_mode = kwargs.get("healz_mode", False)
        self._one_number = str(one_number) if one_number else None
        hasura = res.connectors.load("hasura")
        self._fully_annotate = fully_annotate
        # experimentally we might try to use genomic properties when deciding how to annotate something

        if sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"

        self._sku = sku

        if self.style_sku in ["JR-3117 PIMA7 ATDMLI", "RV-3001 COMCT BIGSBS"]:
            res.utils.logger.info(f"Enabling the special mode")
            self._fully_annotate = True
            # if one_number and one_number % 2 == 0:
            #     pass  # in future enable this thing for even ones

        self._meta_one_options = kwargs

        if self._fully_annotate == True:
            res.utils.logger.info(
                "This meta one will be fully annotated which is an experimental option"
            )
        self._artwork_replacements = artwork_replacements

        # else:
        res.utils.logger.debug(f"Loading {sku} for version {body_version}")
        self._data = queries.get_style_by_sku(
            sku, hasura=hasura, body_version=body_version
        )

        # this is only done because there are some cases where we removed a 3d piece say, and the sku still loads this pending piece but not sure why.
        # good test for clearing up state e.g. resave TH-6002 CTNBA GRAYLN 3ZZMD TODO: ...
        self._data = self._data[
            (self._data["body_piece_type"].notnull())
            & (self._data["style_sizes_status"] == "Active")
        ].reset_index(drop=True)
        assert (
            self._data is not None and len(self._data) > 0
        ), f"Unable to load the meta one for SKU {sku} - either the style header or the style size or both do not exist"

        res.utils.logger.debug(f"Loaded style pieces length {len(self._data)}")
        if multiplier:
            self._data["piece_instance"] = self._data["body_piece_piece_key"].map(
                lambda x: list(range(multiplier))
            )

            self._data = self._data.explode("piece_instance").reset_index(drop=True)

            self._data["body_piece_piece_key"] = self._data.apply(
                lambda row: PieceName(row["body_piece_piece_key"]).identity_prefix(
                    row["piece_instance"]
                ),
                axis=1,
            )
            # self._data['key'] =

        # this depends on the graph ql schema evolution so is a little sensitive
        self._data = self._data.rename(
            columns={
                "body_piece_piece_key": "key",
                "body_piece_outer_geojson": "outline",
                "body_piece_inner_geojson": "inline",
                "body_piece_outer_notches_geojson": "notches",
                "garment_piece_base_image_uri": "uri",
                "garment_piece_metadata": "piece_metadata",
                "garment_piece_piece_set_size": "piece_set_size",
                "garment_piece_piece_ordinal": "piece_ordinal",
                "garment_piece_normed_size": "normed_size",
                "garment_piece_artwork_uri": "artwork_uri",
                "body_piece_outer_corners_geojson": "corners",
                "body_piece_seam_guides_geojson": "seam_guides",
                "body_piece_sew_identity_symbol": "sew_identity_symbol",
                # TODO: this is only some of the internal lines actually
                "body_piece_internal_lines_geojson": "internal_lines",
                "body_piece_buttons_geojson": "buttons_geojson",
                "body_piece_pleats_geojson": "pleats_geojson",
            }
        )

        try:
            self._data["meta_piece_code"] = self._data["key"].map(
                lambda x: "-".join(x.split("-")[-2:])
            )
        except:
            pass

        self._data = soft_swap(self._data, context=material_code)

        self._data["make_instance"] = make_instance

        if len(self._data) > 0:
            self._num_deleted_body_pieces = len(
                self._data[~self._data["body_piece_deleted_at"].isnull()]
            )
            if self._num_deleted_body_pieces > 0:
                res.utils.logger.warn(
                    f"{self._num_deleted_body_pieces} pieces deleted from the body"
                )
            if not kwargs.get("retain_deleted_body_pieces"):
                # TODO: move this to query layer and check style history
                self._data = (
                    self._data[self._data["body_piece_deleted_at"].isnull()]
                    .reset_index()
                    .drop("index", 1)
                )

        self._descrim_hash_options = {"one_number": self._one_number}

        # filters
        if piece_type:
            self._descrim_hash_options["piece_type"] = piece_type
            # we have to do this hack and it will hurt is again - lining pieces are treated as self in the old code so they are bundled together in the request
            piece_type = [piece_type] if piece_type != "self" else ["self", "lining"]

            self._data = self._data[
                self._data["body_piece_type"].isin(piece_type)
            ].reset_index()

        if material_code:
            self._data = self._data[
                self._data["garment_piece_material_code"] == material_code
            ].reset_index()
            self._descrim_hash_options["material_code"] = material_code

        # this body lookup must be safe
        self._body_ref = dict(
            queries.get_body_by_id(
                self._data["body_piece_body_id"].iloc[0], hasura=hasura
            )["meta_bodies"][0]
        )

        self._delegate_row = dict(self._data.iloc[0])

        self._contracts_failing = self._delegate_row.get("contracts_failing", [])
        self._rank = self._delegate_row.get("rank")
        self._created_at = self._delegate_row["created_at"]
        self._normed_size = self._delegate_row.get("normed_size")
        # remove the petite thing from the VS key if its there
        self._data["key"] = self._data["key"].map(lambda x: x.split("_")[0])

        self._init_data()
        self._sku_versioned = f"{self._sku} V{self._body_version}"

        self._descrim_hash = res.utils.hash_dict_short(self._descrim_hash_options)

        self._key = (
            f"{self._sku_versioned}-{self._one_number}-{self._descrim_hash}".replace(
                " ", "-"
            )
        )

        if (min_body_version or 0) > self.body_version:
            raise Exception(
                f"The body version of the meta one is not accepted: current version {self.body_version} < suggested min version {min_body_version}"
            )

        # note the one number must be a string

    @property
    def one_code(self):
        try:
            make_instance = "something"  # add it on the end if known
            one_code = getattr(self, "_one_code", None)
            # try derived it
            if not one_code:
                first_part = f"{self.body_code}-V{self.body_version}".replace("-", "")
                second_part = f"G{self._rank}-{self.normed_size}"
                return f"{first_part}-{second_part}"
            return one_code
        except:
            res.utils.logger.warn(
                "error determining the one code on this code path. it is recommended to read it from data rather that derive it"
            )
            # error determining the one code due to bad code path
            # ew should have this as a property of the data so it is read in rather than computing
            return "________"

    def map_body_piece_operations(self):
        """
        Maps operations to body pieces
        can also be used to just iterate on groups
        """
        from res.flows.meta.pieces import load_dictionary

        res.utils.logger.info("loading the lookup")
        pc = load_dictionary()
        lookup = dict(pc[["tag_abbreviation", "sew_symbol_key"]].dropna().values)

        groups = {}
        for p in self:
            if p.piece_name.is_printable:
                name = p.piece_name.get_grouping_symbol_name(lookup)
                if name:
                    if name not in groups:
                        groups[name] = []
                    groups[name].append(p.piece_name.name)

        duck = res.connectors.load("duckdb")
        data = duck.execute(
            f""" SELECT * FROM 's3://res-data-platform/data-lake/cache/meta/construction/piece_operations.parquet'  where body_code = '{self.body_code}'   """
        )

        data["pieces"] = data["group_symbol_key"].map(lambda x: groups.get(x))
        return data

    def get_pieces_hash(self, raw=False, write=False):
        """
        TBD: we generate a hash that describes the uniqueness of the style
        - its not obvious what this should be but we use
          - the body piece key because it contains the piece name, body and version
          - the artwork uri because it was placed specifically
          - the material code
          - the material offset because it may have been buffered in different ways
        - we sort values based on the date of the body piece
        - we use specific names for the keys in the pieces that are stored

        See Also MetaOneRequest
        """
        d = self._data[
            [
                "key",
                "artwork_uri",
                "piece_metadata",
                "garment_piece_material_code",
                "garment_piece_color_code",
                "body_piece_created_at",
            ]
        ].rename(
            columns={
                "garment_piece_color_code": "color_code",
                "garment_piece_material_code": "material_code",
            }
        )

        d = d.sort_values("body_piece_created_at")
        d["offset_size_inches"] = d["piece_metadata"].map(
            lambda x: x.get("offset_size_inches")
        )

        d = {
            "pieces": d.drop(["piece_metadata", "body_piece_created_at"], 1).to_dict(
                "records"
            )
        }
        pieces_hash = res.utils.uuid_str_from_dict(d)

        # we store the material hash in the header too!!! these are different hashe
        piece_material_hash = self.get_non_sized_piece_material_hash()

        if write == True:
            res.utils.logger.info(
                f"registering the pieces hash {pieces_hash} and material hash {piece_material_hash} on the style'{self.name}' ({self.style_id}) while saving the base size"
            )
            # we update the style header with the pieces hash and update the registry with hash, body code and timestamp for ranking
            helpers.try_register_pieces_hash(
                self.style_id,
                pieces_hash,
                piece_material_hash,
                self.body_code,
                self.created_at,
            )

        return d if raw else pieces_hash

    @property
    def one_number(self):
        return getattr(self, "_one_number")

    @staticmethod
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def safe_load(sku):
        return MetaOne(sku)

    @property
    def created_at(self):
        return self._created_at

    @property
    def body_file_uploaded_at(self):
        stored = self._body_ref.get("body_file_uploaded_at")
        if not stored:
            s3 = res.connectors.load("s3")
            # seek the file time stamp
            body_lower = self.body_code.lower().replace("-", "_")

            expected_bw_files = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/v{self.body_version}"

            files = [
                f for f in s3.ls_info(expected_bw_files) if ".bw" in f["path"].lower()
            ]
            if len(files):
                return res.utils.dates.utc_iso_string_of(files[0]["last_modified"])
        return stored

    def get_style_size_id(self):
        return self._data.iloc[0]["style_sizes_id"]

    def get_style_rank(self):
        return self._data.iloc[0]["rank"]

    def get_piece_from_meta_code(self, code):
        """
        the meta code is the part without the body and version - so we can resolve the piece from that too
        """
        key = f"{self.versioned_body_code}-{code}"
        return self[key]

    def is_label(self):
        piece_codes = self._data["meta_piece_code"].values
        return len(piece_codes) == 1 and "LBLBYPNL" in piece_codes[0]

    @property
    def versioned_body_code(self):
        return f"{self.body_code}-V{self.body_version}"

    @staticmethod
    def get_cut_files_for_all_sizes(body_code, body_version):
        return list(
            res.connectors.load("s3").ls(
                f"s3://meta-one-assets-prod/bodies/cut/{body_code.replace('-','_').lower()}/v{body_version}"
            )
        )

    @staticmethod
    def get_cut_file_list_from_params(
        body_code,
        body_version,
        size_code,
        plt_only=True,
        strict=True,
        ignore_size=False,
    ):
        """
        today the .PLT files are strictly all we want in the new format
        """
        path = f"s3://meta-one-assets-prod/bodies/cut/{body_code.replace('-','_').lower()}/v{body_version}"
        if ignore_size == False:
            path = f"{path}/{size_code.lower()}"
        l = list(res.connectors.load("s3").ls(path))

        def as_key(p):
            key = f"{body_code}-V{body_version}"
            if strict:
                return key in p
            key = key.lower().replace("-", "_")
            return key in p.lower().replace("-", "_")

        l = [
            item
            for item in l
            if ("stamper_" in item.lower() or "fuse_" in item.lower()) and as_key(item)
        ]

        if plt_only:
            l = [item for item in l if ".plt" in item.lower()]
        return l

    def get_cut_files_list(self, plt_only=True, strict=True, ignore_size=False):
        return MetaOne.get_cut_file_list_from_params(
            self.body_code,
            self.body_version,
            self.size_code,
            plt_only=plt_only,
            strict=strict,
            ignore_size=ignore_size,
        )

    def get_pom_files_list(self):
        l = list(
            res.connectors.load("s3").ls(
                f"s3://meta-one-assets-prod/bodies/3d_body_files/{self.body_code.replace('-','_').lower()}/v{self.body_version}/extracted/rulers"
            )
        )
        return [i for i in l if i[-4:] in [".csv"]]

    def _init_data(cls):
        # THIS IS RISKY: CAN WE HAVE MIXED TYPES AND IF SO WE NEED TO MODEL IT ON THE PIECE
        cls._metadata = cls._data["metadata"].iloc[0]
        cls._flow = (cls._body_ref.get("metadata") or {}).get("flow", "3d")
        cls.is_directional = cls._metadata.get("print_type") == "Directional"
        cls._body_version = _try_parse_version_from_row(dict(cls._data.iloc[0]))
        cls._body_code = cls._data.iloc[0]["body_code"]
        cls._name = cls._data.iloc[0]["name"]
        cls._size_code = cls._data.iloc[0]["style_sizes_size_code"]

        for col in ["outline", "notches", "corners", "seam_guides", "internal_lines"]:
            cls._data[col] = cls._data[col].map(safe_loads)
        cls._pieces = [meta_one_piece(row, cls) for row in cls._data.to_dict("records")]
        cls._pieces = {p.key: p for p in cls._pieces}

    def test_piece_drift(self, plot=False, threshold=0.02):
        """
        is the color placement lagging the body
        """
        if self._delegate_row.get("metadata", {}).get("is_unstable_material"):
            return False
        for p in self:
            olim = p.piece_image_outline
            ol = p.piece_outline
            diff = Polygon(ol).area - Polygon(olim).area
            res.utils.logger.info(f"{self.sku} area diff {diff}")
            res.utils.logger.info(f"{self.sku} area factor {diff/Polygon(ol).area}")

            if plot:
                from geopandas import GeoDataFrame

                GeoDataFrame([ol, olim], columns=["geometry"]).plot()
            if abs(diff / Polygon(ol).area) > threshold:
                return True
        return False

    def validate(self, bootstrap_failed=None, deep=False):
        """
        we run a number of tests on the piece names, linked files and geometries
        """

        # read any from the database and re-validate
        failed_contracts = bootstrap_failed or []
        res.utils.logger.info(f"validating from bootstrapped {failed_contracts}....")
        piece_validations = {}
        for p in self:
            if p.piece_type != "unknown":
                errors = p.piece_name.validate(expected_version=self.body_version)
                if len(errors):
                    piece_validations[p.key] = errors

        for p in self:
            # just check for any one missing - assume valid uri or it blows up
            if p["uri"] and p.piece_name.is_printable:
                if not res.connectors.load("s3").exists(p["uri"]):
                    res.utils.logger.warn(f"{p['uri']} does not exist")
                    failed_contracts.append("ALL_COLOR_PIECES")
                    break

        if len(piece_validations):
            res.utils.logger.warn(piece_validations)
            failed_contracts.append("PIECE_NAMES")

        # TODO test geometries

        if deep:
            if self.test_piece_drift():
                failed_contracts.append("COLOR_PLACEMENT_LAGS_BODY_PIECE")

        # test linked assets like poms and stampers

        return list(set(failed_contracts))

    def get_body(self):
        """
        get the body data for example if you want to look at stampers

            d = m1.get_body()
            from res.flows.meta.ONE.geometry_ex import geometry_data_to_hpgl
            geometry_data_to_hpgl(d[d['body_pieces_type']=='stamper'].rename(columns={
                'body_pieces_piece_key':'key'}),
            filename=SOME_OUT_FILE, plot=True)
        """
        bid = helpers._body_id_from_body_code_and_version(
            self.body_code, self.body_version
        )

        b = queries.get_body_by_id_and_size(id=bid, size_code=self.size_code)

        return b

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def status_at_node(
        cls,
        sample_size=None,
        request=None,
        node="Meta.ONE.Style",
        stage="Generate Meta.ONE",
        status="Active",
        sizes=None,
        object_type=None,
        validate_paths=False,
        bootstrap_failed=None,
        reset_validate=False,
        **kwargs,
    ):
        """
        TODO: implement status at node interface


        bootstrap_failed: experimental - some processes may combine status that we have saved with additional context but it would be better a call to validate is idempotent
        """
        from schemas.pydantic.meta import MetaOneResponse
        from dateparser import parse

        # playing with merging contracts fail on validate and passed in but really they should be the same

        if not bootstrap_failed:
            bootstrap_failed = []

        # this flows needs more thought - the issue is transactions around partially saving sizes in the same style and setting contracts failed
        # here we are saying there is a mode where we ignore what is in the style until the final status check
        # this makes sense if this node is the only thing that can do certain types of validation or if the state is passed into the node
        contracts_failed = (
            cls.validate(bootstrap_failed=bootstrap_failed + cls.contracts_failing)
            if not reset_validate
            else bootstrap_failed
        )

        style_sizes = []
        # the last status is the one that we need - make sure same for all calls to get_style_status_by_sku which turns multiple for re-names
        status = queries.get_style_status_by_sku(cls.style_sku)["meta_styles"]
        status = sorted(status, key=lambda x: parse(x["modified_at"]))[-1]
        style_sizes = status["style_sizes"]

        good_sizes = [s["size_code"] for s in style_sizes if s["status"] == "Active"]
        # all or nothing for now - do not say any sizes are ready unless all of them are but find a good way to track failed sizes (there should not be except for db bugs)
        if len(contracts_failed):
            good_sizes = []

        return MetaOneResponse(
            body_code=cls.body_code,
            print_type=cls._metadata.get("print_type"),
            body_version=cls.body_version,
            style_name=cls.name,
            sku=cls.sku,
            # stage="Done",
            # node
            size_code=sample_size,
            printable_pieces=cls.get_previews_uris(),
            trace_log=kwargs.get("trace_log"),
            status="Active" if len(contracts_failed) == 0 else "Failed",
            sizes=good_sizes,
            available_sizes=[s["size_code"] for s in style_sizes],
            id=cls.get_style_size_id(),
            contracts_failed=contracts_failed,
            metadata=cls._metadata,
            piece_material_hash=cls.get_non_sized_piece_material_hash(),
        )

    def as_asset_response(cls, validate=True, **kwargs):
        from schemas.pydantic.meta import MetaOneResponse

        contracts_failed = []
        if validate:
            contracts_failed = cls.validate()
        return MetaOneResponse(
            body_code=cls.body_code,
            body_version=cls.body_version,
            style_name=cls.name,
            sku=cls.sku,
            # stage="Done",
            status="Active",
            id=cls.get_style_size_id(),
            contracts_failed=contracts_failed,
            metadata=cls._metadata,
        )

    def nest(self, piece_types=PRINTABLE_PIECE_TYPES, return_plot=False, **kwargs):
        from res.learn.optimization.nest import nest_dataframe

        piece_type_col = (
            "body_pieces_type"
            if "body_pieces_type" in self._data.columns
            else "body_piece_type"
        )
        plot = kwargs.get("should_plot", True)
        df = nest_dataframe(
            self._data[self._data[piece_type_col].isin(piece_types)].reset_index(
                drop=True
            ),
            return_plot=return_plot,
            geometry_column="outline",
            plot=plot,
            **kwargs,
        )

        return df

    def compute_costs(self):
        """
        - Create one cost fetch
        - add the nesting statistics (per material)
        """
        return {}

    def get_sized_piece_material_hash(self):
        return res.utils.uuid_str_from_dict(
            dict(self._data[["body_piece_key", "garment_piece_material_code"]].values)
        )

    def get_non_sized_piece_material_hash(self):
        """
        this is the import one used for hashing the BM contract and laster we use the size as well
        at style level we saved non sized
        """
        return res.utils.uuid_str_from_dict(
            dict(self._data[["key", "garment_piece_material_code"]].values)
        )

    def get_bms_request_payload(self):
        """
        generates the request to save a BMS contract entry
        """
        pmhash = self.get_non_sized_piece_material_hash()
        return {
            "body_code": self.body_code,
            "body_version": self.body_version,
            "size_code": self.size_code,
            "reference_style_size_id": self.style_size_id,
            "style_pieces_hash": pmhash,
            "created_at": res.utils.dates.utc_now_iso_string(),
            "id": res.utils.uuid_str_from_dict(
                {"style_hash": pmhash, "size_code": self.size_code}
            ),
        }

    @property
    def piece_keys(self):
        return self._data["key"].unique()

    @property
    def dxa_previews(self):
        for p in self:
            yield p.dxa_preview

    @staticmethod
    def status(sku, body_version=None, hasura=None, return_status_info_object=False):
        _status = helpers.get_style_size_status(sku, body_version, hasura=hasura)

        if not return_status_info_object:
            return _status

        _status["body_code"] = _status["sku"].split(" ")[0].strip()
        _status["material_code"] = _status["sku"].split(" ")[1].strip()
        _status["flags"] = []
        if not _status["is_body_valid"]:
            _status["flags"].append("PENDING_BODY")
            _status["status"] = "PENDING_BODY"
        if not _status["is_style_active"]:
            if _status.get("status") is None:
                _status["status"] = "PENDING_PIECES"
            _status["flags"].append("PENDING_PIECES")

        return MetaOneStatus(**_status)

    def status_info(self, return_status_info_object=False):
        d = {
            "name": self.name,
            "sku": self.sku,
            "style_sku": self.style_sku,
            "body_code": self.body_code,
            "body_version": self.body_version,
            "material_code": self.sku.split(" ")[1].strip(),
            "print_type": self._metadata.get("print_type"),
            "is_unstable_material": self._metadata.get("is_unstable_material"),
            "flow": self._flow,
            "status": "Active",
            "printable_pieces": self.printable_piece_codes,
            "printable_pieces_previews": [f for f in self._data["uri"].dropna()],
            "cut_pieces": self.non_printable_piece_codes,
            "flags": [],
            "flag_owners": [],
        }

        return d if return_status_info_object == False else MetaOneStatus(**d)

    def deactivate(self):
        return queries.add_style_contract_failed("STYLE_ONE_READY")

    @staticmethod
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def for_sample_size(sku):
        """
        pass a three part sku and resolve to a 4 part sku based on the bodies samples size code
        """
        from res.flows.meta.ONE.body_node import BodyMetaOneNode

        three_parts = f" ".join([s.strip() for s in sku.split(" ")[:3]])
        body_code = three_parts.split(" ")[0].strip()
        data = BodyMetaOneNode.get_body_metadata(body_code)
        sku = f"{three_parts} {data['sample_size']}"
        res.utils.logger.debug(f"Resolved sample size sku {sku}")
        try:
            return MetaOne(sku)
        except Exception as ex:
            res.utils.logger.warn(
                f"Failing to load the meta one for for the sku {sku} :> {ex}"
            )
            return None

    def build(
        sku,
        locally=True,
        try_naming=True,
        force_upsert_style_header=False,
        update_kafka=False,
        body_version=None,
        lookup_body_piece_names=False,
        flow="3d",
    ):
        """
        build a meta one that does not exist locally
        TODO implement request via kafka
        """

        if flow == "3d":
            res.utils.logger.info(f"building for the 3d flow")
            from res.flows.meta.marker import MetaMarker

            return MetaMarker.from_sku(sku, body_version=body_version).build()

        payload = helpers.compile_piece_info_for_style_size(
            sku, try_naming=try_naming, body_version=body_version
        )
        if not locally:
            pass  # TODO send to kafka in some env
        return asset_builder.make_style_from_payload(
            payload,
            force_upsert_style_header=force_upsert_style_header,
            update_kafka=update_kafka,
            body_version=body_version,
            lookup_body_piece_names=lookup_body_piece_names,
        )

    def from_meta_marker_uri(uri):
        from res.flows.meta.marker import MetaMarker

        return MetaMarker(uri).as_meta_one()

    @property
    def style_id(self):
        return self._delegate_row.get("id")

    @property
    def style_size_id(self):
        return self._delegate_row.get("style_sizes_id")

    @property
    def sku_versioned(self):
        return self._sku_versioned

    @property
    def sku(self):
        """
        4 components BMCS
        """
        return self._sku

    @property
    def name(self):
        return self._name

    @property
    def style_sku(self):
        """
        the three components BMC
        """
        return " ".join(self.sku.split(" ")[:3])

    @property
    def body_code(self):
        return self._body_code

    @property
    def primary_color_code(self):
        return self.sku.split(" ")[2].strip()

    @property
    def size_code(self):
        return self._size_code

    def get_brand_name(self):
        # todo get the brand name from the code
        return self.sku.split("-")[0]

    @property
    def normed_size(self):
        """
        Todo this is not correct
        """
        if not self._size_lu_data:
            self._size_lu_data = helpers.accounting_size_to_normed_size_lu()

        normed_size = self._size_lu_data.get(self.size_code)
        if normed_size:
            return normed_size

        normed_size = self.size_code[-2:]
        if normed_size == "00":
            # Z2300 - Z5400 (we should probably look up the sizes but need to investigate)
            normed_size = self.size_code.replace("Z", "").replace("0", "")
        if normed_size == "One Size Fits All":
            return "OS"
        normed_size = normed_size.replace("-", "")

        return normed_size

    @property
    def chart_size(self):
        """
        this is a stopgap approach to resolve the sizes from a dumb cache. size management needs to be improved
        """
        return helpers.accounting_size_to_size_chart(self.size_code)

    @property
    def body_version(self):
        return self._body_version

    @property
    def material_codes(self):
        return list(self._data["garment_piece_material_code"])

    @property
    def printable_piece_codes(self):
        n = []
        for p in self:
            if p.piece_name.is_printable:
                n.append(p.piece_name.name)
        return n

    @property
    def non_printable_piece_codes(self):
        n = []
        for p in self:
            if not p.piece_name.is_printable:
                n.append(p.piece_name.name)
        return n

    @property
    def input_objects_s3(self):
        """
        we store objects here.
        glb = f"{root}/3d.glb"
        point_cloud = f"{root}/point_cloud.json"
        front_image = f"{root}/front.png"
        """
        ext = f"{self.body_code.replace('-', '_')}/v{self.body_version}/{self.primary_color_code}".lower()
        return f"s3://meta-one-assets-prod/color_on_shape/{ext}"

    def _repr_html_(self):
        return self._data._repr_html_()

    def __getitem__(self, key):
        return self._pieces[key]

    def __iter__(self):
        for _, v in self._pieces.items():
            yield v

    def _as_preview_path(cls, key):
        return f"s3://{RES_DATA_BUCKET}/meta-one-assets/style-requests-previews/{cls.sku.replace(' ','-')}/v{cls.body_version}/{key}.png"

    def get_previews_uris(cls):
        s3 = res.connectors.load("s3")
        root = cls._as_preview_path("KEY").replace(f"KEY.png", "")
        l = list(s3.ls(root))

        def _filter(f):
            """
            in case we save things before that are not in our latest set we need this safety
            """
            key = f.split("/")[-1].split(".")[0]
            return key in cls.piece_keys

        l = [f for f in l if _filter(f)]

        return l

    def get_order_html(self, exit_factory_date, make_signed_url=True):
        """
        To preview we can do this
        H = m1.get_order_html(exit_factory_date=res.utils.dates.utc_now(), make_signed_url=False)
        s3.write_html_as_pdf_to_s3(H, "s3://res-data-platform/samples/a.pdf")
        s3._download('s3://res-data-platform/samples/a.pdf')

        or look at the HTML in jupyter
        from IPython.display import HTML(H)

        default args are used to get a presigned url to post to airtable
        """
        from res.flows.meta.ONE.annotation import get_order_html_2, get_order_html

        html_stuff1 = get_order_html(self, exit_factory_date=exit_factory_date)
        html_stuff2 = get_order_html_2(self, exit_factory_date=exit_factory_date)

        if make_signed_url:
            uri = f"s3://res-data-platform/order-pdfs-by-one-number/v2/{self.one_number}.pdf"
            res.utils.logger.info(uri)
            res.connectors.load("s3").write_html_as_pdf_to_s3(
                html_stuff2,
                out_uri=uri,
                make_signed_url=make_signed_url,
            )
        if make_signed_url:
            uri = f"s3://res-data-platform/order-pdfs-by-one-number/v1/{self.one_number}.pdf"
            res.utils.logger.info(uri)
            res.connectors.load("s3").write_html_as_pdf_to_s3(
                html_stuff1,
                out_uri=uri,
                make_signed_url=make_signed_url,
            )

        return html_stuff2

    def _should_preview_piece(cls, piece_name):
        return piece_name.is_printable

    def build_asset_previews(cls, modified_before=None, use_thumbnail=False):
        """
        we will generate any previews modified since a date: TODO
        """

        def thumbnail_of(im, factor=4):
            im.thumbnail(size=(int(im.size[0] / factor), int(im.size[1] / factor)))
            return im

        s3 = res.connectors.load("s3")
        root = cls._as_preview_path("KEY").replace(f"KEY.png", "")
        existing = {f["path"]: f["last_modified"] for f in s3.ls_info(root)}
        skip = (
            [f for f, v in existing.items() if v < modified_before.replace(tzinfo=None)]
            if modified_before
            else []
        )

        res.utils.logger.debug(
            f"Skip list for asset previews based on modified date {modified_before}: {skip}"
        )

        paths = []

        for p in cls:
            path = cls._as_preview_path(p.key)
            if path in skip or not cls._should_preview_piece(p.piece_name):
                continue

            paths.append(path)
            # set this here so we can run the contract as though its an image we can load
            p._row["uri"]

            res.utils.logger.info(
                f"saving image {path} thumbnail option: {use_thumbnail}"
            )
            s3.write(
                path,
                p.dxa_preview if not use_thumbnail else thumbnail_of(p.dxa_preview),
            )
        return paths

    def get_costs(self):
        s = self.get_nesting_statistics()
        s = res.utils.dataframes.replace_nan_with_none(s)
        s = s.to_dict("records")
        # anything else

        return s

    def read_costs(self):
        c = (
            meta_one_queries.get_one_costs_by_one_number(self._one_number)
            if self._one_number
            else {}
        )
        c = {"make_costs": c, "sku": self.sku}
        c.update(
            meta_one_queries._get_costs_match_legacy(
                self.sku, stat_name="piece_stats_by_material"
            )
        )
        return c

    @property
    def body_piece_count(self):
        return self._body_ref["body_piece_count_checksum"]

    def style_cost_capture(self, costs=None):
        """
        this is called and we augment the cost data - it could be when we are computing it in the node or after
        we use the size yield data + other data that is known on the body and elsewhere

        The total costs and the dataframe are both saved on the style

        """
        from res.flows.finance.queries import (
            get_material_rates_by_material_codes,
            get_node_rates,
        )
        from stringcase import titlecase

        SIZE_YIELD_FACTOR = 1 / 1.53

        node_rates = get_node_rates(expand=True)

        material_props = get_material_rates_by_material_codes(self.material_codes)

        number_of_pieces = self.body_piece_count

        if costs is None:
            # if they are not passed in we can try to load them
            costs = self.read_costs()
            costs = costs.get("piece_stats_by_material")
            if len(costs) == 0:  # or some validation
                # if we really have to we can compute them again
                costs = self.get_costs()
        else:
            costs = costs.get("piece_stats_by_material")

        style_type = "woven"
        print_type = "directional" if self.is_directional else "placement"
        height_statistic = "height_nest_yds_raw"
        if self._metadata.get("is_unstable_material"):
            height_statistic = "height_nest_yds_block_buffered"
            style_type = "knit"
            if not self.is_directional:
                height_statistic = "height_nest_yds_buffered"

        # missing cost error here - check database that these all exist or make them
        costs = pd.DataFrame(costs)[["material_code", height_statistic]]

        material_costs = pd.merge(costs, material_props, on="material_code")
        material_costs["quantity"] = costs[height_statistic]

        # at sew we based on on the sewing effort estimate
        node_rates.loc[node_rates["item"] == "Sew", "quantity"] = self._body_ref.get(
            "estimated_sewing_time", 0
        )
        # at cut we base it on the number of pieces
        node_rates.loc[node_rates["item"] == "Cut", "quantity"] = number_of_pieces
        # the size yield (nest height) which is the sum of the printing is the labor rate factor to use
        node_rates.loc[node_rates["item"] == "Print", "quantity"] = material_costs[
            "quantity"
        ].sum()

        new_rows = [
            {
                "item": "Trim",
                "category": "Materials",
                "rate": self._body_ref.get("trim_costs", 0),
                "quantity": 1,
                "unit": "bundle",
            }
        ]
        node_rates = pd.concat(
            [node_rates, pd.DataFrame(new_rows), material_costs]
        ).reset_index(drop=True)

        # add the regular costs as the quantity used for overheads
        node_rates.loc[node_rates["item"] == "Overhead", "quantity"] = (
            node_rates["rate"] * node_rates["quantity"]
        ).sum()

        # now compute total costs
        node_rates["cost"] = node_rates["rate"] * node_rates["quantity"]

        node_rates.columns = [titlecase(c) for c in node_rates.columns]

        # to get total size yield assume these fields we do this
        ##c[(c['Category']=='Materials')&(c['Unit']=='Yards')]['Quantity'].sum()
        # the total cost of the one is c['cost'].sum()
        res.utils.logger.info(
            f"Style {self.sku} with print type {print_type} in material type {style_type} has a total cost {node_rates['Cost'].sum()}"
        )
        return (
            node_rates[["Item", "Category", "Rate", "Unit", "Quantity", "Cost"]]
            .fillna(0)
            .round(3)
        )

    @staticmethod
    def read_costs_for_one(one_number):
        """
        simple method to merge one and style costs
        """
        c = meta_one_queries.get_one_costs_by_one_number(one_number)
        sku = c.pop("sku")
        c = {"make_costs": c, "sku": sku}
        c.update(
            meta_one_queries._get_costs_match_legacy(
                sku, stat_name="piece_stats_by_material"
            )
        )
        return c

    def piece_type_counts(self, groupby_color=True, filter_piece_codes=None):
        """
        The types of pieces sets broken down by material etc
        Used to both define the meta one i.e. what sorts of pieces it has and for printing we can generate separate print assets

        filter by the meta pieces
        for piece BG-4004-V2-JKTFSPNLRT-S use only JKTFSPNLRT-S
        """

        d = self._data.rename(
            columns={
                "garment_piece_material_code": "material_code",
                "garment_piece_color_code": "color_code",
                "body_piece_type": "type",
            }
        )

        def coerce_type(s):
            """
            not very satisfying
            we want to preserve the piece type but in generate print assets lining pieces should be treat as self pieces
            actually we probably should not have split assets by type as such in the first place just have one asset per material
            in a new system without print assets we wont need this and this is the only function that is required to split print assets
            note: the meta one will include lining pieces in self pieces when actually requesting the self asset based on a previous change
            """
            return s if s == "block_fuse" else "self"

        d["type"] = d["type"].map(coerce_type)

        if filter_piece_codes:
            d = d[d["key"].isin(filter_piece_codes)].reset_index(drop=True)

        d = d.groupby(
            ["material_code", "type"] + (["color_code"] if groupby_color else [])
        ).agg({"key": [len, list]})
        d.columns = d.columns.to_flat_index()
        return d.rename(
            columns={("key", "len"): "count", ("key", "list"): "piece_keys"}
        )

    def make_nesting_comparison(self, factor=50, **kwargs):
        df1 = self.get_nesting_statistics(category="compensated", **kwargs)
        df50 = self.get_nesting_statistics(
            category="compensated", factor=factor, **kwargs
        )
        df = pd.concat([df1, df50])

        df["yard_per_piece"] = df["height_nest_yds_compensated"] / df["piece_count"]
        df["usage"] = df["yard_per_piece"] * df["piece_count"].max()
        df["perc_save"] = df["usage"].diff(-1) / df["usage"].max() * 100
        return df.reset_index()

    # interface
    def get_nesting_statistics(self, **kwargs):
        """
        load the material properties filtered by materials we have
        """
        from res.learn.optimization.nest import (
            nest_dataframe,
            evaluate_nest_v1,
            pixels_to_yards,
        )
        from res.flows.make.materials import get_material_properties

        nestable = self._data[
            ["key", "outline", "body_piece_type", "garment_piece_material_code"]
        ].rename(columns={"garment_piece_material_code": "material_code"})

        if kwargs.get("factor"):
            factor = kwargs.get("factor")
            nestable = (
                pd.concat([nestable for i in range(factor)])
                .reset_index()
                .drop("index", 1)
            )

        # the model will load a map of materials and properties with proper null handling
        mat_props = get_material_properties(
            material_codes=list(nestable["material_code"])
        )

        def get_material_buffer(row):
            material_code = row["material_code"]
            buffer = (mat_props[material_code]["offset_size"] or 0) * 300
            return buffer

        def add_material_buffer(row):
            material_code = row["material_code"]
            buffer = (mat_props[material_code]["offset_size"] or 0) * 300

            return row["outline"].buffer(buffer) if buffer > 0 else row["outline"]

        # how we take three categories of nests and compute statistics.
        # we need to use a correct format for PPP so this will take a transformer in that mode
        nestable["category"] = "raw"
        nestable["twice_offset"] = None

        BB = nestable.copy()
        BB["category"] = "block_buffered_kernel"
        BB["twice_offset_buffer"] = 2 * BB.apply(get_material_buffer, axis=1)

        # add buffered only
        BO = nestable.copy()
        BO["category"] = "buffered"
        BO["outline"] = BO.apply(add_material_buffer, axis=1)
        #
        B = nestable.copy()
        B["category"] = "physical"
        B["outline"] = B.apply(add_material_buffer, axis=1)
        B["stretch_x"] = B["material_code"].map(
            lambda x: mat_props[x]["compensation_width"]
        )
        B["stretch_y"] = B["material_code"].map(
            lambda x: mat_props[x]["compensation_length"]
        )
        C = B.copy()
        C["category"] = "compensated"

        if kwargs.get("category") == "compensated":
            nestable = pd.concat([C]).reset_index()
        else:
            nestable = pd.concat([nestable, B, C, BO, BB]).reset_index()

        # return nestable

        nestable["outline"] = nestable["outline"].map(lambda x: x.convex_hull.exterior)
        nestable["stretch_x"] = nestable["stretch_x"].fillna(1)
        nestable["stretch_y"] = nestable["stretch_y"].fillna(1)

        results = []

        # the contract should out these shapes at the origin anyway
        nestable["outline"] = nestable["outline"].map(shift_geometry_to_origin)

        mat_props = {
            k: v for k, v in mat_props.items() if k in list(nestable["material_code"])
        }

        for material_code, mat_data in nestable.groupby("material_code"):
            mat_prop_row = mat_props[material_code]

            output_bounds_width = (
                mat_props[material_code]["cuttable_width"] or 0
            ) * 300
            twice_offset = (mat_props[material_code]["offset_size"] or 0) * 300 * 2

            for category, data in mat_data.groupby("category"):
                shrinkage = 0 if category != "block_buffered_kernel" else twice_offset
                if shrinkage:
                    print("apply shrinkage", shrinkage)
                stretch_x = data.iloc[0]["stretch_x"]
                stretch_y = data.iloc[0]["stretch_y"]

                result = nest_dataframe(
                    data.reset_index(),
                    geometry_column="outline",
                    stretch_x=stretch_x,
                    stretch_y=stretch_y,
                    output_bounds_length=3000000,
                    output_bounds_width=output_bounds_width - shrinkage,
                    # plot=True,
                )

                # capture the results
                props = evaluate_nest_v1(
                    result, should_update_shape_props=True, geometry_column="outline"
                )
                props = {
                    f"{k}_{category}": v
                    for k, v in props.items()
                    if k not in ["piece_count"]
                }
                props["piece_count"] = len(data)
                props["material_code"] = material_code
                mat_prop_row.update(props)
            results.append(mat_prop_row)
        results = pd.DataFrame(results)

        results["twice_buffer_offset_yards"] = pixels_to_yards(
            2 * results["offset_size"] * 300
        )

        # this implements Robs rule - im worried about burying this in here - advisable to compute this somewhere else
        results["twice_buffer_offset_yards_with_tolerance"] = results[
            "twice_buffer_offset_yards"
        ] + pixels_to_yards((2 / 3) * 300)

        # we add the thing with tolerance
        results["height_nest_yds_block_buffered"] = (
            results["height_nest_yds_block_buffered_kernel"]
            + results["twice_buffer_offset_yards_with_tolerance"]
        )

        return results.replace({np.nan: None})

    def to_meta_one_request(self, as_typed=True):
        d = {
            "style_name": "Dressed In Joy Peasant Dress - SPRING - Golden in Light Weight Tencel",
            "sku": self.sku,
            "color_code": self.sku.split(" ")[2].strip(),
            "body_code": self.body_code,
            "body_version": self.body_version,
            "size_code": self.sku.split(" ")[-1].strip(),
            "pieces": [
                {
                    "key": p["key"],
                    "material_code": p["garment_piece_material_code"],
                    "artwork_uri": p["artwork_uri"],
                    "color_code": p["garment_piece_color_code"],
                    "offset_size_inches": p["piece_metadata"].get("offset_size_inches")
                    or 0,
                    "uri": p["uri"],
                }
                for p in self._data.to_dict("records")
            ],
        }

        if as_typed:
            from schemas.pydantic.meta import MetaOneRequest

            d = MetaOneRequest(**d)
        return d

    @property
    def key(self):
        return self._key

    @property
    def is_valid(self):
        # there are some contracts we can let go at this stage
        PERMISSABLE = ["3D_MODEL_FILES"]
        failing = [d for d in self.contracts_failing if d not in PERMISSABLE]
        return len(failing) == 0

    @staticmethod
    def is_valid_meta_one(sku, body_version=None, quiet=False):
        try:
            m1 = MetaOne(sku)
            # if a body version is specified we are pending one that is after the requested
            if body_version:
                if m1.body_version >= body_version:
                    live_check = m1.validate()

                    return m1.is_valid and len(live_check) == 0
                else:
                    res.utils.logger.warn(
                        f"There is a meta one but the version is less than the one requested"
                    )
                    return False
            return True
        except:
            if not quiet:
                res.utils.logger.warn(traceback.format_exc())
            res.utils.logger.warn(
                f"The meta one can not be loaded so treated as invalid"
            )
            return False

    @staticmethod
    def remove_contracts(sku):
        """
        TODO remove contracts from the meta one
        """
        hasura = res.connectors.load("hasura")
        return queries.remove_style_contracts_failing(sku)

    @staticmethod
    def invalidate_meta_one(
        sku, body_version=None, contracts=None, merge=True, with_audit=False
    ):
        # add the meta one ready contract to the sku
        # get existing contracts and add on the [META_ONE_READY]
        contracts = contracts or ["META_ONE_READY"]
        m1 = None if with_audit == False else MetaOne.for_sample_size(sku)
        return queries.add_style_contract_failing(sku, contracts, m1=m1)

    @staticmethod
    def get_style_status_history(sku, full=False):
        hasura = res.connectors.load("hasura")

        result = hasura.tenacious_execute_with_kwargs(
            queries.GET_STYLE_STATUS_BY_SKU, metadata_sku={"sku": sku}
        )["meta_styles"]

        if not result:
            return None

        r = result[-1]
        sid = r["id"]

        result = hasura.tenacious_execute_with_kwargs(
            queries.GET_STYLE_STATUS_HISTORY_BY_STYLE_ID, style_id=sid
        )["meta_style_status_history"]

        d = {
            "style_id": sid,
            "sku": sku,
            "name": r["name"],
            "body_version": r["metadata"].get("body_version"),
            "contracts_failing": r["contracts_failing"],
            "piece_version_delta_test_results": r["piece_version_delta_test_results"],
            "audit_history": result,
        }

        return d

    @property
    def contracts_failing(self):
        body_contracts_failing = self._body_ref.get("contracts_failing", [])
        return list(set((self._contracts_failing or []) + body_contracts_failing or []))

    @property
    def is_legacy_flow(self):
        return self._flow != "3d"

    @property
    def piece_images(self):
        for piece in self:
            yield piece.piece_image

    @property
    def named_piece_images(self):
        for piece in self:
            yield piece["key"], piece.piece_image

    @property
    def labelled_piece_images(self):
        for piece in self:
            yield piece.labelled_piece_image

    @property
    def named_labelled_piece_images(self):
        for piece in self:
            yield piece["key"], piece.labelled_piece_image

    @property
    def piece_outlines(self):
        for piece in self:
            yield piece.piece_outline

    @property
    def named_buffered_piece_outlines(self):
        for piece in self:
            yield piece["key"], piece.buffered_piece_outline

    @property
    def named_piece_outlines(self):
        for piece in self:
            yield piece["key"], piece.piece_outline

    @property
    def notched_piece_image_outlines(self):
        for piece in self:
            yield piece.piece_image_outline

    @property
    def named_notched_piece_image_outlines(self):
        for piece in self:
            # TODO: safety first - we could use the outline when its a directional because they are close enough
            yield piece["key"], piece.labelled_piece_image_outline

    @property
    def named_notched_piece_raw_outlines(self):
        for piece in self:
            yield piece["key"], piece.get_physical_cutline()

    def get_bom_data(self):
        """
        for now a method that goes to source for the bom but in future we should ingest into our database
        """
        from res.flows.meta.ONE.bom import get_body_bom

        return get_body_bom(self.body_code)

    def update_product(self):
        """
        this should be registered but if its not we can update it here
        """

        from res.flows.dxa.styles.helpers import _update_product

        # def _update_product(style_id, style_sku, name, add_self_ref=False, hasura=None):
        return _update_product(self.style_id, self.style_sku, self.name)

    def save_nested_cutlines(
        self, filename, material_width_inches, paper_scale_x, paper_scale_y, **kwargs
    ):
        """ """
        from res.media.images.providers.dxf import DPI_SETTING, MM_PER_INCH, DxfFile
        from res.learn.optimization.nest import nest_dataframe
        from res.media.images.geometry import scale, invert_axis

        buffer = 10
        mm_width_for_nesting = material_width_inches * MM_PER_INCH  # / DPI_SETTING

        def _pixel_space_to_mm(shape):
            return scale(
                shape,
                paper_scale_x * MM_PER_INCH / DPI_SETTING,
                paper_scale_y * MM_PER_INCH / DPI_SETTING,
            )

        # SA: testing support for bodies - we can load a body and pack it for any material but materials are not mapped to pieces for bodies
        if "material" in kwargs and not isinstance(self, BodyMetaOne):
            piece_names = self._data[
                self._data.garment_piece_material_code == kwargs["material"]
            ].key.values
        else:
            piece_names = self._data.key.values

        # get all the treated cutlines as we would send to nesting (or PPP) - these have all the notches on them
        # althoguh meta ones only have printable pieces adding in the checks means the body subclass could also be used here which is useful
        cutline_df = pd.DataFrame(
            [
                {"key": k, "geometry": v}
                for k, v in self.named_notched_piece_raw_outlines
                if PieceName(k).is_printable and k in piece_names
            ]
        )

        # scale to mms for what we want to do below
        # this makes sense as a "model" of a nest - it does not have to match any nest
        # we are going to invert to the image space
        if kwargs.get("invert_y_axis", True):
            cutline_df["geometry"] = cutline_df["geometry"].map(invert_axis)
        cutline_df["geometry"] = cutline_df["geometry"].map(_pixel_space_to_mm)

        # nest the dataframe in mm scale for the given material so we can scale things and then nest them
        cutline_df = nest_dataframe(
            cutline_df.reset_index(),
            geometry_column="geometry",
            output_bounds_width=mm_width_for_nesting,
            output_bounds_length=mm_width_for_nesting * 100,
            buffer=kwargs.get("buffer", buffer),
            plot=kwargs.get("plot", False),
        )

        # rename, why not
        cutline_df["geometry"] = cutline_df["nested.original.geometry"]
        DxfFile.export_dataframe(
            cutline_df[["geometry"]],
            filename,
            point_conversion_op=lambda p: p.coords,
        )

        return cutline_df

    # - todo: figure out w sirsh where we can move this thing.
    def get_ppp_spec(self):
        ppp_spec = {}
        for (material_code, piece_type), row in self.piece_type_counts(
            groupby_color=False,
        ).iterrows():
            ppp_spec[(material_code, piece_type)] = [
                {
                    "key": key,
                    "make_sequence_number": 1,
                    "multiplicity": 50 if self.is_label() else 1,
                }
                for key in row["piece_keys"]
            ]
        return ppp_spec

    def get_pieces_info(self):
        """
        The pieces that are used in make e.g. completer node
        provides the piece inforamtion that is used for organising printable pieces at and beyond print

        """
        s3 = res.connectors.load("s3")

        pieces = [
            {
                # record id .... - > unique on ONE + Piece Number e.g 10101010 1/17
                # key is a formala
                # "ONE": [record_id],
                "Piece Name": piece.get("key"),
                "Piece Number": piece.get("piece_ordinal"),
                "Piece Preview": [
                    {
                        "filename": piece.get("key"),
                        "url": s3.generate_presigned_url(piece["uri"]),
                    }
                ],
                "Piece Set Size": piece.get("piece_set_size"),
                "Material": piece.get("garment_piece_material_code"),
            }
            for piece in self._data.to_dict("records")
        ]

        return pieces

        # todo return something that the API can use to update the cut app

    def create_cut_requests(
        self,
        one_number,
        paper_markers=[],
        production_cover_photos=[],
        brand_name=None,
        factory_order_pdf_url=None,
        make_one_prod_request_id=None,
        original_order_timestamp=None,
        flag_reason=None,
        factory_request_name=None,
    ):
        s3 = S3Connector()
        cut_request = CUT_REQUESTS.first(formula=f"{{ONE Code}}='{one_number}'")
        request_id = ""
        sku_body, sku_material, sku_color, sku_size = self.sku.split(" ")

        if cut_request:
            request_id = cut_request["id"]

        request_payload = {
            "Request Name": factory_request_name,
            "Original ONE Placed At": original_order_timestamp,
            "__onecode": one_number,
            "Brand Code": brand_name,
            "SKU": self.sku,
            "Body Code": sku_body,
            "Material Code": sku_material,
            "Color Code": sku_color,
            "Factory Order PDF": factory_order_pdf_url,
            "make_one_production_request_id": make_one_prod_request_id,
        }

        cover_images = []
        paper_marker_zip = []

        # Add Paper Markers
        for marker in paper_markers:
            paper_marker_zip.append(
                {
                    "url": marker.get("file").get("url"),
                    "filename": marker.get("file").get("name"),
                }
            )

        request_payload["Paper Markers"] = paper_marker_zip

        for photo in production_cover_photos:
            image_payload = {"url": photo.get("url")}
            cover_images.append(image_payload)

        request_payload["Production Cover Photo"] = cover_images
        if not request_id:
            updated_record = CUT_REQUESTS.create(request_payload)
            req_id = updated_record["id"]
        else:
            print("")
            req_id = request_id
            updated_record = CUT_REQUESTS.update(
                request_id,
                request_payload,
            )
            updated_record["id"]

        piece_data = (
            self._data[
                [
                    "key",
                    "piece_ordinal",
                    "garment_piece_material_code",
                    "piece_set_size",
                    "uri",
                ]
            ]
            .rename(
                columns={
                    "key": "Piece Name",
                    "piece_ordinal": "Piece Number",
                    "garment_piece_material_code": "Material",
                }
            )
            .to_dict("records")
        )
        style_pieces = []
        if req_id:
            for piece in piece_data:
                if not updated_record.get("fields").get(
                    "Pieces Created Name"
                ) or not piece.get("Piece Name") in updated_record.get("fields").get(
                    "Pieces Created Name"
                ):
                    style_pieces.append(
                        {
                            "ONE": [req_id],
                            "Piece Name": piece.get("Piece Name"),
                            "Piece Number": piece.get("Piece Number"),
                            "Piece Preview": [
                                {
                                    "filename": piece.get("Piece Name"),
                                    "url": s3.generate_presigned_url(piece["uri"]),
                                }
                            ],
                            "Piece Set Size": piece.get("piece_set_size"),
                            "Material": piece.get("Material"),
                        }
                    )

        if style_pieces:
            CUT_PIECES.batch_create(style_pieces, typecast=True)

        return req_id

    @staticmethod
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def _try_load_from_print_payload(
        body_code,
        body_version,
        material_code,
        color_code,
        size_code,
        one_number,
        sku=None,
        piece_type=None,
        **kwargs,
    ):
        # if not sku:
        #     sku = f"{body_code.strip()} {material_code.strip()} {color_code.strip()} {size_code.strip()}".upper()

        # safety norm
        if sku[2] != "-":
            sku = f"{sku[:2]}-{sku[2:]}"

        res.utils.logger.info(f"Meta one looking for sku {sku}")
        return MetaOne(
            sku,
            one_number,
            piece_type=piece_type,
            material_code=material_code,
            fully_annotate=kwargs.get("fully_annotate", False),
            multiplier=kwargs.get("multiplier"),
            is_healing=kwargs.get("is_healing"),
            is_extra=kwargs.get("is_extra"),
            make_instance=kwargs.get("make_instance", 0),
            healz_mode=kwargs.get("healz_mode"),
        )


def _get_body_stampers(body_code, body_version, filename=None, plot=True):
    """
    for now a visual tool but we can save stampers as hpgl somewhere and they can be printed
    for old m1 we would save these by size but this does them for the entire body
    """
    from res.flows.meta.ONE.geometry_ex import geometry_data_to_hpgl

    b = queries.get_body_stampers(body_code=body_code, version=body_version)
    lns = geometry_data_to_hpgl(
        b.rename(columns={"body_pieces_piece_key": "key"}), plot=plot, filename=filename
    )


def _post_meta_one_status_preview(m1):
    """
    experimental: when we have a meta one, we can post to a table of statuses
    when we dont have a meta one we can also post progress using the same schema e.g. PENDING BODY for the SKU
    """

    sample = MetaOneStatus(**m1.status_info())
    from res.connectors.airtable.AirtableUpdateQueue import AirtableQueue

    queue = AirtableQueue("tbl12xARFpxe5H3TM", base_id="appUFmlaynyTcQ5wm")
    queue.try_update_airtable_record(
        sample, key_field="One Number", airtable_base="appUFmlaynyTcQ5wm"
    )


class BodyMetaOne(MetaOne):
    def __init__(self, body_code, body_version, size_code, hide_unknown=True, **kwargs):
        self._size_lu_data = {}
        self._healz_mode = kwargs.get("healz_mode", False)
        self._shared_hasura = res.connectors.load("hasura")
        self._one_number = None
        try:
            self._data = queries.get_body(
                body_code=body_code,
                version=body_version,
                size_code=size_code,
                hasura=self._shared_hasura,
            )
        except Exception as e:
            raise Exception(
                f"Failed to load body {body_code} {body_version} {size_code}, may not have been exported & unpacked successfully"
            ) from e
        self._fully_annotate = True
        self._rank = 1

        if len(self._data) == 0:
            all_sizes = queries.list_all_body_sizes(
                body_code, body_version, hasura=self._shared_hasura
            )
            if size_code not in all_sizes:
                raise Exception(f"Size {size_code} not in saved sizes {all_sizes}")
            self._all_sizes = all_sizes

        self._data = self._data.rename(
            columns={
                "body_pieces_piece_key": "key",
                "body_pieces_outer_geojson": "outline",
                "body_pieces_inner_geojson": "inline",
                "body_pieces_outer_notches_geojson": "notches",
                "body_pieces_outer_corners_geojson": "corners",
                "body_pieces_seam_guides_geojson": "seam_guides",
                "body_pieces_sew_identity_symbol": "sew_identity_symbol",
                "body_pieces_internal_lines_geojson": "internal_lines",
                "body_pieces_buttons_geojson": "buttons_geojson",
                "body_pieces_pleats_geojson": "pleats_geojson",
                "body_pieces_fuse_type": "body_pieces_fuse_type",
            }
        )

        try:
            self._data["meta_piece_code"] = self._data["key"].map(
                lambda x: "-".join(x.split("-")[-2:])
            )
        except Exception as ex:
            pass

        if hide_unknown:
            self._data = (
                self._data[self._data["body_pieces_type"] != "unknown"]
                .reset_index()
                .drop("index", 1)
            )

        self._data = self._data.astype(object).replace(np.nan, "None")

        self._contracts_failing = self._data.iloc[0]["contracts_failing"]
        self._data["name"] = self._data.iloc[0]["body_code"]  # TODO version
        self._data["vs_key"] = self._data["key"]
        # remove the petite thing from the VS key if its there
        self._data["key"] = self._data["key"].map(lambda x: x.split("_")[0])
        self._data["size_code"] = size_code
        self._data["normed_size"] = None  # resolve size code
        self._data["piece_ordinal"] = 1000  # todo

        self._data["uri"] = None
        self._data["piece_metadata"] = self._data["key"].map(lambda x: {})
        self._data["artwork_uri"] = WHITE_ARTWORK
        # spoof style
        self._data["style_sizes_size_code"] = size_code

        self._body_ref = dict(self._data.iloc[0]) or {}
        self._init_data()
        # we usually infer from the associated pieces but here expectation is set
        self._body_version = body_version

    @property
    def stamper_pieces(self):
        d = self._data
        return d[d["body_pieces_type"] == "stamper"].reset_index(drop=True)

    @property
    def fuse_pieces(self):
        d = self._data
        return d[d["body_pieces_type"] == "fuse"].reset_index(drop=True)

    def save_complementary_pieces(self):
        s3 = res.connectors.load("s3")
        try:
            cut_path_no_size = f"s3://meta-one-assets-prod/bodies/cut/{self.body_code.replace('-','_').lower()}/v{self.body_version}"
            cut_path = f"{cut_path_no_size}/{self.size_code.lower()}"
            res.utils.logger.info(
                f"Saving cut files to {cut_path} for chart size {self.chart_size}"
            )

            def rekey_with_size(d):
                """
                a little formatting just to add the size qualifier
                """
                # normed_size - self.normed_size

                size_chart_string = self.chart_size.replace("Size ", "").replace(
                    " ", "_"
                )
                d["key"] = d.apply(
                    lambda row: f"{row['key']}_{size_chart_string}",
                    axis=1,
                )
                return d

            res.utils.logger.info(f"Purge old stampers and fuse pieces in {cut_path}")
            for f in s3.ls(f"{cut_path}/stamper_"):
                s3._delete_object(f)

            for f in s3.ls(f"{cut_path}/fuse_"):
                s3._delete_object(f)

            def stamper_path():
                return f"{cut_path}/stamper_{self.body_code}-V{self.body_version}-{self.normed_size}-X.plt"

            def fuse_path():
                return f"{cut_path}/fuse_{self.body_code}-V{self.body_version}-{self.normed_size}-F.plt"

            s = geometry_data_to_hpgl(
                rekey_with_size(self.stamper_pieces), filename=stamper_path()
            )

            """
            if the fuse type is known, we will group the fuse pieces by that - but only if there is more than 1 fuse type
            """
            nest_grouping_column = None
            if "body_pieces_fuse_type" in self.fuse_pieces.columns:
                if len(self.fuse_pieces["body_pieces_fuse_type"].dropna().unique()) > 1:
                    nest_grouping_column = "body_pieces_fuse_type"
            if len(self.fuse_pieces):
                f = geometry_data_to_hpgl(
                    rekey_with_size(self.fuse_pieces),
                    nest_grouping_column=nest_grouping_column,
                    filename=fuse_path(),
                )
            """
            """

            try:
                self._get_elastic_stamper_measurements_inches(
                    save_tabular_image_path=f"{cut_path}/elastic_measurements.pdf"
                )
            except Exception as ex:
                res.utils.logger.warn(
                    f"Failing on the elastic pieces saving {repr(ex)}"
                )
            res.utils.logger.info(f"Cut files saved to {cut_path_no_size}")
        except Exception as ex:
            # contract fail add tag
            raise ex

    def get_all_piece_names(self):
        return queries.list_all_body_pieces(self.body_code, self.body_version)

    @staticmethod
    def get_body_asset_status(body_code, body_version):
        from res.flows.meta.bodies import get_body_asset_status

        return get_body_asset_status(body_code=body_code, body_version=body_version)

    def deactivate(self):
        return queries.add_body_contract_failed("BODY_ONE_READY")

    @property
    def contracts_failing(self):
        return self._contracts_failing

    def as_asset_response(cls, validate=True, full=False, **kwargs):
        from schemas.pydantic.meta import BodyMetaOneResponse

        return BodyMetaOneResponse(
            **{"body_code": cls.body_code, "body_version": cls.body_version},
            contracts_failed=cls.validate(),
        )

    @property
    def all_sizes(self):
        return queries.list_all_body_sizes(
            self.body_code, self.body_version, hasura=self._shared_hasura
        )

    def status_at_node(
        cls,
        sample_size=None,
        request=None,
        node="Meta.ONE.Body",
        stage="01 Automated Contracts Checked",
        status="Active",
        sizes=None,
        object_type=None,
        validate_paths=False,
        bootstrap_failed=None,
        contract_failure_context=None,
        body_request_id=None,
        previous_differences={},
        **kwargs,
    ):
        """
        helper to create a response object with some meta data about the body
        this can be posted to previews etc. but we can always use this to inject some status info about the state or additional failing contracts
        """

        paths = [
            cls._as_preview_path(p["key"]) for p in cls if p.piece_name.is_printable
        ]
        cut_paths = [
            cls._as_preview_path(p["key"]) for p in cls if p.piece_name.is_cut_asset
        ]

        if not bootstrap_failed:
            bootstrap_failed = []
        # the contracts failed are anything is saved on the style OR determined from a live validation
        _failed_contracts = list(
            set(cls.validate() + cls.contracts_failing + (bootstrap_failed))
        )
        # TODO: implement this as  previews are part of the contracts
        if validate_paths:
            s3 = res.connectors.load("s3")
            for p in paths + cut_paths:
                if not s3.exists(p):
                    _failed_contracts.append("MISSING_PREVIEW_PIECES")

        def _get_piece_info(p):
            p = PieceName(p)
            return {
                "key": p._key,
                "piece_code": p.name,
                "piece_type": p.piece_type,
                "piece_type_name": p.piece_type_name,
            }

        d = {
            # adding the body one request link here
            "body_one_ready_request": body_request_id,
            # "record_id": request_id,
            "body_code": cls.body_code,
            "body_version": cls.body_version,
            "stage": stage,
            "sample_size": sample_size,
            "printable_pieces": paths,
            "complementary_pieces": cut_paths,
            "cut_files": cls.get_cut_files_list(ignore_size=True),
            "poms": cls.get_pom_files_list(),
            "node": node,
            "status": status,
            "sizes": cls.all_sizes,
            "pieces": [_get_piece_info(p) for p in cls.piece_keys],
            "size": cls.size_code,
            "contracts_failed": _failed_contracts,
            "contract_failure_context": contract_failure_context,
            "body_file_uploaded_at": cls.body_file_uploaded_at,
            "trace_log": kwargs.get("trace_log"),
            "updated_at": res.utils.dates.utc_now_iso_string(),
            "previous_differences": previous_differences,
        }
        return d if object_type is None else object_type(**d)

    def get_points_of_measure(self):
        """
        returns json like format
        all sizes are returned  - so use the sample size typically to ref
        """
        # we skip rows 0 because how the csv is formatted
        df = pd.read_csv(self.get_pom_files_list()[0], skiprows=[0]).set_index("Size")
        df.index.name = "measurement_name"
        records = df.reset_index().to_dict("records")
        return records

    def _as_preview_path(cls, key):
        return f"s3://{RES_DATA_BUCKET}/meta-one-assets/body-requests-previews/{cls.body_code}/v{cls.body_version}/{key}.png"

    def _should_preview_piece(cls, piece_name):
        """
        override what the meta one parent does and show either the printable OR the cut pieces /complementary pieces
        """
        return piece_name.is_printable or piece_name.is_cut_asset

    @staticmethod
    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def for_sample_size(body_code, body_version=None):
        """
        pass a three part sku and resolve to a 4 part sku based on the bodies samples size code
        """
        from res.flows.meta.ONE.body_node import BodyMetaOneNode

        data = BodyMetaOneNode.get_body_metadata(body_code)
        body_version = body_version or data.get("patternVersionNumber")
        try:
            return BodyMetaOne(body_code, body_version, size_code=data["sample_size"])
        except Exception as ex:
            res.utils.logger.warn(
                f"Failing to load the body meta one for {body_code, body_version} :> {ex}"
            )
            return data

    def _get_elastic_stamper_measurements_inches(self, save_tabular_image_path=None):
        """
        for a body we can return the elastic piece stamper measurements to remove stampers
        """
        measurements = {}

        for p in self:
            if p.piece_name.component == "ELS" and p.piece_name.piece_type == "X":
                measurements[p.key] = (
                    round(p.piece_outline.bounds[-2]) / 300,
                    round(p.piece_outline.bounds[-1]) / 300,
                )
        if save_tabular_image_path and len(measurements):
            from weasyprint import HTML

            df = pd.DataFrame(
                [
                    {
                        "Piece Name": k,
                        "Width": v[0],
                        "Length": v[1],
                        "Size Code": self.size_code,
                        "Piece Type": "Elastic",
                    }
                    for k, v in measurements.items()
                ]
            ).set_index("Piece Name")
            pbytes = HTML(
                string=df.to_html().replace('border="1"', 'border="0"')
            ).write_pdf()
            with res.connectors.load("s3").file_object(
                save_tabular_image_path, "wb"
            ) as f:
                f.write(pbytes)
            res.utils.logger.info(
                f"Wrote measurements for stamper elastics to {save_tabular_image_path}"
            )
        return measurements


class MetaOnePieceCollection(MetaOne):
    """
    A piece collection is used to select pieces at a particular node to observe them

    We can load the pieces as a set from orders and then interact with the pieces as we would a specific meta one

    m1 = MetaOnePieceCollection('Make.Print.RollPacking','ECVIC')
    for pieces in m1:
        #properties can be passed in at runtime to determine certain information
        image = m1.get_labelled_piece_image(**kwargs)
    """

    def __init__(
        self,
        node_name,
        material_code,
        set_key=None,
        status=None,
        lazy_load=False,
        **kwargs,
    ):
        self._shared_hasura = res.connectors.load("hasura")
        self._data = meta_one_queries.get_make_pieces_at_node_and_material(
            node_name, status=status, material_code=material_code, set_key=set_key
        )
        if len(self._data):
            self._data = self._data.rename(
                columns={
                    "body_pieces_piece_key": "key",
                    "body_pieces_outer_geojson": "outline",
                    "body_pieces_inner_geojson": "inline",
                    "pieces_base_image_uri": "uri",
                    "pieces_normed_size": "normed_size",
                    "pieces_piece_set_size": "piece_set_size",
                    "pieces_piece_ordinal": "piece_ordinal",
                    "body_pieces_size_code": "size_code",
                    "body_pieces_outer_notches_geojson": "notches",
                    "body_pieces_outer_corners_geojson": "corners",
                    "body_pieces_seam_guides_geojson": "seam_guides",
                    "body_pieces_sew_identity_symbol": "sew_identity_symbol",
                    "body_pieces_internal_lines_geojson": "internal_lines",
                }
            )

            self._fully_annotate = True
            self._data = self._data.astype(object).replace(np.nan, "None")
            self._data["key"] = self._data["key"].map(lambda x: x.split("_")[0])
            self._data["piece_metadata"] = self._data["key"].map(lambda x: {})
            self._data["one_number"] = self._data["one_order"].map(
                lambda x: x.get("one_number")
            )
            self._data["one_code"] = self._data["one_order"].map(
                lambda x: x.get("one_code")
            )

            self._data["sku"] = self._data["one_order"].map(lambda x: x.get("sku"))

            self._data["normed_size"] = None  # resolve size code
            self._size_code = dict(self._data.iloc[0]).get("size_code")
            self._data["piece_index"] = self._data["piece_ordinal"]

            # interface and one number take from piece or pass in
            self._flow = "3d"
            self._normed_size = None  # NA
            self._init_data()
        else:
            res.utils.logger.warn(f"No relevant data at node {node_name}")

    def _init_data(cls):
        for col in ["outline", "notches", "corners", "seam_guides", "internal_lines"]:
            cls._data[col] = cls._data[col].map(from_geojson)
        cls._pieces = [meta_one_piece(row, cls) for row in cls._data.to_dict("records")]
        cls._pieces = {p.one_code: p for p in cls._pieces}
