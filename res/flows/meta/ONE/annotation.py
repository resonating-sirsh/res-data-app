from PIL import Image
from res.media.images.outlines import draw_lines
from res.media.images.geometry import unary_union, translate, LineString, Point
import cv2
import numpy as np
from shapely.ops import substring
import res
from res.utils.resources import read
from res.media.images import sample_near_point, get_perceptive_luminance_at_point
from res.media.images.qr import get_one_label_qr_code
from res.flows.dxa.res_color import get_one_label
from res.media.images.icons import (
    get_size_label_centered_circle,
    get_limited_edition_placeholder,
    get_size_label,
)
from shapely.geometry import shape
from res.media.images.geometry import (
    LineString,
    Polygon,
    line_angle,
    three_point_angle,
    surface_side,
    inside_offset,
    LinearRing,
    MultiLineString,
    project_point_from_surface,
)
from res.media.images.outlines import get_piece_outline
from shapely.affinity import rotate
from res.flows.dxa.res_color import place_label_from_options
from shapely.ops import substring
import pandas as pd


class Annotation:
    def __init__(self, point, segment, shape, atype):
        self.point = point
        self.segment = segment
        self.shape = shape
        self.atype = atype


class viable_segment:
    def __init__(self, g, ref_outline=None, max_curvature=10):
        self._g = g
        # compute side
        # label edge some other way i.e. get another scaled and named edge, find out intersections on the surface and name
        self._max_curvature = max_curvature
        self._reserved = 0
        self._side = None
        self._edge_id = None
        self._reservations_count = 0

        if ref_outline:
            self._side = surface_side(
                g.interpolate(0.5, normalized=True), ref_outline, line_rule=False
            )

    @property
    def side(self):
        return self._side

    @property
    def residual(self):
        return substring(self._g, self._reserved, self.length)

    def _repr_svg_(self):
        return self.residual._repr_svg_()

    def reserve(self, i):
        self._old_reserved = self._reserved

        while self._reserved + i < self.length:
            # add the reservation length i
            self._reserved = min(self._reserved + i, self.length)
            segment = substring(self._g, self._old_reserved, self._reserved)
            # store the old reserved
            self._old_reserved = self._reserved

            # check if the curvature check passes and return
            if viable_segment.get_segment_curvature(segment) < self._max_curvature:
                self._reservations_count += 1

                return segment
            else:
                pass
        # if we cannot find anything just return
        return None

    @staticmethod
    def get_segment_curvature(l, ruler=700, eps=-1):
        def _curvature(d):
            R = min(l.length, ruler)
            a, c = l.interpolate(d), l.interpolate(d + R)
            b = l.interpolate(d + R / 2.0)
            return (180 - round(three_point_angle(*[a, b, c]))) * 0.5

        if eps == -1:
            return _curvature(d=0)
        # some sort of averaging mode
        return (
            np.mean([_curvature(a) for a in np.arange(0, l.length / eps, l.length)])
            % 180
        )

    @property
    def length(self):
        return self._g.length


class ViableSegments:
    """
    TODO: test knit modes e.g. labels in weird places ??
    """

    def __init__(
        self,
        p,
        min_length=200,
        distance_buffer=20,
        invert=False,
        # zero tolerance on curvature until we test for high qty symbol placement
        max_curvature=3,  # 1.6,
        inside_offset_amount=35,
    ):
        self.is_stable_material = p.is_stable_material
        self._max_curvature = max_curvature
        # remove any built in ahcors on the body or style from the viable surface if relevant
        if p.is_stable_material:
            a = p.piece_outline - p.seam_guides.buffer(10)
            a = a - p.corners.buffer(distance_buffer)
            a = a - p._surface_notches.buffer(distance_buffer)
            self._surface = inside_offset(a, p.piece_outline, r=inside_offset_amount)
            geoms = []
            if hasattr(self._surface, "geoms"):
                geoms = [g for g in self._surface.geoms if g.length > min_length]
            self._surface = unary_union(geoms)
            # force multi line string

        else:
            res.utils.logger.info("using knit placement")
            self._surface = MultiLineString(
                _placement_line_knit_buffer(p, half_width=350)
            )

        # this is an edge case safety if the thing collapses to a line
        S_in = self._surface
        if self._surface.type == "LineString":
            S_in = [self._surface]

        self._restricted_points = [
            (
                p.piece_outline.project(g.interpolate(0.0))
                if g.type == "LineString" and g.length > 10
                else None
            )
            for g in S_in
        ]

        # remove invalid
        def is_valid(l):
            if l is None:
                return False
            if l.length == 0:
                return False
            l = rotate(l, 90)
            p1 = l.interpolate(0, normalized=True)
            p2 = l.interpolate(1, normalized=True)
            P = Polygon(p.piece_outline)
            return not (P.contains(p1) and P.contains(p2))

        self._segments = [
            viable_segment(g, ref_outline=p.piece_outline, max_curvature=max_curvature)
            for g in S_in
            if is_valid(g)
        ]

    def __iter__(self):
        for v in self._segments:
            yield v

    @property
    def surface(self):
        return self._surface

    @property
    def segments(self):
        return self._segments

    def _repr_svg_(self):
        return self._surface._repr_svg_()

    def place_label_with_reservation(
        self,
        im,
        label,
        end_buffer=20,
        start_buffer=10,
        draw_viable_surface_and_placement=False,
        **placement_kwargs,
    ):
        """
        todo validation for placement - the viable surface on which this is based must be inside the outline but we coudl double check

        args such as `directional_label` change behaviour
        """
        # start at the top

        reservation_required = label.size[0] + end_buffer

        def place_reservation(vs, reservation):
            # we place at buffer and then at center of label
            pp = reservation.interpolate(start_buffer + reservation_required / 2.0)
            # the placement sample should be bigger than the label and angle and curvature  computed on this
            placement_sample = substring(
                reservation,
                start_buffer,
                reservation_required * 1.2,
            )
            placement_kwargs["placement_position"] = pp
            placement_kwargs["curvature"] = viable_segment.get_segment_curvature(
                placement_sample
            )
            placement_kwargs["piece_side"] = "top"
            placement_kwargs["placement_angle"] = line_angle(
                placement_sample, last=True
            )
            # we dont control it if its a directional label
            if placement_kwargs.get("directional_label") == True:
                placement_kwargs["placement_angle"] = 0
            # placement here
            _im = (
                im
                if not draw_viable_surface_and_placement
                else draw_individual_lines(im, [l for l in self.surface])
            )
            shift_margin = 0

            return place_label_from_options(
                _im, label, shift_margin=shift_margin, **placement_kwargs
            )

        # try place labels stop first
        for vs in self:
            if vs.side == "top":
                reservation = vs.reserve(reservation_required)
                if reservation:
                    return place_reservation(vs, reservation)
        # then try other spots
        for vs in self:
            if vs.side != "top":
                reservation = vs.reserve(reservation_required)
                if reservation:
                    return place_reservation(vs, reservation)

        res.utils.logger.warn(f"It has not been possible to place the label")
        return im


class AnnotationManager:
    """
    TODO:Fork viable surface - this is one of the things we are using
    in the new stuff that could be cleaned up. its a bit of a mishmash

    RH - sometimes we need to move the piece outline over the image.  when we do so we can pass this thing
    dx and dy so that it will act on the moved version of the piece boundary.
    """

    def __init__(
        self,
        piece_manager,
        min_length=200,
        max_curvature=3,
        inside_offset_amount=35,
        dx=0,
        dy=0,
    ):
        self._piece_manager = piece_manager
        p = piece_manager
        self.is_stable_material = p.offset_size_px == 0
        self._max_curvature = max_curvature
        self._reservations = []

        """
        add any reservations on the body - these are probably internal
        """
        if self._piece_manager.annotations:
            self._import_reservations_from_pieces()
        # remove any built in anchors on the body or style from the viable surface if relevant
        outline = translate(
            p.piece_outline if self.is_stable_material else p.buffered_piece_outline,
            dx,
            dy,
        )
        if self.is_stable_material:
            self._surface = inside_offset(
                translate(p.viable_surface, dx, dy),
                outline,
                r=inside_offset_amount,
            )
        else:
            # somethings hosed with shapely with the inside_offset function (parallel offset gives a screwed polygon.)
            self._surface = Polygon(outline).buffer(-inside_offset_amount).boundary

        if self._surface.type == "LineString":
            self._surface = MultiLineString([self._surface])
        else:
            geoms = [g for g in self._surface.geoms if g.length > min_length]
            self._surface = unary_union(geoms)

        S_in = (
            [self._surface]
            if self._surface.type == "LineString"
            else list(self._surface.geoms)
        )

        self._restricted_points = [
            (
                outline.project(g.interpolate(0.0))
                if g.type == "LineString" and g.length > 10
                else None
            )
            for g in S_in
        ]

        # remove invalid
        def is_valid(l):
            if l is None:
                return False
            if l.length == 0:
                return False
            # for knits we know we are just dealing with the outline, and this normality check is kind of wonku
            # since we are dealing with a closed ring rather than a line (although shapely denies its closure and its ringness for
            # some lame reason)
            if not self.is_stable_material:
                return True
            l = rotate(l, 90)
            p1 = l.interpolate(0, normalized=True)
            p2 = l.interpolate(1, normalized=True)
            P = Polygon(outline)
            return not (P.contains(p1) and P.contains(p2))

        self._segments = [
            viable_segment(g, ref_outline=outline, max_curvature=max_curvature)
            for g in S_in
            if is_valid(g)
        ]

    def __iter__(self):
        for v in self._segments:
            yield v

    def _import_reservations_from_pieces(self):
        df = pd.DataFrame([d.dict() for d in self._piece_manager.annotations]).rename(
            columns={
                "name": "key",
                "position": "placement_position",
                "angle": "placement_angle",
            }
        )
        df["is_reservation_only"] = True
        df["placement_position"] = df["placement_position"].map(Point)
        self._reservations += df.to_dict("records")

    @property
    def reservations(self):
        """
        this could well be a structured pydantic object
        """
        import pandas as pd

        df = res.utils.dataframes.replace_nan_with_none(
            pd.DataFrame(self._reservations)
        )

        if len(df):
            df["placement_position"] = df["placement_position"].map(
                lambda pt: (pt.x, pt.y)
            )
            if "label_outline" in df:
                df["label_outline"] = df["label_outline"].map(str)
        return df

    @property
    def surface(self):
        return self._surface

    @property
    def segments(self):
        return self._segments

    def _repr_svg_(self):
        return self._surface._repr_svg_()

    def get_primary_label_placements():
        """
        predetermines the one label placement and the placement of identity symbols
        - the one label is given a prescribed length for an N digit - if not sure a sample image can be provided
        - the identity symbol is added
        - we do not specify the color properties etc
        """
        pass

    def get_primary_and_secondary_suggested_grayscales(im, site):
        pass

    def show_placement_info(self, requests):
        """
        For example load the manager for a piece
        its stateful so construct it once and then reservce placements

        from res.flows.dxa.res_color import get_one_label
        color = (100, 100, 100, 200)
        lbl = get_one_label(
            "1010101001",   my_part=" (1/1)",   color=color,
        )
        from res.flows.meta.ONE.annotation import AnnotationManager
        manager = AnnotationManager(mp)
        requests = [manager.place_label_with_reservation(im=None, plan=True, label =lbl) for i in range(30)]
        manager.show_placement_info(requests)

        """
        from geopandas import GeoDataFrame

        from res.media.images.geometry import unary_union, translate, rotate

        def placements(outline, placement_info):
            gs = []
            for p in placement_info:
                if p:
                    xoff = p["placement_position"].x
                    yoff = p["placement_position"].y

                    # a hard coded image space offset which is normally calculated when placing images
                    g = translate(p["label_outline"], xoff=xoff - 50, yoff=yoff - 25)
                    g = rotate(g, p["placement_angle"])
                    gs.append(g)
            return unary_union([outline] + gs)

        # by plotting against the surface geometries we can see how the placements avoid notches and corners
        P = placements(self._piece_manager.surface_geometries, requests)
        GeoDataFrame(P, columns=["geometry"]).plot(figsize=(20, 20))
        return P

    @staticmethod
    def place_label_from_loaded_reservation(
        im, label_fn, reservation, offset=None, projection_offset=None, **options
    ):
        """
        could load batch and look for what is not placed somehow
        """

        r = reservation
        outline = get_piece_outline(np.asarray(im))
        if offset:
            # r["placement_position"] = project_point_from_surface(
            #     Point(r["placement_position"]), outline, projection_offset
            # )
            prop = [
                r["placement_position"][0] + offset[0],
                r["placement_position"][1] + offset[1],
            ]
            if Point(prop).distance(outline) < Point(r["placement_position"]).distance(
                outline
            ):
                res.utils.logger.debug(
                    "Note: the proposed offset moves placement closer to the surface - im gonna scale it to account for some offset... "
                )
                prop = [
                    r["placement_position"][0] + offset[0] * 0.5,
                    r["placement_position"][1] + offset[1] * 0.5,
                ]

            r["placement_position"] = prop

        pt = r["placement_position"] = Point(r["placement_position"])
        res.utils.logger.debug(
            f"Placing at {pt} - in image of size {im.size} - at distance {pt.distance(outline)} from outline - on side {surface_side(pt,outline)}"
        )

        color = (0, 0, 0, 0)
        try:
            color = tuple(r["sample_color"])
        except:
            pass

        return place_label_from_options(
            im,
            background_color=color,
            label=label_fn(site_luminosity=r.get("sample_perceptive_luminance", 0)),
            **r,
        )

    def place_label_with_reservation(
        self,
        im,
        label,
        end_buffer=20,
        start_buffer=10,
        draw_viable_surface_and_placement=False,
        plan=False,
        label_fn=None,
        reservations=None,
        key=None,
        **placement_kwargs,
    ):
        """
        todo validation for placement - the viable surface on which this is based must be inside the outline but we could double check

        args such as `directional_label` change behaviour

        label function is a function of luminosity that fetches the image with the right color
        the initial label is used to sample for size and then its refetched
        """
        # start at the top

        if label is None:
            return {}

        reservation_required = label.size[0] + end_buffer

        def place_reservation(vs, reservation):
            # we place at buffer and then at center of label
            pp = reservation.interpolate(start_buffer + reservation_required / 2.0)
            # the placement sample should be bigger than the label and angle and curvature  computed on this
            placement_sample = substring(
                reservation,
                start_buffer,
                reservation_required * 1.2,
            )
            placement_kwargs["key"] = key
            placement_kwargs["placement_position"] = pp
            placement_kwargs["curvature"] = viable_segment.get_segment_curvature(
                placement_sample
            )
            placement_kwargs["piece_side"] = "top"
            placement_kwargs["placement_angle"] = line_angle(
                placement_sample, last=True
            )
            # we dont control it if its a directional label
            if placement_kwargs.get("directional_label") == True:
                placement_kwargs["placement_angle"] = 0

            # sampling the color of the image in the region
            pt = placement_kwargs["placement_position"]
            sample_patch_color_avg = None
            L = None
            try:
                sample_patch_color_avg = tuple(
                    sample_near_point(im, int(pt.x), int(pt.y))
                    .mean(axis=1)
                    .mean(axis=0)
                    .astype(int)
                )

                L = float(
                    get_perceptive_luminance_at_point(list(sample_patch_color_avg))
                )
                res.utils.logger.debug(
                    f"Sampled perceptive luminance {L} @ {sample_patch_color_avg}"
                )

            except Exception as ex:
                print(ex)
                pass
            placement_kwargs["sample_color"] = sample_patch_color_avg
            placement_kwargs["sample_perceptive_luminance"] = L
            placement_kwargs["is_reservation_only"] = True
            self._reservations.append(placement_kwargs)
            if plan:
                # return the simple convex hull not the detailed image points#
                placement_kwargs["label_outline"] = get_piece_outline(
                    np.asarray(label)
                ).convex_hull
                return placement_kwargs

            placement_kwargs["is_reservation_only"] = False
            _im = (
                im
                if not draw_viable_surface_and_placement
                else draw_individual_lines(im, [l for l in self.surface])
            )

            # print(sample_patch_color_avg, placement_kwargs)
            if sample_patch_color_avg[0] < 0:
                res.utils.logger.warn(
                    "negative color - resetting when placing reservation"
                )
                sample_patch_color_avg = (0, 0, 0, 0)
            if L > 0.8:
                res.utils.logger.debug(
                    f"for site luminosity {L} > 0.8 we will average the back as white to provide contrast for the font"
                )
                sample_patch_color_avg = (255, 255, 255, 255)

            shift_margin = 0
            return place_label_from_options(
                _im,
                # if we know the site_luminosity we should just call that
                # the only reason the label passed in is needed is to get its size
                label if not (label_fn and L) else label_fn(site_luminosity=L),
                shift_margin=shift_margin,
                background_color=sample_patch_color_avg,
                **placement_kwargs,
            )

        # try place labels stop first
        for vs in self:
            if vs.side == "top":
                reservation = vs.reserve(reservation_required)
                if reservation:
                    return place_reservation(vs, reservation)
        # then try other spots
        for vs in self:
            if vs.side != "top":
                reservation = vs.reserve(reservation_required)
                if reservation:
                    return place_reservation(vs, reservation)

        res.utils.logger.warn(f"It has not been possible to place the label")
        return im if not plan else placement_kwargs

    def _placement_line_knit_buffer(self, half_width=700, clear_buffer_factor=0.6):
        """
        draw an intercept from the center of the bounding box through the shape
        when the intercept hits the shape, travel north clear_buffer_factor*buffer
        """
        ol = self._piece_manager.buffered_piece_outline

        # could find a center of mass another way
        cx = int(ol.bounds[2] / 2)
        intercept_line = LineString([Point(cx, 0), Point(cx, ol.bounds[-1])])
        intercept_points = ol.intersection(intercept_line)
        if intercept_points.type == "Point":
            res.utils.logger.warn(
                f"Did not intercept at two points!! {self._piece_manager}"
            )
            intercept_points = [intercept_points]
        else:
            intercept_points = intercept_points.geoms
        # on the intercepts get the point the near the top of the image or the bottom of the non image space shape
        distances = [pt.distance(Point(cx, 0)) for pt in intercept_points]
        pt = intercept_points[np.argmin(distances)]
        cy = pt.y + self._piece_manager.offset_size_px * clear_buffer_factor

        vl = LineString([Point(cx - half_width, cy), Point(cx + half_width, cy)])
        # frame this in a small segment
        vl = vl - vl.interpolate(0.2, normalized=True)
        vl = vl - vl.interpolate(0.8, normalized=True)

        # todo we can use the viable surface inside the knit
        # ol = inside_offset(ol, ol, r=)
        vl = MultiLineString([vl])
        return vl  # unary_union([ol, Point(cx, cy), vl])

    @staticmethod
    def one_label_function_from_data(data):
        def make_one_label(one_number, site_luminosity=0, **options):
            """
            prepare a function to generate a ONE number
            """
            make_type = ""  # is_healing, is_extra
            if options.get("is_extra"):
                make_type = "extra"
            if options.get("is_healing"):
                make_type = "healing"
            color = color_from_luminosity(site_luminosity)
            lbl = get_one_label(
                one_number,
                color,
                piece_type=data["piece_type"],
                piece_make_type=make_type,
                my_part=f" ({data['piece_ordinal']}/{data['piece_set_size']})",
                # color=color,
                one_label_text_color=color,
            )
            return lbl

        return make_one_label

    @staticmethod
    def enumerate_body_level_reservations(reservations):
        BODY_LABELS = [
            "one_label_placeholder",
            "one_code_placeholder",
            "limited_edition_index_placeholder",
            "size_placeholder",
        ]
        for k, v in reservations.items():
            if k in BODY_LABELS:
                yield k, v

    @staticmethod
    def label_function_from_data_and_reservation_name(data, name):
        """ """

        def _select_reservation(name):
            for d in data["annotations_json"]:
                if d["key"] == name:
                    return d

        normed_size = "AB"
        # TODO font should be a property of the brand/annotation
        font = "ArchivoBlack-Regular.ttf"
        if data.get("piece_name", data.get("key")).startswith("ME-9000"):
            font = "Chapman-Medium.otf"
        reservation = _select_reservation(name)
        # we assume the reservation exists for this to be called

        def make_label(
            one_number,
            multi_piece_sequence=None,
            style_make_instance=None,
            sku_make_instance=None,
            site_luminosity=0,
            **options,
        ):
            """
            provide a generic label interface for functions

            :param one_number: the make one number
            :param multi_piece_sequence: for labels or similar we create multiple instances of same piece and need to id them
            :param style_make_instance: a specific style (BMC) in any size, how many times have we made it
            :param sku_make_instance: a specific sku (BMCS) in a size, how many times have we made it
            """
            size = tuple(reservation["size"]) if "size" in reservation else None

            if name in ["one_label_placeholder", "one_code_placeholder"]:
                if not size:
                    size = (350, 350)  # default
                return get_one_label_qr_code(
                    f"9{multi_piece_sequence}{one_number}", size=size
                )

            if "limited_edition_index_placeholder" in name:
                # default size? probably we want to insist on this
                return get_limited_edition_placeholder(style_make_instance, size=size)

            if name in ["size_placeholder"]:
                # default size?
                return get_size_label_centered_circle(
                    text=normed_size, size=size, font=font
                )

            return None

        return make_label

        # # if qr code and we have piece instance


def load_identity_symbol(isymbol, light_mode=False, color=None):
    return read(
        f"sew_identity_symbols/{isymbol}.svg", light_mode=light_mode, currentColor=color
    )


def color_from_luminosity(l=0):
    if l > 0.84:
        # for very bright colors we have a silvery color
        return (150, 150, 150, 255)
    if l > 0.9:
        # for very bright colors we have a silvery color
        return (200, 200, 200, 255)
    # otherwise we can choose the default which could be a white text or black border
    # white might look good on most dark color homogenous backgrounds
    return None  # (50, 50, 50, 255)


def dashed_lines(ring, sep=30, space=5):
    points = []
    if ring.type == "LineString":
        ring = [ring]
    for a in ring:
        for b in np.arange(0, a.length, sep):
            b = a.interpolate(b)
            points.append(b.buffer(space))
    result = unary_union(ring) - unary_union(points)

    if not isinstance(result, list):
        if result.type == "LineString":
            result = [result]
    return result


def post_previews(m1, request_id, contracts_failed=None, skip_exists=False, **kwargs):
    """
    use the m1 to generate previews in different modes
    report status updates and fail to allow another mode

    post previews can be a trivial op of just passing failed contracts to the old queue
    """
    paths = []
    s3 = res.connectors.load("s3")
    tab = res.connectors.load("airtable")["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    d = {
        "record_id": request_id,
        "Meta.ONE Flags": contracts_failed,
    }
    if m1:
        for p in m1:
            sample_path = f"s3://res-data-platform/samples/apply-color-requests-previews/{request_id.lower()}/{p['key'].lower()}.png"
            res.utils.logger.info(
                f"**************** {p.key} - {sample_path} ***************",
            )
            if s3.exists(sample_path) and skip_exists:
                res.utils.logger.info(
                    f"skipping the file that already exists - it may be old"
                )
            else:
                img = p.dxa_preview
                img.thumbnail((2048, 2048))
                s3.write(sample_path, img)
            paths.append(sample_path)

        # if we have the preview paths we can use them -otherwuse
        res.utils.logger.info(f"*****Updating previews for paths {paths}")
        d["Meta.ONE Piece Preview"] = [
            {"url": s3.generate_presigned_url(pt)} for pt in paths
        ]
        d["Meta.ONE Piece Preview Updated"] = res.utils.dates.utc_now_iso_string()

    tab.update(d)

    return {}


USER_VALIDATED_CONTRACTS = [
    "PLACEMENT_NOTCHES",
    "NOTCH_LOCATION_USER",
    "NOTCH_LOCATION_BOT",
    "COMPLEMENTARY_PIECES",
    "MISSING_SEAM_ALLOWANCE",
]
CONTRACTS = {
    "BERTHA_RAN": "rec1Z29kRNXum4cep",
    "3D_BODY_FILE": "rec3FHJjyNx8zvhkc",
    "NON_BLANK_COLOR_PLACEMENT": "rec5zQvsTEQonEX9d",
    "ALL_COLOR_PIECES": "rec6V62qpP0W2SAh8",
    "KNIT_BUFFER": "rec75Rsf7jZEixG1Y",
    "BODY_PIECES_SAVED": "rec7gvrwUVhEzHExH",
    "CORRECT_COLOR_APPLIED": "rec7pXGvBg5RoXPI9",
    "PLACEMENT_NOTCHES": "recBiqgoCdj1iT6GG",
    "ALL_SEAM_GUIDES": "recBwVzCLZppwkd7c",
    "ALL_PREVIEWS": "recGYByyXNUQSoRIE",
    "STYLE_ONE_READY": "recKyvERVOWSeOnN2",
    "3D_BODY_FILE_UNPACKED": "recL8OlzAW2pSzlsf",
    "PIECE_OUTLINES": "recLZJmlqKEGOvNln",
    "POM_ASSETS": "recNIy5jE5Py8BoRW",
    "BERTHA_JOB_QUEUED": "recNSzaSAr943AGtO",
    "NOTCH_LOCATION_BOT": "recPOz4azmrKLNMM2",
    "PIECE_NAMES": "recQywAnhTtkP2Azb",
    "TRIMS_ASSETS": "recSKZhZ0JwBe66jk",
    "MISSING_PIECE": "recV1fQ2UegfxCjIG",
    "BODY_ONE_READY": "recVeuQgm7WObJSfg",
    "3D_BODY_FILENAME": "recX2vPs9Rx8VZxvf",
    "NO_EXTRA_PIECE": "recXy9iKhQegyszTk",
    "CONSTRUCTION_MAPPED": "recZMgV6LV1YTvSVM",
    "COMPLEMENTARY_PIECES": "recgnfBaYRjLYfSyS",
    "MISSING_SEAM_ALLOWANCE": "rechOFS8scrxTTkUC",
    "PIECE_INTERNAL_LINES": "rechXcKRcYQycjoet",
    "STYLE_PIECE_MAPPING": "recjSb5g2hfWEfKP7",
    "STYLE_COSTS": "recmX72oxaKltBX2i",
    "NO_TRIM_ARTWORK": "recorykbdizibq816",
    "NOTCHES_CONSISTENT_ON_SIZES": "recqW3XruC0oU8SPF",
    "NOTCH_LOCATION_USER": "recwDIlwwMBSMNNpD",
}


# temp - these will move to the database
def map_contract_rid(c):
    return CONTRACTS.get(c)


def map_from_contract_rid(c):
    inverse = {v: k for k, v in CONTRACTS.items()}
    return inverse.get(c)


def try_updated_flow_api(d):
    try:
        from schemas.pydantic.meta import BodyMetaOneResponse
        from res.flows.api import FlowAPI
        import stringcase

        res.utils.logger.info(f"updating flow api")

        def snake_case(d):
            def sc(s):
                s = stringcase.snakecase(s)
                s = s.replace("__", "_")
                return s

            return {sc(k): v for k, v in d.items()}

        # # assumes the contracts are populated on the base
        api = FlowAPI(BodyMetaOneResponse, base_id="appa7Sw0ML47cA8D1")
        b = BodyMetaOneResponse(**snake_case(d))

        return api.update_airtable_queue(b)

    except Exception as ex:
        res.utils.logger.warn(f"Failing to update body one request in flow api {ex}")


def post_style_previews(response, paths=None, plan=False, **kwargs):
    return None


def post_body_previews(m1, plan=False, **kwargs):
    """
    https://airtable.com/appqtN4USHTmyC6Dv/tblVo2zZdoB1ZPg3i/viwlgIH2j2InKmp7Z?blocks=hide
    """
    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")
    paths = []
    cut_paths = []
    # for migration testing we can just update the new table
    _ignore_dxa_queue = kwargs.get("ignore_dxa_queue", False)
    tab = airtable["appqtN4USHTmyC6Dv"]["tblVo2zZdoB1ZPg3i"]
    upstream_failed_contracts = kwargs.get("failed_contracts", [])
    # in case of junk or other tag use cases just prune
    upstream_failed_contracts = [
        c for c in upstream_failed_contracts if c in CONTRACTS.keys()
    ]

    def get_record(oid):
        mapping = tab.to_dataframe(
            fields=["Name", "Stage", "Contracts Failed"],
            filters=tab._key_lookup_predicate(oid),
        )
        if len(mapping):
            return dict(mapping.iloc[0])
        return {}

    if m1 is None:
        from res.flows.meta.bodies import get_body_asset_status

        body_code = kwargs.get("body_code")
        body_version = kwargs.get("body_version")

        s = {}
        try:
            res.utils.logger.info(f"Checking body status: {body_code} V{body_version}")
            if body_code is None or body_version is None:
                res.utils.logger.warning("no body/version passed in - nothing to do")
                return
            # due to failure try and get the status
            s = get_body_asset_status(body_code=body_code, body_version=body_version)

            # merge all contracts we know about
            s["failed_contracts"] = list(
                set(s["failed_contracts"] + upstream_failed_contracts)
            )

        except Exception as ex:
            res.utils.logger.warn(f"Unable to get body status for this body")
        if body_code and body_version is not None:
            name = f"{body_code}-V{body_version}".upper()
            r = get_record(name)
            r["Body Code"] = body_code
            r["Body Version"] = body_version
            r["Stage"] = "00 Check Contracts"
            r["Name"] = name
            r["_trace"] = kwargs.get("trace")

            if s and "failed_contracts" in s:
                # get the ids for the failed contracts
                s["failed_contracts"] = [
                    map_contract_rid(c) for c in s["failed_contracts"]
                ]
                existing_contracts_failed = r.get("Contracts Failed") or []
                # these are id format
                r["Contracts Failed"] = list(
                    set(s["failed_contracts"] + existing_contracts_failed)
                )
                # add the trace if given

            if plan:
                return r

            if not _ignore_dxa_queue:
                res.utils.logger.info(f"Updating airtable {r}")
                tab.update(r)
            # undo the contract fails as id
            # we will snake case this later
            r["Contracts Failed"] = [
                map_from_contract_rid(c) for c in r["Contracts Failed"]
            ]

            try_updated_flow_api(r)

        return

    def as_path(key):
        return f"s3://res-data-platform/samples/body-requests-previews/{m1.body_code}/v{m1.body_version}/{key.lower()}.png"

    # contracts(asset) / m1.validate()
    name = f"{m1.body_code}-v{m1.body_version}".upper()
    # collect contracts
    r = get_record(name)
    new_checks = list(set(m1.validate() + upstream_failed_contracts))

    if r is not None:
        # try to upsert contracts in a sensible way
        contracts_failing = r.get("Contracts Failed") or []
        contracts_failing = [map_from_contract_rid(c) for c in contracts_failing]
        state = list(contracts_failing)
        # pruning contracts failing
        contracts_failing = [
            c
            for c in contracts_failing
            if c in new_checks or c in USER_VALIDATED_CONTRACTS
        ]
        # if we set this contracts we should be confident not to remove ones the user wants to re-validate. tricky
        res.utils.logger.info(
            f"Existing contracts failing {state} - keeping {contracts_failing}"
        )

        # TODO write observed contracts to kafka

        new_checks = list(set(contracts_failing + new_checks))
        res.utils.logger.info(
            f"New contracts failing {new_checks} - merged state {new_checks}"
        )

    # the slow bit
    for p in m1:
        path = as_path(p.key)
        if p.piece_name.is_printable:
            paths.append(path)
        if p.piece_name.is_cut_asset:
            cut_paths.append(path)
        # set this here so we can run the contract as though its an image we can load
        p._row["uri"]
        res.utils.logger.info(path)
        s3.write(path, p.dxa_preview)

    all_pieces = m1.get_all_piece_names()
    sizes = list(all_pieces["pieces_size_code"].unique())

    d = {
        # "record_id": request_id,
        "Body Code": m1.body_code,
        "Body Version": m1.body_version,
        "Stage": "01 Automated Contracts Checked",
        "Printable Pieces": paths,
        "Cut Pieces": cut_paths,
        "Cut Files": m1.get_cut_files_list(),
        "Poms": m1.get_pom_files_list(),
        "Name": name,
        "Sizes": sizes,
        "Contracts Failed": new_checks,
    }
    # is it an update?
    if r.get("record_id"):
        # dont regress this stage - if the server has it, send it back as such
        if r["Stage"] == "02 All Contracts Checked":
            d["Stage"] = "02 All Contracts Checked"
        d["record_id"] = r["record_id"]

    if plan:
        return d

    # build a record typed and queue it
    try_updated_flow_api(d)

    for attachment_type in ["Printable Pieces", "Cut Pieces", "Cut Files", "Poms"]:
        # modify for airtable AFTER sending to flow api
        d[attachment_type] = [
            {"url": s3.generate_presigned_url(pt)} for pt in d[attachment_type]
        ]

    d["Contracts Failed"] = [map_contract_rid(c) for c in new_checks]

    if not _ignore_dxa_queue:
        res.utils.logger.info(f"Updating airtable with {d}")
        tab.update(d)

    res.utils.logger.info("Done")


def draw_positional_notches(
    im,
    lines,
    extend_fold_lines=True,
    distance_factor=0.65,
    max_allowed_length=75,
    line_to_surface_threshold=15,
    outline_guide=None,
    color=(0, 0, 0, 255),
    ignore_notch_distance_errors=False,
):
    """
    One way to draw a notch with width from a seam guide - its cheaper than cutting them out like in knits if you dont have to
    TODO: we could fit the lines closer to the surface but preserve the angle somehow.
    problem here is if its too far we must reject
    """

    def filter_line_ok(l):
        if outline_guide is None:
            return True
        if line_to_surface_threshold is None:
            return True
        if outline_guide.distance(l) > line_to_surface_threshold:
            if not ignore_notch_distance_errors:
                res.utils.logger.warn(
                    f"Filtering a notch at distance {outline_guide.distance(l)} from outline"
                )
            return False
        return True

    for l in lines:
        if not filter_line_ok(l):
            continue
        # just to avoid obnoxiously long notches
        if max_allowed_length < l.length:
            l = substring(l, 0, max_allowed_length)
        hl = substring(l, start_dist=0, end_dist=distance_factor, normalized=True)
        im = Image.fromarray(
            draw_lines(
                im,
                hl,
                thickness=11,
                color=color,
            )
        )
        im = Image.fromarray(draw_lines(im, hl, thickness=5, color=(255, 255, 255, 0)))

        if extend_fold_lines:
            tl = substring(l, start_dist=distance_factor, end_dist=1.0, normalized=True)
            im = Image.fromarray(
                draw_lines(
                    im,
                    tl,
                    thickness=1,
                    dotted=True,
                    color=(50, 50, 50, 255),
                )
            )

    return im


def draw_annotations(im, annotations):
    return im


def _placement_line_knit_buffer(p, half_width=700, clear_buffer_factor=0.6):
    """
    draw an intercept from the center of the bounding box through the shape
    when the intercept hits the shape, travel north clear_buffer_factor*buffer
    """
    ol = p.buffered_piece_outline

    # could find a center of mass another way
    cx = int(ol.bounds[2] / 2)
    intercept_line = LineString([Point(cx, 0), Point(cx, ol.bounds[-1])])
    intercept_points = ol.intersection(intercept_line)
    # on the intercepts get the point the near the top of the image or the bottom of the non image space shape
    distances = [pt.distance(Point(cx, 0)) for pt in intercept_points]
    pt = intercept_points[np.argmin(distances)]
    cy = pt.y + p._offset_buffer * clear_buffer_factor

    vl = LineString([Point(cx - half_width, cy), Point(cx + half_width, cy)])
    # frame this in a small segment
    vl = vl - vl.interpolate(0.2, normalized=True)
    vl = vl - vl.interpolate(0.8, normalized=True)

    # todo we can use the viable surface inside the knit
    # ol = inside_offset(ol, ol, r=)
    vl = MultiLineString([vl])
    return vl  # unary_union([ol, Point(cx, cy), vl])


def draw_outline_on_image_center_with_offset_ref(
    pil_image, ol, inside_offset_image_outline, dx=0, dy=0, thickness=24, **kwargs
):
    # the reference shape is the original shape with an internal offset f(offset)
    dx = inside_offset_image_outline.centroid.x - ol.centroid.x
    dy = inside_offset_image_outline.centroid.y - ol.centroid.y
    ol = translate(ol, xoff=dx, yoff=dy)
    return draw_outline_on_image(
        pil_image, ol, dx=0, dy=0, thickness=thickness, **kwargs
    )


def draw_outline_on_image_center(pil_image, ol, dx=0, dy=0, thickness=24, **kwargs):
    """
    helper to center align overlays
    TODO implement auto color based on intensity
    """
    x, y = int(pil_image.size[1] / 2), int(pil_image.size[0] / 2)
    pt = ol.centroid
    # note the switch
    dx = x - pt.y
    dy = y - pt.x
    # compute an offset
    return draw_outline_on_image(
        pil_image, ol, dx=dx, dy=dy, thickness=thickness, **kwargs
    )


def draw_dashed_line_on_image(im, l, thickness=10, **kwargs):
    color = kwargs.get("color", (0, 0, 0, 255))
    alt_color = kwargs.get("alt_color", (250, 0, 0, 255))
    im = np.array(im)
    dashed = dashed_lines(l)
    compliment = l - unary_union(dashed)
    for line in dashed:
        ol = np.array(line.coords)
        ol = [tuple(l) for l in ol]
        cv2.polylines(
            im,
            [np.array(ol).astype(int)],
            # closed
            True,
            # black
            color=color,
            thickness=thickness,
        )
    if alt_color:
        for line in compliment:
            ol = np.array(line.coords)
            ol = [tuple(l) for l in ol]
            cv2.polylines(
                im,
                [np.array(ol).astype(int)],
                # closed
                True,
                # black
                color=alt_color,
                thickness=thickness,
            )

    return Image.fromarray(im)


def draw_outline_on_image(
    pil_image, ol, dx=0, dy=0, thickness=24, padding=True, **kwargs
):
    """
    Draw a shape outline on the image space image
    an offset can be specified
    ordinarily we pad the image with the cutline width - we would not necessarily have to do this if we are drawing inside the bounds
    """
    color = kwargs.get("color", (0, 0, 0, 255))
    ol = np.array(ol.coords) + np.array([int(dy), int(dx)])
    closed_poly = [tuple(l) for l in ol]

    if padding:
        # padding offset for placing a new cutline thickness around the image
        im = Image.new(
            pil_image.mode,
            (pil_image.size[0] + 2 * thickness, pil_image.size[1] + 2 * thickness),
            (0, 0, 0, 0),
        )
        im.paste(pil_image, (thickness, thickness))
        # pad the polygon too
        closed_poly = [tuple([t[0] + thickness, t[1] + thickness]) for t in closed_poly]
        # end padding
        pil_image = im

    im = np.array(pil_image)

    return Image.fromarray(
        cv2.polylines(
            im,
            [np.array(closed_poly).astype(int)],
            # closed
            True,
            # black
            color=color,
            thickness=thickness,
        )
    )


def draw_individual_lines(
    im, lines, dx=0, dy=0, thickness=6, padding=False, dotted=True, **kwargs
):
    color = kwargs.get("color", (50, 50, 50, 255))

    if len(lines):
        im = np.array(im)
        for line in lines:
            im = Image.fromarray(
                draw_lines(
                    im,
                    line,
                    thickness=thickness,
                    dotted=dotted,
                    color=color,
                )
            )

    return im


def add_one_code_hack(piece, im, size="M", piece_instance=1):
    """ """
    size = size_map = {
        "LG": "L",
        "MD": "M",
        "SM": "S",
    }.get(size, size)

    # dirty
    if "fits" in size.lower():
        size = "OS"

    try:
        if "LBLBYPNL" in piece.piece_name.name and piece._parent.body_code == "CC-9007":
            sl = None

            # BG and default font - these should be looked up from somewhere
            font = "ArchivoBlack-Regular.ttf"
            if piece._parent.body_code == "ME-9000":
                font = "Chapman-Medium.otf"

            sl = get_size_label(text=size, size=(250, 250), font=font)

            IDX = ""
            if "piece_instance" in piece._row:
                IDX = str(piece._row["piece_instance"]).zfill(3)
                IDX = f"{9}{IDX}"
            qrcode = get_one_label_qr_code(f"{IDX}{piece._parent._one_number}")

            im.paste(qrcode, (130, 2100), mask=qrcode)
            im.paste(sl, (690, 2140), mask=sl)
            res.utils.logger.debug(f"Added labels for piece instance {piece_instance}")
    except Exception as ex:
        res.utils.logger.warn(f"Failed to add one label hack {ex}")
        return im
    return im


def add_material_wall_hack(piece, im, material_code):
    from res.flows.make.materials import get_material_description_label
    import pandas as pd
    from res.flows.meta.ONE.queries import TEMP

    try:
        if piece._parent.style_sku in TEMP:
            res.utils.logger.info(
                f"material wall special case - bottom left 90 deg placement"
            )
            lbl = get_material_description_label(material_code, size=(6050, 1150))
            lbl = lbl.rotate(90, expand=True)
            im.paste(lbl, (200, 2500), mask=lbl)
            return im

        if (
            piece._parent.body_code == "CC-9011"
            and piece._parent.sku.split(" ")[2] == "UPDAUJ"
        ):
            res.utils.logger.info(
                "Doing another dirty thing; adding the material description label in a fixed location"
            )
            lbl = get_material_description_label(material_code)

            msk = np.asarray(im)
            # remove any stray lines from the image
            msk = cv2.medianBlur(msk, 11)
            msk = cv2.erode(msk, np.ones((5, 5)))
            msk = cv2.threshold(msk, 200, 255, cv2.THRESH_BINARY)[1]
            msk = cv2.GaussianBlur(msk, (5, 0), 11)
            msk = cv2.threshold(msk, 120, 255, cv2.THRESH_BINARY)[1]
            msk = cv2.erode(msk, np.ones((31, 31)))
            # a little signal processing to find where the color squares end
            xy = pd.DataFrame(msk.sum(axis=-1))
            y = xy.mean(axis=1).diff()[100:-100]
            y = y[y > 50].index[-1]
            x = xy.mean(axis=0).diff()[100:-100]
            x = x[x > 50].index[-1]

            # use a properly sized image as a reference for the white space from the corner found above
            px = x - lbl.size[0]
            py = y - lbl.size[1]

            # now place the material label image in position
            im.paste(lbl, (px, py), mask=lbl)
    except:
        res.utils.logger.debug("skipping the material wall hack")

    return im


def debug_placeholder(base_image, placeholder):
    from PIL import ImageDraw

    point = shape(placeholder["geometry"])
    props = placeholder["properties"]
    # artwork_name = props["name"]
    width = props["width"]
    height = props["height"]
    radians = props["radians"]

    d = ImageDraw.Draw(base_image)
    # debug
    x = point.x - width / 2
    y = point.y - height / 2
    d.rectangle(
        [(x, y), (x + width, y + height)],
        fill=(200, 200, 100, 255),
    )

    return base_image


def fill_in_placeholder(
    piece, base_image, placeholder, image_loader, offset_x=0, offset_y=0, resize=False
):
    """
    Using either a fixed rule f(piece, artwork_name) or a lookup passed in for artwork
    add artwork into locations specified on the body
    """

    point = shape(placeholder["geometry"])
    props = placeholder["properties"]
    artwork_name = props["name"]
    width = props["width"]
    height = props["height"]
    radians = props["radians"]
    image = image_loader(artwork_name, size=((int(width), int(height))))

    # we should not do this when the contract is ready but for now we just fail silently when we cannot load
    if image is None:
        res.utils.logger.warn(
            f"No image was returned for artwork {artwork_name} - check the contract"
        )
        return base_image

    if resize == False:
        # keep our size
        width = image.size[0]
        height = image.size[1]

    if image:
        image = image.resize((int(width), int(height)))
        image = image.rotate(-1 * np.degrees(radians), expand=True)

        (width, height) = image.size  # might be different when rotated

        x, y = point.coords[0]
        x = int(x - width / 2 + offset_x)
        y = int(y - height / 2 + offset_y)

        base_image.paste(image, (x, y), image)

    return base_image


def update_assembly_asset_links(mone, production_record_id, ex_date=None, plan=False):
    """


    When we prepare print pieces we are also preparing for assembly
    prep print pieces will be a good hook to make sure that the assets related to the pieces are also available

    - the factory order pdf
    - cut files

    #without a record id we cannot relate this to an actual production order
    ="rec807L7wfuvcHRUy"
    WHEN the record id is null we reolve

    we are tenacious here becasue make one production often clogged

    payload like this passed in - construct a meta one from it
    {
        "id": "reczQiGUrPtDB7tPT",
        "one_number": "10236934",
        "body_code": "JR-3109",
        "size_code": " 3ZZMD",
        "color_code": "SELF--",
        "material_code": "OC135",
        "uri": "s3://meta-one-assets-prod/styles/meta-one/3d/jr_3109/v5_3a9ce2c0a1/self--/3zzmd/",
        "piece_type": "self",
        "metadata": {
            "max_image_area_pixels": null,
            "intelligent_marker_id": "s3://meta-one-assets-prod/styles/meta-one/3d/jr_3109/v5_3a9ce2c0a1/self--/3zzmd/"
        }
     }

    """

    s3 = res.connectors.load("s3")

    if production_record_id is None:
        pass

    res.utils.logger.debug(
        f"Updating assembly data for meta one - see record {production_record_id} for order {mone._one_number}"
    )
    ex_date = ex_date or res.utils.dates.utc_now()

    airtable = res.connectors.load("airtable")
    prod = airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"]

    # for now just add the plotter files
    purls = [
        s3.generate_presigned_url(f)
        for f in mone.get_cut_files_list(ignore_size=False)
        if ".plt" in f.lower()
    ]

    # todo come up with a better summary document : customer details, SKU, ONE Code, Wholesale order info etc.
    # qr_code_link = (
    #     f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data="
    #     f"{one_number}%09"
    # )

    payload = {
        "record_id": production_record_id,
        # TODO - this is not the correct name will check on testing from prep pieces
        # could add fuse pieces too
        "Paper Markers": [{"url": purl} for purl in purls],
        "Number of Printable Body Pieces": len(mone._data),
        "Body Version": mone.body_version,
        "body_version": mone.body_version,
        # cut files for nest if exists
    }

    try:
        # update the payload with this thing
        furl = mone.get_order_html(exit_factory_date=ex_date)
        # payload["Factory Order PDF"] = [{"url": furl}]
        # payload["Print Factory Order"] = [{"url": furl}]

    except Exception as ex:
        res.utils.logger.warn(
            f"Failing to add factory order pdf{res.utils.ex_repr(ex)}"
        )
    res.utils.logger.debug(f"payload {payload}")
    if not plan:
        res.utils.logger.info("posting")
        prod.update_record(
            payload,
            use_kafka_to_update=False,
            RES_APP="metaone",
            RES_NAMESPACE="meta",
        )

    return payload


def get_image_table_html(meta_one):
    artwork_url = ""
    style_thumbnail_url = ""
    return f"""
            <table width='670' height='60'>
            <tr>
            <td style='width:160px; height:60px'>
                <div align='center'>
                <img src='{style_thumbnail_url}'style='max-height:130px; max-width:100%'>
                </div>
            </td>
            <td style='width:160px; height:60px'>
                <div align='center'>
                <img src='{artwork_url}'style='max-height:130px; max-width:100%'>
                </div>
            </td>
            </tr>
            </table>
            """


def get_order_html(meta_one, exit_factory_date, make_one_request_type="finished goods"):
    one_number = meta_one.one_number

    image_table = get_image_table_html(meta_one)

    brand_name = meta_one.get_brand_name()

    size_breakdown_table = meta_one.size_code

    qty = 1

    qr_code_link = (
        f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data="
        f"{one_number}%09"
    )

    template = f"""
    <html style="margin: 20px">
        <table width='100%' height='100'>
                <tr>
                    <th valign='top' width='40%'><font size='5'>
                     <div align='left'>ORDER </div></th>
                    <td valign='top' width='10%'> <font color='white'> </font> </td>
                        <td valign='top' width='25%'><font size='3'> <strong> <div align='center'>EX - FACTORY<br> DATE</strong></font></td>
                        <td valign='top' width='25%'><strong><div align='center'> UNITS</strong></td>
                </tr>
                <td><font size='10'> 
                  <div align='left'>
                   # {one_number}
                 </div>
                </td>
                <td><font color='ffffff'><img src='{qr_code_link}' style='max-height:100%; max-width:100%'></td>
                <td><div align='center'><font size='5'>
                {exit_factory_date.strftime("%d %B, %Y")}
                </td> 
                <td><div align='center' style='position: relative;'>
                <img src='https://s3.amazonaws.com/resonance-magic-0.0.0.0/Factory+Orders/small-grey.png' style='max-height:100%; max-width:100%'> 
                <div align  style='position: absolute; top: 0px; left: 90px;'><font size='10' color='000000'>
                    {qty}
                </div></td>
            </tr>
            </table>
            <br>
            <table width='100%' height='5'>
            <tr>
            <td colspan='4'> <hr size = '5' noshade='' ></td>
            </tr>
            </table>
              {image_table}
            <table width= '100%' >
                <tr>
                    <td> <div align='center'> <font size='5'>{meta_one.size_code}</font></div>
                </td>
                </tr>
                <tr>
                    <td colspan='4'>
                        <hr size='5' noshade=''>
                    </td>
                </tr>
            </table>
            <br>
            <table width='100%' height='45'>
                <tr>
                    <td>
                        <div align='center' style='position: relative;'><img src='https://s3.amazonaws.com/resonance-magic-0.0.0.0/Factory+Orders/wide-grey.png' width='100%' height='50'>
                            <div align style='position: absolute; top: 10px; left: 250px;'><font size='5' color='ffffff'> 
                            {1} <font size='3'>     &nbsp;  &nbsp;  &nbsp;  OF &nbsp;   &nbsp;  &nbsp;  </font>
                            {meta_one.style_sku} in sizes {meta_one.size_code}</font>
                            </div>
                        </div>
                    </td>
                </tr>
            </table width='100%'>
            {size_breakdown_table}
            </table>
            </tr>
            <br>
            <table width='100%' height='50' border='1' style='border-collapse: collapse;'>
                <tr>
                    <td style='word-wrap: break-word'>
                        <div align='left'><strong>  Notes:</strong> This is a {make_one_request_type} request.
                            <br> </div>
                    </td>
                </tr>
            </table>
            <br>
            <br>
            <table width='100%' height='50'>
                <tr align='left'>
                    <th><font size='2'>Bill To:</th>    
                <th><font size='2'>Ship To:</th>
                <th><font size='2'>Manufactored By:</th>
            </tr>
            <tr align='left'><font size='2'>
                <td width='30%'><font size='2'><strong>{brand_name}</strong><br>59 Chelsea Piers<br> Suite 5926 <br> New York, NY   10001<br><br><br></td> 
                <td width='30%'><font size='2'><strong>Resonance</strong><br>59 Chelsea Piers<br> Suite 5926 <br> New York, NY  10001<br>ATTN: Silvia/Leo<br><br></td>
                <td width='30%'><font size='2'><strong>Resonance Manufacturing</strong><br>Zona Franca Industrial Santiago -Etapa II<br>Calle Navarrete no. 4 <br>Santiago, Dominican Republic <br>ATTN:NALDA MORONTA <br> TEL: 809-575-4100 <br> <br></td></font>
                </tr>
            </table>
            </font>
            <table>
                <tr>
                    <td>
                        <img src='https://s3.amazonaws.com/resonance-magic-0.0.0.0/Factory+Orders/FactoryOrder+Footer.png' width='100%'>
        </table>
        </html>
    """

    return template


def get_order_html_2(
    self,
    exit_factory_date,
    memo="Care of [], Ship to []",
):
    def get_first_cover_image(sku):
        Q = """query style($code: String) {
          style(code: $code) {
            id
            coverImages {
              url
              s3 {
                key
                bucket
              }
            }

          }
        }"""

        try:
            g = res.connectors.load("graphql")
            return g.query_with_kwargs(Q, code=sku)["data"]["style"]["coverImages"][0][
                "url"
            ]
        except:
            res.utils.logger.info("Failed to resolve a cover image")
            return None

    def to_row(d):
        suffix = lambda k: "-".join(k.split("-")[3:])
        return f"""<tr>
                <td>{suffix(d['key'])}</td>
                <td>{d['garment_piece_material_code']}</td>
                <td>{d['garment_piece_color_code']}</td>

            </tr>"""

    stuff = [
        to_row(d)
        for d in self._data[
            [
                "key",
                "garment_piece_material_code",
                "garment_piece_color_code",
                "artwork_uri",
            ]
        ].to_dict("records")
    ]
    stuff = " ".join(stuff)

    cover_image = get_first_cover_image(self.style_sku)

    # if we dont have a cover image maybe a thumbnail

    qr_code_link = (
        f"https://api.qrserver.com/v1/create-qr-code/?size=150x150&data="
        f"{self.one_number}%09"
    )

    num_pieces = len(self._data)
    H = f"""<html>
    <style>
    .styled-table {{
        border-collapse: collapse;
        margin: 25px 0;
        font-size: 0.7em;
        font-family: sans-serif;
        min-width: 650px;
        box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
    }}

    </style>
    <head>

    </head>
    <body>
    <table class="styled-table">
        <td style="text-align: left">
            <h3># {self.one_number} </h3> <br>
          
            <img src='{qr_code_link}' style='max-height:150px; max-width:200px'>
        </td>
        <td style="vertical-align: baseline; text-align: left;">
            <h3>{self.sku} </h3> 
            
            <p style="font-size:45px;  text-align: left;">    {self.normed_size}</p>
        </td>
        <td><img src='{cover_image}' style='max-height:200px; max-width:400px'></td>
        </tr>

    </table>
    <hr/>
    <div>
     <h3>Make Date: {exit_factory_date.strftime("%d %B, %Y")} </h3>  {memo}
    </div>

    <hr/>

        <table class="styled-table">
        <thead>
            <tr>
                <th>{num_pieces} Pieces</th>
                <th>Material</th>
                <th>Color</th>
            </tr>
        </thead>
        <tbody>
           {  stuff  }     
            <!-- and so on... -->
        </tbody>
    </table>

    <table>
    <tr><td> <img src='https://s3.amazonaws.com/resonance-magic-0.0.0.0/Factory+Orders/FactoryOrder+Footer.png' width='650px'></td></tr>
    </table>

    </body>

    </html>"""
    return H
