"""
TODO
- image processing perf. some suspicious slow handling of large sizes e.g. take a 6x JCRT
- fix block fuse icon bug - the square is always white
- fix the luminosity reading in the viable surface
- properly implement is-color-placement in future flow - we have used the meta data for now
- port new fusing stuff in
- unit tests for piece labellers
- move fonts to docker and deprecate svg icons that are not in repo
- implement gets status no the m1 for the prepare assets
- set the versions in the data for the files - could probe in the caching - keep in mind we have the annotated image version
- the status and the piece observations and propagation of make seq
"""

import res
import requests
from schemas.pydantic.meta import MetaOnePrintAsset, MetaOnePrintablePiece
from schemas.pydantic.make import PrepPieceResponse, PrepPrintPiece, PPPStats
from typing import Union
from dateparser import parse
from datetime import datetime, timedelta
import pytz
from res.media.images.geometry import (
    MultiLineString,
    substring,
    LineString,
    Polygon,
    make_v_structure_element,
    translate,
    MultiPoint,
    swap_points,
    pil_image_alpha_centroid,
    shift_geometry_to_origin,
    move_polygon_over_alpha,
    scale,
)
from res.media.images.outlines import get_piece_outline, place_artwork_in_shape
from res.flows.meta.ONE.geometry_ex import (
    parse_seam_guides,
    parse_placeholders,
    extract_points,
    extract_notch_lines,
    notch_blocks,
    _remove_shapes_from_outline,
)
from res.flows.meta.ONE.annotation import (
    draw_outline_on_image,
    draw_positional_notches,
    load_identity_symbol,
    color_from_luminosity,
    AnnotationManager,
)
from res.flows.make.materials import get_material_properties
from res.flows.meta.pieces import PieceName
from res.utils import ping_slack, post_with_token
from res.flows.meta.ONE import queries
from pathlib import Path
from res.media.images.geometry import LinearRing, unary_union, translate
import numpy as np
from res.media.images import get_perceptive_luminance
from res.flows.meta.pieces import PieceName
from res.flows.dxa.res_color import get_one_label
from functools import partial
import pandas as pd
import json
from res.flows.meta.ONE.bom import get_body_bom, get_thread_colors
from res.flows.meta.ONE.queries import fix_sku
from tenacity import retry, wait_fixed, stop_after_attempt

DEFAULT_LIGHT_COLOR = (150, 150, 150, 255)
DEFAULT_LTHRESHOLD = 0.9
CUTLINE_THICKNESS = 24
DPI = 300
QUARTER_INCH = DPI / 4.0
CACHE_SCHEMA_VERSION = "v1"


class MetaOnePieceManager:
    """
    The piece manager wraps the raw data loaded from the database/api
    Provides drawing functions and geometry functions
    """

    def __init__(self, meta_one_piece: MetaOnePrintablePiece):
        self._piece = meta_one_piece
        self._piece_image = None
        self._annotation_manager = AnnotationManager(self)
        # these are lazy loaded if needed

    def __getattr__(self, name):
        try:
            return object.__getattribute__(self, name)
        except:
            return getattr(self._piece, name)

    def __repr__(self):
        return f"Piece Manager: Piece {self._piece.key}"

    @property
    def piece_info(self):
        """
        provides the structured piece object
        it renders as SVG in jupyter so the components are obvious
        piece component derived values such commercial acceptability zones or grouping symbols are given
        """
        return PieceName(self._piece.key)

    @property
    def piece_image(self):
        """
        load the s3 image to PIL object (renders in jupyter)
        cached in memory for single load
        """
        if self._piece_image is None:
            s3 = res.connectors.load("s3")
            # could also check we are not a full width piece
            if (
                s3.exists(self._piece.image_uri)
                and not self.is_piece_material_dependent
            ):
                res.utils.logger.info(
                    f"loading the image s3-version={self.image_s3_version} into memory..."
                )
                # TODO have not implemented versions in this loader
                self._piece_image = s3.read_image(
                    self._piece.image_uri, version_id=self.image_s3_version
                )
            else:
                res.utils.logger.info(
                    f"No valid image file or uri provided. The uri is {self._piece_image} - generating for {self.piece_outline} in the case that mat dep {self.is_piece_material_dependent}"
                )
                self._piece_image = place_artwork_in_shape(
                    self.piece_outline,
                    self.artwork_uri,
                    simple_rectangle=self.is_piece_material_dependent,
                )

        return self._piece_image

    @property
    def piece_outline(self):
        """
        the piece outline comes from the body - except if we have some custom logic such as stretch to material
        """
        if self.is_piece_material_dependent:
            # we strictly support rect
            return self.try_transform_rectangle_piece_outline()
        else:
            return LinearRing(self._piece.outline)

    @property
    def is_piece_material_dependent(self):
        """
        a special base that operates as a function of BxM
        """
        return self.piece_info.part in ["FX"]

    @property
    def piece_corners(self):
        return MultiPoint(self._piece.corners)

    @property
    def piece_image_outline(self):
        """
        in future this will be loaded from the database
        """
        return get_piece_outline(np.asarray(self.piece_image))

    @property
    def piece_notches(self):
        sg = MultiLineString(self._piece.seam_guides)
        mp = MultiLineString(self._piece.notches)
        valid_notch_lines = [
            l for l in mp.geoms if len(sg.geoms) == 0 or l.distance(sg) > 50
        ]
        # apply a filter on anything that is to close to seam guides
        return MultiLineString(valid_notch_lines)

    @property
    def piece_seam_guides(self):
        def is_line_away_from_outline(l):
            try:
                if (
                    l.interpolate(0, normalized=True).distance(self.piece_outline) < 10
                    and l.interpolate(1, normalized=True).distance(self.piece_outline)
                    < 10
                ):
                    return False
            except:
                res.utils.logger.warn(
                    f"Failed to prove one way or another but dont want to screw up all for an edge case"
                )
            return True

        try:
            ml = MultiLineString(self._piece.seam_guides)
            # remove zero seam allowance guides
            ml = unary_union(
                [l for l in ml.geoms if is_line_away_from_outline(LineString(l))]
            )
            short_seam_guides = [substring(l, 0, QUARTER_INCH) for l in ml.geoms]
            # rh - sometimes these collapse to points (TH-4000-V1-JKTWTWBN-BF)
            short_seam_guides = [
                s for s in short_seam_guides if s.geom_type == "LineString"
            ]
            return MultiLineString(short_seam_guides)
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to load piece seam guide for {self._piece.key} ({self._piece.seam_guides}): {repr(ex)}"
            )
            return MultiLineString()

    @property
    def surface_geometries(self):
        return unary_union(
            [self.piece_outline, self.piece_seam_guides, self.piece_notches]
        )

    @property
    def viable_surface(self):
        DISTANCE_BUFFER = 20

        # for knits lets just put the thing anywhere near the edge for now.
        if self.offset_size_px > 0:
            return self.buffered_piece_outline

        a = self.piece_outline - self.piece_seam_guides.buffer(10)
        # a = a - self.corners.buffer(DISTANCE_BUFFER)
        surface_notches = unary_union(
            [l.interpolate(0) for l in self.piece_notches.geoms]
        )
        a = a - surface_notches.buffer(DISTANCE_BUFFER)
        a = a - self.piece_corners.buffer(DISTANCE_BUFFER)
        return a

    def get_piece_notch_vs(self, min_seam_guide_distance=QUARTER_INCH):
        """
        get the V notches that are not too close to seam guides and makes the structure element
        """

        f = make_v_structure_element(self.piece_outline)
        sg = self.piece_seam_guides
        vs_notches = [
            f(l.interpolate(0))
            for l in self.piece_notches
            if l.distance(sg) > min_seam_guide_distance
        ]
        v = unary_union(vs_notches)
        return v

    def get_piece_notch_blocks(self, half_width=3, fill=False):
        """
        expanding the lines into blocks
        parallel lines and merging the convex hull
        this can be merged into the outline if required

        """
        notch_like_things = unary_union([self.piece_seam_guides, self.piece_notches])

        notch_like_things = unary_union(
            notch_blocks(notch_like_things, half_width=half_width, fill=fill)
        )

        return Polygon(self.piece_outline).intersection(notch_like_things)

    @property
    def buffered_piece_outline(self):
        return self.get_buffered_piece_outline()

    def get_piece_physical_outline(self, use_vs=False, half_width=3):
        """
        the physical outline is what we call the outline with "physical" notches
        physical notches are not drawn but cut

        To see how this might look for knits use
        Knits mode will apply if the offset size is non 0 on the piece

        from geopandas import GeoDataFrame
        G = mp.get_piece_physical_outline(use_vs=True, half_width=7)
        GeoDataFrame([G],columns=['geometry']).plot(figsize=(40,40))
        """
        g = self.piece_outline
        """
        note we use the offset size being non zero to see if its unstable knit
        """

        def omit(n):
            """
            a special case omission for now on neck bindings after speaking to Xi to help sewers
            """
            k = PieceName(self._piece.key)._key
            if "NBDG" in k:
                bnds = self.piece_outline.bounds[-2]
                return n.x > 50 and n.x < (bnds - 50)
            return False

        vs = (
            self.get_piece_notch_vs()
            if self._piece.offset_size_px > 0 or use_vs
            else MultiLineString([])
        )

        notches = self.get_piece_notch_blocks(fill=True, half_width=half_width)
        # notches are the square type castle like notches that are eroded
        if notches.geom_type != "Polygon":
            notches = unary_union([n for n in notches if not omit(n.centroid)])

        if not notches.is_empty:
            g = _remove_shapes_from_outline(g, notches)

        # vs are the protruding v notches mostly on knits
        if not vs.is_empty and (vs.type == "Polygon" or len(vs.geoms)):
            g = unary_union([vs, Polygon(g)]).boundary

        # rh - handle weird shapely edge case where the thign is a multilinestring with one line in it
        # and the notch adding edge case of putting the notches in the wrong place.
        if g.geom_type == "MultiLineString":
            return max(g.geoms, key=lambda g: g.length)

        return g

    def _get_image_offset(self, shape):
        """
        given a shape, determine a reference offset from the image it is placed in
        """
        image_bounds = self.piece_image_outline.bounds
        width_diff = image_bounds[2] - shape.bounds[2]
        height_diff = image_bounds[3] - shape.bounds[3]
        offset_x = width_diff / 2
        offset_y = height_diff / 2

        return offset_x, offset_y

    def get_buffered_piece_outline(self, close_openings=False):
        """
        we buffer the meta piece outline with a buffer >= 0
        this is then offset for the image ots placed in - generally it should match the image outline itself approximately

        because sometimes the cutlines come into the piece and cause problems, we can remove small openings
        """

        ol = self.piece_outline
        if close_openings:
            T = 25
            res.utils.logger.debug(f"Closing small opening < {T}")
            ol = Polygon(ol).buffer(T).buffer(T * -1).exterior

        ol = (
            Polygon(ol).buffer(self.offset_size_px).boundary
            if self.offset_size_px
            else ol
        )
        offset_x, offset_y = self._get_image_offset(ol)

        ############# image diff offset
        im_size = self.piece_image.size
        ol_size = ol.bounds[2], ol.bounds[3]

        offset = (
            (im_size[0] - ol_size[0]) / 2,
            (im_size[1] - ol_size[1]) / 2,
        )
        # offset_x += offset[0] # no idea why this x isn't needed but y is...
        offset_y += offset[1]
        ################

        g = translate(ol, xoff=offset_x, yoff=offset_y)

        # need to check this because im sure why the buffer boundary is anything but a line string
        if g.type == "MultiLineString":
            return g[0]

        return g

    def get_surface_font_color(self):
        """
        simple color
        """
        color = (0, 0, 0, 255)

        all_notches = unary_union([self.piece_notches, self.piece_seam_guides])

        # a circular piece may not have any notches
        if all_notches.geoms == []:
            return DEFAULT_LIGHT_COLOR

        L = get_perceptive_luminance(
            self.piece_image, [l.interpolate(0) for l in all_notches]
        )

        return color if L < DEFAULT_LTHRESHOLD else DEFAULT_LIGHT_COLOR

    def get_sew_identity_label(self, site_luminosity=0):
        """
        supply the luminosity of the site where the symbol will be placed
        the sew identity symbol provides info to distinguish pieces based on avatar location
        """
        color = color_from_luminosity(site_luminosity)
        if self.sew_identity_symbol != None:
            symbol = load_identity_symbol(self.sew_identity_symbol, color=color)
            if symbol:
                return symbol.resize((50, 50))

    def get_grouping_label(self, site_luminosity=0):
        """
        supply the luminosity of the site where the symbol will be placed
        the grouping label is a symbol based on the body part avatar region
        """
        color = color_from_luminosity(site_luminosity)
        symbol = self.piece_info.get_grouping_symbol(color=color)
        if symbol:
            return symbol.resize((50, 50))

    def get_one_number_label(self, one_number, site_luminosity=0, **kwargs):
        """
        supply the luminosity of the site where the symbol will be placed
        the grouping label is a symbol based on the body part avatar region

        make type denotes if its healing or extra piece

        """

        make_type = ""  # is_healing, is_extra
        if kwargs.get("is_extra"):
            make_type = "extra"
        if kwargs.get("is_healing"):
            make_type = "healing"
        color = color_from_luminosity(site_luminosity)
        res.utils.logger.debug(f"font forecolor based on luminosity is {color}")
        # experimental
        if kwargs.get("reserve_one_label"):
            one_number = "12345678"
            color = (0, 0, 0, 0)
        lbl = get_one_label(
            one_number,
            piece_type=self.piece_type,
            piece_make_type=make_type,
            my_part=f" ({self.piece_ordinal}/{self.piece_set_size})",
            # color=color,
            one_label_text_color=color,
        )

        return lbl

    def try_transform_rectangle_piece_outline(self):
        """
        For special pieces we can apply a scaling on the geometries so its fits the with of the the material for this piece

        the material properties are sometimes loaded for the meta one piece's materials and if they exist we can use that information


        """

        md = self._piece.metadata
        if "cuttable_width" in md:
            # required: material_width_px / scale_x - 2 * buffer_px

            offset_size = DPI * 2 * (md.get("offset_size") or 0)
            cuttable_width = md["cuttable_width"] * DPI
            # the piece when
            compensated_width = cuttable_width - offset_size
            width = int(compensated_width / md["compensation_width"])
            # assumption for this mode
            # use just the length part of the ract
            rectangle = self._piece.outline
            # set the rectangle width
            PAD = 21
            rectangle[1][0] = width - PAD
            rectangle[2][0] = width - PAD
            outline = LinearRing(rectangle)

            return shift_geometry_to_origin(outline)

        return self._piece.outline

    @staticmethod
    def annotate_cached_image_from_ppp(data: Union[PrepPrintPiece, dict]):
        """
        wrapper method so the existing PPP response for the piece can be used
        you can get the one number and is-healing rank from the PPP parent but then you want to pass the pieces
        one by one, into this thing.
        there are some other make context things in here such as the make sequences

        we could usefully create a factory for this that just parses this stuff from the PPP parent to iterate over images
        """

        data = data.dict() if not isinstance(data, dict) else data

        # we used to pass these as params now we embed them in data adjacent to the annotation_json
        multi_piece_sequence: int = data.get("multi_piece_sequence")
        style_make_instance: int = data.get("style_make_sequence_number")
        sku_make_instance: int = data.get(
            "sku_make_sequence_number"
        )  ##see PP response on make pieces
        is_healing: bool = data.get("is_healing")
        is_extra: bool = data.get("is_extra")

        """
        we can do some renaming between the schema if necessary here
        """
        data["annotations_json"] = json.loads(data["annotation_json"])
        data["key"] = data["piece_name"]
        data["annotated_image_uri"] = data["filename"]
        data["piece_type"] = PieceName(data["key"]).piece_type

        return MetaOnePieceManager.annotate_cached_image(
            one_number=data["asset_key"],
            data=data,
            style_make_instance=style_make_instance,
            multi_piece_sequence=multi_piece_sequence,
            sku_make_instance=sku_make_instance,
            is_healing=is_healing,
            is_extra=is_extra,
        )

    @staticmethod
    def annotate_cached_image(
        one_number,
        data,
        multi_piece_sequence=None,
        style_make_instance=None,
        sku_make_instance=None,
        **options,
    ):
        """

        ***
        - similar what draw can do -
        static because its supposed to be determined entirely from data and make context

        the data corresponds to the cached schema of what we cache once per style
        this includes the annotated images and the reserved annotations on the image
        we can compile all this plus import attributes used for labelling in `data`
        ***

        :param one_number: the make one number
        :param multi_piece_sequence: for labels or similar we create multiple instances of same piece and need to id them
        :param style_make_instance: a specific style (BMC) in any size, how many times have we made it
        :param sku_make_instance: a specific sku (BMCS) in a size, how many times have we made it
        :param data: the cached data row for any style piece

        Notes:
        loading an image from a predetermined cached location - raise an exception if the file does not exist or cache it now
        its important to know the difference between the cached annotated images and other images because they have cutlines and other things on them

        We add a small offset e.g. half the cutline width to all the reservations
        """

        # d = search_meta_annotations(skus=['LH-3003 LN135 CREABM 4ZZLG'], piece_filters=['LH-3003-V1-SHTBKYKELFTP-S'])
        # d = dict(d.drop(['outline_0','outline_1'],1).iloc[0])

        """
        take a copy as we will manipulate dictionaries
        """

        reservations = {d["key"]: dict(d) for d in data["annotations_json"]}

        # start off a little inside the cutline from the outside of the image which should be 0.5 cutline shifted
        OFFSET_TUPLE = (CUTLINE_THICKNESS * 1.2, CUTLINE_THICKNESS * 1.2)
        """
        add the one label - this uses the data to provide some extra bits + options
        apart from these we need the make time ONE number
        """
        # we can generate the image here for that width pece using place_artwork_in_shape

        if "LBLBYPNL" not in data["key"]:
            # with the exception of labels
            label_fn = partial(
                AnnotationManager.one_label_function_from_data(data),
                one_number=one_number,
                **options,
            )
            try:
                im = AnnotationManager.place_label_from_loaded_reservation(
                    im=res.connectors.load("s3").read_image(
                        data["annotated_image_uri"]
                    ),
                    label_fn=label_fn,
                    reservation=reservations["one_label"],
                    offset=OFFSET_TUPLE,
                )
            except Exception as ex:
                res.utils.logger.debug(f"Fail:{ex}")
                # ping_slack(
                #    f"Cannot place one label on part {data['key']} for {one_number} ```{repr(ex)}```",
                #    "autobots",
                # )
                im = res.connectors.load("s3").read_image(data["annotated_image_uri"])
        else:
            im = res.connectors.load("s3").read_image(data["annotated_image_uri"])

        """
        for each reservation, we need to get the label; size, qr code, special edition
        what we call "body level" are positional 
        """

        for (
            reservation_name,
            reservation,
        ) in AnnotationManager.enumerate_body_level_reservations(reservations):
            res.utils.logger.info(f"Annotate {reservation_name} {sku_make_instance}")
            label_fn = partial(
                AnnotationManager.label_function_from_data_and_reservation_name(
                    data, reservation_name
                ),
                multi_piece_sequence=multi_piece_sequence,
                style_make_instance=style_make_instance,
                sku_make_instance=sku_make_instance,
                one_number=one_number,
                **options,
            )

            res.utils.logger.debug(f"Offset the placement by")
            im = AnnotationManager.place_label_from_loaded_reservation(
                im=im,
                label_fn=label_fn,
                reservation=reservation,
                offset=OFFSET_TUPLE,
            )

        return im

    def draw(self, im=None, one_number=None, labels=None, reservations=None, **options):
        """
        we only show what the print assets needs
        use existing meta one for previews / pull that out into annotations
        labels can be resources, s3 uris or image data
        the annotation lingo allows for specific placements or conventional placements
        for example one numbers are conventionally placed

        label types
        - text
        - identity_symbol
        - text_with_make_symbols

        labels generally are placed on a background color which is computed in the region
        the foreground of the text is also determine by the region
        """
        if im is None:
            im = self.piece_image

        is_label_type = "LBLBYPNL" in self.piece_info.name

        ol = self.get_buffered_piece_outline(close_openings=True)

        # align stuff on the image properly.
        dx, dy = 0, 0
        cutline_thickness = CUTLINE_THICKNESS
        if self.offset_size_px > 0:
            # place knit cutline by matching up the centroids of the cutline and piece image.
            dx, dy = move_polygon_over_alpha(im, ol)
            # also do a thin line.
            cutline_thickness = 12
            # remake the annotator so it knows about dx and dy -- we could consider setting inside_offset_amount
            # to some function of the offset_size_px here - to give the cutter some opportunity to actually have the
            # piece label inside teh cut piece.  as it is with this it will be discarded when cutting.
            self._annotation_manager = AnnotationManager(self, dx=dx, dy=dy)

        ol = translate(ol, dx, dy)
        all_notches = translate(
            unary_union([self.piece_notches, self.piece_seam_guides]), dx, dy
        )

        im = draw_positional_notches(
            im,
            all_notches,
            outline_guide=ol,
            color=self.get_surface_font_color(),
            ignore_notch_distance_errors=self.offset_size_px > 0,
        )

        """
        built in labels
        the label function of local luminosity is passed allowing us to make the color dynamic
        You can test each of the functions in isolation to see how they respond to luminosity
        note the average of background is always used as a bed for the labels
        """

        label_functions = {
            "group_label": self.get_grouping_label,
            "sew_identity_label": self.get_sew_identity_label,
        }

        if one_number:
            label_functions["one_label"] = partial(
                self.get_one_number_label, one_number=one_number, **options
            )
        elif options.get("reserve_one_label"):
            label_functions["one_label"] = partial(
                self.get_one_number_label,
                one_number="12345678",
                reserve=True,
                **options,
            )

        """
        place labels either a new or from reservations - but not if its a body label
        """

        if not is_label_type:
            for key, label_fn in label_functions.items():
                plan = True if not one_number and key == "one_label" else False
                # check if we have an existing reservation and use it for this label

                self._annotation_manager.place_label_with_reservation(
                    im,
                    label=label_fn(),
                    label_fn=label_fn,
                    plan=plan,
                    reservations=reservations,
                    key=key,
                    # this label should just be placed with gravity
                    directional_label=True if key in ["sew_identity_label"] else False,
                )

        """
        cutline always needs to be drawn last because it changes the coordinate reference
        """
        im = draw_outline_on_image(
            im,
            ol=ol,
            thickness=cutline_thickness,
        )

        return im

    @staticmethod
    def cost_for_pieces_in_material(sku, material_code=None, piece_filter=None):
        """
        get the status for the sku
        """
        try:
            from res.flows.meta.ONE.queries import _get_costs_match_legacy

            x = _get_costs_match_legacy(sku, stat_name="piece_stats_by_material")[
                "piece_stats_by_material"
            ]
            assert not (
                len(x) > 1 and material_code == None
            ), "If there are multiple materials you must specify which one you want stats for"
            x = [a for a in x if a["material_code"] == material_code or len(x) == 1]
            if len(x):
                x = x[0]
                total = x["piece_count"]
                if piece_filter:
                    x["piece_count"] = len(piece_filter)
                    for k in x.keys():
                        if x[k] and ("area" in k or "height" in k):
                            factor = total / len(piece_filter)
                            x[k] /= factor
            return PPPStats(**x).dict()
        except Exception as ex:
            res.utils.logger.warn(f"Failing to add costs {ex}")
            return {}

    @staticmethod
    def prepare_prep_piece_response(
        request,
        cache=None,
        piece_instance_map=None,
    ):
        """
        This uses a special case of data for the meta one
        if we have to we build the cache inline but its faster to check this if we already cached

        find a request PPP request (see kafka schema) or generate one for one of these skus and call the method with the request only

        """

        # note that this is fucked because the kafka schema doesnt expect a sku field and somehow
        # just drops it instead of rejecting the entire message.
        assert "sku" in request, "the sku is required in the v2 flow for PPP"

        DEFAULT_MAKE_SEQUENCE = 1  # the first time, 2 for the first healing

        def get_make_sequence_for_piece(code):
            if not piece_instance_map:
                return 0
            return (
                piece_instance_map.get(code, {}).get("make_sequence_number")
                or DEFAULT_MAKE_SEQUENCE
            )

        sku = fix_sku(request["sku"])

        cache = (
            cache if cache is not None else MetaOnePieceManager.read_meta_one_cache(sku)
        )

        piece_filter = piece_instance_map.keys() if piece_instance_map else None

        cache_records = {d["piece_code"]: d for d in cache.to_dict("records")}

        """
        support filtering pieces by long or short codes.
        """
        res.utils.logger.info(f"PPP filtered by pieces {piece_filter}")

        def hp(piece_code, i=None):
            cache = cache_records[piece_code]
            piece_key = cache["key"]
            """
            for any aux files we depend on we will link the version for the related file
            """
            d = {}
            d["asset_id"] = request["id"]
            d["asset_key"] = request["one_number"]
            d["piece_name"] = (
                piece_key if i is None else PieceName(piece_key).identity_prefix(i)
            )
            d["piece_code"] = piece_code
            d["piece_type"] = cache["piece_type"]
            # file version is cache['s3_version_id']
            d["filename"] = cache["annotated_image_uri"]
            d["cutline_uri"] = cache["cutline_uri"]
            d["outline_uri"] = cache["outline_uri"]
            d["artwork_uri"] = cache["artwork_uri"]
            d["fusing_type"] = cache["fusing_type"]
            d["make_sequence"] = get_make_sequence_for_piece(piece_code)

            d["sku_make_sequence_number"] = request.get("sku_make_sequence_number")
            d["style_make_sequence_number"] = request.get("style_make_sequence_number")
            d["annotation_json"] = cache["annotations_json"]
            d["piece_set_size"] = cache["piece_set_size"]
            d["piece_ordinal"] = cache["piece_ordinal"]
            d["piece_area_yds"] = cache["piece_area"]

            # not sure about this and who uses it
            # TODO add the annotation json when we area ready to test that
            d["meta_piece_key"] = res.utils.res_hash(d["filename"].encode())
            d["piece_id"] = res.utils.res_hash(
                f"{d['asset_id']}{d['piece_name']}".encode()
            )

            d["requires_block_buffer"] = cache["requires_block_buffer"]

            # for healing - keep the meta one cache path along with each piece also.
            d["meta_one_cache_path"] = cache["meta_one_cache_path"]

            return d

        request["make_pieces"] = []
        for piece_code in cache_records.keys():
            if piece_instance_map is None or piece_code in piece_instance_map:
                multiplicity = (
                    None
                    if piece_instance_map is None
                    else piece_instance_map.get(piece_code, {}).get(
                        "multiplicity", None
                    )
                )
                if multiplicity is None or multiplicity == 1:
                    request["make_pieces"].append(hp(piece_code))
                else:
                    request["make_pieces"].extend(
                        hp(piece_code, i) for i in range(multiplicity)
                    )
        request["piece_count"] = len(request["make_pieces"])

        style_code = " ".join(
            [
                request["body_code"],
                request["material_code"],
                request["color_code"],
            ]
        )
        request["metadata"]["thread_colors"] = "|".join(get_thread_colors(style_code))
        request["uri"] = "foo"
        """
        Load statistics for PPP back compat
        """

        # request.update(
        #    MetaOnePieceManager.cost_for_pieces_in_material(
        #        sku, material_code=request["material_code"], piece_filter=piece_filter
        #    )
        # )
        ###########################

        return PrepPieceResponse(**request)

    @staticmethod
    def enumerate_annotated_images_from_ppp(data: PrepPieceResponse):
        """
        the ppp response is the data from what we generated in make
        we can use this to restore labelled images for make
        if its not convenient to reconstruct the PPP response some helpers can be provided OR
        use the piece level one to provide a smaller set of attributes per piece and iterate that way
        """

        for d in []:
            yield None

    @staticmethod
    def annotated_image_from_ppp_piece(data: PrepPrintPiece):
        return None

    @staticmethod
    def cache_path(sku, m1=None):
        m1 = m1 or get_meta_one(fix_sku(sku))
        uri_root_metadata = m1.annotated_images_metadata_uri.rstrip("/")
        return f"{uri_root_metadata}/piece_info.parquet"

    @staticmethod
    def make_cache_dirty(sku, body_version, hard_reset=False):
        """
        when creating meta ones in the style node, we should dirty any that exist
        sometimes we replace color in some way and we dont want the cache to persist
        ordinarily when the style node runs, there is no cache.......
        """
        try:
            if not MetaOnePieceManager.is_cache_ready(sku):
                res.utils.logger.info(
                    f"No need to invalidate cache as it does not exist for sku {sku}"
                )
                return
        except Exception as ex:
            res.utils.logger.warn(
                f"Cache ready check failed-  maybe no cache - make check safer {ex}"
            )

        res.utils.logger.info(f"Invalidating cache for {sku}")

        try:
            uri_root_metadata = (
                MetaOnePrintAsset.get_annotated_images_metadata_uri_for_sku(
                    sku, body_version=body_version
                ).rstrip("/")
            )
            s3 = res.connectors.load("s3")
            # keep a record
            s3.copy(
                f"{uri_root_metadata}/piece_info.parquet",
                f"{uri_root_metadata}/piece_info_dirty.parquet",
            )
            # remove the thing we look for to see the cache is valid
            s3._delete_object(f"{uri_root_metadata}/piece_info.parquet")

            res.utils.logger.info(
                f"Invalidated cache {uri_root_metadata}/piece_info.parquet"
            )
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to invalidate even though the cache claimed readiness {ex}"
            )

    @staticmethod
    def is_cache_ready(sku, m1=None):
        """
        this simply checks for a meta file piece_info.parquet so we can invalidate the cache easily by removing that file
        there is still some stale stuff lying around but this should be overwritten when the caches is restored.
        there may be some risk of stale stuff being loaded but the piece_info that is remade should be the master record
        """
        return res.connectors.load("s3").exists(
            MetaOnePieceManager.cache_path(sku, m1=m1)
        )

    @staticmethod
    def read_meta_one_cache(sku, m1=None, cache_path=None):
        return res.connectors.load("s3").read(
            cache_path
            if cache_path is not None
            else MetaOnePieceManager.cache_path(sku, m1=m1)
        )

    @staticmethod
    def write_meta_one_cache(sku, m1=None):
        """
        cache annotations works at style BMC+Size level to pre cache the annotate image without the make context
        the style is the placed color and we add notches and symbols on top of it - these are assumed to change slowly

        there is a version field allowing us to change the schema or format
        """
        from tqdm import tqdm

        sku = fix_sku(sku)

        s3 = res.connectors.load("s3")
        m1 = m1 or get_meta_one(sku)
        """
        RH - for now assume the meta one is valid since the validation check is upstream.
        This is screwing up healing pieces of assets which should not have passed validation...
        if not validate_meta_one(sku, m1=m1):
            requests.post(
                "https://data.resmagic.io/meta-one/bertha/redo_apply_color_requests_new_flow?test=false",
                json=[sku],
            )
            raise ValueError(f"Meta one {sku} failed validation.")
        """
        uri_root_pieces = m1.annotated_images_uri.rstrip("/")
        metadata_uri = MetaOnePieceManager.cache_path(sku, m1=m1)

        res.utils.logger.info(
            f"Processing - \n-Metadata to: {metadata_uri} \n-Files to: {uri_root_pieces}"
        )
        """
        temp add fusing here - we are moving it to the database
        """
        ############################################################
        res.utils.logger.info("Fetching BoM data from airtable for now")
        bom = get_body_bom(m1.body_code)
        if bom is None:
            ping_slack(f"[PPP] Failed to get BOM for body {m1.body_code}", "autobots")
            bom = {}
        fusing_info = bom.get("piece_fusing", {}) or {}
        fusing_info = {k: v["code"] for k, v in fusing_info.items()}
        ############################################################

        records = []
        for p in tqdm(list(m1)):
            p = MetaOnePieceManager(p)
            uri = f"{uri_root_pieces}/{p.key}.png"
            """
            TODO we want a more sophisticated system here
                 we should first check a uri where the merge could have happened
                if not (s3.exists(uri) or s3.exists(compacted_uri)) or invalidate:    
                this would allow us to move the files into large parquet files but keep a pointer to the file 
                duckdb scales well with large parquet files one would expect
                so we can run periodic compaction to move the cache into fewer large files
            """
            res.utils.logger.info(f"Preparing annotated image {uri}")
            im = p.draw(reserve_one_label=True)
            file_info = s3.write_get_info(uri, im)
            s3_version_id = file_info["s3_version_id"]
            res.utils.logger.info(f"Saved with info {file_info}. Getting outline")
            """
            Note we swap points to use the right convention in image space for shapely geometries YX
            """
            outline_geom = get_piece_outline(np.asarray(im))
            outline_area = Polygon(outline_geom).area / (300 * 36) ** 2
            outline = np.asarray(swap_points(outline_geom))
            cutline = np.asarray(swap_points(p.get_piece_physical_outline()))

            # TODO check if we have the placed version id otherwise look it up and use below
            # save the file version that we placed and the version that we just annotated

            if len(p._annotation_manager.reservations):
                s3.write(
                    f"{uri_root_pieces}/v_{s3_version_id}/annotation.{p.key}.feather",
                    p._annotation_manager.reservations,
                )

            """
            for back compatibility we should cache the outlines as files
            """
            outline_uri = f"{uri_root_pieces}/v_{s3_version_id}/outline.{p.key}.feather"
            s3.write(
                outline_uri,
                pd.DataFrame(outline, columns=["column_0", "column_1"]),
            )
            cutline_uri = f"{uri_root_pieces}/v_{s3_version_id}/cutline.{p.key}.feather"
            s3.write(
                cutline_uri,
                pd.DataFrame(cutline, columns=["column_0", "column_1"]),
            )

            record = {
                "key": p.key,
                "sku": sku,
                "piece_code": p.piece_code,
                "piece_set_size": p.piece_set_size,
                "piece_ordinal": p.piece_ordinal,
                "is_color_placement": p.is_color_placement,
                "body_version": m1.body_version,
                "annotated_image_uri": uri,
                # restore geom with df['geometry'] = LinearRing(df.apply(lambda row: np.stack([row['outline_0'],row['outline_1']]),axis=1).iloc[0].T)
                "outline_0": outline[:, 0].astype(int),
                "outline_1": outline[:, 1].astype(int),
                "outline_uri": outline_uri,
                "cutline_0": cutline[:, 0].astype(int),
                "cutline_1": cutline[:, 1].astype(int),
                "cutline_uri": cutline_uri,
                "created_at": res.utils.dates.utc_now_iso_string(),
                "updated_at": res.utils.dates.utc_now_iso_string(),
                "annotations_json": res.utils.numpy_json_dumps(
                    p._annotation_manager.reservations.to_dict("records")
                ),
                # the original placed image version - we can update these if we dont have it
                "placed_image_s3_version": p.image_s3_version,
                # this fusing type is the actually fusing type. for legacy these are mapped two types self/block_fuse and that is what the PPP parent says
                # if in future we are using this type for nesting we may have to map this type to the same binary classification
                "piece_type": p.piece_type,
                "fusing_type": fusing_info.get(p.piece_code),
                "material_code": p.material_code,
                "artwork_uri": p.artwork_uri,
                "material_piece_offset_px": p.offset_size_px,
                "piece_area": outline_area,
                "meta_one_cache_path": metadata_uri,
                "requires_block_buffer": p.is_color_placement == False
                and p.offset_size_px > 0,
            }
            record.update(file_info)
            records.append(record)

        df = pd.DataFrame(records)

        res.utils.logger.info(f"Writing metadata {metadata_uri}")
        # TODO merge options
        for t in ["fusing_type"]:
            # funny parquet things - infer types
            df[t] = df[t].astype(str)
        s3.write(metadata_uri, df)

        return df

    def _repr_svg_(self):
        return self.surface_geometries._repr_svg_()


######
###
######


def _search_table(
    table_uri,
    one_numbers=None,
    piece_filters=None,
    skus=None,
    fields=None,
    include_as_system_fields=["outline_0", "outline_1", "cutline_0", "cutline_1"],
    drop_cols=["outline_0", "outline_1", "cutline_0", "cutline_1"],
):
    """
    just convenience for now - we will move this
    """
    from res.media.images.geometry import LinearRing

    def norm_sku(s):
        return s.replace("_", " ")

    duckdb = res.connectors.load("duckdb")
    if not isinstance(skus, list):
        skus = [skus]

    fields = "*" if not fields else ",".join(fields + include_as_system_fields)
    Q = f"""SELECT {fields} FROM "{table_uri}"  """

    filters = 0
    pred = lambda: " WHERE " if filters == 0 else "AND"
    if piece_filters:
        piece_filters = f",".join([f"'{a}'" for a in piece_filters])
        Q += f" {pred()} key in ({piece_filters})"
        filters += 1
    if skus:
        skus = f",".join([f"'{norm_sku(a)}'" for a in skus])
        Q += f" {pred()} sku in ({skus})"
        filters += 1
    if one_numbers:
        one_numbers = f",".join([f"'{a}'" for a in one_numbers])
        Q += f" {pred()} one_number in ({one_numbers})"
        filters += 1

    res.utils.logger.info(Q)

    df = duckdb.execute(Q)

    try:
        df["geometry"] = df.apply(
            lambda row: LinearRing(np.stack([row["outline_0"], row["outline_1"]]).T),
            axis=1,
        )
        df["cutline"] = df.apply(
            lambda row: LinearRing(np.stack([row["cutline_0"], row["cutline_1"]]).T),
            axis=1,
        )

        if drop_cols:
            df = df.drop(columns=[d for d in drop_cols if d in df.columns], index=1)

    except Exception as ex:
        res.utils.logger.warn(ex)

    try:
        df["annotations_json"] = df["annotations_json"].map(json.loads)
    except:
        pass

    return df


def search_meta_annotations(skus, piece_filters=None, fields=None):
    """
    simple utility to load by sku and lead up to using SQL queries over our cache
    """
    root = "s3://meta-one-assets-prod/versioned-skus/api-v1/metadata/*.parquet"
    """
    a hint
    """
    if isinstance(skus, str) or len(skus) == 1:
        s = skus
        if isinstance(skus, list):
            s = s[0]
        body = s.split(" ")[0].strip().lower().replace("-", "_")
        root = (
            f"s3://meta-one-assets-prod/versioned-skus/api-v1/metadata/{body}/*.parquet"
        )

    return _search_table(
        root,
        skus=skus,
        piece_filters=piece_filters,
        fields=fields,
    )


def root_for_body(body_code, version=None):
    body = body_code.lower().replace("-", "_")
    root = f"s3://meta-one-assets-prod/versioned-skus/api-v1/metadata/{body}"
    if version:
        root += f"v{version}"
    return root


#########################################################################################################
##                           PARSING LOGIC BELOW - LOADS FROM DATABASE
#########################################################################################################


def treat_piece(
    p,
    is_placement=True,
    encode_urls=False,
    apply_soft_swaps=None,
    material_metadata=None,
):
    """
    parser logic for each piece - based on the hasura/postgres schema
    we parse geometries etc from the graph
    """
    body_piece = p.pop("body_piece")

    p["size_code"] = body_piece["key"].split("_")[-1]

    p["outline"] = extract_points(body_piece["outer_geojson"])
    outline = LinearRing(p["outline"])
    bounds_check = outline.bounds
    p["seam_guides"] = parse_seam_guides(
        body_piece["seam_guides_geojson"], bounds_check=bounds_check
    )
    p["corners"] = extract_points(
        body_piece["outer_corners_geojson"], bounds_check=bounds_check
    )

    p["notches"] = extract_notch_lines(
        body_piece["outer_notches_geojson"], outline=outline
    )

    p["annotations"] = parse_placeholders(body_piece["placeholders_geojson"])

    # Add a safety to remove the _P we sometimes have - there should be no underscores in a piece
    p["key"] = body_piece["piece_key"].split("_")[0]

    p["piece_type"] = body_piece["type"]
    p["sew_identity_symbol"] = body_piece["sew_identity_symbol"]

    p["is_color_placement"] = is_placement
    p["offset_size_px"] = DPI * p["metadata"].get("offset_size_inches") or 0

    # group symbol from piece logic
    p["image_uri"] = p["base_image_uri"]
    p["image_s3_version"] = p["base_image_s3_file_version_id"]
    p["annotated_image_uri"] = p["annotated_image_uri"]
    p["annotated_image_s3_file_version_id"] = p["annotated_image_s3_file_version_id"]

    if encode_urls:
        # if we want we can show the downloadable links
        s3 = res.connectors.load("s3")
        for s in ["image_uri", "annotated_image_uri", "artwork_uri"]:
            try:
                p[s] = s3.generate_presigned_url(p[s])
            except:
                pass

    # RH - set the create/update fields on the piece using the date coming from the body piece
    # this way we may compare the date on the geometry to the date on the piece image file
    # when validating the meta one.
    p["created_at"] = body_piece["created_at"]
    p["updated_at"] = body_piece["updated_at"]

    if apply_soft_swaps:
        existing = p["material_code"]

        p["material_code"] = apply_soft_swaps.get(existing, existing)
        if existing != p["material_code"]:
            res.utils.logger.warn(
                f"Applying swap {existing} -> {p['material_code']} for piece {p['key']}"
            )

    # this replaces the metadata loaded from the db tier -> maps material properties
    p["metadata"] = (material_metadata or {}).get(p["material_code"])

    # these image outlines are quote big and should maybe be on demand
    # p["annotated_image_outline"] = Not worth loading but we can cache it next to the PNG
    return p


def load_material_info_for_special_pieces(sp, parts_filter=["FX"]):
    """

    Special pieces can have properties that are a function of material
    >for example our swatch that fills some yardage for the width of the material
    for this we load the material properties up front so we can generate shape transformations
    color will later be filled in on the fly e.g. when we are generating the cache
    caches should probably be re-generated when material properties change but we can put something through the queue again to trigger this today
    """
    try:
        materials = []
        for p in sp:
            p = p["piece"]
            if PieceName(p["body_piece"]["piece_key"]).part in parts_filter:
                materials.append(p["material_code"])
        materials = list(set(materials))
        if materials:
            res.utils.logger.info(f"Fetching materials info {materials}")
            ##look up material properties for the materials here
            return get_material_properties(materials)
    except:
        import traceback

        res.utils.logger.warn(
            f"failed to load material properties {traceback.format_exc()}"
        )
    return {}


def _parse_meta_one_query_response(
    result, return_input_json=False, encode_urls=False, apply_soft_swaps=None
):
    style_header = sorted(
        result["meta_styles"],
        key=lambda x: parse(x["modified_at"]),
    )[-1]

    # assume placement which has not effect on physical mods but its dangerous to interpret
    is_placement = (
        style_header.get("metadata", {}).get("print_type", "Placement") == "Placement"
    )
    style = style_header.pop("style_sizes")[0]
    style_pieces = style.pop("style_size_pieces")

    metadata = {
        "style_id": style_header["id"],
        "style_size_id": style["id"],
        "is_unstable_material": style_header["metadata"]["is_unstable_material"],
        "pieces_hash_registry": style_header["pieces_hash_registry"],
    }
    style["sku"] = style["metadata"]["sku"]
    style["created_at"] = style_header["created_at"]
    style["updated_at"] = style_header["modified_at"]

    # TODO add body file updated at and the s3 file versions and change dates

    style["body_version"] = 0
    for key in ["body_code", "name"]:
        style[key] = style_header[key]
    style["metadata"] = metadata
    # only when we need it will we load it - collect all materials and piece codes to decide what we need
    # get metadata and add it to the piece
    material_metadata = load_material_info_for_special_pieces(style_pieces)
    style["printable_pieces"] = [
        treat_piece(
            dict(sp["piece"]),
            is_placement=is_placement,
            encode_urls=encode_urls,
            apply_soft_swaps=apply_soft_swaps,
            material_metadata=material_metadata,
        )
        for sp in style_pieces
    ]

    # can be inferred here - but maybe fetching the body id is a good idea so we can get the last updated at body version and other bits
    style["body_version"] = int(
        style["printable_pieces"][0]["key"].split("-")[2].replace("V", "")
    )

    sample_piece = (
        style["printable_pieces"][0] if len(style["printable_pieces"]) else None
    )
    if sample_piece:
        style["metadata"]["piece_root_uri"] = str(
            Path(sample_piece["image_uri"]).parent
        )

    if return_input_json:
        return style

    # return style
    m1 = MetaOnePrintAsset(**style)

    return m1


# add resilience since we use it in val
@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def get_meta_one(
    sku,
    return_input_json=False,
    return_raw=False,
    presigned_urls=False,
    apply_soft_swaps=None,
):
    """
    pass sku e.g. LH-3003 LN135 CREABM 4ZZLG
    """

    assert len(sku.split(" ")) == 4, "The sku must have four parts"

    sku = fix_sku(sku)

    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        queries.GET_META_ONE_BY_STYLE_SKU_AND_SIZE,
        size_code=sku.split(" ")[-1].strip(),
        metadata_sku={"sku": queries.style_sku(sku)},
    )

    if return_raw:
        return result["meta_styles"]

    return _parse_meta_one_query_response(
        result,
        return_input_json=return_input_json,
        encode_urls=presigned_urls,
        apply_soft_swaps=apply_soft_swaps,
    )


@retry(wait=wait_fixed(1), stop=stop_after_attempt(2), reraise=True)
def validate_meta_one(style_id, sku, body_version, m1=None):
    """
    Ripping off what Sirsh did in MetaOne - also checking the geometry for drift since we got
    burned by TK-3001.  This can take a few seconds - should consider moving the ACQ request placement into
    an async job when making the prod request.

    TODO:
    - combine all of the codes bellow in a single SOT so we can reference them altogether
    - are all of these reasons appended to m1_contracts_failing valid reasons to treat the style as invalid and trigger new apply color requests?
    """
    m1_contracts_failing_codes = []

    class FoundFailingContract(Exception):
        pass

    try:
        from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

        gql = ResGraphQLClient()
        # TODO: move somewhere else..
        STYLE = """
            query getStyle($id: ID!) {
                style(id: $id) {
                id
                code
                name
                isStyle3dOnboarded
                isOneReadyOverridden
                }
            }
            """
        style = gql.query(STYLE, {"id": style_id})["data"]["style"]
        is_style_3d_onboarded = style["isStyle3dOnboarded"]
        onboarded_overridden = style.get("isOneReadyOverridden", False)

        # if onboarded_overridden:
        #     return True, []

        from res.flows.meta.apply_color_request.apply_color_request import (
            get_apply_color_requests,
        )

        # Validate if we have made the BMC before
        done_requests = get_apply_color_requests(
            variables={
                "where": {
                    "styleId": {"is": style_id},
                    "applyColorFlowStatus": {"is": "Done"},
                    "styleCode": {"is": style["code"]},
                },
                "first": 1,
                "sort": [{"field": "CREATED_AT", "direction": "DESCENDING"}],
            }
        )["applyColorRequests"]

        if not (len(done_requests) > 0):
            res.utils.logger.warn(
                f"Style has not been onboarded through the apply color queue"
            )
            m1_contracts_failing_codes.append("CREATE_META_ONE_FOR_BODY_VERSION")
            raise FoundFailingContract(m1_contracts_failing_codes)

        # Validate if there is an in progress request for this style - in that case the requested version should Hold until the request is Done
        open_requests = get_apply_color_requests(
            variables={
                "first": 1,
                "where": {
                    "styleId": {"is": style_id},
                    "isOpen": {"is": True},
                    "bodyVersion": {"is": body_version},
                },
                "sort": [{"field": "CREATED_AT", "direction": "DESCENDING"}],
            }
        )["applyColorRequests"]

        if len(open_requests) > 0:
            open_acr = open_requests[0]
            open_acr_contracts_failing_codes = [
                cv["code"]
                for cv in open_acr["requestReasonsContractVariables"]
                if cv and cv["code"]
            ]
            m1_contracts_failing_codes.extend(
                open_acr_contracts_failing_codes
                or ["FOUND_IN_PROGRESS_APPLY_COLOR_REQUEST"]
            )
            # intentionally continue to see if more failing contracts are found

        if not is_style_3d_onboarded:
            # NOTE: commenting this out in the meantime - if the style has not been onboarded we need to say that the meta.one is invalid bc it means the meta.one/dxa assets are not ready.
            # if not onboarded_overridden:

            # Check if style has been onboarded: meaning, the style has been successfully gone through dxa at least once
            # NOTE: when trying to load the meta.one, it seems we dont currently get any failing contracts if the style hasn't ever been through the apply color queue - adding it here
            res.utils.logger.warn(f"Style marked as not onboarded")
            m1_contracts_failing_codes.append("CREATE_META_ONE_FOR_BODY_VERSION")
            raise FoundFailingContract(m1_contracts_failing_codes)

        # NOTE: SKU doesn't contain hyphens...
        body_code_without_hyphen = sku.split(" ")[0]
        body_code = (
            body_code_without_hyphen[:2] + "-" + body_code_without_hyphen[2:]
            if "-" not in body_code_without_hyphen
            else body_code_without_hyphen
        )
        lower_body_code = body_code.lower().replace("-", "_")

        try:
            m1 = m1 or get_meta_one(sku)
        except:
            # we should not get to here if the style is not ready
            m1_contracts_failing_codes.append("STYLE_PIECES_SAVED")
            raise FoundFailingContract(m1_contracts_failing_codes)

        if body_version and body_version > m1.body_version:
            res.utils.logger.warn(
                f"Loaded a meta one with body version {m1.body_version} but required {body_version}"
            )
            m1_contracts_failing_codes.append("BODY_VERSION_CHANGED")
            raise FoundFailingContract(m1_contracts_failing_codes)

        # filter out contracts that are considered warnings at style level
        m1_enforced_contracts_failing = (
            list(
                set(m1.contracts_failing).difference(
                    ["3D_MODEL_FILES", "NO_CLOSE_TOGETHER_POINTS"]
                )
            )
            if len(m1.contracts_failing) > 0
            else []
        )
        if len(m1_enforced_contracts_failing) > 0:
            res.utils.logger.warn(
                f"Meta one {sku} is failing contracts {m1.contracts_failing}"
            )
            m1_contracts_failing_codes.extend(m1.contracts_failing)
            raise FoundFailingContract(m1_contracts_failing_codes)
        for p in m1:
            if p.piece_type != "unknown":
                if len(PieceName(p.key).validate()) > 0:
                    res.utils.logger.warn(f"Piece {p.key} failed validation")
                    m1_contracts_failing_codes.append(
                        "PIECE_FAILED_VALIDATION"  # a better name?
                    )  # is this a valid reason to say its invalid and create an apply color request?
                    raise FoundFailingContract(m1_contracts_failing_codes)
        s3 = res.connectors.load("s3")
        if not all(s3.exists(p.image_uri) for p in m1):
            res.utils.logger.warn(f"Meta one {sku} has missing pieces on s3")
            m1_contracts_failing_codes.append("PIECE_NOT_FOUND")
            raise FoundFailingContract(m1_contracts_failing_codes)
        body_3d_file_uri = f"s3://meta-one-assets-prod/bodies/3d_body_files/{lower_body_code}/v{body_version}/{body_code}-V{body_version}-3D_BODY.bw"

        # remove this for now as do not understand what it blocks except for a super weird case of something getting this far without a body e.g. how could we load a meta one at all for the correct version?
        # if not s3.exists(body_3d_file_uri):
        #     res.utils.logger.warn(f"Body File for {body_code} missing on s3")
        #     m1_contracts_failing_codes.append("MISSING_3D_BODY_FILE")
        #     raise FoundFailingContract(m1_contracts_failing_codes)

        if s3.exists(body_3d_file_uri):
            # check the style is older than the piece image.
            for piece in m1:
                body_3d_file_date = next(
                    res.connectors.load("s3").ls_info(body_3d_file_uri)
                )["last_modified"].replace(tzinfo=pytz.UTC)
                # Commenting for now and using body_3d_file_date instead - the piece.updated_at is updated everytime the body is refreshed, which sometimes doesn't mean that the body actually changed.
                # piece_geom_date = datetime.strptime(
                #     piece.updated_at, "%Y-%m-%dT%H:%M:%S.%f%z"
                # )  # some meta ones have 5 digits of milliseconds and fuck the iso format up.
                piece_image_date = next(
                    res.connectors.load("s3").ls_info(piece.image_uri)
                )["last_modified"].replace(tzinfo=pytz.UTC)
                # check timestamps with a lot of slop - sometimes theres a few minutes delay between updating the image and the geometry which
                # i assume is part of the process.  trying to avoid false positives.
                # if piece_geom_date > piece_image_date + timedelta(days=1):
                #     res.utils.logger.info(
                #         f"Meta one {sku} piece {piece.key} failed validation since the image was created {piece_image_date.isoformat()} and the geometry was updated {piece_geom_date.isoformat()}"
                #     )
                #     return False

                if body_3d_file_date > piece_image_date:
                    res.utils.logger.info(
                        f"Meta one {sku} failed validation since the style is outdated - the body file was updated on {body_3d_file_date.isoformat()} and the style images were updated on {piece_image_date.isoformat()} "
                    )
                    m1_contracts_failing_codes.append("BODY_FILE_CHANGED")
                    raise FoundFailingContract(m1_contracts_failing_codes)
    except FoundFailingContract as e:
        pass
    except Exception as e:
        import traceback

        print(e)
        # Can't resolve if the m1 is valid or not - should not proceed (production request will be flagged)
        # TODO: Ping slack
        raise Exception(f"UNKNOWN_ERROR: {e} {traceback.format_exc()}")

    is_meta_one_valid = len(m1_contracts_failing_codes) == 0

    return is_meta_one_valid, list(set(m1_contracts_failing_codes))


def get_meta_one_pieces(keys, return_input_json=False, return_raw=False):
    """
    pass
    """
    hasura = res.connectors.load("hasura")
    result = hasura.execute_with_kwargs(
        queries.GET_META_ONE_PIECES_BY_KEYS,
        piece_keys=keys,
    )

    if return_raw:
        return result["meta_styles"]

    return _parse_meta_one_query_response(result, return_input_json=return_input_json)


def __util_purge_cache_folder():
    """
    if we change our mind about anything and want to clear the cache
    """
    s3 = res.connectors.load("s3")
    from tqdm import tqdm

    uri = "s3://meta-one-assets-prod/versioned-skus/"
    res.utils.logger.info(f"Purging {uri}")
    for f in tqdm(s3.ls(uri)):
        res.utils.logger.info(f"Purging {f}")
        s3._delete_object(f)


"""
snippet

#clear out the cache data - in the long term better to just bump the active version to api-v2 and start fresh e.g for schema changes etc.
counter = 0
for file in s3.ls('s3://meta-one-assets-prod/versioned-skus/api-v1'):
    print(file)
    s3._delete_object(file)
    counter += 1
counter

"""
