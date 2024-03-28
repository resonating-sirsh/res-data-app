from distutils.log import warn
import os
from glob import glob
from pathlib import Path
import ezdxf
from sklearn.neighbors import BallTree
import numpy as np
import pandas as pd
import res
import re
import tempfile
from res.media.images.geometry import *
from res.media.images.outlines import draw_outline, mask_from_outline
from res.media.images.text import get_one_label
from res.utils import dataframes, logger, dates
from shapely.geometry import (
    LinearRing,
    LineString,
    MultiLineString,
    MultiPoint,
    Point,
    Polygon,
)
from shapely.ops import unary_union
import zipfile
import json
from shapely.ops import orient, unary_union
from shapely.affinity import rotate, scale
from datetime import datetime
from .vstitcher_pieces import VstitcherPieces
import itertools
from functools import partial
from sklearn.neighbors import KDTree


DPI_SETTING = 300

cols = [
    "key",
    "SIZE",
    "CATEGORY",
    "Fabric",
    "geometry",
    "polylines",
    "entity_layer",
    "entity_layer_name",
    "piece_name",
    # derived
    "quantity",
    "is_mirrored",
    "mirror_quantity",
    "type",
    "fold_line",
    "piece_code",
]
column_selector = {c: c for c in cols}
for c in ["CATEGORY", "SIZE", "Fabric"]:
    column_selector[c] = column_selector[c].lower()
column_selector["entity_layer_name"] = "layer_name"
column_selector["entity_layer"] = "layer"


BASE_INHERITED_ATTRIBUTES = [
    "quantity",
    "is_mirrored",
    "mirror_quantity",
    "Fabric",
    "CATEGORY",
]
OUTLINE_KEYPOINT_LAYER = 1
INTERNAL_LINE_KEYPOINT_LAYER = 8
NOTCH_LAYER = 4
OUTLINE_LAYER = 84
INTERNAL_LINE_LAYER = 85
MIRROR_LINE_LAYER = 6
DEFAULT_DPI = 300
DEFAULT_NOTCH_BUFFER = 0.125
DEFAULT_BOUNDARY_PADDING = 0.01
OUTLINE_300_DPI = "outline_300_dpi"

SMALL_SEAM_SIZE = DPI_SETTING * 0.25
HALF_SMALL_SEAM_SIZE = SMALL_SEAM_SIZE / 2

PRINTABLE_PIECE_TYPES = ["self", "block_fuse", "lining", "combo"]

DPMM = 1 / 0.084667

MM_TO_INCHES = 0.393701
MM_PER_INCH = 25.4


def upload_plt_file_urls_to_body(path):
    """
    this is a helper file that does not belong in the DXF but adding it for now
    when we export the meta one we can create stampers from dxf and upload to airtable

                body_code_for_path = (
                    self.code.replace("-", "_").lower() + "/" + body_version
                )

      f"s3://meta-one-assets-prod/bodies/cut/{body_code_for_path}"

    """

    def get_latest_body_version(tab, body):
        """
        for now using latest body version in requests but will probably change to using a new table
        """
        from res.connectors.airtable import AirtableConnector

        pred = AirtableConnector.make_key_lookup_predicate(body, "Body Number")
        tab = tab.to_dataframe(
            fields=["Brand", "Body Number", "Name"],
            filters=pred,
        )
        res.utils.logger.debug(f"filter {pred}")
        tab["version"] = tab["Name"].map(
            lambda x: x.split("-")[-1].replace("V", "") if x else None
        )
        return tab.sort_values("version").iloc[0]["record_id"]

    try:
        body = path.split("/")[5].upper().replace("_", "-")
        version = path.split("/")[6].upper()
        body_version = f"{body}-{version}"

        res.utils.logger.info(
            f"Upserting .plt files from {path} to airtable attachments for body request {body_version} or latest version"
        )

        s3 = res.connectors.load("s3")
        airtable = res.connectors.load("airtable")
        tab = airtable["appa7Sw0ML47cA8D1"]["tblrq1XfkbuElPh0l"]

        # look up record id
        record_id = get_latest_body_version(tab, body)

        files = [f for f in s3.ls(path) if ".plt" in f]

        tab.update_record(
            {
                "record_id": record_id,
                "Meta.ONE Fusing and Stamper files": [
                    {"url": s3.generate_presigned_url(filename)} for filename in files
                ],
            }
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"Unable to update airtable with files - check object names etc."
        )


def _make_quantity(g):
    """
    starting a lib of adapters to merge formats
    """
    Q = g[["key", "PIECE NAME", "QUANTITY", "ANNOTATION", "CATEGORY"]].drop_duplicates()
    Q = pd.merge(
        Q,
        Q.groupby("CATEGORY").count()["key"].reset_index().rename(columns={"key": "c"}),
        on=["CATEGORY"],
    )
    try:
        Q["E"] = Q["ANNOTATION"].map(lambda q: q.split(" ")[-1]).astype(int)
        Q["e"] = (Q["E"] / Q["c"]).astype(int)
    except:
        Q["e"] = 1

    # spoofing 1 or mirror
    Q["Quantity"] = Q["e"].map(lambda x: f"{x},0" if x == 1 else f"1,1")

    mapping = dict(Q[["key", "Quantity"]].values)

    g["Quantity"] = g["key"].map(lambda k: mapping[k])

    return g


def _get_files(path):
    s3 = res.connectors.load("s3")
    dxf_files = []
    if "s3://" in path:
        if ".dxf" in path.lower():
            dxf_files = [path]
        else:
            dxf_files = [
                f for f in list(s3.ls(path)) if Path(f).suffix.lower() == ".dxf" in f
            ]
        odir = s3.object_dir(path)
        rul_files = [f for f in list(s3.ls(odir)) if Path(f).suffix == ".rul" in f]
    else:
        odir = f"{Path(path)}/*"
        # assume its a file first
        dxf_files = list(glob((f"{odir}/*.dxf")))
        rul_files = []
        # if we have a dxf file we need to pop the level and look for rul riles
        if Path(path).suffix == ".dxf":
            dxf_files = [path]
            odir = f"{Path(path).parent}/*"

        if len(dxf_files) == 1:
            # if we have a dxf file we need to match on it
            rul_files = [
                f
                for f in glob(f"{odir}.rul")
                if Path(f).stem == Path(dxf_files[0]).stem
            ]

    # restrict
    if ".dxf" in path:
        # if the name specified use the matching rule
        rul_files = [
            f for f in rul_files if Path(f).stem.lower() == Path(path).stem.lower()
        ]

    return dxf_files, rul_files


class PieceGrading:
    """
    A utility to use the rul file in grading all the geometries of the piece
    this works by determining which grade points are closest to any other geometry
    by taking subsets of a geometry, their actual or interpolated points can be graded into a new geometry
    for example outlines and sew lines can be graded into segments that form new outlines or sew lines
    notches can be graded in the same way to create a new cloud as can drill holes and other geometries
    """

    def __init__(
        self, rules_df, key, sizes, r=0.01, layers=None, drop_duplicates=False
    ):
        self._key = key
        self.layers = layers
        self._rules_df = rules_df[rules_df["key"] == key].copy()
        # drop redundant points
        if drop_duplicates:
            self._rules_df["pt"] = self._rules_df["geometry"].map(
                lambda x: tuple(x.coords)
            )
            self._rules_df = (
                self._rules_df.drop_duplicates(subset=["key", "pt"])
                .drop("pt", 1)
                .reset_index(drop=True)
            )

        # must be string
        self.sizes = [str(s) for s in sizes]
        self._rules_geom = unary_union(self._rules_df["geometry"])
        self.tree = BallTree(np.array([[p.x, p.y] for p in self._rules_df["geometry"]]))
        self.r = r

    @staticmethod
    def close_sew_geometry(sg):
        """
        the sew geometries are sometimes not closed as complete outlines but its convenient if we can close then
        this could be graded in the same way as the outline but the reality is it should be an offset function with fixed seam allowance

        the scale factor is small - and should be increased for DPI
        this is used to close a sew line to a circuit if it has an internal cut line
        """
        try:
            p1 = sg.interpolate(0)
            p2 = sg.interpolate(1, normalized=True)
            sg2 = unary_union([LineString([p1, p2]), sg])
            g = list(polygonize(sg2))[0].exterior
        except:
            res.utils.logger.warn(f"Failed to closed the sew geometry")
            g = sg
        return g

    def interpolate_points_for_geom(self, g, samples=200):
        ranges = [(i + 1) / samples for i in range(samples)]
        pts = [g.interpolate(i, normalized=True) for i in ranges]
        return unary_union(pts)

    def select_rules_near_geom(self, g):
        # we could add a tolerance and sample down with the tree
        selected_graded_points = self._rules_geom.intersection(g)
        # ensure iterable
        if selected_graded_points.type == "Point":
            selected_graded_points = [selected_graded_points]
        # rules should be grade points
        pts = [[p.x, p.y] for p in list(selected_graded_points)]
        if len(pts):
            selected = np.concatenate(self.tree.query_radius(pts, r=self.r))
            return self._rules_df.iloc[selected]
        return None

    def get_graded_piece(self, size, dxf_compact_layers=None):
        """
        Grade piece runs a specific operation for each type of geometry which for now needs to be graded differently but it might be lack of understanding about how grading works
        """
        dxf_compact_layers = dxf_compact_layers or self.layers
        dxf_layers_selection = dxf_compact_layers[
            dxf_compact_layers["key"] == self._key
        ]

        data = DxfFile.select_geometries_from_layers(
            dxf_layers_selection, key=self._key, size=size
        )
        outline = data["outline"]
        # i believe the same fold line applies to all layers for the piece - if not we can do the annoying thing of fetching one per layer
        fold_line = data["fold_line"]
        grain_line = data["grain_line"]
        sew_line = data["sew_line"]
        # internal cutouts(11/86), internal_lines(8), curve_points(3), turn_points(2), drill holes (13), mirror line (6)
        internal_lines = data["internal_lines"]
        notches = data["notches"]

        # we do this to make the sew line a circuit to undo convention of not always closing it
        res.utils.logger.debug(f"grading {size=}, key={self._key}...")
        graded_outline = self.grade_outline_for_size(g=outline, size=size)
        # in some sense the seam allowance is fixed here so we can take other routes
        graded_sew_line = (
            self.grade_outline_for_size(g=sew_line, size=size) if sew_line else None
        )
        # notches are not always present - we pass the outline in as a circuit to help with grading notches
        graded_notches = (
            self.get_notch_grading_for_size(g=outline, n=notches, size=size)
            if notches
            else None
        )
        # here we merge all the internal lines and shapes into one layer that is graded together - this will not work in all cases (testing)
        graded_internal_lines = (
            self.grade_internal_lines_and_points(il=internal_lines, size=size)
            if internal_lines
            else None
        )

        # unfold = lambda g: unfold_on_line(g, fold_line) if fold_line else g

        return {
            "key": self._key,
            "size": size,
            "outline": graded_outline,
            "sew_line": graded_sew_line,
            "internal_lines": graded_internal_lines,
            "notches": graded_notches,
            "fold_line": fold_line,
            "grain_line": grain_line,
        }

    def get_grading_data_for_piece_outline(self, g, size=None):
        """
        Each geometry should be segmented into something that is either a point and lies on one grade point or is a segment that lies on two
        for linear lines and some internal points this makes sense but for notches not so much - but if we construct any circuit from the notches we can do the assignation
        Creates data for use in grading a geometry in the DXF input data

        """

        def make_segment(row):
            maxo = row["geometry"]
            mino = row["prev_geometry"]
            maxo = g.project(maxo)
            mino = g.project(mino)

            # g is either the direct linear run or line or it could be a surrogate
            sg = substring(g, maxo, mino)
            g_alt = g - sg
            # this is a little bit of magic for the passes-go problem
            # a ring passing the origin will split the segment into two, and our logic will select the complement
            # but if that is bigger then we can assume __almost__ surely that we want the other thing
            if g_alt.length < sg.length:
                sg = g_alt

            return sg

        a = self.select_rules_near_geom(g)
        if a is None or len(a) == 0:
            return None

        fn = first_corner_projection_offset_function(g, a.iloc[0]["geometry"])
        a["circuit_order"] = a["geometry"].map(fn)
        a = a.sort_values("circuit_order")
        a["prev_geometry"] = np.roll(a["geometry"], 1)
        a["prev"] = a["prev_geometry"].map(fn)
        a["next"] = g.length
        a.loc[a["circuit_order"] != 0, "next"] = a["circuit_order"]
        for s in self.sizes:
            # the tuple is from the prev to the next - observe in the dataframe that they are chained
            a[s] = np.roll(a[s], 1) + a[s]

        a["base_segment"] = a.apply(make_segment, axis=1)

        return a

    @staticmethod
    def grade_point(pt, g1, g2, grade_point_deltas, ref_point=None, debug=False):
        """
        pt: a point to grade
        g1: the first grade point nearby
        g2: the second grade point nearby
        grade_point_deltas: the deltas at those grade points
        """
        # determine the inverse distances from grade points to point
        # if we have a reference point we want to use as a surrogate to choose are delta e.g. a notch offset from a graded segment
        g1 = g1 or g2
        g2 = g2 or g1
        ref_point = ref_point or pt
        D1 = grade_point_deltas[:2]
        D2 = grade_point_deltas[2:]
        d1 = ref_point.distance(g1)
        d2 = ref_point.distance(g2)

        if debug:
            res.utils.logger.info(f"{ref_point=} {d1=} {d2=} {ref_point=}")
        # if the point is sufficiently close to the grade point just use it as is e.g. on button holes
        # if d1 < 0.001:
        #     return Point(pt.x + D1[0], pt.y + D1[1])
        # if d2 < 0.001:
        #     return Point(pt.x + D2[0], pt.y + D2[1])

        # else weight the contributions
        id1 = d1 / (d1 + d2) if (d1 + d2) > 0 else 1
        id2 = d2 / (d1 + d2) if (d1 + d2) > 0 else 1
        # these are the pair of points that come from the grading data - they are paired e.g. each end of a string segment we want to grade
        # they could be the same point if the grading applies to a single point

        # compute the delta
        delta = (id2 * np.array(D2)) + (id1 * np.array(D1))
        # add it to the incoming point
        return Point(pt.x + delta[0], pt.y + delta[1])

    def _grade_segment(self, row, size, samples=50):
        # graded points
        grade_point_deltas = row[
            size
        ]  # this order is correct for working with grade point
        p2, p1 = row["prev_geometry"], row["geometry"]

        # get the geometry to grade
        part = row["base_segment"]
        try:
            sample_points = [Point(p) for p in part.coords]
        except:
            sample_points = self.interpolate_points_for_geom(part, samples=samples)

        # grade the sample points in the geometry with ref to the nearby grade points and their deltas
        graded_points = [
            PieceGrading.grade_point(sp, p1, p2, grade_point_deltas)
            for sp in sample_points
        ]

        try:
            # return type(part)(graded_points)
            # is this segment always
            return (
                LineString(graded_points)
                if len(graded_points) > 1
                else Point(graded_points)
            )

        except:
            raise Exception(
                f"Unable to create type {type(part)} from the graded points - tried to coerce to a "
            )

    def grade_outline_for_size(
        self, g, size, samples=50, debug=False, allow_close_small_gaps=True, **kwargs
    ):
        """
        calls the underlying grading data  to grade a specific geometry by size
        each size in the rul table provides offsets - you can see the underlying data
        once we have these data setup, we are choosing just one size and applying the grading rules to the geometry

        the surrogate geometry is used if we need to grade respect to some other object e.g. notches that move with the surface
        """
        size = str(size)
        gf = self.get_grading_data_for_piece_outline(g, **kwargs)

        fn = partial(self._grade_segment, size=size, samples=samples)
        gf["graded_segments"] = gf.apply(fn, axis=1)
        if debug:
            return gf

        # todo coerce type
        h = unary_union(gf["graded_segments"])
        if g.type == "LinearRing":
            # i thought making a linear ring of the lines should be enough
            # close small gabs

            Ps = list(polygonize(h))
            if len(Ps) > 0:
                h = Ps[0].exterior
            else:
                if allow_close_small_gaps:
                    # grading is assumed to be in inches as per rul file
                    try:
                        h = coerce_linear_ring_fill_small_gaps(h)
                    except:
                        res.utils.logger.warn(
                            f"Failed to do last resort filling gaps for expected circuit {h}"
                        )
        return h

    @staticmethod
    def assign_notches(row, g, N, tolerance=0.01):
        """
        how do we know what segment to grade a notch with?
        here we project a notch onto the geometry and see what segment it falls on
        that segment is graded based on its grade delta interval
        """
        s = row["base_segment"]
        ns = list(N)
        projections = [g.interpolate(g.project(n)) for n in ns]
        return unary_union(
            [
                n
                for i, n in enumerate(ns)
                if projections[i].intersects(s.buffer(tolerance))
            ]
        )

    @staticmethod
    def grade_notches_for_size(row, size):
        """
        in this case, the delta is on the weighted distance between the projected points and the grade points
        """
        pts = row["base_notches"]
        ns = list(pts) if pts.type != "Point" else [pts]
        g = row["base_segment"]
        g1, g2 = row["geometry"], row["prev_geometry"]
        projections = [g.interpolate(g.project(n)) for n in ns]
        deltas = row[size]

        return unary_union(
            [
                PieceGrading.grade_point(n, g1, g2, deltas, projections[i])
                for i, n in enumerate(ns)
            ]
        )

    def get_notch_grading_for_size(self, g, n, size, debug=False):
        """
        the outline geometry g is passed in with the notches
        """

        pa = self.get_grading_data_for_piece_outline(g, size)
        fn = partial(self.assign_notches, g=g, N=n)
        gfn = partial(self.grade_notches_for_size, size=size)
        pa["base_notches"] = pa.apply(fn, axis=1)
        pa["graded_notches"] = pa.apply(gfn, axis=1)
        if debug:
            return pa
        return unary_union(pa["graded_notches"])

    def grade_internal_lines_and_points(self, il, size, samples=50):
        """
        this is designed to match grade points to separate lines and points that are not circuits
        by looping through each sub geometry, we match it to connected grade points
        we then create a grade operation for the base sub geometry using nearby grade points
        """
        geometries = []

        # this is a safety for things that are not iterable but its not done perfectly right yet
        for geom in il.geoms if hasattr(il, "geoms") else [il]:
            """
            similar strategy as we do for outlines but we split geometries into sections e.g. separate line strings and points
            """
            rgs = self.select_rules_near_geom(geom).reset_index(drop=True)
            if len(rgs) == 0:
                continue

            if geom.type != "Point":
                for s in self.sizes:
                    # the tuple is from the prev to the next - observe in the dataframe that they are chained
                    rgs[s] = rgs[s].shift(1) + rgs[s]
                rgs["projection"] = rgs["geometry"].map(lambda x: geom.project(x))
                rgs = rgs.sort_values("projection")
                rgs["next_geometry"] = rgs["geometry"]  # alias
                rgs["prev_projection"] = rgs["projection"].shift(1)
                rgs["prev_geometry"] = rgs["geometry"].shift(1)

                # does this make sense if there is only one grade point there will be no next?
                rgs = rgs.dropna(subset=[self.sizes[0]])

                # print("****")
                # print(rgs.to_dict("records"))
                # print("****")

                ###############################################

                rgs["base_segment"] = geom
                # unless this is a circuit im not sure about this - might need a counter case
                # rgs["base_segment"] = rgs.apply(
                #     lambda row: substring(
                #         geom, row["projection"], row["next_projection"]
                #     ),
                #     axis=1,
                # )
                # because we have created a dataframe with the right structure to grade a segment we can call the same function as for outlines

            else:
                if len(rgs) != 1:
                    res.utils.logger.warn(
                        f"Type grade-ee a point but we have matched {len(rgs)} grade point rules. using the first only."
                    )
                rule = rgs.iloc[0][size]  # use the matched rule on the point
                geometries.append(Point(geom.x + rule[0], geom.y + rule[1]))
                continue

            # for line segments do this...the line samples should not apply for simple geometries inside grade segment
            fn = partial(self._grade_segment, size=size, samples=samples)
            RG = rgs.apply(fn, axis=1)
            try:
                rgs["graded_segments"] = RG
            except:
                res.utils.logger.warn(f"{RG}, {rgs}")
                raise
            # we merge all the graded segments into a multi geom
            geometries.append(unary_union(rgs["graded_segments"]))

        # return the collection of all internal objects - it assumed for now they are marking and we dont need them as separate entities
        return unary_union(geometries)


class vstitcher_piece_info:
    """
    vstitiher provides extended piece information and here we parse it
    """

    def __init__(self, file_or_data):
        s3 = res.connectors.load("s3")
        readit = lambda path: (
            res.utils.read(path) if "s3://" not in path else s3.read(path)
        )
        self._data = (
            file_or_data if not isinstance(file_or_data, str) else readit(file_or_data)
        )
        self._data = {item["key"]: item for item in self._data}
        self._df = self._as_dataframe()

    def __iter__(self):
        for item in self._data.values():
            yield item

    def __repr__(self):
        return str(self._data)

    def __getitem__(self, key):
        return dict(self._df.loc[key])

    def plot_seam_allowances_for_key(self, key, **kwargs):
        """
        quickly plot the seam allowances we have - return the dataframe
        """
        from res.utils.dataframes import expand_column
        from geopandas import GeoDataFrame

        to_linestring = lambda x: (
            LineString([[item["y"], item["x"]] for item in x])
            if len(x) > 1
            else Point()
        )

        buffer = kwargs.get("buffer", 0.2)

        df = pd.DataFrame(self[key]["edges"])
        df["raw_properties"] = df["raw_properties"].map(json.loads)
        df["geometry"] = df["points"].map(to_linestring)
        df = expand_column(df, "raw_properties")
        df["geometry"] = df.apply(
            lambda row: row["geometry"].buffer(row["seam_allowance"] * buffer), axis=1
        )
        GeoDataFrame(df).plot(figsize=(10, 10))
        # print(list(df["raw_properties_seam_allowance"].unique()))
        return df

    def _as_dataframe(self, plot=False):
        df = pd.DataFrame(self).set_index("key")
        to_linear_ring = lambda x: LinearRing([[item["y"], item["x"]] for item in x])
        to_linestring = lambda x: (
            LineString([[item["y"], item["x"]] for item in x])
            if len(x) > 1
            else Point()
        )

        df["geometry"] = df["points"].map(to_linear_ring)
        df["seam_allowances"] = df["edges"].map(
            lambda sa: [item["seam_allowance"] for item in sa]
        )

        # when unary_union joins lines it will merge overlapping points, it could use the
        # last point of the first line over the first point of the next line or vice versa,
        # worst case is a 2 point line has both ends overwritten and the seam_allowance (z) is
        # lost. This very explicity sets the seam_allowance for each line...
        def unary_union_preserve_z(row):
            key = row["bzw_key"]
            edges = row["edges"]

            multilines = unary_union(
                [to_linestring([i for i in item["points"]]) for item in edges]
            )

            if len(multilines.geoms) > len(edges):
                logger.warn(
                    f"{key} has more geoms after unary_union, attempting to fix"
                )

                # simplify doesn't work in this case (TH-6005-V2-DRSFTPNL-S), there's some
                # overlap that creates tiny geometries, try to filter these out
                multilines = MultiLineString(
                    [l for l in multilines.geoms if l.length > 0.01]
                )

            if len(multilines.geoms) != len(edges):
                raise Exception(f"{key} unary_union failed, can't match for sas")

            lines = []
            for i, line in enumerate(multilines):
                seam_allowance = edges[i]["seam_allowance"]
                coords = [
                    (coord[0], coord[1], seam_allowance) for coord in line.coords[:]
                ]
                lines.append(LineString(coords))

            return MultiLineString(lines)

        # unary union after labelling the edges with seam allowances
        # we do this because we need to scale a single shape to match other outlines
        # but we also need to preserve the SAs on the segments.
        # Unary union could change the number of segments so we need to keep the SAs tagged here

        df["edge_geometry"] = df.apply(unary_union_preserve_z, axis=1)

        # we flip the y,x above and now mirror on X - is the correct transformation for all scan.json
        df["edge_geometry"] = df["edge_geometry"].map(mirror_on_y_axis)

        if plot:
            from geopandas import GeoDataFrame

            res.utils.logger.info(f"Using the x mirrored geometry to plot")
            GeoDataFrame(df, geometry="edge_geometry").plot(figsize=(20, 20))
        return df

    def get_seam_allowances_near_shape(
        self, key, edges, plot_shapes=False, scale_to_pixels=False
    ):
        """
        although the shapes passed in should be identical and to the right scale
        this function tries to be resilient to this by scaling the shapes to size and then finding the nearest corresponding edges and properties

        edges: a list of line geometries, each expecting its own seam allowance. should not be a single linear ring or polygon
        ...
        """
        e1 = shift_geometry_to_origin(edges)

        e2 = self[key]["edge_geometry"]
        l1 = geom_len(e2)

        e2 = vstitcher_piece_info.scale_to(e2, e1)
        l2 = geom_len(e2)
        assert l1 == l2, "geom length changed when scaling"

        if plot_shapes:
            from geopandas import GeoDataFrame

            ax = GeoDataFrame(
                {
                    "title": (
                        ["vs edges"] * geom_len(e2) + ["dxf edges"] * geom_len(e1)
                    ),
                    "geometry": geom_list(e2) + geom_list(e1),
                },
            ).plot(
                column="title",
                legend=True,
            )
            ax.set_title(key)

        scale_by = 1
        if scale_to_pixels:
            # its only if the DXF is in mm units that we should do the second term but we have no seam allowances when this is not the case so ignoring for now
            scale_by *= DPI_SETTING * MM_TO_INCHES
        return vstitcher_piece_info.get_corresponding_edge_seam_allowances(
            e1, e2, scale_by
        )

        return e2

    def plot_seam_allowances_for_shape(
        self, key, edges, scale_to_pixels=False, scaling_buffer=5
    ):
        from geopandas import GeoDataFrame

        E = self.get_seam_allowances_near_shape(
            key, edges, scale_to_pixels=scale_to_pixels
        )

        df = GeoDataFrame([{"geometry": e, "sa": E[i]} for i, e in enumerate(edges)])
        df["geometry"] = df.apply(
            lambda row: row["geometry"].buffer(row["sa"] * scaling_buffer), axis=1
        )
        df.plot(figsize=(10, 10))
        return df

    def scale_to(b, c):
        """
        scale one shape to be near there other
        """
        w1, h1 = b.bounds[2] - b.bounds[0], b.bounds[3] - b.bounds[1]
        w2, h2 = c.bounds[2] - c.bounds[0], c.bounds[3] - c.bounds[1]
        return shift_geometry_to_origin(scale(b, xfact=w2 / w1, yfact=h2 / h1))

    def get_corresponding_edge_seam_allowances(edge_list, vs_edge_list, scaler):
        """
        take a reference point on the edge
        then look at the VS edges and find the one closest to me
        Take its properties
        Show the mapping between my edge and their edge
        for each eid, we want to store properties
        """

        try:
            sas = []
            edges = geom_list(vs_edge_list)

            for e in edge_list:
                # center of my edge
                pt = e.interpolate(0.5, normalized=True)

                # find distances to all the other vs edges
                distances = [
                    pt.distance(k.interpolate(0.5, normalized=True)) for k in edges
                ]
                # get the edge
                ref_edge = edges[np.argmin(distances)]
                # interpolate a small distance onto the edge
                ref_point = ref_edge.interpolate(ref_edge.length * 0.2)

                # get the edge at the closest distance and return the attributes
                d = edges[np.argmin(distances)].interpolate(0).z * scaler
                sas.append(d)
            return sas
        except:
            res.utils.logger.debug(f"Failed on {edge_list}, {vs_edge_list} ")
            raise


class DxfFile:
    """
    DXF files are normally stored on box and sometimes S3
    Here is a complete flow to get and open a DXF file

    Example:
    import res
        s3 = res.connectors.load('s3')
        box = res.connectors.load('box')
        box.copy_to_s3(645461957728, 's3://onelearn-platform/test/test.dxf')
        dxf = s3.read('s3://onelearn-platform/test/test.dxf')

    The DXF files are opened as dataframes with attributes and geometries per layers
    The idea is we learn to create annotated geometric information that can be used for intelligent markers

    Convention notes:

        We will run a job that stores ONE artifacts in the S3 datalake by body code e.g.

        s3://meta-one-assets-prod/bodies/ll_2003/pattern_files/body_ll_2003_v1_pattern.dxf

    """

    @staticmethod
    def size_dxf_if_supported(path, size):
        s3 = res.connectors.load("s3")
        new_root = path.rstrip("/") + "_by_size"
        res.utils.logger.debug(f"searching {new_root}")
        sizes = set([str(Path(f).parent).split("/")[-1] for f in list(s3.ls(new_root))])
        if sizes:
            size = DxfFile.map_res_size_to_gerber_size_from_size_list(sizes, size)
            # this is done because the petites dont include the data we want so then should be ignored and we should get everything from the standard
            if "P-" in size:
                res.utils.logger.debug("removing the petite part")
                size = size.replace("P-", "").replace("P_", "")
                res.utils.logger.debug(
                    f"Delegating petite to {size} in the set of dxfs"
                )

            res.utils.logger.debug(f"found size {size} in the set of dxfs")
            dxf_path = f"{new_root}/{size}/"
            dxf_path = [f for f in s3.ls(dxf_path) if ".dxf" in f][-1]
            return dxf_path
        return path

    @staticmethod
    def split_size(s):
        """Get sizeless key (e.g. KT-6041-V7-DRSNKCLR-X) and size"""
        return s.split("_", 1)

    @staticmethod
    def remove_size(s):
        return DxfFile.split_size(s)[0]

    def get_layer_geometries_for_piece_key(self, key, size=None):
        return DxfFile.select_geometries_from_layers(
            self.compact_layers, key=key, size=size or self.sample_size
        )

    @staticmethod
    def select_geometries_from_layers(dxf_compact_layers, key, size):
        dxf_layers_selection = dxf_compact_layers[dxf_compact_layers["key"] == key]

        outline = dxf_layers_selection[dxf_layers_selection["layer"] == 1][
            "geometry"
        ].iloc[0]
        # i believe the same fold line applies to all layers for the piece - if not we can do the annoying thing of fetching one per layer
        fold_line = dxf_layers_selection[dxf_layers_selection["layer"] == 1][
            "fold_line"
        ].iloc[0]

        fold_line = None if pd.isnull(fold_line) else LineString(fold_line)

        grain_line = dxf_layers_selection[dxf_layers_selection["layer"] == 7]
        grain_line = grain_line["geometry"].iloc[0] if len(grain_line) else None
        sew_line = dxf_layers_selection[dxf_layers_selection["layer"] == 87]
        if len(sew_line) == 0:
            # try the sew line instead of sew validation curve
            sew_line = dxf_layers_selection[dxf_layers_selection["layer"] == 14]
        # we close the circuit for convention
        sew_line = (
            PieceGrading.close_sew_geometry(sew_line["geometry"].iloc[0])
            if len(sew_line)
            else None
        )

        # internal cutouts(11/86), internal_lines(8), curve_points(3), turn_points(2), drill holes (13), mirror line (6)
        il_select = dxf_layers_selection[
            dxf_layers_selection["layer"].isin([8, 86, 13])
        ]
        internal_lines = unary_union(il_select["geometry"]) if len(il_select) else None

        notches = dxf_layers_selection[dxf_layers_selection["layer"] == 4]

        return {
            "key": key,
            "size": size,
            "outline": outline,
            "sew_line": sew_line,
            "internal_lines": internal_lines,
            "notches": notches["geometry"].iloc[0] if len(notches) else None,
            "fold_line": fold_line,
            "grain_line": grain_line,
        }

    def get_all_graded_geometries(
        self, sample_size_only=False, on_error="raise", out_failures=[]
    ):
        """
        generates all sizes and grading
        testing with EP-6002 which tests many cases that can trip up
        - cases of non closed sew lines where outlines and sew lines are expected to be circuits
        - notches where they are only 1 and not the normal collection
        - general cases of unfolding geometry
        - grading buttons and button holes as well as other internal lines
        - the need to close some graded circuits
        - should do a checksum that points like notches in particular are not dropped after grading
        """
        res.utils.logger.info("Grading all sizes...")
        results = []
        for size in self.sizes if not sample_size_only else [self.sample_size]:
            for key in self.keys:
                try:
                    results.append(self.get_piece_geometry_dict(key=key, size=size))
                except:
                    if on_error == "raise":
                        raise
                    res.utils.logger.warn(f"Failing on {size=} {key=}")
                    out_failures.append((key, size))
        results = pd.DataFrame(results)
        return results

    def get_body_data(self, body_code, body_version, size_code, normed_size):
        """
        at body level - load the body pieces and name them and format them in a way we can save to the database
        this can be passed to the code in sync body to db
        """

        # the piece naming needs to come from some place
        piece_name_map = {}
        data = self.get_all_graded_geometries()
        # we can rename the pieces
        data["key"] = data["key"].map(lambda x: piece_name_map.get(x))
        # now we can map the dictionary data into the right format
        data = data.to_dict("records")
        data = [
            DxfFile.format_dxf_piece_for_db_upsert(
                piece, body_code, body_version, size_code, normed_size
            )
            for piece in data
        ]
        # these data are the piece level data used in the DB body sync code
        # we can refactor part of this out for
        return data

    def get_piece_geometry_dict(
        self, key, size=None, unfold=True, to_image_format=True, plot=False
    ):
        """
        returns the compact geometries while grading off-sample
        there are specific geometries that we need and want to save to the database
        this is the stuff that needs to map to our database for a body pieces collection
        this method can be tested with the sample size which is simpler and then check off-sample sizes which depend on grading to be correct
        """
        size = size or self.sample_size
        cl = self.compact_layers
        cl = cl[cl["key"] == key]
        if size == self.sample_size:
            d = DxfFile.select_geometries_from_layers(cl, key=key, size=size)
        else:
            d = self.piece_grader(key).get_graded_piece(size=size)

        """
        first convert to image space for consistency of operations and scaling
        """
        outline = d["outline"]
        if to_image_format:
            for k in [
                "outline",
                "sew_line",
                "internal_lines",
                "notches",
                "fold_line",
                "grain_line",
            ]:
                if d[k]:
                    d[k] = shift_geometry_to_origin(d[k], bounds=outline.bounds)
                    d[k] = scale_shape_by(DPI_SETTING)(d[k])

                    d[k] = swap_points(d[k])
                    d[k] = invert_axis(d[k], bounds=outline.bounds)
                    # rotate by a fixed amount or to the grain line??

        """
        unfold the symmetric geometries over the fold line
        """

        # do we trust the fold line after transformation
        if unfold:
            fold_line = d.get("fold_line")
            if fold_line:
                for k in ["outline", "sew_line", "internal_lines", "notches"]:
                    d[k] = unfold_geometry(d[k], fold_line)

        """
        compute corners from the exterior and interior circuits and the determine the seam guides from the seam allowances between them        
        """
        # in image space add the corners and seam guides
        scale_factor = 10

        outer_circuit = (
            Polygon(d["outline"])
            .buffer(scale_factor)
            .buffer(-1 * scale_factor)
            .exterior
        )

        d["outer_corners"] = unary_union(magic_critical_point_filter(outer_circuit))

        if d["sew_line"]:
            inner_circuit = (
                Polygon(d["sew_line"])
                .buffer(scale_factor)
                .buffer(-1 * scale_factor)
                .exterior
            )

            d["inner_corners"] = unary_union(magic_critical_point_filter(inner_circuit))
            d["seam_guides"] = DxfFile.get_seam_guide_for_corners(
                outer_circuit, d["outer_corners"], inner_circuit
            )
        else:
            d["inner_corners"] = d["seam_guides"] = None

        d["edges"] = d["outline"] - d["outer_corners"]
        d["internal_edges"] = d["sew_line"] - d["inner_corners"]
        # determine this from context of how we do the other thing
        d["grain_line_degrees"] = None

        """
        standards
        """

        # ensure notches are multis
        d["notches"] = ensure_multi_point(d["notches"])

        if plot:
            from geopandas import GeoDataFrame
            from shapely.geometry.base import BaseGeometry

            geoms = [{"geometry": g} for g in d.values() if isinstance(g, BaseGeometry)]

            GeoDataFrame(geoms).plot(figsize=(20, 10))

        return d

    @staticmethod
    def format_dxf_piece_for_db_upsert(
        piece, body_code, body_version, size_code, normed_size
    ):
        """
        DXF data is saved to the postgres database using the schema described here.
        We should really be using Pydantic types for this but this is fine for fleshing out the new flow
        A mature system should refactor the DXF parser to be far more light weight and the have a piece iterator that can be saved to the database
        we are pretty far from having a clean version of this but it might not matter than much
        the key is to create a reliable parser that exports piece geometries in the right format and saves them to the database so we can forget about the DXF files
        """

        from res.flows.meta.pieces import PieceName
        from res.flows.dxa.styles.helpers import (
            _body_id_from_body_code_and_version,
            _body_piece_id_from_body_id_key_size,
        )

        normed_size = None

        body_id = _body_id_from_body_code_and_version(body_code, body_version)
        sew_identity_symbol = "TODO"  # PieceName(piece_key_no_size).identity_symbol
        piece_key_no_size = piece["key"]

        d = {
            "id": _body_piece_id_from_body_id_key_size(
                body_id, piece_key_no_size, size_code
            ),
            "key": f"{piece['key']}_{piece['size_code']}",
            "piece_key": piece_key_no_size,
            "type": PieceName(piece_key_no_size).type,
            "vs_size_code": normed_size,
            "size_code": size_code,
            "outer_geojson": to_geojson(piece["outline"]),
            "inner_geojson": to_geojson(piece["inner_lines"]),
            "outer_edges_geojson": to_geojson(piece["edges"]),
            "inner_edges_geojson": to_geojson(piece["inner_edges"]),
            "outer_corners_geojson": to_geojson(piece["corners"]),
            "outer_notches_geojson": to_geojson(piece["notches"]),
            "seam_guides_geojson": to_geojson(piece["seam_guides"]),
            "internal_lines_geojson": to_geojson(piece["internal_lines"]),
            # inferred annotations
            "placeholders_geojson": None,
            "pins_geojson": None,
            "pleats_geojson": None,
            "buttons_geojson": None,
            "symmetry": None,  # fold lines, mirror lines?
            "grain_line_degrees": piece["grain_line_degrees"],
            "sew_identity_symbol_old": None,
            "sew_identity_symbol": sew_identity_symbol,
        }

        return d

    @staticmethod
    def type_from_piece_name(piece_name, assume_no_suffix_is_self=False):
        # extract map to a dict later -
        tail = piece_name.split("-")[-1]
        # the code seems to have NO tail for self. this is like 4 components instead of 5
        # it would be better of pieces names consistently had the same number of components
        # as is we need to catch all cases or assume self

        if tail == "F":
            return "fuse"
        if tail == "B":
            return "b-something"
        if tail == "BF":
            return "block_fuse"
        if tail in ("L", "LN"):
            return "lining"
        if tail == "X" or tail == "X2":
            return "stamper"
        if tail == "C":
            # going to treat combos as self - we still know from the piece mappings how the material works
            #
            return "self"
            # return "combo"
        if tail == "P":
            return "paper"
        if tail == "S":
            return "self"

        # this is a little trick for testing some older bodies but its not safe
        if (
            os.environ.get("DXF_NO_PIECE_SUFFIX_ASSUMES_SELF")
            or assume_no_suffix_is_self
        ):
            if len(piece_name.split("-")) == 4:
                return "self"

        return "unknown"

    def __init__(
        self,
        file_or_buffer,
        use_geopandas=False,
        apply_fold_lines=True,
        fuse_buffer_function=None,
        read_raw=False,
        filter_not_like_piece_key=False,
        rule_file_path=None,
        # only legacy things should need this
        should_unfold_geometries=False,
        home_dir=None,
        assume_no_suffix_is_self=False,
    ):
        should_unfold_geometries = (
            str(
                os.environ.get("DXF_SHOULD_UNFOLD_GEOMETRIES", should_unfold_geometries)
            ).lower()
            == "true"
        )
        self._home_dir = home_dir
        self._file = file_or_buffer
        self._grader = None
        self._compact = None
        # this block means we can just point to a path with dxf file or dxf file
        # if we use the path we will also look for a rul file
        if isinstance(file_or_buffer, str):
            self._home_dir = "/".join(file_or_buffer.split("/")[:-1])
            dxf_files, rul_files = _get_files(file_or_buffer)
            if len(dxf_files) > 1:
                raise Exception(
                    "The dxf path is ambiguous; you have pointed to a folder containing multiple: {file_or_buffer}"
                )
            file_or_buffer = dxf_files[0]
            rule_file_path = rul_files[0] if rul_files else None

        self._should_unfold_geometries = should_unfold_geometries
        self._df = self.open(file_or_buffer, use_geopandas=use_geopandas)
        self._attributes = dict(self._df.iloc[0].dropna())
        self._rule_file_path = rule_file_path
        self._validation_details = {}

        self._loaded_rule_file = None
        if rule_file_path:
            self._loaded_rule_file = DxfFile.load_rule_file(self._rule_file_path)
            if self._loaded_rule_file is not None:
                self._loaded_rule_file = self._loaded_rule_file.set_index("id")

        # it seems weird to load this now but the issues is its hard to test the
        # exports without these things together - so treating them here like a bundle
        self._piece_info = []
        self._new_piece_info = None
        if self._home_dir:
            pt = f"{self._home_dir}/pieces.json"
            # res.utils.logger.debug(f"trying to load piece info if it exists")
            try:
                self._piece_info = vstitcher_piece_info(pt)
                try:
                    pieces = VstitcherPieces(pt)
                    if pieces.is_new:
                        self._new_piece_info = pieces
                except Exception as e:
                    res.utils.logger.warn(f"Unable to load new piece info {pt} {e}")
            except Exception as ex:
                pass
                # res.utils.logger.debug(f"{pt} does not exist {repr(ex)}")

        def is_piece_key(x):
            """only allow pieces that are named properly"""
            return x[: len(self.code)] == self.code

        def is_versioned_piece_key(x):
            """only allow pieces that are named properly"""
            return x[: len(self.versioned_code)] == self.versioned_code

        self._apply_fold_lines = apply_fold_lines
        self._fuse_buffer_function = fuse_buffer_function
        self._geom_df = (
            self._df[self._df["has_geometry"]].reset_index().drop("index", 1)
        )
        self._geom_df["has_versioned_piece_code"] = self._geom_df["key"].map(
            is_versioned_piece_key
        )

        if filter_not_like_piece_key:
            self._geom_df = self._geom_df[
                self._geom_df["key"].map(is_piece_key)
            ].reset_index()

        # SOME 3D files TODO learn formats
        if "QUANTITY" in self._geom_df.columns:
            # logger.debug(f"Adding in a quantity by some rule")
            self._geom_df = _make_quantity(self._geom_df)
            for k, v in {
                "PIECE NAME": "Piece Name",
            }.items():
                self._geom_df[v] = self._geom_df[k]
        else:
            self._geom_df["QUANTITY"] = 1

        # Applying a bit opf logic based on teasing how the Resonance conventions
        # we should refactor this later when the model is fully understood
        # the goal for now is to create outline methods that return sets of outlines we would want to best
        # we can re-write the DXF file with the conventions applied

        # if we do not want to apply resonance checks we can look at raw ASTM data
        if read_raw:
            return

        if filter_not_like_piece_key:
            missing_vcodes = self._geom_df[~self._geom_df["has_versioned_piece_code"]]
            assert (
                len(missing_vcodes) == 0
            ), f"Some of the pieces for style {self.name} do not have versioned piece key: {list(missing_vcodes['Piece Name'].unique())}"

        for att in ["geometry", "has_geometry"]:
            if att in self._attributes:
                self._attributes.pop(att)

        length = len(self._geom_df)

        # this should virtually never happen
        if "Piece Name" not in self._geom_df.columns:
            self._geom_df["Piece Name"] = ""

        # FYI, we're overwriting Gerber piece names to support petites
        self._geom_df["piece_name"] = self._geom_df["key"].map(DxfFile.remove_size)
        self._geom_df["SIZE"] = self._geom_df["key"].map(
            lambda k: DxfFile.split_size(k)[-1]
        )

        # we sometimes would override this when we generate pieces in the old mode
        self._geom_df["res.piece_name"] = self._geom_df["key"]

        # the key without the size
        # TODO: Sirsh to take a look
        self._geom_df["key_base"] = self._geom_df["key"].map(
            lambda x: "_".join(str(x).split("_")[:-1])
        )

        # the key for the sample piece
        self._geom_df["key_sample"] = self._geom_df["key_base"].map(
            lambda x: f"{x}_{self.sample_size}"
        )

        def determine_type(row):
            # we can have our own standard attributes
            if "res.type" in row:
                return row["res.type"]

            return self.type_from_piece_name(
                row.get("piece_name"), assume_no_suffix_is_self
            )

        # parse quantity and join back to layer keys
        def fix_up_hack(s):
            # i need to double check how to interpret this commad separated field
            # sometimes its 1,0 and sometimes its 0,1 but i dont think both can be true?
            if s == "0,1":
                return "1,0"
            return s

        def try_mirror(x):
            parts = x.split(",")
            if len(parts) > 1:
                return int(parts[1])
            return 0

        def clean_category(c):
            if pd.notnull(c) and self.key in c:
                return c.replace(f"{self.key}-", "")

        # default the quantity

        if "Quantity" not in self._geom_df:
            self._geom_df["Quantity"] = "1"

        if "CATEGORY" not in self._geom_df:
            self._geom_df["CATEGORY"] = "UNKNOWN"

        q = self._geom_df[["key", "Quantity"]].dropna().drop_duplicates()
        # i am fixing this up for now because i dont know if its human error or standard
        q["Quantity"] = q["Quantity"].map(fix_up_hack)
        q["quantity"] = q["Quantity"].map(lambda x: int(x.split(",")[0]))
        q["is_mirrored"] = q["Quantity"].map(try_mirror)
        q["mirror_quantity"] = q["Quantity"].map(try_mirror)

        q = q[["key", "quantity", "is_mirrored", "mirror_quantity"]]
        self._geom_df = pd.merge(self._geom_df, q, on="key", how="left")

        AVAIL_BASE_INHERITED_ATTRIBUTES = [
            c for c in BASE_INHERITED_ATTRIBUTES if c in self._geom_df.columns
        ]
        self._sample_values = self._geom_df[
            (self._geom_df["SIZE"] == self.sample_size)
            & (self._geom_df["entity_layer"] == 1)
        ][["key"] + AVAIL_BASE_INHERITED_ATTRIBUTES].drop_duplicates()

        # clean the category which is sometimes wrong

        self._geom_df = pd.merge(
            self._geom_df,
            self._sample_values,
            left_on="key_sample",
            right_on="key",
            suffixes=["", "_sample_value"],
            how="left",
        )

        self._geom_df["res.piece_name"] = self._geom_df["key"].map(DxfFile.remove_size)

        for a in AVAIL_BASE_INHERITED_ATTRIBUTES:
            # copy the sample value for this attribute
            self._geom_df[a] = self._geom_df[f"{a}_sample_value"]

        try:
            self._geom_df["type"] = self._geom_df.apply(determine_type, axis=1)
            self._geom_df["CATEGORY"] = self._geom_df["CATEGORY"].map(clean_category)
            self._geom_df["piece_code"] = self._geom_df["key"].map(
                lambda x: x.split("-")[3] if len(x.split("-")) > 3 else "NO CODE"
            )

            # Contract: defaulting to 1 and 0 for q and m
            self._geom_df["quantity"] = self._geom_df["quantity"].fillna(1).astype(int)
            # im not sure about the spec here - this could lead to a bug if it is possible for one of the replicas to be mirrored and one not
            self._geom_df["is_mirrored"] = (
                self._geom_df["is_mirrored"]
                .map(lambda x: True if x > 0 else False)
                .fillna(0)
            )

            self._add_geometries()

        except Exception as ex:
            logger.warn(
                "Failed to apply some steps in parsing by convention - see dump!!"
            )
            self._geom_df.to_pickle("/Users/sirsh/Downloads/dump.pkl")
            raise ex

        # check sum after joins
        assert len(self._geom_df) == length

    def _validate_piece_names(self, self_bf_pieces_only=True):
        """
        Does basic validation on names
        we also load the PieceName form meta and do a convention validation
        """
        from res.flows.meta.pieces import PieceName

        body_name = self.name
        bparts = body_name.split("-")
        body_parts = "-".join(bparts[:3])

        df = self.layers[["key", "category", "size", "type"]].drop_duplicates()

        if self_bf_pieces_only:
            res.utils.logger.debug(
                f"Restricting to self and block fuse pieces for weak validation"
            )
            df = df[df["type"].isin(PRINTABLE_PIECE_TYPES)].reset_index()

        def record_validation_error_for_asset(key, asset):
            """
            add asset key to list of things with this problem
            """
            pe = self.validation_details.get(key, [])
            pe.append(asset)
            self.validation_details[key] = pe

        def f(row):
            errors = []
            k = row["key"]
            try:
                size = row["size"]
                piece_name, s = DxfFile.split_size(k)
                comp = piece_name.split("-")
                piece_body_parts = "-".join(comp[:3])
                body = "-".join(comp[:2])
                version = comp[2]
                if "--" in piece_body_parts:
                    errors.append("DOUBLE_HYPHEN_PIECE_NAME")
                    record_validation_error_for_asset("DOUBLE_HYPHEN_PIECE_NAME", k)

                if s != size:
                    errors.append("BAD_SIZE_COMPONENT")
                    record_validation_error_for_asset("BAD_SIZE_COMPONENT", k)
                if version.lower()[0] != "v":
                    errors.append("BAD_VERSION_COMPONENT")
                    record_validation_error_for_asset("BAD_VERSION_COMPONENT", k)
                if body_parts != piece_body_parts:
                    errors.append("BAD_BODY_NAME_COMPONENT")
                    record_validation_error_for_asset("BAD_BODY_NAME_COMPONENT", k)

                pval = PieceName(piece_name).validate()
                if pval:
                    errors += pval
                    record_validation_error_for_asset("PIECE_NAME_ERRORS", k)

                if errors:
                    return errors
            except:
                errors += ["BAD_PIECE_NAME_STRUCTURE"]
                record_validation_error_for_asset("BAD_PIECE_NAME_STRUCTURE", k)
                return

            return []

        df["errors"] = df.apply(f, axis=1)

        errors = list(df.explode("errors")["errors"].dropna().unique())

        return errors

    def plot(self, xsize=20):
        # convenience method to plot the dxf in e.g., jupyter
        import matplotlib.pyplot as plt

        x0, y0, x1, y1 = unary_union(
            [Polygon(p).boundary for p in self._geom_df.geometry.values]
        ).bounds
        plt.figure(figsize=(xsize, xsize * (y1 - y0) / (x1 - x0)))
        ax = plt.gca()
        ax.set_xlim(x0 - 20, x1 + 20)
        ax.set_ylim(y0 - 20, y1 + 20)
        for i in range(len(self._geom_df.geometry.values)):
            p = Polygon(self._geom_df.geometry[i])
            ax.plot(*p.boundary.xy, color=(0.3, 0.3, 0.45))

    def _validate_piece_categories(self, self_bf_pieces_only=True):
        from res.flows.meta.pieces import PieceName, get_piece_components_cache

        # weaker validation is on the printable pieces by detail
        cats = (
            self.printable_piece_categories
            if self_bf_pieces_only
            else self.piece_categories
        )

        lookup = get_piece_components_cache().read()
        # lookup = None

        validation_errors = []

        for c in cats:
            c = PieceName(c, category_only=True)
            validation_errors += c.validate(known_components=lookup)

        return validation_errors

    def list_missing_3d_color_files(self, color, size, path=None):
        """
        for example
            dxf.list_missing_3d_color_files("WINDIF", "PTZZ0" )
        ^ this should return an empty list
        """
        s3 = res.connectors.load("s3")
        body_code = self.code.replace("-", "_").lower()
        version = self.body_version.lower()

        # you can pass one in if you have it e.g. from an asset (3d only)
        path = (
            path
            or f"s3://meta-one-assets-prod/color_on_shape/{body_code}/{version}/{color.lower()}/{size.lower()}/pieces"
        )

        # res.utils.logger.info("loading pieces...")
        keys = list(self.get_sized_geometries(self.sample_size)["key"])

        missing = []
        for k in keys:
            size_part = f"_{self.sample_size}"
            k = k.replace(size_part, "")
            p = f"{path}/{k}.png"

            if not s3.exists(p):
                missing.append(p)

        return missing

    @property
    def validation_details(self):
        return self._validation_details

    @staticmethod
    def get_seam_allowance_samples_in_inches(
        outline, inner_line, corners, scale_down=DPI_SETTING, buffer=25
    ):
        """
        this function samples the points just beside each corner and then assigns the seam allowance for that edge by averaging two points on the edge and rounding
        parameters are for pixel space in DPI 300
        we scale down to get the seam allowance in inchdes and buffer 25 pixesl by default
        """
        outer_edges = outline - corners.buffer(buffer)
        seam_allowance_sample_points = {}

        for edge in outer_edges.geoms:
            p1 = edge.interpolate(0.1, normalized=True)
            p2 = edge.interpolate(0.9, normalized=True)
            d1 = p1.distance(inner_line)
            d2 = p2.distance(inner_line)
            d = (d1 + d2) / 2
            d /= scale_down
            d = np.round(d, 2)
            p1 = edge.interpolate(0, normalized=True)
            p2 = edge.interpolate(1, normalized=True)

            seam_allowance_sample_points[(p1.x, p1.y)] = d
            seam_allowance_sample_points[(p2.x, p2.y)] = d

        return seam_allowance_sample_points

    def interpolate_away_from_point_by_distance(g, c, pt, d, scale_up=DPI_SETTING):
        """
        interoplate away from a point by disance by sampling
        """
        d *= scale_up
        c = g.project(c)
        p1 = g.interpolate(c + d)
        sample_p2 = g.interpolate(c - d)

        if sample_p2.distance(pt) > p1.distance(pt):
            p1 = sample_p2

        return p1

    @staticmethod
    def get_seam_guide_for_corners(outline, corners, inner_line):
        """
        the seam allowance samples are from the dges made from the corners
        so there are two sample points very close to the corners that we can select via distance function
        once we have these we can interpolate away from the points based on the alternate seam allowances
        then we can create lines that point in to the interior and are parallel to the adjacent segment
        this is done by determining the inner corner (defined here as the closest point from the corner to the inner circuit - could be fragile)

        we return a collection of lines "hinges" at each corner in ordered pairs which can be later used to make seam notches
        scale_factor is used to close the circuit by default in image space
        """

        # get the seam allowance sample points which are convenient for geometric ops
        # for each corner, get the seam allowance just before and after the corner
        seam_allowance_samples = DxfFile.get_seam_allowance_samples_in_inches(
            outline=outline, inner_line=inner_line, corners=corners
        )

        keys = [list(pt) for pt in seam_allowance_samples.keys()]
        values = list(seam_allowance_samples.values())
        tree = KDTree(np.array(keys))

        SAs = []
        for corner in corners:
            # get the two closest seam allowance sample points (indexes of)
            [id1, id2] = list(tree.query(np.array(corner.coords), k=2)[1][0])
            # and their distances
            d1, d2 = values[id1], values[id2]
            # interpolate away from the corner along the outline a distance of the adjacent seam allowance for each direction
            sa_2 = DxfFile.interpolate_away_from_point_by_distance(
                outline, corner, Point(keys[id1]), d=d1
            )
            sa_1 = DxfFile.interpolate_away_from_point_by_distance(
                outline, corner, Point(keys[id2]), d=d2
            )
            # might be safer to do a tree again on inner corners but then there is a risk of the corner detection being unstable between outer and inner
            # as is the sew line could even be a non circuit but we might need to test this on some edge cases
            inner_corner = inner_line.interpolate(inner_line.project(corner))
            # SAs+= [sa_1,sa_2]
            SAs += [LineString([sa_1, inner_corner]), LineString([sa_2, inner_corner])]

        return unary_union(SAs)

    def validate(
        self,
        validate_printable_only=True,
        warnings_as_errors=None,
        validation_size=None,
        try_load_off_sample_size=False,
    ):
        """
        Using the FlowAPI contract to describe validation tags at piece or meta level
        ERRORS
        - is the name valid
        - are the piece names valid for self and block fuse pieces
        - are the units ENGLISH or METRIC -> inches metric
        - is any notch outside the outline bounds with some tolerance -> this could be a symmetry error

        - has no self or block fuse pieces
        - has all sizes ??

        - version should be VXX

        - no samples pieces at all for some reason

        - generated polygons

        notch model
        - if we have a corner seam, we should have an adjacent seam allowance
        [advanced]
        - (did we grade all the notches)


        WARNINGS: These are warnings unless the caller wants to raise them as errors
        - are the units mm
        - the quantity should be 1 or it suggests a legacy gerber thing
        - the path of the file does not match the name
        """

        validation_errors = []

        def outline_contains_notches(row, tolerance=5):
            """
            Usually due to a symmetry error: we had one
            s3://meta-one-assets-prod/color_on_shape/tt_3036/v1/indivr/dxf/TT-3036-V1-INDIVR.dxf
            """
            n = row["notches"]
            o = row["outline"]
            if not pd.isnull(n):
                return Polygon(o).buffer(tolerance).contains(n)
            return True

        # depcreated grade points
        # try:
        #     missing = []
        #     gkeys = self.get_graded_piece_keys()
        #     for k in self.sample_piece_keys:
        #         if k not in gkeys:
        #             missing.append(k)
        #     # if len(missing):
        #     #     res.utils.logger.debug(
        #     #         f"These keys are not included in the grade set {missing} - check the grade point ids match in DXF and rul file"
        #     #     )
        #     #     validation_errors.append("MISSING_GRADED_PIECES")
        # except Exception as ex:
        #     res.utils.logger.warn(
        #         f"Error trying to validate grading {res.utils.ex_repr(ex)}"
        #     )
        #     # validation_errors.append("GRADE_ERROR_UNKNOWN")

        try:
            validation_size = validation_size or self.sample_size

            pcs = self.get_sized_geometries(validation_size)

            if len(pcs) == 0:
                validation_errors.append("BAD_SAMPLE_PIECES")
            # have seen some examples of degenerate polygons e.g. piece KT-6076-V1-SKTFTPNLLF-S_SM
            pcs["poly_area"] = (
                pcs["geometry"].map(lambda g: Polygon(g).area).astype(int)
            )

            pcs["notches_in_outline"] = pcs.apply(outline_contains_notches, axis=1)

            if len(pcs[pcs["poly_area"] == 0]) > 0:
                validation_errors.append("ZERO_AREA_PIECE_OUTLINE")

            if len(pcs[~pcs["notches_in_outline"]]) > 0:
                validation_errors.append("NOTCHES_OUTSIDE_OUTLINE")

        except Exception as ex:
            res.utils.logger.warn(
                f"unable to get sized geometries when validating dxf: {repr(ex)}"
            )
            validation_errors.append("SIZE_GEOMS_FAILED")

        # try:
        #     this validation is redundat now as we will always be on the same size with sized dxf exports or we will find other ways to get notches
        #     if try_load_off_sample_size:
        #         other_size = [s for s in self.sizes if s != self.sample_size]
        #         if len(other_size):
        #             res.utils.logger.debug(
        #                 "trying the off sample size for validation that includes grading"
        #             )
        #             validation_size = other_size[0]

        #             pcs = self.get_sized_geometries(validation_size)
        #             res.utils.logger.debug(f"We can load off-sample sizes")

        # except Exception as ex:
        #     res.utils.logger.warn(
        #         f"Unable to get graded sized geometries when validating dxf: {repr(ex)}"
        #     )
        #     validation_errors.append("GRADED_SIZE_GEOMS_FAILED")

        if self.units.upper() not in ["ENGLISH", "METRIC", "INCHES", "MM"]:
            validation_errors.append("BAD_UNITS")
        if self.units.upper() not in ["METRIC", "MM"]:
            res.utils.logger.warn(f"The pieces should ideally be METRIC -> mm")

        if "self" not in self.piece_types:
            validation_errors.append("NO_SELF_PIECES")

        # f = DxfFile.piece_validation_errors(self.name)

        validation_errors += self._validate_piece_names(
            self_bf_pieces_only=validate_printable_only
        )

        validation_errors += self._validate_piece_categories(
            self_bf_pieces_only=validate_printable_only
        )

        # check grading

        if validation_errors:
            return list(set(validation_errors))

        return []

    @staticmethod
    def validate_files(files, report_dir=False):
        # read from s3 or locally and call validate
        pass

    def get_grade_segments(
        self, size="SM", key=None, added_grade_points=None, preview_ordered_points=False
    ):
        """
        For a particular size we can get the graded segments which are all the lines that move together
        Then for any given point on that surface, we can find a transformation
        We would do this by re-ranking the new point with all the others and see where it sits
        """

        def g(row):
            p1 = row["geometry"]
            s1 = row[size]
            if pd.isnull(s1):
                raise Exception(f"Size {size} is not in the graded segment")
            p1 = Point(p1.x + s1[0], p1.y + s1[1])
            p2 = row["geometry_next"]
            s2 = row[f"{size}_next"]
            p2 = Point(p2.x + s2[0], p2.y + s2[1])

            return LineString([p1, p2])

        df = self.load_grade_point_rules(
            added_grade_points=added_grade_points, key=None
        )

        # filter the size after loading

        if key:
            # take the filter as the piece with the sample size filter
            key = f"{DxfFile.remove_size(key)}_{self.sample_size}"
            df = df[df["key"] == key].reset_index().drop("index", 1)

        # print(len(df), key, added_grade_points)
        df = df[
            [
                "key",
                size,
                "geometry",
                "circuit_rank",
                "next_rank",
                "circuit_order",
                "type",
                "max_set_rank",
                "circuit_length",
            ]
        ]

        if preview_ordered_points:
            return df

        l0 = len(df)

        if l0 == 0:
            raise Exception(f"No grade segments for this size {size} and key {key}")

        df = pd.merge(
            df,
            df,
            left_on=["key", "type", "next_rank"],
            right_on=["key", "type", "circuit_rank"],
            suffixes=["", "_next"],
        )

        assert len(df) == l0, "After merging the adjacent segments we dropped some rows"

        df["segment"] = df.apply(
            lambda row: LineString([row["geometry"], row["geometry_next"]]), axis=1
        )

        df["graded_segment"] = df.apply(g, axis=1)

        df["transform"] = df.apply(
            lambda row: find_2D_transform(
                np.array(row["segment"]), np.array(row["graded_segment"])
            ),
            axis=1,
        )

        cols = [
            "key",
            "segment",
            "type",
            "graded_segment",
            "circuit_order",
            "circuit_order_next",
            "circuit_rank",
            "next_rank",
            "max_set_rank",
            "transform",
        ]

        # boundary confition on the last one
        df.loc[df["circuit_order_next"] == 0, "circuit_order_next"] = df[
            "circuit_length"
        ]

        return df[cols]

    def load_grade_point_rules(
        self,
        added_grade_points=None,
        key=None,
        add_circuit_orders_to_geometries=["outline"],
    ):
        """
        Grade rule are loaded from file but if we are given corner displacements we can augment these rules

        """

        assert (
            self._rule_file_path
        ), "The .rul file path for this DXF must be specified to load grade rules for off-sample size grading"
        df = self._loaded_rule_file

        df = df.join(self.grade_points.set_index("id"))

        if key:
            df = df[df["key"] == key]

        assert (
            len(df) > 0
        ), "After joining the grade point ids there are no grade points for the selected pieces"
        # NOW  we have joined on the id remove it
        # df = df.reset_index().drop(["id", "Delta"], 1)

        if added_grade_points is not None:
            ldash = len(added_grade_points)

            # we should for safety not admit and points that are too near existing points
            existing = unary_union(df["geometry"].dropna())

            added_grade_points["d"] = added_grade_points["geometry"].map(
                lambda g: g.distance(existing)
            )
            # filter grade points outside some inch based threshold because otherwise they are colliding with existing corners that we own
            added_grade_points = (
                added_grade_points[added_grade_points["d"] > 0.125]
                .reset_index()
                .drop("index", 1)
            )

            res.utils.logger.debug(
                f"Adding corner grade points of length {len(added_grade_points)} filtered from {ldash} for keys {set(added_grade_points['key'])}"
            )
            df = pd.concat([df, added_grade_points])

        # warning
        df = df[df["key"].notnull()].reset_index()

        res.utils.logger.debug(
            f"for the key there are {len(df)} grade points with corners added if exists"
        )

        cl = self.compact_layers
        outlines = dict(cl[cl["layer"] == 1][["key", "geometry"]].values)
        # we can also add the grade line somewhere but im not sure how to use it - it might be we just need to
        # transform (rotate 90) the offset but we will know when we see it

        def surface_projection(row):
            # this row key is grading space on the sample size
            k = row["key"]
            p = row["geometry"]
            ol = outlines[k]
            # temp really close
            if ol.intersects(p):
                return ol.project(p)

        # todo consider other grade surfaces
        df["type"] = "surface"

        # we can use the surface order to match other geometries i.e. notches to the segment
        # this is done by projecting the notch and getting the previous and next grade point
        df["circuit_order"] = df.apply(surface_projection, axis=1)
        df["circuit_length"] = df["key"].map(lambda k: outlines[k].length)

        # res.utils.logger.debug(
        #     f"PROC 1 before circuit order filter for the key there are {len(df)} grade points with corners added if exists"
        # )

        df = df[df["circuit_order"].notnull()]

        # res.utils.logger.debug(
        #     f"PROC 2 before circuit order filter for the key there are {len(df)} grade points with corners added if exists"
        # )

        df["circuit_rank"] = (
            df.groupby(["key", "type"])["circuit_order"].rank(
                method="first", ascending=True
            )
            - 1
        )

        df = (
            df[["key", "circuit_rank"]]
            .groupby("key")
            .max()
            .rename(columns={"circuit_rank": "max_set_rank"})
            .join(df.set_index("key"))
            .reset_index()
        )
        df["next_rank"] = df.apply(
            lambda row: (row["circuit_rank"] + 1) % (row["max_set_rank"] + 1), axis=1
        )

        res.utils.logger.debug(
            f"After PROCESSING for the key there are {len(df)} grade points with corners added if exists"
        )

        return df

    @staticmethod
    def compute_graded_points(pts, grade_segments, ref_surface, return_df=False):
        """
        Using a reference surface as used to cover the grade segments
        take some points and fit them t the surface to find the grade segments
        then we can transform the points using the transformation that was used to transform the grade segment

        For example, given a dxf file
        grade_segments = dxf.get_grade_segments(size='MD', key='KT-2011-V9-BODBKPNL-S_XS')
        #get the notches and outline for the piece in the original crs

        graded_notches = compute_graded_points(some_notches, grade_segments, ref_surface=outline)

        """
        dfn = pd.DataFrame(pts, columns=["geometry"])

        def transform_with_z(pt, tr):
            if tr is None:
                return None
            pts = [list(np.array(pt))]
            p = tr(pts)[0]
            try:
                return Point(p[0], p[1], pt.z)
            except Exception as ex:
                res.utils.logger.warn(f"Failed ot grade the points {pt} with {tr}")
                return pt

        dfn["circuit_order"] = dfn.geometry.map(lambda pt: ref_surface.project(pt))

        # we do an interval selection on the grade segments table
        # the indexing is based on the interpolated projection of a point along the circuit or line that is graded
        # we have calculated actual segment transformations for each grade segment so any points fitted to this segment move with it

        def find_segment(x):
            """
            There may be cases where the surface is incomplete and we would lose a notch
            but these could be ones that we can repair from the vs_export
            """
            gs = grade_segments[
                (x >= grade_segments["circuit_order"])
                & (x < grade_segments["circuit_order_next"])
            ]
            if len(gs) == 0:
                res.utils.logger.warn(
                    f"We could not find any graded segment for this piece"
                )
                return None
            return gs.iloc[0]["transform"]

        dfn["transform"] = dfn["circuit_order"].map(find_segment)

        dfn["graded_geometry"] = dfn.apply(
            lambda row: transform_with_z(row["geometry"], row["transform"]), axis=1
        )

        # we may not be able to grade the points
        geom = None
        try:
            geom = unary_union(dfn["graded_geometry"].values)
        except:
            # no report - if this fails is due to the warning above
            pass
        return dfn if return_df else geom

    def get_graded_piece_keys(self):
        df = self._loaded_rule_file

        df = df.join(self.grade_points.set_index("id"), how="left")
        df[df["key"].isnull()]

        return list(df["key"].unique())

    @staticmethod
    def load_rule_file(path):
        """
        loads and parse a grade rule file to a dataframe
        """
        from res.media.images.providers.rul_file import parse_rule_file

        try:
            points = parse_rule_file(path)["points"]
            points = pd.DataFrame(points)
            points["id"] = points["id"].map(lambda x: x.replace("DELTA ", "")).map(int)
            return points

            # for s in sizes:
            #     grade_rules[f"P_{s}"] = grade_rules[s]
            # # for s in sizes:
            # #     grade_rules[s] = grade_rules[s].map(Point)
            # return grade_rules
        except:
            import traceback

            res.utils.logger.warn(
                f"Silently failing to load rul file becasue this functionality is deprecated. we no longer have a grading use case"
            )
            res.utils.logger.debug(traceback.format_exc())

    @staticmethod
    def get_assets(
        seed_df=None,
        size=None,
        fuse_buffer_function=None,
        image_path_root="/Users/sirsh/Downloads/test_images",
        path="/Users/sirsh/Downloads/body_cc_6001_v8_pattern.dxf",
    ):
        """
        If you have a DXF file with correct sizes for images, then you can fetch images assumed to be saved somewhere
        and the later nest with those files
        """

        # test data for now
        dxf_file = DxfFile(path, fuse_buffer_function=fuse_buffer_function)
        size = size or dxf_file._attributes.get("Sample Size")
        df = dxf_file.res_geometry_piece_layer(
            size=size, piece_types=PRINTABLE_PIECE_TYPES
        )

        df["order_key"] = "10101010"
        df["color"] = "PERPIT"
        df["material"] = "CTW70"
        df["file"] = df["res.key"].map(lambda f: f"{image_path_root}/{f}.png")
        # df["resonance_color"] = df.apply(_compile_res_color, axis=1)

        return dataframes.rename_and_whitelist(
            df,
            columns={
                "order_key": "key",
                "res.key": "asset_key",
                "geometry": "geometry",
                "resonance_color": "resonance_color_inches",
                "color": "color",
                "material": "material",
                "type": "piece_type",
                "file": "file",
            },
        )

    def _add_geometries(self):
        """
        we have a few example so far where we *may* want to do this
        1. Unfold seems to be cases that have a mirror line as a layer -> so just include the fold line on any relevant layer for later use
        2. We may need a buffering in xy on certain layers - add this too
        """
        ml = self._geom_df
        ml = ml[ml["entity_layer"] == MIRROR_LINE_LAYER][["key", "geometry"]].rename(
            columns={"geometry": "fold_line"}
        )

        if len(ml) == 0:
            self._geom_df["fold_line"] = None
        else:
            self._geom_df = pd.merge(
                self._geom_df, ml, left_on="key", right_on="key", how="left"
            )

        # TODO we should add some reference points from the shape outline which gives bounds and origin points

    def piece_geometry_metadata(self, row):
        """
        For each piece we get all the resonance color using attributes passed in to data & global algorithm
        This will become an important function of row data to full exploit the dxf for overlay geometry
        It is out so it can be unit tests / called in isolation but used here in the annotation function
        """
        # this used to be called piece_key but im changing it to be the res.key
        piece_key = row["res.key"]

        # generate the labels to know the dims for some label value in the data
        label = get_one_label(row.get("res.asset_key", "0000000"))

        # the disadvantage of making the autobound too big is we will fail to find a viable location on the surface
        width_in = row.get("width_in", label.shape[1] / DEFAULT_DPI)
        height_in = row.get("height_in", label.shape[0] / DEFAULT_DPI)

        # we know if the data are mirrored / unfolded and we should use that information here in future
        return self.get_piece_label_placement(
            piece_key, width=width_in, height=height_in
        )

    def annotate_asset_set(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        get the piece layer data for assets passed in e.g. pieces are adorened with label info and outline geometries
        data contains
        {
            one_number, piece_key, size ...
        }
        see res.flows.make.print_asset_sets for full context

        The purpose of this method is to take a dataset that is comprehensive in its column set but lacking geometric info
        The columns will provide all the information we need to map assets to geometric information provided either in the dxf or global rules

        The result of running this function on an existing dataset is added piece (geometric) info that only the DXF file knows
        In future a database could store these data so they are not generated from this file every time

        Given this inforation res res_color can be added

        [Assets for nesting] -> [ * Dxf/adorned geometric information * ] -> [Image overlay "res color" functions] -> [nested print file]
        """

        # get all non mirrored outlines
        outlines_image_space = self.generate_piece_image_outlines()
        # dont now yet if the piece key will have the mirror tag on it

        def piece_outlines(piece_key: str) -> np.ndarray:
            # for now just choose the first key match but we need to take into account mirrors etc so the key in truth will be derived
            return outlines_image_space[outlines_image_space["key"] == piece_key].iloc[
                0
            ][OUTLINE_300_DPI]

        # augment the data with dxf file details / check all serializable just in case
        data["outline_image_space"] = data["piece_key"].map(piece_outlines)
        # the label info is in inches in the original CRS but we keep enough information to transfor (scale, origin, mirror etc)
        data["res_color_metadata"] = data.apply(self.piece_geometry_metadata, axis=1)

        return data

    def _get_keys_with_mirror_lines(self):
        return self._geom_df[self._geom_df["entity_layer_name"] == 6]["key"].values

    def __repr__(self) -> str:
        return str(self._attributes)

    @property
    def layers_original_names(self):
        return self._geom_df

    @property
    def sample_size(self):
        return self._attributes.get("Sample Size", self._attributes.get("SAMPLE SIZE"))

    @property
    def name(self):
        return self._attributes.get("Style Name", self._attributes.get("STYLE NAME"))

    @property
    def brand(self):
        return "-".join(self.name.split("-")[:1])

    @property
    def code(self):
        return "-".join(self.name.split("-")[:2])

    @property
    def versioned_code(self):
        try:
            return "-".join(self.name.split("-")[:3])
        except:
            # res.utils.logger.warn(
            #     f"Unable to parse the versioned code from our conventions"
            # )
            return ""

    @property
    def body_version(self):
        try:
            return self.name.split("-")[2]
        except:
            res.utils.logger.warn(
                f"The version could not be parsed from the name {self.name} "
            )
            return None

    @property
    def key(self):
        return "-".join(self.name.split("-")[:3])

    @property
    def units(self):
        """
        some assumptions here but we do need to be clear what the units are in all cases
        """
        units = self._attributes.get("UNITS")
        if units == "METRIC":
            return "mm"
        return "inches"

    @property
    def grade_points(self):
        df = pd.DataFrame([d for d in self._grade_points]).drop_duplicates(
            subset=["key", "id"]
        )
        df["geometry"] = df["geometry"].map(Point)
        return df

    @property
    def grade_points_with_rules(self):
        df = self.grade_points
        rules = self._loaded_rule_file
        return pd.merge(df, rules, on="id")

    def piece_grader(self, key) -> PieceGrading:
        """
        returns an object that can grade geometries near grade points for any size

        for example an outline g can be graded in this way for a size '7'

        self.geometry_grader.grade_geometry_for_size(g,'7')

        """
        # if self._grader is None:
        graded_sizes = self._loaded_rule_file.columns
        self._grader = PieceGrading(
            self.grade_points_with_rules,
            key=key,
            layers=self.compact_layers,
            sizes=graded_sizes,
            drop_duplicates=True,
        )

        return self._grader

    @property
    def primary_piece_codes(self):
        """
        this should match the codes used in the style but need to check conventions
        """
        ll = self.layers
        l = list(ll[ll["type"].isin(PRINTABLE_PIECE_TYPES)]["category"].unique())
        return [item.rstrip("-") for item in l]

    @property
    def body_header(self):
        return pd.DataFrame(
            [
                {
                    "brand": self.brand,
                    "name": self.name,
                    "sizes": self.sizes,
                    "body_code": self.code,
                    "body_version": self.body_version,
                    "sample_size": self.sample_size,
                    "key": self.key,
                    "units": self.units,
                }
            ]
        )

    @property
    def sizes(self):
        try:
            # this is trusted if we have it - the rul file columns gives the sizes
            return self._loaded_rule_file.columns
        except:
            # a fully valid dxf may have all graded sizes but based size dxfs dont have multiple sizes
            return list(self.layers["size"].unique())

    @property
    def sample_piece_keys(self):
        l = self.layers
        return list(
            l[(l["size"] == self.sample_size) & (l["type"] != "unknown")][
                "key"
            ].unique()
        )

    @property
    def piece_types(self):
        return list(self.layers["type"].dropna().unique())

    @property
    def piece_category_suffixes(self):
        cats = self.layers["category"].dropna().map(lambda x: x.split("-")[-1]).unique()

        return list(cats)

    @property
    def piece_categories(self):
        cats = self.layers["category"].dropna().unique()

        return list(cats)

    @property
    def printable_piece_categories(self):
        l = self.layers
        cats = l[l["type"].isin(PRINTABLE_PIECE_TYPES)]["category"].dropna().unique()

        return list(cats)

    def describe(self):
        d = self.body_header.to_dict("records")[0]

        d["piece_categories"] = self.piece_categories
        d["piece_category_suffixes"] = self.piece_category_suffixes

        return pd.DataFrame([d])

    @staticmethod
    def corners_as_grade_points(row, sample_size, key):
        def line_to_grade(l, base_size, my_size):
            """
            just turns corner displacements into grade
            the sample size is the origin geometry here and that is important because we fit the grade points to a sample surface and then move it
            the translation is the displacement to the base size from the sample size

            this is not needed and should not be used of there are already at least 2 grade points on the line including corners
            this should be be used if there is no corner correspondence
            """
            base = l.interpolate(0)
            sample = l.interpolate(1, normalized=True)
            return {
                "Delta": None,
                "geometry": sample,
                base_size: (0.0, 0.0),
                my_size: (
                    np.round(base.x - sample.x, 5),
                    np.round(base.y - sample.y, 5),
                ),
                "key": key,
            }

        # a, b = dxf_notches.pair_corners(row)

        a = dxf_notches.sorted_corners(row, "outline")
        b = dxf_notches.sorted_corners(row, "outline_sample")
        if len(a) != len(b):
            res.utils.logger.warn(f"Ignoring corners as grade points")
            return None
            # raise Exception(
            #     f"When the corners were sorted on the sample size and self size for piece {row['key']}, we found a different number of corners - unable to grade from corners"
            # )

        """
        Here we need to add corners as grade points as sometimes they are missing grade points
        maybe, we should add them if they are too close to other grade points

        corner displacements go from a -> b , sample size to current size but check how line to grade works above as this is just convention
        """
        corner_displacement_vectors = [LineString([a[i], b[i]]) for i in range(len(a))]
        corner_grade_rules = pd.DataFrame(
            [
                line_to_grade(l, sample_size, row["size"])
                for l in corner_displacement_vectors
            ]
        )

        return corner_grade_rules

    def export_static_meta_one(self, filename, material_codes):
        data = self.get_compact_layer_selection()
        DxfFile.export_static_meta_one_for_material(
            df=data, material_codes=material_codes, filename=filename
        )
        return data

    def get_compact_layer_selection(
        self,
        layers={
            1: "outlines",
            4: "notches",
            8: "internal_lines",
            11: "internal_lines_x",
        },
        scale_factor=300,
        make_notch_lines=True,
    ):
        from res.media.images.geometry import to2d, nearest_points

        """
        This is used for a more raw loading of DXF data that might not use our conventions
        """
        scale_fx = scale_shape_by(scale_factor)
        c = self.compact_unfolded_layers.copy()
        data = {}
        for k, v in layers.items():
            l = c[c["layer"] == k].set_index("key").rename(columns={"geometry": v})[[v]]
            if v == "notches" and not make_notch_lines:
                # this for drawing purposes - buffer the little guys
                l[v] = l[v].map(
                    lambda x: (
                        None
                        if pd.isnull(x)
                        else unary_union([n.buffer(0.1).exterior for n in x])
                    )
                )
            l[v] = l[v].map(to2d)
            data[k] = l

        keys = list(layers.keys())
        base = data[keys[0]]
        for k in keys[1:]:
            base = base.join(data[k], how="left")

        if make_notch_lines:

            def _proc_row(row):
                o = row["outlines"]
                n = row["notches"]
                if pd.isnull(n):
                    return None

                def nrm(pt, o):
                    opt = nearest_points(o, pt)[0]
                    return LineString([pt, opt])

                off_surface = unary_union(
                    [nrm(p, o) for p in n if round(p.distance(o), 2) > 0.0]
                )

                return off_surface

            base["notches"] = base.apply(_proc_row, axis=1)

        base["geometry"] = base.apply(lambda row: unary_union(row.dropna()), axis=1)
        base["geometry"] = (
            base["geometry"]
            .map(lambda x: scale_fx(x) if not pd.isnull(x) else None)
            .map(shift_geometry_to_origin)
        )

        base["original"] = base["outlines"]
        base["outlines"] = (
            base["outlines"]
            .map(lambda x: scale_fx(x) if not pd.isnull(x) else None)
            .map(lambda x: to2dint(x) if pd.notnull(x) else None)
        ).map(shift_geometry_to_origin)
        return base.reset_index()

    @property
    def keys(self):
        c = self.compact_layers
        keys = c["key"].unique()
        return keys

    @property
    def compact_unfolded_layers(self):
        c = self.compact_layers
        c["geometry"] = c.apply(DxfFile.try_unfold_on_axis(), axis=1)
        return c

    @property
    def compact_layers(self):
        if self._compact is not None:
            return self._compact

        self._compact = self.make_compact_layers(self.layers)

        def from_piece_code(s):
            if pd.isnull(s):
                return s
            return s.split("_")[0]

        ##TODO: fix up logic maybe move to one place bit this is an efficient place
        # some pieces have no code for some reason even in the same size
        self._compact["category"] = self._compact["category"].fillna(
            self._compact["piece_code"].map(from_piece_code)
        )
        return self._compact

    def get_raw_shape(self, key):
        key = key.split("_")[0]
        c = self.compact_layers
        c = c[c["piece_name"] == key]
        c = c[c["layer"] == 1]
        return dict(c.iloc[0])

    def grade_from_sample(self, df):
        """
        today we only grade notches
        notches live on graded surfaces and move with the surfaces once we identify them
        """

        def f(row):
            n = row.get("notches_sample")
            if pd.notnull(n):
                ol = row.get("outline_sample")
                # see what happens later
                # restricting notches for the surface
                # n = n.intersection(ol)

                # we need the segments for all surfaces where there are notches.
                # this requires some test

                ######################

                # res.utils.logger.debug(f"NOTCH COUNT for row {row['key']} - {len(n)}")
                # resample if there is only one point...
                if n.type == "Point":
                    n = MultiPoint([n])

                if self._rule_file_path and len(n) > 0:
                    # he key is graded based on the sample size
                    size = row["size"]

                    okey = row["key"]

                    key = f'{DxfFile.remove_size(row["key"])}_{self.sample_size}'

                    # get the grade corners between the corners on sample size and other sizes
                    # the key is the ones used by the system e.g. in the sample size - its not used to join anything
                    corner_grade_points = DxfFile.corners_as_grade_points(
                        row, self.sample_size, key=key
                    )

                    grade_segments = self.get_grade_segments(
                        size=size,
                        key=key,
                        added_grade_points=corner_grade_points,
                    )

                    res.utils.logger.info(
                        f"Grading {okey} with {len(grade_segments)} segments"
                    )

                    try:
                        return DxfFile.compute_graded_points(
                            n, grade_segments, ref_surface=ol
                        )
                    except:
                        message = f"WARNING failed when looking for graded segments and computing graded points for a {row['size']} of {key} "
                        res.utils.logger.warn(message)

                        raise

                return DxfFile.get_graded_notches(
                    row["outline_sample"],
                    row["outline"],
                    n,
                    plot=False,
                )

        res.utils.logger.info(
            f"Grading notches from sample. The .rul file will be used if exists:> {self._rule_file_path}"
        )

        if self._rule_file_path:
            df["notches"] = df.apply(f, axis=1)

        return df

    # TODO
    # def iterate_edges(g):
    #     pts = np.array(g.coords)
    #     zs = np.unique(pts[:,-1])
    #     #expand points at boundaries so points can belong to multiple edges at the boundaries
    #     lines = []
    #     for z in zs:
    #         pts = [p for p in l.coords if p[-1]==z]
    #         lines.append(LineString(l.intersection(MultiPoint(pts))))
    #     return unary_union(lines)

    # iterate_edges(l)

    @staticmethod
    def label_outline_edges_from_ref(
        g: LinearRing, ref="center_of_mass_bottom", out_ref=[], corners=None
    ):
        """
        We label by iterating over keypoints (supplied or dervied) which we call corners
        These are our seam dividers and we label the z point of the outline based on the start corner
        The start corner is chosen as the one that is at the start of the bottom line
        the bottom line is anyone that contains the exremeum point near the center of mass at the bottom

            g = row['outline']
            l = label_outline_edges_from_ref(g, out_ref=out_ref)
            GeoDataFrame([l,out_ref[0]],columns=['geometry']).plot()

        """

        # edges unlike outlines are orientated clock`wise. we remember the sense for reflections on the outline
        # it was my understanding that orient works on many objects but it seems non-polygon does
        # not behave as expected so we orient the polygon and retrieve the linear ring for the outline
        g = orient(Polygon(g), sign=-1)
        g = LinearRing(g.exterior)

        corners = corners or magic_critical_point_filter(g)

        # wrt to g
        def near_to(x):
            return g.interpolate(g.project(Point(x)))

        pts = np.array(g)
        com = list(pts.mean(axis=0))
        # this is the EAST point in ASTM space - if the model is somewhere weirdly offset to the right this is wrong
        # todo go left of the bounds
        # unit test cases with curvature :KEY = "CC-6001-V9-DRSCLRU-BF_SM"
        # these offsets are not set in stone yet
        # the X offset chooses the right infinity point and the y offset is a bias for symmetry
        ref = near_to([g.bounds[0] - 5, com[1] - 0.5])
        out_ref.append(ref)
        df = pd.DataFrame([Point(p) for p in g.coords], columns=["geometry"])
        df["res.num_corners"] = len(corners)
        df["corner_index"] = df["geometry"].map(lambda g: 1 if g in corners else 0)
        df["corner_index"] = df["corner_index"].cumsum() % len(corners)
        df["dist_ref"] = df["geometry"].map(lambda g: g.distance(ref))
        df["first"] = 0
        df.loc[np.argmin(df["dist_ref"]), "first"] = 1

        # re-index the corners by the one that is crossing the ref point
        start_corner = df[df["first"] == 1].iloc[0]["corner_index"]
        df["corner_index"] = df["corner_index"].map(
            lambda x: (x - start_corner) % len(corners)
        )

        def label_point(row):
            g = row["geometry"]
            return Point(g.x, g.y, row["corner_index"])

        # maybe duplicate points at boundaries if we want to generate edges - this requires some care on the boundary
        df["geometry"] = df.apply(label_point, axis=1)

        return LinearRing([p for p in df["geometry"].values])

    @staticmethod
    def mirror_geometry(row, geometry):
        """
        We mirror on the y axis in astm space but we do so using the view port of the full piece
        note this is usees a trick which be tested for many types of geometries
        """
        G = row[geometry]

        if pd.notnull(G):
            if (
                geometry != "outline"
            ):  # or more generally if the bounds are smaller than the bounds of the outline - could lead to a bug if we dont just remove the magic bounds
                B = row["bounds"]
                B_ = mirror_on_y_axis(B)
                return mirror_on_y_axis(unary_union([G, B])) - B_
            else:
                return mirror_on_y_axis(G)

        return None

    def fold_aware_bounds():
        """
        the bounds are used to shift geometries - but the viable surface is already assumed to respect this
        """

    @staticmethod
    def unpack_folds_and_replicas(df):
        """
        Check unfolds and mirrors e.g.

            #for mirrors
            row = g[g['res.key']=='CC-6001-V8-PKTBAG_SM-L-1'].iloc[0]
            G = row['outline']
            N = row['notches']
            B = row['bounds']
            ax = GeoDataFrame([G],columns=['geometry']).plot()
            GeoDataFrame(N,columns=['geometry']).plot(ax=ax)

            #folds - yoke:
            res.key: CC-6001-V8-YOKE_SM-R-0

        """

        def replica_to_code(i):
            if i == 0:
                return "TOP"
            if i == 1:
                # for under
                return "U"
            if pd.isnull(i):
                return ""
            return str(i)

        def new_cat(row):
            """
            some convention attempt to replace pieces with pose suffixes
            ASTM files do mirror and duplicate so we need a reliable rule to
            name replicas/mirrors (we could consult Airtable and make the best assignments)
            """

            # hack for cc-6001

            rc = replica_to_code(row.get("replica_index"))
            if row["quantity"] < 2:
                rc = ""
            m = "L" if row["is_mirrored"] else "R"
            if row["mirror_quantity"] == 0:
                m = ""
            return f"{row['category']}{m}{rc}"

        def get_res_name(row):
            return row["key"].replace(row["category"], new_cat(row))

        def get_res_piece_name(row):
            return row["piece_name"].replace(row["category"], new_cat(row))

        for geometry in ["outline", "notches", "internal_lines"]:
            # print("unfolding ", geometry, "line 592")
            if geometry in df.columns:
                try:
                    df[geometry] = df.apply(
                        DxfFile.try_unfold_on_axis(geometry), axis=1
                    )
                except Exception as ex:
                    print("error unfolding", repr(ex))
                    df.to_pickle("/Users/sirsh/Downloads/dump.pkl")
                    raise
            # if its not it may cause problems in res color i.e. if its not for an entire body

        # adding a magic bounds in the z space
        df["bounds"] = df["outline"].map(
            lambda g: MultiPoint([Point(*g.bounds[:2], -42), Point(*g.bounds[2:], -42)])
        )

        df["_replica_spec"] = df.apply(DxfFile.gen_replicas, axis=1)
        df = df.explode("_replica_spec").reset_index().drop("index", 1)
        df["replica_index"] = df["_replica_spec"].map(lambda x: x[0])
        df["is_mirrored"] = df["_replica_spec"].map(lambda x: x[-1])

        for geometry in ["outline", "notches", "internal_lines"]:
            if geometry in df.columns:
                op = lambda g: DxfFile.mirror_geometry(g, geometry)
                df.loc[df["is_mirrored"], geometry] = df.apply(op, axis=1)

        df["res.key"] = df.apply(get_res_name, axis=1)
        df["res.piece_name"] = df.apply(get_res_piece_name, axis=1)
        df["bounds"] = df["outline"].map(lambda g: g.bounds)

        return df

    def export_printable_pieces(self, size, target=None):
        size = size  # or self.sample_size
        pcs = self.get_sized_geometries(
            size=size, piece_types=PRINTABLE_PIECE_TYPES
        ).copy()
        target = self._home_dir or target
        assert (
            target is not None
        ), "Specify a target as it could not be inferred from the dxf file"

        data = DxfFile._process_geoms_for_export(pcs)

        # hack :( we went down a bad route when the tech pack was corrputed to try to recover sizes from the double category dxf
        # now we are loosing the path for the target which should be named for the petitie
        if "P_" in size:
            res.utils.logger.info(
                f"Applying a hack to change the target swapping {self.sizes}"
            )
            other_size = [s for s in self.sizes if s != size][0]
            target = target.replace(f"dxf_by_size/{other_size}", f"dxf_by_size/{size}")

        filename = f"{target}/printable_pieces.feather"
        res.utils.logger.info(f"exporting  pieces to {filename}")

        if "s3://" in target:
            s3 = res.connectors.load("s3")

            s3.write(filename, data)
        else:
            data.to_feather(filename)

    @staticmethod
    def export_static_meta_one_for_material(
        df,
        filename,
        material_codes,
        outline_column="outlines",
        geometry_column="geometry",
        **kwargs,
    ):
        """
        helper for some new conventions
        """
        from res.media.images.providers.dxf import DPI_SETTING, MM_PER_INCH, DxfFile
        from res.learn.optimization.nest import nest_dataframe
        from res.media.images.geometry import scale, invert_axis
        from res.flows.meta.ONE.geometry_ex import geometry_data_to_hpgl

        if isinstance(material_codes, str):
            material_codes = [material_codes]
        res.utils.logger.info(f"Fetching materials data for {material_codes}")
        mat_props = res.connectors.load(
            "airtable"
        ).get_airtable_table_for_schema_by_name(
            "make.material_properties",
            f"""FIND({{Material Code}}, '{",".join(material_codes)}')""",
        )
        mat_props = mat_props[mat_props["key"].notnull()]

        res.utils.logger.debug(mat_props.to_dict("records"))
        mat_dict = {
            r["key"]: r for r in mat_props.to_dict("records") if not pd.isnull(r["key"])
        }

        material_code = material_codes[0]

        material_width_inches = mat_dict[material_code]["cuttable_width"]
        paper_scale_x = mat_dict[material_code]["paper_marker_compensation_width"]
        paper_scale_y = mat_dict[material_code]["paper_marker_compensation_length"]
        buffer = 10
        mm_width_for_nesting = material_width_inches * MM_PER_INCH  # / DPI_SETTING

        def _pixel_space_to_mm(shape):
            return scale(
                shape,
                paper_scale_x * MM_PER_INCH / DPI_SETTING,
                paper_scale_y * MM_PER_INCH / DPI_SETTING,
            )

        filename = filename.split(".")[0]

        res.utils.logger.info(
            f"<<<<<< Preparing HPGL plotter files, will convert to scale and print outlines and full geometries >>>>>>>"
        )

        geometry_data_to_hpgl(
            df.copy(),
            # assumed geometry for now
            geometry_column=geometry_column,
            filename=f"{filename}.plt",
        )

        geometry_data_to_hpgl(
            # rename to copy and also to use a consistent input
            df[["key", outline_column]].rename(columns={outline_column: "geometry"}),
            filename=f"{filename}_outlines.plt",
        )

        # # scale to mms for what we want to do below
        # # this makes sense as a "model" of a nest - it does not have to match any nest
        # # we are going to invert to the image space
        # if kwargs.get("invert_y_axis", True):
        #     cutline_df["geometry"] = cutline_df["geometry"].map(invert_axis)
        res.utils.logger.info(
            f"<<<<<< Preparing DXF format for col {outline_column} (pixels -> mm ) >>>>>>>"
        )

        df[outline_column] = df[outline_column].map(_pixel_space_to_mm)

        # nest the dataframe in mm scale for the given material so we can scale things and then nest them
        cutline_df = nest_dataframe(
            df.reset_index(drop=True),
            # nest the outlnes in this case
            # TODO we could try to make notches
            geometry_column=outline_column,
            output_bounds_width=mm_width_for_nesting,
            output_bounds_length=mm_width_for_nesting * 100,
            buffer=kwargs.get("buffer", buffer),
            plot=kwargs.get("plot", False),
        )

        # sending the outlien column
        cutline_df["geometry"] = cutline_df["nested.original.geometry"]
        DxfFile.export_dataframe(
            cutline_df[[outline_column]],
            column=outline_column,
            path=f"{filename}.dxf",
            point_conversion_op=lambda p: p.coords,
        )

        return cutline_df

    def get_sample_piece(self, key):
        pcs = self.get_sized_geometries(size=self.sample_size).set_index("key")
        return dict(pcs.loc[key])

    def get_sized_geometries(
        self,
        size,
        piece_types=PRINTABLE_PIECE_TYPES,
        key=None,
        layers={
            84: "outline",
            4: "notches",
            6: "fold_line",
            8: "internal_lines",
            7: "grain_line",
            9: "stripe",
            10: "plaid",
            14: "sew_lines",
        },
        add_dervied=True,
        image_space_geometry_field="geometry",
        image_space_options={},
        warn_on_empty_set=True,
        on_fail_derived="raise",
    ):
        """
        Compact geoms for any size along with properties/geoms from the sample size for grading
        """

        cl = self.compact_layers.drop("layer_name", 1)
        layers = dict(layers)

        # TODO: Contract: for some 3d file compatability
        if 84 not in cl["layer"].unique() and 84 in layers:
            logger.warn("removing layer 84, replace with layer 1")
            layers[1] = layers.pop(84)

        # if we do not have an outlie later, we can auto -replace with keypoints in future

        cl = cl[cl["layer"].isin(list(layers.keys()))]
        if piece_types is not None:
            cl = cl[cl["type"].isin(piece_types)]
        cl["layer"] = cl["layer"].map(lambda k: layers[k])
        # take a copy from here
        cl = cl[cl["size"].isin([self.sample_size, size])].copy()

        # res.key has a better chance of being unique but in future astm files will be source of truth unique
        try:
            gm = cl.pivot("key", "layer", "geometry").reset_index()
        except Exception as ex:
            cl.to_pickle("/Users/sirsh/Downloads/dump.pkl")
            raise ex

        cl = cl.drop(["geometry", "layer"], 1).drop_duplicates()
        cl = pd.merge(cl, gm, on=["key"])

        geoms = list(layers.values())

        # TODO: move somewhere else - this is just for consistency
        for g in ["fold_line", "notches", "internal_lines", "sew_lines"]:
            if g not in cl.columns:
                cl[g] = None
        sample_cl = cl[cl["size"] == self.sample_size]
        # now restruct cl too to size
        cl = cl[cl["size"] == size]

        if len(cl) == 0:
            if not warn_on_empty_set:
                return cl

            res.utils.logger, warn(
                f"after filtering body {self.name} by size {size} there are no items"
            )
            return cl

        geoms = [g for g in geoms if g in sample_cl.columns]

        # there was a time we used the res piece name but should never need that in future
        cl = pd.merge(
            cl,
            sample_cl[geoms + ["piece_name"]],
            on=["piece_name"],
            suffixes=["", "_sample"],
        )

        # deprecating this beahviour
        # if size != self.sample_size:
        #     logger.debug(
        #         f"attempting to grade size {size} from sample size {self.sample_size}"
        #     )
        #     # this adds any shapes like notches that we dont have by grading them
        #     cl = self.grade_from_sample(cl)
        #     logger.debug(f"set of size {len(cl)} after grading")

        if self._should_unfold_geometries:
            res.utils.logger.info("unfolding geometries")
            cl = DxfFile.unpack_folds_and_replicas(cl)
            # the sample does not need to be unfolded
            # sample_cl = DxfFile.unpack_folds_and_replicas(sample_cl)
            cl["res.num_notches_unfolded"] = sample_cl["notches"].map(
                lambda g: 0 if g is None or pd.isnull(g) else len(list(g))
            )

        # notch filtering - we can apply this filter to ensure that we have the correct number of notches on the surface
        # only if these are surface notches that we care about -> we could do this for each surface but its a bit messy
        # len(n.intersection(ol.buffer(5)))

        surface_notches_only = False

        def remove_non_surface_notches(row):
            ol = row["outline"]
            n = row.get("notches")
            try:
                small_parameter = 5
                return n.intersection(ol.buffer(small_parameter))
            except:
                return n

        if surface_notches_only:
            # if we dont this we can also make sure to dedup on the notch frame because
            # the biggest risk with superfluous notches is messing up the notch index
            res.utils.logger.info(
                f"Removing all non-surface notches - make sure this is intended"
            )
            cl["res.all_notches"] = cl["notches"]
            cl["notches"] = cl.apply(remove_non_surface_notches, axis=1)

        # todo need to add fold aware bounds

        # drop metadata absorbed into other fields
        cl = cl.drop(
            [
                c
                for c in [
                    "quantity",
                    "mirror_quantity",
                    "_replica_spec",
                    "replica_index",
                    "is_mirrored",
                    "fold_line_sample",
                ]
                if c in cl.columns
            ],
            1,
        )

        # image_space_options["mirror"] = True
        image_space_options["source_units"] = self.units
        outline_to_image_space = lambda row: geometry_to_image_space(
            row, "outline", **image_space_options
        )
        if image_space_geometry_field:
            cl[image_space_geometry_field] = cl.apply(outline_to_image_space, axis=1)

        # pose info
        cl["side"] = "center"
        try:
            cl.loc[cl["piece_code"].map(lambda x: x[-1] == "L"), "side"] = "left"
            cl.loc[cl["piece_code"].map(lambda x: x[-1] == "R"), "side"] = "right"

            # this should be in the name in future but for now when mirroring etc. we need to make sure this exists as a unique piece type
            cl["res.category"] = cl["piece_name"].map(
                lambda s: "-".join((s.split("-")[3:]))
            )
        except Exception as ex:
            res.utils.logger.warn(f"Missing something {repr(ex)}")
        # this key is sometimes important - the sizeless one
        cl["piece_key"] = cl["key"].map(DxfFile.remove_size)

        notch_angles = dxf_notches.get_notch_angles(self)

        # adding the notch angles to the row - the number of notches will match the ordered number of angles
        cl = pd.merge(cl, notch_angles, how="left", on="key")

        res.utils.logger.debug(f"making corners and edges")

        try:
            cl["piece_key_no_version"] = cl["piece_name"].map(
                DxfFile._remove_version_from_piece_name
            )
            cl["corners"] = cl.apply(self.make_corners, axis=1)
            cl["edges"] = cl.apply(self.make_edges, axis=1)
            cl["seam_allowances"] = None

            if self._piece_info:
                res.utils.logger.debug(
                    "getting seam allowances on edges used swap points to match scans"
                )
                cl["seam_allowances"] = cl.apply(
                    lambda row: self._piece_info.get_seam_allowances_near_shape(
                        row["piece_name"],
                        swap_points(row["edges"]),
                        # row["piece_name"], row["edges"] # JL I think it should be this...
                    ),
                    axis=1,
                )

        except Exception as ex:
            res.utils.logger.debug(
                f"Failed to make derived geometries {repr(ex)} - will continue without it for now"
            )
            if on_fail_derived == "raise":
                raise ex

        if key:
            return cl.set_index("key").loc[key]

        return cl

    @staticmethod
    def _remove_version_from_piece_name(s):
        parts = s.split("-")
        return "-".join([p for i, p in enumerate(parts) if i != 2])

    @staticmethod
    def make_edges(row, buffer=1):
        """
        given a row that has both an outline and the corresponding corners
        """
        assert "corners" in row, "the corners must be added to the row to make edges"
        return unary_union(
            label_edge_segments(
                row["geometry"],
                fn=dxf_notches.projection_offset_op(row),
                corners=row["corners"],
                buffer=buffer,
            )
        )

    @staticmethod
    def make_corners(row):
        angles = []

        corners = None

        try:
            corners = magic_critical_point_filter(
                row["geometry"], angles=angles, critical_angle_threshold=141
            )

            if len(corners):
                # instead of the default corners, first sort them on convention
                # corners = sorted_corners_conventional(row["outline"])
                corners = MultiPoint(corners)
                # this is a temp hack because we do now know how to remove darts yet
                # but then sometimes if we do this it can remove some corners we want
                # if self.code in ["DJ-6000"]:
                corners = filter_sensitive_corners(
                    corners, angles=angles, g=row["geometry"]
                )
            else:
                return None

        except Exception as ex:
            res.utils.logger.warn(f"failed to filter corners {repr(ex)} {dict(row)}")

        return corners

    def make_compact_layers(self, layers):
        """

        DXF gives many partial geometries per "row" and we comact them into geometries

        Refactor some of these field properties out
        """
        layer_names = [
            "Piece boundary",
            "Piece boundary quality validation curves",
            "Turn points",
            "Notches; V-notch and slit-notch; alignment.",
            "Curve points",
            "Drill holes",
            "Grain line",
            "Mirror line",
        ]

        layer_types = [
            "LinearRing",
            "LinearRing",
            "MultiPoint",
            "MultiPoint",
            "MultiPoint",
            "MultiPoint",
            "LineString",
            "LineString",
        ]

        keep_fields = [
            "key",
            "layer_name",
            "layer",
            "size",
            "category",
            "piece_name",
            "quantity",
            "mirror_quantity",
            "type",
            "piece_code",
        ]

        mapping = dict(zip(layer_names, layer_types))

        geoms = []

        def try_LineString(s):
            try:
                return LineString(list(s))
            except:
                return s

        for gkey, v in layers.groupby(["key", "layer_name"]):
            try:
                piece_key, key = gkey
                rr = dict(v.iloc[0])
                tp = rr["type"]
                fold_line = rr["fold_line"]

                layer_type = mapping.get(key, "MultiLineString")

                if layer_type in ["MultiPoint"]:
                    geom = unary_union(v["geometry"])
                elif layer_type in ["MultiLineString"]:
                    geom = v["geometry"].map(try_LineString)
                    geom = unary_union(geom.values)
                elif layer_type == "LineString":
                    # some geometries might yield their points this way - test cases
                    # we try because we need at least two points etc for it to be a line
                    geom = try_LineString(unary_union(v["geometry"].values))
                elif layer_type in ["LinearRing", "Polygon"]:
                    # TODO: is this valid: are the points useful too? should we default null polylines to geom
                    geom = unary_union(v["polylines"].dropna().values)

                    # if tp != "unknown":
                    #     assert (
                    #         geom.type != "LineString"
                    #     ), f"the piece outline for {gkey} type {tp} should not be a {geom.type} but a LinearRing"
                    pgz = list(polygonize(geom))
                    # not sure what is the best way to ensure polygons
                    # often polygonize does it bit in some cases e.g. CC-6053-V4-CLRU_XXSM we can just take a polygon

                    geom = pgz[0] if len(pgz) > 0 else Polygon(geom)

                    if layer_type == "LinearRing":
                        geom = LinearRing(geom.exterior.coords)
                else:
                    geom = unary_union(v["geometry"])

                geoms.append(
                    {
                        "key": piece_key,
                        "layer_name": key,
                        "geometry": geom,
                        "fold_line": fold_line,
                    }
                )
            except Exception as ex:
                print(gkey, "failed with", repr(ex))

                raise

        geoms = pd.DataFrame(geoms)

        return pd.merge(
            layers[[k for k in keep_fields if k in layers.columns]].drop_duplicates(),
            geoms,
            on=["key", "layer_name"],
            how="left",
        )

    def get_piece_count(self, piece_types=None):
        """
        Get the piece outline layers for the base size and compute the derived count
        It is possible to filter by piece types e.g. to count only self or fused pieces - supply a list of types
        """

        def counter(row):
            c, m = row["quantity"], row["mirror_quantity"]
            return 2 * m + c - m

        outlines = self.get_piece_outlines(
            size=self.sample_size, piece_types=piece_types
        )
        data = outlines[["key", "quantity", "mirror_quantity"]].reset_index()

        data = data.groupby("key").min()
        data["total"] = data.apply(counter, axis=1)
        return int(data["total"].sum())

    @property
    def layers(self):
        """
        A collection of layers in our schema
        it is important to note that the column selector determines what is visible so any future derived fields should be added to the column selector
        """
        return dataframes.rename_and_whitelist(
            self.layers_original_names, columns=column_selector
        )

    def get_layer(self, i):
        return self.layers[self.layers["layer"] == i]

    # def get_outlines(self, hd=False):
    #     layer = 1 if not hd else 84
    #     shapes = self.layers[self.layers["layer"] == layer]
    #     return outlines_from_point_set_groups()

    def get_filled_notches(self, **kwargs):
        pass

    def iterate_all_outlines(self, df, geometry_column=OUTLINE_300_DPI):
        """

        DEPRECATE??
        This will not be a core method as we want to do all the transformations from an index of piece keys using a compsite key
        -composite key with be (key, applied_transformation e.g. mirrored and index of repilcas) e.g. ABCDEF_SM_RF_01
        - unfolded pieces are just unfolded with their normal key as there are no pieces that are not unfolded for this cases

        Apply some resonance logic that understands DXF semantics such as piece counts
        and then iterate all actual shapes with a key (note this key is no longer unique with mirrors etc)
        - the 300 dpi is the default scale we use as this is typically used to get piece outlines in image space
        - we return is_mirrored and replica_index to be used in constructing a composite key

        """
        for _, row in df.iterrows():
            for i in range(row["quantity"]):
                p = row[geometry_column]
                d = {
                    "key": row["key"],
                    "replica_index": i,
                    geometry_column: p,
                    "is_mirror": False,
                    "type": row["type"],
                    "size": row["size"],
                }
                yield d
                if row["is_mirrored"]:
                    # the mirroring happens on the X axis in image space which in the dxf is y axis
                    md = dict(d)
                    md["is_mirror"] = True
                    # TODO - if we dont apply the transformation here we can confine it to get_transformed layer??
                    # md[geometry_column] = mirror_on_y_axis(Polygon(p)).exterior.coords
                    yield md

    def unique_piece_keys(self):
        """
        iterate the piece keys and generate a unique res.key for the piece
        """

    def generate_piece_image_outlines(self, size=None, piece_types=None, **kwargs):
        """
        higher level function that does these things:
        1. Calls piece outlines with the right parameters to create comparable images in image space in the correct coordinate reference system
        2. creates all mirrored, unfolded, duplicated pieces in the set of piece outines
        3. constructs some sort of derived piece key

        These new outlines should always match the pieces that are found in markers for a particular size
        Filtering by piece type is possible in cases where some types are not used in the given marker?

        """

        # Change strategy because we dont want to generate everything until we need it so we should lazt load transformed shapes
        # TODO this, instead here we need to create the index for the shapes that then when we need a piece such as KEY or KEY-mirrored
        # we can ask for it. When asking for it we apply the mirroring or unfolding as a transformation
        # in cases where we need to fetch notches for a layer, we can also get the transformed notches per piece

        # change how this is done - generate the index and the map over it getting the transformed piece: default the res.key for the default

        layers = self.get_piece_outlines(size=size, piece_types=piece_types)
        # iterate over the shapes applying all logical transformations and providing
        df = pd.DataFrame(self.iterate_all_outlines(layers))

        df[OUTLINE_300_DPI] = df[OUTLINE_300_DPI].map(DxfFile.outline_to_image_space)

        return df

    @property
    def transformer_layers(self):
        """
        will implement this but may only use it for testing as we do not want to transform lots of shapes for lots of sizes we may not even need
        the preferred way will be to generate all the keys for mirrored pieces etc and then map/apply and generate the transformed pieces
        just for pieces we need
        """

    @staticmethod
    def outline_to_image_space(
        outline: np.ndarray, as_shapely: bool = False
    ) -> np.ndarray:
        """
        The purpose of this function is simple to transform outlines so they are in the same CRS (coordinate reference system) as ndarray outlines from PNGs
        allowing us to compare like for like

        In dxf, the layout is cartesian i.e xy instead of yx.
        Images in numpy are yx and the y axis is only inverted

        To convert to image space we "mirror" or invert what would be the y axis i.e. the x axis in the dxf file

        The geometry is then shifted to the origin

        We only return as shapely for inspection but ndarray is the default
        """
        g = mirror_on_x_axis(Polygon(outline), shift_to_origin=True)
        return np.array(g.exterior.coords) if not as_shapely else g

    @staticmethod
    def resonance_key(row):
        """
        A row based on convention to generate a unique key after transformations

        we should look at gerber conventions. i think we do right first and then mirror onto left  but we should look to see where the fold lines are

        """
        TR = "RF" if row["is_mirrored"] else "00"
        IDX = row["replica_index"]
        return f"{row['key']}_{TR}{IDX}"

    @staticmethod
    def gen_replicas(row):
        """
        generate the replicas - spec mirror
        example 2,1 becomes so that we have replicas some of which are mirrors
        [
            [0,0]
            [0,1]
            [1,0]
        ]
        """

        def _default(a, b):
            return b if pd.isnull(a) else a

        reps = []
        v = [_default(row["quantity"], 1), _default(row["mirror_quantity"], 0)]

        # adding in a min of 1 here because otherwise it does bit make sense - check     body_cc_3075_v4_pattern
        for i in range(max(1, v[0])):
            reps.append((i, False))
            if i < v[-1]:
                reps.append((i, True))
        return reps

    def show_fold_for_piece(self, key, buffer=False, plot=True):
        """
        utility to viz unfolding logic
        after looking at the result when we unfold we should find it is a single polygon
        if there are thin lines we an use a buffer expansion trick with a threshold
        unfold_on_line(df.iloc[0]['geometry'], df.iloc[1]['geometry'], eps=0.001)

        """
        cl = self.compact_layers
        pc = cl[cl["key"] == key]
        gg = pc[pc["layer"] == 84].iloc[0]["geometry"]
        ff = pc[pc["layer"] == 6].iloc[0]["geometry"]

        if buffer:
            df = pd.ataFrame([gg] + [ff.buffer(0.1)], columns=["geometry"])
        else:
            df = pd.DataFrame([gg] + [ff], columns=["geometry"])
        if plot:
            from geopandas import GeoDataFrame

            df = GeoDataFrame(df)
            df.plot(figsize=(10, 10), cmap="autumn")
        return df

    @staticmethod
    def try_unfold_on_axis(field="geometry"):
        """
        these needs to work for all geometry types e.g. notches are multipoints, internal lines are multilinestrings
        outlines and polygons are treated as polygons that need to be buffered sometimes and then we take the outer ring

        buffering and also removing 0-area polygons is something needed too

        In future ASTM files will not require unfolding

        Test case examples:
            RB-6012-V3-BK: 0 polygons on unfolds

        """

        def f(row):
            g = row[field]

            if pd.notnull(row.get("fold_line")) and pd.notnull(g):
                # SA changed to assume line not points
                fline = row["fold_line"]
                if fline.type != "LineString":
                    fline = LineString(fline)

                type_in = g.type

                # we do this only because unfolding polygons is easier - but convert back to a linear ring at the end
                if g.type == "LinearRing":
                    g = Polygon(g)

                origin_type = g.type
                # for unfolding we must always treat the outline as a polygon
                try:
                    g = unfold_on_line(g, fline, eps=0.001)
                except Exception as ex:
                    res.utils.logger.warn(
                        f"Failed to unfold {row.get('key')}: {repr(ex)}"
                    )
                    # raise ex
                # TODO: this is a safety but we need to engineer this better - what this does is make sure unfolds are not thwarted by imperfect manual alignments
                # it is not worth investing too much in this now as we may changed manual flows in future
                # the shape buffer needs to be be approximate to some small order as we really use the images as the high-res artifacts - especially when we are not using fold lines
                # however should ensure transformations and notch alignments make sense still. there is a concern that some corners and boundaries may be corrupted
                if g.type == "MultiPolygon":
                    # i think for multi line strings and multipoints we dont mind so much where the unfold as tiny precision
                    # polygons cause problems though and i think only polygons
                    g = buffer_expansion_trick(g, buffer=0.005)

                    # buffer expansion and unfolds works on polygons and we only want polygons out if polygons in
                    assert (
                        g.type != "MultiPolygon"
                    ), f"an unfolded geometry [{field}] on piece in \n{row}\n became a {g.type} which is not supported"

                if type_in == "LinearRing":
                    assert (
                        g.type == "Polygon"
                    ), f"expected a polygon for [{field}] on piece in \n{row}\n when unfolding a linear ring"

                    g = LinearRing(g.exterior.coords)

            return g

        return f

    def get_transformed_layer(
        self,
        layer,
        size=None,
        piece_types=None,
        column_mapping=None,
        **kwargs,
    ):
        """
        Apply ALL the transformations from raw DXF to Resonance geometries e.g. mirror, unfold etc.
        We should maintain this in the original CRS for simplicity and not do things like scale the DPI or shift to the origin which is done later

        We add a piece filter because this is the only way to transform a piece even though it is batch by default

        """
        # TODO - need to figure out which geometries we should use - for now using polylines when we have them
        # for some shapes the layer returns two types but there is always a best geometry to default to for any shape type
        # want to do this properly without losing information

        # outlines are outlines and are treated different to point sets but we do not handle that yet
        # if however the shapes are perfectly fetched including if they are mirrored or not then we can handle that all here
        # at the moment the other place we apply transformations is in the iteration where we mirror

        l = self.get_piece_layers(
            layer=layer, size=size, piece_types=piece_types, compact=True
        )

        assert len(l) > 0 or layer in [
            INTERNAL_LINE_KEYPOINT_LAYER,
            INTERNAL_LINE_LAYER,
        ], f"Unable to find entities for size {size} and layer {layer}"

        if len(l) == 0:
            return l

        def try_mirror_on_axis(row):
            g = row["geometry"]
            if row["is_mirrored"] == True:
                return mirror_on_y_axis(g)
            return g

        try:
            l["geometry"] = l.apply(DxfFile.try_unfold_on_axis(), axis=1)
        except Exception as ex:
            print(repr(ex))

        l["_replica_spec"] = l.apply(DxfFile.gen_replicas, axis=1)
        l = l.explode("_replica_spec")

        # logger.debug(f"exploded replicas and size went from {ct} to {len(l)}")
        l["replica_index"] = l["_replica_spec"].map(lambda x: x[0])
        l["is_mirrored"] = l["_replica_spec"].map(lambda x: x[-1])

        # not sure about this - i think the DXF format also names left and right coming out of the page??
        # SA 6/9/2021 -> when i looked at the piece matching it seems tha the dxf appears on the page in the opposite pose
        l["geometry"] = l["geometry"].map(mirror_on_y_axis)

        # temp
        l["original_geometry"] = l["geometry"]

        # mirror here when there is a mirror in the composite key
        l["geometry"] = l.apply(try_mirror_on_axis, axis=1)
        l["res.key"] = l.apply(DxfFile.resonance_key, axis=1)

        l["outline"] = l["geometry"].map(DxfFile._outline)

        # label the edges for the boundary - what does this break
        # NB: contract: for any lines that can be labelled we labelled in the Z coordinate - this MUST be done for res-color
        # for viable surfaces we also need to iterate edges to find fold lines on v-notches etc.
        # CONTRACT:

        if layer in [OUTLINE_KEYPOINT_LAYER, OUTLINE_LAYER]:
            l["edges"] = l["outline"].map(lambda g: label_edge_segments(g))

        # default this - for simple buffering we can make use of this
        l["outline_prescale_factor"] = l["key"].map(lambda x: (1, 1))

        if self._fuse_buffer_function is not None:
            # experimental - roughly scale block fuse
            logger.debug(f"Scaling block fuse pieces")
            l["geometry"] = l.apply(
                lambda row: (
                    row["geometry"].buffer(0.75).exterior
                    if row["type"] == "block_fuse"
                    else row["geometry"]
                ),
                axis=1,
            )
            # we can record this factor as the scaling factor to apply before scaling to 300 dpi in image space
            l["outline_prescale_factor"] = l.apply(
                lambda row: compute_scale_factor(row[OUTLINE_300_DPI], row["outline"]),
                axis=1,
            )
            l[OUTLINE_300_DPI] = l[OUTLINE_300_DPI].map(scale_shape_by(DEFAULT_DPI))

        else:
            l[OUTLINE_300_DPI] = l["outline"].map(scale_shape_by(DEFAULT_DPI))

        l = (
            l
            if not column_mapping
            else dataframes.rename_and_whitelist(l, column_mapping)
        )

        return l

    # TODO - get notches layer using the layer-transformed-reader so we can uniformly apply the same rules to any layer we read

    # TODO refactor following two - just differ on layer
    def get_piece_outline_keypoints(
        self, size=None, piece_types=None, piece_key=None, **kwargs
    ):
        """
        Low resolution piece bundaries - key points
        All configured DPIS are returned by the underlying function
        """
        return self.get_transformed_layer(
            OUTLINE_KEYPOINT_LAYER,
            size=size,
            piece_types=piece_types,
            piece_key=piece_key,
            **kwargs,
        )

    def get_piece_outlines(self, size=None, piece_types=None, piece_key=None, **kwargs):
        """
        High resolution piece bundaries - with full details
        All configured DPIs are returned by the underlying function
        """
        return self.get_transformed_layer(
            OUTLINE_LAYER,
            size=size,
            piece_types=piece_types,
            piece_key=piece_key,
            **kwargs,
        )

    def get_piece_outline(
        self,
        key,
        shift_to_origin=False,
        swap_axes=True,
        as_shapely=True,
        mirrored=False,
    ):
        """
        Get a specific piece outline in inches in the DXF format
        TEST: must return the original CRS for default parameters
         - swap axis are not applied unless we shift to image space

        we can perform transformations to make this useful in other contexts
        For example, if we wanted to overlay this in image space, we would scale and transform it
        Note: Because this function has so many options, adding overloads later that are more simple is a good idea but we leave it for now

        in dxf x is x and y is y but when we want to create a numpy array we must invert
        - so the shapely result is preferred as xy and the numpy result as yx
        - this confusion always arises somewhere but we should decide where we want it

        Returns shapely or ndarray:
        different use cases will prefer different formats. For example in image space it may be misleading to look at the yx shapely object
        for simplicity we default to non image space and shapely but a special use case would be

            (shift_to_image_space=True, shift_to_origin=True, as_shapely=False)

        as this is the ndarray outline of the piece that can be compared with real pieces
        - in `.annotate_assets()` we set this as result in the dataframe as `outline_yx_at_origin`
        """
        ol = self.get_piece_outlines(piece_key=key)
        g = ol[(ol["key"] == key) & (ol["is_mirrored"] == mirrored)].iloc[0]["geometry"]

        if shift_to_origin:
            g = shift_geometry_to_origin(g)

        return np.array(g.exterior.coords) if not as_shapely else g

    def get_piece_notches(self, piece_key, mirrored=False, **kwargs):
        """
        Returns multipoint of notches on each layer row
        We can ask for the mirrored version if it exists
        """
        notch_layer = 4  # do we need multiple?
        l = self.get_transformed_layer(layer=notch_layer, piece_key=piece_key)
        l = l[l["is_mirrored"] == mirrored].iloc[0]
        # compose the geomtries into a multipolygon -> l
        return unary_union(l["geometry"])

    @staticmethod
    def _outline(g):
        # TODO - the smartest outline of a shape
        # we really just need the polygon outlines
        try:
            if g.type == "Polygon":
                return g.exterior
            else:
                return g.convex_hull.exterior
        except Exception as ex:
            logger.warn(
                f"Failed to add outline to shape - maybe something that has no outline or hull - {repr(ex)}"
            )
            return None

    def get_base_geometries(
        self, piece_types=None, piece_outline_layer=OUTLINE_KEYPOINT_LAYER
    ):
        l = self.get_piece_layers(
            layer=piece_outline_layer,
            size=self.sample_size,
            piece_types=piece_types,
            compact=True,
        )
        n = self.get_piece_layers(
            layer=NOTCH_LAYER,
            size=self.sample_size,
            piece_types=piece_types,
            compact=True,
        )

        # could cache this
        return pd.merge(
            l, n[["key", "geometry"]], on="key", suffixes=["", "_sample_notches"]
        )

    def _apply_grading(
        self,
        size,
        piece_types=None,
        grading_base=None,
        layer=NOTCH_LAYER,
        piece_outline_layer=OUTLINE_KEYPOINT_LAYER,
    ):
        """
        For each key, compute e.g. the notches - assume its the notch layer for now
        """
        # assume layers is filtered for size of pieces

        l = self.get_piece_layers(
            layer=piece_outline_layer, size=size, piece_types=piece_types, compact=True
        )
        if grading_base is None:
            grading_base = self.get_base_geometries(piece_types=piece_types)

        l = pd.merge(l, grading_base, on="piece_name", suffixes=["", "_sample"])

        assert len(l) == len(
            grading_base
        ), f"Error joining sample pieces - the length of the join should equal the number of sample pieces {grading_base}"

        def f(row):
            # if troubleshooting failures on examples uncomment this
            # logger.debug(f"{row['key']}")
            return self.get_graded_notches(
                row["geometry_sample"],
                row["geometry"],
                row["geometry_sample_notches"],
                piece_key=row["key"],
            )

        l["geometry"] = l.apply(f, axis=1)
        l["polylines"] = None
        l["layer"] = layer
        l["layer_name"] = "Graded Notches"
        # this is a useful troubleshooting number because a given layer may not be getting the correct number of notches which "may" break things
        l["res.graded_notch_count"] = l["geometry"].apply(lambda g: len(unary_union(g)))

        return l[[c for c in l.columns if "_sample" not in c]]

    def get_piece_layers(
        self, layer, size=None, piece_types=None, compact=False, add_outline=True
    ):
        """
        Filter the underlying layers
        Compact: Actually create the unified polygons e.g. from points->polygons or multipoints
        """

        l = self.layers

        if (
            layer not in [OUTLINE_KEYPOINT_LAYER, OUTLINE_LAYER]
            and size != self.sample_size
        ):
            logger.debug(
                f"layer {layer} must be computed from the sample size {self.sample_size}"
            )

            return self._apply_grading(size=size, piece_types=piece_types, layer=layer)

        if len(l) > 0:
            idx = l["layer"] == layer
            if size:
                idx &= l["size"] == size
            if piece_types:
                if not isinstance(piece_types, list):
                    piece_types = [piece_types]
                idx &= l["type"].isin(piece_types)

            l = l[idx]

            # this assertion does not need to exist sometimes
            assert len(l) > 0 or layer in [
                INTERNAL_LINE_KEYPOINT_LAYER,
                INTERNAL_LINE_LAYER,
            ], f"Returned an empty set when filtering layers by size {size} and layer {layer} for {self.name}"

            if len(l) == 0:
                return l

            is_outline_or_polygon = layer in [OUTLINE_KEYPOINT_LAYER, OUTLINE_LAYER]
            geom_column = "polylines" if is_outline_or_polygon else "geometry"

            if compact:
                # for internal lines we are given multipoints that we can form lines with
                pre_map = None
                if layer in [
                    INTERNAL_LINE_KEYPOINT_LAYER,
                    INTERNAL_LINE_LAYER,
                ]:
                    pre_map = lambda g: LineString(g)
                lc = (
                    outlines_from_point_set_groups(l, geometry_column=geom_column)
                    if is_outline_or_polygon
                    else union_geometries(
                        l, geometry_column=geom_column, pre_map_geometry=pre_map
                    )
                )

                assert (
                    len(lc) > 0
                ), "Failed to generate outlines when union layer entities"
                # logic here is to drop all old columns but assume that we have a geom key value
                l = (
                    pd.merge(lc, l, on="key", suffixes=["", "old"])
                    .drop("geometryold", 1)
                    .drop_duplicates(subset=["key"])
                )

        return l

    def _read(self, file_or_buffer):
        def try_int(v):
            try:
                return int(v)
            except:
                return v

        if isinstance(file_or_buffer, str):
            if file_or_buffer[:5] == "s3://":
                file_or_buffer = res.connectors.load("s3").mounted_file(file_or_buffer)
            doc = ezdxf.readfile(file_or_buffer)
        else:
            doc = ezdxf.read(file_or_buffer)

        # cache
        self._doc = doc
        self._grade_points = []

        self._text_with_loc = []

        for i, b in enumerate(doc.blocks):
            block_attributes = {}
            for j, entity in enumerate(b):
                if entity.DXFTYPE == "TEXT":
                    plain_text = entity.plain_text()
                    if re.match(r"^[A-Z0-9\-]+$", plain_text) and "-" in plain_text:
                        # piece name label possibly
                        self._text_with_loc.append(
                            (plain_text, Point(list(entity.dxf.insert)[0:2]))
                        )
                    if "#" in plain_text:
                        att = entity.dxf.all_existing_dxf_attribs()
                        try:
                            grade_id = int(plain_text.split(" ")[-1].rstrip().lstrip())
                            pt = list(
                                np.array(
                                    entity.dxf.all_existing_dxf_attribs()["insert"]
                                )[:2]
                            )
                            self._grade_points.append(
                                {"key": b.name, "id": grade_id, "geometry": pt}
                            )
                        except Exception as ex:
                            res.utils.logger.debug(
                                f"Failing to parse a record that should be a grade point"
                            )
                    if ":" in plain_text:
                        plain_text = plain_text.split(":")
                        block_attributes[plain_text[0].rstrip().lstrip()] = (
                            plain_text[1].rstrip().lstrip()
                        )
                    else:
                        block_attributes["__label"] = (
                            plain_text.split("#")[-1].lstrip().rstrip()
                        )

            # is this from where we get block attributes

            for j, entity in enumerate(b):
                points = []
                lines = []
                polylines = []
                attributes = {}
                attributes["block"] = i
                # add the conventional res-schema field
                attributes["key"] = b.name
                attributes["block_name"] = b.name
                attributes["block_layout_key"] = b.layout_key

                layer = try_int(entity.dxf.layer)
                attributes["entity_layer"] = layer

                attributes["layer_has_points"] = False
                attributes["layer_has_lines"] = False
                attributes["layer_has_polylines"] = False

                ldef = LAYER_DEF.get(attributes["entity_layer"])
                attributes["entity_layer_name"] = (
                    f"LAYER_{attributes['entity_layer']}"
                    if not ldef
                    else ldef.get("definition")
                )
                attributes["entity_layer_role"] = (
                    "UNKNOWN" if not ldef else ldef.get("purpose")
                )

                # geoms
                if entity.DXFTYPE == "POINT":
                    points.append(entity.dxf.location)
                    # if layer == 4:
                    attributes[
                        "geometry_attributes"
                    ] = entity.dxf.all_existing_dxf_attribs()
                    attributes["layer_has_points"] = True
                elif entity.DXFTYPE == "LINE":
                    p = [list(entity.dxf.start)[:2], list(entity.dxf.end)[:2]]
                    points += p
                    lines.append(p)
                    attributes["layer_has_lines"] = True
                elif entity.DXFTYPE == "POLYLINE":
                    # add all the points - we may want to be adding lines here instead
                    points += [list(p)[:2] for p in entity.points()]
                    polylines.append([list(p)[:2] for p in entity.points()])
                    attributes["layer_has_polylines"] = True
                elif entity.DXFTYPE == "LWPOLYLINE":
                    # add all the points - we may want to be adding lines here instead
                    points += [list(p)[:2] for p in entity.vertices()]
                    polylines.append([list(p)[:2] for p in entity.vertices()])
                    attributes["layer_has_polylines"] = True
                else:
                    pass  # print('have not handled',entity.DXFTYPE )

                attributes["geometry"] = MultiPoint(points)
                attributes["lines"] = MultiLineString(lines) if len(lines) > 0 else None
                attributes["polylines"] = (
                    MultiLineString([LineString(p) for p in polylines])
                    if len(polylines) > 0
                    else None
                )

                hull = attributes["geometry"].convex_hull
                attributes["nearest_text"] = min(
                    self._text_with_loc,
                    key=lambda lp: hull.distance(lp[1]),
                    default=(None, None),
                )[0]

                attributes["has_geometry"] = len(points) > 0
                attributes.update(block_attributes)

                yield attributes

    def open(self, file_or_buffer, use_geopandas=False):
        f = self._read(file_or_buffer)
        if use_geopandas:
            from geopandas import GeoDataFrame

            return GeoDataFrame(f)

        return pd.DataFrame(f)

    def _compute_sa_notches_from_piece_info(self, row, scale_by=10):
        """
        use the seam allowance information to compute seam allowance notches - we dont try to avoid collisions with existing
        this is a cluster fuck
        we have to last minute add stamper sa notches
        to do this we have to reuse existing code that does things like make edges
        we have all sorts of geometries in different coordinate systems

        basically here what we want to do is reliably calculate the SA notches in the metric system DXF-space shape using the edges in a rotated VS export - we swap the incoming outline to match that edge collection
        we need to scale shapes to the same place to match edges and then transfer the SA values onto those edges
        then we have to interpolate along adjacent edges with those SA values
        then we have to rotate the passed in outline generated notches back
        :(
        """
        to_linestring = lambda x: (
            LineString([[item["y"], item["x"]] for item in x])
            if len(x) > 1
            else Point()
        )

        def scale_to(b, c):
            """
            scale one shape to be near the other
            """
            w1, h1 = b.bounds[2] - b.bounds[0], b.bounds[3] - b.bounds[1]
            w2, h2 = c.bounds[2] - c.bounds[0], c.bounds[3] - c.bounds[1]
            b = scale(b, xfact=w2 / w1, yfact=h2 / h1)
            x_shift = c.bounds[0] - b.bounds[0]
            y_shift = c.bounds[1] - b.bounds[1]
            return translate(b, xoff=x_shift, yoff=y_shift)

        def set_sa_ends(data, edges):
            l = len(data)
            _next = lambda i: (i + 1) % l
            _prev = lambda i: (i - 1 + l) % l
            pairs = []
            points = []
            for a in range(l):
                pairs.append((data[_prev(a)], data[_next(a)]))

            for e in edges:
                z = int(e.interpolate(0).z)
                #             print(z, pairs[z])
                #             print(e)
                a, b = pairs[z][0], pairs[z][1]
                # print(e.length)
                if a:
                    points.append(e.interpolate(e.length - a))
                if b:
                    points.append(e.interpolate(b))
            return points

        def make_arbitrary_edges(row):
            corners = magic_critical_point_filter(
                row["outline"], critical_angle_threshold=141
            )
            corners = MultiPoint(corners)
            edges = row["outline"] - corners.buffer(2)
            edges = [tr_edge(e, i) for i, e in enumerate(edges)]

            return unary_union(edges)

        key = row["piece_name"]
        sas = [e["seam_allowance"] for e in self._piece_info[key]["edges"]]
        a = [to_linestring(e["points"]) for e in self._piece_info[key]["edges"]]
        vedges = unary_union(a)
        # we make an aribtrary edge number - for this it does not matter and we swap points to match before scaling to location
        edges = swap_points(make_arbitrary_edges(row))
        ####matches stampers
        vedges = scale_to(vedges, edges)

        d = {}
        for sai, v in enumerate(vedges):
            pt = v.interpolate(0.5, normalized=True)
            # find distances from vs edges that have seam allowances to our main edges that have a z component
            distances = [
                pt.distance(k.interpolate(0.5, normalized=True)) for k in edges
            ]
            # get the z compoement of that other edge becasue that is where we want to assign seam allowances
            eids = [e.interpolate(0).z for e in edges]
            # print(eids)
            # create a map, key'd by the edge id that is matched with the shortest distance
            d[eids[np.argmin(distances)]] = sas[sai] * scale_by
        if not len(d):
            return []
        if sum(d.values()) == 0:
            return []
        # print(d)
        b = set_sa_ends(d, edges)

        return swap_points(unary_union(b))

    def export_to_hpgl(self, filename, df, bounds=None, **kwargs):
        """
        saves the geometry dataframe to a plotter format
        - outlines can be added as solid lines or we can add
        - internal lines column can be used too and we use a different plotting style: supply a map of geometries and line types

        todo test bounds and sizes
        spec: https://www.hpmuseum.net/document.php?catfile=213
        see: https://www.isoplotec.co.jp/HPGL/eHPGL.htm#-PM(Polygon%20Mode)

        try convert to svg to see if it looks ok
        https://products.aspose.app/cad/conversion/plt-to-svg

        view plt online
        https://products.groupdocs.app/viewer/plt

        https://fileproinfo.com/tools/viewer/plt

        """

        import numpy as np
        from res.media.images.geometry import unary_union
        from res.learn.optimization.nest import nest_dataframe

        s3 = res.connectors.load("s3")

        def notch_lines(row):
            try:
                ls = unary_union(dxf_notches.get_notch_frame(row)["notch_line"])
            except:
                return None
            return ls

        def translate_geom(col):
            """
            using contract shift from nest data frame - take a column geom and translate it
            """

            def f(row):
                try:
                    offset = row["coff"]
                    # max_y = row["max_y"]
                    # offset = row["coff_origin"]
                    g = row[col]
                    # g = mirror_on_y_axis(row[col])
                    # we invert on the y axis too becuase of the origin shift
                    g = translate(g, xoff=offset[0], yoff=offset[1])

                    # g = invert_axis(g) ... ...
                    return g
                except Exception as ex:
                    res.utils.logger.warn(ex)
                    return None

            return f

        # this is something like dots per mm
        # the plotter plots 0.025 mm resolution such that 1mm is 40 units
        HPGL_PLOTTER_RES = 40
        FACTOR = HPGL_PLOTTER_RES * 25.4

        # scale by 39.37 or look at 0.0254 as the inch scaling by the res
        def scale_for_plotter(g, scaler=39.37):
            """
            looked at examples - we simply scale from mm to inches and then multiply by 1000 when using this header

            IN;
            IP0,0,1016,1016;
            SC0,1000,0,1000;


            """
            try:
                if pd.notnull(g):
                    # we are in mm when doing this so change to inches and then remove decimals
                    #
                    return scale_shape_by(scaler)(g)
                    # return scale_shape_by(0.0393701 * FACTOR)(g)
            except Exception as ex:
                print(repr(ex))
                return None

        def centroid_offset(row, geometry_column="outline"):
            """
            a little confusing but we nest in image space and its confising to know intent for 0 index
            here we invert as though we nested from 0 down
            """

            a = row[geometry_column].centroid
            b = row[f"nested.{geometry_column}"].centroid
            y_max = row["max_y"]
            b = Point(b.x, y_max - b.y)
            return (b.x - a.x, b.y - a.y)

        geom_col = "outline"

        # these notches are tested to match the outline - that is important as later we can transform all the basic DXF stuff in the same way; outlines, notches*2 and internal lines
        try:
            df["sa_notches"] = None
            df["sa_notches"] = df.apply(
                self._compute_sa_notches_from_piece_info, axis=1
            ).map(lambda x: MultiPoint(x))
            df["sa_notches"] = df["sa_notches"].map(
                lambda N: (
                    unary_union([n.buffer(2).exterior for n in N])
                    if pd.notnull(N)
                    else None
                )
            )
        except Exception as ex:
            res.utils.logger.warn(f"FAILING TO MAKE SA NOTCHES {ex}")

        try:
            df["notch_lines"] = df["notches"].map(
                lambda N: (
                    unary_union([n.buffer(2).exterior for n in N])
                    if pd.notnull(N)
                    else None
                )
            )
        except Exception as ex:
            res.utils.logger.warn(f"Failed to make notch lines {ex}")
            df["notch_lines"] = None

        # this is so we dont get internal lines that overlap with outlines and double up in the plotter
        df["internal_lines"] = df["internal_lines"] - df[geom_col]

        for col in [geom_col, "notch_lines", "internal_lines", "sa_notches"]:
            # df[col] = df[col].map(lambda g: swap_points(g) if pd.notnull(g) else None)
            df[col] = df[col].map(
                lambda g: scale_for_plotter(g) if pd.notnull(g) else None
            )

        # mm scaling ########
        TXT_FACTOR = 600  # function of eigen size of label
        PLOTTER_WIDTH = (
            100 * 1000
        )  # 25.4 # this factor is an inch scaled by 1000 for the units we want to nest/plot
        BUFFER = 500

        # TXT_FACTOR = 5
        # PLOTTER_WIDTH = 56 * 25.4
        # BUFFER = 20

        # TXT_FACTOR = 5 * 12
        # PLOTTER_WIDTH = 16500
        # for col in [geom_col, "notch_lines", "internal_lines"]:
        #     # replace the cols with the translated ones after nesting and rescale for contract
        #     df[col] = df.apply(lambda row: safe_scale_space(row, col), axis=1)
        ##################
        # pre nest we invert because how nesting works and we want to keep our our orientation to make it easir to shift back other things

        # df["outline"] = df["outline"].map(mirror_on_y_axis)
        df = nest_dataframe(
            df.reset_index(),
            geometry_column=geom_col,
            output_bounds_width=PLOTTER_WIDTH,
            buffer=BUFFER,
        )
        # invert the coefficents

        res.utils.logger.info(f"Translating other geoms by a nesting offset")

        df["max_y"] = unary_union(df["nested.outline"]).bounds[-1]
        # dealing with nesting orientation
        df["coff"] = df.apply(centroid_offset, axis=1)

        for col in [geom_col, "notch_lines", "internal_lines", "sa_notches"]:
            # replace the cols with the translated ones after nesting and rescale for contract
            df[col] = df.apply(translate_geom(col), axis=1)

        def hpol(pts, line_style=None, term="PU;"):
            # cast
            pts = np.array(pts).astype(int)
            p0 = pts[0]

            # first
            s = f"PU{p0[0]},{p0[1]};"
            # rest
            path = ",".join([f"{p[0]},{p[1]}" for p in pts[1:]])
            # compose
            if line_style:
                ls = f"LT{line_style[0]},{line_style[1]}"
                # resetting line style when we change it
                return f"{ls};{s}PD{path};{term}LT;"

            return f"{s}PD{path};{term}"

        def hline(pts, line_style=None):
            # we may need term=""
            return hpol(pts, line_style, term="PU;")

        def hpol_from_geom(g):
            return hpol(list(g.coords))

        def hline_from_geom(g, line_style=None):
            if g is None:
                return ""
            try:
                g = list(g)
            except:
                g = [g]
            lines = ""
            for l in g:
                l = hline(list(l.coords), line_style=line_style)
                if l is not None and len(l.lstrip().rstrip()) > 0:
                    lines += l + "\n"

            return lines.rstrip("\n")

        bnds = unary_union(df[geom_col]).bounds
        res.utils.logger.debug(f"Nest bounds {bnds}")
        bounds = bounds or bnds
        bds = int(bounds[-2]), int(bounds[-1])
        res.utils.logger.debug(
            f"inches*factor bounds {bds} - there are {len(df)} records"
        )

        if kwargs.get("preview_dataframe"):
            return df

        # header copied from reference and we add a text delim char $ as DT$
        header = """IN;IP0,0,1016,1016;SC0,1000,0,1000;SP1;\nDI1.000,0.000;DT$;SI0.423,0.635;LO5;"""
        lines = list(df[geom_col].map(hpol_from_geom))
        # ilines = list(df["internal_lines"].map(hpol_from_geom))
        nlines = list(df["notch_lines"].map(lambda g: hline_from_geom(g)))
        nlines += list(df["sa_notches"].map(lambda g: hline_from_geom(g)))
        # NOTE line style 3,40 taken from a reference sample
        ilines = []

        for _il in df["internal_lines"]:
            try:
                IL = geom_list(_il)
                for _line in IL:
                    ilines.append(hline_from_geom(_line))
                    # res.utils.logger.debug("adding some internal lines")
            except Exception as ex:
                res.utils.logger.warn(f"Failing to add internal lines: {ex}")
                # raise ex

        def prune(ln):
            ls = []
            for l in ln:
                if l is None:
                    continue
                if len(l.replace("\n", "").lstrip().rstrip()) == 0:
                    continue
                ls.append(l)
                # res.utils.logger.debug(f"ADD LINE {l}")
            return ls

        nlines = prune(nlines)
        lines = prune(lines)
        ilines = prune(ilines)
        res.utils.logger.debug(f"adding {len(ilines)} internal lines")
        text_lines = []

        if kwargs.get("preview_dataframe"):
            return df

        for _, row in df.iterrows():
            c = row[geom_col].centroid
            bounds = row[geom_col].bounds
            W = bounds[2] - bounds[0]
            H = bounds[3] - bounds[1]
            # text durection depends on shape
            DIR = "DI;" if W * 3 > H else "DI0,1;"
            offset = (TXT_FACTOR, 0) if W * 3 > H else (0, TXT_FACTOR)
            res.utils.logger.debug(f"offset text {offset}")
            # need a rotational model for text positioning DI can make it 180 if piece too narrow
            text_lines.append(
                f"PU{int(abs(c.x-offset[0]))},{int(c.y-offset[1]-(TXT_FACTOR/20))};{DIR}LB{row['key']}$;"
            )

        res.utils.logger.info(f"Exporting to {filename}")

        if "s3://" not in filename:
            with open(filename, "w") as f:
                f.writelines(f"{header}\n")
                for l in lines:
                    f.writelines(f"{l}\n")
                for l in nlines:
                    f.writelines(f"{l}\n")
                for l in text_lines:
                    f.writelines(f"{l}\n")
                for l in ilines:
                    f.writelines(f"{l}\n")

        # this is a bit uncessarily not DRY but in a hurry
        else:
            with s3.file_object(filename, "w") as f:
                f.writelines(f"{header}\n")
                for l in lines:
                    f.writelines(f"{l}\n")
                for l in nlines:
                    f.writelines(f"{l}\n")
                for l in text_lines:
                    f.writelines(f"{l}\n")
                for l in ilines:
                    f.writelines(f"{l}\n")

        res.utils.logger.debug(f"finished upload")
        return df

    @staticmethod
    def export_dataframe(
        df,
        path,
        column="geometry",
        laters={1: "geometry"},
        units="METRIC",
        point_conversion_op=scale_shape_by(1),
        global_attributes_in=None,
    ):
        from ezdxf import units as ezunits

        d = res.utils.dates.utc_now()

        global_attributes = {
            "STYLE NAME": "UNKOWNN",
            "CREATION DATE": d.strftime("%m/%d/%Y"),
            "CREATION TIME": d.strftime("%H:%M:%S"),
            "AUTHOR": "Resonance Companies",
            "SAMPLE_SIZE": "X",
            "UNITS": "METRIC",
            "QUANTITY": 1,
        }

        if global_attributes_in is not None:
            global_attributes.update(global_attributes_in)

        doc = ezdxf.new("R12")
        # metric milli meters
        doc.units = ezunits.MM
        msp = doc.modelspace()

        if "key" not in df.columns:
            # this is a safety net - the DXF should have some meainful key
            df["key"] = list(range(len(df)))
            df["key"] = df["key"].map(lambda x: f"key_{x}")

        for contract_col in ["size", "type", "category"]:
            if contract_col not in df.columns:
                df[contract_col] = "EMPTY"

        for _, row in df.iterrows():
            b = doc.blocks.new(name=row["key"])
            att = {
                "PIECE NAME": row["key"],
                "CATEGORY": row["category"],
                "ANNOTATION": "NO ANNOT",
                "SIZE": row["size"],
                "res.type": row["type"],
            }
            att.update(global_attributes)

            for k, v in att.items():
                t = b.add_text(f"{k}: {v}")
                t.dxf.layer = 1

            geometry = point_conversion_op(row[column])
            points = [tuple(p) for p in np.array(geometry).astype(int)]

            e = b.add_polyline2d(points)  # the flag symbol as 2D polyline
            e.dxf.layer = 1

        if "s3://" in path:
            s3 = res.connectors.load("s3")

            with tempfile.NamedTemporaryFile(
                suffix=Path(path).suffix, prefix="f", mode="wb"
            ) as f:
                doc.saveas(f.name)
                s3.upload(f.name, path)
        else:
            doc.saveas(path)

        return doc

    def export_outlines(self, size, piece_types, path, mode="dxf", **kwargs):
        """
        from this view we are converting pixel space to metric space for the laser cutter
        we can add as many layers "geometries" as we like and the writer converts to the dxf

        the default scaler is a scaling of the 300DPI pixel space back to inches and then to milimiters i.e. divided by 300 and multiple by 25.4

        AC1009 	AutoCAD R12

        blocks :
        https://ezdxf.readthedocs.io/en/stable/dxfinternals/block_management.html

        set units https://ezdxf.readthedocs.io/en/stable/concepts/units.html

        We can create very special cutlines for special physical notches and we can nest etc with pandings and all the rest
        We can do etchings as special layers too
        """

        df = self.get_sized_geometries(
            size=size, piece_types=piece_types, warn_on_empty_set=False
        )
        if len(df) == 0:
            res.utils.logger.debug(
                f"There are no pieces in the filter set to export - check size {size} and types {piece_types} for intent"
            )
            return

        if mode == "hpgl":
            res.utils.logger.info(
                f"Export mode is hpgl - there are {len(df)} pieces to export"
            )
            return self.export_to_hpgl(path, df, **kwargs)

        return DxfFile.export_dataframe(df, path)

    @staticmethod
    def export_res_color(r):
        """
        doing some to-strings here that we may change later

        the export geom columns are all converted to image space but some that we derived can already be in image space
        this is a bit confusing but we need to make things back comptabile in a few place
        """
        # an explicit json dumps might be best - lets see
        # CONTRACT: We must be very careful here as we are deciding what gets exported to the database which can cause confusion
        # it is good during development to track (surface area) though so going to leave it for now
        res_color_export = [
            "length",
            "angle",
            "label_corners",
            "piece_bounds",
            "outline",
            "viable_surface",
            "geometry_internal_lines",
            "fold_aware_edges",
        ]
        return {
            k: str(v) if not pd.isnull(v) else None
            for k, v in r.items()
            if k in res_color_export
        }

    @staticmethod
    def _process_geoms_for_export(
        data,
        geom_columns=[
            "notches",
            "internal_lines",
            "viable_surface",
            "stripe",
            "plaid",
            "sew_lines",
        ],
        keep_columns=None,
        units="mm",
    ):
        def scale_sa(l):
            scaler = 300  # DPI
            if units == "mm":
                scaler *= 0.393701  # scale for mm from inches default
            try:
                # this does not fail for any valid reason
                return [item * scaler for item in l]
            except:
                return l

        data = data.where(pd.notnull(data), None)

        add_geoms = [d for d in geom_columns if d in data.columns]
        keep_columns = keep_columns or []
        data = data[
            [
                "key",
                "type",
                "piece_code",
                "piece_name",
                "piece_key_no_version",
                "size",
                "seam_allowances",
                "outline",
                "geometry",
                "corners",
                "edges",
            ]
            + add_geoms
            + keep_columns
        ].rename(columns={"piece_code": "code"})
        for k in add_geoms:
            allow_out_of_bounds = k in ["plaid", "stripe"]
            if k in data.columns:
                # empty point defualt
                try:
                    data[k] = data[k].fillna(Point())
                except:
                    # TODO - remove this trying to understand this case for all geom types
                    pass
                data[k] = data.apply(
                    lambda row: geometry_to_image_space(
                        row,
                        k,
                        allow_out_of_bounds=allow_out_of_bounds,
                        source_units=units,
                    ),
                    axis=1,
                )
            # for now we use string - we need 3d points on dgraph does not support
            data[k] = data[k].map(lambda g: None if pd.isnull(g) else str(g))

        # this is a shitty abstraction but we started not scaling these but we still want to to-string them
        for k in ["corners", "edges", "geometry"]:
            data[k] = data[k].map(lambda g: None if pd.isnull(g) else str(g))

        data["seam_allowances"] = data["seam_allowances"].map(scale_sa)

        # after scaling this is redundant
        return data.drop("outline", 1)

    def export_body(
        self,
        sizes=["SM"],
        piece_types=PRINTABLE_PIECE_TYPES,
        provider=None,
        meta_name="meta.bodies",
        plan=False,
        **kwargs,
    ):
        """
        Export body and piece piece geometry inforamtin from the ASTM file
        All the resonance logic for unfolding and mirroring etc is applied as the res color is generated
        We create all the geomtries that are used downstream e.g. for nesting.
        we export these to some datastore...

        We will evolve how we buld the graph but queries ;ike the following can retrieve body pieces by body key

          query{
                get(func: type(meta.body_pieces))
                @filter(eq(body_key, "CC-6001-V9"))
                {
                expand(_all_){

                }
                }
            }

         #the geometries can then be parsed
        """
        if provider == None:
            provider = res.connectors.load("dgraph")

        def outline_format(g):
            """
            TODO: unit test contract - we should be stricy on linear rings instead of polygons for outlines
            """
            return str(Polygon(g).exterior)

        def _compile_res_color(row):
            """
            this will become a proper entity in the graph later
            """
            return str(
                {
                    "outline": row["outline"],
                    "viable_surface": row["viable_surface"],
                    "internal_lines": row.get("internal_lines"),
                }
            )

        if sizes is None:
            logger.warning(f"no size supplied - exporting all sizes {self.sizes}")
            sizes = self.sizes

        for size in sizes:
            logger.info(f"exporting size {size}")
            data = self.get_sized_geometries(size=size, piece_types=piece_types)
            # map to meta.body_pieces schema

            if len(data) == 0:
                raise Exception(f"There are no valid geometries for size {size}")

            logger.warning(
                f"exporting meta-one geometries using source units {self.units} in the DXF. Will convert to pixels/image space"
            )

            data = DxfFile._process_geoms_for_export(data, units=self.units)

            # merge the child pieces and header
            body = self.body_header
            as_dict = body.iloc[0]
            # playing around with how we organise types - is the body unique with version and size or what?
            as_dict["key"] = f"{as_dict['key']}-{size}"
            # the inches level outline is not needed
            data = data.drop("outline", 1)
            # playinf with different ways of making rels
            data["body_code"] = as_dict["body_code"]
            # playing around with how we organise types - is the body unique with version and size or what?
            # here we use the body key from the parent with the size and version qualification
            data["body_key"] = as_dict["key"]

            as_dict["body_pieces"] = data.to_dict("records")

            body = pd.DataFrame([as_dict])

            if plan:
                return body

            # nesting update to do - also we are not adding reverse edge to body_code from pieces
            provider[meta_name].update(
                body,
                child_field="body_pieces",
                child_meta_type="meta.body_pieces",
                parent_fk_name="body_key",
            )

    def export_body_specs(
        self,
        colors,
        materials,
        sizes,
        piece_types=PRINTABLE_PIECE_TYPES,
        provider=None,
        meta_name="make.one_specification",
        plan=False,
        one_spec_suffix="00001",
        **kwargs,
    ):
        """
        Similar to export bodies, this export option projects the body/pieces against material and color
        Is just a way to use the body source of truth to generate specs' for pieces that can be used in make
        This is exported to a datastore and used as the primary data record for Make

        sizes should be a dictionary for now mapping resMake Size (Gerber) to resSell size ('accounting' e.g. zzz01)
        """
        if provider == None:
            provider = res.connectors.load("dgraph")

        res.utils.logger.debug(f"Exporting body spec for mapped sizes {sizes}")

        for size, res_sell_size in sizes.items():
            data = self.get_sized_geometries(size=size, piece_types=piece_types)
            data = DxfFile.project_body_specs(data, colors=colors, materials=materials)
            # here we map to the make.body_pieces schema
            data = dataframes.rename_and_whitelist(
                data,
                {
                    "res.key": "body_piece_key",
                    "material_code": "material_code",
                    "color_code": "color_code",
                },
            )

            # find some way to generate a key for this similar to spec - this could be just a hash
            suffix = "00000"
            # the key suffix for this piece for now goes with the one spec even though these pieces could be shared
            if kwargs.get("key"):
                suffix = kwargs.get("key").split("-")[-1]
            data["key"] = data["body_piece_key"].map(lambda x: f"{x}-{suffix}")
            # the body header is similar for mapping to meta bodies or one spec

            # merge the child pieces and header
            body = self.body_header
            as_dict = body.iloc[0]
            as_dict["body_pieces"] = data.to_dict("records")
            body = pd.DataFrame([as_dict])
            # we will generate some spec from info on the spec projection incouding customs
            # we will first keep the actual body key as a lookup to the meta body
            body["body_key"] = body["key"]
            # TODO: We're making a very specific decision here about One Spec Keys that needs to be managed
            if "key" in kwargs:
                body["key"] = kwargs.get("key")
            else:
                # TODO we need to support the one-spec template properyl
                # but for now we will call this function for each size so this for loop is not really used
                body["key"] = body["key"].map(
                    lambda k: f"{k}-{res_sell_size.upper()}-{one_spec_suffix}"
                )

            keys = list(body["key"].unique())
            logger.debug(f"saving one spec key(s): {keys}")

            if plan:
                return body

            # I added the ONE-spec key in the schema but i should not really need that if i use reverse edges properly
            # TODO update with group by nested-header
            provider[meta_name].update(
                body,
                child_field="body_pieces",
                child_meta_type="make.body_pieces",
                parent_fk_name="one_specification_key",
            )

            return body

    def export_cut_files(
        self,
        acc_size,
        gerber_size,
        res_size,
        body_version,
        clear_existing=False,
        try_post_attachment=False,
    ):
        lower_acc_size = acc_size.lower()
        # e.g. k2_2011/v4
        body_code = self.code.replace("-", "_").lower()
        body_version = str(body_version).lower().replace("v", "")
        body_code_for_path = f"{body_code}/v{body_version}"

        # Body code + Version + Size + Type (Stamper or fusible -X or -F)
        file_key = (
            f"{body_code.upper().replace('_','-')}-V{body_version}-{res_size}".upper()
        )
        # save the cut files
        cut_meta_path = f"s3://meta-one-assets-prod/bodies/cut/{body_code_for_path}"
        size_path = f"{cut_meta_path}/{lower_acc_size}"
        remove_files = list(size_path)
        if (
            clear_existing
            and body_code_for_path is not None
            and len(remove_files) < 100
        ):
            for f in res.connectors.load("s3").ls(size_path):
                res.utils.logger.debug(f"removing {f}")
                res.connectors.load("s3")._delete_object(f)

        # if not s3.exists(f"{cut_meta_path}/{lower_acc_size}/fuse_pieces.dxf"):
        res.utils.logger.info(
            f"Exporting body size cut pieces for fuse for size {gerber_size}:{lower_acc_size} - normed size {res_size}"
        )

        try:
            self.export_outlines(
                gerber_size,
                piece_types=["fuse"],
                path=f"{size_path}/fuse_{file_key}-F.dxf",
            )
            self.export_outlines(
                gerber_size,
                piece_types=["fuse"],
                path=f"{size_path}/fuse_{file_key}-F.plt",
                mode="hpgl",
            )

            res.utils.logger.info(
                f"Exporting body size cut pieces for fuse and stamper for size {gerber_size}:{lower_acc_size}"
            )

            stamper_path = f"{size_path}/stamper_{file_key}-X.plt"
            stampers = self.export_outlines(
                gerber_size,
                piece_types=["stamper"],
                path=stamper_path,
                mode="hpgl",
            )
            if try_post_attachment:
                res.utils.logger.info("Posting attahcment to Airtable body")
                upload_plt_file_urls_to_body(path=cut_meta_path)
        except Exception as ex:
            res.utils.logger.warn(f"Failed to export pieces  : {repr(ex)}")
            return False
        return True

    def export_meta_marker(
        self,
        sizes,
        res_size_map,
        color_code,
        piece_material_map,
        piece_color_map,
        material_props,
        # this should maybe required
        metadata=None,
        # optional fields
        color_pieces_root="s3://meta-one-assets-prod/color_on_shape",
        meta_marker_root="s3://meta-one-assets-prod/styles/meta-marker",
        save=True,
        key=None,
        meta_one_version=None,
        # the piece path suffix is the one chosen by the caller or we default it -> unit test
        pieces_path_suffix=None,
        use_size_in_piece_name=False,
        validation_result=None,
    ):
        """
        s3://meta-one-assets-prod/color_on_shape/tk_6077/v6/artnkd/3zzmd/named_pieces/

        sizes={gerber_size: asset["size"]},
        materials can be any of a constant, or piece material map
        for now assume we have a key with default and/or piece codes mapping to materials

        we can add one spec and material props as part of gating the marker

        pieces  path suffix is to use a new convention to add pieces:> this is for testing primarily to create meta marker assets in other places like data lake test locations
        """

        # hack to infer if the path is bad
        if "/None" in str(pieces_path_suffix):
            pieces_path_suffix = None

        s3 = res.connectors.load("s3")

        body_version = self.body_version.lower()
        res.utils.logger.info(f"Using pieces path in dfx fn {pieces_path_suffix}")
        # if we have not yet validated the body version on the astm we can maybe trust the upload path

        def determine_file_path(key, offset, color_pieces_path):
            return (
                f"{color_pieces_path}/{key}.png"
                if not offset
                else f"{color_pieces_path}/{int(offset)}/{key}.png"
            )

        def check_number(x):
            """
            floating value in inches will later be mapped to an integer scalled by DPI
            """
            return float(x) if not pd.isnull(x) else 0

        if metadata:
            mode = metadata.get("flow_unit_type")
            check_body_version_meta = metadata.get("path_meta")
            if check_body_version_meta:
                body_version = check_body_version_meta.split("/")[-1].split("_")[3]

        for gerber_size, acc_size in sizes.items():
            res_size = res_size_map.get(acc_size)
            version = body_version

            if meta_one_version:
                # this is important but experimental: we are versioning the meta one using a hash of some kind with the body version for readability
                version = f"{body_version}_{meta_one_version}"
            meta_one_suffix = f'{self.code.replace("-","_")}/{version}/{color_code}/{acc_size}'.lower()

            bvcs_path = f'{self.code.replace("-","_")}/{body_version}/{color_code}/{acc_size}'.lower()

            color_pieces_path = pieces_path_suffix
            if pieces_path_suffix is None:
                color_pieces_path = f"{color_pieces_root}/{bvcs_path}/pieces"

            # spaces are not allowed
            meta_marker_path = f"{meta_marker_root}/{meta_one_suffix}".replace(" ", "_")

            # check
            pieces = self.get_sized_geometries(
                gerber_size, piece_types=PRINTABLE_PIECE_TYPES
            )

            res.utils.logger.info(f"Saing pieces as printable {pieces['key']}")

            if len(pieces) == 0:
                raise Exception(
                    f"There are no relevant geometries to export from DXF in size {gerber_size} - the relavant types are {PRINTABLE_PIECE_TYPES}"
                )

            pieces["piece_key"] = pieces["key"].map(DxfFile.remove_size)
            piece_key = "key" if use_size_in_piece_name else "piece_key"

            pieces = DxfFile.project_body_specs(
                pieces, piece_color_map, piece_material_map
            )
            pieces["material_offset_size"] = pieces["material_code"].map(
                lambda x: check_number(material_props.get(x, {}).get("offset_size", 0))
                * DPI_SETTING
            )

            pieces["filename"] = pieces.apply(
                lambda row: determine_file_path(
                    row[piece_key], row["material_offset_size"], color_pieces_path
                ),
                axis=1,
            )

            logger.warning(
                f"exporting meta-one geometries using source units {self.units} for size {gerber_size} in the DXF. Will convert to pixels/image space"
            )

            # a validation would be for the filename -> we could do it here but the meta one should do it but then we need to test the cotract here for
            # -filename
            # -??

            # Body code + Version + Size + Type (Stamper or fusible -X or -F)

            if save:
                res.utils.logger.info(
                    f"exporting printable pieces for size {gerber_size}"
                )
                # the gerber size must be passed in because otherwise we use the sample size which is sometimes wrong if we have multiples
                self.export_printable_pieces(size=gerber_size)

                # disable this legacy way of saving them and do in the new way
                # cut_status = self.export_cut_files(
                #     acc_size=acc_size,
                #     gerber_size=gerber_size,
                #     res_size=res_size,
                #     body_version=body_version,
                # )
                cut_status = True

                if metadata:
                    metadata["cut_status"] = cut_status
                # currently this is the path for meta objects such as sew instructions
                # res_meta = f"s3://meta-one-assets-prod/bodies/{body_code_for_path}/res_meta/construction/{self.body_version.lower()}"
                #

                res.utils.logger.info(f"saving meta marker to {meta_marker_path} ")
                # just the color info
                pieces = pieces[
                    [
                        "key",
                        "piece_key",
                        "filename",
                        "material_code",
                        "color_code",
                        "material_offset_size",
                    ]
                ]

                s3.write(
                    f"{meta_marker_path}/pieces.feather",
                    pieces.reset_index().drop("index", 1),
                )

                # res.utils.logger.info("importing any meta objects for body version")
                # # import the body meta objects
                # for f in s3.ls(res_meta):
                #     filename = f.split("/")[-1]
                #     if filename is not None and filename != "":
                #         res.utils.logger.info(f"import {f}")
                #         s3.write(f"{meta_marker_path}/{filename}", s3.read(f))

                # upsert the meta object
                meta = {}
                mpath = f"{meta_marker_path}/meta.json"
                if s3.exists(mpath):
                    meta = s3.read(mpath)
                meta.update(
                    {
                        "sizes": sizes,
                        "piece_material_mapping": piece_material_map,
                        "color_code": color_code,
                        "key": key,
                        "material_props": material_props,
                        # TODO see full contract e.g. validations
                        "validation_result": validation_result,
                        "cut_status": cut_status,
                        "metadata": metadata,
                        "invalidated_date": None,
                        "last_modified": dates.utc_now_iso_string(),
                    }
                )
                s3.write(mpath, meta)

                # ensure no spaces in path - reare but possible
                return meta_marker_path.replace(" ", "_")
                # we should merge the meta marker into meta.json

    def _get_res_piece_names(self):
        """
        This should just be the codes from the astm but there are some stray conventions fow now
        """
        piece_names = list(
            self.get_sized_geometries(size=self.sample_size)["piece_name"]
        )

        def part(p):
            ps = p.split("-")
            for i, v in enumerate(ps):
                if v[0] == "V":
                    break
            return "-".join(ps[i + 1 :])

        piece_names = [part(p) for p in piece_names]

        return piece_names

    def transform(self, transformation, **kwargs):
        """
        Return a new dxf file with the same attributes but geometries transformed by the op

        This can be used to create new shape files eg. dxf_file.transform(op).write(path)

        """

    def transform_pieces(self, transformations, **kwargs):
        """
        Return a new dxf file with the same attributes but geometries transformed by the ops
        In this case a different transformation is applied to each pieces layers. For example after nesting we could move the shapes to new locations and the write dxf

        This can be used to create new shape files eg. dxf_file.transform_pieces(ops).write(path)

        TODO: play with transformations ... are they inline?
        https://ezdxf.readthedocs.io/en/stable/math.html#transformation-classes

        """

    @staticmethod
    def from_dataframe(df, **kwargs):
        """
        given a dataframe of geometries and attributes in some convention, and passing some extra kwargs, create a dxf file
        For example if we nest some shapes we can create a dxf file of the nesting. Note however, calling a transform on all shapes on the layers works too

        """
        return None

    @staticmethod
    def get_segments(ml, origin="top_left"):
        """
        TODO: split out straight lines with two points

        Get the feasible line segments dataframe
        Anything that is longer than a buffered label and based on a rank (near center of mass)
        We pick any optimal line and we align the label parallel to it in the seam allowance

        From the angle -> clockwise we must find an offset x,y along the tangent into the shape
        The label is placed here at the same angle as the line
        """

        def try_ntype(g):
            """a mapping from a z coord to a notch type; two supported for now see how we map in the fold lines"""
            try:
                return np.array(g)[:, -1].min()
            except:
                return 0

        def _iter(ml, all_points=[]):
            for l in ml:
                cc = list(l.coords)
                all_points += list(cc)

                if len(cc) == 2:
                    yield l
                else:
                    # unpack them - later we can do a trick to extend if they happened to be parallel
                    for i, p in enumerate(l.coords):
                        if i > 0:
                            yield LineString([l.coords[i - 1], p])

        all_points = []
        df = pd.DataFrame(_iter(ml, all_points), columns=["geometry"])
        df["notch_type"] = df["geometry"].map(try_ntype)

        all_points = np.array(all_points)
        # top is based on the DXF crs
        top_left = [int(all_points[:, 1].min()), int(np.ceil(all_points[:, 0].max()))]
        bottom_left = [
            int(all_points[:, 1].min()),
            int(np.ceil(all_points[:, 0].min())),
        ]
        # top right is the top left when rotated onto the x axis
        top_right = [int(all_points[:, 1].max()), int(np.ceil(all_points[:, 0].max()))]
        # merge almost straight consecutive lines segments
        df["length"] = df["geometry"].map(lambda x: x.length)
        df["centroid"] = df["geometry"].map(lambda x: x.centroid)
        origin = top_right  # bottom or top left depending on y axis mode tbd
        df["origin"] = str(origin)
        df["distance_from_origin"] = df["centroid"].map(
            lambda pt: np.linalg.norm(np.array([pt.x, pt.y]) - np.array(origin))
        )

        return df.sort_values("distance_from_origin")

    def get_surface_without_notches(
        self,
        piece_key,
        notch_buffer_inches=DEFAULT_NOTCH_BUFFER,
        # outline_layer=OUTLINE_KEYPOINT_LAYER,
        # notch_layers=[4, 80, 81, 82, 83],
        shift_to_origin=True,
        angle_threshold=150,
        # this is a low level func and maybe not one we want to transform
        apply_crs_transformation=None,
        mirrored=False,
    ):
        """
        [DEPRECATE in favour of the static method used in the res layer]
        provide the res-color surface on which labels etc are applied
        we read layers with unfolding and mirroring in the raw crs and optionally shift to the origin at the end strictly
        all geometries fetched from get_piece_x are in the same CRS
        """
        # TODO change this to get the outline once in a consistent way
        outline = self.get_piece_outline(piece_key, mirrored=mirrored)
        corners = polygon_vertex_angles(outline, less_than=angle_threshold)
        # determine segments here over the keypoints and find if they have certain notch types on them
        notches = self.get_piece_notches(piece_key, mirrored=mirrored)
        # get differences of geomtries to remove notices from viable locations

        # get fold edges
        # segments = geo_line_segments(corners)
        # which one of these overlaps with notches that have negative z

        logger.debug(
            f"Will be removing {len(corners)} sharp edges from the candidate surface"
        )

        # get all the corner points with buffer and treat them the same as nothes
        # the notch buffer can actually be scaled here as a function of the angle . the more acute the angle, the further away we need to be
        CP = unary_union(
            [
                Point(corner_angles[0]).buffer(notch_buffer_inches)
                for corner_angles in corners
            ]
        )
        MP = unary_union([p.buffer(notch_buffer_inches) for p in notches])
        ML = outline.boundary

        # after =shapely.ops.orient(before, sign=1.0)
        # is_ccw <- for liner rings at least
        # make segments dataframe with metadata from the diff -> easily reversed
        ML = orient(ML - MP - CP, sign=-1)

        # we apply transformations here on the surface which is the thing from which res color is generated
        # this is important because we can observe in the pipeline that no layers need to be at the origin
        # this is important gate as we have orientated the surface segments, at the origin CRS in a specific sense in one place

        if shift_to_origin:
            ML = shift_geometry_to_origin(ML)

        if apply_crs_transformation == "rotate_onto_x_axis":
            ML = rotate_onto_x_axis(ML)

        return ML

    def get_viable_piece_surface_label_locations_inches(
        self,
        piece_key,
        notch_buffer_inches=DEFAULT_NOTCH_BUFFER,
        # the minimal outline layer
        outline_layer=OUTLINE_KEYPOINT_LAYER,
        length_threshold_inches=1.0,
        notch_layers=[4, 80, 81, 82, 83],
        mirrored=False,
        plot=False,
        **kwargs,
    ):
        """
        Example files and piece key
        's3://meta-one-assets-prod/bodies/cc_4047/pattern_files/body_cc_4047_v2_pattern.dxf
        'CC-4047-V2-FT_SM'
        """

        ML = self.get_surface_without_notches(
            piece_key,
            notch_buffer_inches=notch_buffer_inches,
            outline_layer=outline_layer,
            notch_layers=notch_layers,
            mirrored=mirrored,
            # apply_crs_transformation=apply_crs_transformation,
        )

        # what is important here is that the application of the surface and viability is invariant to any transforamtions
        # the choice of segment should be the same

        ml_df = DxfFile.get_segments(ML)
        ml_df["viable"] = ml_df["length"].map(lambda x: x > length_threshold_inches)

        if plot:
            from geopandas import GeoDataFrame

            return GeoDataFrame(ml_df, columns=["geometry"]).plot(figsize=(15, 15))

        return ml_df

    @staticmethod
    def get_label_width_height_from_sample(label_text, **kwargs):
        label = get_one_label(label_text)

        # the disadvantage of making the autobound too big is we will fail to find a viable location on the surface
        width_in = kwargs.get("width_in", label.shape[1] / DEFAULT_DPI)
        height_in = kwargs.get("height_in", label.shape[0] / DEFAULT_DPI)

        return width_in, height_in

    def get_piece_label_placement(
        self,
        piece_key,
        label_text="1000000",
        notch_buffer_inches=DEFAULT_NOTCH_BUFFER,
        # we can offset for example to place inside a cutline
        offset_distance=DEFAULT_BOUNDARY_PADDING,
        used_segments=None,
        shift_to_origin=True,
        test_return_viable_segments=False,
        **kwargs,
    ):
        """
        AT THE ORIGIN IN INCHES
        Not in image space

        Used segments can be passed in and removed from the viable list allowing this function to be called multiple times
        for new label locations

        #link to graphic in docs that explains the geometry

        Parallel offset is used https://shapely.readthedocs.io/en/latest/manual.html#object.parallel_offset

        The side parameter may be ‘left’ or ‘right’. Left and right are determined by following the direction of the given geometric points of the LineString. Right hand offsets are returned in the reverse direction of the original LineString or LineRing, while left side offsets flow in the same direction.
        BUT BUT BUT if offset_distance is 0 it returns the original vector so the switch is not made therefore be careful  about only using it with non zero offset

        The viable places on the surface depends very much on the notch buffer as well as the label length
        the notch ones is more confusing to track because it determines the size of small viable segments that we create

           # this used to be called piece_key but im changing it to be the res.key
        piece_key = row["res.key"]

        # generate the labels to know the dims for some label value in the data
        label = get_piece_label(row.get("res.asset_key", "0000000"))

        # the disadvantage of making the autobound too big is we will fail to find a viable location on the surface
        width_in = row.get("width_in", label.shape[1] / DEFAULT_DPI)
        height_in = row.get("height_in", label.shape[0] / DEFAULT_DPI)

        width=1.35,
        height=0.15,

        Pass height or width for test from kwargs or generate from a sample label


        """

        def get_offset_line(d, offset_amount, geometry="geometry"):
            """
            it is important to know the orientation of the polygon - we should assert something on the way in and use this
            """
            return (
                LineString(
                    reversed(
                        list(d[geometry].parallel_offset(offset_amount, "right").coords)
                    )
                )
                if offset_amount > 0
                else d[geometry]
            )

        # this is fetched for a bounds check only
        piece_outline = self.get_piece_outline(
            piece_key, shift_to_origin=shift_to_origin, swap_axes=False
        )

        # if apply_crs_transformation == "rotate_onto_x_axis":
        #     piece_outline = rotate_onto_x_axis(piece_outline)

        # get a list of surface segments that are sorted and tagged as viable
        data = self.get_viable_piece_surface_label_locations_inches(
            piece_key,
            outline_layer=OUTLINE_KEYPOINT_LAYER,
            notch_buffer_inches=notch_buffer_inches,
            used_segments=used_segments,
            # apply_crs_transformation=apply_crs_transformation,
        )

        assert (
            len(data) > 0
        ), "Unable to get viable segments. The label width is too big with respect to surface line segments"

        data = data.reset_index()
        seam_allowance = None
        notch_height = None
        offset_distance = (
            offset_distance
            if offset_distance is not None
            else (seam_allowance or notch_height)
        )

        # TODO add the biggest segment to this assetion message for context
        largest_segment = data["length"].max()

        width, height = DxfFile.get_label_width_height_from_sample(
            label_text=label_text, **kwargs
        )

        assert (
            len(data[data["viable"]]) > 0
        ), f"No viable line segment for label of size ({width},{height}) on piece {piece_key}. The largest segment out of {len(data)} we have is {largest_segment}"

        # get the first of the viable segments
        d = dict(data[data["viable"]].iloc[0])
        v = list(d["geometry"].coords)
        # i think this works for clockwise
        x1, y1, x2, y2 = v[0][0], v[0][1], v[1][0], v[1][1]
        # and this for anti
        # x2, y2, x1, y1 = v[0][0], v[0][1], v[1][0], v[1][1]
        # the angle of the line at which the label will be placed
        d["angle"] = np.rad2deg(np.arctan2(y2 - y1, x2 - x1))

        d["label_size"] = (width, height)
        logger.debug(
            f"angle in this CRS for placement on {v} is {d['angle']} - placing a line of length {width} on a segment of size {d['geometry'].length}"
        )

        pline = get_offset_line(d, offset_distance)
        mline = get_offset_line(d, offset_distance + height / 2.0)
        lline = get_offset_line(d, offset_distance + height)

        d["piece_bounds"] = piece_outline.bounds
        # assert pline is inside the shape or raise exception
        # get the opposite end from the right parallel- we reversed the line above should this should be going clockwise
        d["notch_height"] = notch_height
        d["seam_allowance"] = seam_allowance
        d["height"] = height

        d["label_bottom_line"] = lline
        # the label is placed horizontally here
        d["offset_line_start"] = pline.coords[0]
        # d["label_center_point"] = mline.interpolate(width / 2.0)
        # the color sampling area is where the label will be placed along pline
        d["color_sampling_mask_region"] = mline.buffer(height / 2.0)

        # the horizontal top of label - if we know this we can rotate it to get the translation of the first point
        # the label is parallel to the line segment geometry interpolated to the length of the label
        # here we use either the first or last line points ----->    <------ for clockwise or anti clockwise lines

        # 0 or 1 for the xline coords depends on the sense of the polygon!!!!!!
        # the label other end will be interpolated in the right way though
        label_top_end_point = pline.interpolate(width / 1.0)
        label_top = LineString([pline.coords[0], label_top_end_point])

        label_bottom_end_point = lline.interpolate(width / 1.0)
        label_bottom = LineString([lline.coords[0], label_bottom_end_point])

        # reverse the bottom line so this becomes a polygon
        d["label_corners"] = MultiPoint(
            list(label_top.coords) + list(label_bottom.coords)
        )

        mline_outside_piece = piece_outline.contains(mline)

        if not mline_outside_piece:
            message = f"the mid placement line {mline} for piece {piece_key} is outside the shape with bounds {piece_outline.bounds} suggesting an invalid CRS somewhere"
            logger.warn(message)
            if not test_return_viable_segments:
                raise Exception(message)

        for p in d["label_corners"]:
            # assuming that the piece and corners are in the same reference frame
            if not piece_outline.contains(p):
                message = f"The corner at {p} is not contained within the piece polygon for piece {piece_key} with bounds {piece_outline.bounds}"
                logger.warn(message)
                if not test_return_viable_segments:
                    raise Exception(message)

        if test_return_viable_segments:
            data = pd.concat(
                [
                    data,
                    pd.DataFrame(
                        [
                            # {"geometry": d["label_corners"], "color": "black", "size": 1},
                            {
                                "geometry": d["label_bottom_line"],
                                "color": "black",
                                "size": 1,
                            }
                        ]
                    ),
                ]
            )
            return data

        d["label_top"] = label_top

        return d

    def get_named_piece_image_outlines(
        self,
        size=None,
        piece_types=PRINTABLE_PIECE_TYPES,
        use_notched_outline=True,
        block_fuse_buffer=None,
        material_props=None,
        remove_size_suffix=True,
    ):
        """
        Return the named piece outlines in the same CRS and orientation as image outlines would be extracted

        #FOR two 2D gerber files at least this makes sense

        """
        pcs = self.get_sized_geometries(
            size=size, piece_types=piece_types, on_fail_derived="ignore"
        )

        def try_notched_outline(row):
            try:
                return dxf_notches.notched_outline(row)
            except:
                return row["geometry"]

        def add_material_buffer(props):
            """
            for any piece code we can attribute a material buffer in a lookup
            """

            def f(row):
                # category may not be it in general but something like res.piece_name
                b = props.get(row["res.category"])
                return row["geometry"].buffer(b).exterior if b else row["geometry"]

            return f

        if use_notched_outline:
            logger.info(f"Using notched outlines to do the piece naming")
            # i may only want to do this for symmetrical pieces
            pcs["geometry"] = pcs.apply(try_notched_outline, axis=1)

        if material_props:
            logger.info(f"Using material props to apply buffers")
            pcs["geometry"] = pcs.apply(add_material_buffer(material_props), axis=1)
        elif block_fuse_buffer:
            # experimental - roughly scale block fuse
            logger.debug(f"Scaling block fuse pieces")
            pcs["geometry"] = pcs.apply(
                lambda row: (
                    row["geometry"].buffer(block_fuse_buffer).exterior
                    if row["type"] == "block_fuse"
                    else row["geometry"]
                ),
                axis=1,
            )

        named_pieces = dict(pcs[["key", "geometry"]].values)

        if remove_size_suffix:
            remove_size = lambda k: "_".join(k.split("_")[:-1])
            named_pieces = {remove_size(k): v for k, v in named_pieces.items()}
        # return {k: swap_points(v) for k, v in named_pieces.items()}
        # not going to swap points for the new contract that uses the geometry straight into image space
        return {k: v for k, v in named_pieces.items()}

    @staticmethod
    def get_fold_aware_edges(row, axis=1):
        """
        this is a res gem op e.g. res contract on res geom piece layer populated or partially populated
        """
        try:
            edges = row["edges"]
            notches = row.get("geometry_notches")
            vsurface = row["viable_surface"]

            return DxfFile.make_fold_aware_segments(edges, notches, vsurface)
        except:
            print(f"failed to get folds on {row['res.key']}")
            return None

    def res_geometry_piece_layer(
        self, size=None, piece_types=None, outline_layer=OUTLINE_LAYER
    ):
        """
        Combine multiple layers to determine the res color geometry dict

        This needs to be tested in a static context when we have first loaded
        data from the database in the dxf format, and THEN apply the res color
        In this world we could, load a body, project an order and then apply the res color
        The res color function does not need to know the order but we want the option
        """

        def temp(row):
            """
            back tracking from a bad design - transforms need context of primary piece/geom

            """

            g = row["geometry"]
            return (
                invert_axis(row["original_geometry"], bounds=g.bounds)
                if "_RF" not in row["res.key"]
                else row["geometry_notches"]
            )

        l = self.get_transformed_layer(
            outline_layer, size=size, piece_types=piece_types
        )
        n = self.get_transformed_layer(NOTCH_LAYER, size=size, piece_types=piece_types)
        # using the res key join the notches
        l = pd.merge(
            l,
            n[["res.key", "geometry", "original_geometry"]],
            on="res.key",
            suffixes=["", "_notches"],
            how="left",
        )

        il = self.get_transformed_layer(
            INTERNAL_LINE_LAYER, size=size, piece_types=piece_types
        )
        if len(il) > 0:
            # join on internal lines
            l = pd.merge(
                l,
                il[["res.key", "geometry"]],
                on="res.key",
                suffixes=["", "_internal_lines"],
                how="left",
            )
        else:
            l["geometry_internal_lines"] = None

        # l["geometry_notches"] = l.apply(temp, axis=1)

        l["viable_surface"] = l.apply(DxfFile.get_viable_surface_geometry, axis=1)
        # fold awareness is used for image space offsets and we contractually ignore it until then in ASTM space
        l["fold_aware_edges"] = l.apply(DxfFile.get_fold_aware_edges, axis=1)
        # given an asset this could be swapped for an actual - but ref is easier and preemptive
        # TODO: if res.asset.key exists, generate this from the data and pass it in to the row app below

        label = get_one_label("0000000")
        width_in = label.shape[1] / DEFAULT_DPI
        height_in = label.shape[0] / DEFAULT_DPI
        l["res.label.width"] = width_in
        l["res.label.height"] = height_in

        try:
            # a static method is used for unit testing given a contract with geometry and geometry_notches etc.
            l["resonance_color"] = l.apply(DxfFile.get_res_geometry, axis=1)
        except Exception as ex:
            print("PROBLEM", ex)

        return l

    def get_res_geometry(
        row,
        offset_distance=DEFAULT_BOUNDARY_PADDING,
        **kwargs,
    ):
        """
        See notes from original function
        Need to check conventions for space: this is always in inches in the original frame
        Origin shifts are easy to do at the last minute

        TODO:
        1. processed data would be good for versioning
        2. more edge awareness

        """

        piece_outline = row["geometry"]
        piece_key = row.get("res.key", row.get("key"))
        # print(piece_key) #to catch culprit in row ops
        surface_geometry = row["viable_surface"]
        label_width = row["res.label.width"]
        label_height = row["res.label.height"]
        folds = row.get("fold_aware_edges")

        def get_offset_line(d, offset_amount, geometry="geometry"):
            """
            it is important to know the orientation of the polygon - we should assert something on the way in and use this
            """
            if offset_amount == 0:
                return d[geometry]
            # i thought i trusted the offset "side" but not quite sure if its robust for lines or else i misunderstand it
            l = LineString(
                reversed(
                    list(d[geometry].parallel_offset(offset_amount, "right").coords)
                )
            )
            # so we do a hit test and take the other one
            if not piece_outline.contains(l):
                l = d[geometry].parallel_offset(offset_amount, "left")
            return l

        # this is fetched for a bounds check only

        data = DxfFile.get_segments(surface_geometry)
        data["viable"] = data["length"].map(lambda x: x > label_width)

        assert (
            len(data) > 0
        ), "Unable to get viable segments. The label width is too big with respect to surface line segments"

        # data = data.reset_index()
        seam_allowance = None
        notch_height = None
        offset_distance = (
            offset_distance
            if offset_distance is not None
            else (seam_allowance or notch_height)
        )

        # TODO add the biggest segment to this assetion message for context
        largest_segment = data["length"].max()

        assert (
            len(data[data["viable"]]) > 0
        ), f"No viable line segment for label of size ({label_width},{label_height}) on piece {piece_key}. The largest segment out of {len(data)} we have is {largest_segment}"

        # get the first of the viable segments
        d = dict(data[data["viable"]].iloc[0])

        if d.get("notch_type") == -0.1:
            # add something for folds - todo: what is the value
            # we have determine here that the segments sits on a fold line somehow
            offset_distance += 0.2

        d["offset_distance"] = offset_distance

        d["errors"] = []
        v = list(d["geometry"].coords)
        # i think this works for clockwise
        x1, y1, x2, y2 = v[0][0], v[0][1], v[1][0], v[1][1]
        # and this for anti
        # x2, y2, x1, y1 = v[0][0], v[0][1], v[1][0], v[1][1]
        # the angle of the line at which the label will be placed
        d["angle"] = np.rad2deg(np.arctan2(y2 - y1, x2 - x1))

        # useful debug message but supressing for now
        message = f"angle in this CRS for placement on {v} is {d['angle']} - placing a line of length {label_width} on a segment of size {d['geometry'].length}"

        d["info"] = message

        pline = get_offset_line(d, offset_distance)
        mline = get_offset_line(d, offset_distance + label_height / 2.0)
        lline = get_offset_line(d, offset_distance + label_height)

        d["piece_bounds"] = piece_outline.bounds
        # assert pline is inside the shape or raise exception
        # get the opposite end from the right parallel- we reversed the line above should this should be going clockwise
        d["notch_height"] = notch_height
        d["seam_allowance"] = seam_allowance
        d["height"] = label_height

        d["label_bottom_line"] = lline
        # the label is placed horizontally here
        d["offset_line_start"] = pline.coords[0]
        # d["label_center_point"] = mline.interpolate(width / 2.0)
        # the color sampling area is where the label will be placed along pline
        # d["color_sampling_mask_region"] = mline.buffer(label_height / 2.0)

        # the horizontal top of label - if we know this we can rotate it to get the translation of the first point
        # the label is parallel to the line segment geometry interpolated to the length of the label
        # here we use either the first or last line points ----->    <------ for clockwise or anti clockwise lines

        # 0 or 1 for the xline coords depends on the sense of the polygon!!!!!!
        # the label other end will be interpolated in the right way though
        label_top_end_point = pline.interpolate(label_width / 1.0)
        label_top = LineString([pline.coords[0], label_top_end_point])

        label_bottom_end_point = lline.interpolate(label_width / 1.0)
        label_bottom = LineString([lline.coords[0], label_bottom_end_point])

        d["label_corners"] = MultiPoint(
            list(label_top.coords) + list(label_bottom.coords)
        )

        mline_outside_piece = piece_outline.contains(mline)

        if not mline_outside_piece:
            message = f"the mid placement line {mline} for piece {piece_key} is outside the shape with bounds {piece_outline.bounds} suggesting an invalid CRS somewhere"
            # logger.warn(message)
            # raise Exception(message)
            d["errors"].append(message)

        for p in d["label_corners"]:
            # assuming that the piece and corners are in the same reference frame
            if not piece_outline.contains(p):
                message = f"The corner at {p} is not contained within the piece polygon for piece {piece_key} with bounds {piece_outline.bounds}"
                # logger.warn(message)
                # raise Exception(message)
                d["errors"].append(message)

        d["label_top"] = label_top

        d["fold_aware_edge_bounds"] = folds.bounds if folds else None

        d["outline"] = row["outline"]
        d["viable_surface"] = row["viable_surface"]
        d["fold_aware_edges"] = folds

        d["geometry_internal_lines"] = (
            row["geometry_internal_lines"]
            if not pd.isnull(row["geometry_internal_lines"])
            else None
        )

        return d

    @staticmethod
    def make_fold_aware_segments(
        edges,
        notches,
        segments,
        buffer=0.5,
        plot=False,
        fold_distance_threshold=0.1,
        fold_notch_height=0.18,
    ):
        """
        There is information encoded in the notches that we can "transfer" to edges and then small segments on edges
        An important thing here is the offset due to folding out the fold notches bt nothch height as this dispaces the res-color on the png piece images
        when going to image space we can offset bt the bounding box due to adding these "negative" segments
        plot to see what these look like;

        eg. body piece key ="CC-3076-V3-CUFF_XSM" with res suffix e.g. (_000)

        EXAMPLE
            edges = row["edges"]
            notches = row.get("geometry_notches")
            vsurface = row['viable_surface']

        seg = make_fold_aware_segments(edges, notches, vsurface, plot=True)

        I think it is important not to exploit the offset until going to image space as these locations are just derived aesthetics

        """

        if pd.isnull(notches):
            return segments

        def has_fold_notch(g, n):
            """Do not know all notch types yet but some we care about have a neg offset"""
            inn = np.array(g.buffer(buffer).intersection(n))
            if len(inn) == 0 or inn.ndim == 0:
                return False
            if inn.ndim == 1:
                inn = np.array([list(inn)])

            if inn.ndim > 1 and inn[:, -1].min() < 0:
                return True
            return False

        edges = [e for e in edges if has_fold_notch(e, notches)]
        fold_edges = unary_union(edges)
        has_folds = not fold_edges.is_empty

        added_segements = []
        for s in segments:
            added_segements.append(tr_edge(s, False))
            # if we detect a fold add a "negative viable segement" parrallel offset from the fold some threshold
            if (
                has_folds
                and s.buffer(buffer).distance(fold_edges) <= fold_distance_threshold
            ):
                # TODO the side depends on piece orientation!!!! name convention should tell us if its reflected or not
                side = "right"
                offset = tr_edge(s.parallel_offset(fold_notch_height, side), -0.1)
                added_segements.append(offset)

        segments = added_segements

        if plot:
            from geopandas import GeoDataFrame

            df = GeoDataFrame(segments, columns=["geometry"])
            df["has_fold"] = df["geometry"].map(lambda g: np.array(g)[:, -1].max() < 0)
            df["geometry"] = df["geometry"].map(swap_points)
            df.plot(color=df["has_fold"].map(lambda g: "k" if not g else "r"))

        return unary_union(segments)

    @staticmethod
    def get_viable_surface_geometry(
        row, notch_buffer_inches=DEFAULT_NOTCH_BUFFER, angle_threshold=150
    ):
        """
        Refactored the old viable surface code
        THIS IS REALLY AN OXBOW for controlling res color so take care with this one
        """
        # oriented outline - clockwise

        outline = row["geometry"] if "geometry" in row else Polygon(row["outline"])
        notches = (
            row["geometry_notches"] if "geometry_notches" in row else row["notches"]
        )

        def edge_labeler(edges):
            """
            this is the only way i can think of to be sure that we trust the ids
            the edge id labellnng is gated and safest on a linear ring and then we can use adjacency
            the interesction of the nearest point preserves the edge id
            We can then label any other line that is near this one
            things to check are that the viable surface is away from the edges
            """

            def edge_point_near(l):
                """
                tricky - need to interpolate to find a z point
                """
                p = shapely.ops.nearest_points(l, edges)[-1]

                q = edges.interpolate(edges.project(p))
                # print(p, q)
                return q.z

            def label_line(l):
                return tr_edge(l, edge_point_near(l))

            return label_line

        outline = row["outline"]
        notches = row["notches"]

        corners = polygon_vertex_angles(outline, less_than=angle_threshold)

        # use a small edge buffer so that we can trust as much of the edge as possible for labelling
        # it is important to remove corners to get line segements
        SMALL_EDGE_BUFFER = 0.1
        CP = unary_union(
            [
                Point(corner_angles[0]).buffer(SMALL_EDGE_BUFFER)
                for corner_angles in corners
            ]
        )

        edges = row["edges"] - CP

        # re make corners with the notch buffer to keep a sricter viable surface
        CP = unary_union(
            [
                Point(corner_angles[0]).buffer(DEFAULT_NOTCH_BUFFER)
                for corner_angles in corners
            ]
        )

        ML = outline
        if pd.isnull(notches):
            ML = orient(ML - CP, sign=-1)
        else:
            MP = notches.buffer(DEFAULT_NOTCH_BUFFER)
            ML = orient(ML - MP - CP, sign=-1)

        lbl = edge_labeler(edges)

        try:
            ML = unary_union([lbl(l) for l in ML])
        except Exception as ex:
            print(row["key"], "failed on viable surface labeling")
            # raise ex

        return ML

    @staticmethod
    def project_body_specs(df, colors, materials, piece_key="res.category"):
        """
        The projection simply puts bodies on as many materials and colors as we have
        For testing just using one of each
        """

        def try_map(m):
            def f(p):
                default = m.get("default")
                try:
                    return m[p] if not default else m.get(p, default)
                except:
                    if isinstance(m, str):
                        return m
                    if isinstance(m, list):
                        return m[0]
                    if isinstance(m, dict) and len(m) == 0:
                        return list(m.keys())[0]
                raise ValueError(
                    f"Unable to map piece materials using material mapping {materials}"
                )

            return f

        try_material = try_map(materials)
        try_color = try_map(colors)

        df["material_code"] = df[piece_key].map(try_material).map(lambda s: s.upper())
        df["color_code"] = df[piece_key].map(try_color).map(lambda s: s.upper())

        return df

    # def plot_viable_label_location_geometry(
    #     self, piece_key, mirrored=False, label_width_in=None, label_height_in=None
    # ):
    #     """
    #     convenience method - get the piece polygon and just display it in the polygon
    #     """
    #     gdf = self.get_viable_piece_surface_label_locations_inches(
    #         piece_key, mirrored=mirrored
    #     )

    #     return plot_viable_location_segments(gdf)

    @property
    def file_modified_date(self):
        d = self._home_dir
        if "s3://" in d:
            return list(res.connectors.load("s3").ls_info(self._home_dir))[0][
                "last_modified"
            ]
        d = os.path.getmtime(d)
        return datetime.fromtimestamp(d)

    def save_piece_samples(
        self, path, size, pieces_types=PRINTABLE_PIECE_TYPES, color=None, **kwargs
    ):
        """
        Generate images that could be used to nest
        TODO: we need to use buffers on unstable materials b
        """
        from pathlib import Path

        from skimage.io import imsave

        force = kwargs.get("force")
        override_newer = kwargs.get("override_newer", False)
        # my own modified timestamp is used unless we pass in something else to determine the change ref point
        # this is used for example to see if the images below which should be generated from this file are older than this file
        timestamp = kwargs.get("meta_updated_at", self.file_modified_date)
        outlines = self.get_sized_geometries(size=size, piece_types=pieces_types)
        # todo - we dont need to do this inside the function below any more if just either get the outlines here or get the vald self/bf pieces somewhere

        def _exists(path):
            if "s3://" in path:
                return res.connectors.load("s3").exists(path)
            else:
                return Path(path).exists()

        def newer(path):
            a = list(res.connectors.load("s3").ls_info(path))
            if not a:
                return False
            if not timestamp:
                return True
            return a[0]["last_modified"] > timestamp

        # we can choose to use the piece key with or without size -> generally moving to size invariant keys
        keys = outlines["piece_key"]
        for k in keys:
            fpath = f"{path}/{k}.png"
            if _exists(fpath) and not force:
                logger.info(
                    f"{fpath} already exists - skipping. Force==true will overide"
                )
                continue
            if newer(fpath) and not override_newer:
                logger.info(
                    f"{fpath} is newer than the dxf so we do not need to create it again"
                )
                continue
            logger.info(f"saving sample {k} as .png to {path}")
            im = self.generate_piece_polygon_sample_image(
                piece_key=k, size=size, **kwargs
            )

            if "s3://" in path:
                res.connectors.load("s3").write(fpath, im)
            else:
                Path(path, parents=True).mkdir(exist_ok=True)
                imsave(fpath, im, check_contrast=False)

    def plot_raw_notches(self, key):
        from geopandas import GeoDataFrame

        c = self.compact_layers
        c = c[c["key"] == key]
        n = swap_points(c[c["layer"] == 4].iloc[0]["geometry"])
        o = swap_points(c[c["layer"] == 1].iloc[0]["geometry"])

        df = GeoDataFrame(list(n) + [o], columns=["geometry"])
        df.plot(figsize=(20, 20))
        return df

    def plot_geometries(self, size=None, zip_file_name=None):
        if zip_file_name:
            with zipfile.ZipFile(zip_file_name, mode="w") as zf:
                return dxf_plotting.plot_labeled_edges(self, size=size, zip_archive=zf)
        return dxf_plotting.plot_labeled_edges(self, size=size, zip_archive=None)

    def save_validation_bundle(self, filename, size=None):
        with zipfile.ZipFile(filename, mode="w") as zf:
            self.validate()
            data = self.validation_details
            data = json.dumps(data)
            zf.writestr("validation_details.json", data)
            return dxf_plotting.plot_labeled_edges(self, size=size, zip_archive=zf)

    @staticmethod
    def generate_physical_geometry(g, material_key, material_properties):
        """
        We use buffers and compensation to create the material physical geometrry
        """
        props = material_properties
        props = props.get(material_key) if not pd.isnull(props) else None
        if props:
            b = props.get("offset_size")
            if b and not pd.isnull(b):
                g = g.buffer(DPI_SETTING * b)

        g = affinity.scale(
            g, xfact=props["compensation_width"], yfact=props["compensation_length"]
        )

        return g

    def generate_piece_polygon_sample_image(
        self,
        piece_key,
        size,
        pieces_types=PRINTABLE_PIECE_TYPES,
        # denim, startdust
        fill_color_rgb=(255, 255, 255),  # (225, 246, 255),
        shift_to_origin=True,
        invert_y=True,
        full_resolution=True,
        swap_yx=False,
        outline_field="geometry",
        plot_image=False,
        add_label_text=True,
        **kwargs,
    ):
        """
        note that certain function shift to origin
        these are "last minute" functions for exporting but we keep all data in the original DXF locs to avoid confusion
        because the shapefile/dxf is not an image, we invert the y axis to create an image

        the default is to get the higher def outlines but we can also see what the keypoint outines look like
        """

        material_properties = kwargs.get("material_properties")
        if material_properties:
            res.utils.logger.info(
                "TODO: Apply material properties BUFFER only to the piece generation"
            )

        def bounds(outline):
            return int(np.ceil(outline[:, 0].max())), int(np.ceil(outline[:, 1].max()))

        # get the fully dervied stuff and we will use a res.key to pick one piece to generate
        outlines = self.get_sized_geometries(size=size, piece_types=pieces_types)

        # TODO - i think the geometry in image space needs to rotated but check

        # size is required as a parameter so piece key makes sense
        outline = np.array(
            swap_points(
                outlines[outlines["piece_key"] == piece_key].iloc[0][outline_field]
            )
        )

        mask = mask_from_outline(outline)

        canvas = np.ones((*bounds(outline), 4))
        rgb = tuple(np.array(fill_color_rgb) / 256.0)
        # fill the entire bounds
        canvas[:, :, :] = (*rgb, 1.0)
        # alpha out the complement of the mask
        canvas[~mask] = 0

        canvas = (canvas * 255).astype(np.uint8)
        # draw a little outline

        canvas = draw_outline(canvas, dxa_piece_outline_thickness=2)

        if add_label_text:
            # todo add text as this is a debugging tool - we will generate sample images on S3 for bodies
            pass

        if plot_image:
            from matplotlib import pyplot as plt

            plt.figure(figsize=(10, 10))
            plt.imshow(canvas)

        return canvas

    @staticmethod
    def outline_geometry_as_image(g, fill_color_rgb=(220, 230, 220)):
        outline = np.array(swap_points(g))

        def bounds(outline):
            return int(np.ceil(outline[:, 0].max())), int(np.ceil(outline[:, 1].max()))

        mask = mask_from_outline(outline)
        canvas = np.ones((*bounds(outline), 4))
        rgb = tuple(np.array(fill_color_rgb) / 256.0)
        # fill the entire bounds
        canvas[:, :, :] = (*rgb, 1.0)
        # alpha out the complement of the mask
        canvas[~mask] = 0
        canvas = (canvas * 255).astype(np.uint8)
        return canvas

    @staticmethod
    def get_graded_notches(
        sample_outline,
        sized_outline,
        sample_notches,
        fit_to_surface=True,
        angle_treshold=150,
        surface_buffer=0.2,
        congruence_r=0.7,
        allow_missing_notches=True,
        piece_key=None,
        plot=False,
    ):
        """
        Try this with just keypoint outlines but should work with validation curves too
        We would use the validation curves if the transformations are off but its unlikely especially with post fitting enabled

        0.7 may be an eigen-tolerance - but the surface buffer can lead to conflicts because the buffered edges overlap and multiple notches can fall in each
        so we generally relax the surface for now to make sure we assign the correct ones
        """

        def assign_notches_to_segments(notches, segments, buffer) -> MultiPoint:
            ns = {}
            for i, s in enumerate(segments):
                g = s.buffer(buffer).intersection(notches)

                # note g can be multipoint so there is always a map from segment->geometries
                if not g.is_empty:
                    if g.type == "Point":
                        g = MultiPoint([g])
                    ns[i] = g
            return ns

        # risk here is we lose an important part of the definition of the shape - uses an angle parameter and needs testing
        sample_kp = polygon_keypoints(sample_outline)
        sized_kp = polygon_keypoints(sized_outline)

        # this should be save if there is a good marriage of points
        a, b = find_congruence(sample_kp, sized_kp, r=congruence_r, returns="data")

        segments_base = list(geo_line_segments(a))
        segments_sized = list(geo_line_segments(b))

        # we are transforming each line segment or "seam" separately because is grading is like that
        transformations = []
        for i in range(len(segments_base)):
            transformations.append(
                find_2D_transform(
                    np.array(segments_base[i]), np.array(segments_sized[i])
                )
            )

        # we can map notches and other things into the seam locations
        assigned_notches = assign_notches_to_segments(
            sample_notches, segments_base, buffer=surface_buffer
        )

        def transform_with_z(g, i):
            tr = transformations[i]
            original_points = list(g)
            return MultiPoint(
                [Point(p[0], p[1], original_points[i].z) for i, p in enumerate(tr(g))]
            )

        # "carry the z" because our transformations are always in 2d but notch information is in the z coord
        transformed_notches = {
            i: transform_with_z(g, i) for i, g in assigned_notches.items()
        }

        # TODO - fit transformed notches to the outline if required using interpolated projection

        # for debugging
        if plot:
            from geopandas import GeoDataFrame

            df = GeoDataFrame(index=range(len(segments_base)))
            df["geometry"] = df.index.map(lambda x: assigned_notches.get(x))
            ax = df.plot(cmap="Set2", figsize=(20, 20))
            GeoDataFrame(segments_base, columns=["geometry"]).plot(ax=ax, cmap="Set2")

            df = GeoDataFrame(index=range(len(segments_base)))
            df["geometry"] = df.index.map(lambda x: transformed_notches.get(x))
            df.plot(cmap="Set1", figsize=(20, 20), ax=ax)
            GeoDataFrame(segments_sized, columns=["geometry"]).plot(ax=ax, cmap="Set1")

        projected_notches = unary_union(transformed_notches.values())

        if not allow_missing_notches:
            assert len(sample_notches) == len(
                projected_notches
            ), f"The number of notches has changed for piece key {piece_key} has changed from {len(sample_notches)} to {len(projected_notches)} - considering changing the congruence_r parameter from {congruence_r} or the surface_buffer from {surface_buffer}"

        return projected_notches

    def _upload_named_pieces(dxf, path, target_path, size=None, rotate=90):
        """
        This could be added to a DXF file with some rules to name pieces and upload them some place
        Added here for convenience to flesh out idea of pointing metadata and images to tie them together in nesting flows
        """
        from glob import glob

        import res
        from PIL import Image
        from res.media.images.outlines import get_piece_outline

        s3 = res.connectors.load("s3")

        size = size or dxf.sample_size

        res_layer = dxf.get_sized_geometries(size=size)

        # teh geometry is where we put the real image space outline
        named_pieces = res_layer[["res.key", "geometry"]].reset_index().drop("index", 1)

        named_pieces = dict(named_pieces.values)

        for idx, file in enumerate(glob(f"{path}/*.png")):
            im = Image.open(file)
            if rotate:
                im = im.rotate(rotate, expand=True)

            im = np.asarray(im)

            outline = get_piece_outline(im)
            name, confidence = name_part(
                outline, named_pieces, idx=idx, invert_named_pieces=False
            )

            logger.info(f"{name} used with confidence {confidence} for {file}")

            target = f"{target_path}/{name}.png"

            logger.info(f"will upload file to {target}")

            s3.write(target, im)

        return named_pieces

    @staticmethod
    def res_size_to_gerber_size(s):
        # reverse the size map

        # a known rule in gerber size mapping
        if "Size" in s:
            return s.replace("Size ", "").replace(" ", "")

        NMAP = {v: k for k, v in SIZE_NAMES.items()}

        def try_int(s):
            if s == None:
                return None
            if s[-1].upper() in ["T", "Y"]:
                s = s[:-1]
            try:
                return str(int(float(s)))
            except:
                return None

        return NMAP.get(s, try_int(s))

    def map_res_size_to_gerber_size(self, res_normed_size, ignore_errors=False):
        return DxfFile.map_res_size_to_gerber_size_from_size_list(
            self.sizes, res_normed_size, ignore_errors=ignore_errors
        )

    def size_from_accounting_size(self, size_code, lookup=None):
        """
        many processes give is an accounting size like 2ZZSM but we need a res size like S
        what is worse we can have alises in the DXF like SM
        This code looks up the fixed mapping to the normned size and then looks up an alis in the DXF file
        """
        if not lookup:
            from res.connectors.airtable import AirtableConnector

            airtable = res.connectors.load("airtable")
            sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
            # predicate on sizes that we have

            slu = sizes_lu.to_dataframe(
                fields=["_accountingsku", "Size Normalized"],
                filters=AirtableConnector.make_key_lookup_predicate(
                    [size_code], "_accountingsku"
                ),
            )
            lookup = dict(slu[["_accountingsku", "Size Normalized"]].values)

        gerber_size = self.map_res_size_to_gerber_size(lookup[size_code])

        return gerber_size

    @staticmethod
    def map_res_size_to_gerber_size_from_size_list(
        sizes, res_normed_size, ignore_errors=False
    ):
        # res.utils.logger.debug(f"determine gerber size for {res_normed_size}")
        # this is how we map one size fits all

        # case where we have inches in the name and we dont need it for mapping
        res_normed_size = res_normed_size.replace('"', "").replace("''", "")

        if res_normed_size.replace(" ", "_").lower() == "one_size_fits_all":
            res_normed_size = "1"

        if res_normed_size.replace(" ", "_").lower() == "one_fits_all":
            res_normed_size = "1"

        if res_normed_size in sizes:
            # res.utils.logger.debug(f"trying to alias {res_normed_size} from {sizes}")
            return res_normed_size

        prefix = ""
        if "P-" in res_normed_size:
            res_normed_size = res_normed_size.replace("P-", "")
            prefix = "P-"
        if "P_" in res_normed_size:
            res_normed_size = res_normed_size.replace("P_", "")
            prefix = "P_"

        # print("mapping", res_normed_size)
        if res_normed_size in sizes:
            size = res_normed_size
        else:
            size = DxfFile.res_size_to_gerber_size(res_normed_size) or res_normed_size
        # print("mapped", size, "prefix", prefix)

        if size is None:
            res.utils.logger.warn(
                f"Null size mapping when mapping from {res_normed_size}"
            )
            # raise Exception(f"Null size when mapping from {res_normed_size}")

        if size not in sizes:
            res.utils.logger.debug(f"trying to alias {size} from {sizes}")
            # some aliases
            if size == "1" and "ONE SIZE FITS ALL" in sizes:
                size = "ONE SIZE FITS ALL"
            if size == "1" and "One Size Fits All" in sizes:
                size = "One Size Fits All"
            if size == "MD" and "MED" in sizes:
                size = "MED"
            if size == "M" and "MD" in sizes:
                size = "MD"
            elif size == "MED" and "MD" in sizes:
                size = "MD"

            if size in ["XXS", "XX"] and "XXSM" in sizes:
                size = "XXSM"
            elif size in ["XXS", "XX"] and "XXS" in sizes:
                size = "XXS"

            if size == "SM" and "S" in sizes:
                size = "S"

            if size == "LG" and "L" in sizes:
                size = "L"
            if size == "L" and "LG" in sizes:
                size = "LG"
            if size == "2XL" and "XXL" in sizes:
                size = "XXL"

            res.utils.logger.debug(f"size is {size}")
            # consider this invairance e.g. if we have P_SM but the correct res input is P-SM

        size = f"{prefix}{size}"

        # print("checking ", size)

        char_removed_sad_face = {s.replace("_", "-"): s for s in sizes}
        # print(size, char_removed_sad_face)
        if size not in sizes and size in char_removed_sad_face.keys():
            size = char_removed_sad_face[size]
            return size
        # seems like they use underscores in some bodies?? e.g. 2_XL
        char_removed_sad_face = {s.replace("_", ""): s for s in sizes}
        if size not in sizes and size in char_removed_sad_face.keys():
            size = char_removed_sad_face[size]
            return size

        if size in sizes:
            return size

        if not ignore_errors:
            raise Exception(
                f"Could not find size {size} in ASTM mapped from res size {res_normed_size} - the astm sizes are {sizes}"
            )

    @staticmethod
    def get_body_pieces(body, res_normed_size, gerber_size=None):
        """
        this can be merged and refactored with nest body
        """

        s3 = res.connectors.load("s3")
        size = gerber_size or DxfFile.res_size_to_gerber_size(res_normed_size)
        if size is None:
            size = res_normed_size

        if "-" not in body:
            body = body[:2] + "_" + body[2:]
        body = body.replace("-", "_")

        body = body.lower()
        logger.info(f"loading body,size from astm file {body, size}")
        body_file = list(
            reversed(
                list(s3.ls(f"s3://meta-one-assets-prod/bodies/{body}/pattern_files/"))
            )
        )

        if len(body_file):
            for bf in body_file:
                try:
                    logger.info(f"trying version {bf}")
                    dxf = DxfFile(bf)
                    logger.info(f"available sizes in this file are {dxf.sizes}")
                    # return dxf
                    if size not in dxf.sizes:
                        # hacky but seems like its inconsistent - i hve an example where the size is XL in Gerber for example not XLG
                        if res_normed_size in dxf.sizes:
                            size = res_normed_size

                        # some aliases
                        if size == "MD" and "MED" in dxf.sizes:
                            size = "MED"
                        elif size == "MED" and "MD" in dxf.sizes:
                            size = "MD"

                        if size in ["XXS", "XX"] and "XXSM" in dxf.sizes:
                            size = "XXSM"
                        elif size in ["XXS", "XX"] and "XXS" in dxf.sizes:
                            size = "XXS"

                        if size == "LG" and "L" in dxf.sizes:
                            size = "L"
                        if size == "2XL" and "XXL" in dxf.sizes:
                            size = "XXL"

                    logger.info(f"Looking to nest bodies of size {size}")
                    # size = DxfFile.map_res_size(size)

                    res_layer = dxf.get_sized_geometries(size)

                    return res_layer, dxf
                except:
                    pass

    def display_nested_pieces(self, size=None):
        """
        Usually used to display pieces
        we do not reverse points as used in some legacy setups as the geometry in image space is the thing we are nesting
        nest datafframe looks for a geometry column by default...
        """
        from res.learn.optimization.nest import nest_dataframe

        size = size or self.sample_size
        df = self.get_sized_geometries(size)
        return nest_dataframe(df, plot=True, reverse_points=False)

    @staticmethod
    def open_net_file(filename, debug=False, plot=False):
        """
        Parse shapes and names out of a dxf file
        """

        def _parse_names_for_body(l, body):
            """
            Get body text and location of body text
            """

            def text_locator(group):
                chka = group.reset_index()
                chka["my_delim"] = chka["s"].shift(1)
                chka[chka["delim"] == 0]
                chka["my_delim"] = chka["my_delim"].map(lambda x: str(x).strip())
                parts = dict(chka[["my_delim", "s"]].astype(str).values)
                return Point(float(parts["10"]), float(parts["20"]))

            data = pd.DataFrame(l, columns=["s"])
            data["s"] = data["s"].map(lambda x: x.rstrip("\n").rstrip("\r"))
            data["text_start"] = data["s"].map(lambda x: 1 if x == "AcDbText" else 0)
            data["delim"] = (data.index % 2 == 0).astype(int)
            data["ends"] = ((data["delim"] == True) & (data["s"] == "  0")).astype(int)
            data["cst"] = data["text_start"].cumsum()
            data["section"] = data["ends"].cumsum().shift(1)
            sections = []
            gs = []

            texts = []
            for key, g in data.groupby("section"):
                g["any"] = g["s"].map(lambda x: 1 if body in x else 0)
                if g["any"].sum() > 0:
                    gs.append(g)
                    text = g[g["any"] == 1].iloc[0]["s"].split(";")[-1].strip()
                    if " " not in text and g.iloc[0]["s"] in ["TEXT", "MTEXT"]:
                        geom = text_locator(g)
                        texts.append({"text": text, "geometry": geom})

            return pd.DataFrame(texts)

        def _get_nearby_name_fn(names_df):
            def f(g):
                X = names_df.reset_index()
                dist = X["geometry"].map(lambda x: Polygon(g).distance(x))
                idx = np.argmin(dist)
                return X.iloc[idx]["text"]

            return f

        s3 = res.connectors.load("s3")
        BODY = filename.split("/")[5].replace("_", "-").upper()
        l = None
        if "s3://" in filename:
            with s3.file_object(filename) as f:
                l = [l for l in [l.decode() for l in f.readlines()]]
        else:
            with open(filename) as f:
                l = [l for l in f.readlines()]

        names = _parse_names_for_body(l, BODY)
        res.utils.logger.debug(
            f"{len(names['text'])} names and {len(names['text'].unique())} unique when processing NET DXF {filename}"
        )
        fn = _get_nearby_name_fn(names)

        def to_outline(g):
            try:
                if g.type == "MultiLineString":
                    g = geom_list(g)[0]
                return LinearRing(g)
            except:
                return None

        def seems_like_polygon(g):
            try:
                c = (
                    g.coords
                    if g.type != "MultiLineString"
                    else list(itertools.chain(*[h.coords for h in g]))
                )
                a = Point(c[0])
                b = Point(c[-1])
                return a.distance(b) < 1
            except:
                # assume the best
                return True

        dxf = DxfFile(filename, read_raw=True)
        df = pd.DataFrame(dxf._geom_df)
        df["valid_looking"] = df["polylines"].map(seems_like_polygon)
        df = df[df["valid_looking"]].reset_index().drop("index", 1)
        df["outline"] = df["polylines"].map(to_outline)
        df = df[df["outline"].notnull()].reset_index().drop("index", 1)

        def contains_other(row):
            try:
                g, i = Polygon(row["outline"]), row.name

                for idx, p in enumerate(df["outline"]):
                    if i != idx and g.contains(p):
                        return True
                return False
            except:
                return False

        df["is_boundary"] = df.apply(contains_other, axis=1)
        df = df[df["is_boundary"] == False]
        # we could fall back to using ordinals in some cases but it does not seem reliable
        # better to work out cases or formats for the coord system if they exist
        df["key"] = df["outline"].map(fn)

        if plot:
            from geopandas import GeoDataFrame

            ax = GeoDataFrame(df, geometry="outline").plot(figsize=(50, 50))
            for record in names.to_dict("records"):
                t = record["geometry"].x, record["geometry"].y
                ax.annotate(
                    str(record[11]),
                    xy=t,
                    arrowprops=dict(facecolor="black", shrink=0.005),
                )
        if debug:
            return df, names
        return df

    @staticmethod
    def make_setup_file_svg(filename, return_shapes=False, ensure_valid=True):
        """
        experimental method to read NET dxf files and generate setup files
        this produces IMAGE space data that is the same as what we see in markers so its very useful
        we can NAME the pieces in the setup file and these will be the same shapes with notches and buffers that we have in markers so the naming is perfect and its im Image space already
        We merge combos in the workflow

        we dont know how to get the names from the layers so we assume an ordered list of names in the DXF which sort of makes sense as the dataframe is built in the same order
        but we need to parse out something like BODy name and assume the file name contains that body name. all a bit funky

        another assumption we make is that there split layers so every second layer is the piece in the DXF ::2

        then we generate the SVG with some transformations

        #we can read and write with the s3 connector and display with IPython.display.SVG

        s3.write("s3://res-data-platform/test/test.svg", svg)
        SVG(s3.read("s3://res-data-platform/test/test.svg"))

        #some files for testing are in
        s3://meta-one-assets-prod/bodies/net_files/[body_code]/v[body_version]/[filename].dxf

        outputs will goto
        s3://meta-one-assets-prod/bodies/setup_files/[body_code]/v[body_version]/[filename].svg

        """
        from res.media.images.geometry import (
            unary_union,
            scale_shape_by,
        )

        BODY = filename.split("/")[-1].replace("_", "-").upper()
        BODY = "-".join((BODY.split("-")[:2]))
        # res.utils.logger.warn(f"processing setup file for body {BODY}")

        def unpack_ml(ml):
            for l in ml:
                for p in l.coords:
                    yield p

        def as_svg_path(l, name):
            H = l.bounds[-1] - l.bounds[1]
            W = l.bounds[-2] - l.bounds[0]

            centroid = l.centroid
            l = (
                np.array(l)
                if l.type != "MultiLineString"
                else np.array(list(unpack_ml(l)))
            )

            id_ = int(l[:, -1].min())
            points = " ".join([f"{p[0]},{p[1]}" for p in l])

            simple_rotate = (
                ""
                if W > 1000
                else f'transform="rotate(-90, {centroid.x}, {centroid.y})"'
            )
            xoff, yoff = (-600, 0) if W > 1000 else (-600, 0)
            p = f"""
            <g id="piece_names" {simple_rotate} >
            <text x="{centroid.x+xoff}" y="{centroid.y+yoff}" font-size="140px" class="heavy">{name}</text>
            </g>
            <g id="outlines">
            <polyline points="{points}" id="{id_}" fill="none" stroke="black" stroke-width="10"/>
            </g>
            """

            return p

        def try_get_notches(outline):
            try:
                return detect_castle_notches_from_outline(outline)
            except:
                return None

        def invert_image_space(df):
            """
            invert image space and reorigin
            """
            all_shape_bounds = unary_union(df["outline"]).bounds
            # shift to the origin
            df["outline"] = df["outline"].map(
                lambda g: invert_axis(g, bounds=all_shape_bounds)
            )
            all_shape_bounds = unary_union(df["outline"]).bounds
            df["outline"] = df["outline"].map(
                lambda x: translate(
                    x, xoff=-1 * all_shape_bounds[0], yoff=-1 * all_shape_bounds[1]
                )
            )
            all_shape_bounds = unary_union(df["outline"]).bounds
            return df

        df = DxfFile.open_net_file(filename)
        # move things around and scale them
        all_shape_bounds = unary_union(df["outline"]).bounds
        # flip and scale from the DXF
        df["outline"] = (
            df["outline"]
            .map(lambda g: rotate(g, 90, origin=(0, 0)))
            .map(lambda x: scale_shape_by(300)(x))
        )

        if ensure_valid:
            df["outline"] = df["outline"].map(try_ensure_valid)

        df = invert_image_space(df)
        # get a full reference and then make sperate polygons
        full_geom = unary_union(df["outline"])
        H = full_geom.bounds[1] + full_geom.bounds[-1]
        gbounds_str = " ".join(
            [str(f + 500 if i > 1 else 0) for i, f in enumerate(full_geom.bounds)]
        )

        # print(all_shape_bounds, gbounds_str)
        # make a big string
        ps = ""
        for row in df.to_dict("records"):
            name = row["key"]
            g = row["outline"]
            # TODO test text orientation
            ps += as_svg_path(g, name) + "/n"
        # transform="translate(0,{H}) scale(1, -1)"
        svg = f"""<svg viewBox="{gbounds_str}" xmlns="http://www.w3.org/2000/svg">
        <g >
            {ps}
        </g>
        </svg>
        """

        if return_shapes:
            # shapes are teturned
            # ordered shapes will have scan order
            data = df[["key", "outline", "polylines"]]
            # we dont meed to invert but we can plot with the inverted Y - this is added as a note
            # data = invert_image_space(data)
            """
            dat = GeoDataFrame(data,geometry='outline')
            dat.plot(figsize=(10,10))
            plt.gca().invert_yaxis()
            """
            data["original_shape_bounds"] = data["outline"].map(
                lambda g: list(g.bounds)
            )
            data["geometry"] = data["outline"].map(shift_geometry_to_origin)
            data["bounds"] = data["geometry"].map(lambda g: list(g.bounds))
            data["minx"] = data["original_shape_bounds"].map(lambda b: b[0])
            data["maxy"] = data["original_shape_bounds"].map(lambda b: b[-1])
            data["notches"] = data["outline"].map(try_get_notches)
            # v-notches are needed - physical notchesa are different though
            # compute some original offset transformation to find a tiling
            data = data.sort_values(by=["maxy", "minx"], ascending=[False, True])
            data = data.reset_index().drop("index", 1)
            data = data.reset_index().rename(columns={"index": "scan_order"})

            return svg, data

        return svg

    @staticmethod
    def nest_body(
        body,
        res_normed_size,
        gerber_size=None,
        version=None,
        geoms_as_array=True,
        ranked_piece_names=None,
        material_width=None,
        rank=0,
    ):
        """
        Supply a body code in format CC-6001, CC6001 or CC6001 anda  resonance normed size e.g. S, 1, one size fits all etc.

        TODO: if we know the version use it to find the pattern file otherwise get the latest
        """
        from res.learn.optimization.nest import nest_dataframe, update_shape_props
        from shapely.geometry import Polygon

        size = gerber_size or DxfFile.res_size_to_gerber_size(res_normed_size)
        if size is None:
            size = res_normed_size

        s3 = res.connectors.load("s3")

        if "-" not in body:
            body = body[:2] + "_" + body[2:]
        body = body.replace("-", "_")

        body = body.lower()
        logger.info(f"nesting body,size from astm file {body, size}")
        body_file = list(
            reversed(
                list(s3.ls(f"s3://meta-one-assets-prod/bodies/{body}/pattern_files/"))
            )
        )
        if len(body_file):
            for bf in body_file:
                try:
                    logger.info(f"trying version {bf}")
                    dxf = DxfFile(bf)
                    logger.info(f"available sizes in this file are {dxf.sizes}")
                    # return dxf
                    if size not in dxf.sizes:
                        # hacky but seems like its inconsistent - i hve an example where the size is XL in Gerber for example not XLG
                        if res_normed_size in dxf.sizes:
                            size = res_normed_size

                        # some aliases
                        if size == "MD" and "MED" in dxf.sizes:
                            size = "MED"
                        elif size == "MED" and "MD" in dxf.sizes:
                            size = "MD"

                        if size in ["XXS", "XX"] and "XXSM" in dxf.sizes:
                            size = "XXSM"
                        elif size in ["XXS", "XX"] and "XXS" in dxf.sizes:
                            size = "XXS"

                        if size == "LG" and "L" in dxf.sizes:
                            size = "L"
                        if size == "2XL" and "XXL" in dxf.sizes:
                            size = "XXL"

                    logger.info(f"Looking to nest bodies of size {size}")
                    # size = DxfFile.map_res_size(size)
                    res_layer = dxf.get_sized_geometries(size)
                    if ranked_piece_names:
                        l = len(res_layer)
                        res_layer["res.category"] = res_layer["piece_name"].map(
                            lambda x: x.split("-")[-1]
                        )
                        print("filtering rank")
                        # coud replace with a fuzzy match
                        if rank == 0:
                            res_layer = res_layer[
                                ~res_layer["res.category"].isin(ranked_piece_names)
                            ]
                        else:
                            res_layer = res_layer[
                                res_layer["res.category"].isin(ranked_piece_names)
                            ]

                        print(
                            "after filtering, length of res layer is ",
                            len(res_layer),
                            "from",
                            l,
                        )
                        assert len(
                            res_layer
                        ), "after apply of rank filter there are no items"
                        assert (
                            len(res_layer) == l - len(ranked_piece_names)
                            if rank == 0
                            else len(ranked_piece_names)
                        ), "after apply of rank filter the correct number of pieces were not filtered"

                    if len(res_layer) == 0:
                        logger.warn(f"Did not find pieces of this size")
                        continue
                    # return res_layer
                    widest = (
                        res_layer["geometry"]
                        .map(lambda g: g.bounds[3] - g.bounds[1])
                        .max()
                    )

                    width = material_width or 16500
                    # stretch to the biggest pieace but at least 16500 or whatever we input
                    width = max(width, int(widest + 10))

                    # a large temporary bou
                    nested = nest_dataframe(
                        res_layer.reset_index(),
                        output_bounds_width=width,
                        output_bounds_length=1000000,
                        # because we are inmage space do not reverse
                        reverse_points=False,
                    )
                    nested = nested[
                        ["res.key", "size", "nested.geometry", "tile_width"]
                    ]
                    nested["shape_area"] = nested["nested.geometry"].map(
                        lambda g: Polygon(g).area
                    )

                    nested = update_shape_props(nested)

                    nested["res_size"] = res_normed_size

                    logger.info("nesting complete")

                    return nested

                except Exception as ex:
                    logger.warn(f"Failed in this step due to {repr(ex)}")
                    raise ex
        else:
            logger.warn(f"no astm file found for body {body}")


class dxf_notches:
    @staticmethod
    def get_notch_angles(self, image_space=False):
        """
        this reads the notch angles from the dxf attributes
        when we are in image space we will neede to rotate though
        this includes internal and external but we can align them at notch model level
        """
        g = self._geom_df
        g = g[g["entity_layer"] == 4][["key", "geometry_attributes", "geometry"]]

        def nap(r):
            pt = list(list(r["location"]))
            pt[-1] = r.get("angle", 0)
            return Point(pt)

        def safe_angle(pts):
            if pd.isnull(pts):
                return None
            if pts.type == "Point":
                return [pts.z]
            return [p.z for p in pts]

        g["notch_angles"] = g["geometry_attributes"].map(nap)
        # strict mode should check if the points are the correct ones but they attributes must correspond here
        g = g.groupby("key").agg({"notch_angles": list})
        g["notch_angles"] = g["notch_angles"].map(unary_union)
        g["notch_angles"] = g["notch_angles"].map(safe_angle)
        return g

    @staticmethod
    def notched_outline(row, width=15):
        """
        WIP this requires lots of testing and understanding of notches and orientations
        Wem also want to support different types of notches like Vs etc.
        """

        def draw_notch(l):
            l = LineString([Point(list(l.coords)[0]), l.interpolate(l.length / 2.0)])

            return unary_union(
                [l.parallel_offset(width, "left"), l.parallel_offset(width, "right")]
            ).convex_hull

        notches = dxf_notches.get_notch_frame(row)

        notches["d"] = None
        notches.loc[notches["distance_to_edge"] == 0, "d"] = notches["notch_line"].map(
            draw_notch
        )

        N = unary_union(notches["d"].dropna().values)
        return (Polygon(row["geometry"]) - N).exterior

    @staticmethod
    def projection_offset_op(
        row,
        geometry_column="geometry",
        plot=False,
        debug=False,
    ):
        """
        This could be promoted out of notches to DXF ot geometry
        it helps to order points from a critical corner on an edge
        This is a robust way to sort points clockwise on edges without worrying about conventions for polygon ordering

        TEST: the 0 corner should be the one before the center of mass
        """
        crn = pd.DataFrame(
            magic_critical_point_filter(row[geometry_column]), columns=[geometry_column]
        )
        kps = get_geometry_keypoints(row[geometry_column])
        # critical point with bias - im image space this is like far to the bottom right
        some_factor = 10
        kp = Point(
            kps["center_of_mass"].x * some_factor,
            row[geometry_column].bounds[-1] * some_factor,
        )
        # print(kp)
        crn["prj"] = crn[geometry_column].map(
            lambda x: row[geometry_column].project(x.centroid)
        )
        crn["dist"] = crn[geometry_column].map(lambda g: g.distance(kp))

        # it is very rare that there are no corners but we default to 0
        _p = dict(crn.sort_values("dist").iloc[0]) if len(crn) else {"prj": 0}
        # difference from end - its a rotation so that our key point is the start of the cycle
        offset = row[geometry_column].length - _p["prj"]

        if plot:
            from geopandas import GeoDataFrame
            from matplotlib import pyplot as plt

            a = crn.sort_values("dist").reset_index()
            ax = GeoDataFrame(a).plot(figsize=(10, 10))

            plt.gca().invert_yaxis()
            i = 0
            for record in a.to_dict("records"):
                t = record["geometry"].x, record["geometry"].y
                ax.annotate(
                    str(i), xy=t, arrowprops=dict(facecolor="black", shrink=0.005)
                )
                i += 1

        if debug:
            return crn, kp

        # print(offset)
        def f(pt):
            pt = Point(pt)
            # we take a point and project it and the offset so it is orientated from the origin corner kp
            p = row[geometry_column].project(pt)
            return (p + offset) % row[geometry_column].length

        return f

    @staticmethod
    def filter_my_adj(
        df, factor=45 / 11.1, epsilon=5 / 11.1
    ):  # 35 small value is good but may safety e.g. be sure to leave a SA notch
        """
        TODO: mm precision is used for default filters here

        helper function to use seam allowances to remove any notches that are not supposed to be there
        we find the adjacent edges and if we are the corner placed notch, we consult if we are too close

        a tolerance factor can be used to allow for small offsets
        """

        if "seam_allowance" in df.columns:
            # some unfortuante casting here because pandas us using flaots from ints
            df["nearest_edge_id"] = df["nearest_edge_id"].astype(int)
            # we must always filter these things by surface notches
            edge_notch_count = dict(
                df[df["distance_to_edge"] == 0]
                .groupby("nearest_edge_id")[["edge_order"]]
                .max()
                .reset_index()
                .values
            )

            # these need to agree
            edge_notch_count = {int(k): v for k, v in edge_notch_count.items()}
            edge_sa = dict(
                df.groupby("nearest_edge_id")[["seam_allowance"]]
                .min()
                .reset_index()
                .values
            )

            edge_sa = {int(k): v for k, v in edge_sa.items()}
            edge_notch_count = {int(k): v for k, v in edge_notch_count.items()}

            # EXPECT: keys match and are contiguos

            def my_adjacent_edge_sa(row):
                num_edges = max(edge_notch_count.keys()) + 1
                my_edge = int(row["nearest_edge_id"])

                for i in range(num_edges):
                    if i not in edge_sa:
                        edge_sa[i] = 0

                if row["is_corner"]:
                    # -1 is very illegal just trying to undersatnd cases
                    my_max_notch = edge_notch_count.get(my_edge, -1)

                    if row["edge_order"] == 0:
                        adj = (my_edge - 1) % num_edges
                        if adj not in edge_sa:
                            raise Exception(
                                f"The selected edge {adj} is not valid in the list of seam allowances - we expect {num_edges} edges fromm {edge_sa} and my edge is {my_edge}"
                            )
                        return edge_sa[adj] * DPI_SETTING
                    if row["edge_order"] == my_max_notch:
                        adj = (my_edge + 1) % num_edges
                        if adj not in edge_sa:
                            raise Exception(
                                f"The selected edge {adj} is not valid in the list of seam allowances - we expect {num_edges} edges from {edge_sa} and my edge is {my_edge}"
                            )
                        return edge_sa[adj] * DPI_SETTING

                return None

            # things right on corners should always be removed no matter what
            SOME_SMALL_MIN_DIST = 5
            df["my_min_corner_threshold"] = df.apply(
                my_adjacent_edge_sa, axis=1
            ).fillna(SOME_SMALL_MIN_DIST)

            # dont tolerate 0 seam allowance
            df.loc[
                df["my_min_corner_threshold"] == 0, "my_min_corner_threshold"
            ] = SOME_SMALL_MIN_DIST

            # get the others and see how far away we are
            ref = unary_union(df["geometry"])
            df["dist_other"] = df["geometry"].map(lambda x: x.distance(ref - x))

            # because we are removing something, we should check that our nearest ngh is within the same tolerance i.e. there is a notch near as and we are near and within the seam allowance
            df["fail"] = (
                df["to_corner_distance"] + (factor - epsilon)
                < df["my_min_corner_threshold"]
            ) & (df["dist_other"] < (factor + 2 * epsilon))

            # force remove anything right on the corner
            df.loc[df["to_corner_distance"].astype(int) < 5.1, "fail"] = True

            res.utils.logger.warn(
                f"removing {len(df[df['fail']])} notches that are within threshold: {(factor + 2 * epsilon)} - dists are {list(df[df['fail']]['to_corner_distance'])}"
            )
            # work on the condition but force remove anything right on corners
            df = df[~df["fail"]].reset_index().drop(["index", "fail"], 1)

            # recompute - really?
            # df["my_min_corner_threshold"] = df.apply(my_adjacent_edge_sa, axis=1)
            return df

        # re-determine the value here as i may now be a corner - ughh

        return df

    @staticmethod
    def pair_corners(row):
        """
        matches the outline and sample outlines to return a,b corners that match

        this uses ne
        """

        corners_a = magic_critical_point_filter(row["outline"])
        corners_b = magic_critical_point_filter(row["outline_sample"])

        corners_a = np.array([[p.x, p.y] for p in corners_a])
        corners_b = np.array([[p.x, p.y] for p in corners_b])

        d, i = KDTree(corners_a).query(corners_b, k=1)

        corners_a = [corners_a[idx] for idx in i]

        # sort the correspondance

        return corners_a, corners_b

    @staticmethod
    def sorted_corners(row, geometry_column):
        """
        this is still WIP but we may need to sort corners by some lgoic so that sizes correspond
        for example to check this

            dxf = DxfFile('s3://meta-one-assets-prod/color_on_shape/tt_3036/v1/citrbq/dxf/')
            LC = sorted_corners(L.set_index('key').loc['TT-3036-V1-SHTSLPLKLF-BF_LG'])
            SMC = sorted_corners(SM.set_index('key').loc['TT-3036-V1-SHTSLPLKLF-BF_SM'])

        These should all be senslible lines

            df = GeoDataFrame({
                'L' : LC,
                'SM': SMC
            })
            df['geometry'] = df.apply(lambda row : LineString([row['L'], row['SM']]),axis=1)
            df.plot()
        """
        g = row[geometry_column]
        fn = dxf_notches.projection_offset_op(row, geometry_column=geometry_column)
        corners = magic_critical_point_filter(g)

        corners = sorted(list(corners), key=lambda g: fn(g))

        return corners

    @staticmethod
    def adjacent_seam_selector(edges, num_edges, edge_order_fn, eid=0, buffer=350):
        """
        this ise used to find segments nearby a labelled edge using some buffer
        the use case for this is to select nearby edges to tag notches on seam allowances or facing
        so buffer should be a little bigger than facing

        conditions
        - if buffer too big we take in too many lines (we could trim the list)
        - if buffer too small, we dont have enough line segment to interpolate along
        We can use the edge parameters we are give here

        """

        SMALL_EDGE_PARAM = 50

        def get_id(e):
            return e.interpolate(e.length / 2).z

        lines = edges

        allowed_others = [
            (eid + 1) % (num_edges),
            (eid - 1) % (num_edges),
        ]

        res.utils.logger.debug(
            f"Selecting edges adjacent to {eid} i.e. {allowed_others}"
        )
        # edge selectors
        ml1 = unary_union(
            [
                e
                for e in lines.geoms
                if e.length > SMALL_EDGE_PARAM and get_id(e) == allowed_others[0]
            ]
        )
        ml2 = unary_union(
            [
                e
                for e in lines.geoms
                if e.length > SMALL_EDGE_PARAM and get_id(e) == allowed_others[-1]
            ]
        )

        # to do we need to group into 2 objects by eid - do we need to check these are things we can reliably interpolate along

        if ml1.type != "LineString":
            res.utils.logger.warn(
                "When selecting adjacent edges we need to sort the points - check this is ok"
            )
            ml1 = sort_as_line(ml1, edge_order_fn=edge_order_fn)

        if ml2.type != "LineString":
            res.utils.logger.warn(
                "When selecting adjacent edges we need to sort the points - check this is ok"
            )
            ml2 = sort_as_line(ml2, edge_order_fn=edge_order_fn)

        return [ml1, ml2]

    @staticmethod
    def place_at_distance_away_from_corners(
        edges, num_edges, eid, edge_order_fn, offsets
    ):
        """
        Finds the adjacent seams
        For each one, for each distance in the set
        We interpolrate away from the trusted reference corners between points
        """

        def get_id(e):
            return e.interpolate(e.length / 2).z

        if not isinstance(offsets, list):
            offsets = [offsets]

        sel = dxf_notches.adjacent_seam_selector(
            edges, num_edges, edge_order_fn=edge_order_fn, eid=eid
        )
        # get the self edge - later we will interpolate away from this
        e = unary_union([e for e in geom_list(edges) if get_id(e) == eid])

        sel = list(sel)
        two_edge_piece = sel[0] == sel[-1]

        for i, ae in enumerate(sel):
            if ae.length == 0:
                continue
            for d in offsets:
                if i == 1 and two_edge_piece:
                    res.utils.logger.debug(
                        f"two side piece - adjusting {d} length offset: {ae.length}- {d}"
                    )
                    d = ae.length - d
                try:
                    # interpolate way from self edge: offsets in the source are always in inches
                    pt = interpolate_away_from(ae, e, d)
                except Exception as ex:
                    res.utils.logger.warn(
                        f"Failed to interpolate on edge {eid} length {e.length}({e.type}), {ae.length}({ae.type}) - {d}(offset) "
                    )
                    raise ex

                # assert pt.type =='Point', f"{pt} is not a point"

                yield pt

    @staticmethod
    def try_repair_notches(
        edges,
        notches,
        seam_allowances,
        corners,
        edge_order_fn,
        # 8th inch 0.125
        distance_threshold,
        # 16th inch 0.0625
        corner_threshold,
        plan=False,
    ):
        """

        <<<<<<< HEAD

        =======
        >>>>>>> main
                [TODO:  we can move this logic to the dxf notch model which does the corner notch filter right now]

                This is a really critical function and it may be biting off more than we can chew
                It may be that if we cannot trust notches from 3D we can attempt to repair using some rules and certain measurements

                for example remove anything closer than the smallest seam allowance
                We can do a bit better though because we still should not have anything between SA and corners

                test cases:
                - when there are no existing notches: KT-3030-V5-SHTSLBDGLF-S_SM
                - when there are corner notches and missing notches: KT-3030-V5-SHTFTPNLLF-S_SM

                ASSUME we can add these logical notches after frading so does not need to happen in DXF
        """
        last_edge = None

        res.utils.logger.debug(f"repairing notches....")

        try:
            sa_measures = seam_allowances
            existing_notches = notches
            if pd.isnull(existing_notches):
                existing_notches = []

            illegal_corner_notches = []

            if existing_notches and corner_threshold is not None:
                # we cannot really do it like this
                # we need to know if its within the seam allowance on that particular edge
                # it might be better to do this on notch frame because we can add the SA on that edge and then filter
                illegal_corner_notches = [
                    p
                    for p in geom_iter(existing_notches)
                    if p.distance(corners) < corner_threshold
                ]
            # remove any notches that are closer than seam allowance to a corner
            # add center notches if they are supposed to be there
            added_notches = []
            for k, v in sa_measures.items():
                last_edge = k
                sa = dxf_notches.place_at_distance_away_from_corners(
                    edges=edges,
                    num_edges=geom_len(corners),  # JL? this one makes sense
                    eid=k,
                    offsets=v,
                    edge_order_fn=edge_order_fn,
                )
                sa = list(sa)
                # if its near something we already have, ignore
                filtered = []
                distance_threshold = 20
                for p in sa:
                    dist = (
                        p.distance(existing_notches)
                        if existing_notches
                        else 2 * distance_threshold
                    )
                    # temp set a really low threshold

                    if v == 0:
                        continue

                    if dist > distance_threshold:
                        res.utils.logger.debug(
                            f"Adding a notch edge {k} {p} at distance {v} for nearest notch {dist} > {distance_threshold} out of {len(sa)} locations"
                        )
                        filtered.append(p)
                    else:
                        res.utils.logger.warn(
                            f"Skip a notch on edge {k} p {p} at distance {v} for nearest notch {dist} > {distance_threshold} out of {len(sa)} locations"
                        )

                added_notches += filtered
                # maybe label the SA notches with a z value

            if not plan:
                repaired_notches = unary_union(
                    [
                        p
                        for p in geom_iter(existing_notches)
                        if p.distance(corners) > corner_threshold
                    ]
                    + added_notches
                )
                return repaired_notches
            return notches
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to repair notches for last edge {last_edge} due to an unexpected error {res.utils.ex_repr(ex)}"
            )
            raise ex
            return [], []
        # do the same for facing distances

    @staticmethod
    def get_notch_frame(
        row,
        units="mm",
        plot=False,
        include_notch_lines=True,
        include_placement_lines=True,
        to_image_space=False,
        known_corners=None,
        edge_seam_allowances=None,
        edge_facing_allowances=None,
    ):
        """
        model the notches
        known corners and measurements can be passed in to decide how to model notches
        notches should become deprecated really

        """

        def scale_inchesto_mm(v):
            return v * 25.4

        PLINE = SMALL_SEAM_SIZE * 0.75
        notches = (
            geometry_to_image_space(row, "notches")
            if to_image_space
            else row.get("notches")
        )

        # todo  repair notches internally to notch frame and then we can filter on seam allowances after
        # we can also add notches for facing and sa notches here too

        # TODO: document deduplicate notches - i think on fold lines then can be duplicated

        # the geometry is def in image space so it would be illegal to do both mapping etc. but marker is the way forward and it hs always in image space ??

        geom_col = "outline"
        if to_image_space or geom_col not in row:
            geom_col = "geometry"

        egeom = row[geom_col]

        edge_order_fn = dxf_notches.projection_offset_op(row, geometry_column=geom_col)
        edges = label_edge_segments(egeom, fn=edge_order_fn, corners=known_corners)

        try:
            if isinstance(edge_seam_allowances, list):
                edge_seam_allowances = {
                    i: v for i, v in enumerate(edge_seam_allowances)
                }
            edge_seam_allowances = dict(edge_seam_allowances)
        except:
            edge_seam_allowances = {}

        if len(edge_seam_allowances):
            res.utils.logger.debug(
                f"Reparing notches with same allowances {edge_seam_allowances}"
            )
            if to_image_space:
                edge_seam_allowances = {
                    k: v * 300 for k, v in edge_seam_allowances.items()
                }
                res.utils.logger.debug(
                    f"scaling seam allowances to image space values: {edge_seam_allowances}"
                )
            if known_corners is None:
                # use the outline object to find
                known_corners = magic_critical_point_filter(egeom)
                known_corners = cascaded_union(known_corners)

            #     # 2.5 knowing the corners and measurements ?? we can now attempt to repair notches
            threshold_values = 0.165 * 300, 0.0625 * 300
            # if units == "mm":
            #     threshold_values = scale_inchesto_mm(
            #         threshold_values[0]
            #     ), scale_inchesto_mm(threshold_values[1])

            if len(known_corners):
                # there should almost always be corners
                notches = dxf_notches.try_repair_notches(
                    edges=edges,
                    notches=notches,
                    seam_allowances=edge_seam_allowances,
                    corners=known_corners,
                    edge_order_fn=edge_order_fn,
                    distance_threshold=threshold_values[0],
                    corner_threshold=threshold_values[1],
                )  # egeom, notches, edge_seam_allowances, corners=known_corners  plan=False)

        if not notches:
            return pd.DataFrame()

        # the single notch is rare and should not happen but if it does this blows up

        if notches.type == "Point":
            notches = MultiPoint([notches])

        inotch = pd.DataFrame(geom_iter(notches)).dropna()
        if len(inotch) == 0:
            return pd.DataFrame()

        inotch["x"] = np.round(inotch[0].map(lambda g: g.x), 2)
        inotch["y"] = np.round(inotch[0].map(lambda g: g.y), 2)
        # we do remove duplicates This is important !!!
        notches = unary_union(inotch.drop_duplicates(subset=["x", "y"])[0].values)

        edges = edges if to_image_space else edges

        # edge functions
        f = edge_query(edges, None)

        def is_on_edge(e, eid):
            # use the edge query above which magically determines the edge id for the surface
            # the center of the surface is more robust
            pt = e.interpolate(e.length / 2.0)
            z = f([pt.x, pt.y])

            return z == eid

        clocwise_order_fn = point_clockwise_order(egeom)

        # find an offset on the selected edge
        def selected_edge(eid):
            E = [
                inside_offset(e, egeom, r=PLINE)
                for e in edges.geoms
                if is_on_edge(e, eid)
            ]

            E = [e for e in E if e is not None]

            return unary_union(E)

        # * DPI_SETTING
        def my_seam_allowance(eid):
            if edge_seam_allowances:
                return unary_union(
                    [
                        inside_offset(e, egeom, r=edge_seam_allowances[eid])
                        for e in edges
                        if is_on_edge(e, eid)
                    ]
                )

        def clip_near_notch(row):
            g = row["isolated_edge_offset"]
            nl = row["notch_line"]
            if not pd.isnull(nl):
                pt = nl.interpolate(nl.length)
                DISC = DPI_SETTING * 1.5
                return pt.buffer(DISC).intersection(g)

        def pline_points(row):
            """
            although we check null on all of these, there really should be an intersection offset on all 0 distance obhects
            """
            # 4 is a rtemp
            DISC = SMALL_SEAM_SIZE * 4
            d = row["distance_to_edge"]
            l = row["isolated_edge_offset"]
            # test
            nl = row["notch_line"]
            if pd.notnull(nl) and pd.notnull(l) and d == 0:
                b = nl.interpolate(PLINE).buffer(DISC)
                I = l.intersection(b)
                if not I.is_empty:
                    # if we trust the line and not the chord we can do this
                    # return I
                    # this chord would only make sense for small segments
                    # but we can have really large notch offsets now so we need to keep the entire segment
                    # when we are placing we should get the small chord actual
                    pts = [I.interpolate(0), I.interpolate(I.length)]
                    return pts

        # safety on types
        if notches.type == "Point":
            notches = MultiPoint([notches])

        df = pd.DataFrame(notches, columns=["geometry"])
        df["distance_to_edge"] = df["geometry"].map(lambda g: int(g.distance(egeom)))
        DIST_SURF_THRESHOLD = 10
        res.utils.logger.warn(
            f"We use a dist threshold for surface notches: {DIST_SURF_THRESHOLD} - will clip anything smaller than this to 0"
        )
        df.loc[df["distance_to_edge"] < DIST_SURF_THRESHOLD, "distance_to_edge"] = 0

        df["nearest_edge_id"] = df["geometry"].map(lambda g: g.distance(egeom))
        df["nearest_edge_id"] = df["geometry"].map(lambda pt: f([pt.x, pt.y]))
        df["isolated_edge_offset"] = df["nearest_edge_id"].map(selected_edge)

        try:
            df["notch_angles"] = row.get("notch_angles")
        except:
            res.utils.logger.debug(
                f"Failed loading notch angles probably due to an index error i.e. incorrect number of objects assigned to df. check angles of notches match notch count"
            )
        df["ordered_from_avatar_center"] = df["geometry"].map(
            distance_to_relative_inside(egeom)
        )
        df["ordered_from_avatar_outside"] = df["geometry"].map(
            distance_to_relative_outside(egeom)
        )

        # todo refactor this whole thing corners and notches need to be more aware of each other in one place
        #
        crsn = edge_corners_controlled(egeom, notches)

        nline = lambda pt: dxf_notches.notch_line(pt, egeom)
        # pass the corners to the corner resolution so we can have a dynmic find corners buffer
        corner_nline = lambda pt: dxf_notches.notch_line(
            pt, egeom, is_corner=True, corners=unary_union(crsn)
        )

        def pline(pts):
            """
            this is a clipping based on two boundary points on the offset edge
            from the notch location (offset too a small seam)
            """

            try:
                if pts:
                    if edge_order_fn(pts[0]) > edge_order_fn(pts[-1]):
                        pts = list(reversed(pts))
                    return to2d(LineString([pts[0], pts[-1]]))

            except Exception as ex:
                res.utils.logger.warn(
                    f"Failed to find a placement line for point {pts} - {repr(ex)}"
                )

        if len(crsn) == 0:
            # dummy point to not break contract
            crsn = [row["geometry"].interpolate(0)]
        arr = np.array([[p.x, p.y] for p in crsn])
        # if len(arr) == 0:
        #     raise Exception(f"We have no corners!!")
        # this is not stricatly illegal any more

        narray = np.array([[p.centroid.x, p.centroid.y] for p in df["geometry"].values])

        if len(narray) == 0:
            raise Exception(
                f"We have notches for comparison - should not have got to here"
            )

        d, a = KDTree(narray).query(arr, k=min(2, len(narray)))

        # TODO: contract 2 inches is a near corner cut off - so if we are really far from the corner we cannot be a corner notch
        # we update this later
        a = a[d < 2 * 300].flatten()
        df["is_corner"] = 0
        df["to_corner_distance"] = df["geometry"].map(
            lambda pt: pt.distance(unary_union(crsn))
        )
        df.loc[a, "is_corner"] = 1

        df["clockwise_order"] = df["geometry"].map(clocwise_order_fn)
        df["edge_order"] = df["geometry"].apply(edge_order_fn)
        df["edge_index"] = df["edge_order"]

        if edge_seam_allowances:
            df["seam_allowance"] = df["nearest_edge_id"].map(
                lambda x: edge_seam_allowances.get(x)
            )
        if edge_facing_allowances:
            df["facing_allowance"] = df["nearest_edge_id"].map(
                lambda x: edge_facing_allowances.get(x)
            )

        # # if we have known seam allowances we can filter and re-rank notches
        if edge_seam_allowances:
            if sum(edge_seam_allowances.values()) != 0:
                # filter and later re-rank
                df = dataframes.column_as_group_rank(
                    df, ["nearest_edge_id", "distance_to_edge"], "edge_order"
                )
                df = dxf_notches.filter_my_adj(df)
            else:
                res.utils.logger.warn(
                    f"We cannot filter notches because we dont have seam allowance values"
                )

        # turn some values into ranks
        for r in [
            "ordered_from_avatar_center",
            "ordered_from_avatar_outside",
            "clockwise_order",
            "edge_order",
        ]:
            df = dataframes.column_as_group_rank(
                df, ["nearest_edge_id", "distance_to_edge"], r
            )

        edge_notch_count = dict(
            df[df["distance_to_edge"] == 0]
            .groupby("nearest_edge_id")[["edge_order"]]
            .max()
            .reset_index()
            .values
        )
        edge_notch_count = {int(k): v for k, v in edge_notch_count.items()}

        def is_corner_notch(row):
            try:
                max_notch = edge_notch_count[int(row["nearest_edge_id"])]
                pre_check = row["edge_order"] == 0 or row["edge_order"] == max_notch
                dist = row["to_corner_distance"]
                # TODO: SA this is a safety because some notch in the middle of the edge
                # is not a corner notch even if it is the fitst/last
                # if we add SA notches that are missing this should be redundant so really
                # this is just to avoid confusion
                return pre_check and dist < 300
            except Exception as ex:
                res.utils.logger.warn(
                    "unable to determine some corner notches maybe due to a bad edge mapping"
                )
                return False

        df["last_notch"] = (
            df["nearest_edge_id"].map(int).map(lambda e: edge_notch_count.get(e))
        )
        df["is_corner"] = df.apply(is_corner_notch, axis=1)
        expr = lambda row: (
            nline(row["geometry"])
            if row["is_corner"] != 1
            else corner_nline(row["geometry"])
        )

        if len(df) == 0:
            return df

        df["notch_line"] = df.apply(expr, axis=1)
        df["intersection_offset"] = df.apply(pline_points, axis=1)
        # lets try making the placement line the entire offset surface
        df["placement_line"] = df["intersection_offset"].map(pline)
        df["placement_line"] = df.apply(clip_near_notch, axis=1)

        # need to refactor but need to refresh some values after adding
        if edge_seam_allowances:
            if sum(edge_seam_allowances.values()) != 0:
                try:
                    res.utils.logger.debug(f"filtering adj edges...")
                    # this is a hack to refresh some values - i need to update this now that we are removing edges and need to recompute
                    df = dxf_notches.filter_my_adj(df)
                except Exception as ex:
                    print(
                        "FAILED TO FILTER ADJ EDGES - THERE ARE MAYBE SOME STRANGE SHAPES"
                    )
                    res.utils.logger.warn(f"Failed filter adjacent")
            else:
                res.utils.logger.warn(
                    f"Cannot filter notches without non zero values SAs"
                )
        # df["notch_line"] = df["geometry"].map(
        #     lambda pt: dxf_notches.notch_line(pt, egeom)
        # )

        if plot:
            from geopandas import GeoDataFrame
            from matplotlib import pyplot as plt

            df["geometry"] = df["geometry"].map(lambda g: g.buffer(5))

            geoms = [egeom]

            if include_notch_lines:
                geoms += list(df["notch_line"].values)
            if include_placement_lines:
                geoms += list(
                    df["placement_line"].dropna().map(lambda g: g.buffer(2)).values
                )

            if edge_seam_allowances and len(df):
                res.utils.logger.debug(
                    f"drawing seam allowances: {edge_seam_allowances}"
                )
                sa = dotted_lines(
                    unary_union(
                        df["nearest_edge_id"].dropna().map(my_seam_allowance).values
                    )
                )

                geoms += list(sa)

            ax = GeoDataFrame(df).plot(
                color=df["nearest_edge_id"].map(lambda i: dxf_plotting.colors[int(i)]),
                figsize=(20, 20),
            )
            GeoDataFrame(geoms, columns=["geometry"]).plot(ax=ax)

            plt.gca().invert_yaxis()

        return df

    @staticmethod
    def nearby_surfaces(n, edges, buffer):
        """
        Using a radial buffer on the notch find edges nearby
        there should be at least one touching surface which is always returned first and a possible adjacent surface which is returned second
        This adjacent surface is the thing that provides the notch angle (sometimes at least)

        BUFFER: bug enough to catch an edge but small enough to ignore far edges or multiple segments over nearby notches
        there really should be just 2 edges though, edges is a linear rung and we just remove the corners
        in a region
        """

        corners = MultiPoint(magic_critical_point_filter(edges))

        # if we know we are a curner notch, see if there is "another" edge to use as our second edge

        es = n.buffer(buffer).intersection(edges - corners.buffer(5))

        # TODO: if we are not near a corner we dont have two relevant lines
        # Thisis a temporary fix because we do not really understand yet what the surfaces are like in general (2 inches from the notch?)
        if n.buffer(buffer).distance(corners) > 600:
            return [es, None] if es.type == "LineString" else [geom_get(es, 0), None]

        if es == None or es.is_empty:
            return [None, None]

        if es.type == "LineString":
            es = [es, None]

        # check if the other one is closer and reverse
        # contract is to return lines in sort order by closest
        min_distance = geom_get(es, 0).distance(n)
        if geom_get(es, 1) is not None:
            if geom_get(es, 1).distance(n) < min_distance:
                return list(reversed(list(geom_iter(es))))

        return es

    @staticmethod
    def adjacent_sa_notch_angle(n, outline, corners):
        """
        create two small segments at the corner and take angle of the one further from the notch
        """
        nc = nearest_points(n, corners)[-1]
        region = nc.buffer(150)
        segments = outline.intersection(region) - nc.buffer(5)
        further_segment = (
            segments[0]
            if n.distance(segments[0]) > n.distance(segments[1])
            else segments[1]
        )
        further_segment_angle = line_angle(further_segment)
        return further_segment_angle

    @staticmethod
    def notch_line(
        n, edges, is_corner=False, length=75, offset=None, surfaces=None, corners=None
    ):
        """
        the notch line is a useful object.
        It detemrines the direction of the notch on the shape and is important to get right.
        We can annotate at interporated distances the long it, or offsets of it that are "inside" or "outside" the notch

        """

        def orient_line_away_from_surface(g):
            pts = list(g.coords)
            p1 = Point(pts[0])
            p2 = Point(pts[1])
            if p1.distance(edges) > p2.distance(edges):
                return LineString([p2, p1])
            return g

        # hacking for now: dont know how to define corner notches yet
        # distance to corner
        corner_distance = corners.distance(n) + 20 if corners else 400
        NEARBY_RADIUS = 50 if not is_corner else corner_distance
        se = surfaces or dxf_notches.nearby_surfaces(n, edges, buffer=NEARBY_RADIUS)
        # se = [s for s in se if s is not None]

        # assert geom_len(se) > 0, "wooops! we found no nearby surfaces"

        if geom_get(se, 0) is None:
            return None

        # safety - improve testing
        if geom_len(se) == 1:
            se.append(None)

        # we can shave some of these surfaces to get the best approximation for the tangent
        # we want only a small segment of the line near the notch - if we are on the surface we can interporate left or right otherwise interpolrate a little away

        # THE NEAREST LINE SHOULD BE RETURNED TO USE AS OURS IF WE ARE NOT CORNER AND THE FURTHER ONE SHOULD BE THE ADJACENT FOR ANY MULTLINE LENGTH

        # if we are trusting our adjacent surfaces, we average the angles??
        # a = np.mean([line_angle(l) for l in se])
        adj_angle = None
        if is_corner:
            # l2 = geom_get(se, -1)
            # if l2 is not None and len(list(l2.coords)) > 0:
            #     adj_angle = line_angle(l2)
            adj_angle = dxf_notches.adjacent_sa_notch_angle(
                n, outline=edges, corners=corners
            )

        ep = Point(n.x + length, n.y)
        init_coords = [Point(n.x, n.y), ep]
        l = LineString(init_coords)
        try:
            # SA: the average of the angles may or may not be the best approx
            # if the three point angle is concave ?

            if is_corner:
                a = adj_angle
                # print("SET adj angle", a, n, NEARBY_RADIUS, len(se))
            else:
                c = chordify_line(se[0])
                ang = line_angle(c)
                # logic is if we are basically touching two edges we should average their angles rather than use one or the other
                if (
                    se[1] is not None
                    and abs(se[0].distance(n) - se[-1].distance(n)) < 5
                ):
                    res.utils.logger.debug(
                        f"Using the average surface angle for this notch line: {l}"
                    )
                    ang = np.mean([line_angle(l) for l in se])

                a = ang + 90

            rl = (
                rotate(l, a, origin=init_coords[0])
                if a is not None and abs(int(a)) > 0
                else l
            )
            assert rl is not None, "a rotated line is null"
            coords = list(rl.coords)
            ep = Point(coords[-1])
            if not Polygon(edges).contains(ep):
                # res.utils.logger.debug(f"rotating the line {rl} into the surface")
                rl = rotate(rl, 180, origin=coords[0])

            rl = orient_line_away_from_surface(rl)

            return rl
        except Exception as ex:
            # raise ex
            # warning on this - need to understand it more
            # print(f"failed to rotate a notch line {repr(ex)}")
            return l  # orient_line_away_from_surface(l)

    @staticmethod
    def placement_line(
        n,
        edges,
        # this is half the samllest seam allowance?
        r=HALF_SMALL_SEAM_SIZE,
        is_corner=False,
        surfaces=None,
        surface_buffer=100,
    ):
        """
        the placement line is normally a function of the surface
        but we can alsp default to a 90 rotation of the notch line which is the same or a good approx
        """
        l = dxf_notches.notch_line(n, edges, is_corner=is_corner)

        p1 = l.interpolate(0)
        p2 = l.interpolate(HALF_SMALL_SEAM_SIZE)
        dispx = p2.x - p1.x
        dispy = p2.y - p1.y
        # orient the notch line into the shape
        # return inside_offset(center_line, edges, r=SMALL_SEAM_SIZE)
        default = affinity.rotate(l, 90)
        # # we only need a small nearby surface to define a placement line IF its just a parallel surface
        se = surfaces or dxf_notches.nearby_surfaces(n, edges, buffer=surface_buffer)
        if se[0] is None:
            return default
        # radiall around the notch about the size of the seam for now
        # we get a fixed size surface line that we will offset inside
        # up to an inch can be used to place symbols 1-> 4
        se = n.buffer(1 * SMALL_SEAM_SIZE).intersection(se[0])

        return affinity.translate(se, xoff=dispx, yoff=dispy)

        # the inside offset is not quite right because at angles it moves away from the center of the notch
        # Do the above instead

        return inside_offset(se, edges, r=r)


class dxf_plotting:
    colors = ["k", "b", "y", "g", "r", "orange", "gray", "yellow"]

    staticmethod

    def plot_sized_geometries(
        piece_image,
        row,
        geometries=["outline", "viable_surface", "notches", "internal_lines"],
        poffset="left",
        **kwargs,
    ):
        """
        Supply
        """
        from geopandas import GeoDataFrame
        from matplotlib import colors
        from matplotlib import pyplot as plt
        from res.media.images.geometry import geometry_to_image_space

        figsize = kwargs.get("fig_size", (20, 20))
        plt.figure(figsize=figsize)
        ax = plt.gca()
        plt.imshow(piece_image)
        plt.axis("off")

        def try_map_color(g):
            try:
                p = list(g.coords[0])
                if len(p) == 3:
                    return int(float(p[-1]))
                return 0
            except:
                return 0

        for g in geometries:
            g = geometry_to_image_space(row, g)

            try:
                gg = GeoDataFrame(g, columns=["geometry"])
            except:
                # todo relection to know if its iterable
                gg = GeoDataFrame([g], columns=["geometry"])

            gg["z"] = gg["geometry"].map(try_map_color)

            colors = ["k", "b", "y", "g", "r", "orange", "gray", "yellow"]
            gg.plot(ax=ax, color=gg["z"].map(lambda i: colors[int(i)]))

        return ax

    @staticmethod
    def plot_viable_location_segments(gdf, **kwargs):
        """
        using a dataframe with viable segments for a label, plot it
        """
        # gdf = dxf.get_viable_piece_surface_label_locations_inches(PIECE_KEY)
        gdf["ordinal"] = gdf["viable"].map(lambda x: int(x)).cumsum()
        gdf["color"] = gdf["viable"].map(lambda x: "green" if x else "red")
        gdf["size"] = gdf["ordinal"].map(lambda x: 2 if x != 1 else 10)
        import geopandas

        return geopandas.GeoDataFrame(gdf).plot(
            color=gdf["color"], linewidth=gdf["size"], figsize=(20, 20)
        )

    @staticmethod
    def plot_geometries(df, idx, properties=None):
        """
        WIP plotting - take some things in the geometries and plot them for a given piece idx

        Use  `res_geometry_piece_layer` to get a dataframe with shapes of interest


        """

        def g_from_row(row):
            # todo make this params
            # see the
            return [
                row["resonance_color"]["geometry"],
                row["resonance_color"]["label_corners"],
                row["viable_surface"],
            ]

        from geopandas import GeoDataFrame

        ax = GeoDataFrame(g_from_row(df.iloc[idx]), columns=["geometry"]).plot(
            figsize=(20, 20)
        )

        return ax

    @staticmethod
    def plot_piece(
        key,
        base_path,
        label=None,
        seam=None,
        postition=None,
        seam_allowance_height=None,
    ):
        """
        if seam, the numbered sum from some localtion is used, we place anyway along it on the viable surface

        Example params
        base_path = "s3://meta-one-assets-prod/color_on_shape/cc_6001/v8/perpit/2zzsm/pieces"
        key='CC-6001-V8-YOKE_SM_000' <- this may change when names change (look fora  res.key in the data)

        """
        import res
        from matplotlib import pyplot as plt

        s3 = res.connectors.load("s3")
        im = s3.read(f"{base_path}/{key}.png")
        plt.figure(figsize=(30, 30))
        plt.imshow(im)
        return im

    @staticmethod
    def plot_numbered_internal_lines(row):
        """
        This is just to see them numbered
        """
        from geopandas import GeoDataFrame
        from matplotlib import pyplot as plt

        il = row["internal_lines"]
        if il.type == "LineString":
            il = [il]

        df = GeoDataFrame(il, columns=["geometry"]).reset_index()
        ax = df.plot(figsize=(20, 20))
        plt.gca().invert_yaxis()

        kps = {}
        for k, v in df.iterrows():
            l = v["geometry"]
            pt = l.interpolate(l.length / 3)

            kps[str(k)] = pt

        for k, v in kps.items():
            v = (v.x, v.y)
            t = ax.text(
                *v,
                k,
                size=15,
                ha="center",
                va="center",
            )

    @staticmethod
    def plot_viable_surface(piece_image, row, poffset="left", **kwargs):
        """
        Supply
        """
        from geopandas import GeoDataFrame
        from matplotlib import colors
        from matplotlib import pyplot as plt
        from res.media.images.geometry import geometry_to_image_space

        g = geometry_to_image_space(row, "viable_surface")
        figsize = kwargs.get("fig_size", (20, 20))
        plt.figure(figsize=figsize)
        ax = plt.gca()
        plt.imshow(piece_image)
        gg = GeoDataFrame([l for l in g], columns=["geometry"])
        # if this fails its because we have not added z to our viable surface
        gg["z"] = gg["geometry"].map(lambda g: g.coords[0][2])

        gg["geometry"] = gg["geometry"].map(lambda g: g.buffer(10))

        colors = ["k", "b", "y", "g", "r", "orange", "gray", "yellow"]
        gg.plot(ax=ax, color=gg["z"].map(lambda i: colors[int(i)]))
        lines = [l for l in g]
        # TODO: we should be able to determine the poffset based on the sense
        GeoDataFrame(
            [l.parallel_offset(100, poffset) for l in lines], columns=["geometry"]
        ).plot(ax=ax)

    @staticmethod
    def plot_geometry_keypoints(im, row, pose="right"):
        from matplotlib import pyplot as plt
        from res.media.images.geometry import (
            get_geometry_keypoints,
        )

        # pt = geometry_to_image_space(row, geometry)
        g = row["geometry"]
        kps = get_geometry_keypoints(g, pose=pose)

        plt.figure(figsize=(12, 12))
        ax = plt.gca()
        plt.imshow(im)
        for k, v in kps.items():
            # print(k, v)
            if isinstance(v, tuple):
                continue
                # v = v[0]
            v = (v.x, v.y)
            t = ax.text(
                *v,
                k,
                size=15,
                ha="center",
                va="center",
            )
        plt.axis("off")
        return kps

    @staticmethod
    def plot_labeled_edges(dxf, size=None, zip_archive=None):
        """
        a way to show what we know (for relevant geometries) about a piece

        changing strategy so computed fields like edges and corners are directly in image space
        """
        from geopandas import GeoDataFrame
        from matplotlib import colors
        from matplotlib import pyplot as plt
        from res.media.images.geometry import (
            geometry_to_image_space,
        )
        import io

        df = dxf.get_sized_geometries(size or dxf.sample_size)
        info = ""
        for _, row in df.iterrows():
            res.utils.logger.debug(row["key"])
            # g = geometry_to_image_space(row, "edges")
            g = row["edges"]
            df = GeoDataFrame(g, columns=["geometry"])
            colors = [
                "k",
                "b",
                "y",
                "g",
                "r",
                "orange",
                "gray",
                "yellow",
                "cyan",
                "teal",
            ]

            df["c"] = df.index.map(lambda i: colors[i % len(colors)])
            plt.figure()
            ax = df.plot(figsize=(18, 18), color=df["c"])

            try:
                ilines = GeoDataFrame(
                    geometry_to_image_space(row, "internal_lines"), columns=["geometry"]
                )
                ilines.plot(ax=ax)
            except:
                pass

            try:
                notches = GeoDataFrame(
                    geometry_to_image_space(row, "notches"), columns=["geometry"]
                )
                notches.plot(ax=ax)

            except:
                pass

            # these are important because they determine the edges
            try:
                corners = GeoDataFrame(
                    # geometry_to_image_space(row, "corners").buffer(25),
                    row["corners"].buffer(25),
                    columns=["geometry"],
                )

                corners.plot(ax=ax)
                info = f" - Corner count = {geom_len(row['corners'])}"
            except:
                pass
            ax.set_title(row["key"] + info, fontsize=24)
            plt.gca().invert_yaxis()
            plt.axis("off")

            # todo annotate number and colour edges

            if zip_archive:
                buf = io.BytesIO()
                plt.savefig(buf)
                plt.close()
                img_name = f"{row['key']}.png"
                zip_archive.writestr(img_name, buf.getvalue())

    @staticmethod
    def geom_thumb(geom, scale=5, as_svg=False):
        """
        simple utility to create little visual thumbs of outlines with extra context
        an example of why this is useful is we want to number all edges so they are 0,1 ... going clockwise but testing for all cases can be slow
        if we assign black and ping to the first two colours and then add other colours, we can
        see if the orietnation and colur assignment is correct for all pieces

        Example:
        - the viable surface is Z-Linestring where the third component is an edge id assigned by numbering segments between keypoints or corners
        - if keyponts are known we use them otherwise we infer by conventions

        geom =pieces.iloc[0]['viable_surface']
        im = geom_thumb(geom)


        #SVG(d) #if option to return SVG

        """

        import io

        from cairosvg import svg2png
        from PIL import Image

        def as_svg_path(l):
            l = np.array(l)
            colours = [
                "black",
                "pink",
                "#b91919",
                "#f59e11",
                "#890f60",
                "#e4e3e3",
                "#35592e",
                "#ecc5e6",
                "#ffe2eb",
                "#d0ccea",
                "#a4b4da",
                "#4f6fae",
            ]

            id_ = int(l[:, -1].min())
            points = " ".join([f"{p[0]},{p[1]}" for p in l])

            return f"""<polyline points="{points}" id="{id_}" fill="none" stroke="{colours[id_]}" stroke-width=".5"/>"""

        vs = shift_geometry_to_origin(invert_axis(swap_points(geom)))
        mw, mh = int(vs.bounds[-2] * scale + 20), int(vs.bounds[-1] * scale + 20)
        d = "\n".join([as_svg_path(l) for l in vs])
        d = f"""<svg xmlns="http://www.w3.org/2000/svg" width="{mw}" height="{mh}">
        <g transform="translate(10,10) scale({scale} {scale})">
        {d}
        </g>
        </svg>"""
        if as_svg:
            return d

        return Image.open(io.BytesIO(svg2png(d)))

        # plot edges - check colours for ids
        # edges = row['edges'] - CP <- remove curner points somehow
        # lines = list(swap_points(edges))
        # df = GeoDataFrame(lines, columns=['geometry'])
        # df['id'] = df['geometry'].map(lambda l : list(l.coords)[0][-1]).astype(int)
        # df['id'] = df['id'].map(lambda x : [ 'k', 'r', 'b', 'y', 'g', 'gray' , 'orange' ][x])
        # df.plot(color=df['id']).plot(figsize=(20,20))
        # plt.axis('off')
        # df

        # show the critical point used to pin the starting corner
        # g = row['outline']
        # pts = np.array(g)
        # com = list(pts.mean(axis=0))
        # def near_to(x):
        #     q = g.interpolate(g.project(Point(x)))
        #     return q
        # ref = near_to([g.bounds[0] - 5, com[1]-.5])
        # GeoDataFrame([g, ref.buffer(.1), Point(-5, com[1])],columns=['geometry']).plot()
        # # this is the EAST point in ASTM space - if the model is somewhere weirdly offset to the right this is wrong
        # # todo go left of the bounds


LAYER_DEF = {
    1: {
        "definition": "Piece boundary",
        "purpose": "Outline of each pattern piece and style system text",
    },
    2: {
        "definition": "Turn points",
        "purpose": "Turn points for layers 1, 8, 11, 14",
    },
    3: {
        "definition": "Curve points",
        "purpose": "Curve points for layers 1, 8, 11, 14",
    },
    4: {
        "definition": "Notches; V-notch and slit-notch; alignment.",
        "purpose": "Articulation of molding; I-shape or V-shape: alignment pieces",
    },
    5: {
        "definition": "Grade reference and alternate grade reference line(s)",
        "purpose": "Grading",
    },
    6: {"definition": "Mirror line", "purpose": "Symmetry of fold"},
    7: {"definition": "Grain line", "purpose": "Direction of fabric grain"},
    8: {
        "definition": "Internal line(s)",
        "purpose": "Graphic annotation of placement. Not cut.",
    },
    9: {
        "definition": "Stripe reference line(s)",
        "purpose": "Fabric alignment of stripes",
    },
    10: {
        "definition": "Plaid reference line(s)",
        "purpose": "Fabric alignment of chequers",
    },
    11: {"definition": "Internal cutouts(s)", "purpose": "Cutline inside of outline"},
    12: {"definition": "Intentionally left blank", "purpose": ""},
    13: {"definition": "Drill holes", "purpose": "Punch markers"},
    14: {"definition": "Sew line(s)", "purpose": "Line(s) indicate where to stitch"},
    15: {
        "definition": "Annotation text",
        "purpose": "Annotation, not style system text (1) or piece system text (1)",
    },
    80: {
        "definition": "T-notch",
        "purpose": "T-shape: slit with T-branch at end of notch",
    },
    81: {
        "definition": "Castle notch",
        "purpose": "U-shape: equal width, rectangular at end of notch",
    },
    82: {
        "definition": "Check notch",
        "purpose": "V-pointed notch, left or right side perpendicular to boundary",
    },
    83: {
        "definition": "U-notch",
        "purpose": "U-shape: equal width, semi-circle at end of notch",
    },
    84: {
        "definition": "Piece boundary quality validation curves",
        "purpose": "ASTM: Mandatory system information for polyline(s) layer 1",
    },
    85: {
        "definition": "Internal lines quality validation curves",
        "purpose": "ASTM: Mandatory system information for polyline(s) layer 8",
    },
    86: {
        "definition": "Internal cutouts quality validation curves",
        "purpose": "ASTM: Mandatory system information for polyline(s) layer 11",
    },
    87: {
        "definition": "Sew lines quality validation curves",
        "purpose": "ASTM: Mandatory system information for polyline(s) layer 14",
    },
}


# map normed size names to gerber size names
SIZE_NAMES = {
    "XX": "XXS",
    "XSM": "XS",
    "SM": "S",
    "MED": "M",
    "LG": "L",
    "XLG": "XL",
    "2XL": "2X",
    "3XL": "3X",
    "4XL": "4X",
    #
    "0": "Size 0",
    "2": "Size 2",
    "4": "Size 4",
    "8": "Size 8",
    "10": "Size 10",
    "12": "Size 12",
    "14": "Size 14",
    "16": "Size 16",
    #
    "1": "One-Fits-All",
}


PIECE_TYPE_CODES = {
    "S": "Self (S)",
    "F": "Fusing (F)",
    "BF": "Block Fuse (BF)",
    "P": "Paper Control (P)",
    "X": "Stamper",
    # C: 'Combo (C)'
}


"""
snippets

#useful to look at the geometry in the dxf - there are some updates that mean the edge numbering is messed up out if image space as used in meta-one
from res.connectors.s3.datalake import list_astms
from res.media.images.providers.dxf import dxf_plotting
s3 = res.connectors.load('s3')
p = list_astms('TK-3075').iloc[0]['path']
dxf = s3.read(p)
dxf_plotting.plot_labeled_edges(dxf)

"""
