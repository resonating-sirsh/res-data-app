"""
The meta marker is the piece intelligence with backwards compatability for nest/make ONEs
It is a gate for onboarding styles
It contains geometric awareness of pieces including merging in Sew instructions
It can be used to label pieces
S3 is used to stored versioned meta markers and kafka holds the meta contract header
We can easily ingest to the meta marker data into other databases from S3 but there is not short-term benefit
see https://coda.io/d/Technology_dZNX5Sf3x2R/Style-MetaMarker-ONE-spec_suH1U#_luQyk


CONTRACTS:

Body level contacts  are Dxf.validation related
When creating the meta marker we absolutely must check that the files we say are there are actually there
We *should* check the sew annotations if we suport sew or warn -> are the annotations compiling with icons etc.



annotation -  contract

        #                         "dx": 0,
        #                         "dy": 0,
        #                         "edge_id": e.get("edge_id"),
        #                         "label_name": main_label,
        #                         "notch_id": None,
        #                         "position": None,
        #                         "placement_angle": 0,
        #                         "placement_position": pt,

"""

from argparse import ArgumentError
from cv2 import dft
from res.connectors.dynamo.DynamoConnector import DynamoTable

from res.flows.FlowContext import USE_KGATEWAY, FlowContext, FlowValidationException
from res.flows.dxa.res_color import *
from res.flows.meta.pieces import PieceName
from res.media.images.geometry import (
    filter_sensitive_corners,
    invert_axis,
    detect_outline_castle_notches,
    outside_offset,
    unary_union,
    zshift_geom,
    affinity,
    label_edge_segments,
    to2d,
    cascaded_union,
    Polygon,
    place_structure_element,
    swap_points,
    shape_size,
    geom_iter,
    geom_len,
    geom_list,
    orient,
    LinearRing,
    minimum_bounding_rectangle,
)
from res.media.images.outlines import (
    get_piece_outline,
    place_directional_color_in_outline,
    draw_lines,
)

from res.media.images.icons import get_size_label
from res.media.images.qr import get_one_label_qr_code

import res
import numpy as np
import json
from res.media.images.providers.dxf import (
    HALF_SMALL_SEAM_SIZE,
    SMALL_SEAM_SIZE,
    DxfFile,
)
from ..meta.construction import (
    load_template,
    get_template_keys,
    get_templates,
)

from res.utils.dataframes import not_null_or_enumerable
from res.utils.configuration import bool_env
import os
import traceback

RES_ENV = os.environ.get("RES_ENV")

DPI = 300
DPMM = 1 / 0.084667

# for now dynamo is hardcoded as the cache and this is the table name
CACHE_NAME = "res_meta_markers"

META_CONTRACT = {
    "one_spec_key": None,
    "style_id": None,
    "material_props": None,
    "size_map": None,
    "color": None,
    "piece_material_mapping": None,
    "material_properties": None,
    "passed_gates": [],
    "failed_gates": [],
    # can add piece validation errors separately on the marker but this level is all or nothing strict
    "validation_errors": [],
}

GEOMETRIES = [
    "geometry",
    "notches",
    "internal_lines",
    "viable_surface",
    "stripe",
    "plaid",
    "edges",
    "sew_lines",
    "corners",
]

EXCLUDE_LABELS = ["sa_annotation"]


BASIC_NOTCHES = [
    "position_notch",
    "sa_notch",
    "center_notch",
    "orientation_notch",
]

META_ONE_ROOT = "s3://meta-one-assets-prod/styles/meta-one"

# these are fields in asset types legacy and current print assets /prep print pieces
# that we use to slice meta ones or orders and generate a key - color code should be the same these days as we dont print/nest colors seprately
WHITELIST_ASSET_PAYLOAD_DISCRIMINATOR = [
    "uri",
    "one_number",
    "key",
    "value",
    "material_codes",
    "piece_types",
    "material_code",
    "piece_type",
    "color_code",
]


# ths is a piece-level function because its on the edes dataframe on the sew row for the piece
# we can get the sew dataframe once for making annotaions and a private method can display what it would look like as a function of the edge
# really want we are the annotations - it makes sense to only load them if we have sew data which we may not and thenonly when first creating annotations?
def make_template(row, template_factory=load_template):
    """
    This can be applied directly on the sew_edges object in the MetaOne <- which was ETL'd from airtable
    This creates a template which can be "embedded" onto a piece/edge which basically means
    converting it from an abstraction description to actually coordinates of edges and notch anchors

    This generates the full instruction for relative placement from an edge
    often there is a need to look up a template and do other things based on the op
    in such cases we store the templates in a config database
    typically the templates are ready to go as is and just need the embedding of piece and edge ids etc.

    #simple enough cases are:
    - the notches that just take the default alignment
    - the edges as opposed to notches that takes seam allowance operations
    - the more complex ones have offsets and ayb often come in groups


    """

    if pd.isnull(row):
        return row

    def notch_template(**kwargs):
        """
        notches compile this anchor contract
        """
        t = {
            "edge_id": None,
            "notch_id": None,
            "operation": None,
            # physical notches usually sit on the surface and may extrude or intrude into the shape
            # for example if we drew a castle notch it would be like a valley on the surface and a v would be a hill
            # the cutline is draw on the surfaces at the end but in image space we may also need a resizing op with care over geo placements
            "is_physical": False,
            "position": "near_edge_center",
            "dx": 0.5,
            "dy": None,
        }

        t.update(kwargs)

        return t

    d = {
        "piece_key": row.get("key"),
        "edge_id": row["edge_id"],
        "edge_annotations": [],
        "keypoint_annotations": [],
    }

    order = row.get("body_construction_order")
    if pd.notnull(order):
        order = int(order.split("-")[-1])
    else:
        order = None

    notches = []
    # making a decision to use the notch id or the notch pairing. but weird, re-check abstraction
    n = row.get("notch1", row.get("notch_id"))
    if n:
        notches.append(n)
    n = row.get("notch2")
    if n:
        notches.append(n)

    # this is a bit unfortunate but suggests a poor defintion of types

    # specials = ["sa_piece_fuse", "top_apply_in_seam_main_label"]
    # if there is a template that will override
    specials = get_template_keys()
    if row.get("category") == "sa_operation_symbol" and row.get("name") not in specials:
        d["edge_annotations"] = [
            {
                "operation": row.get("name"),
                "label": row.get("name"),
                "body_construction_order": order,
                "body_construction_key": row.get("body_construction_order"),
                "body_construction_sequence": row.get("body_construction_sequence"),
                "edge_id": row.get("edge_id"),
            }
        ]
        for i, n in enumerate(notches):
            d.update({f"notch_{i}": n})

    # expect this to work either for notch by notch definitions or notch sets on SE defs
    # for example the snip fusing situtation just puts this symbol on all notches on the edge
    elif row.get("category") == "symbolized_notch" and row.get("name") not in specials:
        for n in notches:
            t = notch_template(
                **{
                    "edge_id": row["edge_id"],
                    "notch_id": n,
                    "operation": row.get("name"),
                }
            )

            # change
            t["dx"] = 0.0
            # because i cannot recall how to configure this
            if row.get("name") == "outward_fold_notch":
                t["position"] = "near_edge_corner"
                t["dx"] = 50  # small delta actually for any directional

            # add all the notches defined on the sectional edge can be 1 or two in this case
            d["keypoint_annotations"].append(t)

    # above here are simple ones and now we get into
    else:
        d = template_factory(
            row.get("name"), row["edge_id"], row.get("key"), sectional_notches=notches
        )

    d["op_category"] = row.get("category")
    return d


def load_vs_export(
    path, size, unit_scaling=1 / 0.0084667, scale_sa=0.393701, debug=False
):
    """
    This will be refactored - just a WIP

    This does a bunch of data munging to unpack and rescale the VS export
    this allows is to compare our outlines to theirs and make sure that we map
    Their edge properties to whatever our edge ids are

    We keep a per piece column set
    - the VS Z-Shape with their ids which we can use for geo lookup[
    - edge properties using their ids

    later we can use the find-me function to lookup their attributes for our ids
    """

    import itertools

    from res.media.images.geometry import (
        tr_edge,
        shift_geometry_to_origin,
    )
    import itertools

    def get_scaled_geom(v):
        if geom_len(v) > 1:
            g = LineString([[k["y"], k["x"]] for k in v])
            g = scale_shape_by(unit_scaling)(g)
            return g

    def tag_geom(row):
        g = row["geometry"]
        idx = row.name
        g = tr_edge(g, idx)
        return g

    def merge_geometry(edges):
        l = [list(v.coords) for v in edges["geometry"]]
        l = list(itertools.chain(*l))
        l = [[p[0], p[1]] for p in l]
        return Polygon(l)

    def update_props(row):
        rp = row["raw_properties"]
        if isinstance(rp, str):
            rp = json.loads(rp)
        rp["seam_code"] = row["seam_code"]
        rp["key"] = row["key"]
        # depending on if we get cms or mms we need to be careful
        rp["seam_allowance"] = np.round(abs(rp["seam_allowance"]) * scale_sa, 3)

        return rp

    s3 = res.connectors.load("s3")
    if "P_" in size:
        # TODO : SA hack (i dont even know what the rule is anymore)
        size = size.replace("P_", "P-")
        #####################

    data = pd.DataFrame(s3.read(path))["sizes"].to_frame()

    if size not in list(data.index):
        raise Exception(
            f"The size {size} is not in the list of sizes {list(data.index)} read from the scan.json file: {path}"
        )
    data = pd.DataFrame(data.loc[size]["sizes"])
    edge_data = pd.DataFrame([d for d in data["pieces"]])

    data = []

    if debug:
        return edge_data

    for key, edges in edge_data.groupby("key"):
        # the edges we want are inside the edges as points (there are also points top level)
        edges = pd.DataFrame(edges["edges"].iloc[0])
        # we map reversing yx to our geometry system in their coord units
        edges["geometry"] = edges["points"].map(
            lambda x: LineString([[p["y"], p["x"]] for p in x])
            if geom_len(x) > 1
            else None
        )

        l0 = len(edges)
        # filter dodgy ones but we should really warn
        edges = edges.dropna(subset=["geometry"]).reset_index().drop("index", 1)

        if len(edges) != l0:
            res.utils.logger.warn(
                f"We needed to filter a geometry from the edges in the scan json file {path} - maybe 1d lines?"
            )
        # we scale
        edges["geometry"] = edges["points"].map(get_scaled_geom)
        # we tag their id on each edge
        edges["geometry"] = edges.apply(tag_geom, axis=1)
        # we proceed to use the entire shape as context for moving things around to the right frame of reference
        # a reference geometry to do the inversion in the bounds
        # this is kind of funky - we want to keep the lines separate but transform them with respect to the entire shape - this is one way to do that
        # troubleshoot:> if this step fails the edges are not closed and this can happen if for example we have missing symmetric fold
        # E = list(polygonize(cascaded_union(edges["geometry"])))[0].exterior
        E = merge_geometry(edges)
        # now do the inversion
        edges["geometry"] = edges["geometry"].map(lambda p: invert_axis(p, E.bounds))

        # E = list(polygonize(cascaded_union(edges["geometry"])))[0].exterior
        E = merge_geometry(edges)

        edges["geometry"] = edges["geometry"].map(
            lambda p: shift_geometry_to_origin(p, E.bounds)
        )

        edges["raw_properties"] = edges.apply(update_props, axis=1)

        # make into a dict on the VS edge ids - we dont know ours yet
        prop_map = {p["key"]: p for p in edges["raw_properties"]}
        # take the geometry and all the properties combined for edges
        data.append(
            {
                "piece_key": key,
                "vs_edge_geometry": list(edges["geometry"]),
                "vs_edge_properties": prop_map,
            }
        )

    return pd.DataFrame(data)


def _body_version_from_body_code(root, body_lower):
    try:
        bversion = (
            root[root.index(body_lower) :].split("/")[1].split("_")[0].replace("v", "")
        )
    except:
        res.utils.logger.warn(
            f"Unable to extract a body version - bad conventions on body name in root"
        )
        bversion = 0

    return bversion


def try_merge_body(meta_one_path, body_dxf):
    color_fields = ["piece_name", "material_code", "color_code", "filename"]

    # if "/P-" in body_dxf:
    #     res.utils.logger.debug(
    #         "removing the petite size from the path - it should not be here"
    #     )
    #     body_dxf = body_dxf.replace("/P-", "/")
    # do we need to cast these
    geom_cols = ["geometry", "edges", "notches", "internal_lines"]

    s3 = res.connectors.load("s3")
    p = "/".join(body_dxf.split("/")[:-1]) + "/printable_pieces.feather"

    data = s3.read(meta_one_path)
    data["piece_name"] = data["key"].map(DxfFile.remove_size)

    if s3.exists(p):
        res.utils.logger.debug(f"merging the body to the color meta one")
        body_data = s3.read(p)
        data = pd.merge(body_data, data[color_fields], on="piece_name")
    else:
        res.utils.logger.debug(f"body file {p} does not exist")

    data["piece_key"] = data["piece_name"]
    return data


def read_meta_marker(root, geoms=GEOMETRIES):
    """
    The reader to read meta objects from S3
    This is used by the MetaMarker but can be used by other clients to just load the dataframe object
    Handles converting to shapely objects too

    TODAY we are using a mapped size to the sample size but in future we should join on non sized keys

    We load the sew interface exported from the construction logic
    WE expect this dataframe to exist on S3(edges.feather) but in future we load it from a database

    Contract:
    Geom Attributes:
     - Panel Internal Apply
     - Gender->Top
     - Gender->Under
     - Fold -> Interior
    """
    from shapely.wkt import loads as shapely_loads

    res.utils.logger.debug(f"Opening marker at root {root}")
    root = root.rstrip("/")

    # TODO body root which is where we will start brining in body level data from
    path = f"{root}/pieces.feather"

    # the sew interface is defined in the edge annotations which can subsume keypoints for now
    edges = f"{root}/edges.feather"

    meta_path = f"{root}/meta.json"

    s3 = res.connectors.load("s3")
    # the meta object may have some things we can use
    meta = s3.read(meta_path) if s3.exists(meta_path) else {}

    dxf_path = meta["metadata"]["meta_path"]

    def try_load(s):
        try:
            return (
                shapely_loads(s) if not pd.isnull(s) and s.lower() != "none" else None
            )
        except:
            return None

    def body_from_key(k):
        return "-".join(k.split("-")[:2])

    # merge pieces from the body beyond legacy
    meta_marker = try_merge_body(path, dxf_path)

    # this block adds a sizeless key to the marker
    size = meta_marker.iloc[0]["size"]
    body_lower = body_from_key(meta_marker.iloc[0]["key"]).lower().replace("-", "_")

    res.utils.logger.warn(
        f"Mapping lining pieces to self while we figure out naming conventions"
    )

    meta_marker.loc[meta_marker["type"].isin(["lining"]), "type"] = "self"

    for g in geoms:
        if g in meta_marker.columns:
            meta_marker[g] = meta_marker[g].map(try_load)

    def remove_version(s):
        parts = s.split("-")
        return "-".join([p for i, p in enumerate(parts) if i != 2])

    def has_attribute(x):
        def c(s):
            # consistent lower case no spaces
            return s.lower().replace(" ", "")

        def f(l):
            try:
                l = list(l)
            except:
                pass
            if isinstance(l, list):
                l = [c(item) for item in l]
                return c(x) in l
            return False

        return f

    # s3://meta-one-assets-prod/bodies/3d_body_files/tt_3036/v1/extracted/vs_export.json'
    # CONVENTION to get the body version that comes after the body. lots dangerous here
    bversion = _body_version_from_body_code(root, body_lower)

    # the body source is where the vs export lives and its called output.json
    vs_ext = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/v{bversion}/extracted/scan.json"

    if s3.exists(vs_ext):
        res.utils.logger.info(f"Loading the VS export from {vs_ext}")
        try:
            vs_ext = load_vs_export(vs_ext, size=size)

            # we use the versioned piece key (without size) unlike for sew where there is no version below
            meta_marker = pd.merge(meta_marker, vs_ext, on="piece_key", how="left")
        except Exception as ex:
            res.utils.logger.warn(
                f"There was a path to vs exports but the load failed maybe due to a parse error. check the file. {repr(ex)}"
            )
            # raise ex
    else:
        res.utils.logger.info(f"There is no vs export at path {vs_ext}")

    # if not s3.exists(edges):
    edges = f"s3://meta-one-assets-prod/bodies/sew/{body_lower}/edges.feather"

    # regardless of sew we need this
    meta_marker["piece_key_no_version"] = meta_marker["piece_key"].map(remove_version)

    if s3.exists(edges):
        # this is all read interface stuff that could be moved into the meta marker but keeping it here for now because
        # we dont know how the ETL interface will change anyway
        res.utils.logger.debug(f"adding edges from {edges}")
        try:
            df = s3.read(edges)
        except:
            res.utils.logger.warn(f"the sew edges exit but are invalid")
            return meta_marker

        # new versions
        if "piece_key_no_version" in df:
            meta_marker = pd.merge(
                meta_marker, df, on="piece_key_no_version", how="left"
            )

            any_of = lambda x: has_attribute("Fold -> Interior")(x) | has_attribute(
                "Interior"
            )(x)
            meta_marker["is_interior"] = meta_marker["sew_geometry_attributes"].map(
                any_of
            )

            meta_marker["has_pairing_qualifier"] = meta_marker[
                "sew_geometry_attributes"
            ].map(has_attribute("Panel Internal Apply"))

            # Avatar - Top, Avatar - Under
            # Pockets and Panels use front and back from piece name
            meta_marker["identity_qualifier"] = meta_marker[
                "sew_geometry_attributes"
            ].map(
                lambda x: "-top"
                if has_attribute("Gender->Top")(x) or has_attribute("Position ->Top")(x)
                else "-under"
                if has_attribute("Gender->Under")(x)
                or has_attribute("Position ->Under")(x)
                else None
            )

            def extract_templates(df):
                """
                This pattern takes a list of sew instructions per row, extracts it on the key index into a flat list
                Then maps the row template op f(sew_row) -> template
                Then groups again by the index applying a list to pack the list up again and the rejoins the templates onto the original frame

                """
                data = df[["sew_edges"]].explode("sew_edges")
                data["edge_templates"] = data["sew_edges"].map(make_template)
                return df.join(
                    data[["edge_templates"]].groupby(data.index).agg(list), how="left"
                )

            res.utils.logger.debug(f"extracting edge templates...")

            meta_marker = extract_templates(meta_marker)
    else:
        res.utils.logger.warn(f"There is no sew edge export at path {edges}")

    return meta_marker


def _try_as_list(o):
    """
    if we expect something to be a list the checks can be annoying
    or at least, im not sure the right way to do this for all cases because of how pandas handles nulls on list cols
    """
    try:
        return list(o)
    except:
        return []


def _pocket_square_model(il):
    """
    starting to mock up internal line annotations - pure and utter tedium
    this gets the square part of the pocket and corner keypoints
    then it draws small inlines from the top at certain distances
    """
    pocket_square = []
    for l in il[-2:]:
        pocket_square += list(l.coords)
    pocket_square = MultiPoint(pocket_square)

    bounds = pocket_square.bounds
    pocket_square_kps = [
        nearest_points(pocket_square, Point(bounds[0], bounds[1]))[0],
        nearest_points(pocket_square, Point(bounds[0], bounds[3]))[0],
        nearest_points(pocket_square, Point(bounds[2], bounds[1]))[0],
        nearest_points(pocket_square, Point(bounds[2], bounds[3]))[0],
    ]

    def interpolate_along(pts, d):
        p = LineString(pts).interpolate(d)
        return LineString([pts[0], p])

    def point_of(l, i):
        lst = list(l.coords)
        return Point(lst[i])

    l1 = interpolate_along(pocket_square_kps[:2], 0.5 * 300)
    l2 = interpolate_along(pocket_square_kps[2:], 0.5 * 300)
    l3 = interpolate_along([pocket_square_kps[0], pocket_square_kps[2]], (3 / 16) * 300)
    l4 = interpolate_along([pocket_square_kps[2], pocket_square_kps[0]], (3 / 16) * 300)
    l1 = LineString([point_of(l3, 1), point_of(l1, 1)])
    l2 = LineString([point_of(l4, 1), point_of(l2, 1)])

    return [l1, l2, l3, l4]


def temp_hack_customize_internal_lines(row):
    from res.media.images.geometry import cascaded_union

    sl = row.get("sew_internal_line")

    if isinstance(sl, list) or isinstance(sl, np.ndarray):
        """
        disable mode
        contract:
        """

        if "Disable Internal Line" in sl:
            return None

    # if row["key"] in ["KT-2011-V9-PNTBKPNLRT-S_XS", "KT-2011-V9-PNTBKPNLLF-S_XS"]:
    #     s = row["internal_lines"]
    #     if pd.notnull(s):
    #         # pockets are inside offset
    #         # may want an interpolation factor to find a place for a notch key

    #         p = list(polygonize(s[:3]))[0].exterior
    #         p = inside_offset(p, p, r=(1 / 16) * 300)
    #         p = list(p - cascaded_union(magic_critical_point_filter(p)).buffer(5))
    #         # remove the top of the picket which is the edge 3 in our convention
    #         remove_idx = 3 if row["key"] == "KT-2011-V9-PNTBKPNLRT-S_XS" else 4
    #         p = [l for i, l in enumerate(p) if i not in [remove_idx]]

    #         p += _pocket_square_model(p)

    #         return p

    # if row["key"] in ["KT-2011-V9-BODFTPLKRT-BF_XS", "KT-2011-V9-BODFTPLKLF-BF_XS"]:
    #     il = row["internal_lines"]

    #     side = "left" if row["key"] == "KT-2011-V9-BODFTPLKLF-BF_XS" else "right"
    #     pts = points_from_bounds(il.bounds, side)
    #     pts = list(pts.values())
    #     pts.append(MultiPoint(pts).centroid)
    #     hull = MultiPoint(pts).convex_hull
    #     pts = MultiPoint(pts).buffer(37.5)
    #     res.utils.logger.debug("adding a line emphasis for internal lines")
    #     return il.intersection(pts).intersection(hull)

    #     row["res.internal_lines_emphasis"] = il.intersection(pts).intersection(hull)
    #     return il

    if row["key"] in ["KT-2011-V9-PNTFTPNLLF-S_XS", "KT-2011-V9-PNTFTPNLRT-S_XS"]:
        il = row["internal_lines"]
        # the first one seems to be the button holes
        il = MultiLineString([il[0]])
        return il

    # FOR sleeves we might get a way with making sure the center of the line is not too near the center of the piece
    # because the binding space is what we need and this is offset right now - this is all best effort stuff to buy time
    if row["key"] == "KT-3030-V5-SHTFTPNLLF-S_SM":
        row["internal_lines"] = unary_union(
            [l for i, l in enumerate(row["internal_lines"]) if i in [4, 5, 6, 7, 8]]
        )

    if "SLPNL" in row["key"]:
        # from inpsection the last multi lines is the placket binding
        il = row["internal_lines"]
        if not pd.isnull(il):
            if il.type == "LineString":
                il = [il]
            row["internal_lines"] = unary_union([l for i, l in enumerate(il) if i >= 7])

    # for plackets i think ill do this
    if PieceName(row["key"]).component == "PLK":
        outline = row["geometry"]
        # filter the ones that are near the edges away
        # more than an inch away we keep it
        ls = row["internal_lines"]
        if not pd.isnull(ls):
            if ls.type == "LineString":
                ls = [ls]
            lines = cascaded_union([l for l in ls if outline.distance(l) > 200])
            row["internal_lines"] = lines

    if isinstance(sl, list) or isinstance(sl, np.ndarray):
        """
        anytime we have a model we should return these and draw them but we can customize them first
        """

        return row["internal_lines"]
    return None


####################
###  Meta marker classes...
####################


class PieceSymbolsModel:
    """
    Abstracting out momentarily the logic or piece keypoints
    """

    def __init__(self, row, keypoints):
        self._row = row

        self._keypoints = []

        if len(keypoints):
            try:
                # only the surface points are used
                self._keypoints = keypoints[
                    keypoints["distance_to_edge"] == 0
                ].reset_index()
                self._keypoints["edge_id"] = self._keypoints["nearest_edge_id"].astype(
                    int
                )
                self._last_edge = int(self._keypoints["nearest_edge_id"].max())
                self._last_notches_per_edge = dict(
                    self._keypoints[["edge_id", "edge_order"]]
                    .groupby("edge_id")
                    .max()
                    .astype(int)
                    .reset_index()
                    .values
                )
            except Exception as ex:
                res.utils.logger.warn(
                    f"Unable to determine piece symbol model due to a bad notch mode {repr(ex)}"
                )
        self._name = row.get("piece_name")
        self._key = row.get("key")

    def __getitem__(self, eid):
        return EdgeSymbolsModel(eid, self)

    def __call__(self, template):
        template = dict(template)

        eid = template.get("edge_id")
        return self[eid].update_template(template)

    def notch_ordinal(self, edge, notch):
        return self._keypoints.query(
            f"edge_id=={edge} and edge_order=={notch} and distance_to_edge==0"
        ).iloc[0]["edge_index"]

    def notch_corner_sa(self, edge, notch):
        data = self._keypoints.query(
            f"edge_id=={edge} and edge_order=={notch} and distance_to_edge==0"
        )

        if len(data) == 0 or "my_min_corner_threshold" not in data.columns:
            # testing
            return 0
            raise Exception(f"Expecting edge/notch {edge}.{notch} which does not exist")

        return data.iloc[0]["my_min_corner_threshold"]

    def update_all_templates(self, templates):
        """
        lookup through each template and update it
        """
        try:
            if len(self._keypoints):
                return [self(t) for t in templates if pd.notnull(t)]
            else:
                res.utils.logger.warn(
                    f"We cannot update templates as we have no notches for pieces {self._key}"
                )
                return templates
        except Exception as ex:
            res.utils.logger.warn(
                f"Unable to update all templates in the piece model - maybe there are no valid ones in self edge_templates:> {repr(ex)}"
            )

            return templates


class EdgeSymbolsModel:
    """ """

    def __init__(self, edge, piece_model):
        self._piece_model = piece_model
        self._edge = edge
        # seam allowance

    @property
    def next_edge(self):
        return (self._edge + 1) % (self._piece_model._last_edge + 1)

    @property
    def prev_edge(self):
        return (self._edge - 1) % (self._piece_model._last_edge + 1)

    @property
    def prev_sa_notch(self):
        e = self.prev_edge
        # for non sectional edges
        n = self._piece_model._last_notches_per_edge[e]
        # for sectional edges
        return {"edge_id": e, "notch_id": n}

    @property
    def next_sa_notch(self):
        e = self.next_edge
        # for non sectional edges
        n = 0
        # for sectionals
        return {"edge_id": e, "notch_id": n}

    @property
    def prev_facing_notch(self):
        e = self.prev_edge
        # for non sectional edges
        n = self._piece_model._last_notches_per_edge[e] - 1
        # for sectional edges
        return {"edge_id": e, "notch_id": n}

    @property
    def next_facing_notch(self):
        e = self.next_edge
        # for non sectional edges
        n = 1
        # for sectionals
        return {"edge_id": e, "notch_id": n}

    def notch_ordinal(self, edge, notch):
        return self._piece_model.notch_ordinal(edge, notch)

    def notch_seam_corner_allowance(self, item):
        n = item.get("notch_id")
        e = item.get("edge_id")
        if pd.notnull(n) and pd.notnull(e):
            return self._piece_model.notch_corner_sa(e, n)
        return 0

    def update_relative_seam_notch(self, item, sectional_notches=None):
        """
        update the dict with my values
        """
        rf = item.get("edge-ref", "self")
        notch = item.get("notch-ref")
        # get seam allowance on this edge

        # update dx with function of my sa(dx-ref)
        if rf == "self":
            item["edge_id"] = self._edge

            # for this edge we have [0,-1] -> [0, LAST]
            my_notches = [0, self._piece_model._last_notches_per_edge[self._edge]]
            # we resolve the edge using the notch ref
            if pd.notnull(notch):
                item["notch_id"] = my_notches[int(notch)]
            # we could warn but i think we can have some pieces with relative notches that have not ids e.g. geometric centers
            # but we should still be careful with the template if this is intended
        if rf == "next":
            if notch != 1:
                item.update(self.next_sa_notch)
            else:
                item.update(self.next_facing_notch)
        if rf == "prev":
            if notch != -2:
                item.update(self.prev_sa_notch)
            else:
                item.update(self.prev_facing_notch)

        # For the piece model we also map to image space for annotations
        for inch_measurement in ["dx", "dy"]:
            if not pd.isnull(item.get(inch_measurement)):
                # TODO: this is a temp hack because i think there is idempotency issue
                if abs(item[inch_measurement]) < 2:
                    item[inch_measurement] = item[inch_measurement] * DPI

        # if we have sectional edges we can replace the bounds of the edge
        # the notch -ref describes which notch we want e.g. first or last and then we can, in an immutable way add the actual id
        if sectional_notches:
            item["notch_id"] = sectional_notches[int(item["notch-ref"])]

        item["E"] = self._edge
        item["hash"] = res.utils.res_hash()

        sa = self.notch_seam_corner_allowance(item)

        # test that SAs returned for items
        # print(sa, item)

        # we can make the dx a function of the seam allowance on this edge - this is the corner seam for the adjacent allowance
        if sa and item.get("dx-ref"):
            # dx should be in pixels as seam allowance is in pixels
            # we need to see why the dx-ref was updated though - not sure if this is what we want
            item["dx"] = (item.get("dx-ref", 0) * sa) / 300.0
            res.utils.logger.debug(
                f"set relative seam allowance offset annotation {item['dx']}"
            )

        return item

    def update_template(self, template):
        """
        There are some contract validations we can do on notch frame after cleaning notches
        There must be an SA notch always! some things these can be cleaned away
        """
        # copy
        t = dict(template)

        t["edge_id"] = self._edge

        for s in t.get("edge_annotations", []):
            s["edge_id"] = self._edge

            # these are useful resolved values for defining the actual edge coordinate of the notch
            # this is more useful for some ops -> transfercne from the defined seam onto any (seam) operations on that seam
            if t.get("notch_0"):
                s["notch_0_ordinal"] = self.notch_ordinal(self._edge, t.get("notch_0"))
            if t.get("notch_1"):
                s["notch_1_ordinal"] = self.notch_ordinal(self._edge, t.get("notch_1"))

        sectional_notches = list(t.get("sectional_notches", []))

        updated_annotations = [
            dict(
                self.update_relative_seam_notch(t, sectional_notches=sectional_notches)
            )
            for t in t.get("keypoint_annotations", [])
        ]

        t["keypoint_annotations"] = updated_annotations

        return t


class _MetaMarkerPiece:
    """
    Meta pieces handle row lev`el Meta Marker i.e pieces
    This object can be used to handle geom/image processing such as intelligently labeling images
    """

    def __init__(self, parent, key):
        self._parent = parent
        df = parent._data
        self._row = dict(df[df["key"] == key].iloc[0])
        piece_key = self._row["piece_key"]
        # print type is on the request - the marker must be generated from now for knits with this
        self._row["print_type"] = parent._meta.get("print_type")
        # if the print type is Directional for sure, we will try block buffering - otherwise its old fashioned way
        self._row["requires_block_buffer"] = self._row["print_type"] == "Directional"

        try:
            v = piece_key.split("-")[2]
            # we should only do this for some pieces for now
            if piece_key.replace(v, "*") in [
                "TK-3087-*-TOPFTPNL-S",
                "TH-3002-*-BLSBKPNL-S",
                "TH-3002-*-BLSFTPNL-S",
            ]:  # self._parent.body in ["TH-3002"] or
                g = self._row["geometry"]
                # g = connect_small_gaps_between_corners(c, g)
                # self._row["geometry"] = g

                # g = self._row["geometry"]
                g = orient(Polygon(g).buffer(10).buffer(-10), sign=1).exterior
                # g = orient(Polygon(g), sign=1)

                res.utils.logger.warn(
                    f"Correcting for small surface cutlines etc. by closing the boundary on {g.type}"
                )

                if "Multi" not in g.type:
                    self._row["geometry"] = g
        except:
            pass

        # this is a very handy function to have around

        res.utils.logger.debug(f"Loading piece {key}")
        # 1 determine edge seps and corners unless we use VS - new approach is to try the dxf first
        # the corners must match the scale of the geometry if the exist

        fn = dxf_notches.projection_offset_op(self._row)

        self._corners = self["corners"]

        if not self._corners:
            # below is some legacy testing we should deprecate for new bodies
            self._corners = MultiPoint(
                magic_critical_point_filter(self._row["geometry"])
            )
            # hack
            if self._parent.body in [
                "DJ-6000",
            ]:  #  "TH-3002",
                res.utils.logger.debug(f"we have {len(self._corners)} corners")
                self._corners = filter_sensitive_corners(self._corners)
                res.utils.logger.debug(
                    f"after filtering we have {len(self._corners)} corners"
                )

                # we cannot implement this because we dont know difference between where to merge the chord
            if self._parent.body in ["TH-3002"]:
                l = geom_len(self._corners)
                res.utils.logger.debug(f"removing small chord corners on this body")
                # testing for iterations
                self._corners = _MetaMarkerPiece.filter_corners_using_corner_model(
                    self._corners
                )
                self._corners = _MetaMarkerPiece.filter_corners_using_corner_model(
                    self._corners
                )
                res.utils.logger.debug(f"{l}->{len(self._corners)}")

        self._num_edges = geom_len(self._corners) - 1  # JL? square has edges == corners

        # this stuff is pretty safe if we already have our geometries and corners

        self._edge_order_fn = fn
        self._edges = label_edge_segments(
            self._row["geometry"],
            fn=fn,
            corners=self._corners,
        )
        # self._row["edges"] = self.label_my_outline_points()
        self._row["edges"] = DxfFile.label_outline_edges_from_ref(
            self["geometry"], self["corners"]
        )
        # now we make the self['edges'] to be the geometry numpered as edges which is different

        # get seam allowances from the dxf if known - we should make sure they are scaled
        self._seam_allowances = None
        try:
            # we are going to make this more efficient but need to be sure we use the correct edge context at this state i.e. sorted edges
            # WARNING - here we are assuming that the units in the DXF are
            # WARNING - we are doing something complex here to match the piece info and DXF shape. need to make this clear somewhere else
            sas = self._parent._piece_info.get_seam_allowances_near_shape(
                self["piece_name"],
                self._edges,
                scale_to_pixels=True,
            )

            res.utils.logger.debug(f"Set SAs {self._seam_allowances}")

            edge_ids = [l.interpolate(0).z for l in self._edges]
            # seam allowances are a map to edge ids
            self._seam_allowances = dict(zip(edge_ids, sas))

            res.utils.logger.debug(f"Set SAs {self._seam_allowances}")
        except Exception as ex:
            print(f"Failed to add SAs using pieces.json {ex}")
        if not self._seam_allowances:
            res.utils.logger.debug(
                f"could not get seam allowances from body source - loading legacy way"
            )
            # 2 get edge properties from VS - the legacy approach will not have them in the dxf so look up here
            self._seam_allowances = _MetaMarkerPiece.sas_from_vs(
                self._edges,
                self["vs_edge_geometry"],
                self["vs_edge_properties"],
            )

        # TODO ensure seam allowances which may be imported from external body data
        self._sew_line_distances = self._edge_distances_to_sew_lines()

        # 3. compute notches from DXF and or VS data
        # resolve notch model for the piece
        self._keypoints = dxf_notches.get_notch_frame(
            self._row,
            edge_seam_allowances=self._seam_allowances,
            known_corners=self._corners,
        )
        # TODO: we can filter keypoints that are inside the seam allowance in terms of distance to corner

        notches = (
            unary_union(self._keypoints["geometry"])
            if len(self._keypoints) > 0
            else None
        )
        t2d_notches = to2d(notches) if len(self._keypoints) > 0 else None
        corner_points = (
            unary_union([self._corners, t2d_notches]) if t2d_notches else self._corners
        )
        # update the viable surface from legacy dxf provider
        vs = self._edges - unary_union(corner_points).buffer(10)
        self._row["viable_surface"] = vs

        # self._row["viable_surface"] = label_edge_segments(
        #     vs, corners=self._corners, fn=fn
        # )

        # we dont know how to handle internal lines yet but we can pick some specifically via hard coding
        self._row["internal_lines"] = temp_hack_customize_internal_lines(self._row)
        # we use the distance of the adjacent edge - the corner notches defines the seam
        # this could break down if we are missing notches so needs some testing
        # we use max of either side but if there are some seams in the middle this does not work
        self._row["center_notches"] = self._get_center_notches()

        # need a better abstraction for this
        self._row["pairing_qualifier"] = PieceName.get_pairing_component(self._row)

        self._has_internal_sew_line_model = pd.notnull(self["sew_internal_line"])

        # these are relativity ops to orient things
        self._set_relative_offset_ops()

        # could be refactored because now we unpack edge annotations in here but the ones are returned or just for keypoints
        # side-effects self {"edge_annotations", "annotations", "edge_templates", "piece model"}
        self._annotations = self._add_annotations().to_dict("records")

    def plot_my_edges(self):
        from geopandas import GeoDataFrame
        from matplotlib import pyplot as plt

        df = GeoDataFrame(self._edges, columns=["geometry"]).reset_index()
        df["center"] = df["geometry"].map(lambda g: g.interpolate(0.5, normalized=True))
        df["idx"] = df["geometry"].map(
            lambda g: int(g.interpolate(0, normalized=True).z)
        )
        colors = ["k", "b", "y", "g", "r", "orange", "gray", "yellow"]
        ax = df.plot(color=df["index"].map(lambda i: colors[int(i)]), figsize=(20, 10))
        GeoDataFrame(list(self._corners), columns=["geometry"]).plot(ax=ax)

        plt.gca().invert_yaxis()

        for record in df.to_dict("records"):
            t = record["center"].x, record["center"].y
            ax.annotate(
                str(record["idx"]),
                xy=t,
                arrowprops=dict(facecolor="red", shrink=0.005),
            )

        return df

    def label_my_outline_points(self):
        """
        after segmenting edges, label the points on the outlines
        """
        points = []
        for pt in geom_iter(self["geometry"]):
            found = False
            # these are the edges that we segmented and labelled - its easier to label and store as segments
            for i, e in enumerate((self._edges)):
                i = e.interpolate(0).z
                if e.distance(Point(pt[0], pt[1])) == 0:
                    points.append(Point(pt[0], pt[1], i))
                    found = True
                    break
            # if the segments are not write on the corner we can use the same trick to see if we are on a corner
            # but the corners need to be sorted by the same projection function
            if not found:
                fn = dxf_notches.projection_offset_op(self._row)
                # sort the corners in the normal way
                scorners = sorted(list(geom_iter(self["corners"])), key=lambda g: fn(g))
                for i, c in enumerate(scorners):
                    if c.distance(Point(pt[0], pt[1])) == 0:
                        points.append(Point(pt[0], pt[1], i))
                        found = True
                        break
            if not found:
                raise Exception("unable to label all points on the edge")
        return LinearRing(points)

    @staticmethod
    def filter_corners_using_corner_model(c, plot=False, factor=300):
        def selector(row):
            if row["dprev"] < factor and row["angle"] > row["ang_prev"]:
                return True
            if row["dnext"] < factor and row["angle"] > row["ang_next"]:
                return True
            return False

        c = geom_list(c)

        df = pd.DataFrame(c, columns=["geometry"])

        df["prev"] = df.shift(1)["geometry"]
        df["next"] = df.shift(-1)["geometry"]

        df["prev"] = df["prev"].map(lambda x: x if pd.notnull(x) else c[-1])
        df["next"] = df["next"].map(lambda x: x if pd.notnull(x) else c[0])

        df["dprev"] = df.apply(
            lambda row: row["geometry"].distance(row["prev"]), axis=1
        )
        df["dnext"] = df.apply(
            lambda row: row["geometry"].distance(row["next"]), axis=1
        )
        df["angle"] = df.apply(
            lambda row: three_point_angle(row["prev"], row["geometry"], row["next"]),
            axis=1,
        )

        df["ang_prev"] = df.shift(1)["angle"]
        df["ang_next"] = df.shift(-1)["angle"]
        la, fa = df["angle"][len(df) - 1], df["angle"][0]

        df["ang_prev"] = df["ang_prev"].fillna(la)
        df["ang_next"] = df["ang_next"].fillna(fa)
        df["remove"] = df.apply(selector, axis=1)

        if plot:
            from geopandas import GeoDataFrame

            df = GeoDataFrame(df)
            df.loc[df["remove"], "geometry"] = df["geometry"].buffer(50)
            df.plot(figsize=(20, 20))
            return df

        return unary_union(df[~df["remove"]]["geometry"])

    @property
    def sew_df(self):
        return pd.DataFrame([d for d in self["sew_edges"]])

    @property
    def template_df(self):
        return pd.DataFrame([d for d in self["edge_templates"]])

    @property
    def annotations_df(self):
        """
        determine what is useful for testing
        """
        cols = [
            "flip",
            "operation",
            "dx",
            "dy",
            "E",
            "edge_order",
            "placement_position",
            "placement_line",
            "notch_line",
        ]
        df = pd.DataFrame(self._annotations)

        return df[[c for c in cols if c in df.columns]]

    @property
    def notched_outline(self):
        return self.get_notched_outline()

    @property
    def notched_piece_outline(self):
        """
        alias
        """
        return self.notched_outline

    @property
    def notched_piece_image_outline(self):
        """
        typically we take this outline from the image
        this falls back to a regular outline if there are no physical notches

        check the contract for this e.g. in nesting should be the same as the outlines
        """

        res.utils.logger.info(
            f"getting the piece image outline - will augment with physical notches if exists"
        )
        _, ol = self.get_piece_image_and_outline()

        if self.is_unstable_material:
            return ol

        ol = self.get_notched_outline(g=ol, match_terms=["v_notch"])

        return ol

    def match_physical_notches(self, match_terms):
        def mt(x):
            for t in match_terms:
                if t in x.lower():
                    return True

            return False

        kp = pd.DataFrame(self._annotations)

        if not len(kp):
            return kp

        kp = kp[
            [
                "distance_to_edge",
                "geometry",
                "placement_line",
                "operation",
                "is_corner",
                "to_corner_distance",
            ]
        ]
        kp = kp[(kp["distance_to_edge"] == 0)]

        if self.is_unstable_material:
            pass  # we are not filtering as we assuming everything is a v
        elif match_terms:
            kp = kp[kp["operation"].map(lambda x: mt(x) if pd.notnull(x) else False)]
        else:
            return pd.DataFrame()

        return kp

    def get_notched_outline(
        self,
        g=None,
        offset_angle_factor=270,
        match_terms=None,
        struct_element_buffer=30,
    ):
        """
        a property of the marker piece
        this is a structured outline that we can nest
        we can also create a notched piece outline if we prefer that source of truth
        these things can then be drawn on the shape
        we correct for image alignment

        when nesting, these objects are added to a nested dataframe so that they are centered on the piece
        """
        # get the typed keypoints or annotations later
        # loop through and get the structure element types
        # place the thing at the location

        kp = self.match_physical_notches(match_terms=match_terms)

        if len(kp) == 0:
            return self.piece_outline

        # NOTE the normal way to use this would be to just resolve or outline and not pass something in which is just for test. TODO: make it non null able
        g = g or self.outline_with_sa_castle_notches

        def magic_angle(row):
            """
            the angle of notch line or the dxf notch we have but not sure if we trust
            the placement line seems a stable enough way to determine the angle to approx the surface but requires some care
            we could maybe truncate the region for accuracy or do something more clever

            90 degrees means perp to the placement line
            if we run into seom trouble on the sense we can use symmetric structure elements which will not offset the boundary
            """
            pl = row["placement_line"]
            l = LineString([pl.interpolate(0), pl.interpolate(1, normalized=True)])
            return line_angle(l) + 90

        try:
            kp["angles"] = kp.apply(magic_angle, axis=1)  # p['notch_angles']
            # we buffer this outside of the placement location to erode surface lines

            res.utils.logger.debug(f"Applying to {len(kp)} physical notches")

            # deprecate flow

            V_NOTCH_CLOSE_TO_CORNER = 290
            kp["v_notches"] = kp[
                kp["to_corner_distance"] > V_NOTCH_CLOSE_TO_CORNER
            ].apply(
                lambda row: place_structure_element(
                    row["geometry"], row["angles"], factor=offset_angle_factor
                ).buffer(struct_element_buffer),
                axis=1,
            )

            shapes = list(kp["v_notches"].dropna())
        except:
            shapes = None
        if not shapes:
            return g
        # add also castle notches
        c = cascaded_union(shapes)
        # add the v notches that we added from the angle
        g = cascaded_union([c, Polygon(g)]).boundary

        # some smoothing logic
        # some_factor = 10
        # g = Polygon(g).buffer(some_factor).buffer(-1 * some_factor).exterior

        return g

    def get_edge(self, eid):
        """
        try to get the edge - need to check its a proper line segment and there are some boundary case issues
        should return a single line in any case
        - current sue case though is just to interpolrate to the center
        """

        def edge_id_fn(l, id):
            return list(l.coords)[0][-1] == id

        ls = [l for l in self._edges if edge_id_fn(l, eid)]

        return unary_union(ls) if len(ls) > 1 else ls[0]

    @staticmethod
    def sas_from_vs(edges, vs_edge_geometry, vs_edge_properties, scaler=300):
        """
        update our edge properties by linking to VS properties based on some geom look ups and respective edge ids
        the problem is we don't really know the edges do we ??
        future work will take critical corners from them perhaps ??

        scale by DPI
        """
        sas = {}
        edge_props = {}
        if not vs_edge_geometry:
            res.utils.logger.warn(
                f"There are no vs exports for this body. This affects seam allowances and notches"
            )
            return

        sas = {i: 0 for i in range(len(edges))}

        for e in geom_iter(edges):
            try:
                props = _MetaMarkerPiece._find_me_on_vs_geom(
                    e, vs_edge_geometry, vs_edge_properties
                )
                eid = props.get("eid")

                if eid is not None:
                    edge_props[eid] = props
                    # set the same allowances and add things on demand

                    sas[eid] = abs(props["seam_allowance"]) * scaler
            except Exception as ex:
                res.utils.logger.warn(
                    f"Failed to update the seam allowance from VS due to a missing entry maybe for the piece: {repr(ex)}"
                )
                raise ex

        res.utils.logger.info(f"Edge seam allowances: {sas}")
        return sas

    @staticmethod
    def _find_me_on_vs_geom(e, vs_edge_geometry, vs_edge_properties):
        """
        take a reference point on the edge
        then look at the VS edges and find the one closest to me
        Take its properties
        Show the mapping between my edge and their edge
        for each eid, we want to store properties
        """
        SMALL_DIST = 5
        pt = e.interpolate(e.length / 2)
        try:
            # the VS geometry is imported
            edges = list(vs_edge_geometry)
            attributes = list(vs_edge_properties.values())
            distances = [pt.distance(k) for k in edges]
            ref_edge = edges[np.argmin(distances)]
            ref_point = ref_edge.interpolate(SMALL_DIST)

            # get the attrs for the seam and add some ids
            d = attributes[np.argmin(distances)]
            d.update({"eid": pt.z, "vs_eid": ref_point.z})

            return d
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to apply seam allowance on edge {e} for piece: {repr(ex)}"
            )

        return {}

    def _edge_distances_to_sew_lines(p, pin_to_known=[1, 0.5, 0.375]):
        """
        Using a proxy for sew lines for now
        it should be safe if there are not sew lines in the seam allowance otherwise we need an explit later
        """
        il = p["internal_lines"]
        edges = p._edges
        d = {}
        for e in geom_iter(edges):
            pt = e.interpolate(e.length / 2)
            # these edges must have a z
            eid = pt.z
            pt = Point(pt.x, pt.y)
            d[eid] = pt.distance(il) / 300 if pd.notnull(il) else None

        return d

    def __repr__(self):
        return self._row.get("key")

    def _repr_svg_(self):
        g = self._row.get("geometry")

        return invert_axis(g)._repr_svg_()

    def __getitem__(self, key):
        return self._row.get(key)

    @property
    def piece_outline(self):
        return self._row["geometry"]

    @property
    def piece_name(self):
        return PieceName(self._row.get("key"))

    @property
    def is_body_flow(self):
        try:
            return "-body" in self._parent._meta["metadata"]["flow"]
        except:
            return False

    # we can runtime control these
    @property
    def sew_symbols_enabled_settings(self):
        v = False
        # v = True

        if (
            self._parent.body == "KT-2011"
            or self._parent._order_type.lower() == "sample"
            or self.is_body_flow
        ):
            v = True
        # we can also use the sales channel in future
        # and the default can come from RES_SEW_SYMBOL_ANNOTATIONS_ENABLED env
        return {
            "symbolized_notches": v,
            "identity": v,
            "construction_order": v,
            "ops": v,
            "piece_name": False,
        }

    ##############################

    @property
    def piece_image(self):
        if "s3://" in self._row["filename"]:
            s3 = res.connectors.load("s3")
            return Image.fromarray(s3.read(self._row["filename"]))
        else:
            # for testing noly - we really should stay on the cloud
            from res.media.images import read as imread

            return Image.fromarray(imread(self._row["filename"]))

    def get_piece_image_outline(self, image=None):
        """
        The piece image outline should be the same as the outline in the metadata but if not we can get the image outline
        Sometimes e.g. when things are buffered we may want to use this for nesting etc.
        """
        image = image if image is not None else self.piece_image

        return get_piece_outline(np.asarray(image))

    def get_piece_image_and_outline(self):
        """
        The piece image outline should be the same as the outline in the metadata but if not we can get the image outline
        Sometimes e.g. when things are buffered we may want to use this for nesting etc.
        """
        im = self.piece_image
        return im, get_piece_outline(np.asarray(im))

    @property
    def is_unstable_material(self):
        return (
            str(
                self._parent._meta.get("material_props", {})
                .get(self["material_code"], {})
                .get("material_stability")
            ).lower()
            == "unstable"
        )

    def get_labelled_piece_image(self, piece_instance=1):
        im = self.piece_image
        if self.is_unstable_material:
            return im
        try:
            return self._label(im, piece_instance=piece_instance)
        except Exception as ex:
            res.utils.logger.warn(
                f"Failing in marker {self._parent._home_dir}: {repr(ex)}"
            )
            raise ex

    @property
    def labelled_piece_image(self):
        return self.get_labelled_piece_image()

    def surface_distances(self, divs=100, image=None):
        """
        compute the distance on the surface of the DXF outline from the outline of the image
        any point that is not in the image is in negative space
        """
        o1 = self.piece_outline
        o2 = self.get_piece_image_outline(image)

        P = Polygon(o2)

        length = o1.length
        if o2.length < o1.length:
            length = o2.length

        pts = []
        for i in np.arange(0, length, divs):
            pts.append(o1.interpolate(i))

        pts2 = []
        for i in np.arange(0, length, divs):
            pts2.append(o2.interpolate(i))

        df = pd.DataFrame(pts, columns=["outline"])
        df["image_outline"] = pts2

        df["x"] = df["outline"].map(lambda g: g.x)
        df["y"] = df["outline"].map(lambda g: g.y)

        df["distance"] = df["outline"].map(lambda g: g.distance(o2))
        # add the nearest point

        df["contained"] = df["outline"].map(lambda g: P.contains(g))
        df.loc[~df["contained"], "distance"] = -1 * df["distance"]

        return df

    def get_surface_compensation_function(self, image=None):
        """
        factory up the surface distances optionally passing the image outline
        return a f(point)->distance_to_image_surface
        """
        from sklearn.neighbors import KDTree

        df = self.surface_distances(image=image)

        tree = KDTree(dft[["x", "y"]])

        def c(pt):
            d, idx = tree.query([[pt.x, pt.y]])
            # return the distance and the corrected point
            return df.loc[idx[0]].iloc[0]["distance"]

        return c

    def show_corner_sensitivity(self):
        """
        works with geometry filter sensitive corners
        a threshold value can be tuned for body pieces to not admint corners
        """
        from matplotlib import pyplot as plt

        a = self["geometry"]
        b = MultiPoint(magic_critical_point_filter(a))

        ref = Polygon(b)
        points = list(b.geoms)
        rem = points

        diffs = []
        for idx in range(len(points)):
            P = Polygon([p for i, p in enumerate(rem) if i != idx])
            diff = (ref.area - P.area) / ref.area

            diffs.append(diff)

        ax = pd.DataFrame(diffs).plot(figsize=(20, 10))
        plt.axhline(y=0, color="r", linestyle="-")
        ax.axhspan(0, 0.02, alpha=0.2)

        return a, b

    def show_labelled_piece_image(self, **kwargs):
        """
        This does the same thing as labelled piece image but if images are too big matplotlib is better than PIL
        Also, this currently is more representative of the computation that is happening even if in future we would load from cache

        """
        mplot = kwargs.get("use_matplot_lib")
        im = self._label(im)
        if mplot:
            from matplotlib import pyplot as plt

            plt.figure(figisze=(20, 20))
            plt.imshow(np.asarray(im))

            return im

        return im

    @property
    def annotated_image(self):
        return self._annotate_piece(self.piece_image)

    def get_min_bounding_box(self):
        return Polygon(
            minimum_bounding_rectangle(np.array(self["geometry"].coords))
        ).exterior

    def show_labelled_piece_image_placement_data(self):
        return self._label(self.piece_image, show_metadata=True)

    def _draw_notches(self, im):
        """
        this is a test function mostly
        """
        notches = self._keypoints
        im = np.array(im)
        if len(notches):
            for _, row in (
                notches[notches["distance_to_edge"] == 0][["notch_line", "edge_order"]]
                .dropna()
                .iterrows()
            ):
                row = dict(row)
                nl = row["notch_line"]
                order = int(row["edge_order"])
                im = draw_lines(im, nl, thickness=10)
                # annotate with text at the location near
                ce = Point(list(nl.coords)[-1])
                label = get_text_image(
                    f"N-{order}",
                    height=75,
                )

                options = {"placement_angle": 0, "placement_position": ce, "dy": 100}

                im = place_label_from_options(Image.fromarray(im), label, **options)

        return Image.fromarray(im) if isinstance(im, np.ndarray) else im

    def _annotate_piece(self, im):
        """
        for annotation support: add geometries over the images with numbering
        these are not prod annotations but the  raw geometries that inform what we will do
        """

        from res.media.images.text import PAL1

        row = self._row

        edges = self._edges
        internal_lines = self._row.get("internal_lines")
        sew_lines = self._row.get("sew_lines")

        fn = lambda g: inside_offset(g, shape=row["edges"])

        def fn(g, r=35):
            return inside_offset(g, shape=row["edges"], r=r)

        ilines = row.get("internal_lines", [])
        if ilines:
            if not isinstance(ilines, list) and ilines.type == "LineString":
                ilines = [ilines]
            for v in ilines:
                # TODO handle the polymorphism on line types better in draw lines
                im = draw_lines(im, v, dotted=True, thickness=2)

        for i, e in enumerate(edges):
            eid = int(list(e.coords)[0][-1])
            # draw with color TODO: warn if there are more lines that colors!
            im = draw_lines(im, e, color=(*PAL1[eid % len(PAL1)], 255), thickness=20)
            # annotate with text at the location near
            label = get_text_image(
                f"Edge {eid}", height=75, fill_color=(*PAL1[eid % len(PAL1)], 255)
            )
            edash = inside_offset(e, row["edges"])

            im = place_label_on_edge_segment(
                Image.fromarray(im),
                label,
                edash,
                inside_offset_fn=fn,
                fit_to_line=True,
                offset=edash.length / 2.0,
            )

        # pieces should really have notches generally but they may not
        # we should probably have intercepted if they do not and at least at the SA notches
        im = self._draw_notches(im)

        try:
            im = draw_lines(im, internal_lines, dotted=True, color=(255, 165, 0, 255))
            im = draw_lines(im, sew_lines, dotted=True, color=(255, 165, 0, 255))
        except:
            res.utils.logger.info(f"Failed to draw internal or sew line")

        return Image.fromarray(im) if isinstance(im, np.ndarray) else im

    @property
    def seam_allowance_lines(self):
        edge_id_fn = lambda g: int(np.array(g.coords)[:, -1].mean())
        lines = []
        if self._seam_allowances:
            for e in self._edges.geoms:
                eid = edge_id_fn(e)

                offset = self._seam_allowances.get(eid, 0)

                e = inside_offset(e, self._row["geometry"], r=offset)
                lines.append(e)
        return unary_union(lines)

    def seam_allowance_points(self, buffer=50, as_points=False):
        """
        geometrically, if you know your seam allowance lines, we can just compute where the cross the edges with buffering
        """

        ls = geom_list(self.seam_allowance_lines)

        def f(s):
            # orient the vector based - this is symmetric creating a segment around the center
            a, b = s.interpolate(0.4, normalized=True), s.interpolate(
                0.6, normalized=True
            )
            ss = LineString([a, b])

            return ss

        if len(ls):
            segments = self._row["geometry"].intersection(
                unary_union([e.buffer(buffer) for e in ls])
            )

            # create quarter or half segments near the
            segments = [f(l) for l in segments]

            return (
                segments if not as_points else segments.intepolate(0.5, normalzied=True)
            )

    def seam_allowance_points_and_notches(self):
        """
        geometrically, if you know your seam allowance lines, we can just compute where the cross the edges with buffering
        """
        segments = self.seam_allowance_points()
        g = self._row["geometry"]

        def make_notch(s):
            """
            the three sides coming in from the segment but without the segment
            """
            part = inside_offset(s, g, r=50)
            return unary_union([part, s]).convex_hull - s

        return segments, unary_union([make_notch(s) for s in segments])

    @property
    def outline_with_sa_castle_notches(self):
        _, b = self.seam_allowance_points_and_notches()
        G = Polygon(self["geometry"]) - b
        if G.type == "MultiPolygon":
            G = G[0]
        return G.exterior

    def draw_seams(self, im):
        edge_id_fn = lambda g: int(np.array(g.coords)[:, -1].mean())

        if self._seam_allowances:
            for e in self._edges.geoms:
                eid = edge_id_fn(e)
                offset = self._seam_allowances.get(eid, 0)
                e = inside_offset(e, self._row["geometry"], r=offset)
                # draw orange line for seam guides
                im = draw_lines(im, e, dotted=True, color=(255, 165, 0, 255))

            return Image.fromarray(im)
        return im

    @staticmethod
    def make_surface_offset_function(im, LABEL_SIZE=50, assume_vector=False):
        """
        Give a label size we construct an offset line inside the surface
        then for any geometry that is expected to be ON the surface but might not be,
        we project and translate the geometry onto the surface using its nearest point as a reference for distance
        """

        ol = get_piece_outline(np.asarray(im))
        # note we simplify once - as its WAAAAY faster and can get rid of silly errors of the topological kind
        ol = inside_offset(ol.simplify(1), ol, r=(LABEL_SIZE / 2) + 10)

        def f(g):
            try:
                # assuming the geometry is something that has end points for now
                end_points = (
                    g
                    if g.type == "Point"
                    else MultiPoint(
                        [g.interpolate(0), g.interpolate(1, normalized=True)]
                    )
                )

                # subtle - assume vector is like we know its a vector notch pointing from the surface so the start of the vector is supposed to be on the surface so move it
                p1 = (
                    nearest_points(end_points, ol)[0]
                    if not assume_vector
                    else g.interpolate(0)
                )
                pt = ol.interpolate(ol.project(p1))

                x, y = pt.x - p1.x, pt.y - p1.y

                if x + y < 0.01:
                    return g
                # res.utils.logger.debug(f"Translating {x},{y}")
                return translate(g, xoff=x, yoff=y)
            except Exception as ex:
                res.utils.logger.warn(f"Unable to do surface offset {repr(ex)}")
                return g

        return f

    def _label(self, im, **kwargs):
        """
        the full labelling is done here - seam ops, keypoints and lines
        """
        OFFSET_CENTER_THRESHOLD = 0.5 * 300
        RES_SEW_SYMBOL_ANNOTATIONS_ENABLED = bool_env(
            "RES_SEW_SYMBOL_ANNOTATIONS_ENABLED", True
        )

        add_annotations = True
        add_cutline = kwargs.get("add_cutline", True)
        piece_instance = kwargs.get("piece_instance", 1)
        use_compensation_functions = kwargs.get("use_compensation_functions", False)
        add_sewlines = kwargs.get("add_sewlines", False)
        # SA setting internal lines default off because we do not have enough from 3d
        add_internal_lines = kwargs.get("add_internal_lines", False)
        add_seams = kwargs.get("add_seams", os.environ.get("RES_IMAGE_DEV_LINES"))

        meta = self._parent._meta

        # determine centroid offset for e.g. buffered objects
        size_of_image = im.size
        size_of_outline = shape_size(self["geometry"])

        offset_center = (
            (size_of_image[0] - size_of_outline[0]) / 2.0,
            (size_of_image[1] - size_of_outline[1]) / 2.0,
        )

        if use_compensation_functions:
            res.utils.logger.debug("loading surface compensation function...")

        # comp_fn = self.get_surface_compensation_function(im)
        surface_geometry_offset_function = (
            (_MetaMarkerPiece.make_surface_offset_function(im))
            if use_compensation_functions
            else None
        )
        notch_offset_function = (
            _MetaMarkerPiece.make_surface_offset_function(
                im, LABEL_SIZE=1, assume_vector=True
            )
            if use_compensation_functions
            else None
        )

        # check threshold on offset center - if its significant in any direction, return the image
        if (
            offset_center[0] > OFFSET_CENTER_THRESHOLD
            or offset_center[1] > OFFSET_CENTER_THRESHOLD
        ):
            add_cutline = False
            add_internal_lines = False
            add_seams = False
            add_annotations = False

            res.utils.logger.info(
                f"Center offset is large {offset_center} - skipping annotations - will offset and seam annotations or notch annotations"
            )

            raise Exception(
                f"There is a large offset between outline and image - image: {size_of_image} outline {size_of_outline}"
            )
        #     # return im
        # else:
        #     res.utils.logger.debug(f"There is no center offset")
        #     offset_center = None

        # below control if the seam or symb notches are enabled

        # one label skip - this class is deprecated anyway
        if self.piece_name.name != "BDYONLBL-S":
            # for the one label and maybe for any label in future we dont show a one number

            im = label_seams(
                im,
                self._row,
                one_number=kwargs.get("one_number", self._parent.one_number),
                construction_sequences=meta.get("body_construction_sequences"),
                default_construction_seq=meta.get("body_construction_sequence"),
                show_metadata=kwargs.get("show_metadata"),
                seam_allowances=self._seam_allowances,
                offset_center=offset_center,
                surface_geometry_offset_function=surface_geometry_offset_function,
                # pass sew symbol options to control which symbols we show
                **self.sew_symbols_enabled_settings,
            )

        if add_seams:
            im = self.draw_seams(im)

        # RES_SEW_SYMBOL_ANNOTATIONS_ENABLED -> if we have enabled sym notches we want to use that switch below instead of the simple all ops
        # above we have check for seams
        symb_notches = self.sew_symbols_enabled_settings.get(
            "symbolized_notches", RES_SEW_SYMBOL_ANNOTATIONS_ENABLED
        )
        if add_annotations:
            for dt in self._annotations:
                label_name = dt["operation"]
                if label_name in EXCLUDE_LABELS:
                    continue
                pos = dt.get("position")
                flip = dt.get("flip", False)
                try:
                    nsymbol = notch_symbol(
                        label_name, position=pos, flip=flip, pose=self.piece_name.pose
                    )
                except Exception as ex:
                    res.utils.logger.warn(f"Failed to load notch symbol - continuing")
                    continue
                    raise Exception(
                        f"Failed to load notch symbol {label_name}: {repr(ex)}"
                    )

                if "notch" not in dt.get("operation", "") or not symb_notches:
                    #     # draw the notch line
                    nl = dt["notch_line"]
                    # determine some length for the line
                    pt2 = nl.interpolate(nl.length * 0.6)
                    pt1 = nl.interpolate(nl.length * 0.1)
                    nl = LineString([pt1, pt2])

                    if notch_offset_function is not None:
                        nl = notch_offset_function(nl)

                    im = Image.fromarray(
                        draw_lines(
                            im,
                            nl.parallel_offset(1, "right"),
                            thickness=11,
                            color=(0, 0, 0, 255),
                        )
                    )
                    im = Image.fromarray(
                        draw_lines(im, nl, thickness=3, color=(255, 255, 255, 0))
                    )
                    # for testing we can show the placement lines
                    if os.environ.get("RES_IMAGE_DEV_LINES"):
                        try:
                            im = Image.fromarray(
                                draw_lines(
                                    im,
                                    dt["placement_line"],
                                    thickness=1,
                                    dotted=True,
                                    color=(50, 50, 50, 255),
                                )
                            )
                            hpl = dt["placement_line"]

                            hpl = LineString(
                                [
                                    hpl.interpolate(0),
                                    hpl.interpolate(0.5, normalized=True),
                                ]
                            )
                            im = Image.fromarray(
                                draw_lines(
                                    im,
                                    hpl,
                                    thickness=5,
                                    dotted=False,
                                    color=(50, 50, 50, 255),
                                )
                            )
                        except:
                            print("failed drawing placement line")
                #######################

                # we add the icons optionally but we always draw the notches
                if label_name not in ["void"] and symb_notches:
                    # refactor - notion of a blank label - not sure why we need this yet
                    im = place_label_from_options(
                        im,
                        nsymbol,
                        # not sure if annotations are always directional - feels a safer default as its more intentional
                        directional_label=True,
                        **dt,
                        label_base="center",
                        show_metadata=kwargs.get("show_metadata"),
                    )

        if add_internal_lines and self._row.get("internal_lines"):
            res.utils.logger.debug(f"Adding internal lines")
            im = Image.fromarray(
                draw_lines(im, self._row.get("internal_lines"), dotted=True)
            )
            line_emp = self._row.get("res.internal_lines_emphasis")
            if line_emp:
                res.utils.logger.debug(f"Adding res emphasis to internal lines")
                im = Image.fromarray(
                    draw_lines(
                        im,
                        line_emp,
                        dotted=False,
                        color=(255, 0, 0, 255),
                    )
                )

        if add_sewlines and self._row.get("sew_lines"):
            res.utils.logger.debug(f"Adding sew lines")
            im = Image.fromarray(draw_lines(im, self._row.get("sew_lines")))

        if add_cutline:
            res.utils.logger.debug(f"Adding cutline")

            # plan should be on the notch model to have physical notch labels all\
            # controlled in one place - > that simplifies when we try to apply it

            # check if we have physical notches and if so go this route
            kp = self.match_physical_notches(["v_notch"])

            ol = None
            if len(kp):
                res.utils.logger.debug(
                    f"Adding cutline with considerations for physical notches"
                )
                ol = self.get_piece_image_outline()
                nol = self.get_notched_outline(g=ol, match_terms=["v_notch"])
                offset = None
                ol = nol
                # pad the image by the offset
                # this is needed because why?
                ol = swap_points(ol) if ol else None
                # offset swap ??
                assert "Multi" not in str(
                    ol.type
                ), "When adding a physical notch outline to the shape we produced an invalid outline; must be LineString not MultiLineString"

            im = Image.fromarray(draw_outline(np.asarray(im), outline_image_space=ol))

        self.add_one_code_hack(im, piece_instance=piece_instance)
        return im

    def add_one_code_hack(self, im, size="M", piece_instance=1):
        try:
            if self.piece_name._key == "CC-9007-V1-BDYONLBL-S":
                try:
                    sl = get_size_label(size)
                except:
                    pass
                qrcode = get_one_label_qr_code(
                    f"{self._parent.one_number}.{piece_instance}"
                )
                im.paste(sl, (225, 2340), mask=sl)
                im.paste(qrcode, (600, 2100), mask=qrcode)
                res.utils.logger.debug(
                    f"Added labels for piece instance {piece_instance}"
                )
        except Exception as ex:
            res.utils.logger.warn(f"Failed to add one label hack {ex}")
            return im
        return im

    def _get_center_notches(self):
        """
        using the bounds of the piece, we split it in x and y and add computed center notches to a dictionary
        """
        e = self._row["edges"]
        b = e.bounds
        hw = (b[2] - b[0]) / 2.0 + b[0]
        xintercept = LineString([[hw, b[1]], [hw, b[-1]]])
        hl = (b[3] - b[1]) / 2.0 + b[1]
        yintercept = LineString([[b[0], hl], [b[2], hl]])
        center_notches = e.intersection(unary_union([yintercept, xintercept]))
        df = pd.DataFrame(center_notches, columns=["geometry"])
        df["x"] = df["geometry"].map(lambda pt: pt.x)
        df["y"] = df["geometry"].map(lambda pt: pt.y)
        center_notches = {
            "top_center": df.sort_values("y").iloc[0]["geometry"],
            "bottom_center": df.sort_values("y").iloc[-1]["geometry"],
            "left_center": df.sort_values("x").iloc[0]["geometry"],
            "right_center": df.sort_values("x").iloc[-1]["geometry"],
        }
        return center_notches

    def get_center_pline(self, l, o, PLINE=SMALL_SEAM_SIZE, DISC=HALF_SMALL_SEAM_SIZE):
        """
        Determine the slice of the placement line near the notch
        """
        b = l.interpolate(PLINE).buffer(DISC)
        # I = o.intersection(b)
        # if not I.is_empty:
        #     pts = [I.interpolate(0), I.interpolate(I.length)]
        #     pl = LineString(pts)
        #     return pl.interpolate(0.5, normalized=True)
        # print(l, "near this notch there is no placement line")
        return b.centroid

    def determine_placement(self, r):
        """
        this is to determine placement from a description of the placement
        this is a compile step normally for symbols places at notches

        some things are not at notches and are compiled elsewhere today
        - seam placement - e.g. things that go anywhere on the viable surface of an edge
        - piece-level things are that are placed at logical locations that dont have notches e.g. "center top"

        the concepts do need be clearned up of course
        """
        try:

            def pd_get(r, x, default=None):
                val = r.get(x)
                if pd.notnull(val):
                    return val
                return default

            # resolve to a primitive value
            dx = pd_get(r, "dx")

            # TODO: we have to be careful with this - its flawed to just do parallel offset of the placement line
            # depending on the intern notch angle. we really want to translate placement line along the notch vector
            # @ implement "slide along (g) "
            dy = pd_get(r, "dy", 0)  # 0.125 * DPI
            if pd.isnull(dx):
                dx = 0

            p = r.get("placement_position")
            if pd.notnull(p):
                # if we happen to know it, great
                return p

            # test handle simple notches in more direct way
            # the offsets need to be made more stand
            ln = r["notch_line"]
            if dy < 100:
                # this is a magic switch for really extreme offsets (hack)
                if (
                    not r.get("position")
                    and "_notch" in r["operation"]
                    or r["operation"] == "tunnel_top_stitch_seam"
                ):
                    pts = list(ln.coords)
                    if r["operation"] in BASIC_NOTCHES + [
                        "invisible_stitch_notch",
                        "tunnel_top_stitch_seam",
                    ]:
                        return ln.interpolate(ln.length / 2.0)
                    # we can ensure its the one at the end near the center of mass too
                    return Point(pts[-1])

            func = self._relative_location.get(r.get("position"))

            # we may take an inside offset if there is a dy

            # if there is a dx what we do is interpolrate in the direction from the mid point to the target point up to the dx value e.g. a quarter inch in the direction of the target point along the placement line
            pline = r["placement_line"]

            if pd.isnull(pline):
                res.utils.logger.debug(f"No pline in {pline}")
                raise ValueError(
                    "A placement line was expected for the positioning but is missing"
                )
            if pd.notnull(dy):
                # @we can go in or out from the pline - we dont check boundaries
                # TODO we could be an explicit safety here
                # be careful with very large negative offsets as we can get some weird errors
                pline = (
                    inside_offset(pline, self._row["geometry"], dy)
                    if dy > 0
                    else outside_offset(pline, self._row["geometry"], abs(dy))
                )

            # Contract - the placement line MUST cross the notch line or be close to it

            # project the notch line onto the pline to find the "center" for offsets
            # ct = self.get_center_pline(ln, pline)

            # <--- should really abstract his logic into the notch nid
            # going to move this to notch model processing -> remove from here next week
            if pd.isnull(pline) or pline.is_empty:
                ct = ln.interpolate(0.5, normalized=True)
            else:
                # clip the pline
                ct = ln.interpolate(0.5, normalized=True)
                # trying to bound the pline - main thing is not to cross the center line for SA plines
                # we also need to keep this relatively centered near the notch
                pline = ct.buffer(1.1 * 300).intersection(pline)
                if ln.intersects(pline):
                    ct = ln.intersection(pline).centroid
                else:
                    ct = ln.interpolate(0.5, normalized=True)
                    ct = pline.interpolate(pline.project(ct))

            if pd.notnull(func) and pd.notnull(pline) and dx:
                pt = func(pline)
                # if there is a dx we interpolate in the direction of the point from center rather than use the point itself
                if dx:
                    l = LineString([ct, pt])

                    if dx > l.length:
                        l = self.get_edge(r["edge_id"])
                        res.utils.logger.debug(
                            f"Long offset {dx} - will interpolate along the surface of edge {r['edge_id']} and then offset"
                        )

                        p1 = l.interpolate(dx - 10)
                        p2 = l.interpolate(dx)
                        pt = inside_offset(
                            LineString([p1, p2]), self._row["geometry"]
                        ).interpolate(0)

                    else:
                        pt = l.interpolate(dx)
                    return pt
            # by default use the center of the pline

            return ct
        except Exception as ex:
            res.utils.logger.warn(
                f"error making placement for keypoint annotation {dict(r)}: {res.utils.ex_repr(ex)}"
            )
            # raise ex

    def _extract_annotations_from_edge_templates(self):
        """
        create 'edge_annotations' from the edge templates that contain both edge and keypoint info

        """

        self._row["edge_templates"] = self._piece_model.update_all_templates(
            self["edge_templates"]
        )

        def enumerate_seam_annotations(l):
            if not_null_or_enumerable(l):
                for d in l:
                    try:
                        for e in d.get("edge_annotations", []):
                            if not pd.isnull(e):
                                yield e
                    except Exception as ex:
                        res.utils.logger.warn(
                            f"Error loading an edge annotation for a pieces yield an empty set {repr(ex)}"
                        )
                        yield {}

        def enumerate_keypoint_annotations(l):
            """
            example annotation
            {'edge_id': 2.0, 'notch_id': 2.0, 'operation': 'in_seam_slit_without_cut', 'is_physical': False, 'position': 'near_edge_center', 'dx': 150.0, 'dy': None, 'E': 2.0, 'hash': 'res0770496340'}
            """
            if not_null_or_enumerable(l):
                for d in l:
                    try:
                        for e in d.get("keypoint_annotations", []):
                            if not pd.isnull(e):
                                yield e
                    except Exception as ex:
                        res.utils.logger.warn(
                            f"Error loading an keypoint annotation for a pieces yield an empty set {repr(ex)}"
                        )
                        yield {}

        def edge_of(E):
            if pd.notnull(E):
                return int(E.split("-")[-1])

        ##get all teh regular ones and add special ones
        ea = list(enumerate_seam_annotations(self["edge_templates"]))

        # if one_number is not None:
        if self._parent.one_number:
            ea.append(
                {
                    "body_construction_order": None,
                    "edge_id": -1,
                    "is_last": False,
                    "operation": self._parent.one_number,
                    "type": "add_one_label",
                }
            )

        ea.append(
            {
                "body_construction_order": None,
                "edge_id": self["sew_last_operated_edge"],
                "is_last": True,
                "operation": f"identity-{self.piece_name.identity}",
                "type": "identity",
                # the sew orientation edge is taken from the sew table and suffixed accordingly
                "orientation_edge": edge_of(self._row.get("sew_orientation_edge")),
            }
        )

        ea.append(
            {
                "body_construction_order": None,
                "edge_id": -1,
                "is_last": False,
                "operation": str(self.piece_name),
                "type": "text",
            }
        )

        self._row["edge_annotations"] = ea
        self._row["annotations"] = list(
            enumerate_keypoint_annotations(self["edge_templates"])
        )

    def _add_annotations(self):
        """ """

        notches = self._keypoints

        self._piece_model = PieceSymbolsModel(self._row, self._keypoints)

        def default_placement(row):
            l = row["notch_line"]
            pl = row["placement_line"]
            if pd.isnull(pl):
                pl = affinity.rotate(l, 90)
            return pl

        def make_placement_ref(row):
            try:
                pline = row["placement_line"]
                ln = row["notch_line"]
                if ln is None:
                    return ln

                if pd.isnull(pline) or pline.is_empty:
                    ct = ln.interpolate(0.5, normalized=True)
                else:
                    # clip the pline
                    ct = ln.interpolate(0.5, normalized=True)
                    # trying to bound the pline - main thing is not to cross the center line for SA plines
                    # we also need to keep this relatively centered near the notch
                    pline = ct.buffer(1.1 * 300).intersection(pline)
                    if ln.intersects(pline):
                        ct = ln.intersection(pline).centroid
                    else:
                        ct = ln.interpolate(0.5, normalized=True)
                        ct = pline.interpolate(pline.project(ct))

                return ct
            except Exception as ex:
                res.utils.logger.warn(
                    f"Failed placement ref for row {dict(row)} - you should check the notch frame on the DXF {self._parent._meta['metadata']['meta_path']}: {res.utils.ex_repr(ex)}"
                )
                return None
                # raise ex

        def is_placed_after_notch(row):
            """
            If there is some offset of placement of a symbol, we can use that to see if icons should be flipped
            For example if we place something towards the line center of the notch, this could be left-of (after) or right-of (before)

            test with directional symbols
            - shirring
            - sa_directional
            - fold directional

            #test that the compiled dt actually calcs this properly
            #CONTRACT
            """

            # the notch is the center of the placement line
            pline = row.get("placement_line")
            pp = row["placement_position"]
            if pd.isnull(pline) or pline.is_empty:
                return False
            if pd.isnull(pp) or pp.is_empty:
                return False
            func = self._relative_location["edge_order"]

            mp = row.get("placement_ref")
            prj = pline.project(pp)
            p2 = pline.interpolate(prj)
            # mp = pline.interpolate(0.5, normalized=True)
            if pd.notnull(mp) and pd.notnull(p2):
                return func(p2) > func(mp)
            return False

        self._extract_annotations_from_edge_templates()

        symbols = self._row.get("annotations", [])

        if symbols is not None and len(self._keypoints) > 0:
            # this is an expansion/projection on all symbols at the notch
            symbol_df = pd.DataFrame(list(symbols))
            # errors in sew symbols
            if len(symbol_df):
                symbol_df = (
                    symbol_df[symbol_df["edge_id"].notnull()]
                    .reset_index()
                    .drop("index", 1)
                )
            for c in ["edge_id", "notch_id"]:
                if c not in symbol_df.columns:
                    symbol_df[c] = -1

            if len(symbol_df):
                symbol_df = (
                    symbol_df[symbol_df["edge_id"].notnull()]
                    .reset_index()
                    .drop("index", 1)
                )

            df = pd.merge(
                # surface edges only
                notches[notches["distance_to_edge"] == 0],
                symbol_df,
                # if we left join we can add default annotations ot notches when no symbols
                how="left",
                left_on=["nearest_edge_id", "edge_order"],
                right_on=["edge_id", "notch_id"],
                # right join  allows symbols off of notches
            )
            if len(df):
                df["placement_angle"] = df["notch_line"].map(
                    lambda l: line_angle(l) if pd.notnull(l) else None
                )

                if "operation" not in df.columns:
                    # when we have no sew instructions expect void ops on notch models -> could have other cols here too
                    df["operation"] = None

                # for testing useful to show this because the placement is a function of dx,dy,theta etc. and can be hard to troubleshoot
                # df["placement_angle"] = 0

                # TODO: a just in case patch so we dont blow up but this may not be accurate
                df["placement_line"] = df.apply(default_placement, axis=1)
                df["operation"] = df["operation"].fillna("void")
                df["placement_position"] = df.apply(self.determine_placement, axis=1)
                df["placement_ref"] = df.apply(make_placement_ref, axis=1)
                df["flip"] = df.apply(is_placed_after_notch, axis=1)
                # default -> void or  draw a line if/when we trust positioning
                # already drawing the notch line itself for all non blacklisted notches

            return pd.concat([df, self._determine_piece_level_annotations()])
        return pd.DataFrame()

    def _determine_piece_level_annotations(self):
        """
        add them to annotations

        we use the geometry tags to say if we want them and then we calc the intersection of a fold line
        the fold notches are aligned with this line coming off the outline parts that intersect them
        """

        piece_level_annotations = []

        def make_position(pos, label="fold_notch", dy=None):
            """
            can refactor this later - this is a "semi-compiled" annotation that we can add for custom notches
            """
            # this is a special notch line, the angles are actually given by the position N,S,E,W
            # l = dxf_notches.notch_line(self["center_notches"][pos], self._row["edges"])

            # this is super messy - will clean up - think we need to create notches on the axes
            if pos == "top_center":
                l = LineString(
                    [
                        self["center_notches"][pos],
                        self["center_notches"]["bottom_center"],
                    ]
                )
            elif pos == "bottom_center":
                l = LineString(
                    [
                        self["center_notches"][pos],
                        self["center_notches"]["top_center"],
                    ]
                )
            elif pos == "left_center":
                l = LineString(
                    [
                        self["center_notches"][pos],
                        self["center_notches"]["right_center"],
                    ]
                )
            elif pos == "right_center":
                l = LineString(
                    [
                        self["center_notches"][pos],
                        self["center_notches"]["left_center"],
                    ]
                )

            l = LineString(
                [self["center_notches"][pos], l.interpolate(HALF_SMALL_SEAM_SIZE)]
            )

            dy = l.length if pd.isnull(dy) else dy
            return {
                "position": pos,
                "operation": label,
                "notch_line": l,
                "dy": dy,
                "placement_position": l.interpolate(dy),
            }

        geoms = self._row.get("sew_geometry_attributes", [])
        for g in _try_as_list(geoms):
            # track a line of interest
            if (
                g.lstrip().rstrip().replace(" ", "").lower()
                == "Center Fold  Y-axis".replace(" ", "").lower()
            ):
                piece_level_annotations.append(make_position("top_center"))
                piece_level_annotations.append(make_position("bottom_center"))
            elif (
                g.lstrip().rstrip().replace(" ", "").lower()
                == "Center Fold  X-axis".replace(" ", "").lower()
            ):
                piece_level_annotations.append(make_position("left_center"))
                piece_level_annotations.append(make_position("right_center"))
            else:
                continue

        symbols = self._row.get("annotations", [])
        center_notches = list(self["center_notches"].keys())
        for a in symbols:
            if pd.notnull(a.get("notch_id")) or pd.isnull(a.get("position")):
                continue
            if a["position"] in center_notches:
                res.utils.logger.debug(
                    f"Adding a piece level symmetry position for operation {a.get('operation')}"
                )
                a = make_position(a["position"], a.get("operation"), dy=a.get("dy"))

                piece_level_annotations.append(a)

        df = pd.DataFrame(piece_level_annotations)
        if len(df):
            df["placement_angle"] = df["notch_line"].map(
                lambda l: line_angle(l) if pd.notnull(l) else None
            )
            df["placement_position"] = df.apply(self.determine_placement, axis=1)

        return df

    def _match_from_list(self, obj, match):
        if obj is not None and isinstance(list(obj), list):
            for p in obj:
                if match in p:
                    return p

    def seam_selector(self, eid=0):
        def get_id(e):
            return e.interpolate(e.length / 2).z

        return unary_union([e for e in list(self._edges) if get_id(e) == eid])

    def callout_v_notches(self, g, threshold=300, simplification=1):
        """
        this callout works by comparing the outline and its corner that we have to the one we get in the image
        these v-notches appear as sharp protrusions that do not appear as corners in our data
        Some "v notches merged into an extended plateaux but the corners still protrude
        The distance threshold is to to determine how far away we need to be - v notches normally away from the corners of edges but we can calibrate this
        """
        claimed = pd.DataFrame(
            magic_critical_point_filter(g.simplify(simplification)),
            columns=["geometry"],
        )

        known_corners = self._corners
        claimed["dist"] = claimed["geometry"].map(lambda g: g.distance(known_corners))
        failed = claimed[claimed["dist"] > threshold]
        g = unary_union(failed["geometry"])
        if g.type == "Point":
            return MultiPoint([g])
        return g

    def find_notches_in_piece_image(self):
        """
        get the image outline and then find the notches in it by algorithm
        this can be used to compare or sub the notches from the astm
        for example for size grading or if we cannot find an astm that we like, we can absorb the notches from the image in a fuzz way
        """
        return self.find_notches_in_image_outline(self.get_piece_image_outline())

    def find_notches_in_image_outline(self, g):
        """
        see comments in find_notches_in_image
        here we get different types of visual notches from the image data
        these are configured in gerber experts in legacy
        in future we want all notches in meta data
        """
        # guess these vals for now - lookup gerber and other notch values
        VSHIFT = -0.188
        CSHIFT = 0.157
        vs = self.callout_v_notches(g)
        ts = detect_outline_castle_notches(g)

        # this adds a z component in notch langauge
        vs = zshift_geom(vs, VSHIFT)
        ts = zshift_geom(vs, CSHIFT)

        return unary_union([vs, ts])

    def _get_split_edges(self):
        """
        edges are a single geometry but we can iterate over the parts
        there can be many parts to one edge depending on polygon construction
        """
        corners = MultiPoint(magic_critical_point_filter(self._row["geometry"]))
        return self._row["geometry"] - corners.buffer(5)

    def get_edge_by_index(self, eid):
        """
        the edges contain a z component with an id
        they are usually one line but can be split if they cross the 0 angle in the polygon and that is why we union
        Need to be careful with intent for follow up geometry operations but its hard to "restitch" the line but we could
        """
        return unary_union(
            [g for g in self._get_split_edges() if eid == int(list(g.coords)[0][-1])]
        )

    def edge_center(self, eid):
        e = self.get_edge_by_index(eid)
        l = e.length / 2.0
        return e.interpolate(l)

    def _set_relative_offset_ops(self):
        """
        The marker has geometric intel that we build up here
        """
        edge_order_fn = dxf_notches.projection_offset_op(self._row)

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
            # useful just to sort anything
            "edge_order": edge_order_fn,
            # pose dependent constructs -> center pieces are assumed right
            "inside": relative_inside(self._row["geometry"], pose=self.piece_name.pose),
            "outside": relative_outside(
                self._row["geometry"], pose=self.piece_name.pose
            ),
            "before": circuit_relative_before,
            "after": circuit_relative_after,
            "near_edge_corner": circuit_relative_from_corner_min,
            "near_edge_center": circuit_relative_from_corner_max,
        }

    # TODO - validation e.g. piece names or icons
    def analyse(self, **kwargs):
        """
        perform a shape and nest analysis on the piece
        """

    ######
    ####    some debugging utils to look at placement plans etc.
    ######
    def show_annotation_plan(self, as_html=True):
        def get_label_icon(row):
            flip = row.get("flip", False)
            pos = row.get("position")
            name = row.get("expansion")
            try:
                return get_svg(f"sew-{name}")
            except Exception as ex:
                try:
                    return notch_symbol(name, position=pos, flip=flip)
                except Exception as ex:
                    pass

        from IPython.display import HTML
        from res.utils.dataframes import image_formatter

        # TODO: we should access templates directly if they are moving to a database
        # but we could load them as a function from redis
        tkeys = get_template_keys()

        def template_ops(name):
            t = get_templates().get(name)
            if not t:
                return []
            return [o["operation"] for o in t["edge_annotations"]] + [
                o["operation"] for o in t["keypoint_annotations"]
            ]

        data = self.sew_df
        if len(data) == 0:
            return data

        data["has_template"] = data["name"].map(lambda x: x in tkeys)
        data["expansion"] = data["name"].map(lambda x: [x])
        data.loc[data["has_template"], "expansion"] = data["name"].map(template_ops)

        data = data.explode("expansion")
        data["icon"] = data.apply(get_label_icon, axis=1)

        cols = [
            "body_code",
            "body_construction_order",
            "category",
            "edge_id",
            "notch1",
            "notch2",
            "name",
            "expansion",
            "icon",
            "operation",
        ]

        data = data[cols]
        return (
            data
            if not as_html
            else HTML(data.to_html(formatters={"icon": image_formatter}, escape=False))
        )

    def show_notches(self, surface_notches_only=True):
        from res.media.images.providers.dxf import dxf_notches

        data = dxf_notches.get_notch_frame(
            self._row,
            edge_seam_allowances=self._seam_allowances,
            plot=True,
            known_corners=self._corners,
        )
        if len(data) == 0:
            res.utils.logger.warn("No notches to show")
            return None
        df = data[data["distance_to_edge"] == 0] if surface_notches_only else data
        if len(df) > 0:
            return df.sort_values(["nearest_edge_id", "edge_order"])
        return df

    def show_seam_allowances(self, buffer=20):
        df = pd.DataFrame(self._edges, columns=["geometry"])
        df["z"] = df["geometry"].map(lambda g: g.interpolate(0).z)
        sa = self._seam_allowances
        df["seam_allowance"] = df["z"].map(lambda z: sa[z])
        df["geometry"] = df.apply(
            lambda row: row["geometry"].buffer(row["seam_allowance"] * buffer), axis=1
        )
        from geopandas import GeoDataFrame
        from matplotlib import pyplot as plt

        df = GeoDataFrame(df)
        df.plot(figsize=(10, 10))
        plt.gca().invert_yaxis()
        return df

    def show_notch_plan(self, edge, notch):
        from geopandas import GeoDataFrame
        from res.media.images.providers.dxf import dxf_notches
        from matplotlib import pyplot as plt

        d = dict(
            self._keypoints.query(
                f"nearest_edge_id=={edge} & edge_order=={notch} & distance_to_edge==0 "
            ).iloc[0]
        )
        e = self["edges"]
        d["notch_line"] = dxf_notches.notch_line(d["geometry"], e)
        d["pline"] = dxf_notches.placement_line(d["geometry"], e)

        gdata = GeoDataFrame(
            [e.intersection(d["geometry"].buffer(400))]
            + [d["geometry"].buffer(10)]
            + [d["notch_line"].buffer(5)]
            + [d["pline"].buffer(5)],
            columns=["geometry"],
        )
        gdata.plot(figsize=(10, 10))
        plt.gca().invert_yaxis()

        return d


def as_list(o, upper_case=True):
    if not isinstance(o, list):
        return [o]
    return [item.lower() if not upper_case else item.upper() for item in o]


class MetaMarker:
    """
    Marker is a collection of pieces (see Pieces object)
    This object reads metadata from S3 about pieces, and sub-piece keypoints and lines. There is also high level meta header

    For sew-one-ready there should be edges, keypoints and the body level detail such as construction seq in meta.json
    We can usuall add smarter seams without a notch model - the notch model understands much more about notches for precision placement
    """

    def __init__(self, path, one_number=None, **options):
        s3 = res.connectors.load("s3")
        self._home_dir = path
        meta_path = f"{path.rstrip('/')}/meta.json"
        self._meta = s3.read(meta_path) if s3.exists(meta_path) else {}
        self._one_number = one_number
        self._validation_details = {}
        # sales flow can configre the meta one
        self._sales_channel = options.get("sales_channel")
        self._order_type = options.get("order_type", "")

        self._data = read_meta_marker(path)

        try:
            self._data = (
                self._data[self._data["key"].map(lambda x: PieceName(x).is_printable)]
                .reset_index()
                .drop("index", 1)
            )
        except:
            pass

        # material aliases
        materials = options.get("material_codes", options.get("material_code"))
        if materials:
            materials = as_list(materials)
            res.utils.logger.debug(f"filtering by materials {materials}")
            idx = self._data["material_code"].isin(materials)
            self._data = self._data[idx].reset_index().drop("index", 1)
        piece_types = options.get("piece_types", options.get("piece_type"))
        if piece_types:
            idx = self._data["type"].isin(as_list(piece_types, upper_case=False))
            self._data = self._data[idx].reset_index().drop("index", 1)
        # not sure what to use as they piece key yet
        if options.get("piece_codes"):
            idx = self._data["code"].isin(as_list(options.get("piece_codes")))
            self._data = self._data[idx].reset_index().drop("index", 1)

        # this is a hash generated from these values
        self._piece_slice_hash_suffix = None
        key_options = {
            k: v
            for k, v in options.items()
            if k in WHITELIST_ASSET_PAYLOAD_DISCRIMINATOR
        }
        if len(key_options):
            self._piece_slice_hash_suffix = res.utils.hash_dict_short(key_options)
            res.utils.logger.debug(
                f"Using the options {key_options} to update the marker key - added hash {self._piece_slice_hash_suffix} as suffux"
            )

        res.utils.logger.info("Loading dxf and piece info")
        self._piece_info = self.dxf._piece_info

    def as_meta_one(self):
        from res.flows.meta.ONE.meta_one import MetaOne

        sku = f"{self.sku} {self.size}"
        return MetaOne(sku)

    @property
    def key(self):
        """
        The meta key generated for the style and the one number makes a marker unique for the metaone
          but we also add a hash of the slicer of pieces
        """
        key = f"{self._meta.get('key')}-{self.one_number}"
        if self._piece_slice_hash_suffix:
            key = f"{key}-{self._piece_slice_hash_suffix}"
        return key

    @property
    def validation_details(self):
        return self._validation_details

    @property
    def invalidated_date(self):
        return self._meta.get("invalidated_date", None)

    @property
    def is_valid(self):
        invalidated_date = self._meta.get("invalidated_date")
        if invalidated_date:
            last_modified = self._meta.get("last_modified")
            return invalidated_date >= last_modified
        return True

    @property
    def size(self):
        return self._data.iloc[0]["size"]

    @property
    def accounting_size(self):
        return "TODO"

    @property
    def style_code(self):
        return self._meta.get("metadata", {}).get("res_key")

    @property
    def sku(self):
        s = self.style_code
        if s:
            s = s.replace("-", "")
            s = f"{s[:2]}-{s[2:]}"
            # hack to fix an inconsitency upstream
            if " SELF" in s and "SELF--" not in s:
                s = f"{s}--"
            return s

    @property
    def piece_keys(self):
        return list(self._data["key"])

    def show_dxf(self):
        """
        fetch and show the geometry in the dxf
        """
        from res.media.images.providers.dxf import dxf_plotting

        s3 = res.connectors.load("s3")
        dxf = s3.read(self._meta["metadata"]["meta_path"])
        dxf_plotting.plot_labeled_edges(dxf)
        return dxf

    def write_as_pdf_to_s3(
        self,
        h,
        s3_target_root="s3://res-data-platform/samples/",
        make_signed_url=True,
    ):
        import tempfile
        from weasyprint import HTML as WHTML

        s3 = res.connectors.load("s3")

        s3_target = f"{s3_target_root}{res.utils.res_hash()}.pdf"

        W = WHTML(string=h)

        with tempfile.NamedTemporaryFile(suffix=".pdf", prefix="f", mode="wb") as f:
            W.write_pdf(f.name)
            # f.write("test")

            res.utils.logger.debug(
                f"Writing the html converted to pdf to s3 {s3_target}"
            )

            s3.upload(f.name, s3_target)

            if make_signed_url:
                s3_target = s3.generate_presigned_url(s3_target)

        W = None

        return s3_target

    def get_factory_order_document(self, factory_exit_date, as_pdf_url=False, **kwargs):
        """

        provide a date and then generate the HTML for the meta-one
        if the order number is added in the meta-one, we can show that too

        if we ask for a pdf signed url, then we will instead generate the pdf and provide a signed url that will timeout
        this can be used to create an airtable upload of an attachment
        """
        html = get_order_html(self, exit_factory_date=factory_exit_date)

        if as_pdf_url:
            return self.write_as_pdf_to_s3(html)

        return html

    @staticmethod
    def get_res_artwork_file(fid, as_pil=False, scale_down=None, min_width=None):
        """
        related to meta ones we can have art work files and they are referenced in multiple systems
        here we can cache/resolve the image
        """
        from PIL import Image

        box = res.connectors.load("box")
        s3 = res.connectors.load("s3")
        mongo = res.connectors.load("mongo")
        db = mongo["resmagic"]

        target = f"s3://res-data-platform/artwork_files_cache/{fid}.png"
        if not s3.exists(target):
            bid = pd.DataFrame(db.get_collection("artworkFiles").find({"_id": fid}))[
                "legacyAttributes"
            ].iloc[0]

            bid = bid.get("Box File ID")
            if bid:
                box.copy_to_s3(bid, target)
            else:
                raise ArgumentError(f"The bx file id does not exist in {dict(bid)}")

        if not as_pil:
            return s3.generate_presigned_url(target)

        im = Image.fromarray(s3.read(target))

        if scale_down:
            if min_width:
                if im.size[1] < min_width:
                    return im
                if (im.size[1] // scale_down) < min_width:
                    # find a better scaling
                    scale_down = int(im.size[1] / min_width)
            im.thumbnail((im.size[0] // scale_down, im.size[1] // scale_down))

        return im

    @staticmethod
    def get_style_image_table(artwork_file_id, style_thumbnail_url):
        """
        provide an airtable style record which should have cover image and artwork
        it is better to store these when creating the meta one so we cna just resolve
        we need a signed url so we should generate that on request due to 30 minute timeout

         style_url = style_record["style_cover_image"]["thumbnails"]["large"]["url"]
            artwork_url = MetaMarker.get_res_artwork_file(
                style_record["artwork_file_id"]
            )

        """
        if artwork_file_id == "":
            artwork_file_id = None

        artwork_url = ""
        try:
            artwork_url = (
                MetaMarker.get_res_artwork_file(artwork_file_id)
                if artwork_file_id is not None
                else ""
            )
        except Exception as ex:
            res.utils.logger.warn(
                f"Failing to resolve artwork for ir {artwork_file_id} in get_style_image_table"
            )
        im_table = f"""
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

        return im_table

    def validate(self, validate_color=True, **kwargs):
        validation_errors = []
        validate_dxf = kwargs.get("validate_dxf", True)

        s3 = res.connectors.load("s3")
        dxf = self._try_get_my_dxf()
        metadata = self._meta or {}
        cut_status = metadata.get("cut_status", False)

        if dxf and validate_dxf:
            vals = dxf.validate()
            if vals:
                validation_errors += vals

                # capture specific pieces with validation errors
                # capture details as self._validation_details.update(dxf.validation_details)
                self.validation_details.update(dxf.validation_details)

            body_lower = dxf.code.lower().replace("-", "_")
            bversion = dxf.body_version.lower()
            # vs_ext = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}/{bversion}/extracted/scan.json"
            # res.utils.logger.info(f"Checking vs export {vs_ext} for sample size only")
            # if not s3.exists(vs_ext):
            #     validation_errors.append("VS_SCANFILE_MISSING")
            # else:
            #     try:
            #         load_vs_export(vs_ext, size=dxf.sample_size)
            #     except Exception as ex:
            #         validation_errors.append("VS_SCANFILE_BAD")

        elif validate_dxf:
            res.utils.logger.info(
                f"The DXF file cannot be linked for the meta marker. check path"
            )
            validation_errors.append("CANNOT_FIND_DXF")

        try:
            if validate_color:
                res.utils.logger.info("\n")
                res.utils.logger.info(
                    f"Checking color files exist - this is also a first check on the piece constructor"
                )
                # check all the files exist
                missing_files = 0
                missing_file_names = []
                for p in self:
                    if not p._row.get("filename"):
                        continue
                    # extra check to remove false positives despite any upstream mis classification
                    if not PieceName(p["key"]).is_printable:
                        continue

                    if not s3.exists(p["filename"]):
                        res.utils.logger.warn(
                            f"WARNING: The filename {p['filename']} does not exist for marker"
                        )
                        missing_files += 1
                        missing_file_names.append(p["key"])
                if missing_files == 0:
                    res.utils.logger.info(f"****WE HAVE ALL OUR COLOR FILES****")
                if missing_files:
                    validation_errors.append("MISSING_COLOR_FILES")
                    # capture details as self._validation_details:> MISSING_COLOR_FILES: keys
                    self._validation_details["MISSING_COLOR_FILES"] = missing_file_names

        except Exception as ex:
            res.utils.logger.warn(
                f"Failure to load a piece during validation {res.utils.ex_repr(ex)}"
            )

        # do other gates
        res.utils.logger.info("\n")
        res.utils.logger.info(f"Checking other contracts if configured...")

        if validation_errors:
            return validation_errors

        return []

    def _try_get_my_dxf(self):
        """
        a dxf file is used to create a meta marker
        the metadata object should record this so we can fetch it
        """
        path = self._meta.get("metadata", {}).get("meta_path")
        if path:
            return DxfFile(path)

    staticmethod

    def meta_one_for_simple_color(
        dxf, cache_dir, size=None, color="white", recreate_if_cached=False, **kwargs
    ):
        """
        uses the meta data file to generate a meta-ONE with simple colors
        the cache directory is used to store the pieces e.g. locally or on S3

        this is a useful function for testing DXF files directly be generating color files
        we could support the trick of treating the color as an image tile if its not a strong and generate a tiling
        """
        from res.media.images.providers.dxf import DxfFile

        mod = (
            list(res.connectors.load("s3").ls_info(dxf))[0]["last_modified"]
            if "s3://" in dxf
            else None
        )
        dxf = DxfFile(dxf)
        size = size or dxf.sample_size
        # this is a meta marker conv = consider

        dxf.save_piece_samples(cache_dir, size=size, color=color, meta_updated_at=mod)
        # interesting to reflect on a more flexible meta marker interface that does need some of these things
        return dxf.export_meta_marker(
            sizes={size: size},
            color_code="WHITE",
            material_props={},
            piece_material_map={"default": "CTW70"},
            # the pieces live where we cached them
            pieces_path_suffix=cache_dir,
        )

    @staticmethod
    def place_directional_grid(row, tile, add_notches=False, buffer_offset=None):
        """
        Using a row by contract e.g. what we have in marker files we can place color
        """
        g = row["geometry"]
        logger.debug(f"tiling with tile of size {tile.shape}")
        s = int(tile.shape[0])
        st = row["stripe"]
        pl = row["plaid"]
        # TODO assertions
        return place_directional_color_in_outline(
            g, st, pl, tile, add_notches=add_notches, buffer_offset=buffer_offset
        )

    @classmethod
    def from_prep_piece_payload(self, asset):
        """ """

        uri = asset.get("uri", "")
        if "s3://meta-one-assets-prod/styles/meta-one" in uri:
            return MetaMarker(uri, one_number=asset.get("one_number"))

        return None

    @classmethod
    def from_restNestv1_asset(self, asset):
        """
        This is to allow nesting to select specific pieces to nest
        Example:
        asset = {
            "key": "1000001",
            "value": 's3://meta-one-assets-prod/styles/meta-one/tk_6077/v7/artnkd/3zzmd',
            'piece_types': ['self', 'block_fuse'],
            'material_codes' : ['CTW70'],
            #'piece_codes' : ['DRSFTRT', 'DRSBKYKEUN']
        }
        """
        path = asset["value"]

        # add this for the meta marker constructor
        asset["one_number"] = asset.get("key")

        return MetaMarker(path=path, **asset)

    def analyse(self, plot=False, aggregate_materials=False, **kwargs):
        """
        perform a shape and nest analysis on the marker
        """

        def _aggregate_children(df):
            return df

        pdata = pd.concat([p.analyse(**kwargs) for p in self])
        pdata = _aggregate_children()

        # perform set nests and get nested length for astm using material groups
        # same for nesting and buffered_geometry

        return pdata

    def nest_pieces(self):
        from res.learn.optimization.nest import nest_dataframe

        return nest_dataframe(self._data, plot=True)

    def __getitem__(self, key):
        return _MetaMarkerPiece(self, key)

    @property
    def sew_construction_sequence(self):
        if self._meta is not None:
            # TODO: returning a particular sequence for test
            return self._meta.get(
                "body_construction_sequence",
                ["bodice", "sleeve", "bodice", "bodice", "collar", "bodice"],
            )

    @property
    def one_number(self):
        return self._one_number

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

    def get_named_notched_outlines(self, compensate=True):
        """
        There are some values we want to return with known material properties
        here each pieces knows its material and the marker object knows the material properties
        so we can optional compensate the shapes for the physical context
        """
        props = self._meta["material_props"]

        for piece in self:
            g = piece.notched_outline
            material_code = piece["material_code"]
            dy = props[material_code]["compensation_length"]
            dx = props[material_code]["compensation_width"]

            # tield the compensated peieces
            yield piece["key"], affinity.scale(
                g, xfact=dx, yfact=dy
            ) if compensate else g

    def get_notched_outlines(self, compensate=True):
        """
        There are some values we want to return with known material properties
        here each pieces knows its material and the marker object knows the material properties
        so we can optional compensate the shapes for the physical context
        """
        for _, data in self.get_named_notched_outlines(compesate=compensate):
            yield data

    @property
    def notched_outlines(self):
        """
        The notched outlines are the ones that are raw from the astm
        """
        for piece in self:
            yield piece.notched_outline

    @property
    def piece_outlines(self):
        for piece in self:
            yield piece.piece_outline

    @property
    def named_piece_outlines(self):
        for piece in self:
            yield piece["key"], piece.piece_outline

    @property
    def notched_piece_image_outlines(self):
        """
        we take the outline from the image rather than the astm and then add notches
        """
        for piece in self:
            yield piece.notched_piece_image_outline

    @property
    def named_notched_piece_image_outlines(self):
        """
        we take the outline from the image rather than the astm and then add notches
        """
        for piece in self:
            yield piece["key"], piece.notched_piece_image_outline

    @property
    def named_notched_piece_raw_outlines(self):
        """
        we take the outline from the image rather than the astm and then add notches
        """
        for piece in self:
            yield piece["key"], piece.notched_piece_outline

    @property
    def body(self):
        return (self._home_dir.split("/")[6]).replace("_", "-").upper()

    @property
    def version(self):
        return self._home_dir.split("/")[7].upper().split("_")[0]

    @property
    def body_version(self):
        return self.version.split("_")[0].lower()

    @property
    def color(self):
        return self._home_dir.split("/")[8].upper()

    @property
    def size(self):
        return self._home_dir.split("/")[9].upper()

    @property
    def dxf(self):
        return DxfFile(self._meta["metadata"]["meta_path"])

    def build(self, force_upsert_style_header=False):
        """
        helper to build meta one in database from the marker data.
        this should be saved when we create the meta one in the first place as a bridge but we can re-check here
        """
        from res.flows.dxa.styles import helpers

        s = dict(helpers.get_styles(skus=self.sku).iloc[0])
        res.utils.logger.info(s)
        name = s["style_name"]
        created_at = s["created_at"]

        return helpers._save_meta_one_as_style(
            name,
            self._home_dir,
            created_at=created_at,
            force_upsert_style_header=force_upsert_style_header,
        )

    def get_piece_name_mapping(self):
        """
        part of the migration plan is to create pieces in generic format as saved to a database
        we could resolve the artwork but dont do that here for now since we always have the palced color file instead
        """
        d = {}
        df = self._data
        for pk in df.to_dict("records"):
            d[pk["piece_name"]] = {
                "color_code": pk["color_code"],
                "material_code": pk["material_code"],
                #'size_code' = self.size
                "base_image_uri": pk["filename"],
                "artwork_uri": None,
                "type": pk["type"],
            }
        return d

    def _sync_for_sew_body(self, **kwargs):
        return MetaMarker.sync_sew_for_body(self._home_dir, self.body, **kwargs)

    def get_assets(self):
        """
        WIP
            load all assets that we can assocaite with a style/body for a meta one
            do some checks that things exist and dates are after the body etc.
        """

        s3 = res.connectors.load("s3")
        sew_path = (
            f"s3://meta-one-assets-prod/bodies/sew/{self.body.lower().replace('-','_')}"
        )
        cut_path = f"s3://meta-one-assets-prod/bodies/cut/{self.body.lower().replace('-','_')}/{self.body_version}/{self.size.lower()}"
        body_files = f"s3://meta-one-assets-prod/bodies/3d_body_files/{self.body.lower().replace('-','_')}/{self.body_version}/extracted"

        def info_with_tag(path, tag, match=None):
            for info in s3.ls_info(path):
                info["tag"] = tag
                info["filename"] = info["path"].split("/")[-1]
                skip = False
                if match:
                    skip = True
                    for m in match:
                        if m in info["path"]:
                            skip = False
                            break
                if not skip:
                    yield info

        piece_images = "/".join(self._data.iloc[0]["filename"].split("/")[:-1])

        meta = pd.DataFrame(info_with_tag(self._home_dir, "meta"))
        body = pd.DataFrame(
            info_with_tag(body_files, "body", match=["scan.json", ".dxf"])
        )
        sew = pd.DataFrame(info_with_tag(sew_path, "sew"))
        cut = pd.DataFrame(info_with_tag(cut_path, "cut"))

        pieces = pd.DataFrame(info_with_tag(piece_images, "pieces"))
        pieces["is_in_meta_one"] = pieces["path"].map(
            lambda f: f in list(self._data["filename"])
        )

        all_data = pd.concat([sew, pieces, cut, body, meta])

        body_date = all_data[
            (all_data["tag"] == "body")
            & (all_data["filename"].map(lambda f: ".dxf" in f.lower()))
        ]

        meta_date = all_data[
            (all_data["tag"] == "meta")
            & (all_data["filename"].map(lambda f: "meta.json" in f.lower()))
        ].iloc[0]["last_modified"]

        res.utils.logger.debug(f"meta date {meta_date}")

        if len(body_date):
            date = body_date.iloc[0]["last_modified"]
            res.utils.logger.debug(f"body date {date}")

            if meta_date < date:
                res.utils.logger.warn(
                    "The meta-one does not seem to have been updated since the body was"
                )

        all_data["is_older_than_body"] = False
        all_data.loc[all_data["last_modified"] < date, "is_older_than_body"] = True

        return all_data

    def get_piece_identity_icons(self, display=True, show_edges=False):
        keys = self._data["key"]
        piece_names = [PieceName(p) for p in keys]

        data = [
            {
                "piece_name": str(p),
                "identity": p.identity,
                "icon": get_identity_label(p.identity),
            }
            for p in piece_names
        ]

        df = pd.DataFrame(data)

        if display:
            from IPython.display import HTML
            from res.utils import dataframes

            return HTML(
                df.to_html(
                    formatters={"icon": dataframes.image_formatter}, escape=False
                )
            )

        return df

    def get_summary(self, convert_floats=True, as_event_payload=False):
        """
        Adding some float conversion here just for dynamo. a little unfortunate

        when the event payloaqde
        """

        convert_floats = DynamoTable.convert_floats if convert_floats else lambda f: f

        att = convert_floats(
            {
                "path": self._home_dir,
                "body_code": self.body,
                "version": self.version,
                "color": self.color,
            }
        )

        if self.has_file("nesting_stats.feather"):
            att["nesting_stats"] = convert_floats(
                self.read_file("nesting_stats.feather").to_dict("records")
            )

        if self.has_file("cost_table.feather"):
            att["cost_table"] = convert_floats(
                self.read_file("cost_table.feather").to_dict("records")
            )

        meta_items = dict(convert_floats(self._meta))

        # unpack the metadata
        metadata = meta_items.pop("metadata") if "metadata" in meta_items else {}
        # unpack the size map
        sizes = meta_items.pop("sizes") if "sizes" in meta_items else {}
        for k, v in sizes.items():
            att["res_size"] = k
            att["size"] = v

        att.update(meta_items)
        att.update(metadata)

        att["last_modified"] = self.pieces_modified_date().isoformat()

        if as_event_payload:
            for a in [
                "piece_material_mapping",
                "material_props",
                "requested_meta_path",
                "validation_result",
            ]:
                if a in att:
                    att.pop(a)
            records = self._data[
                ["key", "type", "filename", "color_code", "material_code"]
            ].to_dict("records")
            att["pieces"] = records

        return att

    def _make_resNestv1_request(
        self,
        job_key,
        one_number=str(20000011),
        uri="https://datadev.resmagic.io/res-connect/res-nest",
        **kwargs,
    ):
        """
        e.g "s3://meta-one-assets-prod/styles/meta-one/kt_2011/v9/testc/2zzsm" , "LTCSL
        """
        # TODO:
        material = None
        path = self._home_dir
        one_number = self._one_number or one_number
        # take the asset as we would from res nest but the marker has its own path and maybe one number
        # the material should be chosen from the marker primary as we nest on some material but a secondary can be override
        ev = {
            "apiVersion": "v0",
            "kind": "Flow",
            "metadata": {"name": "dxa.printfile", "version": "expv2"},
            "args": {
                "assets": [
                    {
                        "value": path,
                        "material_code": material,
                        "key": one_number,
                    }
                ],
                "jobKey": job_key,
                "buffer": 75,
                "writeFile": True,
                "material": material,
            },
        }

        from res.utils.safe_http import request_post

        # TODO: write out where the print file will be saved based on job key and conventions

        return request_post(uri, json={"event": ev})

    @staticmethod
    def search_cache(
        key=None, body=None, bmc_res_key=None, sku=None, size=None, version=None
    ):
        if sku:
            if bmc_res_key is None and size is None:
                # unpack the res key and size from the sku if given
                parts = sku.split(" ")
                bmc_res_key = " ".join(parts[:3]).lstrip().rstrip()
                size = parts[-1].lstrip().rstrip()

        dynamo = res.connectors.load("dynamo")
        return dynamo["cache"][CACHE_NAME].search(
            **{
                "_id": key,
                "res_key": bmc_res_key.replace("-", "").replace("_", "").upper()
                if bmc_res_key
                else None,
                "size": size,
                "version": version,
                "body_code": body,
            }
        )

    def cache_summary(self):
        try:
            att = self.get_summary(convert_floats=True)

            res.utils.logger.info(f"Caching meta marker with key {att.get('key')}")
            dynamo = res.connectors.load("dynamo")
            key = dynamo["cache"][CACHE_NAME].put(att)
            return key
        except Exception as ex:
            res.utils.logger.warn(f"Unable to cache summary because {repr(ex)}")
            raise ex

    def __iter__(self):
        for key in self._data["key"]:
            yield self[key]

    def _repr_html_(self):
        return self._data._repr_html_()

    def __repr__(self) -> str:
        s = self._home_dir
        return f"MetaMarker({s})"

    def _list_associated_files(self):
        s3 = res.connectors.load("s3")
        return list(s3.ls(self._home_dir))

    def has_file(self, filename):
        s3 = res.connectors.load("s3")
        return s3.exists(f"{self._home_dir}/{filename}")

    def read_file(self, filename):
        s3 = res.connectors.load("s3")
        return s3.read(f"{self._home_dir}/{filename}")

    def pieces_modified_date(self):
        s3 = res.connectors.load("s3")
        last_modified = None
        # this is no longer safe because we are moving this - but maybe the cache still works here
        for last_modified in s3.ls_info(f"{self._home_dir.rstrip('/')}/pieces.feather"):
            pass
        return last_modified.get("last_modified") if last_modified else None

    def describe(self):
        d = self._meta

        d["piece_statistics"] = self.get_stats().drop("modified", 1).to_dict("records")
        # problem with this is we need a clear contract for how to average things or Blow up if not defined or risk ill defined stat
        # d["piece_statistics_aggregated"] = (
        #     self.get_stats(summarize=True).drop("modified", 1).to_dict("records")
        # )
        return d

    def as_asset_payload(self, rid):
        """
        asset status has these
        {
            "id": "recjfioIT2vjQQm3D",
            "unit_key": "DJ-6000 LY115 GREESG",
            "body_code": "DJ-6000",
            "body_version": "1",
            "color_code": "GREESG",
            "size_code": "4ZZLG",
            "dxf_path": "s3://meta-one-assets-prod/bodies/3d_body_files/dj_6000/v1/extracted/dxf/DJ-6000-V1-3D_BODY.dxf",
            "meta_one_path": "s3://meta-one-assets-prod/styles/meta-one/3d/dj_6000/v1_79b406fcf9/greesg/4zzlg",
            "flow": "3d",

            "task_key": "marker-res494d91a812-res3883fca0f7",

            "validation_error_map": {},
            "status": "IN_REVIEW",
            "tags": [],
            "followers": [],

            "node": "dxa",
            "unit_type": "RES_STYLE",
            "created_at": "2022-07-26T14:52:59.024065"
            }

            #augmented by notifier
            payload["validation_error_map"] = val_map
            payload["status"] = status
            payload["node"] = node
            payload["tags"] = fc.validations
            payload["created_at"] = res.utils.dates.utc_now_iso_string(None)
            payload["task_key"] = fc.key
        """

        return {
            "id": rid,
            "unit_key": self._meta["metadata"]["res_key"],
            "body_code": self.body,
            "flow": self._meta["metadata"]["flow"],
            "unit_type": self._meta["metadata"]["flow_unit_type"],
            "body_version": self.version.lower().replace("v", ""),
            "color_code": self._meta["color_code"],
            "size_code": self.size,
            "dxf_path": self._meta["metadata"]["meta_path"],
            "meta_one_path": self._home_dir,
        }

    def get_stats(self, summarize=False):
        return MetaMarker.get_stats_from_path(
            self._home_dir, res.utils.dates.utc_now(), summarize=summarize
        )

    def _bm_stats_to_warehouse(self, table_name="body_material_yard_usage"):
        """
        We store tables of data
        there could be different channels for this for different modes
        current use case is to get body-material statustucs about utilisation
        color can be ignored but we do need to think about combos etc.
        that combos would just be regular meta ones that rack all the partitions
        the simple use cases just asks what the one takes to make on the primary material
        """
        try:
            from res.connectors.snowflake import CachedWarehouse

            s = self.get_stats()
            s["key"] = s.apply(
                lambda row: f"{row['body'].replace('_', '-')}-{row['material_code']}-{row['size']}",
                1,
            )
            CachedWarehouse(table_name, key="key", df=s).write()
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to save meta one stats data to warehouse: {repr(ex)}"
            )

    @staticmethod
    def get_stats_from_path(pth, modified, summarize=True):
        """
        Utility that combines any useful stats to row level on the marker
        This would be a table in a relational db but here we can read from S3 and move to a warehouse

        The input can be fetched from an s3 query

        import pandas as pd
        df = pd.DataFrame(files)
        df['dir'] = df['path'].map(lambda x : "/".join(x.split('/')[:-1]))
        df['last_modified'] = df['last_modified'].map(str)
        dirs = dict(df.drop_duplicates(subset='dir')[['dir','last_modified']].values)

        for k,v in dirs.items():
            data = get_stats_from_path(k,v)

        we can merge this and use s3 partition saves on dates

        """
        s3 = res.connectors.load("s3")

        files = list(s3.ls(pth.rstrip("/")))
        all_paths = list(set(s3.object_dir(g) for g in files))

        if len(all_paths) == 0:
            return None

        def part(i, p):
            pts = p.split("/")
            return pts[i].upper()

        data = []

        for pth in all_paths:
            pth = pth.rstrip("/")
            if s3.exists(f"{pth}/nesting_stats.feather"):
                df = s3.read(f"{pth}/nesting_stats.feather")
                df["modified"] = modified
                df["path"] = pth
                if s3.exists(f"{pth}/cost_table.feather"):
                    dfc = s3.read(f"{pth}/cost_table.feather")
                    if len(dfc):
                        yrds = dfc.query(
                            "priceBreakdown_item=='Print' and priceBreakdown_unit=='yards'"
                        ).iloc[0]["priceBreakdown_quantity"]
                        df["createONEyards"] = yrds

                df["body"] = df["path"].map(lambda x: part(-4, x))
                df["version"] = df["path"].map(lambda x: part(-3, x))
                df["color"] = df["path"].map(lambda x: part(-2, x))
                df["size"] = df["path"].map(lambda x: part(-1, x))
                data.append(df)

        return pd.concat(data)

    def invalidate(self):
        return MetaMarker.invalidate_meta_one(self._home_dir)

    @staticmethod
    def invalidate_meta_one(path, reason="test"):
        s3 = res.connectors.load("s3")
        path = path.rstrip("/")
        data = s3.read(f"{path}/meta.json")
        data["invalidated_date"] = res.utils.dates.utc_now_iso_string()
        data["invalidation_reason"] = reason
        s3.write(f"{path}/meta.json", data)

        res.utils.logger.info(f"set invalidation date on meta one {path}")
        return data["invalidated_date"]

    @staticmethod
    def approve_meta_one(path, reason="test"):
        s3 = res.connectors.load("s3")
        path = path.rstrip("/")
        data = s3.read(f"{path}/meta.json")
        data["approval_date"] = res.utils.dates.utc_now_iso_string()
        s3.write(f"{path}/meta.json", data)

        res.utils.logger.info(f"set approval date on meta one {path}")
        return data["approval_date"]

    @staticmethod
    def remove_invalidation_for_meta_one(path):
        s3 = res.connectors.load("s3")
        path = path.rstrip("/")
        data = s3.read(f"{path}/meta.json")
        for c in ["invalidated_date", "invalidation_reason"]:
            if c in data:
                data.pop(c)

        s3.write(f"{path}/meta.json", data)
        res.utils.logger.info(f"set invalidation date on meta one {path}")
        return data

    @staticmethod
    def list_markers():
        """
        list the meta markers that have been created on the data lake
        this returns the paths - for example if we load this dataframe we can select the path of one of them as follows

          meta_markers = MetaMarker.list_markers()
          #select one
          mindex = 47
          my_meta_marker = MetaMarker(meta_markers.iloc[mindex]['meta_marker'])

        This function simply lists s3 paths and parses out the style attributes
        A Meta Marker constructor takes the path to the directory containing the meta marker files
        """
        s3 = res.connectors.load("s3")
        mms = pd.DataFrame(s3.ls(f"{META_ONE_ROOT}/"), columns=["path"])
        mms["part"] = mms["path"].map(lambda g: g.split("/")[-1])

        def f(row):
            return row["path"].replace(f"/{row['part']}", "")

        mms["meta_marker"] = mms.apply(f, axis=1)

        mms["size"] = mms["path"].map(lambda g: g.split("/")[-2].upper())
        mms["color"] = mms["path"].map(lambda g: g.split("/")[-3].upper())
        mms["body"] = mms["path"].map(
            lambda g: (g.split("/")[-5]).replace("_", "-").upper()
        )
        mms["body_versioned"] = mms["path"].map(
            lambda g: (g.split("/")[-5] + "-" + g.split("/")[-4])
            .replace("_", "-")
            .upper()
        )

        meta_markers = mms[mms["part"] == "pieces.feather"]
        return meta_markers

    def _reimport_style(self):
        """
        construct the payload and reimport the style
        """
        res_key = self._meta.get("res_key")
        if not res_key:
            # TODO: we can infer this but its not safe
            primary_material = (
                self._data.groupby("material_code").count().sort_values("key").index[0]
            )
            res_key = f"{self.body.replace('-','')} {primary_material} {self.color}"

        asset = {
            "key": res_key,
            "body_code": self.body,
            # this version can come from the style latest or we can ask for any version
            # if we want twe can force it to use the latest DXF file which might work if the pieces look the same - spec that in args
            "version": self.version,
            "color": self.color,
            "size": self.size,
        }

        from . import styles

        res.utils.logger.info(
            f"reimporting the asset {asset} to meta marker {self._home_dir}"
        )

        return styles.export_styles([asset])

    def _delete_meta_marker(self):
        """
        delete all the objects in the meta marker path
        """

        s3 = res.connectors.load("s3")
        files = [f for f in s3.ls(self._home_dir)] + [self._home_dir]
        res.utils.logger.debug(f"deleting meta marker objects {files}")
        s3.delete_files(files)

    def get_image_table_html(self):
        """
        Need to get the style record
        """
        artwork_file_id = self._meta.get("metadata", {}).get("artwork_file_id", "")
        style_thumbnail_url = self._meta.get("metadata", {}).get(
            "style_cover_image", ""
        )
        return MetaMarker.get_style_image_table(
            artwork_file_id=artwork_file_id, style_thumbnail_url=style_thumbnail_url
        )

    def get_brand_name(self):
        """
        when the marker is created we should store the brand name for it
        """
        return self._meta.get("metadata", {}).get("brand_name")

    def _verify(self):
        """
        check that the marker that was created is actually legit e.g. check the piece images exist
        """
        return True

    def _add_compensated_geometry(self):
        """
        compenated is without buffer - we only compensate a raw
        """

        df = self._data

        def f(row):
            g = row["raw_geometry"]

            material = row["material_code"]
            props = self._meta.get("material_props", {})
            if pd.isnull(props):
                raise Exception(
                    f"There are no material properties for {material} - this meant we could not nest the meta marker for that material"
                )
            props = props.get(material) if not pd.isnull(props) else None
            g = affinity.scale(
                g, xfact=props["compensation_width"], yfact=props["compensation_length"]
            )
            return g

        df["compensated_geometry"] = df.apply(f, axis=1)
        return df

    def _remove_buffered_geometry(self, applied_buffer=False):
        """
        the buffered material is the one that has any applied transform pre compensation
        for unstable materials it is material specific for the marker
        for block fused pieces we may have applied a buffer

        if the buffer is already applied this is a null op
        """

        from res.media.images.geometry import shift_geometry_to_origin

        res.utils.logger.debug(
            f"Removing physical buffer to make raw geometry for nesting"
        )

        def remove_buffer(g, buffer):
            return shift_geometry_to_origin((Polygon(g)).buffer(-1 * buffer).boundary)

        df = self._data

        def f(row):
            g = row["geometry"]

            material = row["material_code"]
            props = self._meta.get("material_props", {}).get(material)
            if props:
                b = props.get("offset_size")
                if b and not pd.isnull(b):
                    return remove_buffer(g, DPI * b)
            return g

        # the raw geom is just the geom if the buffer is not assumed in DXF which generallt its not
        df["raw_geometry"] = df.apply(f, axis=1) if applied_buffer else df["geometry"]
        return df

    def _add_physical_geometry(self, apply_buffer=True):
        """
        The physical shape is the one that is nested on material after all compensation and buffer is applied
        some material may not have buffers and we return the original geometry
        in meta-one, the buffer is already applied but in other cases its not
        """

        res.utils.logger.debug(
            f"Making physical geometry - apply buffer is {apply_buffer}"
        )

        df = self._data

        def f(row):
            g = row["geometry"]

            material = row["material_code"]

            props = self._meta.get("material_props", {})
            if pd.isnull(props):
                raise Exception(
                    f"There are no material properties for {material} - this meant we could not nest the meta marker for that material"
                )
            props = props.get(material) if not pd.isnull(props) else None
            if props:
                if apply_buffer:
                    # the offset size is in inches so DPI gives the correct value regardless of the units if the DXF
                    b = props.get("offset_size")
                    if b and not pd.isnull(b):
                        g = g.buffer(DPI * b)

                # assume comp is given
                g = affinity.scale(
                    g,
                    xfact=props["compensation_width"],
                    yfact=props["compensation_length"],
                )

            return g

        df["physical_geometry"] = df.apply(f, axis=1)
        return df

    def _pseudo_order_request(self, plan=False):
        """
        we can test ordering a ONE in this style
        this gerneates print pieces which we can then generate previews for when the task completes

        TODO -> lookup status on tasks

        TO preview generate...
        for m in mm.enumerate_extracted_order_pieces(env='development', post_preview=True):
            print('doing...')
        """
        a = {
            "id": res.utils.res_hash(),
            "one_number": self.one_number or "00000000",
            "body_code": self.body,
            "size_code": self.size,
            "color_code": self.color,
            # test-> the first material code for now, we can loop
            "material_code": self._data["material_code"].iloc[0],
            "uri": self._home_dir,
            "piece_type": "self",
            "metadata": {
                "max_image_area_pixels": None,
                "intelligent_marker_id": self._home_dir,
            },
        }

        if plan:
            return a

        TOPIC = "res_meta.dxa.prep_pieces_requests"
        kafka = res.connectors.load("kafka")
        kafka[TOPIC].publish(a, use_kgateway=True)

        res.utils.logger.info("done")

        return a

    def enumerate_extracted_order_pieces(self, env="production", post_preview=False):
        """
        TODO: check env params as this must work with the settings from pre print pieces e.g. the experiment id
        """
        s3 = res.connectors.load("s3")

        path = f"s3://res-data-{env}/flows/v1/dxa-prep_ordered_pieces/primary/extract_parts/{self.key}"
        res.utils.logger.debug(f"glob {path}")

        for file in s3.ls(path):
            res.utils.logger.debug(f"reading {file}")
            im = s3.read(file)

            if post_preview:
                a = {
                    "one_number": self.one_number,
                    "sku": self._meta.get("metadata", {}).get("res_key"),
                    "image_path": file,
                    "key": file.split("/")[-2] + "/" + file.split("/")[-1],
                }

                MetaMarker._add_image_previews(a)
            yield im

    @staticmethod
    def _add_image_previews(assets):
        """
        post previews to
        https://airtable.com/apprcULXTWu33KFsh/tblWyJKrXJYGgWtv5/viwqAkvnZnaUpORPU?blocks=hide
        """
        if not isinstance(assets, list):
            assets = [assets]
        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")
        tab = airtable["apprcULXTWu33KFsh"]["tblWyJKrXJYGgWtv5"]

        s = "https://airtable.com/apprcULXTWu33KFsh/tblWyJKrXJYGgWtv5/viwqAkvnZnaUpORPU?blocks=hide"
        res.utils.logger.info(f"adding previews to {s}")

        # for item resolve record id and
        for i in assets:
            res.utils.logger.debug(f"adding preview for {i}")
            try:
                name = i["key"]
                rec_id = None  # ids.get(name)
                one = i["one_number"]
                sku = i["sku"]
                body = i["sku"].split(" ")[0].lstrip().rstrip()
                preview_path = i["image_path"]
                # print(preview_path)
                tab.update_record(
                    {
                        "record_id": rec_id,
                        "Name": name,
                        "Attachments": [
                            {"url": s3.generate_presigned_url(preview_path)}
                        ],
                        "ONE": one,
                        "SKU": sku,
                        "Body": body,
                    },
                    use_kafka_to_update=False,
                )
            except Exception as ex:
                print("failed", repr(ex))

        res.utils.logger.info("Done")

    def get_nesting_statistics(self, plot=False, return_dataframes=False):
        """
        When creating a meta marker we can also get nesting statistics and save them for costing etc.
        this is done for each material group in the marker but how we group things may change in future
        """
        from res.learn.optimization.nest import (
            nest_dataframe,
            evaluate_nesting,
            summarize_nest_evaluation,
            plot_comparison,
        )

        dfs = []

        def _nest_partitions(
            self, geometry_column="buffered_geometry", summarize=True, **kwargs
        ):
            """
            The buffered geometry should default to geom if not exists
            """

            # mode dxf includes buffer
            # if it does, physical geometry does not add it only compensate. otherwise add
            # ALSO, if it does not, the raw geometry is the as is one otherwise we need to remove the buffer
            # the compensated one should always be the raw one processed - assert raw exists

            if "physical_geometry" not in self._data:
                # TODO: should buffer
                apply_buffer = True
                self._add_physical_geometry()

            if "unbuffered_geometry" not in self._data:
                applied_buffer = False
                self._remove_buffered_geometry(applied_buffer)

            if "compensated_geometry" not in self._data:
                self._add_compensated_geometry()

            results = []

            for material_code, data in self._data.groupby("material_code"):
                # this is important for the join
                data = data.reset_index().drop("index", 1)
                res.utils.logger.info(
                    f"Nesting material {material_code}  for the geometry columns {geometry_column} - there are {len(data)} items in this set"
                )
                props = self._meta.get("material_props", {}).get(material_code)
                # physical prroperties
                width = DPI * props.get("cuttable_width")
                # these are intentionally NOT used because we have already compensated the pieces in the physical
                # nesting now adds buffers last as a constant everywhere
                # stretch_x = props.get("compensation_width")
                # stretch_y = props.get("compensation_length")
                df = nest_dataframe(
                    data,
                    geometry_column=geometry_column,
                    output_bounds_width=width,
                    plot=plot,
                )

                df["geometry_column"] = geometry_column

                dfs.append(df.copy())

                df = evaluate_nesting(df, geometry_column=f"nested.{geometry_column}")

                if summarize:
                    df = pd.DataFrame([summarize_nest_evaluation(df)])
                    df["material_code"] = material_code
                    df["column"] = geometry_column

                results.append(df)

            return pd.concat(results).reset_index().drop("index", 1)

        # get for each material the nest statistics
        # the default is actually the baseline geometry so we can measure the impact of applying physical considerations per material
        # we can independently looked at buffered-and-compensated and buffered shapes

        # by default this would be true
        # df["raw_geometry"] = df["geometry"]

        res.utils.logger.debug("nesting raw geometry")
        n1 = _nest_partitions(self, geometry_column="raw_geometry")

        res.utils.logger.debug("nesting physical geometry")
        n2 = _nest_partitions(self, geometry_column="physical_geometry")

        res.utils.logger.debug("nesting compensated (but not buffered) geometry")
        n3 = _nest_partitions(self, geometry_column="compensated_geometry")

        alldfs = pd.concat(dfs).reset_index().drop("index", 1)
        if plot:
            plot_comparison(alldfs)

        if return_dataframes:
            return alldfs

        # join meta and physical summaries of piece areas, nesting dims, packing factor -> these are per material group
        for key, df in {"physical": n2, "compensated": n3}.items():
            n1 = (
                n1.join(df, rsuffix=f"_{key}")
                .drop(
                    [
                        # this must conform to the contract from nest evaluation nameing
                        # this is also going to match the raw kafka topic
                        f"piece_count_{key}",
                        # f"width_nest_yds_{key}",
                        f"material_code_{key}",
                        # f"column_{key}",
                        "column",
                    ],
                    1,
                )
                .round(3)
            )

        n1 = n1.reset_index().drop("index", 1)

        stats = (
            "height_nest_yds",
            "width_nest_yds",
            "area_nest_yds",
            "area_nest_utilized_pct",
            "area_pieces_yds",
        )

        for col in stats:
            n1[f"{col}_raw"] = n1[col]

        return n1

    def show_piece_annotation_plans(self, drop_duplicate_ops=True):
        from IPython.display import HTML
        from res.utils.dataframes import image_formatter

        data = []
        for p in self:
            data.append(p.show_annotation_plan(False))
        data = pd.concat(data)
        if drop_duplicate_ops:
            data = data.drop_duplicates(subset=["name"])
        return HTML(data.to_html(formatters={"icon": image_formatter}, escape=False))

    # @retry(wait=wait_fixed(5), stop=stop_after_attempt(4))
    def update_assembly_asset_links(self, record_id, ex_date=None, plan=False):
        """
        When we prepare print pieces we are also preparing for assembly
        prep print pieces will be a good hook to make sure that the assets related to the pieces are also available

        - the factory order pdf
        - cut files

        #without a record id we cannot relate this to an actual production order
        ="rec807L7wfuvcHRUy"
        WHEN the record id is null we reolve

        we are tenacious here becasue make one production often clogged
        """

        s3 = res.connectors.load("s3")

        res.utils.logger.debug(
            f"Updating assembly data for meta one - see record {record_id} for order {self.one_number}"
        )
        ex_date = ex_date or res.utils.dates.utc_now()
        # for production requests
        body = self.body.replace("-", "_").lower()
        body_version = str(self.body_version).lower().replace("v", "")

        # lowered from the path - flaky dep on the path but we can validate
        size = self.size.lower()  # as above; also note this is the accu size
        # assert the meta one was conssturcted with the one number
        # resolve this from the one_number and then resolve the

        if record_id is None:
            pass
            # resolve it here
            # maybe update the exit date here too

        airtable = res.connectors.load("airtable")
        prod = airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"]

        # we can get this date from the prod request but its unfortunate two way
        # cut_files = f"s3://meta-one-assets-prod/bodies/cut/{body}/{size}"
        cut_files = f"s3://meta-one-assets-prod/bodies/cut/{body}/v{body_version}"
        cut_files = f"{cut_files}/{size}"
        cut_files = list(s3.ls(cut_files))

        # filtering cut files for the new format - this meta marker is deprecated anyway as is the assembly we are posting to here
        def has_key(p):
            key = f"{self.body}-V{str(self.body_version).lower().replace('v','')}"
            return key in p

        cut_files = [
            item
            for item in cut_files
            if ("stamper_" in item.lower() or "fuse_" in item.lower()) and has_key(item)
        ]

        res.utils.logger.info(
            f"constructing attachments payload: cut files in {cut_files}"
        )

        # for now just add the plotter files
        purls = [s3.generate_presigned_url(f) for f in cut_files if ".plt" in f.lower()]

        furl = self.get_factory_order_document(
            factory_exit_date=ex_date, as_pdf_url=True
        )

        payload = {
            "record_id": record_id,
            # TODO - this is not the correct name will check on testing from prep pieces
            "Factory Order PDF": [{"url": furl}],
            # could add fuse pieces too
            "Paper Markers": [{"url": purl} for purl in purls],
            # cut files for nest if exists
        }

        if not plan:
            res.utils.logger.debug(f"Posting attachments...")
            prod.update_record(
                payload,
                use_kafka_to_update=False,
                RES_APP="metaone",
                RES_NAMESPACE="meta",
            )

        return payload

    def _rollback_files(self, plan=True):
        """
        plan will shown the file versions and if we dont plan we bump down the version
        use case: we have incorrectly corrupted files on a meta one (you need to know what youre doing)
        """
        s3 = res.connectors.load("s3")
        records = []
        for filename in self._data["filename"]:
            res.utils.logger.debug(f"rolling back file to the previous version")
            data = s3.rollback_object(filename, plan=plan)
            records += data

        df = pd.DataFrame(records)
        df["key"] = df["filename"].map(lambda x: x.split("/")[-1])

        return df

    def publish_pieces_with_res_color_layers(
        m, body_code, piece_key_filter=None, raw_only=False, **kwargs
    ):
        """
        A utility method in dev time to show latest reviews of pieces
        """

        # TODO: could add the record id to the sew df which we save a round trip - stop being stubborn
        airtable = res.connectors.load("airtable")
        s3 = res.connectors.load("s3")
        raise_piece_fail = kwargs.get("on_piece_error", "raise")

        res.utils.logger.info(
            f"looking up pieces and bodies data from airtable - we need the record id"
        )

        pieces = airtable["appa7Sw0ML47cA8D1"]["tbl4V0x9Muo2puF8M"]
        piece_lookup = dict(
            pieces.to_dataframe()[["record_id", "Generated Piece Code"]].values
        )

        def piece_name_resolver(s):
            if isinstance(s, list):
                return [piece_lookup.get(k) if k[:3] == "rec" else k for k in s][0]

        tab = airtable["appa7Sw0ML47cA8D1"]["tblUGsbA6ApGp3ZXc"]
        data = tab.to_dataframe(
            fields=["Name", "Piece", "Preview", "Body Number", "Preview Last Updated"],
            filters=f"""SEARCH(  "{body_code}",   {{Body Number}}   )""",
        ).explode("Body Number")

        if len(data) == 0:
            raise Exception(
                "There are no sew data - at this stage we must have records existing for sew - be sure to push the pieces"
            )
        data = data[data["Body Number"] == body_code].reset_index()

        data["piece_key"] = data["Piece"].map(piece_name_resolver)

        # note the piece level assump. we add the body name which is used locally in our look up
        data["key"] = data.apply(lambda row: f"{body_code}-{row['piece_key']}", axis=1)

        lu = dict(data[["key", "record_id"]].values)

        print(f"using keys like{data.iloc[0]['key']} t'match keys   ")

        failures = {}
        for p in m:
            if piece_key_filter and p["key"] not in piece_key_filter:
                continue

            res.utils.logger.info(
                f"Working on piece {p.piece_name} - will lookup using the name which has no size {p}"
            )

            try:
                rec_id = lu.get(p["piece_key_no_version"])

                if "/pieces/" not in p["filename"]:
                    # adding this for safety because we dont want to overwrite files and the way below is a bit silly
                    raise Exception(
                        "Contract failed: there pieces are expected to be in a pieces folder"
                    )

                preview_path = p["filename"].replace("pieces", "sew_preview")
                geoms_path = p["filename"].replace("pieces", "annotated_images")

                res.utils.logger.info(
                    f"Will fetch and push sew preview and annotations to {preview_path} and {geoms_path}"
                )
                geoms = p.annotated_image
                preview = p.labelled_piece_image

                s3.write(geoms_path, geoms)
                s3.write(preview_path, preview)

                res.utils.logger.info(
                    f"Publishing the uploads to airtable for piece {p.piece_name} using pre signed URLs on s3"
                )
                #
                if rec_id:
                    tab.update_record(
                        {
                            "record_id": rec_id,
                            "Preview": [
                                {"url": s3.generate_presigned_url(preview_path)}
                            ],
                            "Raw Piece Geometries": [
                                {"url": s3.generate_presigned_url(geoms_path)}
                            ],
                            "Preview Last Updated": res.utils.dates.utc_now_iso_string(),
                        },
                        use_kafka_to_update=kwargs.get("use_kafka_to_update", False),
                    )
                else:
                    res.utils.logger.warn(
                        f"Will not create a new record in sew assingation - expect pieces to exist for the body but have not yet been mapped"
                    )

            except Exception as ex:
                res.utils.logger.warn(
                    f"Failed on this piece {p.piece_name}:> {res.utils.ex_repr(ex)}"
                )
                if raise_piece_fail == "raise":
                    raise ex
                failures[str(p.piece_name)] = repr(ex)

        res.utils.logger.info(f"DONE.")

        return failures

    def piece_type_counts(self, groupby_color=True):
        """
        The types of pieces sets broken down by material etc
        Used to both define the meta one i.e. what sorts of pieces it has and for printing we can generate separate print assets
        """
        return (
            self._data.groupby(
                ["material_code", "type"] + (["color_code"] if groupby_color else [])
            )
            .count()[["key"]]
            .rename(columns={"key": "count"})
        )

    def _request_for_marker(self):
        materials = self._data["material_code"].unique()
        path = self._home_dir
        key = "-".join(path.split("/")[-4:]).replace(" ", "-")
        nest_key = f"nest-{key.lower()}".replace("_", "-").rstrip("-")

        for m in materials:
            yield {
                "apiVersion": "v0",
                "kind": "Flow",
                "metadata": {"name": "dxa.printfile", "version": "expv2"},
                "args": {
                    "material": "CTNBA",
                    "jobKey": nest_key,
                    "assets": [
                        {
                            "key": self.one_number or "9999999",
                            "value": path,
                            "material_code": m,
                        }
                    ],
                },
            }

    def nest_meta_one(self, **kwargs):
        """
        We nest on dev to test the meta one by default
        We can pass env: production to nest on dev server

        This method is a simple hook without any interesting error handling
        """
        from res.utils.safe_http import request_post

        env = kwargs.get("env", "dev")

        domain = "datadev" if env != "production" else "data"

        server = f"https://{domain}.resmagic.io/res-connect/res-nest"

        res.utils.logger.info(f"Submitting to env {env}: {server}")

        for event in self._request_for_marker():
            res.utils.logger.info(f"submitting request {event}")
            r = request_post(
                server,
                json={"event": event},
            )

            if r.status_code != 200:
                print("FAIL", r.json())
                return
            # check errors

            return r.json()

        res.utils.logger.info(f"all requests submitted")

    @staticmethod
    def meta_one_piece_type_counts(path):
        s3 = res.connectors.load("s3")
        if s3.exists(path):
            m = MetaMarker(path).piece_type_counts()
            return m.piece_type_counts
        else:
            res.utils.logger.warn(f"There is no marker as the location {path}")

    @staticmethod
    def _path_components(path, s3_connector):
        """
        The conventional path format is iportant as we parse out BMC type info and also versions
        """
        s3 = s3_connector or res.connectors.load("s3")
        path = s3.object_dir(path)
        path = path.rstrip("/")
        parts = path.split("/")
        v = parts[-3]
        return {
            "size": parts[-1],
            "color": parts[-2],
            "body_version": v.split("_")[0],
            "meta_version": v,
            "body": parts[-4],
        }

    @staticmethod
    def from_sku(
        sku,
        m1hash=None,
        body_version=None,
        flow="3d",
        one_number=None,
        valid_only=True,
        approved_only=None,
        return_metadata_only=False,
        load_meta_one=True,
    ):
        """
        supply the one number just to simulate creating a meta one for a given order
        """
        parts = sku.split(" ")
        assert (
            len(parts) == 4
        ), "The sku must be for the form <BODY MATERIAL COLOR SIZE>"

        body = parts[0]
        if "-" not in body:
            body = f"{body[:2]}-{body[2:]}"

        results = MetaMarker.find_meta_one(
            body,
            body_version=body_version,
            color=parts[2],
            size=parts[3],
            material_hint=parts[1],
            flow=flow,
            sku=None,
            m1hash=m1hash,
            valid_only=valid_only,
            approved_only=approved_only,
        )

        if len(results) == 0:
            return None

        if return_metadata_only:
            # remove NaNs
            results = results.where(pd.notnull(results), None)

            return dict(results.to_dict("records")[0])

        if load_meta_one:
            return MetaMarker(results.iloc[0]["path"], one_number=one_number)
        return results

    @staticmethod
    def find_meta_one(
        body,
        body_version=None,
        color=None,
        material_hint=None,
        size=None,
        res_size=None,
        flow="3d",
        sku=None,
        m1hash=None,
        load_metadata=False,
        valid_only=True,
        approved_only=None,
        _invalidate_result=False,
        _approve_result=False,
        path=None,
        modified_before=None,
    ):
        """
        Use the path to find specific meta one matches
        """

        def try_body_integer_version_from_path(s):
            try:
                return int(s.split("/")[7].split("_")[0].replace("v", ""))
            except:
                res.utils.logger.debug(
                    f"We are failing to parse the body version from path {s}"
                )
                return 0

        def check_decimal(d):
            from decimal import Decimal

            def f(i):
                return float(i) if isinstance(i, Decimal) else i

            return {k: f(v) for k, v in d.items()}

        from res.connectors.dynamo import DynamoTable
        from datetime import timedelta
        from dateparser import parse

        s3 = res.connectors.load("s3")
        cache = DynamoTable(CACHE_NAME, "cache")
        matches = cache.search(
            # norm
            body_code=body.upper().replace("_", "-"),
            body_version=body_version,
            color_code=color.upper() if color else None,
            size=size.upper() if size else None,
            res_size=str(res_size).upper() if res_size else None,
            flow=flow,
            path=path,
        )

        if "last_modified" not in matches:
            matches["last_modified"] = None

        # back compat default
        matches["last_modified"] = matches["last_modified"].fillna("2000-01-01")

        res.utils.logger.debug(f"found {len(matches)} on first pass")
        assert (
            "-" in body or "_" in body
        ), "The body code {body} should contain a _ or -"
        if body_version and str(body_version)[0] != "v":
            body_version = str(body_version).lower()
            body_version = f"v{body_version}"
        body = body.replace("-", "_").lower()

        # # badsed on approvals and invalidations
        def is_valid(row):
            d = row.get("invalidated_date")

            if pd.isnull(d):
                return True

            return parse(row["last_modified"]) > (
                parse(d).replace(tzinfo=None) + timedelta(minutes=2)
            )

        def is_approved(row):
            d = row.get("approval_data")
            if pd.isnull(d):
                return False

            return parse(row["last_modified"]) > (
                parse(d).replace(tzinfo=None) + timedelta(minutes=2)
            )

        if len(matches) > 0:
            matches["sku"] = matches.apply(
                lambda row: f"{row['res_key']} {row['size']}", axis=1
            )
            matches["body_version"] = matches["path"].map(
                lambda s: s.split("/")[7].split("_")[0].replace("v", "")
            )

            if body_version:
                # print(matches.to_dict("records"), body_version)
                matches = matches[
                    matches["body_version"].map(
                        lambda x: str(x) == str(body_version.replace("v", ""))
                    )
                ]

            if material_hint:
                matches = matches[
                    matches["res_key"].map(
                        lambda x: x.split(" ")[1].lstrip().rstrip() == material_hint
                    )
                ]

            if len(matches) == 0:
                res.utils.logger.warn(f"No match")
                return pd.DataFrame()
            # we will take the latest body version on the latest date
            matches = matches.sort_values(
                ["body_version", "last_modified"], ascending=[False, False]
            )
            u_matches = list(matches["path"].unique())

            if len(u_matches) > 1:
                if m1hash:
                    res.utils.logger.debug(f"Disambiguating on hash {m1hash}")
                    matches = matches[matches["path"].map(lambda x: m1hash in x)]
                elif sku:
                    res.utils.logger.debug(f"Disambiguating on sku {sku}")
                    matches = matches[matches["sku"].map(lambda x: sku == x)]

            if valid_only:
                if _invalidate_result:
                    for record in matches.to_dict("records"):
                        record["invalidation_date"] = MetaMarker.invalidate_meta_one(
                            record["path"]
                        )
                        res.utils.logger.debug("saving invalidation to cache")
                        cache.put(record)
                # dont approve if we are invalidation of cache_summary
                elif _approve_result:
                    for record in matches.to_dict("records"):
                        record["approval_date"] = MetaMarker.approve_meta_one(
                            record["path"]
                        )
                        res.utils.logger.debug("saving approval to cache")
                        cache.put(record)
                res.utils.logger.debug(f"Filtering out invalid meta-ones")
                matches = matches[matches.apply(is_valid, axis=1)]

            if approved_only:
                res.utils.logger.debug(f"Filtering out non approved meta-ones")
                matches = matches[matches.apply(is_approved, axis=1)]
        if len(matches) == 0:
            res.utils.logger.warn(f"No match")
            return pd.DataFrame()

        # dynamo does annoying things with floats
        try:
            matches["nesting_stats"] = matches["nesting_stats"].map(
                lambda n: [check_decimal(i) for i in n]
            )
            matches["material_props"] = matches["material_props"].map(
                lambda n: {k: check_decimal(v) for k, v in n.items()}
            )
        except:
            pass

        return matches.reset_index()

    @staticmethod
    def sync_sew_for_body(meta_one_path, body, **kwargs):
        """
        The meta one can be inferred from the path but just in case we are being explicit here
        choose a body code and a specific meta one on any flow and then use it to update sew interface
        """
        from res.flows.meta.construction import get_sew_assignation_for_body
        from res.flows.meta.marker import MetaMarker
        from res.media import images

        images.text.ensure_s3_fonts()
        images.icons.extract_icons()

        s3 = res.connectors.load("s3")
        body_lower = body.replace("-", "_").lower()

        # fetch the sew data from airtable and push to s3
        sew_df = get_sew_assignation_for_body(body=body, ensure_pieces=True)
        if sew_df is not None:
            if len(sew_df):
                s3.write(
                    f"s3://meta-one-assets-prod/bodies/sew/{body_lower}/edges.feather",
                    sew_df,
                )

        # load the meta one with sew data and publish
        m = MetaMarker(meta_one_path)

        m.publish_pieces_with_res_color_layers(
            use_kafka_to_update=False, body_code=body
        )

        res.utils.logger.info(
            "synced body by updating sew details for the body and pushing the meta one for preview images"
        )

    @staticmethod
    def make_body_metaone(body, flow="3d", sync_sew=False, force_generate_color=True):
        """
        generate a meta one from the dxf in any flow
        TODO pass in if we should regenerate the white color or not
        """
        from res.flows.meta.marker import _decorate_assets, export_meta_one
        from res.flows.meta.bodies import (
            get_body_request_payloads,
        )

        b = list(get_body_request_payloads(body, flow=flow, default_size_only=True))
        a = export_meta_one(
            _decorate_assets(b)[0], force_generate_color=force_generate_color
        )

        if sync_sew:
            MetaMarker(a["meta_one_path"]).publish_pieces_with_res_color_layers(
                use_kafka_to_update=False
            )
        return a


#######
##     Some helpers for linking meta markers to orders and getting stats or processing ingested meta markers
######


def get_task_logs(task_key, env):
    """
    The meta one runs with a task key that we record in status events
    use this to retrieve the logs
    """
    if env == "development":
        env = "dev"

    k = f"s3://{k}/{task_key}"
    s3 = res.connectors.load("s3")

    files = list(s3.ls_info(f"s3://argo-{env}-artifacts/{task_key}"))
    logs = [list(s3.read_text_lines(f["path"])) for f in files]
    return logs


def get_request_for_style_size_bundle(
    unit_key,
    request_id=None,
    generate_color=False,
    flow="3d",
    sizes_in=None,
    body_version=None,
    style_id=None,
):
    """
    Example 'CC3040 CFT97 ENITZY'

    We can load some styles that we want to process meta ones for
    we can check that we have meta objects in conventional location or we can create color for the style
    Currently this is just used for testing or back testing styles against meta ones
    For a normal flow we should have something all make a meta one request as per the payload

    """
    from res.flows.meta.styles import (
        read as read_styles,
        gen_style_assets_for_sizes,
        gen_3d_style_bundle_assets_for_sizes,
    )

    def request_id_for(sc):
        from res.connectors.airtable import AirtableConnector

        airtable = res.connectors.load("airtable")

        tab = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
        keys = AirtableConnector.make_key_lookup_predicate([sc], "Style Code")
        rec = tab.to_dataframe(filters=keys).iloc[-1]["record_id"]

        res.utils.logger.info(f"Using request id {rec}")

        return rec

    res.utils.logger.info(
        f"Will use request id supplied or look one up for this key {unit_key} ({style_id})"
    )
    s = dict(read_styles(unit_key, style_id=style_id).iloc[0])

    # we could use something else but not sure if  i want to yet lets see
    s["request_id"] = request_id or request_id_for(unit_key)

    sizes = sizes_in or list(s["size_map"].keys())
    if (flow or "").lower() == "3d":
        style_request = gen_3d_style_bundle_assets_for_sizes(
            s, sizes=sizes, body_version=body_version
        )
        # infer style request from path - needs meta path and bd path

    else:
        style_request = gen_style_assets_for_sizes(
            s, sizes=sizes, allow_newer_meta=True
        )

    body_meta = dict(style_request[["size", "path_meta"]].values)
    body_color = dict(style_request[["size", "path_bc"]].values)

    def mapping_with_default_material(s):
        d = s.get("alt_piece_mapping", {})
        if pd.isnull(d):
            d = {}
        d.update({"default": s.get("primary_material")})
        return d

    res.utils.logger.debug(f"Body size mapping {s['size_map']}")

    payloads = [
        {
            "id": s["request_id"],
            "unit_key": unit_key,
            "body_code": s["body_code"],
            "body_version": int(s["body_version"]),
            "color_code": s["color_code"].upper(),
            "size_code": size,
            # thi is added
            "normed_size": s["size_map"].get(size),
            "dxf_path": body_meta[size],
            # optional only if we are generating color
            "pieces_path": body_color.get(size),
            "piece_material_mapping": mapping_with_default_material(s),
            "piece_color_mapping": {"default": s["color_code"].upper()},
            "material_properties": s["material_properties"],
            "status": "ENTERED",
            "tags": [],
            "flow": flow,
            "generate_color": generate_color,
            "unit_type": "RES_STYLE",
            "created_at": res.utils.dates.utc_now_iso_string(),
        }
        for size in sizes
    ]

    return payloads


res.flows.flow_node_attributes("metamarker_stats", memory="4Gi")


def migrate_marker_stats(modified_after=14):
    """
    migrate statistics for any meta markers created since date into snowflake
    """
    s3 = res.connectors.load("s3")
    snowflake = res.connectors.load("snowflake")

    files = list(
        s3.ls_info(
            META_ONE_ROOT,
            modified_after=res.utils.dates.relative_to_now(modified_after),
        )
    )
    df = pd.DataFrame(files)
    df["dir"] = df["path"].map(lambda x: "/".join(x.split("/")[:-1]))
    df["last_modified"] = df["last_modified"].map(str)
    dirs = dict(df.drop_duplicates(subset="dir")[["dir", "last_modified"]].values)

    res.utils.logger.info(f"migrating {len(dirs)} marker statistics")

    data = []
    for k, v in dirs.items():
        data.append(MetaMarker.get_stats_from_path(k, v))
    data = pd.concat(data)

    entity_name = "marker_stats"

    s3.write_date_partition(
        data,
        "modified",
        entity=entity_name,
        dedup_key="path",
    )

    stage_root = f"s3://res-data-platform/entities/{entity_name}/ALL_GROUPS/"

    res.utils.logger.info(f"staging at {stage_root} and sending to snowflake")

    return snowflake.ingest_from_s3_path(stage_root=stage_root, name=entity_name)


def augment_orders_with_meta_markers(df):
    """
    Do the attribute join on what we know about production requests and meta markers
    """
    if len(df) == 0:
        return df

    # in future we can find a faster way to lookup the markers we ned
    res.utils.logger.info("looking up meta markers and merging to production orders")
    markers = MetaMarker.list_markers()

    # to get examples of these that have markers we can load and join on BCS props

    prod = df[["key", "sku", "size_code", "body_code"]].reset_index()
    prod["color_code"] = prod["sku"].map(lambda x: x.split(" ")[-2].lstrip().rstrip())
    prod = pd.merge(
        prod,
        markers,
        left_on=["body_code", "size_code", "color_code"],
        right_on=["body", "size", "color"],
    )
    prod["rank"] = prod["body_versioned"].map(
        lambda x: int(x.split("-")[-1].lower().replace("v", ""))
        if "v" in x.lower()
        else -1
    )
    prod.sort_values(["sku", "rank"])
    prod = prod.drop_duplicates(subset=["key"], keep="last")
    res.utils.logger.info(f"{len(prod)} meta marker mappings loaded for orders")
    return prod[
        ["key", "sku", "size_code", "body_code", "color_code", "rank", "meta_marker"]
    ]


def resolve_for_ordered_assets(order_assets):
    """
    Get meta markers for orders if exists

    the keys are the ONE numbers and the material is also included
    we need to convert the asset to a path which we can replace in the asset.value and then we can load the meta marker from the asset
    the meta marker should be tested to load the correct pieces for the asset request and then we can get stats for the correct pieces

    Example:

    all_markers = resolve_for_ordered_assets([
        {
            "key": '10210903',
            "value" : "s3://meta-one-assets-prod/styles/meta-one/kt_3038/v4/midnbb/3zzmd"
        },
         {
            "key": '10208277',
            "value" : "NA"
        }
        ,
         {
            "key": '10210633',
            "value" : "NA",
            "material_code" : 'CTNBA'
        }
    ])
    """

    from res.connectors.airtable import AirtableConnector

    dynamo = res.connectors.load("dynamo")
    airtable = res.connectors.load("airtable")

    ones = [a["key"] for a in order_assets]

    res.utils.logger.info(f"Query ones: {ones} in production orders")

    prod_orders = dynamo["cache"]["airtable_make_one_production"].get(ones)
    found_ones = list(prod_orders["key"].unique()) if len(prod_orders) else []

    res.utils.logger.info(
        f"found {len(prod_orders)} production requests in cache for assets of length {len(order_assets)}"
    )

    missing_ones = [o for o in ones if o not in found_ones]

    if missing_ones:
        res.utils.logger.info(
            f"using airtable directly to find {len(missing_ones)} ONEs not in cache..."
        )
        aprod_orders = airtable.get_airtable_table_for_schema_by_name(
            "make.production_requests",
            filters=AirtableConnector.make_key_lookup_predicate(
                missing_ones, "Order Number v3"
            ),
        )
        res.utils.logger.info(
            f"found additional {len(aprod_orders)} records directly for assets of length {len(order_assets)}"
        )
        prod_orders = (
            pd.concat([prod_orders, aprod_orders])
            .drop_duplicates()
            .reset_index()
            .drop("index", 1)
        )

    if len(prod_orders) == 0:
        res.utils.logger.warn(
            f"did not find production requests for assets {order_assets}"
        )
        return []

    prod_orders = augment_orders_with_meta_markers(prod_orders)
    lookup_path = dict(prod_orders[["key", "meta_marker"]].dropna().values)

    res.utils.logger.info(
        f"found {len(lookup_path)} meta markers for these style orders"
    )

    def resolve_meta_marker(a):
        if META_ONE_ROOT in a["value"]:
            return MetaMarker.from_restNestv1_asset(a)
        # if we using legact ones, lookup our meta marker lookup for ones
        path = lookup_path.get(a["key"])
        if path:
            newa = dict(a)
            newa["value"] = path
            return MetaMarker.from_restNestv1_asset(newa)

    order_asset_markers = [resolve_meta_marker(a) for a in order_assets]

    return [a for a in order_asset_markers if a is not None]


def _aggregate_metamarker_nesting_stats(markers):
    """
    Load a set of markers and then aggregate all nesting stats in those markers
    this is called at nesting time but loads data generated at style on board time
    - we should gate the creation of the status and upsert of the stats
    - after we nest we can easily update the records written to disk but we could
      such a job would simply run over the save nesting feathers and re-evaluate the nest with the new numbers
      we prefer to run it contiously at the end of nesting jobs for latency and low process overhead
    """

    # this is a bit of a hack to add the create one data onto the nesting status and avoid future joins
    def try_merge_cone_yards(df, m):
        try:
            data = m.read_file("cost_table.feather")
            expr = f"priceBreakdown_item=='Print' and priceBreakdown_category=='Labor' and priceBreakdown_unit == 'yards'"
            df["create_one_yards"] = data.query(expr).iloc[0]["priceBreakdown_quantity"]
        except:
            df["create_one_yards"] = None

        return df

    df = pd.concat(
        [
            try_merge_cone_yards(m.read_file("nesting_stats.feather"), m)
            for m in markers
            if m.has_file("nesting_stats.feather")
        ]
    )

    aggregates = {}

    for c in df.columns:
        if c in ["material_code"]:
            continue
        elif c not in ["area_nest_utilized_pct", "area_nest_utilized_pct_physical"]:
            aggregates[c] = "sum"
        else:
            aggregates[c] = "mean"

    result = dict(df.agg(aggregates).round(3))

    result["asset_count"] = len(markers)

    return result


def orders_with_metamarkers(nested=None):
    """
    This is useful for testing because we want things that have been ordered, already nested and also have meta markers
    we can test the flow with these positive cases

    the flow will change in future where new production requests in meta marker world will always know there meta markers when created
    """
    dynamo = res.connectors.load("dynamo")
    df = dynamo["cache"]["airtable_make_one_production"].read()
    df = augment_orders_with_meta_markers(df)

    if nested:
        # lookup print asset that we have nested and confirmed
        QUERY = """SELECT
            "Key",  "confirmed_utilized_nested_asset_sets", "SKU", "Body", "Rank", "Asset Name", "Print Queue", "Rolls Assignment v3", "Utilized Nested Asset", "Nested Asset Paths", "Size"
        FROM "IAMCURIOUS_DB"."IAMCURIOUS_SCHEMA"."Print_Assets_flow"
        WHERE "Print Queue" = 'PRINTED'
        """
        snowflake = res.connectors.load("snowflake")
        sdata = snowflake.execute(QUERY)
        sdata["key"] = sdata["Key"].map(
            lambda x: x.split("_")[0] if x != None else None
        )
        sdata["nest_job_key"] = sdata["Nested Asset Paths"].map(
            lambda x: x.split("/")[-1] if x != None else None
        )

        df = pd.merge(
            df,
            sdata[["key", "nest_job_key", "Nested Asset Paths"]].rename(
                columns={"Nested Asset Paths": "nest_path"}
            ),
            on="key",
        )

    return df


"""
NODE CONTRACT
"""


def _infer_meta_path_and_check_exists(path):
    """
    Look in the location for dxf files and we can use
    """

    s3 = res.connectors.load("s3")
    v = path.split("/")[-4]
    b = path.split("/")[-5]
    f = f"s3://meta-one-assets-prod/bodies/3d_body_files/{b}/{v}/extracted/"
    items = [g for g in list(s3.ls(f)) if ".dxf" in g]
    if len(items):
        return f

    return None


def _get_style_sample_sizes(style_keys):
    from res.flows.dxa.styles.helpers import get_bodies

    def c(b):
        return "-".join(b.split(" ")[0].split("-")[:2])

    # body_keys = [c(s) for s in s]

    mapping = {s: c(s) for s in set(style_keys)}

    s_bodies = list(set(mapping.values()))
    res.utils.logger.info(f"fetch bodies {s_bodies}")
    s_bodies = dict(get_bodies(s_bodies)[["key", "sample_size"]].values)
    mapping = {s: s_bodies.get(b) for s, b in mapping.items()}

    res.utils.logger.info(f"We have the following style sample size mapping {mapping}")
    return mapping


def _augment_style_attributes(assets):
    """
    unfortunately we need to make queries to airtable for extra attributes sometimes
    """
    from res.flows.meta.styles import read as read_styles

    def fix_key(s):
        # remove the key from the res style
        # it turns out the old style reader norms on the non hyphenated one

        if " SELF" in s and "SELF--" not in s:
            s = s.replace(" SELF", " SELF--")

        if s[2] != "-":
            # we need the hyphenated version
            s = f"{s[:2]}-{s[2:]}"

        return s

    keys = [fix_key(a["unit_key"]) for a in assets]

    # we should ahve styles for style requests but this path also used for the body so ignore
    styles = read_styles(keys, allow_non_one_ready=True)

    if len(styles):
        styles = styles.drop_duplicates(subset=["sku"], keep="last").set_index("sku")
    # we can get the body sample data without the style here
    style_sample_sizes = _get_style_sample_sizes(style_keys=keys)

    for a in assets:
        unit_key_lu = a["unit_key"]
        if a["unit_type"] == "RES_STYLE" and unit_key_lu[2] != "-":
            unit_key_lu = f"{unit_key_lu[:2]}-{unit_key_lu[2:]}"

        a["sample_size"] = style_sample_sizes.get(unit_key_lu)
        # for styles that are valid also do this
        if len(styles):
            if a["unit_type"] == "RES_STYLE":
                s = styles.loc[fix_key(a["unit_key"])]
                for c in [
                    "artwork_file_id",
                    "style_cover_image",
                    "style_name",
                    "print_type",
                ]:
                    a[c] = s[c]

    return assets


def _decorate_assets(assets, augment_with_style_attributes=True, style_object=None):
    """
    Gets look up data from a normed asset request
    for example getting the size mapping from accu to res size or looking up material properties fora  property
    we need a standard way to get lookups like this as this is just an attribute join

    We also resolve the dxf path if we need to know the difference between paths that have rule files or not
    """

    if assets is None or len(assets) == 0:
        return assets

    def _extract_version(s, idx=-4):
        v = s.split("/")[idx].lower()

        if "v" in v:
            try:
                return int(float(v.replace("v", "")))
            except:
                pass
        return None

    if augment_with_style_attributes:
        try:
            res.utils.logger.info(f"adding extra styles attributes to the assets")
            assets = _augment_style_attributes(assets)
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to augment with style attrbutes {traceback.format_exc()}"
            )

    # consume of the queue and split the jobs
    airtable = res.connectors.load("airtable")
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    _slu = sizes_lu.to_dataframe(
        fields=["_accountingsku", "Size Normalized", "Size Chart"]
    )
    slu = dict(_slu[["_accountingsku", "Size Normalized"]].values)
    # the sample size was passed in by body as size chart - everything now to norm
    chart_to_norm = dict(_slu[["Size Chart", "Size Normalized"]].values)
    mat_props = (
        airtable.get_airtable_table_for_schema_by_name("make.material_properties")
        .set_index("key")
        .drop(["__timestamp__", "record_id"], 1)
    )

    def get_material_props(k):
        """ """
        try:
            d = dict(mat_props.loc[k])
            return {k: v if not pd.isnull(v) else None for k, v in d.items()}
        except Exception as ex:
            print(ex, "ERROR in marker get material props")
            return None

    def size_mapper(v):
        """
        there should be a lookup but for z sizes not sure
        """
        return slu.get(v, str(v).replace("Z", ""))

    def mod(a):
        a = dict(a)

        # tarting to think about special modes but may not support this
        if (
            a.get("unit_type") == "RES_BODY_COLOR_DEFAULT"
            or "-body" in a.get("flow", "").lower()
        ):
            a["generate_color"] = True

        a["normed_size"] = size_mapper(a["size_code"])
        a["sample_size"] = chart_to_norm.get(a["sample_size"], a["sample_size"])

        keys = a.get("piece_material_mapping", {})
        materials = list({m for m in keys.values() if m is not None})

        # this is an experimental thing we can do if we start taking data about material mappins from an digital assets
        # a["alt_piece_mapping"] = try_make_alt_piece_mappings_from_dxf(a)
        a["material_properties"] = {k: get_material_props(k) for k in materials}
        # default the body version to the placed color astm path but TBD what is the most reliaable
        # teh ASTM file name seems wrong sometimes
        v = a.get("body_version")
        if pd.isnull(v):
            a["body_version"] = _extract_version(a.get("pieces_path"))
        return a

    return [mod(a) for a in assets]


def hash_asset_row(row, m=5):
    """
    creates a hash of some things in the row
    the nest dict is sorted to create a hash that makes sense
    """
    import json, hashlib

    # what is it that determins the hashable piece
    hashable = {
        "MATERIAL": row.get("piece_material_mapping"),
        "COLOR": row.get("piece_color_mapping"),
    }

    h = json.dumps(hashable, sort_keys=True).encode("utf-8")

    return hashlib.shake_256(h).hexdigest(m).upper()


def make_one_spec_key(row):
    """
    <body_code>-V<style_body_version>-<size>-<color>-<alt_piece_hash>
    """

    h = hash_asset_row(row)

    return f"{row['body_code']}-V{int(row['body_version'])}-{row['size_code']}-{row['color_code']}-{h}"


def _run_validations(asset):
    """
    flows:
     3d-flow-non-strict
     3d-flow-extensive-checks
     s3-flow-all-nodes
     s3-flow-all-nodes-extensive-checks
     maybe a metadata update flow ?

    dxf tests:
        - size exists
        - piece names
        - pieces known (warning)
        - pieces compensated width
        - notch grade checks

    piece image tests:
        - missing pixels and image quality is upstream
        - (extended) testing checks piece images and piece outlines are close enough - this could be done as a final meta marker validation too
    cut tests:
        -
    sew tests:
        - res_color.make_overlays? without worrying about the composition (warning)

    finalize test
        - check the path pointed by the pattern exists for each piece -> png

    disable validation:

        post_validate_meta_one=False

    """
    # do the paths exist
    # are the versions for the assets correct/matching based on the paths

    # check the flow and this determines a profile for tests for example an extended testing
    #  e.g. check the images actually match the piece outlines
    return []


def export_meta_one(
    asset,
    fc=None,
    use_meta_one_hash_for_path=True,
    record_id_is_style=False,
    plan=False,
    failed_asset_one_path_out=[],
    **kwargs,
):
    """
    This processes the asset request queue for meta ones and fulfills the full meta node contract
    Sub nodes print, cut and sew amd thencreate the meta marker

    https://github.com/resonance/res-data-platform/pull/923#issuecomment-1018726740

    example asset:

    {
        'res_key': 'KT1010 CT406 ORANHB',_
        'body_version': 4,
        'body_code': 'KT-1010',
        'size_code': "3ZZMD",
        'color_code': 'ORANHB',
        'piece_material_mapping': {
            'default': "CT406",
            'pieces': {'FTPKTBAGR': 'CTW70',  'FTPKTBAGL': 'CTW70'}
        },
        'piece_color_mapping': {
            'default': "ORANHB",
        },
        "dxf_path" : "s3://meta-one-assets-prod/bodies/kt_1010/pattern_files/body_kt_1010_v4_pattern.dxf",
        "pieces_path" : "s3://meta-one-assets-prod/styles/meta-one/kt_1010/v4/oranhb/3zzmd",
        "flow": "3d-production",
        "created_at" : res.utils.dates.utc_now_iso_string()
    }

    """
    from res.media.images.providers import DxfFile
    from res.flows.meta.styles import get_style_cost_table

    s3 = res.connectors.load("s3")

    body_code_lower = asset["body_code"].lower().replace("-", "_")
    body_version_normed = str(asset["body_version"]).lower().replace("v", "")

    # by defaultf
    dxf_path = asset.get("dxf_path")
    res.utils.logger.debug(f"The DXF path is {dxf_path}")

    if asset.get("flow") == "3d":
        dxf_path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code_lower}/v{body_version_normed}/extracted/dxf/"
        # TODO: check if supports DXF by size - if it does, alias the size and load the correct one ...

        dxf_path = DxfFile.size_dxf_if_supported(dxf_path, size=asset["normed_size"])

        if not dxf_path:
            # the newest dxf file on location
            dir_content = [f for f in s3.ls(dxf_path) if ".dxf" in f]
            if len(dir_content):
                dxf_path = dir_content[-1]

        res.utils.logger.debug(
            f"Using body-level dxf {dxf_path} in the flow {asset.get('3d')}"
        )

    # validate pieces TODO as part of the print sub node
    # validate other things - validations are saved to s3 by the flow context
    # validations = fc.apply([dxa_gate, print_gate,cut_gate,sew_gate], 'meta-one-gates', key=asset['id'])
    res.utils.logger.info(f"Running validations")

    validations = _run_validations(asset)

    # persiste the validation node with critical and warnings levels and the details about codes/categories and subscriers?
    dxf = DxfFile(
        dxf_path,
        # for some flows we may want to do this; source of complexity
        should_unfold_geometries=kwargs.get("should_unfold_geometries", False),
    )
    v = dxf.validate()
    if v:
        if body_code_lower in ["kt_2054", "kt_2052"]:
            res.utils.logger.warn(
                "TODO - remove hack, allowing validation errors on mixed body meta ones until we can model it properly"
            )
            v = [vi for vi in v if vi not in ["BAD_BODY_NAME_COMPONENT"]]
            if v:
                # may qualify all of these with BODY_
                validations += v
                if fc:
                    fc.validations = validations

                raise FlowValidationException(
                    f"The DXF file has validation errors: {v}, {dxf.validation_details}"
                )
    # if there are validation error that are critical fail the task
    # save the validations if there are any at all, critical or non critical

    res.utils.logger.info(f"Loading meta file, exporting the meta marker")
    one_spec_key = make_one_spec_key(asset)
    one_spec_version = hash_asset_row(asset)
    gerber_size = dxf.map_res_size_to_gerber_size(str(asset["normed_size"]))
    # contract to this point: piece names are correct in the expected location, version matches on assets,

    pieces_path_suffix = asset.get("pieces_path")

    res.utils.logger.info(f"Using pieces path in dfx fn {pieces_path_suffix}")

    # force off for now
    asset["generate_color"] = False
    if asset.get("generate_color"):
        # look up spec from color code in our database which may be an art work or something
        color = "WHITE"
        asset["color_code"] = color
        #####################################################
        root = f"s3://res-data-platform/samples/meta-one/auto-color"

        path = f"{asset['body_code']}/v{asset['body_version']}/{color}/{asset['size_code']}".lower().replace(
            "-", "_"
        )
        path = f"{root}/{path}/pieces"
        # the pieces path is the one we are given otherwuse we infer it -> its named pieces but lets do this just for back compath
        asset["pieces_path"] = asset.get("pieces_path", path)
        # we override where the meta marker looks for the pieces
        pieces_path_suffix = path
        res.utils.logger.info(
            "Creating the auto path for sample data for this marker. It will take a moment to generate the pieces in the sample color"
        )
        res.utils.logger.info(f"Path = {path}")
        dxf = DxfFile(dxf_path)

        # we generate an image sample - if we have material properties we will use it to buffer but not compensate
        dxf.save_piece_samples(
            path,
            size=gerber_size,
            color=color,
            material_properties=asset.get("material_properties"),
            force=kwargs.get("force_generate_color", True),
        )
        res.utils.logger.info("Created pieces")
    #### end generator

    # may change the path format to meta-one/<flow>/
    # flow = asset.get('flow', '3D_STYLE') | 2D_STYLE | or 2D_BODY or 3D_STYLE or TEST
    flow = asset.get("flow", "3d")

    meta_marker_root = (
        f"{META_ONE_ROOT}/{flow}".lower()
    )  # <- pass this into the dxf function
    metadata = {
        # if we know what flow/type we can distinguish status of meta markers we want to use
        "flow_unit_type": asset.get("unit_type"),
        "flow": flow,
        "meta_path": dxf_path,
        "requested_meta_path": asset.get("dxf_path"),
        "path_bc": asset["pieces_path"],
        "res_key": asset["unit_key"],
        "meta_sample_size": dxf.sample_size,
        # some of these are deocrated from the style and thus added to the asset that way not in kafka
        "artwork_file_id": asset.get("artwork_file_id"),
        "print_type": asset.get("print_type"),
        "style_cover_image": asset.get("style_cover_image"),
    }
    marker_path = DxfFile.export_meta_marker(
        dxf,
        sizes={gerber_size: asset["size_code"]},
        res_size_map={asset["size_code"]: asset["normed_size"]},
        # test color mappings
        color_code=asset["color_code"],
        piece_material_map=asset["piece_material_mapping"],
        piece_color_map=asset["piece_color_mapping"],
        key=one_spec_key,
        # we can optionaly use versionign mode
        meta_one_version=one_spec_version if use_meta_one_hash_for_path else None,
        material_props=asset["material_properties"],
        # NB where we store them
        meta_marker_root=meta_marker_root,
        # todo add material props
        # todo try fetch sew instructions
        # this is a test purposes overrides
        pieces_path_suffix=pieces_path_suffix,
        metadata=metadata,
    )

    asset["meta_one_path"] = marker_path

    failed_asset_one_path_out.append(marker_path)

    res.utils.logger.info("exporting nesting statistics for marker")
    marker = MetaMarker(marker_path)

    if kwargs.get("post_validate_meta_one", True):
        meta_val = marker.validate(
            validate_dxf=False, validate_color=asset.get("unit_type") == "RES_STYLE"
        )
        if meta_val:
            validations += meta_val
            if fc:
                fc.validations = validations
            vpath = f"{marker_path}/validation_details.json"
            s3.write(vpath, marker.validation_details)
            raise FlowValidationException(
                f"the meta one contains validation errors {meta_val} - dump in meta one path {vpath}"
            )

    # this is also a good gate on reloading the meta marker so leave it
    nstat = marker.get_nesting_statistics()
    s3.write(f"{marker_path}/nesting_stats.feather", nstat)

    # this is also a good gate on reloading the meta marker so leave it
    if flow == "meta":
        # force this mode
        record_id_is_style
    # TODO find a costs solution
    if record_id_is_style:
        rec_id = asset.get("id")
        res.utils.logger.info(f"try lookoup cost information for style {rec_id}")
        try:
            costs = get_style_cost_table(rec_id, asset["size_code"])
            if pd.notnull(rec_id) and costs is not None:
                res.utils.logger.info("exporting style costs for marker")
                s3.write(f"{marker_path}/cost_table.feather", costs)
            else:
                res.utils.logger.warn(
                    f"unable to export style costs for marker where asset key is {asset['unit_key']}"
                )
        except Exception as ex:
            res.utils.logger.warn(f"Failed to lookup style costs - {repr(ex)}")

    if fc:
        fc.validations = validations

    marker.cache_summary()

    # experimental
    # if asset.get("unit_type") == "RES_BODY_COLOR_DEFAULT":
    # lets just collect data... any meta one with the right body can upsert this. they should conform to body version in any flow for any style
    res.utils.logger.info(f"Writing body material size stats for the body ")
    # marker._bm_stats_to_warehouse()
    # this is redundant now - lets find a better way
    # kafka or direct db

    res.utils.logger.info("Style exported.")

    return asset


def generate_meta_meta_one_from_style(style, all_styles=None, queue_request=False):
    """

    example input style format : KT-4004 CT406 BEACDF

    This is a very meta object off flow
    the purpose of this is to create base line data when we can with as much data as we can but not necessarily having everything
    the meta one is a gate so a true meta one is strict but we also want to inspect best effort data

    {
        'unit_key': 'KT-4004 CT406 BEACDF',
        'body_version': 4,
        'body_code': 'KT-1010',
        'size_code': "3ZZMD",
        'color_code': 'ORANHB',
        'piece_material_mapping': {
            'default': "CT406",
            'pieces': {'FTPKTBAGR': 'CTW70',  'FTPKTBAGL': 'CTW70'}
        },
        'piece_color_mapping': {
            'default': "ORANHB",
        },
        "dxf_path" : "s3://meta-one-assets-prod/bodies/kt_1010/pattern_files/body_kt_1010_v4_pattern.dxf",
        "pieces_path" : "s3://meta-one-assets-prod/styles/meta-one/kt_1010/v4/oranhb/3zzmd",
        "flow": "3d-production",
        "created_at" : res.utils.dates.utc_now_iso_string()
    }


    we generate a very meta one without generating color or anything like that
    we just create the meta objects and store stats in the database -> see snowflake logs

    TODO:
     - think properly about combos and conflicts between these and the ones that are less meta already storing stats
     - should_unfold_geometries should maybe true for legacy but not necessarily!!!! This needs to be tested somehow


    some things we cannot do maybe
    - scan.json - anything smart with seam allowances including notch correction
    - grading from .rul files when they do no not exist
    - use/check color pieces unless we generate them from color
    - understand combos unless we trust the DXF (which we often can) :> but how to know the secondary material must be checked
    -


    Batch exec: either queue or dont queue

    #choose kafka server:

    from tqdm import tqdm
    styles = list(M['style_code'].unique())
    fails = {}
    for s in tqdm(styles):
        try:
            generate_meta_meta_one_from_style(s, all_styles=all_styles, queue_request=True) #no style
        except Exception as ex:
            fails[s] = repr(ex)
    """
    from res.connectors.s3.datalake import list_astms
    from res.flows.meta.marker import _decorate_assets, export_meta_one
    from res.flows.meta.styles import read as read_styles

    res.utils.logger.info(
        f"Generating/handling requests for style <{style}> on the meta flow..."
    )
    s3 = res.connectors.load("s3")
    kafka = res.connectors.load("kafka")
    TOPIC = "res_meta.meta_one.requests"

    body_code = style.split(" ")[0].rstrip().lstrip()
    body = style.split(" ")[0].rstrip().lstrip().lower().replace("-", "_")

    assert (
        "_" in body
    ), f"the body code should be formatted with a `-` or `_` but you have {body}"

    color = style.split(" ")[2].rstrip().lstrip()
    material = style.split(" ")[1].rstrip().lstrip()
    dxf = None
    path = None
    version = None
    flow = None

    try:
        if all_styles is not None:
            s = dict(
                all_styles[all_styles["res_key"] == style.replace("-", "")].iloc[0]
            )
        else:
            s = dict(read_styles(style).iloc[0])
    except:
        print("FAILED TO LOAD THE STYLE")
        raise

    size_map = s["size_map"]

    res.utils.logger.info(f"style loaded with size map {size_map}")

    # try asssumed 2d but fall back to 3d dxf files for some new cases
    for flow in ["2d", "3d"]:
        try:
            d = list_astms(body, flow=flow).iloc[0]
            path = d["path"]
            version = d["meta_version"]
            should_unfold_geometries = True
            if flow == "3d":
                should_unfold_geometries = False
            dxf = s3.read(path, should_unfold_geometries=should_unfold_geometries)

            # if we have it move on - dont need to try 3d
            break
        except Exception as ex:
            res.utils.logger.warn(
                f"could not resolve a dxf for the style/body in flow {flow}"
            )

    res.utils.logger.info(f"dxf: {dxf}")

    for size_code, _ in size_map.items():
        res.utils.logger.debug(f"generating payload for size {size_code}")

        # this shows the meta one raw payload structure
        asset = {
            "id": s["record_id"],  # is style yes
            "unit_key": style.replace("-", ""),
            "body_version": version,
            "body_code": body_code,
            "size_code": size_code,
            "color_code": color,
            "piece_color_mapping": {
                "default": color,
            },
            "piece_material_mapping": {
                "default": material,
            },
            "dxf_path": path,
            "pieces_path": "",
            "flow": "meta",
            "created_at": res.utils.dates.utc_now_iso_string(),
        }

        if queue_request:
            kafka[TOPIC].publish(asset, use_kgateway=True)

        else:
            asset = _decorate_assets([asset])[0]
            # record is style id
            export_meta_one(asset, record_id_is_style=True)


REQUEST_TOPIC = "res_meta.meta_one.requests"
RESPONSE_TOPIC = "res_meta.meta_one.responses"


def path_without_size(p):
    """
    util get the meta one path without the size part
    """
    if p:
        p = p.rstrip("/").split("/")[:-1]
        return "/".join(p)
    return ""


def generator(event, context=None):
    """
    below we use the queueu handler one by one (with enough memory)
    but we can also run the generator here to consume off the topics (for testing consumption) OR for direct
    Probably locally direct is better
    When we run the handler, we can publish to the response topic
    We can also just decorate and call handler directly which also does the publish if configured
    """
    # consume off the topic and map over the results possibly using an s3 cache on the node

    with FlowContext(event, context) as fc:
        # consume from the kafka topic for this guy using the right decorator
        # assets = fc.assets
        if len(fc.assets):
            res.utils.logger.info(
                f"Assets supplied by payload - using instead of kafka"
            )
            assets = fc.assets_dataframe
        else:
            res.utils.logger.info(f"Consuming from topic {REQUEST_TOPIC}")
            kafka = fc.connectors["kafka"]
            assets = kafka[REQUEST_TOPIC].consume(give_up_after_records=100)

            res.utils.logger.info(
                f"Consumed batch size {len(assets)} from topic {REQUEST_TOPIC}"
            )

        # vpype read /Users/sirsh/Downloads/test_setup_scaled.svg write --page-size 150mmx150mm   output.hpgl
        if len(assets):
            # this should be "univeral" over request types but be careful with source paths e.g. for version extraction
            assets = _decorate_assets(assets.to_dict("records"))
            # print(assets[0])
            for f in ["style_cover_image"]:
                # remove this because it makes the payload
                for a in assets:
                    if f in a:
                        a.pop(f)
            # we turn the asset into a wrap payload for the flow context
            # that is we add the header information
            assets = fc.asset_list_to_payload(assets, compress=True)

        res.utils.logger.info(
            f"returning the work of type {type(assets)} from the generator for session key {fc.key}"
        )

        # there is a way to do this so that we save this in S3 so it can be mapped
        return assets.to_dict("records") if isinstance(assets, pd.DataFrame) else assets


def _as_asset_status_payload(a):
    d = dict(a)
    d["key"] = a.get("unit_key")
    d["value"] = f'{a.get("unit_key")} {a.get("size_code")}'

    return d


def queue_notify(asset, status, node, fc, exception=None):
    """
    send a statsus on the meta one that can be aggregated later ...
    - body creation
    """
    payload = dict(asset)
    status = status.upper()
    try:
        if asset.get("notifications") == "disabled":
            res.utils.logger.info("skipping notification")
            return
        s3 = res.connectors.load("s3")
        meta_one_path = asset.get("meta_one_path")
        validation_path = (
            f"{meta_one_path.rstrip('/')}/validation_details.json"
            if meta_one_path
            else None
        )

        val_map = (
            s3.read(validation_path)
            if validation_path and s3.exists(validation_path)
            else {}
        )
        if exception is not None:
            val_map["exception"] = str(exception)
        val_map = {k: str(v) for k, v in val_map.items()}

        payload["validation_error_map"] = val_map
        payload["status"] = status
        payload["node"] = node
        payload["tags"] = fc.validations
        payload["pod_name"] = fc.pod_name  # deprecate
        payload["created_at"] = res.utils.dates.utc_now_iso_string(None)
        payload["task_key"] = fc.key
        payload["log_path"] = fc.log_path

        res.connectors.load("kafka")["res_meta.meta_one.status_updates"].publish(
            payload, use_kgateway=USE_KGATEWAY, coerce=True
        )
    except Exception as ex:
        res.utils.logger.warn(
            f"Did not notify on the queue status for {payload} - {traceback.format_exc}"
        )


def try_save_meta_one_to_database(style_name, uri, created_at):
    try:
        from res.flows.dxa.styles.helpers import _save_meta_one_as_style

        # going to try three times to save to the database or die
        _save_meta_one_as_style(style_name, uri, created_at)
    except Exception as ex:
        res.utils.logger.warn(f"Failed to save {ex}")
        raise Exception("Failed to save to database")


def try_generate_meta_one_preview(a, fc=None):
    from res.flows.meta.ONE.meta_one import MetaOne
    from res.flows.meta.ONE.annotation import post_previews

    if a["unit_type"] != "RES_STYLE":
        return {}
    # assuming this is only called for the sample size
    try:
        sku = f"{a['unit_key']} {a['size_code']}"
        try:
            res.utils.logger.info(f"Loading sku {sku}")
            post_previews(MetaOne(sku), request_id=a["id"])
            queue_notify(a, "OK", node="meta-one-previews", fc=fc)
        except Exception as ex:
            # no forcing the preview to be required in update from the saved meta one
            raise ex

    except Exception as ex:
        queue_notify(a, "FAILED", node="meta-one-previews", fc=fc)
        res.utils.logger.warn(f"Failed to generate preview {ex}")


def try_save_complementary_assets(asset, fc=None):
    try:
        from res.flows.meta.ONE.meta_one import BodyMetaOne

        bm = BodyMetaOne(
            asset["body_code"], asset["body_version"], size_code=asset["size_code"]
        )
        bm.save_complementary_pieces()
    except Exception as ex:
        res.utils.logger.warn(f"Failing to save comp pieces for {asset} - {ex}")


def try_post_body_status(asset, fc=None):
    body_code = asset.get("body_code")
    body_version = asset.get("body_version")
    try:
        from res.flows.meta.ONE.meta_one import BodyMetaOne
        from res.flows.meta.ONE.annotation import post_body_previews

        res.utils.logger.info(
            f"<<<<<<<<<Doing the body preview thing for {body_code} V{body_version}>>>>>>>>>>>"
        )
        m1 = None

        try:
            m1 = BodyMetaOne(
                asset["body_code"], asset["body_version"], size_code=asset["size_code"]
            )
        except:
            res.utils.logger.warn(f"Failing on asset for body preview {asset}")
        # we can post a void preview to show that we attempted it
        post_body_previews(
            m1,
            body_code=body_code,
            body_version=body_version,
            # we pass tags from the upstream request e.g. POM failures
            failed_contracts=asset.get("tags"),
        )
        queue_notify(asset, "OK", node="body-one-ready-status", fc=fc)
    except Exception as ex:
        res.utils.logger.warn(f"Failing on asset for body preview {asset}")
        res.utils.logger.info(traceback.format_exc())
        queue_notify(asset, "FAILED", node="body-one-ready-status", fc=fc, exception=ex)


@res.flows.flow_node_attributes(
    memory="24Gi",
)
def handler(event, context=None):
    """
    pstoedit -f plot-hpgl /Users/sirsh/Downloads/test_setup_scaled.svg

    $ vpype read /Users/sirsh/Downloads/test_setup_scaled.svg write --device hp7475a --page-size 1024x768 --landscape output.hpgl

    For this, something e.g. the generator has ensured the contract via lookups from the norm'd kafka queue
    Now we can run the node (does its own child validations) and publish the response
    There are has many nodes as units can spend physical

    Needs as much memory as the largest single piece

    we store requests in a set for RES_STYLE which is default as bundle
    confusingly, this links as most jobs should to a request id
    howerver we have another mode where we are not processing requests but styles at body level
    this will beed to be refactored but when RES_BODY_COLOR_DEFAULT is the type
    - a. we do not callback to an airtable queue
    - b. we use the create one figure for costs
    this is done to avoid developing too many processes until we work out the flow
    the reason for adding the body one is because we can create a pseudo style ina  white to validate the body
    this style is also useul to create jobs to check bodies against all possible materials in the white
    when we are orgniazed though it will be clearner to just create a defailt meta one on this single flow
    """

    ######################################
    # legacy support but we can still fail validation maybe
    import os

    # too much logging when trying to debug
    from warnings import filterwarnings

    filterwarnings("ignore")

    # sadly we need icons this way for previews - this
    from res.media import images

    images.text.ensure_s3_fonts()
    images.icons.extract_icons()

    ENV = "DXF_NO_PIECE_SUFFIX_ASSUMES_SELF"
    os.environ[ENV] = "True"
    ######################################

    def type_is_style_id(t):
        if t in ["RES_BODY_COLOR_DEFAULT"]:
            return True
        return False

    a = {}
    with FlowContext(event, context) as fc:
        # may want to load connector with a context for ttl default but can also added ttl to ops
        redis = fc.connectors["redis"]
        kafka = fc.connectors["kafka"]
        size_cache = redis["meta"]["meta_one_processed_sizes"]
        validation_cache = redis["meta"]["meta_one_validation_errors"]
        request_cache = redis["meta"]["meta_one_requests"]
        # request type lookup from record_id -> type e.g. type could be a body or apply color request
        request_types = redis["meta"]["requests_types"]
        meta_one_path_cache = redis["meta"]["meta_one_uris"]

        for a in fc.decompressed_assets:
            request_type = a.get("unit_type")
            asset_key = a.get("id")

            failed_asset_one_path_out = []
            try:
                # we pass this in because of the failure flow that raises an exception after we create a meta one and the validate it
                # this should not really happen buts its useful to get a status check on something that is almost correct

                a = export_meta_one(
                    a,
                    fc,
                    record_id_is_style=type_is_style_id(request_type),
                    failed_asset_one_path_out=failed_asset_one_path_out,
                )
                status = "IN_REVIEW" if not fc.validations else "VALIDATION_ERRORS"
                # dstatus is ready for approval today and in future maybe direct approval
                queue_notify(a, status, node="dxa", fc=fc)
                # meta one ready for approval queue
                if request_type == "RES_BODY":
                    res.utils.logger.info(
                        f'Body specifics checking for sample size==norm size {a["meta_one_path"], a["body_code"]}, {a.get("sample_size")}=={a.get("normed_size")}'
                    )
                    try:
                        # assume the best for now because we need to try and explicitly fail
                        # disable queue in place of contracts location
                        # cut_status = a.get("metadata", {}).get("cut_status", True)
                        # queue_notify(
                        #     a, "FAILED" if not cut_status else "OK", node="cut", fc=fc
                        # )

                        # if my size is the sample size we can also decide to do the previews

                        # this is part of the newer flow where we are purely meta one based. here we save all the fuse and stamper pieces the new way
                        # if we re-run the bodies we can regenerate this - the body re-run is faily cheep to to requeue enmasse as far as i know except for maybe the previews on the sample size
                        try_save_complementary_assets(a, fc=fc)

                        if a.get("sample_size") in [a.get("normed_size"), "YES"]:
                            try_post_body_status(a, fc=fc)
                        #     MetaMarker.sync_sew_for_body(
                        #         a["meta_one_path"], a["body_code"]
                        #     )
                        #     queue_notify(a, "OK", node="sew", fc=fc)
                        # else:
                        #     res.utils.logger.debug(f"SKIP sew mapping off sample size")
                    except Exception as ex:
                        res.utils.logger.warn(f"FAILED SEW: {res.utils.ex_repr(ex)}")
                        # not notifing for sew anymore
                        # queue_notify(a, "FAILED", node="sew", fc=fc, exception=ex)
                        # raise
                else:
                    res.utils.logger.info(f"trying {a}")
                    try_save_meta_one_to_database(
                        a.get("style_name"), a["meta_one_path"], a["created_at"]
                    )
                    if a.get("sample_size") == a.get("normed_size"):
                        try_generate_meta_one_preview(a, fc=fc)
            except FlowValidationException as fex:
                # this is important becasue if we raise this but do not specify, we could skip and add sizes
                # although we only add size if we have a meta one uri now
                if len(fc.validations) == 0:
                    fc.validations.append("FLOW_VALIDATION_ERRORS")
                res.utils.logger.warn(
                    f"Caught flow validation errors {fc.validations} - will report back and not create the meta one {fex}"
                )

                meta_one_path_cache[asset_key] = failed_asset_one_path_out[0]

                queue_notify(a, "FAILED_VALIDATION", node="dxa", fc=fc, exception=fex)

            except Exception as ex:
                fc.validations.append("EXCEPTION")
                message_err = f"Fatal error - uncaught exception in creating meta one for {asset_key}/ {a.get('unit_key')}: {repr(ex)}. Current validation errors are {fc.validations}"
                res.utils.logger.warn(message_err)

                fc.log_asset_exception(a, ex, datagroup="body_code")

                fc.publish_asset_status(
                    _as_asset_status_payload(a),
                    "meta-one",
                    message_err,
                    status="FAILED",
                )
                # send notifications to correct DxA queue - system error
                queue_notify(a, "FAILED", node="dxa", fc=fc, exception=ex)

            try:
                # this is important both to avoid sending queue responses to the wrong places but also important to make sure we do send feedbacks to the right places
                if request_type == "RES_STYLE":
                    request_types[asset_key] = request_type
                    request_cache.add_to_set(fc.key, asset_key)
                    validation_cache.add_to_set(asset_key, fc.validations)
                    if not fc.validations and a.get("meta_one_path"):
                        size_cache.add_to_set(asset_key, a["size_code"])
                    # else:
                    #     res.utils.logger.debug(
                    #         f"Removing size {a['size_code']} from cache becasue it has validation errors"
                    #     )
                    #     size_cache.remove_from_set_if_exists(asset_key, a["size_code"])

                res.utils.logger.debug(
                    f"The following validation tags were seen: {fc.validations} for record id {a['id']} - will post response to kafka"
                )

                if not fc.validations:
                    # Generate Meta ONE Status we could set this to Done but might wai
                    kafka[RESPONSE_TOPIC].publish(a, use_kgateway=True)
                else:
                    fc.metric_incr(
                        verb="EXITED",
                        group=a.get("body_code"),
                        status="VALIDATION_ERRORS",
                    )
                    # post asset failures
                    fc.publish_asset_status(
                        _as_asset_status_payload(a),
                        "meta-one",
                        "validator errors on asset - see logs",
                        status="FAILED",
                    )
            except Exception as ex:
                res.utils.logger.error(
                    f"Failed in comms when handling meta one creation - data may be in an inconsisitent state for session {fc.key}: {res.utils.ex_repr(ex)} "
                )

    return a


def reducer(event, context=None):
    """
    This reducer is use to update airtable or other queues with status
    """

    with FlowContext(event, context) as fc:
        redis = fc.connectors["redis"]
        request_cache = redis["meta"]["meta_one_requests"]
        request_types = redis["meta"]["requests_types"]
        validation_cache = redis["meta"]["meta_one_validation_errors"]
        meta_one_path_cache = redis["meta"]["meta_one_uris"]
        size_cache = redis["meta"]["meta_one_processed_sizes"]

        airtable = fc.connectors["airtable"]
        s3 = res.connectors.load("s3")
        # this is for the create color queue but actually for other requests we can also do airtable callback
        asset_requests = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]

        keys = request_cache[fc.key] or []

        res.utils.logger.info(
            f"**********Found keys: {keys} for {fc.key} in meta/meta_one_requests********"
        )

        for asset_key in keys:
            validation_errors = validation_cache[asset_key]
            sizes_ready = size_cache[asset_key]
            request_type = request_types[asset_key]
            meta_one_path = meta_one_path_cache[asset_key]
            res.utils.logger.info(
                f"Publishing status for asset {asset_key}({request_type}) ({meta_one_path}); Sizes {sizes_ready} with validation errors {validation_errors}"
            )

            validation_path = (
                f"{meta_one_path.rstrip('/')}/validation_details.json"
                if meta_one_path and len(validation_errors) > 0
                else None
            )
            payload = {
                "record_id": asset_key,
                # failure
                "Meta.ONE Flags": validation_errors or None,
                # success
                "Meta.ONE Sizes Ready": sizes_ready or None,
                "Meta.ONE Last Update": res.utils.dates.utc_now_iso_string(),
                "Meta.ONE Uri": path_without_size(meta_one_path),
                "Meta.ONE Validation Details": json.dumps(s3.read(validation_path))
                if validation_path and validation_errors and s3.exists(validation_path)
                else None,
            }

            # a common interfface would be nice but we should be careful about adding fields to airtable
            if request_type not in ["RES_STYLE"]:
                payload.pop("Meta.ONE Sizes Ready")

            try:
                # update airtable - will move this to the reducer when tested more
                asset_requests.update_record(
                    payload,
                    use_kafka_to_update=fc.args.get(
                        "use_kafka_to_update_airtable", False
                    ),
                    RES_APP="metaone",
                    RES_NAMESPACE="meta",
                )

            except Exception as ex:
                # necessary resilience we need to update for different modes
                res.utils.logger.info(
                    f"Failed to update with {payload} - {traceback.format_exc(ex)}"
                )

    return {}


def queue_handler(event, context=None):
    """
    This mode just runs in an app and consumes from the queue
    """
    from res.flows.FlowContext import FlowValidationException

    def consume_topic(t):
        return None

    def publish_topic(t, data):
        pass

    for asset in consume_topic("res_meta.meta_one.requests"):
        assets = _decorate_assets([assets])[0]
        try:
            asset = export_meta_one(asset)
        except FlowValidationException as vex:
            # do something about the error
            asset["status"] = "ISSUE"
            tags = vex.tags_string
            asset["tags"] = tags or "EXCEPTION"

        publish_topic("res_meta.meta_one.responses", asset)


"""
Request types experiemntal
"""


def get_request_for_white_style_with_meta_body(s, generate_color=True):
    """
    This is used to create a request for body level
    we use inouts that the read styles provides but can also be constructed quite easily from attributes with a material lookup added on
    the idea is this can be made from any valid DXF - it doesnt matter if its 2d or 3d as such assuming the DXF file is the latest
    we use the styles to seed this because we get all the codes from there for BMs -> compress all Cs to one White and take a replica

    """

    unit_key = f"{s['body_code'].replace('-','')} {s['primary_material']} WHITE"

    sizes = list(s["size_map"].keys())

    def mapping_with_default_material(s):
        d = s.get("alt_piece_mapping", {})
        d.update({"default": s.get("primary_material")})
        return d

    payloads = [
        {
            # this reference id can be used to get the create one cost
            "id": s["record_id"],
            "unit_key": unit_key,
            "body_code": s["body_code"],
            "body_version": int(s["body_version"]),
            "color_code": "WHITE",  # s["color_code"].upper(),
            "size_code": size,
            # thi is added
            "normed_size": s["size_map"].get(size),
            "dxf_path": s["dxf_path"],
            # optional only if we are generating color
            "pieces_path": None,
            # todo - pre validations can check these defaults exist or blows up
            "piece_material_mapping": {"default": s["primary_material"]},
            "piece_color_mapping": {"default": "WHITE"},  # s["color_code"].upper()
            "material_properties": s["material_properties"],
            "status": "ENTERED",
            "tags": [],
            "flow": "2D",
            "generate_color": generate_color,
            # record is style
            "unit_type": "RES_BODY_COLOR_DEFAULT",
            "created_at": res.utils.dates.utc_now_iso_string(),
        }
        for size in sizes
    ]

    return payloads


"""
Marker viz
"""


def get_order_html(marker, exit_factory_date, make_one_request_type="finished goods"):
    one_number = marker.one_number

    image_table = marker.get_image_table_html()

    brand_name = marker.get_brand_name()

    size_breakdown_table = marker.size

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
                    <td> <div align='center'> <font size='5'>{marker.size}</font></div>
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
                            {marker.style_code} in sizes {marker.size}</font>
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


def requeue_missing(event, context={}, load_fonts=True):
    """
    Look at what we have not processed and requeue
    meta ones are created by kafka events from some client
    if we drop messages we may enter a bad state.
    for now we should periodically flush
    """
    from res.media import images

    if load_fonts:
        images.text.ensure_s3_fonts()
        images.icons.extract_icons()

    airtable = res.connectors.load("airtable")
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    _slu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Normalized"])
    slu = dict(_slu[["_accountingsku", "Size Normalized"]].values)

    def size_mapper(v):
        return slu.get(v, v.replace("Z", ""))

    def get_sizes_map_for(v):
        return {d: size_mapper(d) for d in v if d not in ["0PTXX"]}

    q = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    q = q.to_dataframe()
    samples = q[
        [
            "_record_id",
            "Style Code",
            "Apply Color Flow Status",
            "Meta.ONE Sizes Ready",
            "Meta ONE Sizes Required",
            "Were All Meta ONEs created",
            "Body Code",
            "Style ID",
        ]
    ]
    samples = samples[samples["Were All Meta ONEs created"] == 0]
    samples = samples[
        samples["Apply Color Flow Status"].isin(["DxA Exit", "Generate Meta ONE"])
    ]
    samples = samples[samples["Meta ONE Sizes Required"].notnull()]

    kafka = res.connectors.load("kafka")

    for row in samples.to_dict("records"):
        try:
            print("style", row["Style Code"])
            req_id = row["_record_id"]

            style = row["Style Code"]
            style_id = row["Style ID"]
            req = list(
                set(row["Meta ONE Sizes Required"])
                - set(
                    row["Meta.ONE Sizes Ready"]
                    if isinstance(row["Meta.ONE Sizes Ready"], list)
                    else []
                )
            )

            sm = get_sizes_map_for(req)

            try:
                reqs = get_request_for_style_size_bundle(
                    style, req_id, sizes_in=sm, style_id=style_id
                )
            except:
                print("WE COULD NOT GET THE STYLE... skipping")
                continue
            data = []
            for b in reqs:
                a = dict(b)
                a.pop("material_properties")
                data.append(a)

            print("publishing")
            # needs port to schema registry
            kafka["res_meta.meta_one.requests"].publish(data, use_kgateway=True)
        except Exception as ex:
            raise
            print(ex)
            print("failed for that reason")


def generate_preview_for_apply_color_request(
    sku, req_id, body_version=None, use_meta_one=None
):
    """
    https://airtable.com/appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwspdN57kMmHpasc?blocks=hide
    supply a request id and a sku e.g
     sku, rid = 'TK-6149 SLCTN DRIELM 2ZZSM','recZ75c2fTCloFh7d'

     some redundacy here compare to handle previews below
    """
    m = use_meta_one or MetaMarker.from_sku(
        sku, body_version=body_version, valid_only=False
    )
    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")
    tab = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
    # catch a ResImageLayerException when we cannot generate a labelled Piece Image
    paths = []
    for p in m:
        sample_path = f"s3://res-data-platform/samples/apply-color-requests-previews/{req_id.lower()}/{p['key'].lower()}.png"
        res.utils.logger.info(
            f"**************** {sample_path} ****************",
        )
        img = p.labelled_piece_image
        img.thumbnail((2048, 2048))
        s3.write(sample_path, img)
        paths.append(sample_path)

    res.utils.logger.info(f"*****Updating previews for paths {paths}")

    tab.update(
        {
            "record_id": req_id,
            "Meta.ONE Piece Preview": [
                {"url": s3.generate_presigned_url(pt)} for pt in paths
            ],
            "Meta.ONE Piece Preview Updated": res.utils.dates.utc_now_iso_string(),
        }
    )

    # queue notify in here


@res.flows.flow_node_attributes(
    memory="120Gi",
)
def handle_previews(event, context={}, plan=False):
    """
    preview gen
    previews should not fail if we are gating meta-ones properly
    """

    from res.media import images
    from res.flows.meta.ONE.meta_one import MetaOne
    from res.flows.meta.ONE.annotation import post_previews

    images.text.ensure_s3_fonts()
    images.icons.extract_icons()

    bodies = event.get("args", {}).get("bodies", None)

    def preview_apply_color_record(row, skip_exists=False, fc=None):
        airtable = res.connectors.load("airtable")
        sample_size = row["Meta.ONE Sizes Ready"][-1]
        style = row["Style Code"]
        sku = f"{style} {sample_size}"
        rid = row["record_id"]
        res.utils.logger.info(sku)

        m1 = MetaOne(sku)
        r = post_previews(m1, rid, skip_exists=skip_exists)

    if os.environ.get("RES_ENV") != "production":
        res.utils.logger.info("We are only running this task in production for now")
        return

    with FlowContext(event, context) as fc:
        airtable = res.connectors.load("airtable")
        res.utils.logger.info(f"Loading state....")
        q = airtable["appqtN4USHTmyC6Dv"]["tblWMyfKohTHxDj3w"]
        last_modified_field = "Last Updated At"
        filters = f'IS_AFTER({{{last_modified_field}}}, DATETIME_PARSE("{res.utils.dates.utc_days_ago(2).isoformat()}"))'
        q = q.to_dataframe(filters=filters)
        # do the custom first since we dont really look at the other previews
        q = q.sort_values(
            ["Color Type", "Meta.ONE Last Update"], ascending=[True, False]
        )
        samples = q[
            [
                "_record_id",
                "Style Code",
                "Apply Color Flow Status",
                "Meta.ONE Sizes Ready",
                "Meta ONE Sizes Required",
                "Were All Meta ONEs created",
                "Body Code",
                "Style Version",
                "Flag for Review",
                "Meta.ONE Piece Preview Updated",
                "Meta.ONE Last Update",
                "Meta.ONE Piece Preview",
            ]
        ]
        samples = samples[samples["Were All Meta ONEs created"] == 1]
        if bodies:
            if not isinstance(bodies, list):
                bodies = [bodies]
            res.utils.logger.info(f"Filtering by bodies: {bodies}")
            samples = samples[samples["Body Code"].isin(bodies)]
        # samples = samples[samples['Body Code']=='KT-2011']
        samples = samples[samples["Apply Color Flow Status"].isin(["DxA Exit"])]
        samples = samples[samples["Flag for Review"].isnull()]

        samples = samples[samples["Meta ONE Sizes Required"].notnull()]
        samples = samples[
            (samples["Meta.ONE Piece Preview Updated"].isnull())
            | (samples["Meta.ONE Piece Preview"].isnull())
        ]

        samples["record_id"] = samples["_record_id"]

        if plan:
            return samples

        res.utils.logger.info(f"Loaded {len(samples)} samples")
        failures = {}
        for row in samples.to_dict("records"):
            try:
                # it makes sense to skip exists true because we have filtered by the attachment not being there which means it probably was not posted but could exist
                # still dangeous though that we might not rengenerate if something is newer
                preview_apply_color_record(row, skip_exists=True, fc=fc)
            except Exception as ex:
                res.utils.logger.error(
                    f"FAILED ON ROW {row.get('record_id')} : {traceback.format_exc()} "
                )
                failures[row.get("record_id")] = repr(ex)
            # raise ex

        res.utils.logger.info(f"Done")
        return failures


def _inspect_scan_jsons():
    """
    scrape utility - check timestamps
    """
    s3 = res.connectors.load("s3")
    metadata = list(s3.ls_info("s3://meta-one-assets-prod/styles/meta-one/3d/"))
    data = pd.DataFrame(metadata)
    data["body"] = data["path"].map(lambda f: f.split("/")[6])
    data["version"] = data["path"].map(lambda f: f.split("/")[7].split("_")[0])
    data["color"] = data["path"].map(lambda f: f.split("/")[8].split("_")[0])
    data["filename"] = data["path"].map(lambda f: f.split("/")[-1])

    data["size"] = data["path"].map(lambda f: f.split("/")[9].split("_")[0])

    data = data[data["filename"] == "meta.json"]

    def scan_json(row):
        p = f"s3://meta-one-assets-prod/bodies/3d_body_files/{row['body']}/{row['version']}/extracted"
        print(p)
        s = [f for f in s3.ls_info(p) if "scan.json" in f["path"]]
        if len(s):
            return s[0]
        return {}

    bdata = data.groupby(["body", "version"]).count().reset_index()
    bdata["scan"] = bdata.apply(scan_json, axis=1)
    bdata["scan_file"] = bdata["scan"].map(lambda x: x.get("path"))
    bdata["scan_modified"] = bdata["scan"].map(lambda x: x.get("last_modified"))
    bdata  # .dropna()

    chk = pd.merge(
        data,
        bdata[["scan_file", "scan_modified", "body", "version"]],
        left_on=["body", "version"],
        right_on=["body", "version"],
        how="left",
    )
    # chk.to_csv('/Users/sirsh/Downloads/compare.csv', index=None)
    return


def reproduce_request_for_sku(sku, req_id, style_id=None, plan=False):
    """
    This is usful to locally troubleshoot one request

        reqs = reproduce_request_for_sku('KT-6075 LY115 TWILOD 2ZZSM','recDFBFaXOlXuL31K' )
        reqs

    normally we require the SKU to make sense for the meta one
    if the stlye is changed, for testing we can still lookup the style id on the request to see what would happen but we should not process it

    """
    style = " ".join(sku.split(" ")[:3])

    size = sku.split(" ")[-1].lstrip().rstrip()

    res.utils.logger.info(f"Loading some lookups...")
    airtable = res.connectors.load("airtable")
    sizes_lu = airtable["appjmzNPXOuynj6xP"]["tblvexT7dliEamnaK"]
    _slu = sizes_lu.to_dataframe(fields=["_accountingsku", "Size Normalized"])
    slu = dict(_slu[["_accountingsku", "Size Normalized"]].values)

    def size_mapper(v):
        return slu.get(v, v.replace("Z", ""))

    def get_sizes_map_for(v):
        return {d: size_mapper(d) for d in v if d not in ["0PTXX"]}

    res.utils.logger.info(f"[{style}] - [{size}] - {req_id}")

    sizes = get_sizes_map_for([size])

    reqs = get_request_for_style_size_bundle(
        style, req_id, sizes_in=sizes, style_id=style_id
    )

    if plan:
        return reqs

    return export_meta_one(_decorate_assets(reqs)[0])


def extract_body_dxf_and_scan_from_box(box_folder_id, plan=False, **kwargs):
    """
    experimental flow: utility to do as title says
    this assumes some manual upload etc.
    in future we write a plugin to just do this...

    errors should be sent as DXF parse errors as validation flags in the queue
    """

    skip = kwargs.get("skip", [])
    if not isinstance(skip, list):
        skip = [skip]

    def clean(s):
        a = s.index("BODY")
        return s.replace(s[a + 4 : -4], "")

    def make_path(row):
        name = row["name"]

        size = row["size"]
        version = row["version"]
        body = row["body"]

        if "scan.json" in name:
            return f"s3://meta-one-assets-prod/bodies/3d_body_files/{body}/{version}/extracted/scan.json"

        return f"s3://meta-one-assets-prod/bodies/3d_body_files/{body}/{version}/extracted/dxf_by_size/{size}/{name}"

    box = res.connectors.load("box")

    c = box._get_client()
    fid = box_folder_id

    folder = c.folder(folder_id=fid).get()

    data = []
    for i in folder.get_items(limit=10):
        if i.object_type == "folder":
            if skip and i.name in skip:
                res.utils.logger.info(f"skipping {i.name}")
                continue
            for f in i.get_items():
                if f.name == "dxf":
                    for f in f.get_items():
                        n = clean(f.name)

                        data.append({"name": n, "size": i.name, "id": f.id})
        else:
            if "scan.json" in i.name:
                data.append({"name": i.name, "size": None, "id": i.id})

    def for_pet(row):
        size = row["size"]
        path = row["path"]
        return path.replace(f"/{size}/", f"/P-{size}/")

    data = pd.DataFrame(data)
    data["body"] = data["name"].map(lambda x: "_".join(x.lower().split("-")[:2]))
    data["version"] = data["name"].map(lambda x: x.split("-")[2].lower().split("_")[0])
    data["path"] = data.apply(make_path, axis=1)
    data["petite_path"] = data.apply(for_pet, axis=1)
    data.iloc[0]["path"]

    if not plan:
        for record in data.to_dict("records"):
            res.utils.logger.info(f"copying to {record['path']}")
            box.copy_to_s3(record["id"], record["path"])
            if kwargs.get("assume_petities"):
                p = record["petite_path"]
                res.utils.logger.info(f"also copying to petite path {p}")
                box.copy_to_s3(record["id"], p)

    return data


@res.flows.flow_node_attributes(
    memory="120Gi",
)
def retry_missing(event, context={}, filters=None, plan=False, hard=False):
    """
    if [hard] reset is used we dont try to infer and just do a hard reset on everything
    """
    from res.flows.meta.ONE.style_node import MetaOneNode

    airtable = res.connectors.load("airtable")
    d = airtable.get_view_data("appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwiu6edbsVEd5b4H")
    uri = "https://data.resmagic.io/res-connect/flows/generate-meta-one"

    # d = d[d["Flag for Review"] != True]
    # limit how many things we run
    for record in d.to_dict("records")[:20]:
        try:
            rid = record["record_id"]
            val = record["Were All Meta ONEs created"]
            if val == 1:
                print("Have all sizes for ", rid)
                continue

            # flags = str(record["History Flag for Review Tags"])
            # TODO if missing ALL_COLOR_PIECES also do the hard reset
            # print(flags)
            # if "Error creating meta.ONE" in flags or hard:
            #     print("hard reset", rid)
            #     res.utils.safe_http.request_post(uri, json={"record_id": rid})
            # else:
            print("Soft reset", rid)
            MetaOneNode.refresh(record["Style Code"], use_record_id=rid)
            # update retries counter on the generate meta one only or both?? Meta ONE Retries

        # every couple of hours we may want to recheck the ones in our new queue with flags
        except Exception as ex:
            res.utils.logger.warn(f"Failed {record}")
            res.utils.logger.warn(f"{traceback.format_exc()}")
    return {}


def handle_status_pings(event, context={}):
    """
    we are sending meta one status updates on a schedule; todo
    - we want to make sure invalidations are added and also Bertha requests
    - a report that shows all open ones against xda requests is also important and maybe in future one that has a priority assigned by other things
    """
    with FlowContext(event, context) as fc:
        kafka = fc.connectors["kafka"]

        kafka_topic = "res_meta.meta_one.status_updates"
        data = (
            fc.assets_dataframe
            if event.get("assets")
            else kafka[kafka_topic].consume(give_up_after_records=1000, errors="ignore")
        )

        s = f"The following assets are passing through DXA recently on env [{RES_ENV}]\n"
        style_url = (
            lambda x: f"https://airtable.com/appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwb6ZwJhSRI5ROQw/{x}?blocks=hide"
        )
        body_url = (
            lambda x: f"https://airtable.com/appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w/viwb6ZwJhSRI5ROQw/{x}?blocks=hide"
        )

        # TODO this url will be linged to the body card or sew etc. for different contracts

        def qualify_node(row):
            title = row["node"]
            if title == "dxa":
                title = (
                    f"{title} - color"
                    if row["unit_type"] == "RES_STYLE"
                    else f"{title} - body"
                )
            return title

        p = {}

        if len(data):
            # some logic tied to our status types - this could easily change
            data["node"] = data.apply(qualify_node, axis=1)

            for tkey, d in data.groupby(["id", "unit_key", "node", "status"]):
                rid = tkey[0]
                title = tkey[1]
                key = tkey[2]
                status = tkey[3]
                emoji = "👌"
                if status == "IN_REVIEW":
                    emoji = "🔍"
                if status == "FAILED_VALIDATION":
                    emoji = "⚠️"
                if status == "FAILED":
                    emoji = "🔥"
                s += f"*{key}*\n"
                # for record in d.to_dict("records"):

                issues = []
                for r in d.to_dict("records"):
                    issues += list(r["validation_error_map"].keys())

                issues = list(set(issues))
                # TODO look up owners from contract
                owner = "Owner <@U01JDKSB196>"

                issues = "" if not issues else " (" + ",".join(issues) + ")"
                q_url = style_url
                if len(key.split(" ")) < 3:
                    q_url = body_url

                s += f"<{q_url(rid)}|{title}> ({len(d)}) - {emoji} {status}{issues} - {owner}\n"

                p = {
                    "slack_channels": ["meta-one-status-updates"],
                    "message": s,
                    "attachments": [],
                }

            res.utils.logger.info("sending notification")
            if fc.args.get("slack_it", True):
                slack = res.connectors.load("slack")
                slack(p)

        return {"message": p}


def _bodies_rebuild_to_prod(bodies, run_local=False):
    """
    a utility for building the payload and running the rebuild for meta ones
    """
    event = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
        "meta.meta_marker.rebuild_meta_ones_for_bodies"
    )

    event["args"]["assets"] = [{"body_code": bc for bc in bodies}]

    url = "https://data.resmagic.io/res-connect/flows/res-flow-node"
    res.utils.logger.info(event)
    # submit it on prod
    if run_local:
        rebuild_meta_ones_for_bodies(event)
    else:
        res.utils.safe_http.request_post(url, json=event)


def rebuild_meta_ones_for_bodies(event, context={}):
    """
    Using meta ones that were created make sure we have all the DB meta ones
    Partioning by bodies is one way to do although some bodies have a large number of styles by size
    """
    s3 = res.connectors.load("s3")

    def rebuild_body(body_code):
        """
        loop through the meta data and load the meta marker and build to a meta one
        """
        items = [
            m.replace("meta.json", "")
            for m in list(
                s3.ls(
                    f"s3://meta-one-assets-prod/styles/meta-one/3d/{body_code.lower().replace('-','_')}"
                )
            )
            if "meta.json" in m
        ]
        for item in items:
            MetaMarker(item).build()
        return item

    with FlowContext(event, context) as fc:
        for asset in fc.assets:
            body_code = asset["body_code"]
            try:
                rebuild_body(body_code)
            except Exception as ex:
                res.utils.logger.warn(
                    f"Failing on body {body_code} = {traceback.format_exc()}"
                )


"""
snippets to consider
"""

# re-cache all meta ones
## import res

# all_meta_ones = []
# bad = {}
# s3 = res.connectors.load('s3')
# for s in s3.ls('s3://meta-one-assets-prod/styles/meta-one/'):

#     if 'meta.json' in s:
#         print(s)
#         s = s.replace('/meta.json','')
#         all_meta_ones.append(s)
#         try:
#             MetaMarker(s).cache_summary()
#         #break
#         except Exception as ex:
#             bad[s] = ex
