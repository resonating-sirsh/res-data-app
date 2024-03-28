from res.media.images.geometry import (
    LineString,
    Polygon,
    Point,
    unary_union,
    three_point_angle,
    line_angle,
    surface_side,
    scale_shape_by,
    swap_points,
    invert_axis,
    rotate,
    pair_nearest_points_as_lines,
    shift_geometry_to_origin,
    from_geojson,
    MultiLineString,
    MultiPoint,
    get_surface_normal_vector_at_point,
)

import numpy as np
import res
from res.learn.optimization.nest import nest_dataframe
import pandas as pd
from res.flows.dxa.styles import helpers
from shapely.affinity import rotate
from shapely.geometry import box
from res.media.images.providers.dxf import DxfFile

DPI = 300
DPMM = 1 / 0.084667
MM_TO_INCHES = 0.393701
MM_TO_HPGL = 39.37
PLOTTER_WIDTH_INCHES = 57


def point_object(tup):
    return dict(zip(["x", "y"], tup[:2]))


def _multiline_array(m, geom_format="array"):
    m = [
        [point_object(p) if geom_format == "xy_points" else list(p) for p in l.coords]
        for l in m.geoms
    ]
    return m


def extract_notch_lines(geo_json, outline, format="array", key=None) -> list:
    """
    pairs of notches appear in the export
    these cna be connected as notch lines
    we make sure these or on the surface or near the surface
    the surface notches are matched to the other notches as lines
    """
    DIST_THRESHOLD = 50
    NOTCH_LINE_LENGTH_THRESHOLD = 160
    points = from_geojson(geo_json)

    if not points:
        return []

    points = shift_geometry_to_origin(points, bounds=outline.bounds)
    points = [points] if points.type == "Point" else list(points.geoms)
    # pack the points into points and distances from outline
    points = list(
        zip(list(points), list(map(lambda x: outline.distance(x), list(points))))
    )

    # partition
    surface_notches = [x[0] for x in points if x[1] == 0]
    non_surface_notches = [x[0] for x in points if x[1] > DIST_THRESHOLD]
    # chk = len(surface_notches),len(non_surface_notches)

    # return surface_notches,non_surface_notches

    ML = None
    if len(surface_notches) == len(non_surface_notches):
        """
        we can use a pairing strategy because the data look good
        """
        ML = pair_nearest_points_as_lines(
            surface_notches, non_surface_notches, threshold=NOTCH_LINE_LENGTH_THRESHOLD
        )
    else:
        ls = [get_surface_normal_vector_at_point(pt, outline) for pt in surface_notches]
        ML = MultiLineString([l for l in ls if l is not None])
    return _multiline_array(ML)


def extract_points(geo_json, geom_format="array", bounds_check=None):
    g = from_geojson(geo_json)
    # should probably factor this out of there - only used for corners for now
    if bounds_check and g:
        try:
            g = shift_geometry_to_origin(MultiPoint(g), bounds=bounds_check)
        except:
            pass

    def points_from_geom(g):
        if g:
            if g.type in ["MultiPoint", "MultiPointZ"]:
                for pt in g.geoms:
                    yield (pt.x, pt.y)
            elif g.type in ["LineString", "LinearRing"]:
                for pt in g.coords:
                    yield pt
            else:
                raise Exception(f"We are not yet handling {type(g)} {g}")

    return [
        {"x": pt[0], "y": pt[1]} if geom_format != "array" else [pt[0], pt[1]]
        for pt in (g if isinstance(g, list) else points_from_geom(g))
    ]


def parse_placeholders(x):
    def _parse(f):
        props = f["properties"]
        d = {"position": f["geometry"]["coordinates"]}
        d.update(
            {
                "size": [props["width"], props["height"]],
                "angle": np.degrees(props["radians"]),
                "name": props["name"],
            }
        )
        return d

    if not len(x):
        return []
    return [_parse(f) for f in x["features"]]


def parse_seam_guides(seam_guides_geojson: str, bounds_check=None, geom_format="array"):
    """
    seam guide json format is parsed and we check that we have
    the correct reference frame WRT outline

    bounds_check is the parent outline that sets the reference frame
    """

    def _named(i, l):
        return {"id": f"SG{i//2}{'A' if i % 2 == 0 else 'B'}", "points": l}

    if seam_guides_geojson == {}:
        return []

    m = unary_union(from_geojson(seam_guides_geojson))
    m = unary_union([s for s in m.geoms if s.type != "Point"])
    if bounds_check:
        m = shift_geometry_to_origin(m, bounds=bounds_check)

    if geom_format:
        m = _multiline_array(m)
        if geom_format == "named_lines":
            m = [_named(idx, l) for idx, l in enumerate(m)]
    return m


def geometry_data_to_hpgl(
    df,
    filename,
    should_nest=True,
    geometry_column="geometry",
    nest_grouping_column=None,
    output_bounds_width=PLOTTER_WIDTH_INCHES * DPI,
    rotate_pieces=True,
    try_fix_wide_pieces=True,
    buffer=70,
    plot=False,
):
    """
    the geometries passed in are assumed to be lines or multi lines
    for example if the multi geometry contained points they should be mapped to buffer exteriors or discs
    this is a simple interface to support rendering shapes on hpgl - we are deprecating these kinds of stampers

    df:> should be
    key: piece key
    geometry: multi geometry of all shapes / lines on the piece

    filename is a local or s3 path. pass none to just return the lines of the files
    saving to a file is useful for testing with PLT viewers such as this one         https://fileproinfo.com/tools/viewer/plt

        spec: https://www.hpmuseum.net/document.php?catfile=213
        see: https://www.isoplotec.co.jp/HPGL/eHPGL.htm#-PM(Polygon%20Mode)

    """
    if df is None or len(df) == 0:
        res.utils.logger.warn(f"No data to save in this dataframe")
        return
    # when we scale we scale down to mm and
    SCALE_FACTOR = (1 / DPMM) * MM_TO_HPGL
    TXT_FACTOR = 600  # function of eigen size of label
    BUFFER = buffer or 70
    header = """IN;IP0,0,1016,1016;SC0,1000,0,1000;SP1;\nDI1.000,0.000;DT$;SI0.423,0.635;LO5;"""  #
    lines = []

    # try to rotate wide pieces
    if try_fix_wide_pieces:
        df[geometry_column] = df[geometry_column].map(
            lambda x: x if x.bounds[-2] < output_bounds_width else swap_points(x)
        )
    ####THIS IS AN IMAGE SPACE THING - we want the Y axis to be inverted so these match the way we generate images that are printed
    try:
        df[geometry_column] = df[geometry_column].map(invert_axis)
    except:
        res.utils.logger.warn(f"Tried and failed to invert the axis")

    ######################################################

    def hpgl_scale(g):
        return scale_shape_by(SCALE_FACTOR)(g)

    def hpol(pts, line_style=None, term="PU;"):
        pts = np.array(pts).astype(int)
        p0 = pts[0]
        s = f"PU{p0[0]},{p0[1]};"
        path = ",".join([f"{p[0]},{p[1]}" for p in pts[1:]])
        if line_style:
            ls = f"LT{line_style[0]},{line_style[1]}"
            # resetting line style when we change it
            return f"{ls};{s}PD{path};{term}LT;"

        return f"{s}PD{path};{term}"

    def get_lines(geom, **kwargs):
        if geom.type.startswith("Multi") or geom.type == "GeometryCollection":
            for l in g.geoms:
                yield hpol(l.coords, **kwargs)
        else:
            yield hpol(g.coords, **kwargs)

    if should_nest and len(df) > 1:
        # nesting is done because we want the pieces to not be all on the origin
        df = nest_dataframe(
            df.reset_index(),
            geometry_column=geometry_column,
            grouping_column=nest_grouping_column,
            output_bounds_width=output_bounds_width,
            buffer=BUFFER,
            output_bounds_length=50000000,
        )

        df[geometry_column] = df[f"nested.original.{geometry_column}"]

    if rotate_pieces:
        # plotter may be on the axis rotated space?
        df[geometry_column] = df[geometry_column].map(swap_points)

    if plot:
        from geopandas import GeoDataFrame

        GeoDataFrame(df).plot(figsize=(40, 40))

    for _, row in df.iterrows():
        g = hpgl_scale(row[geometry_column])
        c = g.centroid
        lines += list(get_lines(g))
        bounds = g.bounds
        W = bounds[2] - bounds[0]
        H = bounds[3] - bounds[1]
        # text direction depends on shape
        DIR = "DI;" if W * 3 > H else "DI0,1;"
        offset = (TXT_FACTOR, 0) if W * 3 > H else (0, TXT_FACTOR)
        # need a rotational model for text positioning DI can make it 180 if piece too narrow
        lines.append(
            f"PU{int(abs(c.x-offset[0]))},{int(c.y-offset[1]-(TXT_FACTOR/20))};{DIR}LB{row['key']}$;"
        )

    if filename:
        res.utils.logger.info(f"Writing file {filename}")
        if "s3://" not in filename:
            with open(filename, "w") as f:
                f.writelines(f"{header}\n")
                for l in lines:
                    f.writelines(f"{l}\n")
        else:
            with res.connectors.load("s3").file_object(filename, "w") as f:
                f.writelines(f"{header}\n")
                for l in lines:
                    f.writelines(f"{l}\n")

    return df  # [header] + lines


def make_body_cutfiles(
    body_code,
    body_version,
    outfile=None,
    plot=False,
    try_fix_wide_pieces=True,
    nest_width=PLOTTER_WIDTH_INCHES * DPI,
    piece_type="stamper",
    hasura=None,
):
    """
    this has a dep on a poor size resolver that we need to fix

    plot=True to show the thing that will be saved to plt format
    """
    from res.flows.dxa.styles import queries

    # outfile = (
    #     outfile
    #     or f"/Users/sirsh/Downloads/{piece_type}_{body_code}-V{body_version}.plt"
    # )
    hasura = hasura or res.connectors.load("hasura")
    sizes = queries.list_all_body_sizes(body_code, body_version, hasura=hasura)
    sizes = [s for s in sizes if "3D-" not in s]
    res.utils.logger.info(f"Compiling data for sizes {sizes}...")
    D = []
    for s in sizes:
        d = queries.get_body(body_code, body_version, s, flatten=True, hasura=hasura)
        if len(d) == 0:
            continue
        d = d[d["body_pieces_type"] != "unknown"].reset_index().drop("index", 1)
        if len(d) == 0:
            continue
        d["key"] = d.apply(
            lambda row: f"{row['body_pieces_piece_key']}_{helpers.accounting_size_to_size_chart(row['body_pieces_size_code'])}",
            axis=1,
        )
        d = d[d["body_pieces_type"] == piece_type].reset_index().drop("index", 1)
        D.append(d)
    d = pd.concat(D).reset_index()

    if outfile is None:
        outfile = f"s3://meta-one-assets-prod/bodies/cut/{body_code.replace('-','_').lower()}/v{int(body_version)}/stamper_{body_code}-V{body_version}_all_sizes.plt"
        res.utils.logger.info(f"Uploading to {outfile}")

    return geometry_data_to_hpgl(
        d,
        filename=outfile,
        plot=plot,
        rotate_pieces=True,
        try_fix_wide_pieces=try_fix_wide_pieces,
        output_bounds_width=nest_width,
    )


def notch_blocks(lines, half_width=3, buffer=2, fill=False, BLOCK_LENGTH=50):
    """
    Create little blocks around the seam guide or notches with some buffer onto the surface.
    found a case KT-6051-V4-DRSFTPLKLF-BF where the seam guide was a point so filtering because we cannot block it

    we usually draw them not filled but we can fill them which is a structure element used to erase from polygons

    by rotating the base and creating a double length piece, we can create a block that extends outside outline and this can be tripped
    this is to remove an irregular joining sections on the surface for sharp angles

    we can use a regular buffer function with the right params to not bevel but this works
    """

    if not lines:
        return lines

    def custom_buffer(l):
        a = l.parallel_offset(half_width, "left")
        b = l.parallel_offset(half_width, "right")
        return unary_union([a, b]).convex_hull

    def _make_block(s):
        def segment(s):
            return LineString([s.interpolate(0), s.interpolate(BLOCK_LENGTH)])

        # below we create a b that protrudes out to mask sharp angles on the surface
        # its clipped when not contained in outline elsewhere
        s = segment(s)
        a = custom_buffer(s)
        # rotate from the base to extend to double length
        b = rotate(a, 180, origin=s.interpolate(0))

        def check_fill(g):
            return g.convex_hull.exterior if fill == False else g.convex_hull

        return check_fill(
            unary_union(
                [
                    a,
                    b,
                ]
            )
        )

    return [_make_block(s) for s in lines if s.type != "Point"]


def _geom_seam_guide_blocks(seam_guides, half_width=3, buffer=2):
    """
    Create little blocks around the seam guide with some buffer onto the surface.
    found a case KT-6051-V4-DRSFTPLKLF-BF where the seam guide was a point so filtering because we cannot block it
    """

    if not seam_guides:
        return seam_guides

    def _make_block(s):
        def half(s):
            return LineString([s.interpolate(0), s.interpolate(50)])

        s = half(s)
        a = s.parallel_offset(half_width, "left")
        b = s.parallel_offset(half_width, "right")

        return unary_union(
            [
                a,
                b,
            ]
        ).convex_hull.buffer(buffer)

    return [_make_block(s) for s in seam_guides if s.type != "Point"]


def _remove_shapes_from_outline(ol, shapes):
    """
    remove the blocks from the 2d shape
    """
    if shapes.is_empty:
        return ol
    g = Polygon(ol) - shapes
    if g.type == "MultiPolygon":
        res.utils.logger.warn(
            f"Shape subtraction resulted in MultiPolygon - may have unexpected results"
        )
        g = max(g, key=lambda q: q.area)
    return g.exterior


def get_forbidden_seam_regions(p, factor=200, max_seam_guide_separation=5):
    """
    remove the segments between the seam guide pairs
    """
    O = Polygon(p.piece_outline).buffer(factor).exterior
    sg_points = [l.interpolate(0) for l in p.seam_guides]
    sg_2_points = [O.interpolate(O.project(k)) for k in sg_points]
    regions = []
    for i in np.arange(0, len(sg_points), 2):
        a = sg_points[i]
        b = sg_points[i + 1]
        # we assume seam guides are in adjacent ordered pairs
        if a.distance(b) > max_seam_guide_separation:
            continue
        c = sg_2_points[i]
        d = sg_2_points[i + 1]
        regions.append(unary_union([a, b, c, d]).convex_hull)
    # there should always be regions but when there are not (maybe circular shapes) we just use the seam guides themselves as forbidden regions
    if len(regions) == 0:
        return p.seam_guides

    regions = p.piece_outline.intersection(unary_union(regions))
    # make sure they are close by!
    return unary_union([r for r in regions if r.distance(p.seam_guides) < 5])


def get_edge_properties(e, sample_factor=150):
    """
    get properties of edges so that we can make choices about placements
    """
    l = e.length
    samples = int(l / sample_factor) + 1

    max_curve, mean_curve = 0, 0
    sampled_values = []
    if l > sample_factor * 2:
        for i in range(samples - 1):
            b = (i + 1) * sample_factor
            a = b - sample_factor
            c = b + sample_factor
            a = e.interpolate(a)
            b = e.interpolate(b)
            c = e.interpolate(c)
            C = three_point_angle(a, b, c)
            C = 1 - (abs(C) / 180.0)
            sampled_values.append(C)
        mean_curve = np.mean(sampled_values)
        max_curve = np.max(sampled_values)

    return {
        "length": l,
        "max_curvature": max_curve,
        "mean_curvature": mean_curve,
        "num_points": len(e.coords),
        "avg_segment_length": l / len(e.coords),
    }


def get_viable_seam_locations(
    vs,
    ref_outline,
    label_width,
    min_segment_length=700,
    min_angle=170,
    allow_sides=["top", "bottom", "left", "right"],
    sort_by="avg_segment_length",
    use_line_rule=False,
):
    """
    For a given label width find locations where we can place labels and the geometric info about those locations
    """

    elements = []

    for lidx, l in enumerate(vs):
        if l.length > min_segment_length:
            # interpolate in small jumps along the segment
            # if the curvature is small enough, accept the coordinate
            # find places from half the label width which is the first place we can center-place until near the end. We can do better
            for pt in np.arange(label_width / 2, l.length - label_width, 10):
                # check curvature where the label would be placed using a,b,c as the end points and b the center of the label
                a, c = l.interpolate(pt), l.interpolate(pt + label_width)
                b = l.interpolate(pt + label_width / 2.0)
                # is abs ok here
                theta = abs(three_point_angle(a, b, c))
                pChord = LineString([a, c])
                side = surface_side(b, ref_outline, line_rule=use_line_rule)

                if side in allow_sides:
                    if theta > min_angle:
                        d = {}
                        d["edge_properties"] = get_edge_properties(l)
                        d["segment"] = l
                        d["segment_idx"] = lidx
                        d["placement_position"] = b
                        d["place_chord"] = pChord
                        d["placement_angle"] = line_angle(pChord)
                        d["directional_label"] = False
                        d["piece_side"] = side
                        elements.append(d)

    # sort the segments by the reversed segment length
    elements = sorted(
        elements, key=lambda e: e.get("edge_properties").get(sort_by), reverse=True
    )
    return elements


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


def bias_sign(g):
    """
    conventional bias sign
    first we detect if it is a bias (a test above) then we create a convention for the sign
    if it leans / in image space the sign is 1 otherwise its -1
    """
    if is_bias_piece(g):
        return 1 if Point(0, 0).distance(g) > g.bounds[-2] / 2 else -1
    return 0


def _is_incorrectly_biased_image(outline):
    return bias_sign(outline) == -1


def is_mismatched_bias(a, b):
    return bias_sign(a) != bias_sign(b)


def make_body_printable_pieces_cutfiles(
    body_code,
    body_version,
    outfile=None,
    plot=False,
    try_fix_wide_pieces=True,
    output_bounds_width=PLOTTER_WIDTH_INCHES * DPI,
    nest_width=PLOTTER_WIDTH_INCHES * DPI,
    hasura=None,
    upload_record_id=None,
):
    """
    this has a dep on a poor size resolver that we need to fix

    plot=True to show the thing that will be saved to plt format
    """
    from res.flows.dxa.styles import queries

    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")
    hasura = hasura or res.connectors.load("hasura")
    sizes = queries.list_all_body_sizes(body_code, body_version, hasura=hasura)
    sizes = [s for s in sizes if "3D-" not in s]
    res.utils.logger.info(f"Compiling data for sizes {sizes}...")
    D = []
    all_files = []
    for s in sizes:
        d = queries.get_body(body_code, body_version, s, flatten=True, hasura=hasura)
        if len(d) == 0:
            continue
        d = d[d["body_pieces_type"] != "unknown"].reset_index().drop("index", 1)
        if len(d) == 0:
            continue
        d["key"] = d.apply(
            lambda row: f"{row['body_pieces_piece_key']}_{helpers.accounting_size_to_size_chart(row['body_pieces_size_code'])}",
            axis=1,
        )

        size_chart = helpers.accounting_size_to_size_chart(s)
        res.utils.logger.info(f"size chart {size_chart} from {s}")
        d = (
            d[d["body_pieces_type"].isin(["self", "combo", "lining", "block_fuse"])]
            .reset_index()
            .drop("index", 1)
        )

        df = d
        res.utils.logger.info(f"nesting the dxf for size {size_chart}")

        # we are nesting the outlines - by default geometry is the full collection of shapes including internal lines etc.
        # we would need to tweak the exporter to save that to dxf
        df_inches = nest_dataframe(
            df.reset_index(),
            geometry_column="body_pieces_outer_geojson",
            output_bounds_width=70 * 300,
            output_bounds_length=50000000,
            buffer=150,
        )

        outfile_dxf = f"s3://meta-one-assets-prod/bodies/printable-cut/{body_code.replace('-','_').lower()}/v{int(body_version)}/printable_{body_code}-V{body_version}_{size_chart.replace(' ','_')}.dxf"
        res.utils.logger.info(f"Uploading the dxf to {outfile_dxf}")

        # return df_inches

        DxfFile.export_dataframe(
            pd.DataFrame.from_dict(
                {
                    "key": df_inches["key"],
                    "geometry": df_inches[
                        "nested.original.body_pieces_outer_geojson"
                    ].values,
                }
            ),
            outfile_dxf,
            global_attributes_in={"STYLE_NAME": f"{body_code} V{body_version}"},
        )

        all_files.append(outfile_dxf)

        outfile = f"s3://meta-one-assets-prod/bodies/printable-cut/{body_code.replace('-','_').lower()}/v{int(body_version)}/printable_{body_code}-V{body_version}_{size_chart.replace(' ','_')}.plt"
        res.utils.logger.info(f"Uploading to {outfile}")

        res.utils.logger.info("nesting the PLT")
        df = geometry_data_to_hpgl(
            df,
            filename=outfile,
            plot=plot,
            buffer=150,
            rotate_pieces=True,
            try_fix_wide_pieces=try_fix_wide_pieces,
            output_bounds_width=nest_width,
        )

        all_files.append(outfile)

    if upload_record_id:
        res.utils.logger.info(all_files)
        files = [{"url": s3.generate_presigned_url(file)} for file in all_files]
        res.utils.logger.info(f"Saving files {files} to record {upload_record_id}")
        airtable["appa7Sw0ML47cA8D1"]["tbl0ZcEIUzSLCGG64"].update_record(
            {"record_id": upload_record_id, "Printable Pieces Cutfiles By Size": files}
        )

    return df


def geometry_df_to_svg_pieces(df):
    for i, row in df.iterrows():
        _df = row.to_frame().T

        yield row["key"] + "_ALL_LINES", geometry_df_to_svg(
            _df[["key", "nested.original.geometry", "size_code"]].rename(
                columns={"nested.original.geometry": "outline"}
            )
        )
        yield row["key"], geometry_df_to_svg(
            _df[["key", "nested.original.outline", "size_code"]].rename(
                columns={"nested.original.outline": "outline"}
            )
        )


def geometry_df_to_svg(df):
    """

    #for example nest body pieces and display

    #see meta one function

    """
    import numpy as np
    from res.media.images.geometry import (
        unary_union,
        scale_shape_by,
        rotate,
        invert_axis,
        translate,
        MultiLineString,
    )

    def unpack_ml(ml):
        for l in ml:
            for p in l.coords:
                yield p

    def as_svg_path(l, name, jitter=(0, 0)):
        if l.type == "LineString" or l.type == "LinearRing":
            l = LineString(l)
            l = MultiLineString([l])

        H = l.bounds[-1] - l.bounds[1]
        W = l.bounds[-2] - l.bounds[0]
        centroid = l.centroid
        lines = []
        for idx, item in enumerate(l):
            points = list(item.coords)
            id_ = "test"  ##int(l[:, -1].min())
            points = " ".join([f"{p[0]},{p[1]}" for p in points])
            points = f"""<polyline points="{points}" id="{id_}_{idx}" fill="whitesmoke" stroke="black" stroke-width="20"/>"""
            lines.append(points)

        lines = f"\n".join(lines)

        simple_rotate = (
            "" if W > 1000 else f'transform="rotate(-90, {centroid.x}, {centroid.y})"'
        )
        xoff, yoff = (-600, 0) if W > 1000 else (-600, 0)

        # for bias pieces we should be smarter about text
        p = f"""
        <g id="outlines">
        {lines}
        </g>
         <g id="piece_names" {simple_rotate} >
        <text x="{centroid.x+xoff}" y="{centroid.y+yoff }" fill="black" font-size="200px" class="heavy">{name}</text>
        </g>
        """

        return p

    # def try_get_notches(outline):
    #     try:
    #         return detect_castle_notches_from_outline(outline)
    #     except:
    #         return None

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

    # move things around and scale them
    all_shape_bounds = unary_union(df["outline"]).bounds
    SCALE = 1
    # flip and scale from the DXF
    df["outline"] = (
        df["outline"]
        .map(lambda g: rotate(g, 90, origin=(0, 0)))
        .map(lambda x: scale_shape_by(SCALE)(x))
    )

    #     if ensure_valid:
    #         df["outline"] = df["outline"].map(try_ensure_valid)

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
        name = row["key"] + " " + row["size_code"]
        g = row["outline"]
        # TODO test text orientation
        ps += as_svg_path(g, name) + "/n"
    # transform="translate(0,{H}) scale(1, -1)"
    svg = f"""<svg viewBox="{gbounds_str}" xmlns="http://www.w3.org/2000/svg">
    <g fill="white" >
        {ps}
    </g>
    </svg>
    """

    return svg
