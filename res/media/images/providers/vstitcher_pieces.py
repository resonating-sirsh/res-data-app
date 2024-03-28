import math
import numpy as np
import pandas as pd
import res
from res.media.images.geometry import *
from shapely.geometry import LinearRing, LineString, MultiPoint, Polygon, box
from shapely.ops import unary_union, substring
from shapely.affinity import rotate, scale

DPI = 300
INCHES_PER_CM = 0.393701
DOTS_PER_CM = DPI * INCHES_PER_CM
SPACE_SCALE = DOTS_PER_CM  # set to 1 to keep at cms

DXF_SPACE_SCALE = SPACE_SCALE / 10  # dxf is in mm we are in cm


class VstitcherPieces:
    def __init__(self, file_or_data):
        s3 = res.connectors.load("s3")
        readit = lambda path: (
            res.utils.read(path) if "s3://" not in path else s3.read(path)
        )
        self._data = (
            file_or_data if not isinstance(file_or_data, str) else readit(file_or_data)
        )
        self.metadata = {}
        if type(self._data) is dict:
            metadata = {}
            for key, value in self._data.items():
                if key == "pieces":
                    self.pieces = {
                        v["bzw_key"]: VstitcherPiece(v)
                        for v in self._data.get("pieces").values()
                    }
                else:
                    setattr(self, key, value)
                    metadata[key] = value

            self.metadata = metadata
        else:  # old pieces.json format
            self._data = {item["bzw_key"]: item for item in self._data}
            self.pieces = {k: VstitcherPiece(v) for k, v in self._data.items()}

        # if the data has "grain_line_degrees" we know it's a new pieces file
        self.is_new = any(
            [p.grain_line_degrees is not None for p in self.pieces.values()]
        )

        # self.df = self._as_dataframe()

        # TODO: interconnect the piece objects for each connections to make a graph

    @staticmethod
    def for_body_code(body):
        s3 = res.connectors.load("s3")
        body_fs = body.lower().replace("-", "_")
        body_file_paths = list(
            s3.ls(f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_fs}")
        )
        body_file_paths = [x for x in body_file_paths if x.endswith("pieces.json")]
        body_file_paths.sort()
        pieces_path = body_file_paths[-1]
        res.utils.logger.debug(f"VS pieces at {pieces_path}")
        d = s3.read(pieces_path)
        return VstitcherPieces(d)

    staticmethod

    def iterate_bodies(drop_dups_over_sizes=True):
        from tqdm import tqdm

        s3 = res.connectors.load("s3")
        df = pd.DataFrame(
            [
                f
                for f in s3.ls("s3://meta-one-assets-prod/bodies/3d_body_files/")
                if "pieces.json" in f
            ],
            columns=["uri"],
        )
        df["body_code"] = df["uri"].map(lambda x: x.split("/")[5])
        df["size"] = df["uri"].map(lambda x: x.split("/")[-2])
        df["version"] = df["uri"].map(lambda x: x.split("/")[6])

        df = df.drop_duplicates(subset=["body_code"])
        res.utils.logger.info(f"Looking at {len(df)} unique bodies")
        for f in tqdm(df["uri"]):
            yield VstitcherPieces(s3.read(f))

    def get_dxf_transformation_for_piece(self, piece, dxf_edges):
        dxf_edges = shift_geometry_to_origin(dxf_edges)

        # in order to line them up, we need to get the transformation matrix
        d = dxf_edges.bounds
        p = piece.geometry.bounds

        translation = (d[0] - p[0], d[1] - p[1])

        x_scale = (d[2] - d[0]) / (p[2] - p[0])
        y_scale = (d[3] - d[1]) / (p[3] - p[1])

        def transform(g):
            to_origin = translate(g, translation[0], translation[1])
            scaled = scale(to_origin, xfact=x_scale, yfact=y_scale, origin=(0, 0))
            return scaled

        return transform

    def get_seam_allowances_per_edge(self, key, dxf_edges, plot=False):
        piece = self.pieces[key]
        transform = self.get_dxf_transformation_for_piece(piece, dxf_edges)

        return piece.get_seam_allowances_per_edge(dxf_edges, transform, plot=plot)

    def project_dxf_to_vs_space(self, dxf_geometry):
        # VS, pieces and print images are in negative y and rotated
        # rotate first, then scale and shift
        dxf_geometry = rotate(dxf_geometry, 90, origin=(0, 0))
        # while we could do scale_to, we know the dxf is in mm and we are in cm so the
        # difference between our geometry and the dxf is PRINT_SPACE * 10
        # so let's keep the aspect correct
        dxf_geometry = scale(
            dxf_geometry, xfact=DXF_SPACE_SCALE, yfact=-DXF_SPACE_SCALE, origin=(0, 0)
        )

        return dxf_geometry

    def align_dxf_with_piece(self, dxf_geometry, piece_key):
        # VS, pieces and print images are in negative y and rotated
        # rotate first, then scale and shift
        dxf_geometry = self.project_dxf_to_vs_space(dxf_geometry)

        piece = self.pieces.get(piece_key)
        if not piece:
            res.utils.logger.warn(
                f"Could not find piece {piece_key} in the pieces file to align with"
            )
        else:
            piece_bounds = box(*piece.exterior_geometry.bounds)
            dxf_bounds = box(*dxf_geometry.bounds)

            # if it's less than 90% overlap, shift it
            if piece_bounds.intersection(dxf_bounds).area < 0.9 * piece_bounds.area:
                res.utils.logger.warn(
                    f"DXF {piece_key} does not overlap piece, shifting..."
                )

                x_offset = piece_bounds.bounds[0] - dxf_bounds.bounds[0]
                y_offset = piece_bounds.bounds[1] - dxf_bounds.bounds[1]
                dxf_geometry = translate(dxf_geometry, x_offset, y_offset)

        return dxf_geometry

    def get_connections_dfs_with_labelled_regions(self, piece_lookup=None):
        """
        adding a util for preping masks
        """
        from res.flows.meta.pieces import PieceName

        if piece_lookup is None:
            piece_lookup = PieceName.get_piece_and_region_lookup_function()

        def modify_set(test):
            test["source_piece_code"] = test["source"].map(lambda x: PieceName(x).name)
            test["target_piece_code"] = test["target"].map(
                lambda x: PieceName(x).name if pd.notnull(x) else None
            )
            test["source_piece_type"] = test["source"].map(
                lambda x: f"{PieceName(x).part}{PieceName(x).component}"
            )
            test["target_piece_type"] = test["target"].map(
                lambda x: (
                    f"{PieceName(x).part}{PieceName(x).component}"
                    if pd.notnull(x)
                    else None
                )
            )
            test["source_piece_region"] = test["source_piece_type"].map(
                lambda x: piece_lookup(x).get("region") if pd.notnull(x) else None
            )
            test["target_piece_region"] = test["target_piece_type"].map(
                lambda x: piece_lookup(x).get("region") if pd.notnull(x) else None
            )

            test["part_operation"] = False
            test.loc[
                (~test["self_connection"])
                & (test["source_piece_type"] == test["target_piece_type"]),
                "part_operation",
            ] = True

            test.loc[test["self_connection"], "target_piece_region"] = "Self"
            test.loc[
                test["target_piece_region"].map(lambda x: "BOD" in str(x)),
                "target_piece_region",
            ] = "Bodice"
            test.loc[
                test["target_piece_region"].map(lambda x: "BTM" in str(x)),
                "target_piece_region",
            ] = "Lower Body"

            return test

        connections = []
        for key, p in self.pieces.items():
            p.shift_all_geometries_to_origin()

            df = p.connections_df
            piece_area = Polygon(p.geometry).area

            if not len(df):
                continue

            if not len(df):
                continue

            if "shape_id" not in df.columns:
                df["shape_id"] = -1

            df["source"] = key
            df["target"] = df["shape_name"]
            df["self_connection"] = False
            df.loc[df["target"] == "<no connection>", "target"] = None
            df.loc[df["source"] == df["target"], "self_connection"] = True
            # we want all the edges even if no connections
            df = df[(df["source"].map(lambda x: x.split("-")[-1] not in ["X", "F"]))]
            cols = [
                "source",
                "target",
                "from_start_percent",
                "from_end_percent",
                "geometry",
                "self_connection",
                "seam_allowance",
            ]
            for c in cols:
                if c not in df.columns:
                    df[c] = None

            if len(df):
                df = modify_set(df[cols])
                connections.append(df)

            df["piece_area"] = piece_area
            piece_bounds = p.geometry.bounds
            df["piece_height"] = piece_bounds[-1]
            df["piece_width"] = piece_bounds[-2]
            df["piece_is_rectangular"] = is_polygon_rectangular(p.geometry)
            # symmetries are important for knowing if we might need to duplicate something - for example a full yoke should probably be symmetric
            df["piece_has_x_symmetry"] = has_x_symmetry(p.geometry)
            df["piece_has_y_symmetry"] = has_y_symmetry(p.geometry)
            df["piece_surface_length"] = p.geometry.length
            df["edge_index"] = df["geometry"].map(p.label_edge)
            df["is_blocked_fused"] = df["source"].map(
                lambda x: x.split("-")[-1] == "BF"
            )
            df["orientation"] = df["source"].map(lambda x: PieceName(x).orientation)
            df["target_orientation"] = (
                df["target"]
                .map(lambda x: PieceName(x).orientation if pd.notnull(x) else None)
                .map(lambda x: x if x != "" else None)
            )

            # the orientation

        df = pd.concat(connections)

        df["length"] = df["geometry"].map(lambda x: x.length)
        df["curvature_score"] = df["geometry"].map(bend_curvature)

        return df

    def iterate_edge_labelled_masks(self, piece_lookup=None):
        """
        This is a crude thing to iterate over images and try to determine what the image masks look like
        """
        from res.flows.meta.pieces import PieceName

        piece_lookup = piece_lookup or PieceName.get_piece_and_region_lookup_function()
        df = self.get_connections_dfs_with_labelled_regions(piece_lookup=piece_lookup)
        return LabellerTool.iterate_connection_region_masks(df, vspcs=self)


class VstitcherPiece:
    @staticmethod
    def geom_fix(g):
        """
        VStitcher's display is in image space where y decreases as you go up
        the points from the api have the rotation already applied to them
        the print file is always at 90 degrees to this
        as a rule we will orient for print space so y decreases as you go up,
        and scale is in dpi not cm

        (whenever we plot we'll invert the axis so it's the right way up comparitively)
        """
        g = rotate(g, -90, origin=(0, 0))
        return scale(g, xfact=SPACE_SCALE, yfact=SPACE_SCALE, origin=(0, 0))

    def __init__(self, piece_data):
        # print(piece_data["bzw_key"])
        # imported here due to a circular ref with dxf...
        from res.flows.meta.pieces import PieceName

        self.geometry = self.geom_fix(
            LinearRing(self.points_to_array(piece_data["points"]))
        )

        self._data = piece_data
        self.key = piece_data["bzw_key"]
        # res.utils.logger.debug(f"   Loading piece {self.key}")
        self.name = PieceName(self.key)
        self.is_base_size = piece_data.get("is_base_size")
        self.grain_line_degrees = piece_data.get("grain_line_degrees")

        self.raw_edges_data = [
            e
            for e in piece_data["edges"]
            if len(e["points"]) > 1
            and LineString(self.points_to_array(e["points"])).length > 0
        ]
        self.edges_df = self.lines_as_df(self.raw_edges_data)

        self.bezier_edges = []
        try:
            self.bezier_edges = [
                {
                    **item,
                    "points": [
                        {
                            "point": self.geom_fix(Point([point["x"], point["y"]])),
                            "handle1": self.geom_fix(
                                Point([point["handle1"]["x"], point["handle1"]["y"]])
                            ),
                            "handle2": self.geom_fix(
                                Point([point["handle2"]["x"], point["handle2"]["y"]])
                            ),
                        }
                        for point in item["points"]
                    ],
                }
                for item in piece_data.get("bezier_edges", [])
            ]
        except:
            pass

        cdf = self.connections_df = self.connections_as_df(self.raw_edges_data)
        self.has_no_connections = cdf[cdf["shape_name"] != "<no connection>"].empty
        self.seam_changes_df = self.seam_changes_as_df(self.connections_df)

        self.internal_lines_data = piece_data.get("internal_lines_by_id", {})
        self.internal_lines = {
            i: {
                **item,
                "geometry": self.geom_fix(
                    LineString(self.points_to_array(item["points"]))
                ),
            }
            for i, item in self.internal_lines_data.items()
            if len(item["points"]) > 1
        }
        self.internal_lines_df = self.lines_as_df(
            list(self.internal_lines_data.values())
        )

        # apply the same mirror fix to the edges, connections and seam_changes
        for df in [
            self.edges_df,
            self.connections_df,
            self.seam_changes_df,
            self.internal_lines_df,
        ]:
            if "geometry" in df.columns:
                df["geometry"] = df["geometry"].map(self.geom_fix)

        self.placeholders = self.extract_special_artworks(
            piece_data.get("placeholders", {})
        )
        self.pins = self.extract_special_artworks(piece_data.get("pins", {}))

        self.pleats = self.extract_pleats(piece_data.get("pleats", {}), self.edges_df)
        self.buttons = self.extract_buttons(piece_data.get("buttons", {}))

        self.edge_geometries = self.edges_df["geometry"].values

        # # if there are no seam changes or connections fall back to the edge ends
        # if "geometry" in self.seam_changes_df:
        #     self.corner_geometries = self.seam_changes_df["geometry"].values
        # else:
        #     self.corner_geometries = [
        #         e.interpolate(0, normalized=True) for e in self.edge_geometries
        #     ]

        self.seam_allowances_in_edge_order = [
            e.get("seam_allowance", 0) for e in self.raw_edges_data
        ]

        self.seam_guides = []
        self.outer_corners = []
        self.inner_corners = []
        self.corner_reasons = []
        self.determine_corners_and_seam_guides()

        self.symmetry = self._data.get("symmetry", None)

        self.geometry_attributes = self.deduce_geometry_attributes()
        self.sew_identity_symbol_id_old = self.get_sew_identity_symbol_id_old()
        self.sew_identity_symbol_id = self.get_sew_identity_symbol_id()

        # once we have the exterior geometry we could shift everything to the origin
        # so that it will match the dxf, but let's let the user decide when/where
        self.exterior_geometry = self.get_exterior()
        # self.shift_all_geometries_to_origin()

        self._key_points = None

    @property
    def key_points(self):
        if self._key_points is None:
            self._key_points = get_geometry_keypoints(self.geometry)
        return self._key_points

    def first_corner(self):
        """
        the first corners is a reference corner typically found at the bottom right of a garment
        notes:
            corners = unary_union(P.inner_corners)
            a = first_corner(P.geometry, corners)
            a = a.buffer(100).exterior
            unary_union([P.geometry, corners.buffer(20),a])

        """
        g = self.geometry
        c = unary_union(self.inner_corners)
        # todo add these into the v-s peices with safety
        if self._key_points is None:
            self._key_points = get_geometry_keypoints(g)
        cpt = self._key_points["center_of_mass"]
        factor = np.sum(g.bounds[-2:]) * 2
        ref_point_far = Point(cpt.x * factor, g.bounds[-1] * factor)
        ref_corner = nearest_points(c, ref_point_far)[0]

        return ref_corner

    def first_corner_projection(self):
        """
        how far is this from the end of the geometry. shift by that much to put it at the ref point
        """
        return self.geometry.length - self.geometry.project(self.first_corner())

    def first_corner_projection_offset_function(self):
        offset = self.first_corner_projection()
        g = self.geometry
        l = g.length

        def f(pt):
            return (g.project(pt) + offset) % l

        return f

    def label_edge(self, e):
        """
        we use reference corners and sort according them to
        the edges are labelled as we move away from the ref corner
        """
        fn = self.first_corner_projection_offset_function()
        # we are arranging the corners and adding into the mix the projection for the center of the edge
        a = [(i, fn(p)) for i, p in enumerate(self.inner_corners)] + [
            ("e", fn(e.interpolate(0.5, normalized=True)))
        ]
        a = sorted(a, key=lambda x: x[1])
        # take the sorted points
        a = [i[0] for i in a]
        # find the edge we just added
        idx = a.index("e")
        # get the label of the corner just before
        return a[idx - 1]

    def extract_pleats(self, pleats, transformed_edges_df):
        result = []
        for pleat in pleats:
            # no idea why this happens yet
            if pleat["id"] == -1:
                res.utils.logger.warning("Skipping pleat with id -1")
                continue

            # get the matching edge
            edge_id = pleat["edge_id"]
            edge_percent = pleat["edge_percent"]
            start_width = pleat["start"] * SPACE_SCALE
            # not entirely sure how this is used yet
            # end_width = pleat["end"] * SPACE_SCALE
            end_vertex = pleat["vertex"]
            end_vertex = self.geom_fix(Point(end_vertex["x"], end_vertex["y"]))

            # get the edge geometry
            edge = transformed_edges_df[transformed_edges_df["key"] == edge_id]
            if edge.empty:
                res.utils.logger.warning(
                    f"Could not find edge {edge_id} for pleat {pleat['id']}"
                )
                continue
            edge = edge.iloc[0]
            edge = edge["geometry"]

            # go percent of the way along the edge
            mid_point = edge.interpolate(edge_percent / 100, normalized=True)

            # now go start_width along and back from that center point for start/end
            mid_dist = edge.project(mid_point)
            start = edge.interpolate(max(mid_dist + start_width / 2, 0))
            end = edge.interpolate(min(mid_dist - start_width / 2, edge.length))

            if pleat["flipped"] == 1:
                start, end = end, start

            # result geometry looks like this
            #        |
            #        |_ end    _
            #        |            '  -  _
            #  edge  |_ mid    _  _  _  _  _ . end_vertex
            #        |                _  -
            #        |_ start  _  -  '
            #        |
            geom = LineString([start, mid_point, end, end_vertex])

            result.append(
                {
                    **pleat,
                    "geometry": geom,
                }
            )

        return result

    def extract_buttons(self, buttons):
        return [
            {
                **button,
                "geometry": self.geom_fix(
                    Point(
                        button.get("position", {}).get("x", 0),
                        button.get("position", {}).get("y", 0),
                    )
                ),
            }
            for button in buttons
        ]

    def extract_special_artworks(self, data):
        return [
            {
                "name": item.get("name", "__NULL__"),
                "width": item.get("width", 0) * SPACE_SCALE,
                "height": item.get("height", 0) * SPACE_SCALE,
                "geometry": self.geom_fix(Point(item.get("x", 0), item.get("y", 0))),
                "position_type": item.get("position_type", "__NULL__"),
                "reference_point_type": item.get("reference_point_type", "__NULL__"),
                "horizontal_alignment": item.get("horizontal_alignment", "__NULL__"),
                "vertical_alignment": item.get("vertical_alignment", "__NULL__"),
                # VStitcher by default rotates all artwork 90 degrees to line up with horiz grain line
                # we rotate everything back to image space (see geom_fix) & need to rotate art angle too
                "radians": round(item.get("radians", 0) - math.pi / 2, 5),
                "tags": item.get("tags", []),
                "shape_id": item.get("shape_id", -1),
                "size_id": item.get("size_id", -1),
                "description": item.get("description", "__NULL__"),
                "container_id": id,
            }
            for id, item in data.items()
        ]

    def shift_all_geometries_to_origin(self, bounds=None):
        bounds = bounds or self.exterior_geometry.bounds
        translation = (-1 * bounds[0], -1 * bounds[1])

        shift = lambda g: translate(g, translation[0], translation[1])
        shift_items = lambda items: [shift(g) for g in items]
        shift_object = lambda o: {**o, "geometry": shift(o["geometry"])}
        shift_objects = lambda items: [shift_object(o) for o in items]

        self.geometry = shift(self.geometry)
        self.exterior_geometry = shift(self.exterior_geometry)

        for df in [
            self.edges_df,
            self.connections_df,
            self.seam_changes_df,
            self.internal_lines_df,
        ]:
            if "geometry" in df.columns:
                df["geometry"] = df["geometry"].map(shift)

        self.seam_guides = shift_items(self.seam_guides)
        self.outer_corners = shift_items(self.outer_corners)
        self.inner_corners = shift_items(self.inner_corners)
        self.corner_reasons = shift_objects(self.corner_reasons)

        self.internal_lines = {
            i: shift_object(g) for i, g in self.internal_lines.items()
        }

        self.placeholders = shift_objects(self.placeholders)
        self.pins = shift_objects(self.pins)
        self.pleats = shift_objects(self.pleats)
        self.buttons = shift_objects(self.buttons)

    def get_seam_allowances_per_edge(self, dxf_edges, transform, plot=False):
        try:
            seam_allowances = []
            chosen_distances = []
            edges = self.edges_df["geometry"].apply(transform).values
            centre_points = [g.interpolate(0.5, normalized=True) for g in edges]
            orig = self.edges_df["geometry"].values

            for e in dxf_edges:
                # center of my edge
                pt = e.interpolate(0.5, normalized=True)

                # find distances to all the other vs edges
                distances = [pt.distance(k) for k in centre_points]

                chosen_distances.append(min(distances))
                i = np.argmin(distances)
                seam_allowance = self.edges_df.iloc[i]["seam_allowance"]
                seam_allowances.append(seam_allowance)

            if plot:
                import geopandas as gpd

                print(self.key)
                print(chosen_distances)
                print(seam_allowances)

                edges = unary_union(edges)
                dxf_edges = unary_union(dxf_edges)
                orig = unary_union(orig)
                centre_points = unary_union(centre_points)

                gdf = gpd.GeoDataFrame(
                    {
                        "set": ["orig", "my edges", "dxf edges", "points"],
                        "geometry": [orig, edges, dxf_edges, centre_points],
                    }
                )
                ax = gdf.plot(categorical=True, column="set", legend=True)
                ax.set_title(self.key)
                ax.invert_yaxis()

            return seam_allowances
        except:
            res.utils.logger.debug(f"Failed on {dxf_edges}, {self.key} ")
            raise

    @staticmethod
    def points_to_array(points):
        return [[p["x"], p["y"]] for p in points]

    def lines_as_df(self, edges):
        if not edges:
            return pd.DataFrame()

        df = pd.DataFrame(edges)
        # drop any "lines" that only have one point (KT-6079 I'm looking at you)
        df = df.drop(df[df["points"].str.len() < 2].index)
        df["geometry"] = df["points"].map(lambda x: LineString(self.points_to_array(x)))
        return df

    def seam_changes_as_df(self, connections_df):
        """A redefinition of "corners" to be where a seam code or connected piece changes"""

        # TODO: may need to ensure all the geoms are in anticlockwise order

        stitch_changes = []
        last = connections_df.iloc[-1]
        for i in range(len(connections_df)):
            this = connections_df.iloc[i]
            this_props = this.get("raw_properties", None)
            last_props = last.get("raw_properties", None)
            this_connected_shape = this.get("shape_name", None)
            last_connected_shape = last.get("shape_name", None)

            if this_props != last_props or this_connected_shape != last_connected_shape:
                point = this["points"][0]
                # and edge can be connected to multiple other pieces so make sure
                # the point doesn't exist already
                if not any(
                    [
                        p["x"] == point["x"] and p["y"] == point["y"]
                        for p in stitch_changes
                    ]
                ):
                    stitch_changes.append(point)
            last = this

        if stitch_changes:
            cdf = pd.DataFrame.from_records(
                [
                    {
                        "points": stitch_changes,
                        "shape_id": -2,
                        "shape_edge_id": -2,
                        "shape_name": "stitch_change",
                        "seam_code": "",
                        "geometry": MultiPoint(self.points_to_array(stitch_changes)),
                    }
                ]
            )
        else:
            cdf = pd.DataFrame(
                columns=[
                    "points",
                    "shape_id",
                    "shape_edge_id",
                    "shape_name",
                    "seam_code",
                    "geometry",
                ]
            )

        return cdf

    def connections_as_df(self, edges):
        """
        Each edge will have a list of connections to other edges of another piece - which
        could be itself.

        This function will create a row per connection in a dataframe
        """
        result = []
        for edge in edges:
            # ignore edges with a single point
            if len(edge["points"]) < 2:
                continue

            line = LineString(self.points_to_array(edge["points"]))
            # changed this next line, looks nicer if the overall edge isn't shown when it has connections
            # result.append({**edge, "geometry": line, "linewidth": 1}) # always add the edge on its own

            # add each connection, but use the markers to determine how much of the edge
            # use the index as a linewidth (just for plotting)
            def abs_length(connection):
                return abs(
                    connection["from_start_percent"] - connection["from_end_percent"]
                )

            connections = edge.get("connections", [])
            connections.sort(key=abs_length)
            if connections:
                for i, connection in enumerate(connections):
                    start = connection["from_start_percent"] / 100
                    end = connection["from_end_percent"] / 100

                    if start > end:
                        start, end = end, start

                    start = max(0, start)
                    end = min(1, end)

                    subline = substring(
                        line,
                        start,
                        end,
                        normalized=True,
                    )

                    result.append(
                        {**edge, **connection, "geometry": subline, "linewidth": i + 1}
                    )
            else:
                result.append({**edge, "geometry": line, "linewidth": 1})

        df = pd.DataFrame(result).replace(np.nan, -1)

        if "shape_name" in df:
            df["shape_name"].replace(-1, "<no connection>", inplace=True)
        else:
            df["shape_name"] = "<no connection>"

        return df

    @staticmethod
    def angle(p1, p2, p3):
        x1, y1 = p1
        x2, y2 = p2
        x3, y3 = p3
        deg1 = (360 + np.degrees(np.arctan2(x1 - x2, y1 - y2))) % 360
        deg2 = (360 + np.degrees(np.arctan2(x3 - x2, y3 - y2))) % 360

        return deg2 - deg1 if deg1 <= deg2 else 360 - (deg1 - deg2)

        return np.arctan2(p3[1] - p2[1], p3[0] - p2[0]) - np.arctan2(
            p1[1] - p2[1], p1[0] - p2[0]
        )

    @staticmethod
    def append_if_not_none(list, element):
        if element is not None:
            list.append(element)

    """
        Each edge can have multiple connections to other pieces e.g. two dress panels
        connect but a label is stitched in there too. This checks if the main connection
        changes between edges, "main" meaning the longest stitch so e.g. the label is ignored
    """

    def edges_connected_to_same_piece(self, edge1, edge2):
        if "connections" not in edge1 or "connections" not in edge2:
            return None  # it's an older pieces.json so I don't know!

        def main_connections(edge):
            try:
                connection_extents = np.asarray(
                    [
                        # get the absolute as VS isn't guaranteed to be in order
                        abs(c["from_end_percent"] - c["from_start_percent"])
                        for c in edge["connections"]
                    ]
                )
                max_connection = np.max(connection_extents)
                main_connection_indexes = np.flatnonzero(
                    connection_extents == max_connection
                )
                return [
                    edge["connections"][c]["shape_name"]
                    for i, c in enumerate(list(main_connection_indexes))
                    if i in main_connection_indexes
                ]
            except:
                return []

        edge1_connections = main_connections(edge1)
        edge2_connections = main_connections(edge2)

        return any(
            [shape_name in edge2_connections for shape_name in edge1_connections]
        )

    def get_next_point_that_is_not(self, line, point, reverse=False, min_cm=0.2):
        """
        some edges have multiple points very close to each other at the ends
        this will find the first point that differs from the last point
        by a tiny amount - parallel_offset fails otherwise
        """
        coords = line.coords[::-1] if reverse else line.coords
        point = Point(point)
        threshold = min_cm * SPACE_SCALE

        return next((x for x in coords if Point(x).distance(point) > threshold), None)

    def edge_will_be_folded(self, edge):
        # the only way to definitively know if an edge will be folded is if browzwear
        # allow access to the "Allowance Corner" of a point, in the UI it can be set
        # to Regular, Square or Foldback, the latter being the one we want for refracting

        return False

    def add_inter_edges_seam_guides(
        self, last, edge, angles, hinges, plot_workings=False
    ):
        [last_sa, curr_sa] = [last["seam_allowance"], edge["seam_allowance"]]

        b = edge["geometry"].coords[0]

        # to handle VS edges with the same point duplicated at the end
        # a = next((x for x in last["geometry"].coords[::-1] if x != b), None)
        # c = next((x for x in edge["geometry"].coords if x != b), None)
        a = self.get_next_point_that_is_not(last["geometry"], b, reverse=True)
        c = self.get_next_point_that_is_not(edge["geometry"], b)

        if not a or not c:
            return

        # rad = self.angle(a, b, c) % math.pi
        # deg = 180 - np.degrees(rad)
        deg = self.angle(a, b, c)

        is_sharp = deg < 140
        seam_allowance_changed = last_sa != curr_sa

        # nb: I don't use connection_changed to determine if it's a corner or not
        # but it's useful information for the future
        connection_changed = not self.edges_connected_to_same_piece(last, edge)

        if is_sharp or seam_allowance_changed:
            hinge = LineString([a, b, c])

            angles.append(deg)
            hinges.append(hinge)

            grouping = self.add_seam_guides(
                hinge, last_sa, curr_sa, save=True, plot=plot_workings
            )
            if not grouping:
                return

            outer_corner = grouping["outer_corner"]
            grouping["type"] = "inter_edge"

            # defend argument that this is a corner
            self.seam_guide_groupings.append(grouping)
            self.corner_reasons.append(
                {
                    "geometry": outer_corner,
                    "is_sharp": is_sharp,
                    "seam_allowance_changed": seam_allowance_changed,
                    "connection_changed": connection_changed,
                }
            )

    def add_intra_edge_seam_guides(self, edge, angles, hinges, plot_workings=False):
        points = edge["geometry"].simplify(0.2 * SPACE_SCALE).coords
        sa = edge["seam_allowance"]

        for i in range(1, len(points) - 1):
            prev = points[i - 1]
            curr = points[i]
            next = points[i + 1]

            # rad = self.angle(prev, curr, next)
            # deg = 180 - np.degrees(rad) % 180
            deg = self.angle(prev, curr, next)

            is_sharp = 50 < deg < 140

            if is_sharp:
                hinge = LineString([prev, curr, next])

                angles.append(deg)
                hinges.append(hinge)

                grouping = self.add_seam_guides(
                    hinge, sa, sa, save=True, plot=plot_workings
                )
                if not grouping:
                    return

                outer_corner = grouping["outer_corner"]
                grouping["type"] = "intra_edge"

                # defend argument that this is a corner
                self.seam_guide_groupings.append(grouping)
                self.corner_reasons.append(
                    {
                        "geometry": outer_corner,
                        "is_sharp": is_sharp,
                        "seam_allowance_changed": False,
                        "connection_changed": False,
                        "angle": deg,
                        "intra_edge": True,
                    }
                )

    def add_seam_guides(
        self,
        hinge,
        prev_seam_allowance,
        curr_seam_allowance,
        save=True,
        plot=False,
    ):
        result = self.get_seam_guides_and_outer_corner(
            hinge, prev_seam_allowance, curr_seam_allowance, plot
        )

        if not result:
            return None

        [seam_guide1, seam_guide2, outer_corner, grouping] = result

        if not self.grouping_valid(grouping):
            return None

        if save:
            self.append_if_not_none(self.inner_corners, Point(hinge.coords[1]))
            self.append_if_not_none(self.outer_corners, outer_corner)
            self.append_if_not_none(self.seam_guides, seam_guide1)
            self.append_if_not_none(self.seam_guides, seam_guide2)

        return grouping

    def determine_corners_and_seam_guides(self, plot=False, plot_workings=False):
        self.seam_guides = []
        self.outer_corners = []
        self.inner_corners = []
        self.corner_reasons = []
        self.seam_guide_groupings = []

        # print(self.key)
        angles = []
        hinges = []

        edges = self.edges_df.iloc

        last = edges[-1]
        for edge in edges:
            self.add_intra_edge_seam_guides(edge, angles, hinges, plot_workings)
            self.add_inter_edges_seam_guides(last, edge, angles, hinges, plot_workings)

            last = edge

        # if there are absolutely no corners, forget about sharpness and seam allowance
        # changes, extend a line from the start point to where it crosses the outer edge
        if len(self.seam_guides) == 0 and edge.get("seam_allowance", 0) > 0:
            [seam_guide1, seam_guide2] = self.get_last_resort_seam_guide(plot)

            self.append_if_not_none(self.seam_guides, seam_guide1)
            self.append_if_not_none(self.seam_guides, seam_guide2)

        if plot:
            import geopandas as gpd

            gdf = gpd.GeoDataFrame(
                {
                    "title": ["base"]
                    + angles
                    + (["outer corners"] * len(self.outer_corners))
                    + (["seam guides"] * len(self.seam_guides)),
                    "geometry": [self.geometry]
                    + hinges
                    + self.outer_corners
                    + self.seam_guides,
                }
            )
            ax = gdf.plot(column="title", legend=True, figsize=(20, 20))

            # move the legend
            ax.get_legend().set_bbox_to_anchor((2, 1))
            # make the image bigger
            ax.figure.set_size_inches(10, 10)
            ax.set_title(f"{self.key} - Corner count: {len(self.outer_corners)}")
            ax.invert_yaxis()

    def grouping_valid(self, grouping):
        # make sure the grouping is valid
        if (
            grouping
            and grouping["edge1_seam_guide"]
            and grouping["edge2_seam_guide"]
            and grouping["outer_corner"]
        ):
            return True

        return False

    def parallelogram_from_seam_guide_grouping(self, grouping):
        if not self.grouping_valid(grouping):
            return None

        # make a parallelogram from the grouping
        edge1_seam_guide = grouping["edge1_seam_guide"]
        edge2_seam_guide = grouping["edge2_seam_guide"]
        outer_corner = grouping["outer_corner"]

        # seam guides -dotted- go from outside to inside, this is the bottom right piece corner
        # seam guide 1 ---------------v
        #                  (b or e)........a
        #                        .'       /
        # seam guide 2 ------> .'       /
        #               ____d.'_______/c (outer_corner)
        #
        [a, b] = edge1_seam_guide.coords
        c = outer_corner
        [d, e] = edge2_seam_guide.coords

        if a and b and c and d and e:
            return Polygon([b, a, c, d, e])

        return None

    def seam_guide_overlaps_well(self, parallelogram, outpoly, plot_workings=False):
        # we use a parallelogram to check if the seam guides and the corner
        # overlap well with the outline
        #
        # there are 2 ways the overlap can fail, one where it's supposed to flare out
        # and one where it's not
        #
        # 1. should flare in (the dot is the seam guide's guess at the corner):
        #    _____\_\                      __                   __
        #    ______\/.    parallelogram is \_\  intersection is \/
        # For this the check is easy, if the intersection of the parallelogram and the
        # outline is the same size as the parallelogram, then it's good! else bad.
        intersection = outpoly.intersection(parallelogram)
        should_flare_in = not math.isclose(
            intersection.area, parallelogram.area, rel_tol=0.03
        )

        # 2. should flare out:
        #    ______/_/                      __                  __
        #    _____/_.\    parallelogram is /_/  intersection is /_/ extra space .\
        # Here the intersection of the parallelogram and the outline is the same size
        # as the parallelogram, but there's a bit of space to the right so the first check
        # doesn't cover this. Let's look to the right and underneath to see if there's
        # anything extra there. If there is, then it's good, else bad.
        points = parallelogram.exterior.coords

        def dxf_area_outside(s, e):
            line = LineString([points[s], points[e]])
            rotated_around_s = affinity.rotate(line, -90, origin=points[s])
            rotated_around_e = affinity.rotate(line, 90, origin=points[e])

            [a, b, c, d] = rotated_around_s.coords[:] + rotated_around_e.coords[:]
            check = Polygon([a, b, c, d, a])

            outside = outpoly.intersection(check)

            return [
                not math.isclose(check.area, check.area - outside.area, rel_tol=0.03),
                check,
                outside,
            ]

        [flare_out_on_1, check_1, intersection_1] = dxf_area_outside(2, 1)
        [flare_out_on_2, check_2, intersection_2] = dxf_area_outside(3, 2)

        if plot_workings:
            import geopandas as gpd

            titles = ["parallelogram"] + ["check"] * 2
            geometry = [parallelogram, check_1, check_2]

            if should_flare_in:
                titles += ["flare in"]
                geometry += [intersection]

            if flare_out_on_1:
                titles += ["flare out on 1"]
                geometry += [intersection_1]

            if flare_out_on_2:
                titles += ["flare out on 2"]
                geometry += [intersection_2]

            gdf = gpd.GeoDataFrame({"titles": titles, "geometry": geometry})
            ax = gdf.plot(column="titles", legend=True, figsize=(2, 2))
            ax.get_legend().set_bbox_to_anchor((2, 1))
            ax.invert_yaxis()

        should_flare_out = flare_out_on_1 or flare_out_on_2

        # it overlaps well if it shouldn't have flared in or out
        return not (should_flare_in or should_flare_out)

    def refract_seam_guide(self, seam_guide_grouping, which=1, plot=False):
        # if the seam allowance on an edge is folded back on itself somehow
        # we need to 'refract' the angle to make it align with how VS presents it
        #
        #                            / /
        #   inner design space -->  / / <-- outer cut line (with seam allowance)
        #                    ______/ /
        #               hem  ______A_\  <-- this flares out because of the fold
        #
        #   the 'A' represents the choice of normal and refracted seam guides /-\
        #
        #   the normal seam guide / would continue the right edge onwards
        #   and the cut line wouldn't flare out when seam allowance is added
        #                    ______/ /
        #               hem  _____/_/
        #
        #   the refracted seam guide \ gets refracted by the hem and flares out the
        #   cut line when seam allowance is added
        #                    ______/ /
        #               hem  ______\_\
        #
        edge1_seam_guide = seam_guide_grouping["edge1_seam_guide"]
        edge2_seam_guide = seam_guide_grouping["edge2_seam_guide"]

        [a, b, c] = edge1_seam_guide.coords[:] + [edge2_seam_guide.coords[0]]
        deg = self.angle(a, b, c)
        refracted_deg = 180 - 2 * deg

        if which == 1:
            a = rotate(Point(a), refracted_deg, origin=Point(b))
        else:
            c = rotate(Point(c), -refracted_deg, origin=Point(b))

        # hinges are normally the edges not the seam_guides but we can infer them...
        hinge = scale(LineString([a, b, c]), xfact=-1, yfact=-1, origin=Point(b))

        last_sa = seam_guide_grouping["edge1_seam_allowance"]
        curr_sa = seam_guide_grouping["edge2_seam_allowance"]

        return self.add_seam_guides(hinge, last_sa, curr_sa, save=False, plot=plot)

    def rectify_seam_guides_using_dxf_outline(
        self, outline, plot=False, plot_workings=False
    ):
        """
            VStitcher doesn't give the outline or the corner type (e.g. if it's a foldback)
            but given the outline we can fuzzily work out that there should be a refraction
        
            Let's look at the bottom right corner of a shirt
            A normal outline WITHOUT any foldback looks like this:
                    ______/ /                                         ______/_/
               hem  _______/  ... and adding seam guides results in:  _____/_/ 

            whereas an outling WITH a foldback looks like this:            
                    ______/ /                                         ______/_/
               hem  ________\  ... and adding seam guides results in: _____/__\

                                                                       ______/_/
                               ... but it should be:                   ______\_\ 
 
            The seam guides and the corner should form a parallelogram when intersected
            with the outline, if not, we can try refracting either seam guide
        """
        outpoly = Polygon(outline)
        good = []
        bad = []
        fixed = []
        final = []
        for grouping in self.seam_guide_groupings:
            # just look at inter edge, shouldn't be foldbacks in the middle of an edge (intra_edge)
            if grouping["type"] == "intra_edge":
                final.append(grouping)
                continue

            try:
                edge1_seam_allowance = grouping["edge1_seam_allowance"]
                edge2_seam_allowance = grouping["edge2_seam_allowance"]

                if edge1_seam_allowance == 0 or edge2_seam_allowance == 0:
                    final.append(grouping)
                    continue

                parallelogram = self.parallelogram_from_seam_guide_grouping(grouping)

                if not parallelogram:
                    bad.append(parallelogram)
                elif not self.seam_guide_overlaps_well(
                    parallelogram, outpoly, plot_workings
                ):
                    bad.append(parallelogram)

                    # try refracting either seam guide to see if that fixes it
                    grouping_try1 = self.refract_seam_guide(grouping, 1, plot_workings)
                    if grouping_try1:
                        try1 = self.parallelogram_from_seam_guide_grouping(
                            grouping_try1
                        )
                        if try1 and self.seam_guide_overlaps_well(
                            try1, outpoly, plot_workings
                        ):
                            fixed.append(try1)
                            final.append(grouping_try1)
                            continue

                    grouping_try2 = self.refract_seam_guide(grouping, 2, plot_workings)
                    if grouping_try2:
                        try2 = self.parallelogram_from_seam_guide_grouping(
                            grouping_try2
                        )
                        if try2 and self.seam_guide_overlaps_well(
                            try2, outpoly, plot_workings
                        ):
                            fixed.append(try2)
                            final.append(grouping_try2)
                            continue
                else:
                    good.append(parallelogram)
            except Exception as e:
                res.utils.logger.warn(
                    f"Error rectifying seam guides for {self.key}: {e}"
                )
            finally:
                final.append(grouping)

        # replace the seam guide groupings with the final list
        self.seam_guides = []
        self.outer_corners = []
        self.inner_corners = []
        self.seam_guide_groupings = final
        for grouping in final:
            self.seam_guides.append(grouping["edge1_seam_guide"])
            self.seam_guides.append(grouping["edge2_seam_guide"])
            self.outer_corners.append(grouping["outer_corner"])
            self.inner_corners.append(grouping["inner_corner"])

        if plot:
            import geopandas as gpd

            gdf = gpd.GeoDataFrame(
                {
                    "title": ["outline"]
                    + [f"good (x{len(good)})"] * len(good)
                    + [f"bad (x{len(bad)})"] * len(bad)
                    + [f"fixed (x{len(fixed)})"] * len(fixed),
                    "geometry": [outline] + good + bad + fixed,
                }
            )
            ax = gdf.plot(column="title", legend=True, figsize=(10, 10))
            ax.get_legend().set_bbox_to_anchor((2, 1))
            ax.set_title(f"{self.key} - seam guides rectified using outline")
            ax.invert_yaxis()

    @staticmethod
    def extended_line_intersection(a, b, needs_to_be_after_a=True):
        """
        extend a and b until they intersect

            x .. .. .. <--- b
            :
            :
            ^
            |
            a

            *** a and b should be directed ***

            so they intersect in the same direction they point
            this is so the intersection of concave edges/offsets are ignored
        """

        def slope_and_intercept(line):
            x1, y1 = line.coords[0]
            x2, y2 = line.coords[1]

            if x1 == x2:
                return None, x1

            m = (y2 - y1) / (x2 - x1)
            c = y1 - m * x1
            return m, c

        # get the slope & intercept of each line
        am, ac = slope_and_intercept(a)
        bm, bc = slope_and_intercept(b)

        # let's say if the lines are parallel there's no intersection
        if am == bm:
            return None

        if am is None:  # a is vertical, so where does b cross the vertical line at ac
            intersection = Point(ac, bm * ac + bc)
        elif bm is None:  # b is vertical, so where does a cross the vertical line at bc
            intersection = Point(bc, am * bc + ac)
        else:
            # solve for x & y
            x = (bc - ac) / (am - bm)
            y = (am * x) + ac
            intersection = Point(x, y)

        # check that the intersection is further along the edge than the start point
        # rotate the edge perpendicualr to the end point
        perp = rotate(a, -90, origin=a.coords[-1])

        # and see what side of the perpendicular the intersection is on
        def is_left(p1, p2, p3):
            # return (p2.x - p1.x) * (p3.y - p1.y) - (p2.y - p1.y) * (p3.x - p1.x) > 0
            return (p2[0] - p1[0]) * (p3[1] - p1[1]) - (p2[1] - p1[1]) * (
                p3[0] - p1[0]
            ) > 0

        # which side of perp is the intersection?
        if not needs_to_be_after_a or is_left(
            perp.coords[0], perp.coords[1], intersection.coords[0]
        ):
            return intersection

        return None

    def get_seam_guides_and_outer_corner(
        self, hinge, edge1_seam_allowance, edge2_seam_allowance, plot
    ):
        LIMIT = 20 * SPACE_SCALE  # if a guide is over 20cm away ignore it
        coords = hinge.coords
        h = Point(hinge.coords[1])

        a = Point(coords[0])
        b = Point(coords[1])
        c = Point(coords[2])

        edge1_parallel = None
        edge2_parallel = None

        if edge1_seam_allowance == 0 and edge2_seam_allowance == 0:
            # if BOTH seam allowances are 0, draw tiny ones so there are always a pair
            # of seam guides at every sharp corner
            edge1_seam_guide = substring(LineString([b, a]), 0, 0.1 * SPACE_SCALE)
            edge2_seam_guide = substring(LineString([b, c]), 0, 0.1 * SPACE_SCALE)

            corner = Point(b)
        elif edge1_seam_allowance == 0 or edge2_seam_allowance == 0:
            # if either of the seam allowances are 0, you can't fit the seam guide in
            # so instead draw a seam guide on the other edge's seam allowance
            #
            #   Normal                  One has SA = 0
            #   (both have SA)          flip the vertical guide, carry on horizontal one
            #
            #   ________  curr edge      ... ___c  curr edge
            #  |...:___c  <-.63 SA      |   :b      <- 0 SA
            #  |   |b                   |   |
            #  |   a                    |   a
            # last                     last
            #
            (point_to_flip, point_to_extend) = (
                (a, c) if edge2_seam_allowance == 0 else (c, a)
            )

            flip_line = LineString([b, point_to_flip])
            fact = 0.5 * SPACE_SCALE / flip_line.length
            flip_line = scale(flip_line, xfact=fact, yfact=fact, origin=b)

            extended_line = LineString([b, point_to_extend])
            fact = (
                -(edge1_seam_allowance or edge2_seam_allowance)
                * SPACE_SCALE
                / extended_line.length
            )
            extended_line = scale(extended_line, xfact=fact, yfact=fact, origin=b)

            edge1_seam_guide = flip_line
            edge2_seam_guide = extended_line

            corner = Point(extended_line.coords[-1])
        else:
            # orient the edges so they are pointing to the center of the hinge h
            #
            #          h <-- e1
            #          ^
            #          | e2
            edge1 = LineString([coords[0], h])
            edge2 = LineString([coords[2], h])  # point at the hinge center

            # get an offset for each edge using the seam allowance
            #
            #   c      b  <-- e1_parallel
            #   a      h  <-- e1
            #   ^      ^
            #   | e2_p | e2
            edge1_parallel = edge1.parallel_offset(
                edge1_seam_allowance * SPACE_SCALE, "right"
            )
            edge2_parallel = edge2.parallel_offset(
                edge2_seam_allowance * SPACE_SCALE, "left"
            )

            # offset to the right will switch the direction, switch it back
            edge1_parallel = scale(edge1_parallel, xfact=-1, yfact=-1)

            if edge1_parallel.is_empty or edge2_parallel.is_empty:
                return

            # def truncate_fallback(line):
            #     return substring(line, line.length - 0.5 * SPACE_SCALE, line.length)

            # where edge1 intersects with edge2_parallel is where the seam guide should start
            edge1_seam_guide = None
            edge1_seam_start = self.extended_line_intersection(
                edge1, edge2_parallel
            )  # b
            if edge1_seam_start:
                edge1_seam_guide = LineString([edge1_seam_start, edge1.coords[-1]])
                if edge1_seam_guide.length > LIMIT:
                    return

            # and vice versa
            edge2_seam_guide = None
            edge2_seam_start = self.extended_line_intersection(
                edge2, edge1_parallel
            )  # a
            if edge2_seam_start:
                edge2_seam_guide = LineString([edge2_seam_start, edge2.coords[-1]])
                if edge2_seam_guide.length > LIMIT:
                    return

            # the corner is where the two offset edges intersect
            corner = self.extended_line_intersection(
                edge1_parallel, edge2_parallel, needs_to_be_after_a=False
            )  # c

            # if it's too far away, just position the shortest seam_allowance at 90 degrees
            # hack to match one of the trickier pieces
            if corner:
                if LineString([corner, coords[1]]).length > LIMIT:
                    if edge1_seam_allowance < edge2_seam_allowance:
                        sa = edge1_seam_allowance
                        edge = edge1
                    else:
                        sa = edge2_seam_allowance
                        edge = edge2

                    fact = sa / edge.length
                    edge = scale(edge, xfact=fact, yfact=fact, origin=h)
                    edge = rotate(edge, -90, origin=h)
                    corner = Point(edge.coords[0])

                    edge1_parallel = None
                    edge2_parallel = None

        if plot:
            import geopandas as gpd

            gdf = gpd.GeoDataFrame(
                {
                    "title": [
                        "hinge",
                        "edge1_parallel",
                        "edge2_parallel",
                        "edge_1_seam_guide",
                        "edge_2_seam_guide",
                        "corner",
                    ],
                    "geometry": [
                        hinge,
                        edge1_parallel,
                        edge2_parallel,
                        edge1_seam_guide,
                        edge2_seam_guide,
                        corner,
                    ],
                }
            )
            ax = gdf.plot(column="title", legend=True)
            ax.get_legend().set_bbox_to_anchor((2, 1))
            ax.invert_yaxis()

        grouping = {
            "edge1_seam_allowance": edge1_seam_allowance,
            "edge2_seam_allowance": edge2_seam_allowance,
            "edge1_seam_guide": edge1_seam_guide,
            "edge2_seam_guide": edge2_seam_guide,
            "inner_corner": b,
            "outer_corner": corner,
        }

        return [edge1_seam_guide, edge2_seam_guide, corner, grouping]

    def get_last_resort_seam_guide(self, plot=False):
        """
        this usually happens if the shape is circular with no seam allowance changes.
        we'll draw a line from the start point out to the outer geometry on both sides
        here's a circlular piece (*) with seam allowance of 1cm (~)

             . ~ ~ ~ .
           ~           ~
         A-----* X *-----B
        ~    *       *    ~
        ~    *       *    ~
        ~    *       *    ~
         ~     * * *      ~
           ~            ~
             ' ~ ~ ~ '

        We'll extend a line from X out to A and B, and use that as the seam guides
        """
        try:
            edge = self.edges_df.iloc[0]
            seam_allowance = edge["seam_allowance"]
            curve = self.geometry.parallel_offset(seam_allowance * SPACE_SCALE, "right")

            def get_intersection(line):
                factor = 100 * SPACE_SCALE
                line = scale(line, xfact=factor, yfact=factor, origin=line.coords[0])
                return line.intersection(curve)

            p1 = edge["geometry"].coords[0]
            p2 = edge["geometry"].coords[1]
            line = LineString([p1, p2])
            a = get_intersection(line)
            b = get_intersection(scale(line, xfact=-1, yfact=-1, origin=p1))

            guide_1 = LineString([a, p1])
            guide_2 = LineString([b, p1])

            if plot:
                import geopandas as gpd

                gdf = gpd.GeoDataFrame(
                    {
                        "title": [
                            "inner",
                            "outer",
                            "line",
                            "a",
                            "b",
                            "guide_1",
                            "guide_2",
                        ],
                        "geometry": [
                            self.geometry,
                            curve,
                            line,
                            a,
                            b,
                            guide_1,
                            guide_2,
                        ],
                    }
                )
                ax = gdf.plot(column="title", legend=True)
                ax.get_legend().set_bbox_to_anchor((2, 1))
                ax.invert_yaxis()

            return [LineString([a, p1]), LineString([b, p1])]
        except:
            return [None, None]

    def get_exterior(self):
        """
        This approximates the exterior of the shape by offsetting each edge by its
        seam allowance and including the corners.
            "It's not perfect, but it's good"
                        - GitHub CoPilot
        Useful for scaling a DXF piece so they match well, if we just shift geometry
        to origin you might get:

             _______________
            |               |
            |__________     |
            |          |    |
            |__________|____|

        And applying a scale_to might change the aspect.

        Whereas what we want is

             _______________
            |   __________  |
            |  |          | |
            |  |__________| |
            |_______________|

        """

        def offset(row):
            return row["geometry"].parallel_offset(
                row["seam_allowance"] * SPACE_SCALE, "right", join_style=2
            )

        df = self.edges_df
        offset_edges = list(df.apply(offset, axis=1).values)

        exterior = offset_edges + self.outer_corners + [self.geometry]
        exterior = unary_union(exterior)

        return exterior

    def deduce_geometry_attributes(self):
        if self.grain_line_degrees is None:
            return ["--No Grain Line--"]

        try:
            # the humon looks at the geom in screen space so we'll transform it here
            # so that the code below for any symmetry or folds are logical e.g.
            # NOT x_sym = is_symmetrical_on_y_axis
            shape = rotate(self.geometry, -90).simplify(0.1)

            centre_point = shape.centroid
            centred_shape = list(
                polygonize(translate(shape, -centre_point.x, -centre_point.y))
            )[0]

            # rotate the shape by the grain_line_degrees
            centred_shape = rotate(centred_shape, round(self.grain_line_degrees, 1))

            def mostly_overlaps(other):
                intersection = centred_shape.intersection(other)
                return math.isclose(
                    1, intersection.area / centred_shape.area, rel_tol=0.001
                )

            is_x_symmetrical = (
                "x-symmetrical"
                if mostly_overlaps(scale(centred_shape, xfact=-1))
                else None
            )
            is_y_symmetrical = (
                "y-symmetrical"
                if mostly_overlaps(scale(centred_shape, yfact=-1))
                else None
            )

            centred_shape = rotate(centred_shape, 45)
            is_geo_45 = None
            if (not is_x_symmetrical and not is_y_symmetrical) and (
                mostly_overlaps(scale(centred_shape, xfact=-1))
                or mostly_overlaps(scale(centred_shape, yfact=-1))
            ):
                is_geo_45 = "45Bias"

            # check the fold lines
            fold_lines = [
                l for l in self.internal_lines.values() if "fold" in l["type_text"]
            ]
            center_fold_x_axis = None
            center_fold_y_axis = None
            for fold_line in fold_lines:
                # humon looks at this rotated so lets do the same so the code matches (as above)
                points = rotate(fold_line["geometry"], 90, origin=shape.centroid).coords

                # so long as the points are within 0.5cm we'll assume they are vertical/horizontal
                tolerance = 0.5 * SPACE_SCALE

                if not center_fold_x_axis and all(
                    [
                        math.isclose(p[0], centre_point.x, abs_tol=tolerance)
                        for p in points
                    ]
                ):
                    center_fold_x_axis = "Center Fold X-Axis"

                if not center_fold_y_axis and all(
                    [
                        math.isclose(p[1], centre_point.y, abs_tol=tolerance)
                        for p in points
                    ]
                ):
                    center_fold_y_axis = "Center Fold Y-Axis"

            # if there's a fold line, and some edge connects back to this shape, it's fold interior
            fold_interior = None
            if center_fold_x_axis or center_fold_y_axis:
                if self.key in self.connections_df["shape_name"].values:
                    fold_interior = "Fold -> Interior"

            interior = "Interior" if self.name.piece_type in ("L", "LN") else None

            atts = list(
                filter(
                    lambda x: x is not None,
                    [
                        fold_interior,
                        interior,
                        is_x_symmetrical,
                        is_y_symmetrical,
                        # is_45_bias,
                        is_geo_45,
                        center_fold_x_axis,
                        center_fold_y_axis,
                    ],
                )
            )

            return atts
        except Exception as e:
            res.utils.logger.warn(f"Could not deduce sew attr for {self.key}: {e}")
            return ["--Geometry Error--"]

    def get_sew_identity_symbol_id_old(self):
        def partially_match_key(dict, key):
            return next(iter([v for k, v in dict.items() if k in key]), None)

        top_or_under = partially_match_key(
            {"TP": "Top Piece", "UN": "Under Piece"}, self.name.identity
        )

        front_or_back = (
            None
            if top_or_under
            else partially_match_key({"FT": "Front", "BK": "Back"}, self.name.part)
        )

        left_or_right = partially_match_key(
            {"LF": "Left", "RT": "Right"}, self.name.identity
        )

        neutral = None
        if not left_or_right and not top_or_under:
            neutral = "Neutral"

        interior = None
        if (
            self.name.piece_type in ("LN", "L")
            or "Fold -> Interior" in self.geometry_attributes
        ):
            interior = "- Interior"

        # just matching the way it is on airtable
        if not front_or_back and not top_or_under:
            if left_or_right:
                left_or_right = left_or_right + " Piece"
            if neutral:
                neutral = neutral + " Piece"

        key = " ".join(
            list(
                filter(
                    lambda x: x,
                    [front_or_back, left_or_right, top_or_under, neutral, interior],
                )
            )
        )

        return key

    def get_sew_identity_symbol_id(self):
        # get the value where dict key k is a substring of the key e.g. "LF" in "LFTP"
        def v_where_k_in(dict, key):
            return next(iter([v for k, v in dict.items() if k in key]), None)

        # X, Y, Z ordering, left or right, top or under, front or back - hopefully logical
        left_or_right = v_where_k_in({"LF": "left", "RT": "right"}, self.name.identity)
        top_or_under = v_where_k_in({"TP": "top", "UN": "under"}, self.name.identity)
        front_or_back = v_where_k_in({"FT": "front", "BK": "back"}, self.name.part)

        # convention seems to be that if it's top or under never show front/back
        front_or_back = None if top_or_under else front_or_back

        is_lining = self.name.piece_type in ("LN", "L")
        has_fold_interior = "Fold -> Interior" in self.geometry_attributes
        interior = "interior" if is_lining or has_fold_interior else None

        neutral = None
        if not left_or_right and not top_or_under and not interior:
            neutral = "neutral"

        key = "_".join(
            list(
                filter(
                    lambda x: x,
                    [left_or_right, top_or_under, front_or_back, neutral, interior],
                )
            )
        )

        return key

    def placeholder_box(self, placeholder):
        point = placeholder["geometry"]
        width = placeholder["width"]
        height = placeholder["height"]
        # it's centred so
        # box = LineString([[0, 0], [width, 0], [width, height], [0, height], [0, 0]])
        box = LineString(
            [
                [-width / 2, -height / 2],
                [width / 2, -height / 2],
                [width / 2, height / 2],
                [-width / 2, height / 2],
                [-width / 2, -height / 2],
            ]
        )

        box = translate(box, xoff=point.x, yoff=point.y)
        box = rotate(box, placeholder["radians"], origin=point, use_radians=True)

        return box

    def plot(self):
        import geopandas as gpd

        # format to label, category (for legend), geometry, linewidth for each element
        xdf = self.connections_df
        xdf["label"] = (
            xdf["seam_code"].replace(-1, "")
            + " "
            + xdf["seam_allowance"].round(2).astype(str)
            + " ["
            + xdf["key"].astype(str)
            + "]"
        )
        xdf["category"] = xdf["shape_name"]
        xdf["geom_size"] = xdf["linewidth"]
        xdf = xdf[["label", "category", "geometry", "geom_size"]]

        sdf = self.seam_changes_df
        sdf["category"] = "stitch change"
        sdf["geom_size"] = 2
        sdf = sdf[["category", "geometry"]]

        ndf = pd.DataFrame.from_dict(
            {
                "category": ["seam guide"] * len(self.seam_guides),
                "geometry": self.seam_guides,
                "geom_size": [2] * len(self.seam_guides),
                "label": range(len(self.seam_guides)),
            }
        )
        cdf = pd.DataFrame.from_dict(
            {
                "category": ["corner"] * len(self.outer_corners),
                "geometry": self.outer_corners,
                "geom_size": [4] * len(self.outer_corners),
            }
        )

        pdf = pd.DataFrame.from_dict(
            {
                "category": ["pleat"] * len(self.pleats),
                "geometry": [p["geometry"] for p in self.pleats],
                "geom_size": [4] * len(self.pleats),
            }
        )

        hdf = pd.DataFrame.from_dict(
            {
                "category": ["placeholder"] * len(self.placeholders),
                "geometry": [self.placeholder_box(h) for h in self.placeholders],
                "geom_size": [4] * len(self.placeholders),
            }
        )

        df = pd.concat([xdf, ndf, cdf, sdf, pdf, hdf]).reset_index(drop=True)
        df["label"] = df["label"].fillna("")
        df["geom_size"] = df["geom_size"].fillna(0)
        gdf = gpd.GeoDataFrame(df)

        geom_sizes = np.array(df["geom_size"])

        ax = gdf.plot(
            "category",
            legend=True,
            categorical=True,
            figsize=(15, 15),
            legend_kwds={"bbox_to_anchor": (1.23, 1)},
            linewidth=geom_sizes,
            markersize=geom_sizes,
        )

        ax.set_title(self.key)

        def get_label_point(geom):
            #  this is better than representative_point() on its own as it will
            #  return the middle of a line, not arbitrary point
            if geom.geom_type in ["LinearRing", "LineString", "MultiLineString"]:
                return geom.interpolate(0.5, normalized=True)
            return geom.representative_point()

        gdf.apply(
            lambda x: ax.annotate(
                text=x["label"] if "label" in x else "",
                xy=get_label_point(x["geometry"]).coords[0],
                xytext=(3, 3),
                textcoords="offset points",
            ),
            axis=1,
        )
        ax.invert_yaxis()

        return gdf

    def plot_bezier(self):
        import geopandas as gpd

        inner = self.geometry
        points = []
        handles = []
        connectors = []
        for bezier_edge in self.bezier_edges:
            for point in bezier_edge["points"]:
                points.append(point["point"])
                handles.append(point["handle1"])
                handles.append(point["handle2"])
                connectors.append(LineString([point["point"], point["handle1"]]))
                connectors.append(LineString([point["point"], point["handle2"]]))

        gdf = gpd.GeoDataFrame(
            {
                "geometry": [inner] + points + handles + connectors,
                "category": ["inner"]
                + ["point"] * len(points)
                + ["handle"] * len(handles)
                + ["connector"] * len(connectors),
            }
        )
        ax = gdf.plot(
            "category",
            categorical=True,
            figsize=(15, 15),
            legend=True,
            markersize=[1]
            + [20] * len(points)
            + [10] * len(handles)
            + [1] * len(connectors),
            linewidth=[1]
            + [1] * len(points)
            + [1] * len(handles)
            + [3] * len(connectors),
        )
        ax.set_title(self.key)
        ax.invert_yaxis()

        return gdf


class LabellerTool:
    def _get_connection_region_mask_with_labels(piece_outline, labelled_edges):
        from PIL import Image, ImageOps
        import cv2
        import numpy as np

        """
        the geometry should be shifted to the origin as should the labelled edges
        the edges are labelled by what the connect to for example ('Bodice', geometry)
        We will merge all of this into a mask with labels that can be used for  segmentation
        
        """
        labels = {
            "Background": 0,
            "Self": 1,
            "Bodice": 2,
            "Lower Body": 3,
            "Sleeve / Arm": 4,
            "Neck": 5,
            "Dependent Accessory": 6,
            "Independent Accessory": 7,
        }

        g = piece_outline

        if hasattr(g, "exterior"):
            g = g.exterior

        mask_image = np.zeros((int(g.bounds[-1]), int(g.bounds[-2])), dtype=np.uint8)
        mk = cv2.fillPoly(mask_image, np.array([np.array(g).astype(int)], "int32"), 255)
        im = Image.fromarray(mk)
        im.thumbnail((512, 512))
        im = ImageOps.expand(im, border=20, fill="black")
        base = im.copy()
        # add the labelled connections for training
        # the trainer data does not need the actual white self
        mask_image = np.zeros((int(g.bounds[-1]), int(g.bounds[-2])), dtype=np.uint8)

        for lbl, poly in labelled_edges:
            id_val = labels.get(lbl)
            mk = cv2.fillPoly(
                mask_image,
                np.array([np.array(poly.exterior).astype(int)], "int32"),
                id_val,
            )

        # plt.imshow(mk, cmap='Spectral')
        im = Image.fromarray(mk)
        im.thumbnail((512, 512))
        im = ImageOps.expand(im, border=20, fill="black")

        return im, base

    def iterate_connection_region_masks(df, vspcs, marker_size=100):
        """
        Using the VS pieces, get the dataframe, treated and create masks of the connected regions with class name determined by target region
        for example sleeve to bodice or bodice to neck
        for semantic seg we create labelled masks using the marker where the class is based on the connected region
        """
        from res.media.images.geometry import (
            shift_geometry_to_origin,
            Polygon,
        )

        for piece, data in df.groupby("source"):
            # return data
            # get all the separate edges and make the ring
            # g = list(polygonize(unary_union(data['geometry'])))[0]
            g = vspcs.pieces[piece].geometry
            # shift
            h = shift_geometry_to_origin(g)
            edges = []
            # now look at edges one by one
            for row in data.to_dict("records"):
                e = row["geometry"]
                # shift in bounds
                e = shift_geometry_to_origin(e, bounds=g.bounds)
                cls = row["target_piece_region"]
                # compute a region for the edge - use the intersection with the polygon
                e = e.buffer(marker_size).intersection(Polygon(h))
                edges.append((cls, e))

            im, base = LabellerTool._get_connection_region_mask_with_labels(h, edges)

            yield piece, im, base
