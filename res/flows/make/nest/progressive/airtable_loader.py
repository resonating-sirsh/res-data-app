import res
import collections
import pandas as pd
import numpy as np
import random
import math
from functools import lru_cache
from shapely.affinity import scale
from shapely.geometry import Polygon
from shapely.wkt import dumps as shapely_dumps, loads as shapely_loads
from res.airtable.print import ASSETS
from res.utils import logger
from res.flows.dxa.prep_ordered_pieces import get_fusing_piece_types
from res.learn.optimization.packing.annealed.progressive import (
    ProgressiveNode,
    OneNode,
)
from .pack_one import BLOCK_FUSER_MAX_WIDTH_PX
from .utils import ping_rob
from .loader import Loader
from ..utils import transform_pieces_for_nesting, validate_shapes

ASSET_VALUE_PER_DAY = 10.0
HEALING_ASSET_VALUE_PER_DAY = 20.0
MIN_ORDER_AGE_TO_DUPLICATE_HEALING = 60

s3 = res.connectors.load("s3")

ASSET_FIELD_MAP = {
    "Key": "key",
    "Prepared Pieces Key": "piece_set_key",
    "Rank": "rank",
    "Healing Piece S3 Piece URI": "healing_s3_path",
    "Pieces Type": "piece_type",
    "Prepared Pieces Count": "piece_count",
    "Prepared Pieces Area": "piece_area",
    "Coalesced Order Date": "order_date",
    "Days Since Customer Ordered (days)": "days_since_ordered",  # this field is sloppy as hell
    "__order_number": "order_number",
    "Intelligent ONE Marker File ID": "intelligent_marker_file_id",
    "SKU": "sku",
    "Material Code": "material_code",
    "S3 File URI": "marker_uri",
    "Color Code": "color_code",
    "Belongs to Order": "order_code",
    "Brand Name": "brand_name",
    "Emphasis": "emphasis",
    "Days in Print Station": "days_in_print_station",
    "Force Material Swap": "force_material_swap",
}


class AirtableLoader(Loader):
    def __init__(self):
        pass

    def _construct_nodes_from_nested_one(
        self,
        asset_record,
        material_props,
        nest_knits=False,
        duplicate=False,
        preemptive_heal=False,
    ):
        # the packed ones are already transformed and nested -- TODO: checks to make sure the material properties all agree.
        df_path = f"s3://res-data-production/flows/v1/make-nest-progressive-pack_one/primary/nest/{asset_record['id']}/output.feather"
        df = s3.read(df_path)
        # check that the compensation is still valid (esp when doign a material swap).
        previous_stretch_x = df.stretch_x.iloc[0]
        previous_stretch_y = df.stretch_y.iloc[0]
        if (
            abs(material_props.stretch_x - previous_stretch_x) > 1e-3
            or abs(material_props.stretch_y - previous_stretch_y) > 1e-3
        ):
            logger.info(
                f"Re-compensating asset {df.asset_key[0]} from {previous_stretch_x:.3f}x{previous_stretch_y:.3f} to {material_props.stretch_x:.3f}x{material_props.stretch_y:.3f}"
            )
            df["nestable_wkt"] = df.apply(
                lambda r: shapely_dumps(
                    transform_pieces_for_nesting(
                        [shapely_loads(r["geometry"])],
                        material_props.stretch_x,
                        material_props.stretch_y,
                        r.buffer,
                        material_props.max_width_px,
                        r.get("force_clip", False),
                    )[0]
                ),
                axis=1,
            )
            df["stretch_x"] = material_props.stretch_x
            df["stretch_y"] = material_props.stretch_y
            s3.write(df_path, df)
        # filter out label pieces which may be in there
        df = df[
            df.piece_id.apply(
                lambda n: not n.startswith("qr_") and not n.startswith("bf_")
            )
        ]
        if not "nestable_wkt" in df.columns:
            df["nestable_wkt"] = df["nested_geometry"]
        if not "piece_info" in df.columns:
            df["piece_info"] = [{}] * df.shape[0]
        validate_shapes(
            [shapely_loads(p) for p in df.nestable_wkt],
            material_props.max_width_px,
            flag_asset=asset_record["id"],
        )
        df.sort_values("packing_order", inplace=True)
        mergeable = (
            material_props.fabric_type == "Woven" or asset_record["rank"] == "Healing"
        )
        sku = asset_record["sku"]
        body = sku[0:2] + "-" + sku[2:6]
        if not "fusing_type" in df.columns:
            ft = self.get_fusing_piece_types_cached(body)
            df["fusing_type"] = df["piece_code"].apply(lambda c: ft.get(c, "light"))
        df["fusing_type"] = df["fusing_type"].where(
            df["fusing_type"].notnull(), "light"
        )
        if body == "CC-9066" or "MTRFXPNL" in df.iloc[0].piece_code:
            df["piece_type"] = "running_yardage"
        if "cutline_uri" not in df.columns:
            df["cutline_uri"] = None
        if "artwork_uri" not in df.columns:
            df["artwork_uri"] = None
        if "wrapped_piece_ids" not in df.columns:
            df["wrapped_piece_ids"] = None
        if "wrapped_piece_codes" not in df.columns:
            df["wrapped_piece_codes"] = None
        if "thread_colors" not in df.columns:
            df["thread_colors"] = ""
        else:
            df["thread_colors"] = df["thread_colors"].apply(str)
        if "uniform_mode" not in df.columns:
            df["uniform_mode"] = False
            df["uniform_mode_placement"] = False
        df["rank"] = asset_record["rank"]
        df["is_healing"] = asset_record["rank"] == "Healing"
        df["node_key"] = asset_record["order_number"]
        df["sku"] = sku
        df = df[
            [
                "node_key",
                "piece_id",
                "asset_id",
                "asset_key",
                "s3_image_path",
                "piece_name",
                "piece_code",
                "piece_description",
                "zone",
                "fusing_type",
                "nestable_wkt",
                "piece_type",
                "cutline_uri",
                "wrapped_piece_ids",
                "wrapped_piece_codes",
                "uniform_mode",
                "uniform_mode_placement",
                "offset_size",
                "artwork_uri",
                "piece_info",
                "thread_colors",
                "rank",
                "is_healing",
                "sku",
                "buffer",
                "geometry",
            ]
        ]
        df["body_code"] = body
        if df.iloc[0].piece_type != "running_yardage":
            if duplicate:
                ddf = df.copy()
                ddf.piece_id = ddf.piece_id.apply(lambda i: i + "::duplicate")
                df = pd.concat([df, ddf], ignore_index=True)
            elif preemptive_heal:
                ddf = df.sample(n=min(3, df.shape[0]))  # todo something smarter here.
                ddf.node_key = ddf.node_key.apply(lambda i: i + "_preemptive_heal")
                ddf.piece_id = ddf.piece_id.apply(lambda i: i + "::duplicate")
                df = pd.concat([df, ddf], ignore_index=True)

        for gkey, gdf in df.groupby(
            ["node_key", "piece_type", "fusing_type"], dropna=False
        ):
            node_key, piece_type, fusing_type = gkey
            nest_type = (
                piece_type
                if piece_type != "block_fuse"
                else "block_fuse_" + fusing_type.split(" ")[0].lower()
            )
            uniform_mode = all(gdf.uniform_mode)
            if uniform_mode:
                nest_type = "uniform_" + sku.replace(" ", "_") + "_" + nest_type
            """
            if body in ["TH-3000"]:
                size_code = sku.split(" ")[3]
                nest_type = (
                    "smocking_"
                    + body
                    + "_"
                    + size_code
                    + "_"
                    + int(math.abs(hash(str(df.iloc[0].thread_colors))))
                )
            """
            piece_count = gdf.shape[0] / 2 if duplicate else gdf.shape[0]
            node_id = asset_record["id"] + ":" + nest_type
            if "preemptive_heal" in node_key:
                node_id += ":preemptive_heal"
            one_piece_asset = (
                piece_count == 1 and "block_0" not in gdf.piece_name.values
            )
            if not mergeable and one_piece_asset:
                logger.info(
                    f"Marking one-piece asset {asset_record['key']} as mergable"
                )
            node = ProgressiveNode.from_pieces(
                gdf,
                material_props.max_width_px,
                node_id=node_id,
                mergeable=mergeable
                or one_piece_asset
                or (nest_knits and "block_0" not in gdf.piece_name.values)
                or uniform_mode,
                margin_x=300 * df["offset_size"][0] * material_props.stretch_x
                if uniform_mode
                else 0,
                margin_y=300 * df["offset_size"][0] * material_props.stretch_y
                if uniform_mode
                else 0,
            )
            yield (node_key, node, nest_type)

    @lru_cache(maxsize=100)
    def get_fusing_piece_types_cached(self, body_code):
        try:
            return get_fusing_piece_types(body_code)
        except:
            logger.warn(f"Failed to get fusing info for {body_code} assuming light")
            return {}

    def _construct_node_from_healing_piece(
        self,
        asset_record,
        material_props,
        duplicate=False,
        previous_material=None,
    ):
        # the healing pieces need transformation.
        healing_shape_path = (
            asset_record["healing_s3_path"]
            .replace(".png", ".feather")
            .replace("extract_parts", "extract_outlines")
        )
        healing_piece = material_props.transform_for_nesting(
            Polygon(s3.read(healing_shape_path)[["column_1", "column_0"]].values)
        )
        sku = asset_record["sku"]
        body = sku[0:2] + "-" + sku[2:6]
        pieces_df = pd.DataFrame.from_records(
            [
                {
                    "piece_id": asset_record["id"],
                    "asset_id": asset_record["id"],
                    "asset_key": asset_record["key"],
                    "s3_image_path": asset_record["healing_s3_path"],
                    "piece_code": "-".join(
                        asset_record["healing_s3_path"]
                        .split("/")[-1]
                        .replace(".png", "")
                        .split("-")[-2:]
                    ),
                    "nestable_wkt": shapely_dumps(healing_piece),
                    "body_code": body,
                    "wrapped_piece_codes": None,
                    "piece_info": {},
                    "thread_colors": "",
                }
            ]
            + (
                []
                if (not duplicate)
                and asset_record["days_since_ordered"]
                < MIN_ORDER_AGE_TO_DUPLICATE_HEALING
                else [
                    {
                        "piece_id": asset_record["id"] + "::duplicate",
                        "asset_id": asset_record["id"],
                        "asset_key": asset_record["key"],
                        "s3_image_path": asset_record["healing_s3_path"],
                        "piece_code": "-".join(
                            asset_record["healing_s3_path"]
                            .split("/")[-1]
                            .replace(".png", "")
                            .split("-")[-2:]
                        ),
                        "nestable_wkt": shapely_dumps(healing_piece),
                        "body_code": body,
                        "wrapped_piece_codes": None,
                        "piece_info": {},
                        "thread_colors": "",
                    }
                ]
            )
        )
        if previous_material is not None:
            logger.info(
                f"Swapping material for asset {pieces_df.asset_key[0]} from {previous_material.material_code}"
            )
            pieces_df["nestable_wkt"] = pieces_df["nestable_wkt"].apply(
                lambda w: shapely_dumps(
                    scale(
                        shapely_loads(w),
                        material_props.stretch_x / previous_material.stretch_x,
                        material_props.stretch_y / previous_material.stretch_y,
                    )
                )
            )
        node_id = asset_record["id"]
        node = ProgressiveNode.from_pieces(
            pieces_df,
            material_props.max_width_px,
            node_id=node_id,
            mergeable=True,  # we nest healing pieces in all materials now.
        )
        nest_type = (
            "self" if not healing_shape_path.endswith("-BF.feather") else "block_fuse"
        )
        if nest_type == "block_fuse":
            # go to airtable to figure out what kind of fusing it needs.
            sku = asset_record["sku"]
            body = sku[0:2] + "-" + sku[2:6]
            ft = "light"  # assume light fusing unless we mapped it.
            for k, t in self.get_fusing_piece_types_cached(body).items():
                if k in healing_shape_path:
                    ft = t
                    break
            nest_type += "_" + ft.split(" ")[0].lower()
        return asset_record["order_number"], node, nest_type

    def load_asset_info(self, materials=[], record_ids=[], material_swaps={}):
        record_filter = (
            f"AND(FIND({{_record_id}}, '{','.join(record_ids)}'), {{Nesting ONE Ready}}>0)"
            if record_ids
            else "{Nesting ONE Ready}>0"
        )
        material_filter = (
            ("OR(" + ",".join(f"{{Material Code}}='{m}'" for m in materials) + ")")
            if materials
            else "TRUE()"
        )
        one_info = ASSETS.all(
            fields=list(ASSET_FIELD_MAP.keys()),
            formula=f"AND({material_filter}, {record_filter}, {{Rank}}!='')",
        )
        logger.info(
            f"Got {len(one_info)} assets to use: {collections.Counter([r['fields']['Rank'] for r in one_info]).items()}"
        )
        self.asset_info = pd.DataFrame(
            [
                {
                    "id": r["id"],
                    "record_id": r["id"],
                    **{
                        ASSET_FIELD_MAP[k]: (v if not isinstance(v, list) else v[0])
                        for k, v in r["fields"].items()
                    },
                }
                for r in one_info
            ]
        )
        self.asset_info["original_material_code"] = self.asset_info["material_code"]
        self.asset_info["material_code"] = self.asset_info["material_code"].apply(
            lambda c: material_swaps.get(c, c)
        )
        # for jan 2024 we want to print theme pima7 on ctw70
        self.asset_info["material_code"] = self.asset_info.apply(
            lambda r: "CTW70"
            if (
                (r.order_code.startswith("TH-") and r.material_code in ["PIMA7", "RYJ01"])
                or (r.piece_count == 50 and "0ZZOS" in r.sku and r.material_code == "PIMA7")
            )
            else r.material_code,
            axis=1,
        )
        self.asset_info["material_code"] = self.asset_info.apply(
            lambda r: "MCTTW"
            if r.order_code.startswith("TH-")
            and r.material_code in ["HC293", "CTSPS", "BRSCT"]
            else r.material_code,
            axis=1,
        )
        # apply material swaps noted in airtable.
        if "force_material_swap" in self.asset_info.columns:
            self.asset_info["material_code"] = self.asset_info.apply(
                lambda r: r.force_material_swap
                if not pd.isna(r.force_material_swap)
                else r.material_code,
                axis=1,
            )
        self.asset_info = self.asset_info.replace(np.nan, None)
        return self.asset_info

    def get_packing_nodes(self, material_props, **kwargs):
        # TODO: handle healing pieces.
        nest_knits = kwargs.get("nest_knits", False)
        duplicate = kwargs.get("duplicate", False)
        preemptive_heal = kwargs.get("preemptive_heal", False)
        logger.info("Loading geometry")
        if duplicate:
            logger.info("Duplicating everything")
        if preemptive_heal:
            logger.info("Pre-emptively healing pieces")
        ones_nodes = {}
        failure_count = 0
        asset_info = {}
        assets_by_nest_type = {}
        for _, row in self.asset_info[
            self.asset_info.material_code == material_props.material_code
        ].iterrows():
            one = {k: v for k, v in dict(row).items() if v is not None}
            is_healing = one["rank"] == "Healing" and "healing_s3_path" in one
            try:
                for node_key, node, nest_type in (
                    self._construct_nodes_from_nested_one(
                        one,
                        material_props,
                        nest_knits,
                        duplicate=duplicate,
                        preemptive_heal=preemptive_heal,
                    )
                    if not is_healing
                    else [
                        self._construct_node_from_healing_piece(
                            one,
                            material_props,
                            duplicate=duplicate,
                        )
                    ]
                ):
                    assets_by_nest_type[nest_type] = 1 + assets_by_nest_type.get(
                        nest_type, 0
                    )
                    nesting_sku = (
                        None
                        if is_healing
                        else (one["sku"].split(" ")[0] + one["sku"].split(" ")[3])
                    )
                    if "uniform" in nest_type:
                        # for uniforms we need the whole sku.
                        nesting_sku = one["sku"]
                    if not node_key in ones_nodes:
                        ones_nodes[node_key] = OneNode(nesting_sku=nesting_sku)
                    # a series of tie breakers which basically define how ones will be ordered when nesting.
                    # in essence we do things by time order, but we also want to deviate from this to:
                    # - keep wholesale orders together (for now really all orders -- need to get sales channel to optimus).
                    # - keep colors together within a wholesale order
                    # - keep sizes together when doing uniforms.
                    order_value = 0
                    if "order_code" in one:
                        random.seed(hash(str(one.get("order_code"))))
                        order_value = random.random()
                    thread_color_matters = one["sku"].split(" ")[0] in [
                        "TH3000",
                        "TH1002",
                    ]
                    ones_color = (
                        node.packing.piece_df.iloc[0].thread_colors
                        if thread_color_matters
                        else one["color_code"]
                    )
                    random.seed(hash(str(ones_color)))
                    color_value = random.random()
                    random.seed(hash(str(one["sku"].split(" ")[-1])))
                    size_value = random.random()
                    tiebreaker_value = (
                        (size_value + color_value * 0.1)
                        if "uniform" in nest_type
                        else (color_value + size_value * 0.1)
                    )
                    node_value = (
                        0.1
                        if "preemptive_heal" in node_key
                        else (
                            (
                                HEALING_ASSET_VALUE_PER_DAY
                                if is_healing
                                else ASSET_VALUE_PER_DAY
                            )
                            * (1 + one["days_since_ordered"])
                            + one.get("emphasis", 0)
                            + 10 * order_value
                            + tiebreaker_value
                        )
                    )
                    ones_nodes[node_key].add_node(
                        nest_type, node, node_value, compress=False
                    )
                    asset_info[node.node_id] = {
                        "airtable_record_id": one["id"],
                        "piece_set_key": one.get("piece_set_key"),
                        "airtable_key": one.get("key"),
                        "order_date": one["order_date"],
                        "piece_count": 1
                        if one["rank"].lower() == "healing"
                        else int(one["piece_count"]),
                        "piece_type": one.get("piece_type"),
                        "rank": one["rank"],
                        "color_code": one["color_code"],
                        "order_code": one.get("order_code"),
                        "brand": one.get("brand_name"),
                        "days_in_print_station": one["days_in_print_station"],
                    }
            except Exception as ex:
                logger.warn(
                    f"Failed to load node for piece set {one['id']} of asset {one['key']}: {repr(ex)}",
                    # exc_info=1,
                )
                ping_rob(
                    f"Failed to load node for piece set {one['id']} of asset {one['key']}: {repr(ex)}"
                )
                if "too large" in repr(ex):
                    ASSETS.update(
                        one["id"],
                        {
                            "Failure Tags": "OVER_MAX_WIDTH",
                        },
                        typecast=True,
                    )
                failure_count += 1
        logger.info(f"Assets by nest type: {assets_by_nest_type}")
        logger.debug(f"Asset node keys: {asset_info.keys()}")
        return ones_nodes, asset_info, failure_count
