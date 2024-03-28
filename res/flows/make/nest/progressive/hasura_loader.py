import res
import pandas as pd
from res.airtable.print import ASSETS
from res.flows.meta.ONE.meta_one import MetaOnePieceCollection
from res.flows.api.core.node import node_id_for_name
from res.utils import logger, res_hash
from shapely.geometry import Polygon
from shapely.wkt import dumps as shapely_dumps
from res.flows.meta.pieces import PieceName
from res.learn.optimization.packing.annealed.progressive import (
    ProgressiveNode,
    OneNode,
)
from res.media.images.geometry import from_geojson
from .loader import Loader

s3 = res.connectors.load("s3")

QUERY = """
query {
  make_one_pieces(where: __WHERE__) {
    id
    code
    meta_piece_id
    set_key
    piece {
      body_piece {
        id
        body_id
        key
        type
        piece_key
        size_code
        outer_geojson
        fuse_type
      }
      material_code
      color_code
    }
    one_order {
      id
      one_code
      one_number
      sku
      order_line_item {
        order {
          name
          created_at
          email
        }
      }
    }
  }
}
"""


class HasuraLoader(Loader):
    def _query_hasura(self, materials):
        # abandon all hope of reading this.
        where_clause = f"""
          {{_and: [{{node_id: {{_eq: "{node_id_for_name("Make.Print.RollPacking")}"}}}}, {{status: {{_eq: "Enter"}}}}]}}
        """
        if len(materials) >= 0:
            where_clause = f"""
            {{_and:[
              {where_clause},
              {{_or: [{",".join(f'{{piece: {{material_code: {{_eq: "{m}"}}}}}}' for m in materials)}]}}
            ]}}
          """
        query = QUERY.replace("__WHERE__", where_clause)
        return res.connectors.load("hasura").execute_with_kwargs(query)

    def load_asset_info(self, materials=[], record_ids=[], material_swaps={}):
        logger.info(f"Loading pieces from hasura for materials {materials}")
        result = self._query_hasura(materials)
        logger.info("Munging dataframe")
        df = pd.DataFrame(result["make_one_pieces"])
        df = res.utils.dataframes.expand_column_drop(df, "piece", alias="pieces")
        df = res.utils.dataframes.expand_column_drop(
            df, "one_order", alias="one_orders"
        )
        df = res.utils.dataframes.expand_column_drop(
            df, "pieces_body_piece", alias="body_pieces"
        )
        logger.info(f"Got {df.shape[0]} pieces")
        self.df = df[
            [
                "id",
                "code",
                "one_orders_one_number",
                "one_orders_one_code",
                "one_orders_sku",
                "body_pieces_type",
                "body_pieces_id",
                "body_pieces_outer_geojson",
                "body_pieces_piece_key",
                "body_pieces_fuse_type",
                "body_pieces_size_code",
                "pieces_material_code",
                "pieces_color_code",
            ]
        ].rename(
            columns={
                "body_pieces_piece_key": "key",
                "id": "piece_id",
                "body_pieces_size_code": "size_code",
                "body_pieces_type": "nest_type",  # TODO
                "body_pieces_fuse_type": "fuse_type",
                "code": "piece_code",
                "pieces_material_code": "material_code",
                "pieces_color_code": "color_code",
                "one_orders_one_number": "one_number",
                "one_orders_one_code": "one_code",
                "one_orders_sku": "sku",
            }
        )

        self.df["nest_type"] = self.df.nest_type.apply(
            lambda n: "self" if n == "lining" else n
        )
        self.df["body_code"] = self.df.sku.apply(lambda s: s.split(" ")[0])
        self.df["zone"] = self.df.key.apply(
            lambda k: PieceName(k).commercial_acceptability_zone
        )
        # TODO: deal with empty one numbers.
        self.df = self.df[self.df.one_number != 0].reset_index(drop=True)
        self.df["material_code"] = self.df["material_code"].apply(
            lambda c: material_swaps.get(c, c)
        )
        return self.df

    def get_packing_nodes(self, material_props, **kwargs):
        material_df = self.df[
            self.df.material_code == material_props.material_code
        ].reset_index(drop=True)
        material_df["nestable_wkt"] = material_df.body_pieces_outer_geojson.apply(
            lambda l: shapely_dumps(
                material_props.transform_for_nesting(Polygon(from_geojson(l)))
            )
        )
        ones_nodes = {}
        asset_info = {}
        for (
            one_number,
            one_code,
            nest_type,
            fuse_type,
            sku,
            color_code,
        ), gdf in material_df.groupby(
            ["one_number", "one_code", "nest_type", "fuse_type", "sku", "color_code"],
            dropna=False,
        ):
            logger.info(
                f"Making node for: {(one_number, one_code, nest_type, sku, color_code)}"
            )
            # lame hack to keep airtable in the loop about whats going on.
            airtable_record = ASSETS.first(
                formula=f"""
                    AND(
                        {{Nesting One Ready}}=1,
                        {{Material Code}}='{material_props.material_code}',
                        {{Pieces Type}}='{nest_type}',
                        {{__order_number}}={one_number}
                    )
                """,
            )
            if airtable_record is None:
                logger.warn("Unable to find airtable record")
                continue
            logger.info(f"Found airtable record {airtable_record['fields']['Key']}")
            piece_count = gdf.shape[0]
            node_id = res_hash(",".join(sorted(gdf.piece_id.values)).encode())
            # hack because hasura skus sometimes dont have the size code.
            sku = airtable_record["fields"]["SKU"]
            nesting_sku = (
                sku.split(" ")[0]
                + sku.split(" ")[3]
                + "_"
                + res_hash(",".join(sorted(gdf.key.values)).encode())
            )
            gdf["asset_key"] = airtable_record["fields"]["Key"]
            gdf["asset_id"] = airtable_record["id"]
            gdf["wrapped_piece_codes"] = None
            gdf["sku"] = sku
            gdf["piece_name"] = gdf.piece_code
            gdf["piece_description"] = "no description"
            gdf["s3_image_path"] = None
            node = ProgressiveNode.from_pieces(
                gdf,
                material_props.max_width_px,
                node_id=node_id,
                mergeable=material_props.fabric_type == "Woven" or piece_count == 1,
            )
            if material_props.fabric_type == "Knit":
                # since knits nests just get concatenated together, make sure they are compressed.
                node.packing.compress()
            node_value = 10  # TODO
            if one_number not in ones_nodes:
                ones_nodes[one_number] = OneNode(nesting_sku=nesting_sku)
            ones_nodes[one_number].add_node(
                nest_type + "_" + str(fuse_type), node, node_value
            )
            asset_info[node_id] = {
                "airtable_record_id": airtable_record["id"],
                "piece_set_key": airtable_record["fields"]["Prepared Pieces Key"],
                "airtable_key": airtable_record["fields"]["Key"],
                "order_date": "todo",
                "piece_count": piece_count,
                "piece_type": nest_type,
                "rank": airtable_record["fields"]["Rank"],
                "color_code": color_code,
                "order_code": "todo",
                "brand": "todo",
                "days_in_print_station": 1,
            }
        return ones_nodes, asset_info, 0
