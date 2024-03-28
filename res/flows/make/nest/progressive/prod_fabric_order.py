import res
import math
from res.airtable.print import ASSETS
from res.utils import logger, ping_slack
from res.flows import flow_node_attributes, FlowContext
from res.learn.optimization.packing.annealed.progressive import (
    nest_to_length,
    NestCache,
    _yards_to_px,
    _px_to_yards,
)
from .airtable_loader import AirtableLoader
from .hasura_loader import HasuraLoader
from .pack_one import BLOCK_FUSER_MAX_WIDTH_PX
from .construct_rolls import (
    _load_material_info,
    MIN_PRINTABLE_YARDS,
)


s3 = res.connectors.load("s3")

SLA_MATERIALS = [
    "CTNBA",
    "CTW70",
    "CHRST",
    "ECVIC",
    "CTSPS",
    "LTCSL",
    "CDCBM",
    "PIMA7",
    "CLTWL",
    "CFT97",
]

SPECIALTY_MATERIALS = [
    "CT406",
    "CTJ95",
    "LY115",
    "CTNSP",
    "CUTWL",
    "SLCTN",
    "LY100",
    "OC135",
    "OCT70",
]


def _get_focus_materials():
    old_assets = ASSETS.all(
        fields=["Material Code"],
        formula="AND({Nesting ONE Ready} = 1, {Asset Age Days} >= 5)",
    )
    materials = set([r["fields"]["Material Code"] for r in old_assets])
    needed_specialty_materials = [m for m in materials if m in SPECIALTY_MATERIALS]
    return SLA_MATERIALS + needed_specialty_materials


@flow_node_attributes(
    "prod_fabric_order.generator",
    slack_channel="autobots",
    slack_message="<@U0361U4B84X> prod fabric orders failed",
)
def generator(event, context):
    with FlowContext(event, context) as fc:
        focus_materials = set(fc.args.get("materials", _get_focus_materials()))
        logger.info(f"Focus materials: {focus_materials}")
        data_loader = (
            HasuraLoader()
            if fc.args.get("source", "") == "hasura"
            else AirtableLoader()
        )
        one_info = data_loader.load_asset_info(
            focus_materials,
        )
        s3.write(
            fc.get_node_path_for_key("cache", fc.key) + "/cache.pickle", data_loader
        )
        assets_by_material = sorted(
            dict(one_info["material_code"].value_counts()).items()
        )
        logger.info("Assets by material:")
        for material, asset_count in assets_by_material:
            logger.info(f"  {material}: {asset_count}")
        mappers = []
        for material_code in focus_materials:
            map_event = dict(event)
            map_event["args"] = {
                **map_event.get("args", {}),
                "material_code": material_code,
            }
            map_event["assets"] = []
            mappers.append(map_event)
        return mappers


@flow_node_attributes(
    "prod_fabric_order.handler",
    cpu="7500m",
    memory="16G",
    slack_channel="autobots",
    slack_message="<@U0361U4B84X> prod fabric orders failed",
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        material = fc.args.get("material_code")
        material_prop = _load_material_info(material)
        bust_nest_cache = fc.args.get("bust_nest_cache", "false").lower() == "true"
        data_loader = s3.read(
            fc.get_node_path_for_key("cache", fc.key) + "/cache.pickle"
        )
        ones_nodes, _, _ = data_loader.get_packing_nodes(material_prop)
        logger.info(f"Loaded {len(ones_nodes)} ones")
        if len(ones_nodes) == 0 or len(ones_nodes) > 100:
            return
        nest_cache = NestCache.load(
            material_prop,
            []
            if bust_nest_cache
            else [id for n in ones_nodes.values() for id in n.covered_node_ids()],
            BLOCK_FUSER_MAX_WIDTH_PX * material_prop.stretch_x,
            f"{fc.get_node_path_for_key('nest', fc.key)}/labels",
        )
        solution, _ = next(
            nest_to_length(
                ones_nodes,
                material_prop,
                _yards_to_px(100),
                nest_cache,
                None,
                slop_frac=0.1,
                ending_ts=None,
                always_finalize=False,
            )
        )
        total_length = _px_to_yards(solution.nested_required_length)
        logger.info(f"Total length of assets {total_length:.1f} yards")
        # save nest cache before adding all the other crapola to it.
        nest_cache.save()
        if total_length >= MIN_PRINTABLE_YARDS:
            return
        solution_length = total_length
        needed_running_yardage = math.ceil(MIN_PRINTABLE_YARDS - solution_length)
        material_type = "SLA" if material in SLA_MATERIALS else "SPECIALTY"
        ping_slack(
            f"For {material_type} material {material} prod assets give {total_length:.1f} yards - would order {needed_running_yardage} yards of running yardage.",
            "one-platform-x",
        )


@flow_node_attributes(
    "prod_fabric_order.reducer",
)
def reducer(event, context):
    return {}
