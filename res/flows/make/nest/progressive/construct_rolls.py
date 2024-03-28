import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import res
import datetime
import itertools
import time
import os
from res.airtable.misc import (
    MATERIAL_PROP,
    HOLIDAYS_TABLE,
)
from res.airtable.print import (
    ROLLS,
    MATERIAL_INFO,
    MACHINE_INFO,
)
from res.utils import logger, ping_slack
from res.flows import flow_node_attributes, FlowContext
from res.learn.optimization.material.swaps import (
    get_neighbor_materials,
    suggest_material_swaps,
)
from res.learn.optimization.packing.annealed.postprocess import (
    postprocess_nest_df,
    plot_postprocessed,
)
from res.learn.optimization.packing.annealed.progressive import (
    nest_into_rolls,
    NestCache,
    _px_to_yards,
    _yards_to_px,
    SolutionState,
    QR_CODE_IMAGE_PATH_PLACEHOLDER,
    QR_CODE_SIZE,
)
from res.learn.optimization.packing.annealed.data.material import Material
from ..utils import (
    write_qr_image_to_s3,
    write_guillotine_seperator_to_s3,
)
from .utils import (
    construct_airtable_nests,
    assign_solution,
    validate_solution,
    write_solution_to_airtable,
    post_assignment_observations,
    allocate_roll_for_ppu,
    add_contract_variables_to_assets,
)
from .airtable_loader import AirtableLoader
from .hasura_loader import HasuraLoader
from .pack_one import BLOCK_FUSER_MAX_WIDTH_PX

"""
Key optimus args:
    -   "materials": list of material codes
        limit optimus to only operate on the selected materials
    -   "roll_names": list of roll names
        limit optimus to use the specified rolls.  when we do this we also ignore the filter on 10yd roll length
        and on expired rolls, so using this is the main way to get around those if necessary.
    -   "fill_allocations": (True/False)
        if true - then look for existing ppu allocations without printfiles and nest into those.
        if false - then nest into whatever rolls are available / specified that dont have ppu allocations, and create them.

Various args that do stuff on occasions we need it:
    -   "time_limit_s": int
        how many seconds to let optimus run while trying to find a solution.
    -   "bust_nest_cache": (True/False)
        whether to nuke the cache before nesting -- useful if assets had broken pieces and we fixed them and want to re-nest.
    -   "source": (hasura/airtable)
        where to load the asset info from (default airtable)
    -   "duplicate": (True/False)
        whether to duplicate every asset -- usually jsut a crude way of getting to 10yd for an assignment
    -   "no_assign": (True/False)
        if True then dont actually make any assignments or ppu allocations, just run nesting.  This is
        really a debugging kind of setting.
    -   "width_override": float
        override the roll widths of everything to the specified width in inches.  Useful when we got a narrow roll
        and need to renest.
    -   "length_override_yd": float
        override the roll length to the specified amount of yards for all rolls we attempt to nest.  used to be useful in combination with
        roll_names for filling in a ppu allocation in an ad-hoc way, although probably not needed anymore.
    -   "reserve_frac": float
        when nesting, allocate more space than whats needed for the nests by a factor of 1 + reserve_frac (i.e., 0.5 = 50% more space reserved)
        this could be useful when we go on the plan of nesting at the last minute -- but we need to figure out how to set this / set it different for 
        each material
    -   "should_validate": (True/False)
        possibly turn off nest validation (checks that the assigned assets are still all assignable when nesting ends etc).
    -   "slack_channel": string
        name of a channel to send messages about what optimus is doing.
    -   "supress_messages": (True/False)
        prevent slack spew
"""

DEFAULT_BUFFER = 75

MAX_DAYS_IN_PRINT_STATION = 0

MIN_PRINTABLE_YARDS = 10

MATERIAL_FIELD_MAP = {
    "Material Code": "material_code",
    "Fabric Type": "fabric_type",
    "Printable Width (In)": "max_width_in",
    "Locked Width Digital Compensation (Scale)": "stretch_x",
    "Locked Length Digital Compensation (Scale)": "stretch_y",
    "Paper Marker Compensation (Width) FORMULA": "paper_stretch_x",
    "Paper Marker Compensation (Length) FORMULA": "paper_stretch_y",
    "Pretreatment Type": "pretreat_type",
}

# how many solutions to look at trying to find a valid one.
MAX_SOLUTIONS = 10

# how much extra material to allocate on the ppu - to try to be sure we can achieve a solution when the time comes.
PPU_ALLOCATION_SLOP_YARDS = 0.25

SOLUTION_KAFKA_TOPIC = "res_make.res_nest.roll_solutions"

CV_NO_ROLLS = "recl4SmE4e3XlBWBw"
CV_OPTIMUS_FAILURE = "rectsWUAwKQ8luTDx"
CV_INSUFFIENT_ASSETS = "recqmpfkdYJOLhdcH"
CV_OPTIMUS_CONSTRAINTS = "recx588pWJmLR9pk7"

s3 = res.connectors.load("s3")


def _load_raw_roll_info(material, allow_recovered_rolls, enforce_length_limit):
    recovered_filter = "" if allow_recovered_rolls else ", FIND('_E', {Name})=0"
    rolls_info = ROLLS.all(
        fields=["Name", "Latest Measurement"],
        formula=f"""AND(
            {{Allocated for PPU}}=FALSE(),
            {{Material Code}}='{material}',
            {{Current Substation}}!='Done',
            {{Current Substation}}!='Write Off',
            {{Current Substation}}!='Expired',
            {{Current Substation}}!='Print',
            {{Current Substation}}!='Steam',
            {{Current Substation}}!='Wash',
            {{Current Substation}}!='Dry',
            {{Current Substation}}!='Inspection',
            {{Current Substation}}!='Soft',
            {{Current Substation}}!='Scour: Wash',
            {{Current Substation}}!='Scour: Dry',
            {{Flag for Review}}=FALSE(),
            {{📏Roll Length Data}}!='',
            {{Print Assets (flow) 3}}='',
            {{Latest Measurement}}!='',
            {{Location V1.1}}!='',
            {{Inventory Cycle Count Status}}!='Not found'
            {recovered_filter}
        )""",
        sort=["Pretreatment Exit Date At"],
    )
    combined_info = [
        (r["fields"]["Name"], float(r["fields"]["Latest Measurement"]), r["id"], False)
        for r in rolls_info
        if float(r["fields"]["Latest Measurement"])
        >= (MIN_PRINTABLE_YARDS if enforce_length_limit else 0)
    ]
    logger.info(f"Got {len(combined_info)} raw rolls: {combined_info}")
    return combined_info


def _load_pretreated_roll_info(material):
    rolls_info = ROLLS.all(
        fields=["Name", "Pretreated Length"],
        formula=f"""
            AND(
                {{Current Substation}}='Print',
                {{Print Assets (flow) 3}}='',
                {{Flag for Review}}=FALSE(),
                {{Pretreated Length}}>5,
                {{Days Since Pretreatment Date}}<90,
                {{Material Code}}='{material}',
                {{Allocated for PPU}}=FALSE()
            )
        """,
        sort=["Pretreatment Exit Date At"],
    )
    rolls_info = [
        (r["fields"]["Name"], r["fields"]["Pretreated Length"][0], r["id"], True)
        for r in rolls_info
    ]
    logger.info(f"Got {len(rolls_info)} pretreated rolls: {rolls_info}")
    return rolls_info


def _load_allocated_roll_info(material):
    # when assigning try to keep the rolls in ppu order.
    rolls_info = ROLLS.all(
        fields=["Name", "Cut To Length (yards)", "Assigned PPU Name"],
        formula=f"""AND(
            {{Allocated for PPU}}=TRUE(),
            {{Material Code}}='{material}',
            {{Assigned Printfiles}}=0,
            {{Current Substation}}!='Done',
            {{Current Substation}}!='Write Off',
            {{Current Substation}}!='Expired',
            {{Current Substation}}!='Steam',
            {{Current Substation}}!='Wash',
            {{Current Substation}}!='Dry',
            {{Current Substation}}!='Inspection',
            {{Current Substation}}!='Soft',
            {{Current Substation}}!='Scour: Wash',
            {{Current Substation}}!='Scour: Dry'
        )
        """,
        sort=["Assigned PPU Name"],
    )
    rolls_info = [
        (r["fields"]["Name"], r["fields"]["Cut To Length (yards)"], r["id"], False)
        for r in rolls_info
    ]
    logger.info(f"Got {len(rolls_info)} allocated rolls: {rolls_info}")
    return rolls_info


def _load_rolls(material, roll_names=[], use_allocated=False, length_override=None):
    """
    Note -- this is responsible for ordering the rolls for progressive nestings consideration.
    When assigning we want to go in ppu order (i.e., so orders are more likely to get on the same ppu
    instead of spread across ppus).  And when allocating we can just go longest roll first.
    """
    if use_allocated:
        rolls = _load_allocated_roll_info(material)
    else:
        pretreated_rolls = _load_pretreated_roll_info(material)
        raw_rolls = _load_raw_roll_info(
            material, any("_E" in r for r in roll_names), len(roll_names) == 0
        )
        rolls = pretreated_rolls + raw_rolls
    if roll_names:
        logger.info(f"Limiting to rolls: {roll_names}")
        focus_rolls_id_only = [n.split(":")[0] for n in roll_names]
        rolls = [r for r in rolls if r[0].split(":")[0] in focus_rolls_id_only]
    if length_override is not None:
        rolls = [
            (name, length_override, id, pretreated) for name, _, id, pretreated in rolls
        ]

    if not use_allocated:
        # when allocating - use the longest roll first.
        rolls = sorted(rolls, key=lambda r: (0 if r[3] else 1, -r[1]))
    return rolls


def _get_allocated_materials():
    rolls_info = ROLLS.all(
        fields=["Name", "Material Code"],
        formula=f"""
        AND(
            {{Allocated for PPU}}=TRUE(),
            {{Assigned Printfiles}}=0,
            {{Current Substation}}!='Done',
            {{Current Substation}}!='Write Off',
            {{Current Substation}}!='Expired',
            {{Current Substation}}!='Steam',
            {{Current Substation}}!='Wash',
            {{Current Substation}}!='Dry',
            {{Current Substation}}!='Inspection',
            {{Current Substation}}!='Soft',
            {{Current Substation}}!='Scour: Wash',
            {{Current Substation}}!='Scour: Dry'
        )""",
    )
    return set(r["fields"]["Material Code"][0] for r in rolls_info)


def _load_material_info(material_code, width_override=None):
    material_info = MATERIAL_PROP.all(
        fields=list(MATERIAL_FIELD_MAP.keys()),
        formula=f"{{Material Code}}='{material_code}'",
    )
    material_props = {
        MATERIAL_FIELD_MAP[f]: v for f, v in material_info[0]["fields"].items()
    }
    if width_override is not None:
        material_props["max_width_in"] = width_override
    return Material(**material_props, buffer=DEFAULT_BUFFER)


def _sync_material_capabilities():
    machine_info = MACHINE_INFO.all(
        fields=["Material Capabilities", "Name"],
        formula="{Model}='MS JP7'",
    )
    logger.info(f"Material Capabilities: {machine_info}")
    MATERIAL_INFO.batch_update(
        [
            {
                "id": r["id"],
                "fields": {
                    "Printers Capable of Printing this Material": [
                        s["id"]
                        for s in machine_info
                        if r["fields"]["Key"] in s["fields"]["Material Capabilities"]
                    ]
                },
            }
            for r in MATERIAL_INFO.all(fields=["Key"])
        ],
        typecast=True,
    )


def _load_fullness_thresholds():
    _sync_material_capabilities()
    material_info = MATERIAL_INFO.all(
        fields=[
            "Key",
            "Printer Aware Fullness Threshold",
            "Enable Nesting",
            "Hours Since Last Assignment",
        ],
        formula="{Key}!=''",  # zzz
    )
    return (
        {
            r["fields"]["Key"]: r["fields"]["Printer Aware Fullness Threshold"]
            for r in material_info
        },
        {
            r["fields"]["Key"]: r["fields"].get("Enable Nesting", False)
            for r in material_info
        },
        {
            r["fields"]["Key"]: r["fields"].get("Hours Since Last Assignment", 666)
            for r in material_info
        },
    )


def _get_current_materials():
    return [
        r["fields"]["Key"]
        for r in MATERIAL_INFO.all(
            fields=["Key"],
            formula=f"""{{Roll Last Assigned At}}>'{(datetime.date.today() - datetime.timedelta(days=30)).strftime('%Y-%m-%d')}'""",
        )
    ]


def _plot_nest(sol, nest_df, path, filename, postprocessed=False):
    from matplotlib import pyplot as plt

    plt.switch_backend("Agg")
    width = 5
    height = width * sol.packing.height() / float(sol.packing.max_width)
    plt.figure(figsize=(width, height))
    if postprocessed:
        plot_postprocessed(nest_df)
    else:
        sol.packing.packing.plot()
    plt.savefig("nest.png", bbox_inches="tight")
    s3.upload("nest.png", path + "/" + filename)
    plt.close()


@flow_node_attributes(
    "construct_rolls.generator",
    cpu="7500m",
    memory="16G",
    slack_channel="autobots",
    slack_message="<@U0361U4B84X> nesting failed",
)
def generator(event, context):
    with FlowContext(event, context) as fc:
        fill_allocations = fc.args.get("fill_allocations", False)
        if not fill_allocations:
            # possibly skip nesting since we dont want to allocate ppus to print on holidays / weekends.
            today_str = datetime.datetime.now().strftime("%Y-%m-%d")
            next_holidays = HOLIDAYS_TABLE.all(
                fields=["Event", "Date"],
                formula=f"AND({{Date}}>='{today_str}', {{Category}}='Holiday - Office Closed', FIND('DR', {{Office}}))",
                sort=["Date"],
                max_records=1,
            )
            if (
                len(next_holidays) > 0
                and next_holidays[0]["fields"]["Date"] == today_str
            ):
                logger.info(f"Not nesting due to holiday {next_holidays[0]}")
                slack_channel = fc.args.get("slack_channel")
                if slack_channel is not None:
                    ping_slack(
                        f"Optimus is not running since it is a holiday in the DR: {next_holidays[0]['fields']['Event']}",
                        slack_channel,
                    )
                return []
        focus_materials = set(fc.args.get("materials", []))
        allocated_materials = []
        if (
            fill_allocations
            and len(focus_materials) == 0
            and not fc.args.get("no_assign", False)
        ):
            logger.info(f"Limiting to materials where we can fill allocations.")
            allocated_materials = _get_allocated_materials()
            if len(allocated_materials) == 0:
                logger.info("No allocations need filling")
                return []
        logger.info(f"Focus materials: {focus_materials}")
        focus_records = set(fc.args.get("asset_ids", []))
        logger.info(f"Focus assets: {focus_records}")
        material_swaps = fc.args.get("material_swaps", {})
        logger.info(f"Material swaps: {material_swaps}")
        data_loader = (
            HasuraLoader()
            if fc.args.get("source", "") == "hasura"
            else AirtableLoader()
        )
        one_info = data_loader.load_asset_info(
            focus_materials,
            focus_records,
            material_swaps,
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
        (
            fullness_thresholds,
            nesting_enabled,
            _,
        ) = _load_fullness_thresholds()
        all_materials = set(
            _get_current_materials() + [k for k, _ in assets_by_material]
        )
        logger.info(
            f"Fullness thresholds: {[(k, v) for k, v in fullness_thresholds.items() if k in focus_materials or len(focus_materials) == 0]}"
        )
        logger.info(f"All materials: {all_materials}")
        mappers = []
        if fill_allocations:
            focus_materials = allocated_materials
        for material_code, _ in assets_by_material:
            if (
                len(focus_materials) == 0 and nesting_enabled.get(material_code, False)
            ) or material_code in focus_materials:
                map_event = dict(event)
                map_event["args"] = {
                    **map_event.get("args", {}),
                    "material_code": material_code,
                }
                map_event["assets"] = []
                mappers.append(map_event)
        if len(focus_materials) == 0 and not fill_allocations:
            materials_with_assets = set([m for m, _ in assets_by_material])
            for material_code in all_materials:
                if not material_code in materials_with_assets:
                    _output_empty_message(
                        material_code,
                        fullness_thresholds.get(material_code, 0.0),
                        str(SolutionState.NO_ASSETS),
                        fc.key,
                    )
                elif not nesting_enabled.get(material_code, False):
                    _output_empty_message(
                        material_code,
                        fullness_thresholds.get(material_code, 0.0),
                        str(SolutionState.NESTING_DISABLED),
                        fc.key,
                    )
                    # SA was going to try and create a hyperlink https://airtable.com/apprcULXTWu33KFsh/tblJAhXttUzwxdaw5/viw1UiWhy5EaeRVAi/{RECORD_ID} but dont want to screw it up without testing the code path
                    ping_slack(
                        f"Nesting is disabled for {material_code} with {dict(assets_by_material)[material_code]} assets (enable in print app materials )",
                        "make_one_team",
                    )
        return mappers


def _output_nest_df(nest_node, material_prop, solution_path, postprocess_nest=False):
    qr_code_path = f"{solution_path}/{nest_node.name}-qr.png"
    write_qr_image_to_s3(nest_node.name, QR_CODE_SIZE, qr_code_path)
    nest_df = nest_node.packing.nested_df(material_prop)
    nest_df.loc[
        nest_df["s3_image_path"] == QR_CODE_IMAGE_PATH_PLACEHOLDER, "s3_image_path"
    ] = qr_code_path
    n = 1
    nest_name_simple = "_".join(nest_node.name.split("_")[1:]).replace("-", "_")
    for i, r in nest_df.iterrows():
        if r.piece_id.startswith("guillotine_spacer"):
            spacer_image_path = f"{solution_path}/{nest_node.name}_{r.piece_id}.png"
            write_guillotine_seperator_to_s3(
                f"{nest_name_simple}_part_{n}",
                f"{nest_name_simple}_part_{n-1}",
                int((r.max_nested_y - r.min_nested_y) / r.stretch_y),
                int((r.max_nested_x - r.min_nested_x) / r.stretch_x),
                spacer_image_path,
            )
            nest_df.loc[i, "s3_image_path"] = spacer_image_path
            n += 1

    if postprocess_nest:
        logger.info(f"Postprocessing: {nest_node.name}")
        nest_df = postprocess_nest_df(nest_df)

    s3.write(f"{solution_path}/{nest_node.name}.feather", nest_df)
    return nest_df


def _total_utilization(solution, roll_length_px):
    if len(solution) == 0 or list(solution.keys()) == ["NO_ROLL_NAME"]:
        return 0
    else:
        return sum(
            s.nested_utilization * s.nested_required_length for r, s in solution.items()
        ) / sum(roll_length_px[r] for r, _ in solution.items())


def _output_empty_message(material_code, fullness_threshold, status, job_key):
    kafka_msg = {
        "material_code": material_code,
        "pending_asset_count": 0,
        "failed_asset_count": 0,
        "created_at": res.utils.dates.utc_now_iso_string(),
        "solution_key": f"{material_code}-{int(time.time())}",
        "solution_status": status,
        "fullness_threshold": fullness_threshold,
        "argo_job_key": job_key,
        "total_roll_length_px": 0,
        "total_roll_length_yd": 0.0,
        "total_reserved_length_px": 0,
        "total_reserved_length_yd": 0.0,
        "total_asset_count": 0,
        "total_healing_pieces": 0,
        "total_utilization": 0.0,
        "total_length_utilization": 0.0,
        "roll_info": [],
    }
    res.connectors.load("kafka")[SOLUTION_KAFKA_TOPIC].publish(
        kafka_msg, use_kgateway=True
    )
    write_solution_to_airtable(
        material_code,
        job_key,
        "",
        {},
        str(SolutionState.NO_ASSETS),
        [],
        0,
        0,
        [],
        True,
        0,
        False,
        0,
    )


def _output_roll_solution(
    material_prop,
    solution_key,
    solution_path,
    job_key,
    solution,
    fullness_threshold,
    solution_state,
    roll_length_px,
    asset_info,
    roll_info,
    failure_count,
    should_validate,
    max_piece_width_in,
    slack_channel,
    duplicate,
    oldest_asset_days,
    should_assign,
    filtering_rolls,
    fill_allocations,
):
    pending_asset_ids = set(a["airtable_record_id"] for a in asset_info.values())
    total_pending_assets = len(pending_asset_ids)
    # write solution files
    if len(solution) > 0:
        for _, roll_solution in solution.items():
            for n in roll_solution:
                postprocess_nest = (
                    fill_allocations
                    and "block_fuse" not in n.name
                    and "uniform" not in n.name
                    and "running_yardage" not in n.name
                    and material_prop.fabric_type == "Woven"
                )
                ndf = _output_nest_df(
                    n,
                    material_prop,
                    solution_path,
                    postprocess_nest=postprocess_nest,
                )
                _plot_nest(
                    n,
                    ndf,
                    solution_path,
                    n.name + ".png",
                    postprocessed=postprocess_nest,
                )
                if not n.packing.validate():
                    raise ValueError(f"{n.name} is an invalid nesting")
    total_assigned_assets = len(
        set(
            [
                id.split("_")[0]
                for _, s in solution.items()
                for n in s
                for id in n.covered_node_ids()
            ]
        )
    )
    solution_empty = len(solution) == 0 or list(solution.keys()) == ["NO_ROLL_NAME"]
    # write kafka message
    kafka_msg = {
        "material_code": material_prop.material_code,
        "pending_asset_count": total_pending_assets,
        "failed_asset_count": failure_count,
        "created_at": res.utils.dates.utc_now_iso_string(),
        "solution_key": solution_key,
        "solution_status": str(solution_state),
        "fullness_threshold": fullness_threshold,
        "argo_job_key": job_key,
        "total_roll_length_px": sum(
            roll_length_px.get(r, 0) for r, _ in solution.items()
        ),
        "total_roll_length_yd": _px_to_yards(
            sum(roll_length_px.get(r, 0) for r, _ in solution.items())
        ),
        "total_reserved_length_px": sum(
            s.nested_required_length for _, s in solution.items()
        ),
        "total_reserved_length_yd": _px_to_yards(
            sum(s.nested_required_length for _, s in solution.items())
        ),
        # for counting assets - note that a one could have been split into various types of block fusing.
        "total_asset_count": total_assigned_assets,
        "total_healing_pieces": len(
            [
                asset_info[i]
                for _, s in solution.items()
                for i in s.contained_node_ids()
                if asset_info[i]["rank"] == "Healing"
            ]
        ),
        "total_utilization": _total_utilization(solution, roll_length_px),
        "total_length_utilization": 0
        if solution_empty
        else sum(float(s.nested_required_length) for r, s in solution.items())
        / sum(roll_length_px[r] for r, _ in solution.items()),
        "roll_info": [
            {
                "roll_name": r,
                "roll_length_px": roll_length_px.get(r, 0),
                "roll_length_yd": _px_to_yards(roll_length_px.get(r, 0)),
                "reserved_length_px": s.nested_required_length,
                "reserved_length_yd": _px_to_yards(s.nested_required_length),
                "additional_reserved_px": s.additional_reserved_px,
                "additional_reserved_yd": _px_to_yards(s.additional_reserved_px),
                "asset_count": s.asset_count,
                "utilization": 0
                if r == "NO_ROLL_NAME"
                else (
                    s.nested_utilization * s.nested_required_length / roll_length_px[r]
                ),
                "length_utilization": 0
                if r == "NO_ROLL_NAME"
                else (float(s.nested_required_length) / roll_length_px[r]),
                "nest_info": [
                    {
                        "nest_key": n.name,
                        "nest_file_path": f"{solution_path}/{n.name}.feather",
                        "nest_length_px": int(n.height()),
                        "nest_length_yd": _px_to_yards(n.height()),
                        "asset_count": sum(1 for _ in n.covered_node_ids()),
                        "utilization": n.utilization(),
                        "guillotined_sections": n.packing.guillotined_sections(),
                        "asset_info": [
                            asset_info[i]
                            for i in n.covered_node_ids()
                            if "preemptive_heal" not in i
                        ],
                    }
                    for n in sorted(list(s), key=lambda n: n.name)
                ],
            }
            for r, s in solution.items()
        ]
        + [
            {
                "roll_name": r,
                "roll_length_px": roll_length_px[r],
                "roll_length_yd": _px_to_yards(roll_length_px[r]),
                "reserved_length_px": 0,
                "reserved_length_yd": 0.0,
                "additional_reserved_px": 0,
                "additional_reserved_yd": 0.0,
                "asset_count": 0,
                "utilization": 0.0,
                "length_utilization": 0.0,
                "nest_info": [],
            }
            for r in roll_length_px.keys()
            if r not in solution
        ],
    }
    logger.info(kafka_msg)
    res.connectors.load("kafka")[SOLUTION_KAFKA_TOPIC].publish(
        kafka_msg, use_kgateway=True
    )
    s3.write(f"{solution_path}/solution.json", kafka_msg)
    if should_assign:
        all_airtable_nests = []
        if not solution_empty:
            for roll_solution in kafka_msg["roll_info"]:
                roll_record_id = next(
                    rid
                    for rn, _, rid, _ in roll_info
                    if rn == roll_solution["roll_name"]
                )
                if (
                    roll_solution["asset_count"] > 0
                    and (should_validate == False or validate_solution(roll_solution))
                    and os.getenv("RES_ENV") == "production"
                ):
                    airtable_nests = construct_airtable_nests(
                        material_prop.material_code,
                        roll_solution,
                        add_subnests=fill_allocations,
                    )
                    all_airtable_nests.extend(airtable_nests)
                    if solution_state == SolutionState.VALID:
                        if fill_allocations:
                            assign_solution(
                                roll_record_id,
                                roll_solution,
                                airtable_nests,
                                material_prop.material_code,
                                solution_key,
                                kafka_msg["created_at"],
                                fullness_threshold,
                            )
                            post_assignment_observations(roll_solution)
                            if (
                                roll_solution["roll_length_yd"]
                                > roll_solution["reserved_length_yd"] + 1
                            ):
                                ping_slack(
                                    f"[NESTING] detected additional buffer on {roll_solution['roll_name']}: {roll_solution['roll_length_yd']:.2f}yd roll with {roll_solution['reserved_length_yd']} nested. <@U0361U4B84X>",
                                    "autobots",
                                )
                        else:
                            allocate_roll_for_ppu(
                                roll_solution["roll_name"],
                                roll_record_id,
                                min(
                                    roll_solution["roll_length_yd"],
                                    roll_solution["reserved_length_yd"]
                                    + PPU_ALLOCATION_SLOP_YARDS,
                                ),  # some slop
                            )
        if os.getenv("RES_ENV") == "production":
            airtable_solution_state = solution_state
            if solution_state == SolutionState.VALID:
                airtable_solution_state = (
                    "ASSIGNED" if fill_allocations else "PPU_ALLOCATED"
                )
            else:
                airtable_solution_state = str(solution_state).replace(
                    "SolutionState.", ""
                )
            write_solution_to_airtable(
                material_prop.material_code,
                job_key,
                solution_key,
                solution,
                airtable_solution_state,
                roll_info,
                total_pending_assets,
                total_assigned_assets,
                all_airtable_nests,
                solution_empty,
                max_piece_width_in,
                duplicate,
                oldest_asset_days,
            )
            total_length = _px_to_yards(
                sum(s.nested_required_length for _, s in solution.items())
            )
            if solution_state == SolutionState.NO_ROLLS and not filtering_rolls:
                ping_slack(
                    f"No rolls available for nesting {material_prop.material_code} with {total_pending_assets} assets pending and {total_length:.1f} yards -- consider swapping to {get_neighbor_materials(material_prop.material_code)}.",
                    "make_one_team",
                )
                add_contract_variables_to_assets(pending_asset_ids, [CV_NO_ROLLS])
            if slack_channel is not None:
                ping_slack(
                    f"Finished optimus run for {material_prop.material_code} with state {solution_state.name} and length {total_length:.1f} yards, using {total_assigned_assets} of {total_pending_assets} pending assets {'(duplicated)' if duplicate else ''}",
                    slack_channel,
                )
            if solution_state == SolutionState.VALID:
                assigned_asset_ids = set(
                    [
                        i
                        for s in solution.values()
                        for n in s
                        for i in n.covered_node_ids()
                    ]
                )
                unused_assets = set(asset_info.keys()) - assigned_asset_ids
                unused_asset_ids = set(
                    [asset_info[id]["airtable_record_id"] for id in unused_assets]
                )
                if not fill_allocations:
                    add_contract_variables_to_assets(
                        unused_asset_ids, [CV_OPTIMUS_FAILURE]
                    )
            else:
                add_contract_variables_to_assets(
                    pending_asset_ids,
                    [
                        CV_INSUFFIENT_ASSETS
                        if solution_state == SolutionState.INSUFFICIENT_ASSET_LENGTH
                        else CV_OPTIMUS_FAILURE
                    ],
                )
                if fill_allocations:
                    ping_slack(
                        f"[NESTING] <@U0361U4B84X> Failed to fill ppu allocation for {material_prop.material_code}",
                        "autobots",
                    )


def _sanitize_roll_name(r):
    return r.replace(":", "-").replace(" ", "")


def _nest_order(nt):
    # how to order nests on the roll.
    if nt.startswith("block_fuse"):
        return 0
    if nt == "self":
        return 1
    if nt == "running_yardage":
        return 10
    # dont know what this is,
    return 2


@flow_node_attributes(
    "construct_rolls.handler",
    cpu="7500m",
    memory="16G",
    slack_channel="autobots",
    slack_message="<@U0361U4B84X> nesting failed",
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        material = fc.args.get("material_code")
        material_prop = _load_material_info(material, fc.args.get("width_override"))
        nest_knits = fc.args.get("nest_knits", "false").lower() == "true"
        time_limit_s = fc.args.get("time_limit_s", 3600 * 3)
        duplicate = fc.args.get("duplicate", "false").lower() == "true"
        material_swaps = fc.args.get("material_swaps", {})
        should_assign = not fc.args.get("no_assign", False)
        if len(material_swaps) > 0:
            logger.info(f"Swapping {material_swaps} -- be careful.")
        bust_nest_cache = (
            duplicate or fc.args.get("bust_nest_cache", "false").lower() == "true"
        )
        logger.info(f"Constructing rolls for {material}: {material_prop}")
        fill_allocations = fc.args.get("fill_allocations", False)
        rolls = _load_rolls(
            material,
            roll_names=fc.args.get("roll_names", []),
            use_allocated=fill_allocations,
            length_override=fc.args.get("length_override_yd"),
        )
        if len(rolls) == 0 and fill_allocations:
            if not should_assign:
                # special mode where we just see what the optimus solution will be without assignment.
                rolls = _load_rolls(
                    material,
                    roll_names=fc.args.get("roll_names", []),
                    use_allocated=False,
                    length_override=fc.args.get("length_override_yd"),
                )
                if len(rolls) == 0:
                    return
            else:
                logger.info("No rolls require filling - aborting")
                return
        pretreated_roll_names = [n for n, _, _, p in rolls if p]
        data_loader = s3.read(
            fc.get_node_path_for_key("cache", fc.key) + "/cache.pickle"
        )
        (ones_nodes, asset_info, failure_count) = data_loader.get_packing_nodes(
            material_prop,
            nest_knits=nest_knits,
            duplicate=duplicate,
            preemptive_heal=False,  # fill_allocations and not duplicate,
        )
        if len(ones_nodes) == 0:
            logger.warn("Failed to load any assets")
            return
        oldest_asset_days = max(a["days_in_print_station"] for a in asset_info.values())
        roll_length_px = {n: _yards_to_px(l) for n, l, _, _ in rolls}
        nest_cache = NestCache.load(
            material_prop,
            []
            if bust_nest_cache
            else [id for n in ones_nodes.values() for id in n.covered_node_ids()],
            BLOCK_FUSER_MAX_WIDTH_PX * material_prop.stretch_x,
            f"{fc.get_node_path_for_key('nest', fc.key)}/labels",
        )
        reserve_extra_frac = fc.args.get("reserve_frac", 0.0)
        if reserve_extra_frac < 1.0 / len(ones_nodes):
            logger.info(f"Setting extra reserve to zero due to small number of assets.")
            reserve_extra_frac = 0.0
        try:
            solver_end_time = time.time() + time_limit_s
            best_solution = None
            best_solution_status = None
            best_solution_value = None
            for solution, solution_status in itertools.islice(
                nest_into_rolls(
                    ones_nodes,
                    rolls,
                    material_prop,
                    nest_cache,
                    None,
                    ending_ts=solver_end_time,
                    slop_frac=0.01,
                ),
                fc.args.get("max_solutions", MAX_SOLUTIONS),
            ):
                # see if we can reserve extra space.
                if (
                    reserve_extra_frac > 0
                    and solution_status == SolutionState.INSUFFICIENT_ASSET_LENGTH
                ):
                    reserve_extra_px = (
                        sum(s.nested_required_length for s in solution.values())
                        * reserve_extra_frac
                    )
                    max_slop_roll, max_slop_solution = max(
                        solution.items(), key=lambda t: t[1].slop
                    )
                    logger.info(
                        f"Attempting to reserve {reserve_extra_px} extra px on roll {max_slop_roll}"
                    )
                    solution = {
                        **solution,
                        max_slop_roll: max_slop_solution.try_reserve_extra(
                            reserve_extra_px
                        ),
                    }
                offcut_size = max(
                    (
                        s.slop
                        for s in solution.values()
                        if s.slop > s.roll_length * 0.05
                    ),
                    default=0,
                )
                cut_length = min(
                    (
                        s.nested_required_length
                        for s in solution.values()
                        if s.slop > max(s.roll_length * 0.05, 2 * 36 * 300)
                    ),
                    default=0,
                )
                cutting_pretreated = any(
                    n in pretreated_roll_names and s.slop > s.roll_length * 0.05
                    for n, s in solution.items()
                )
                if (
                    solution_status == SolutionState.INSUFFICIENT_ASSET_LENGTH
                    and not cutting_pretreated
                    and cut_length > MIN_PRINTABLE_YARDS * 36 * 300
                    and (
                        offcut_size == 0 or offcut_size > MIN_PRINTABLE_YARDS * 36 * 300
                    )
                ):
                    solution_status = SolutionState.VALID
                # if we are filling the allocation then dont worry about excess length we may have.
                if (
                    solution_status == SolutionState.INSUFFICIENT_ASSET_LENGTH
                    and fill_allocations
                ):
                    solution_status = SolutionState.VALID
                # score solutions by:
                # - number of pretreated rolls consumed
                # - asset value
                # - utilization
                # in lexicographical order.
                solution_value = (
                    len([n for n in solution.keys() if n in pretreated_roll_names]),
                    sum(s.value for _, s in solution.items()),
                    _total_utilization(solution, roll_length_px),
                )
                logger.info(
                    f"Got a solution on rolls {solution.keys()} and with value {solution_value} and status {solution_status} (cutting_pretreated: {cutting_pretreated}, cut_length: {cut_length/(36*300):.2f}, offcut_size: {offcut_size/(36*300):.2f})"
                )
                # lower status = more desirable
                if (
                    best_solution is None
                    or solution_status < best_solution_status
                    or (
                        solution_status == best_solution_status
                        and solution_value > best_solution_value
                    )
                ):
                    best_solution = solution
                    best_solution_status = solution_status
                    best_solution_value = solution_value
        except StopIteration as ex:
            logger.info(f"Failed to find more solutions: {repr(ex)}")
        solution_key = f"{material}-{int(time.time())}"
        logger.info(f"Outputting solution {solution_key}")
        solution_path = f"{fc.get_node_path_for_key('nest', fc.key)}/{solution_key}"
        if best_solution is not None:
            for r, s in best_solution.items():
                labeled_nests = sorted(
                    list(s.labeled_nests()), key=lambda l: _nest_order(l[0])
                )
                for i, (l, n) in enumerate(labeled_nests):
                    n.rename(
                        solution_key,
                        _sanitize_roll_name(r),
                        f"{i}_{l}",
                    )
        _output_roll_solution(
            material_prop,
            solution_key,
            solution_path,
            fc.key,
            {} if best_solution is None else best_solution,
            0.0,
            SolutionState.INFEASIBLE
            if best_solution_status is None
            else best_solution_status,
            roll_length_px,
            asset_info,
            rolls,
            failure_count,
            fc.args.get("should_validate", True),
            max(on.max_piece_width() for on in ones_nodes.values()),
            None
            if fc.args.get("supress_messages", False)
            else fc.args.get("slack_channel"),
            duplicate,
            oldest_asset_days,
            should_assign,
            "roll_names" in fc.args,
            fill_allocations,
        )
        # stopping duplicating plan for now.
        """
        if not duplicate:
            nest_cache.save()
        if (
            best_solution_status == SolutionState.INSUFFICIENT_ASSET_LENGTH
            and sum(s.nested_required_length for s in best_solution.values())
            > (MIN_PRINTABLE_YARDS / 2) * 36 * 300
            and sum(s.nested_required_length for s in best_solution.values())
            < MIN_PRINTABLE_YARDS * 36 * 300
            and not duplicate
        ):
            logger.info(f"attempting to double all assets to get to 10 yard cutoff")
            duplicated_event = dict(event)
            duplicated_event["args"]["duplicate"] = "true"
            return handler(duplicated_event, context)
        """


@flow_node_attributes(
    "construct_rolls.reducer",
)
def reducer(event, context):
    with FlowContext(event, context) as fc:
        solution_paths = [
            p
            for p in s3.ls(fc.get_node_path_for_key("nest", fc.key))
            if p.endswith("solution.json")
        ]
        fill_allocations = fc.args.get("fill_allocations", False)
        total_assets = 0
        total_assigned = 0
        total_length_pending = 0
        total_length_printable = 0
        assigned_lengths = {}
        for p in solution_paths:
            sol = s3.read(p)
            assigned_lengths[sol["material_code"]] = sol["total_reserved_length_yd"]
            total_length_pending += sol["total_reserved_length_yd"]
            total_length_printable += (
                0
                if sol["solution_status"] != "SolutionState.VALID"
                else sol["total_reserved_length_yd"]
            )
            total_assets += sol["pending_asset_count"]
            total_assigned += (
                0
                if sol["solution_status"] != "SolutionState.VALID"
                else sol["total_asset_count"]
            )
        slack_channel = fc.args.get("slack_channel")
        if slack_channel is not None:
            verb = "assigning" if fill_allocations else "allocating space for"
            swaps = suggest_material_swaps(assigned_lengths, MIN_PRINTABLE_YARDS)
            material_swap_suggest = (
                ""
                if fill_allocations or len(swaps) == 0
                else f"  Consider material swaps: {', '.join(f'{k} -> {v}' for k, v in swaps.items())} to allow printing of delayed assets."
            )
            ping_slack(
                f"Finished optimus run for {len(solution_paths)} materials {verb} {total_length_printable:.1f} yards of {total_length_pending:.1f} pending and {total_assigned} of {total_assets} assets.{material_swap_suggest}",
                slack_channel,
            )
    return {}
