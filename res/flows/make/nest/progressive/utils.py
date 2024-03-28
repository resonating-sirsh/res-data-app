import re
import res
import time
import json
import math
import os
import json
import cv2
import numpy as np
import pandas as pd
from res.airtable.print import (
    ROLLS,
    NESTS,
    ASSETS,
    MATERIAL_INFO,
    SOLUTIONS,
    SUBNESTS,
)
from res.airtable.misc import (
    CUT_MARKERS,
    PRODUCTION_REQUESTS,
)
from res.learn.optimization.packing.annealed.progressive import (
    MAX_STITCHED_LENGTH_PX,
    DISTORTION_GRIDS_PER_ROLL,
    DISTORTION_GRID_HEIGHT_PX_UNCOMPENSATED,
    SEPERATOR_HEIGHT_PX_UNCOMPENSATED,
    _px_to_yards,
)
from res.learn.optimization.packing.geometry import translate
from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector
from res.utils import logger, secrets_client, ping_slack
from res.utils.airtable import get_table
from shapely.geometry import box
from tenacity import retry, stop_after_attempt, wait_fixed
from res.flows.make.production.queries import get_one_piece_healing_instance_resolver

HEALING_REQUESTS = get_table(
    "appWxYpvDIs8JGzzr",
    "tblgVQe404mNwsTDs",
)

HEALING_REQUEST_PIECES = get_table(
    "appWxYpvDIs8JGzzr",
    "tblbRvw7OzX1929Qi",
)

PIECE_OBSERVATION_TOPIC = "res_make.piece_tracking.make_piece_observation_request"

ROLL_ASSIGNMENT_TOPIC = "res_make.optimus.assign_nests_to_rolls"

ASSETS_CONTRACT_FIELD = "Material - Contract Variable"

s3 = res.connectors.load("s3")


def _get_print_material(material_code):
    return MATERIAL_INFO.all(formula=f"{{Key}}='{material_code}'", max_records=1)


def stitch_solution(solution):
    """
    Utility function for sending a complete material solution message to the printfile stitching flow.
    """
    for roll_solution in solution["roll_info"]:
        if roll_solution["asset_count"] > 0:
            submit_roll_to_stitching(roll_solution)


def allocate_roll_for_ppu(roll_name, roll_id, cut_to_length_yards):
    if os.getenv("RES_ENV") == "production":
        logger.info(
            f"Updating roll {roll_name} with cut to length: {cut_to_length_yards}"
        )
        ROLLS.update(
            roll_id,
            {
                "Cut To Length (yards)": cut_to_length_yards,
                "Allocated for PPU": True,
                # this isnt really true since there wont be a printfile yet - but some operational views
                # want this to be set to true for rolls we are using in a ppu.
                "Ready for Print": True,
            },
            typecast=True,
        )
    else:
        logger.info(
            f"Would update roll {roll_name} with cut to length: {cut_to_length_yards}"
        )


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3), reraise=True)
def construct_airtable_nests(material_code, roll_solution, add_subnests=False):
    """
    Make the same nest records as what optimus would do.
    """
    s3 = res.connectors.load("s3")
    airtable_payload = [
        {
            "Assets": [
                a["airtable_record_id"]
                for a in nest["asset_info"]
                if a["airtable_record_id"] != "todo"
            ],
            "Material": material_code,
            "Argo Jobkey": nest["nest_key"],
            "Nested Length in Yards": nest["nest_length_yd"],
            "Nesting Utilization Score": nest["utilization"],
            "Dataframe Path": nest["nest_file_path"],
            "Nested Pieces": sum(a["piece_count"] for a in nest["asset_info"]),
            "Print Queue": "TO DO",
            "Asset Path": "TODO",
            "Eligible for Assignment": "true",
            "Nesting Approach": "progressive",
            "Guillotined Sections": nest["guillotined_sections"],
            "Preview": [
                {
                    "url": s3.generate_presigned_url(
                        nest["nest_file_path"].replace(".feather", ".png")
                    )
                }
            ],
        }
        for nest in roll_solution["nest_info"]
    ]
    if os.getenv("RES_ENV") == "production":
        logger.info(f"Creating nest with payload: {airtable_payload}")
        nests = NESTS.batch_create(airtable_payload, typecast=True)
        if add_subnests:
            subnest_payload = [
                {
                    "Nest": [n["id"]],
                    "Section Index": i,
                }
                for n in nests
                for i in range(n["fields"]["Guillotined Sections"])
            ]
            SUBNESTS.batch_create(subnest_payload, typecast=True)
        return nests
    else:
        logger.info(f"Would create airtable nests with payload: {airtable_payload}")
        return []


def validate_solution(roll_solution):
    # make sure that none of the assets have gone away since we constructed the solution.
    asset_record_ids = [
        a["airtable_record_id"]
        for nest in roll_solution["nest_info"]
        for a in nest["asset_info"]
    ]
    invalid_assets = ASSETS.all(
        formula=f"""AND({{Nesting ONE Ready}}=0, FIND({{_record_id}}, "{','.join(asset_record_ids)}"))""",
        max_records=1,
    )
    valid = len(invalid_assets) == 0
    if not valid:
        logger.warn(f"Invalid assets detected: {invalid_assets}")
    return valid


def post_assignment_observations(roll_solution):
    logger.info(
        f"Posting assignment observations for roll {roll_solution['roll_name']}"
    )
    for nest in roll_solution["nest_info"]:
        post_assignment_observations_for_nest(
            roll_solution["roll_name"],
            nest["nest_key"],
            nest["nest_file_path"],
        )


def _post_messages_for_nest(nest_df_path, id_prefixes, common_fields):
    df = s3.read(nest_df_path)
    df = df[~df.asset_key.isnull()]
    df = df[~df.piece_code.isnull()]

    df["one_number"] = df["asset_key"].apply(lambda k: int(k.split("_")[0]))

    try:
        non_zero_piece_instances = get_one_piece_healing_instance_resolver(
            df["one_number"].unique()
        )
    except Exception as ex:
        res.utils.logger.warn(f"Failed to load the healing pieces resolver {ex}")
        non_zero_piece_instances = lambda a, b: None

    for one_number, g in df.groupby("one_number"):
        for id_prefix, common in zip(id_prefixes, common_fields):
            piece_codes_single = [
                r.piece_code for _, r in g.iterrows() if pd.isna(r.wrapped_piece_codes)
            ]
            piece_codes_wrapped = [
                c
                for _, r in g.iterrows()
                if pd.notna(r.wrapped_piece_codes)
                for c in r.wrapped_piece_codes.split(",")
            ]
            kafka_msg = {
                "pieces": [
                    {
                        "code": c,
                        "make_instance": non_zero_piece_instances(one_number, c),
                    }
                    for c in piece_codes_single + piece_codes_wrapped
                ],
                "id": f"{id_prefix}_{one_number}",
                "one_number": one_number,
                "observed_at": res.utils.dates.utc_now_iso_string(),
                **common,
            }
            res.connectors.load("kafka")[PIECE_OBSERVATION_TOPIC].publish(
                kafka_msg, use_kgateway=True
            )


def post_assignment_observations_for_nest(roll_name, nest_key, nest_df_path):
    logger.info(f"Posting assignment observations for nest {nest_key}")
    _post_messages_for_nest(
        nest_df_path,
        [f"roll_assign_{nest_key}"],
        [
            {
                "node": "Make.Print.RollPacking",
                "status": "Exit",
                "metadata": {
                    "roll_name": roll_name,
                    "nest_key": nest_key,
                },
            }
        ],
    )


def post_unassignment_observations_for_nest_by_id(roll_name, nest_record_id):
    try:
        nest_info = NESTS.get(nest_record_id)
        if nest_info:
            post_unassignment_observations_for_nest(
                roll_name,
                nest_info["fields"]["Argo Jobkey"],
                nest_info["fields"]["Dataframe Path"],
            )
    except Exception as ex:
        logger.warn(f"Failed to post unassignment messages: {repr(ex)}")


def post_unassignment_observations_for_nest(roll_name, nest_key, nest_df_path):
    logger.info(f"Posting unassignment observations for nest {nest_key}")
    _post_messages_for_nest(
        nest_df_path,
        [f"roll_unassign_exit_{nest_key}", f"roll_unassign_reentry_{nest_key}"],
        [
            {
                "node": "Make.Print.Printer",
                "status": "Exit",
                "metadata": {
                    "note": "unassigned from roll",
                    "roll_name": roll_name,
                    "nest_key": nest_key,
                },
            },
            {
                "node": "Make.Print.RollPacking",
                "status": "Enter",
                "metadata": {
                    "note": "unassigned from roll",
                    "roll_name": roll_name,
                    "nest_key": nest_key,
                },
            },
        ],
    )


def assign_solution(
    roll_record_id,
    roll_solution,
    airtable_nests,
    material_code,
    solution_key,
    created_at,
    fullness_threshold,
):
    logger.info(f"Assigning solution {roll_solution} with nests {airtable_nests}")
    material_record = _get_print_material(material_code)
    if len(material_record) < 1:
        logger.warn(f"Failed to get material info for material: {material_code}")
        return
    kafka_msg = roll_assignment_message(
        roll_record_id,
        roll_solution,
        {n["fields"]["Argo Jobkey"]: n["id"] for n in airtable_nests},
        material_code,
        material_record[0]["id"],
        solution_key,
        created_at,
        fullness_threshold,
    )
    logger.info(f"Posting kafka message {kafka_msg}")
    res.connectors.load("kafka")[ROLL_ASSIGNMENT_TOPIC].publish(kafka_msg)
    # could be a problem here since the kafka shit is async and now i want to update the rows...
    time.sleep(30)
    try:
        res.connectors.load("hasura").execute(
            """
            mutation MyMutation($assigned_roll_name: String, $nest_keys: [String!]) {
                update_make_nests_many(updates: {where: {nest_key: {_in: $nest_keys}}, _set: {assigned_roll_name: $assigned_roll_name}}) {
                    affected_rows
                }
            }
            """,
            {
                "assigned_roll_name": roll_solution["roll_name"],
                "nest_keys": [n["fields"]["Argo Jobkey"] for n in airtable_nests],
            },
        )
    except Exception as ex:
        logger.warn(f"Failed to update hasura nests with assignment: {repr(ex)}")


def roll_assignment_message(
    roll_record_id,
    roll_solution,
    nest_keys_to_airtable_ids,
    material_code,
    material_record_id,
    solution_key,
    created_at,
    fullness_threshold,
):
    stitched_nest_info = [
        {
            "headers": block_headers,
            "nest_ids": [nest_keys_to_airtable_ids[n["nest_key"]] for n in block_nests],
            "nests_parts": [n["guillotined_sections"] for n in block_nests],
            "roll_offset_px": roll_offset_px,
            "header_offset_idx": header_offset_idx,
        }
        for _, block_headers, block_nests, roll_offset_px, header_offset_idx in _get_stitch_info(
            roll_solution
        )
    ]

    return {
        "roll_record_id": roll_record_id,
        "roll_name": roll_solution["roll_name"],
        "roll_length": roll_solution["roll_length_yd"],
        "nest_record_ids": [
            nest_keys_to_airtable_ids[n["nest_key"]] for n in roll_solution["nest_info"]
        ],
        "nest_keys": [n["nest_key"] for n in roll_solution["nest_info"]],
        "asset_record_ids": [
            a["airtable_record_id"]
            for nest in roll_solution["nest_info"]
            for a in nest["asset_info"]
        ],
        "task_key": solution_key,
        "material_record_id": material_record_id,
        "material_code": material_code,
        "print_files_bijective_with_rolls": False,
        "stitch_nests": True,
        "stitched_nest_info": json.dumps(stitched_nest_info),
        "total_utilization": roll_solution["utilization"],
        "check_total_utilization": fullness_threshold > 0
        and roll_solution["roll_length_yd"] > 20,
        "length_reserved_yards": roll_solution["reserved_length_yd"],
        "length_utilization": roll_solution["length_utilization"],
        "created_at": created_at,
    }


def _get_stitch_info(roll_solution):
    """
    Convert a rolls solution into stitched sections and post to the stitching job.
    """
    stitched_blocks = []
    current_block = []
    current_length = 0
    nests = roll_solution["nest_info"]
    for nest in nests:
        nest_length = nest["nest_length_px"]
        if current_length > 0 and current_length + nest_length > MAX_STITCHED_LENGTH_PX:
            stitched_blocks.append(current_block)
            current_block = []
            current_length = 0
        current_block.append(nest)
        current_length += nest_length
    if len(current_block) > 0:
        stitched_blocks.append(current_block)
    num_stitched_blocks = len(stitched_blocks)
    grids = min(DISTORTION_GRIDS_PER_ROLL, len(nests))
    distortion_grids = [0] * num_stitched_blocks
    distortion_grid_stride = max(1, grids // num_stitched_blocks)
    for i in range(DISTORTION_GRIDS_PER_ROLL):
        distortion_grids[(i * distortion_grid_stride) % num_stitched_blocks] += 1
    for i, block_nests in enumerate(stitched_blocks):
        distortion_grids[i] = min(
            distortion_grids[i], len(block_nests)
        )  # dont print a bunch of headers in a row just to get 3 per roll.
    roll_offset = 0
    header_offset = 0
    for rank, (block_headers, block_nests) in enumerate(
        zip(distortion_grids, stitched_blocks)
    ):
        yield rank, block_headers, block_nests, roll_offset, header_offset
        header_offset += block_headers
        roll_offset += (
            block_headers * DISTORTION_GRID_HEIGHT_PX_UNCOMPENSATED
            + sum(n["nest_length_px"] for n in block_nests)
            + (len(block_nests) - block_headers) * SEPERATOR_HEIGHT_PX_UNCOMPENSATED
        )


def submit_roll_to_stitching(roll_solution):
    """
    Convert a rolls solution into stitched sections and post to the stitching job.
    """
    for (
        rank,
        block_headers,
        block_nests,
        roll_offset_px,
        header_offset_idx,
    ) in _get_stitch_info(roll_solution):
        _stitch_block(
            roll_solution["roll_name"],
            rank,
            block_headers,
            block_nests,
            roll_offset_px,
            header_offset_idx,
        )


def _stitch_block(
    roll_name, rank, num_headers, nests, roll_offset_px, header_offset_idx
):
    job_name = f"stitch-{'' if not roll_name else re.sub('[^0-9a-z]', '-', roll_name.lower())}-{rank}-{int(time.time())}"
    payload = {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {"name": "make.nest.stitch_printfiles", "version": "primary"},
        "assets": [
            {
                "nest_job_key": n["nest_key"],
                "nest_df_path": n["nest_file_path"],
            }
            for n in nests
        ],
        "args": {
            "num_headers": num_headers,
            "roll_name": roll_name,
            "roll_offset_px": roll_offset_px,
            "header_offset_idx": header_offset_idx,
        },
        "task": {"key": job_name},
    }
    logger.info(f"Submitting payload to argo: {payload}")
    ArgoWorkflowsConnector().handle_event(
        payload,
        unique_job_name=job_name,
    )


def write_solution_to_airtable(
    material_code,
    argo_jobkey,
    solution_name,
    solution,
    solution_status,
    rolls_info,
    pending_asset_count,
    total_assigned_assets,
    airtable_nests,
    solution_empty,
    max_piece_width,
    duplicate,
    oldest_asset_days,
):
    total_roll_length = sum(yds for name, yds, _, _ in rolls_info if name in solution)
    payload = {
        "Material Code": material_code,
        "Argo Job Key": argo_jobkey,
        "Solution Name": solution_name,
        "Status": str(solution_status).replace("SolutionState.", ""),
        "Rolls": [rid for name, _, rid, _ in rolls_info if name in solution],
        "Pending Asset Count": pending_asset_count,
        "Assignable Asset Count": total_assigned_assets,
        "Assignable Yards": _px_to_yards(
            sum(s.nested_required_length for _, s in solution.items())
        ),
        "Length Utilization": 0
        if solution_empty
        else _px_to_yards(
            sum(float(s.nested_required_length) for _, s in solution.items())
        )
        / total_roll_length,
        "Nests": [n["id"] for n in airtable_nests],
        "Max Piece Width (in)": max_piece_width,
        "Orders Duplicated": duplicate,
        "Oldest Asset Days": oldest_asset_days,
    }
    SOLUTIONS.create(payload)


def update_production_request(one_number, dxf_paths, block_buffered):
    if os.getenv("RES_ENV") != "production":
        logger.info(f"Would update production request for one {one_number}")
        return
    # get some info thats only in graphql that we need (size code id)
    prod_request_gql = res.connectors.load("graphql").query(
        """
        query f($order_number: String) {
            makeOneProductionRequest(orderNumber: $order_number) {
                id
                size {
                    id
                }
            }

        }
        """,
        {"order_number": one_number},
    )["data"]["makeOneProductionRequest"]
    prod_record = PRODUCTION_REQUESTS.get(prod_request_gql["id"])
    size_id = prod_request_gql["size"]["id"]
    if prod_record:
        PRODUCTION_REQUESTS.update(
            prod_record["id"],
            {
                "Paper Markers": [
                    {"url": s3.generate_presigned_url(u)} for u in dxf_paths
                ]
                + prod_record["fields"].get("Paper Markers", []),
                "Block Buffered": "true" if block_buffered else "false",
                "_dxf_json": json.dumps(
                    [
                        {
                            "fileId": s3.put_resmagic_file(p),
                            "sizeId": size_id,
                        }
                        for p in dxf_paths
                    ]
                    + json.loads(prod_record["fields"].get("_dxf_json", "[]"))
                ),
            },
            typecast=True,
        )
    else:
        logger.warn(f"Unable to find prod request record for asset {one_number}")


def tile_image(artwork_path, height, width):
    npimage = res.connectors.load("s3").read(artwork_path)
    if npimage.dtype == np.uint16:
        npimage = (npimage / 256).astype(np.uint8)
    if height > npimage.shape[0] or width > npimage.shape[1]:
        npimage = np.tile(
            npimage,
            (
                int(np.ceil(height / npimage.shape[0])),
                int(np.ceil(width / npimage.shape[1])),
                1,
            ),
        )
    return npimage[0:height, 0:width, :]


def make_piece_image(shape, artwork_path):
    # tile the artwork so that its a big enough area
    x0, y0, x1, y1 = shape.bounds
    height = int(y1 - y0)
    width = int(x1 - x0)
    npimage = tile_image(artwork_path, height, width)
    if height > npimage.shape[0] or width > npimage.shape[1]:
        npimage = np.tile(
            npimage,
            (
                int(np.ceil(height / npimage.shape[0])),
                int(np.ceil(width / npimage.shape[1])),
                1,
            ),
        )
        npimage = npimage[0:height, 0:width, :]
    # mask off the shape.
    alpha = np.zeros(npimage.shape[0:2], dtype=np.uint8)
    cv2.fillPoly(
        alpha,
        pts=np.array([translate(shape, (-x0, -y0)).boundary.coords], dtype=np.int32),
        color=255,
    )
    alpha = cv2.flip(alpha, 0)
    npimage = np.stack(
        [npimage[:, :, 2], npimage[:, :, 1], npimage[:, :, 0], alpha], axis=-1
    )
    return npimage


def make_bbox_image(rect_width, rect_height, artwork_s3_path, label):
    box_image_uncompensated = tile_image(artwork_s3_path, rect_height, rect_width)
    bbox_image_label = 255 * np.ones(
        (200, box_image_uncompensated.shape[1], box_image_uncompensated.shape[2]),
        np.uint8,
    )
    cv2.putText(
        bbox_image_label,
        label,
        (600, 145),
        cv2.FONT_HERSHEY_SIMPLEX,
        5,
        (0, 0, 0, 255),
        10,
        cv2.LINE_AA,
    )
    combined = cv2.vconcat([box_image_uncompensated, bbox_image_label])
    if combined.shape[2] == 3:
        combined = np.stack(
            [
                combined[:, :, 2],
                combined[:, :, 1],
                combined[:, :, 0],
                255 * np.ones(combined.shape[0:2], dtype=np.uint8),
            ],
            axis=-1,
        )
    return combined


def add_blocks_to_healing_app(
    print_app_record_id, one_number, material_code, block_s3_paths
):
    # TODO: deal with multi block nests.
    healing_request = HEALING_REQUESTS.all(formula=f"{{__one_code}}='{one_number}'")
    if len(healing_request) != 1:
        logger.error(f"Failed to find healing request for {one_number}")
        return
    piece_records = [
        {
            "Request": [healing_request[0]["id"]],
            "Block's Child Pieces": healing_request[0]["fields"]["Request Pieces"],
            "Material Code": material_code,
            "Piece Rank": "Primary",
            "S3 Piece URI": block_path,
            "Piece Iteration": 1,
            "Print Asset ID (Print App)": print_app_record_id,
            "Piece Preview": [
                {
                    "url": s3.generate_presigned_url(
                        block_path.replace(".png", "_small.png")
                    ),
                }
            ],
            "Block Buffer": "true",
        }
        for block_path in block_s3_paths
    ]
    if os.getenv("RES_ENV") != "production":
        logger.info(f"Would write healing app records: {piece_records}")
        return
    added = HEALING_REQUEST_PIECES.batch_create(piece_records, typecast=True)
    # link children back to block
    child_records = [
        {
            "id": rid,
            "fields": {
                "Block Buffer Child": "true",
                "Parent Block": [added[0]["id"]],
            },
        }
        for rid in healing_request[0]["fields"]["Request Pieces"]
    ]
    HEALING_REQUEST_PIECES.batch_update(child_records, typecast=True)


def ping_rob(msg):
    ping_slack(
        f"{msg} - <@U0361U4B84X>",
        "autobots",
    )


def add_contract_variables_to_assets(
    asset_ids,
    contract_variable_ids,
):
    current_cvs = {
        r["id"]: r["fields"].get(ASSETS_CONTRACT_FIELD, [])
        for r in ASSETS.all(
            fields=[ASSETS_CONTRACT_FIELD],
            formula=f"FIND({{_record_id}}, '{','.join(asset_ids)}')",
        )
    }
    ASSETS.batch_update(
        [
            {
                "id": id,
                "fields": {
                    ASSETS_CONTRACT_FIELD: list(
                        set(current_cvs.get(id, []) + contract_variable_ids)
                    ),
                },
            }
            for id in asset_ids
        ]
    )
