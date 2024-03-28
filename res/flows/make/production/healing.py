import res
import json
import traceback
import numpy as np
import pandas as pd
from res.airtable.healing import (
    REQUESTS,
    REQUEST_PIECES,
    PIECES_WITH_ZONES,
    ONES,
    get_defect_id_to_name_map,
)
from res.airtable.misc import PRODUCTION_REQUESTS
from res.connectors.s3 import S3Connector
from res.flows import flow_node_attributes, FlowContext
from res.flows.make.production.queries import get_one_number_record
from res.utils import logger, ping_slack

from .print_request import create_print_asset_requests

"""
Add the healing request and healing request pieces to the healing app.
Previously done by some bs bot.
"""

s3 = S3Connector()


def generate_healing_request_from_ppp_response(ppp_response):
    one_number = ppp_response["one_number"]
    request = REQUESTS.first(formula=f"""{{__one_code}}='{one_number}'""")
    if request is not None:
        logger.info(f"Healing request already generated for {one_number}.")
    else:
        one = ONES.first(formula=f"{{Order Number V2}}='{one_number}'")
        if one is None:
            logger.info(f"One not synced to airtable yet - will need to retry.")
            return
        request = REQUESTS.create(
            {
                "__one_code": one_number,
                "Reporting Node": "Print",
                "_make_one_request_id": ppp_response["upstream_request_id"],
                "__print_app_request_id": ppp_response["id"],
                "ONE": [one["id"]],
                "PPP Flow": ppp_response.get("flow"),
            }
        )
        logger.info(f"Created new healing request for {one_number}: {request}")
    if "flow" in ppp_response and ppp_response["flow"] == "v2":
        generate_healing_request_pieces_from_ppp_response_v2(request, ppp_response)
    else:
        generate_healing_request_pieces_from_ppp_response_v1(request, ppp_response)


def munge_filename_for_healing_app(filename):
    # the old healing app uses the bucket-free filename as the identifier for a piece.
    return filename.replace("s3://res-data-production/", "")


def generate_healing_request_pieces_from_ppp_response_v1(request, ppp_response):
    healing_request_id = request["id"]
    one_number = ppp_response["one_number"]
    logger.info(
        f"Generating healing request pieces for {one_number} based on v1 ppp flow"
    )
    existing_pieces = REQUEST_PIECES.all(
        fields=["S3 Piece URI"], formula=f"""{{ONE Code}}='{one_number}'"""
    )
    existing_pieces = set(r["fields"]["S3 Piece URI"] for r in existing_pieces)
    logger.info(f"Found existing pieces for {one_number}: {existing_pieces}")
    all_piece_codes = [p["piece_code"] for p in ppp_response["make_pieces"]]
    zone_map = get_zones_for_piece_codes(all_piece_codes)
    piece_payload = [
        {
            "Request": [healing_request_id],
            "One Number": one_number,
            "Print Asset ID (Print App)": piece["asset_id"],
            "Healing Request Node": "Roll Inspection",
            "S3 Piece URI": munge_filename_for_healing_app(piece["filename"]),
            "ONE Marker Rank": ppp_response["piece_type"],
            "Piece Rank": ppp_response["metadata"]["rank"],
            "Piece Preview": [{"url": s3.generate_presigned_url(piece["filename"])}],
            "Material Code": ppp_response["material_code"],
            "Healing Request Node": "Roll Inspection",
            "Piece Iteration": 1,
            "__piece_counter": i,
            "Piece Code": piece["piece_code"],
            "Piece Key": piece["piece_name"],
            "Piece Id": piece["piece_id"],
            "Zone Info": [zone_map.get(piece["piece_code"])],
            "PPP Flow": ppp_response.get("flow"),
            "Piece Area": piece["piece_area_yds"],
        }
        for i, piece in enumerate(ppp_response["make_pieces"])
        if munge_filename_for_healing_app(piece["filename"]) not in existing_pieces
    ]
    logger.info(f"Piece payload for {one_number}: {piece_payload}")
    REQUEST_PIECES.batch_create(piece_payload)


def generate_healing_request_pieces_from_ppp_response_v2(request, ppp_response):
    healing_request_id = request["id"]
    one_number = ppp_response["one_number"]
    print_app_id = ppp_response["id"]
    logger.info(
        f"Generating healing request pieces for {one_number} based on v2 ppp flow"
    )
    existing_pieces = REQUEST_PIECES.all(
        fields=["Piece Key", "Piece Iteration"],
        formula=f"""{{Print Asset ID (Print App)}}='{print_app_id}'""",
    )
    existing_pieces = set(
        (r["fields"]["Piece Key"], r["fields"]["Piece Iteration"])
        for r in existing_pieces
    )
    logger.info(f"Found existing healing piece records {existing_pieces}")
    all_piece_codes = [p["piece_code"] for p in ppp_response["make_pieces"]]
    zone_map = get_zones_for_piece_codes(all_piece_codes)
    piece_payload = [
        {
            "Request": [healing_request_id],
            "One Number": one_number,
            "Print Asset ID (Print App)": print_app_id,
            "Healing Request Node": "Roll Inspection",
            "Piece Rank": ppp_response["metadata"]["rank"],
            "Piece Preview": [{"url": s3.generate_presigned_url(piece["filename"])}],
            "Material Code": ppp_response["material_code"],
            "Healing Request Node": "Roll Inspection",
            "Piece Iteration": piece["make_sequence"],
            "Piece Code": piece["piece_code"],
            "Piece Key": piece["piece_name"],
            "Piece Id": piece["piece_id"],
            "Zone Info": [zone_map.get(piece["piece_code"])],
            "PPP Flow": ppp_response.get("flow"),
            "Is piece a Healing?": ppp_response["metadata"]["rank"].lower()
            == "healing",
            "Piece Area": piece.get("piece_area_yds") or -1.0,
            "Cache Path": piece.get("meta_one_cache_path"),
            "Piece Type": piece.get("piece_type"),
        }
        for piece in ppp_response["make_pieces"]
        if (piece["piece_name"], piece["make_sequence"]) not in existing_pieces
        and piece["piece_code"] in zone_map
    ]
    all_piece_codes = [p["piece_code"] for p in ppp_response["make_pieces"]]
    bad_pieces = [c for c in all_piece_codes if c not in zone_map]
    if len(bad_pieces) > 0:
        logger.warn(f"Missing zone info for pieces: {bad_pieces} -- will not be healed")
        ping_slack(
            f"Missing zone info for piece codes {bad_pieces} -- will not be healed until fixed",
            "assemble_node",
        )
    logger.info(f"Piece payload for {one_number}: {piece_payload}")
    REQUEST_PIECES.batch_create(piece_payload)


def get_zones_for_piece_codes(piece_codes):
    if len(piece_codes) == 0:
        return {}
    zone_info = PIECES_WITH_ZONES.all(
        fields=["Generated Piece Code"],
        formula=f"AND({{Generated Piece Code}}!='', find({{Generated Piece Code}}, '{','.join(piece_codes)}'), {{Commercial Acceptability Zone (from Part Tag)}}!='')",
    )
    return {r["fields"]["Generated Piece Code"]: r["id"] for r in zone_info}


# get the set of pieces from airtable which are marked as needing healing.
def get_healing_piece_df():
    # note that sku can be empty if the one went out of make one production.... the linked record goes away and then the sku goes away
    # we can interpret this to mean that we no longer need to heal the piece.
    return REQUEST_PIECES.df(
        formula="""
        AND(
            {PPP Flow}='v2',
            {_heal_piece}='Heal Piece',
            {__generated_print_asset_request_id}='',
            {SKU}!=''
        )
        """,
        field_map={
            "Request": "print_request_id",
            "Material Code": "material_code",
            "Piece Iteration": "make_sequence",
            "Piece Code": "piece_code",
            "Piece Key": "piece_name",
            "Piece Id": "piece_id",
            "Piece Type": "piece_type",
            "SKU": "sku",
            "ONE Code": "one_number",
        },
    )


def mark_healing_prod_request(one_number):
    prod_request = PRODUCTION_REQUESTS.first(
        formula=f"{{Order Number V2}}='{one_number}'"
    )
    if prod_request:
        PRODUCTION_REQUESTS.update(
            prod_request["id"],
            {
                "Healing Created?": True,
            },
        )


def try_body_version_from_healing_pieces(healing_pieces):
    """

    Parse the body version out of the piece name e.g. 'LA-6009-V1-SKTBKPNLLF-LN' -> 1
    Assume if there is a list of healing pieces for a ONE we are healing on the same version
    possibly we can error out if not but then we have a different problem

    """
    try:
        if isinstance(healing_pieces, list) or isinstance(healing_pieces, np.ndarray):
            healing_pieces = healing_pieces[0]
        return int(healing_pieces.split("-")[2].replace("V", ""))
    except:
        return None


def place_healing_requests():
    heal_df = get_healing_piece_df()
    if heal_df.empty:
        return
    # deal with missing piece types since they were added recently.
    heal_df["piece_type"] = heal_df.apply(
        lambda r: r["piece_type"]
        if r.get("piece_type") is not None
        else "block_fuse"
        if "-BF" in r["piece_code"]
        else "self",
        axis=1,
    )
    heal_df["sku"] = heal_df["sku"].apply(lambda l: l[0])
    for (one_number, sku), gdf in heal_df.groupby(["one_number", "sku"]):
        try:
            gdf["piece_specs"] = gdf.apply(
                lambda r: {
                    "key": r.piece_name,
                    "make_sequence_number": r.make_sequence + 1,
                },
                axis=1,
            )
            piece_specs = (
                gdf.groupby(["material_code", "piece_type"])["piece_specs"]
                .apply(list)
                .to_dict()
            )

            logger.info(
                f"Creating healing print assets for {one_number} pieces {piece_specs}"
            )
            # rh - need to set the ordered at timestamp to properly prioritze the healing pieces
            mone_prod_data = get_one_number_record(one_number)
            _, healing_asset_ids_to_piece_names = create_print_asset_requests(
                one_number,
                sku,
                piece_specs,
                is_healing=True,
                add_to_existing_print_request=True,
                plan=False,
                ppp_flow="v2",
                original_order_timestamp=mone_prod_data["ordered_at"]
                .date()
                .strftime("%Y-%m-%d"),
                sales_channel=mone_prod_data["sales_channel"],
            )
            piece_name_to_healing_asset = {
                v: k for k, vs in healing_asset_ids_to_piece_names.items() for v in vs
            }
            logger.info(
                f"Created healing print assets {piece_name_to_healing_asset} for {one_number}"
            )
            healing_update = [
                {
                    "id": r.id,
                    "fields": {
                        "__generated_print_asset_request_id": piece_name_to_healing_asset[
                            r.piece_name
                        ],
                    },
                }
                for _, r in gdf.iterrows()
            ]
            logger.info(f"Sending healing update {healing_update}")
            REQUEST_PIECES.batch_update(healing_update)
            mark_healing_prod_request(one_number)
            ping_slack(
                f"[HEALING] Requested {gdf.shape[0]} pieces for {one_number}",
                "autobots",
            )
        except Exception as ex:
            logger.error(f"Failed to heal {one_number}: {repr(ex)}")
            ping_slack(
                f"[HEALING] <@U0361U4B84X> Failed to heal {one_number}: {traceback.format_exc()}",
                "autobots",
            )


def retry_create_healing_records(print_asset_id):
    resp = res.connectors.load("snowflake").execute(
        f"""
        select
            *
        from
            IAMCURIOUS_PRODUCTION.PREP_PIECES_RESPONSES_QUEUE
        where
            "id" = '{print_asset_id}'
        order by "created_at" desc
        limit 1
        """
    )
    resp = json.loads(resp["RECORD_CONTENT"][0])
    generate_healing_request_from_ppp_response(resp)


# cron job which just generates the healing assets in the print app and healing app.
@flow_node_attributes(
    "healing.handler",
    slack_channel="autobots",
    slack_message="<@U0361U4B84X> healing asset generation failed",
    mapped=False,
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        place_healing_requests()


# utility method.
def get_roll_inspection_results(one_numbers):
    df = res.connectors.load("snowflake").execute(
        f"""
        select
            "__one_code",
            "Piece Name",
            "_heal_piece",
            "v31_All_Defects",
            "v32_All_Defects",
            "Roll Inspection Matrix Resolution"
        from
            IAMCURIOUS_DB.IAMCURIOUS_PRODUCTION.AIRTABLE__HEALING_APP__REQUEST_PIECES
        where
            "__one_code" in ({','.join(f"'{on}'" for on in one_numbers)})
        """
    )
    defect_id_to_name_map = get_defect_id_to_name_map()
    tolist = (
        lambda x: json.loads(x) if x is not None else []
    )  # fucking shit pandas fucking chrsit.
    df["all_defects"] = df.apply(
        lambda r: tolist(r.get("v31_All_Defects")) + tolist(r.get("v32_All_Defects")),
        axis=1,
    )
    df["defect_names"] = df.apply(
        lambda r: [defect_id_to_name_map[i] for i in set(r["all_defects"])], axis=1
    )
    df["was_healed"] = df["_heal_piece"] == "Heal Piece"
    df = df.rename(
        columns={
            "__one_code": "one_number",
            "Piece Name": "piece_name",
        }
    )
    return df[["one_number", "piece_name", "was_healed", "defect_names"]]
