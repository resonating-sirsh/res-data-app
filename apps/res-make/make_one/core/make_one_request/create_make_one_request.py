import res
import boto3
import json
from res.airtable.misc import BRANDS, ORDER_LINE_ITEMS, PRODUCTION_REQUESTS
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger, ping_slack, post_with_token, res_hash
from res.utils.dates import utc_days_ago, utc_now_iso_string
from res.flows.meta.ONE.controller import validate_meta_one  # added retry nudge)
from res.flows.meta.ONE.style_node import MetaOne
from res.flows.make.production.queries import (
    set_one_status_fields,
    mark_ones_cancelled,
)
from schemas.pydantic.make import MakeAssetRequest
from .create_one_pdf import get_one_pdf
from . import graphql_queries as queries

s3 = res.connectors.load("s3")

MAKE_ONE_SLA_DAYS = 9


def add_mopr_to_line_item(
    mopr_id,
    one_number,
    order_line_item_id,
):
    ORDER_LINE_ITEMS.update(
        order_line_item_id,
        {
            "_make_one_production_request_id": mopr_id,
            "Allocation Link": f"https://airtable.com/tblptyuWfGEUJWwKk/{mopr_id}",
            "res.Magic.Request Link": f"https://airtable.com/tblptyuWfGEUJWwKk/{mopr_id}",
            "Allocation Match": "ONE placed",
            "Active ONE Number": str(one_number),
        },
        typecast=True,
    )


def get_brand_journey_state(brand_code):
    brand = BRANDS.all(formula=f"{{Code}}='{brand_code}'", fields=["Journey State"])
    if len(brand) > 0:
        return brand[0]["fields"].get("Journey State", [None])[0]
    return None


def create_make_one_request_directly_in_airtable(request: MakeAssetRequest):
    body_code, material_code, color_code, size_code = request.sku.split(" ")
    # first be sure we dont already have a thing for this order line item.
    if request.order_line_item_id is not None:
        existing_record = PRODUCTION_REQUESTS.first(
            formula=f"AND({{_open}}=TRUE(), {{_order_line_item_id}}='{request.order_line_item_id}')"
        )
        if existing_record is not None:
            raise ValueError(
                f"Prod request {existing_record['id']} already exists for order id {request.order_line_item_id}"
            )
    # theres still some style crap in the api (TODO?)
    gql = ResGraphQLClient()
    style = gql.query(queries.QUERY_STYLE, {"id": request.style_id})["data"]["style"]
    if not style["hasBeenMade"]:
        gql.query(
            queries.UPDATE_STYLE_MUTATION,
            {"id": style["id"], "input": {"hasBeenMade": True}},
        )
    logger.info("done updating style")
    # see if the things a TTP order.
    is_ttp = get_brand_journey_state(request.brand_code) == "Prove"
    # create the record in airtable.
    new_request_payload = {
        # stuff about the request
        "_open": True,
        "RequestType": request.request_type,
        "Request Name": request.request_name,
        "Sales Channel": request.sales_channel,
        "Order Priority Type": "Prove"
        if is_ttp
        else (request.priority_type or "Normal"),
        "_order_line_item_id": request.order_line_item_id
        or res_hash(request.json().encode()),
        "Color Placed in 3D": True,
        "_request_pending_assets": True,
        "Botless Print Flow": True,
        "Digital Asset Status": "Processing Request",
        "Factory": "res.Factory.DR ✅",
        "Allocation Status": request.allocation_status or "Reserved",
        "Line Item ID": request.channel_line_item_id,
        "_channel_order_id": request.channel_order_id,
        "_original_request_placed_at": request.ordered_at,
        "Belongs to Order": request.order_name,
        "Exit Factory Date": utc_days_ago(-MAKE_ONE_SLA_DAYS).isoformat(),
        "_request_processed_at": utc_now_iso_string(),
        "__brandcode": request.brand_code,
        "Brand": request.brand_name,
        # stuff about the sku
        "SKU": request.sku.replace("-", "", 1),
        "Body Code": body_code,
        "Material Code": material_code,
        "Color Code": color_code,
        "Size Code": size_code,
        # style stuff
        "Body Version": request.body_version,
        "Style Code": f"{body_code} {material_code} {color_code}",
        "styleId": request.style_id,
        "Style Version Number": request.style_version,
        "Pattern Version Number": style["body"]["patternVersionNumber"],
        "Styles": style["name"],
        "Body": style["body"]["name"],
        "Color": style["color"]["name"],
        "Print Type": (
            None if style["printType"] is None else style["printType"].capitalize()
        ),
        "Body Category": style["body"].get("category", {}).get("name"),
        "Body Pieces": style["body"].get("numberOfPieces"),
        "Number of Body Operations": style["body"].get("numberOfOperations"),
        # photos
        "Production Cover Photo": [
            {"url": style["color"]["images"][0]["largeThumbnail"]},
            {"url": style["body"]["images"][0]["largeThumbnail"]},
        ]
        + [{"url": a.get("url")} for a in style.get("coverImages", [])[0:1]],
        "ArtworkFile Preview": [{"url": style["color"]["images"][0]["largeThumbnail"]}],
        # dxf
        "Paper Markers": [
            {"url": res.connectors.load("s3").generate_presigned_url(p)}
            for p in MetaOne.get_cut_file_list_from_params(
                body_code, request.body_version, size_code
            )
        ],
    }
    response = PRODUCTION_REQUESTS.create(new_request_payload)
    if request.order_line_item_id is not None:
        add_mopr_to_line_item(
            response["id"],
            response["fields"]["Order Number V2"],
            request.order_line_item_id,
        )
        logger.info(f"added prod request to line item {request.order_line_item_id}")
    if is_ttp:
        ping_slack(
            f"[TTP] - making a time to prove one for {request.brand_code} -- {response['fields']['Order Number V2']} <@U0361U4B84X>",
            "autobots",
        )
    return response


def _three_part_sku(s):
    if s:
        return " ".join(s.split(" ")[:3])


def try_place_apply_color_request(make_one_record):
    is_request_pending_assets = True
    try:
        return place_apply_color_request(make_one_record)
    except Exception as err:
        import traceback

        is_request_pending_assets = True
        logger.warn(f"an error ocurred while creating apply color request: {err}")
        logger.info(
            f"Flagging production request {make_one_record['id']} {traceback.format_exc()}"
        )
        PRODUCTION_REQUESTS.update(
            make_one_record["id"],
            {
                "Flag For Review": True,
                "Flag for Review Reason": f"🤖 An Error ocurred while trying to place apply color request (dxa), please investigate: {err} {traceback.format_exc()}",
                "Flag for Review: Tag": ["Error trying to create DxA request"],
            },
        )
        ping_slack(
            f"Failed to place apply color request {repr(err)}",
            "autobots",
        )
    return None, None, is_request_pending_assets


def get_factory_order_s3_path(one_number, presign=False):
    pdf_path = s3.make_one_production_path(one_number, "factory-order.pdf")
    if presign:
        return s3.generate_presigned_url(pdf_path)
    else:
        return pdf_path


def add_to_assembly_queue(mopr_id, one_number):
    try:
        lock = res.connectors.load("redis").try_get_lock(
            f"LOCK:ASSY_QUEUE:{mopr_id}:{one_number}", timeout=2 * 60
        )
        if not lock or not lock.owned():
            raise Exception("Failed to get redis lock")
        # make sure the prod request is still open.
        prod_req = PRODUCTION_REQUESTS.get(mopr_id)
        if not prod_req or not prod_req["fields"].get("_open", False):
            logger.info(f"{one_number} is a closed request")
            mark_ones_cancelled([mopr_id])
            return
        pdf_path = get_factory_order_s3_path(one_number, presign=False)
        pdf_path_presigned = get_factory_order_s3_path(one_number, presign=True)
        get_one_pdf(mopr_id, pdf_path)
        PRODUCTION_REQUESTS.update(
            mopr_id,
            {
                "PRINTQUEUE": "PREPARE MATERIALS",
                "DxA Exit Date": utc_now_iso_string(),
                "Assembly Node Status": "To Do",
                "Digital Asset Status": "In Make Queue",
                "__move_to_assembly_queue_bot_ts": utc_now_iso_string(),
                "Factory Order PDF": [{"url": pdf_path_presigned}],
            },
        )
        set_one_status_fields(one_number, entered_assembly_at=utc_now_iso_string())
        response = post_with_token(
            f"https://data.resmagic.io/make-one/print_assets/{one_number}",
            secret_name="RES_META_ONE_API_KEY",
        )
        response.raise_for_status()
        response_json = response.json()
        if "status" not in response_json:
            raise Exception(f"Got a bad response: {response_json}")
        if response_json["status"] != "CREATED":
            raise Exception(f"Got response with status: {response_json['status']}")
        logger.info(f"Added {one_number} to assembly queue")
        ping_slack(
            f"[MAKE-ONE] Added {one_number} to assembly queue",
            "autobots",
        )
    except Exception as ex:
        if "Order no longer exists" in repr(ex):
            mark_ones_cancelled([mopr_id])
            ping_slack(
                f"[MAKE-ONE] <@U0361U4B84X> {one_number} is seemingly canceled - deleted it from hasura",
                "autobots",
            )
        else:
            logger.warn(f"Failed to add {one_number} to assembly queue: {repr(ex)}")
            ping_slack(
                f"[MAKE-ONE] <@U0361U4B84X> Failed to add {one_number} to assembly queue: `{repr(ex)}`",
                "autobots",
            )


def sync_make_one_request_to_mongo(request_id):
    """
    Some stuff still needs the graph api (apply color request)
    and it breaks if mongo isnt up to date with airtable seemingly.
    """
    client = boto3.client("lambda", region_name="us-east-1")

    client.invoke(
        FunctionName="res-meta-prod-sync-mongodb-collection-from-airtable",
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {"record_id": request_id, "table_id": "tblptyuWfGEUJWwKk", "dry_run": False}
        ),
    )


def place_apply_color_request(make_one_record):
    dxa_graphics_request_ids = []
    request_update = {}
    is_request_pending_assets = True

    is_meta_one_ready, m1_contracts_failing_codes = validate_meta_one(
        make_one_record["fields"]["styleId"],
        make_one_record["fields"]["SKU"],
        body_version=make_one_record["fields"]["Body Version"],
    )

    if not is_meta_one_ready:
        res.utils.logger.metric_node_state_transition_incr(
            "Meta.One.Ready.Request",
            _three_part_sku(make_one_record["fields"]["SKU"]),
            "FAIL",
        )

    logger.info(
        f"is meta.one ONE ready?: {is_meta_one_ready} {m1_contracts_failing_codes}"
    )
    if not is_meta_one_ready:
        # Create DXA Request
        contracts_vars_ids = []

        if len(m1_contracts_failing_codes) > 0:
            # parse contract variable codes to ids
            cv_formula = (
                "OR("
                + ",".join([f"Code='{c}'" for c in m1_contracts_failing_codes])
                + ")"
                if len(m1_contracts_failing_codes) > 0
                else None
            )
            from res.connectors.airtable.AirtableClient import ResAirtableClient

            airtable_client = ResAirtableClient()
            contracts_vars = list(
                airtable_client.get_records(
                    "appqtN4USHTmyC6Dv",
                    "tbl4s194nIMut9xoA",
                    filter_by_formula=cv_formula,
                    fields=["Variable Name", "Code"],
                )
            )
            contracts_vars_ids = [cv["id"] for cv in contracts_vars]

        is_request_pending_assets = True
        gql = ResGraphQLClient()
        create_dxa_request_response = gql.query(
            queries.CREATE_APPLY_COLOR_REQUEST_MUTATION,
            {
                "input": {
                    "styleId": make_one_record["fields"]["styleId"],
                    "styleVersion": make_one_record["fields"]["Style Version Number"],
                    "hasOpenOnes": True,
                    "requestsIds": [make_one_record["id"]],
                    "requestType": make_one_record["fields"]["RequestType"],
                    "requestReasonsContractVariablesIds": contracts_vars_ids,
                }
            },
        )

        logger.info(f"create dxa request: {create_dxa_request_response}")

        apply_color_request = create_dxa_request_response["data"][
            "createApplyColorRequest"
        ]["applyColorRequest"]
        request_update = {
            "_request_pending_assets": is_request_pending_assets,
            "_asset_request_id": apply_color_request["id"],
        }
    else:
        is_request_pending_assets = False
        request_update = {
            "_request_pending_assets": is_request_pending_assets,
        }
        res.utils.logger.info(f"Got valid one")
        res.utils.logger.metric_node_state_transition_incr(
            "Meta.One.Ready.Request",
            _three_part_sku(make_one_record["fields"]["SKU"]),
            "OK",
        )

    resp = PRODUCTION_REQUESTS.update(
        make_one_record["id"],
        request_update,
        typecast=True,
    )

    logger.info(f"updated make one production request: {resp}")

    return (
        resp,
        (dxa_graphics_request_ids[0] if len(dxa_graphics_request_ids) > 0 else None),
        is_request_pending_assets,
    )


def norm_sku(s):
    """
    dont forget to norm the sku - there should be a hyphen in the body code part
    """
    if s[2] != "-":
        return f"{s[:2]}-{s[2:]}"
    return s
