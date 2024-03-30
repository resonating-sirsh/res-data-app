"""
MAKE API..............
"""

from __future__ import annotations

import os
import uuid
import time
import traceback
from warnings import filterwarnings
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from res.flows.make.inventory.rolls_loader import RollLoader
from res.flows.make.inventory.inventory_validation import (
    get_material_info_for_code_or_name,
    get_can_place_order_for_request,
)
from core.make_one_request.create_make_one_request import (
    add_mopr_to_line_item,
    add_to_assembly_queue,
    create_make_one_request_directly_in_airtable,
    get_factory_order_s3_path,
    sync_make_one_request_to_mongo,
    try_place_apply_color_request,
)
from core.make_one_request.graphql_queries import process_mopr
from core.make_one_request.reproduce_make_one_request import reproduce_one
from core.nesting.terminated_job import mark_partial_printing_in_healing_app
from core.print_request import cancel_print_request
from fastapi import APIRouter, Body, Depends, FastAPI, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi_utils.tasks import repeat_every
from structlog.contextvars import bound_contextvars

from schemas.pydantic.make import (
    MakeAssetRequest,
    MakeAssetStatus,
    OneOrderStatus,
    OneOrderStatusNodeAndExpDate,
    PieceAssetRequest,
    UnassignRollResponse,
    MaterialInfo,
    OrderInfoRequest,
    OrderMaterialInfoResponse,
    UnassignRollsRequest,
    UnassignRollsResponse,
)

import res
from res.airtable.misc import PRODUCTION_REQUESTS
from res.connectors.graphql.ResGraphQLClient import GraphQLException, ResGraphQLClient
from res.flows.make.nest.clear_nest_roll_assignments import clear_nest_roll_assignments
from res.flows.make.production.inbox import reconcile_order_for_sku
from res.flows.make.production.locked_settings_validation import (
    locked_settings_validator,
)
from res.flows.make.production.print_request import create_print_asset_requests
from res.flows.make.production.queries import (
    add_one_number_record,
    get_one_number_record,
    get_one_number_record_by_order_line_item_id,
    get_ones_to_retry_assembly_enter,
    mark_ones_cancelled,
    mark_ones_dxa_ready,
    set_one_status_fields,
)
from res.flows.meta.ONE.meta_one import MetaOne
from res.learn.optimization.schedule.ppu import ppu_renest_roll, ppu_replace_roll
from res.utils import ping_slack, run_job_async_with_context, secrets_client
from res.utils.dates import utc_days_ago, utc_now_iso_string
from res.utils.logging.ResLogger import add_contextvar_to_log_message

filterwarnings("ignore")

# bumpo: 21 .....

# set up some logging preprocessors to add the app name and request id to log lines.
res.utils.logger.reset_processors()  # so we dont add these every time uvicorn reloads.
res.utils.logger.prepend_processor(add_contextvar_to_log_message("app_name"))
res.utils.logger.prepend_processor(add_contextvar_to_log_message("request_id"))


def verify_api_key(key: str):
    RES_META_ONE_API_KEY = secrets_client.get_secret("RES_META_ONE_API_KEY")

    if key == RES_META_ONE_API_KEY:
        return True
    else:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid << RES_META_ONE_API_KEY >>.  Please refresh this value from AWS secrets manager",
        )


app = FastAPI(
    title="Make ONE API",
    openapi_url="/make-one/openapi.json",
    docs_url="/make-one/docs",
)
security = HTTPBearer()
api_router = APIRouter()


PROCESS_NAME = "make-one"

# storing the roll loader in a global variable so we can reload it and keep it in memory for fast access
roll_loader = None


@app.on_event("startup")
@repeat_every(seconds=60 * 60)  # every hour
async def reload_roll_loader():
    global roll_loader
    roll_loader = RollLoader()
    res.utils.logger.info("Reloaded roll loader")


def get_current_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if not verify_api_key(token):
        raise HTTPException(
            status_code=401,
            detail="Invalid << RES_META_ONE_API_KEY >> in token check. Check AWS Secrets for this value",
        )
    return token


@app.middleware("http")
async def handle_with_logs(request: Request, call_next):
    request_id = request.headers.get("X-Request-Id", str(uuid.uuid4()))
    with bound_contextvars(request_id=request_id, app_name=PROCESS_NAME):
        if request.scope["path"] != f"/{PROCESS_NAME}/healthcheck":
            # need to figure out why ALB sends 5 RPS of health checks.
            res.utils.logger.info(
                f"Got request {request.scope['path']} from {request.client.host}."
            )
        t0 = time.time()
        response = await call_next(request)
        dt = time.time() - t0
        if request.scope["path"] != f"/{PROCESS_NAME}/healthcheck":
            res.utils.logger.info(
                f"Request processed in {dt:.2f} seconds for path {request.scope['path']} from {request.client.host}."
            )
            prom_name = ".".join(
                p
                for p in request.scope["path"].split("/")
                if not all(c.isdigit() for c in p)
            )
            res.utils.logger.timing(
                prom_name + ".time",
                dt * 1000,
            )
            res.utils.logger.incr(
                prom_name + ".requests",
            )
        if dt > 60.0:
            ping_slack(
                f"[MAKE-ONE] <@U0361U4B84X> Long running request {request_id} took {dt:.2f} seconds for path {request.scope['path']}",
                "autobots",
            )

        response.headers["X-Request-Id"] = request_id
        return response


def _retry_assembly_add_sync():
    try:
        lock = res.connectors.load("redis").try_get_lock(
            f"LOCK:ASSY_QUEUE:RETRY_ALL", timeout=10 * 60
        )
        if not lock or not lock.owned():
            raise Exception("Failed to get redis lock")
        for one in get_ones_to_retry_assembly_enter(utc_days_ago(5).isoformat()):
            res.utils.logger.info(
                f"Retrying to add {one['one_number']} to assembly queue."
            )
            ping_slack(
                f"[MAKE-ONE] Retrying to add {one['one_number']} to assembly queue.",
                "autobots",
            )
            add_to_assembly_queue(one["airtable_rid"], one["one_number"])
        # something happened once when we didnt get notified of unblocked requests to lets be sure to do it here.
        unblocked_ids = ResGraphQLClient().query(
            """
            query {
                makeOneProductionRequests(where: {isOpen: {is: true}, printRequestId: {is: null}, isRequestPendingAssets: {is: false}}, first: 100) {
                    makeOneProductionRequests {
                        id
                        sku
                    }
                }
            }
            """,
            {},
        )["data"]["makeOneProductionRequests"]["makeOneProductionRequests"]
        unblocked_ids = [
            r["id"] for r in unblocked_ids if not r["sku"].startswith("TT")
        ]
        if len(unblocked_ids) > 0:
            ping_slack(
                f"[MAKE-ONE] <@U0361U4B84X> Unblocking some assets of which make-one was not notified",
                "autobots",
            )
            unblock_in_dxa(unblocked_ids)
    except:
        pass
    if lock and lock.owned():
        lock.release()


# method to periodically retry the assembly add for things that failed.
@api_router.post(
    f"/{PROCESS_NAME}/retry_assembly_add",
    status_code=200,
)
def retry_assembly_add(
    token: str = Security(get_current_token),
):
    run_job_async_with_context(_retry_assembly_add_sync)
    return "ok"


@app.get(f"/{PROCESS_NAME}/healthcheck")
async def healthcheck():
    return {"status": "ok"}


@api_router.post(
    f"/{PROCESS_NAME}/notify_dxa_request_complete",
    status_code=200,
)
def unblock_in_dxa(
    production_request_ids: list[str],
    token: str = Security(get_current_token),
):
    res.utils.logger.info(f"Unblocking assets: {production_request_ids}")
    ping_slack(
        f"[MAKE-ONE] Unblocking assets from dxa: {production_request_ids}",
        "autobots",
    )
    for updated in mark_ones_dxa_ready(production_request_ids):
        run_job_async_with_context(
            add_to_assembly_queue,
            mopr_id=updated["airtable_rid"],
            one_number=updated["one_number"],
        )
    return "ok"


@api_router.post(
    f"/{PROCESS_NAME}/rolls/unassign",
    response_model=UnassignRollsResponse,
    status_code=200,
)
def unassign_rolls(
    request: UnassignRollsRequest,
    token: str = Security(get_current_token),
):
    roll_responses = []
    for roll_name in request.roll_names:
        res.utils.logger.info(f"Unassigning roll: {roll_name}")
        total_assets, total_nests, total_pfs = clear_nest_roll_assignments(
            ["all"],
            [roll_name],
            os.environ.get("RES_ENV") != "production",
            notes=request.notes,
            asset_contract_variables=(
                [] if request.contract_variables is None else request.contract_variables
            ),
        )
        roll_responses.append(
            UnassignRollResponse(
                roll_name=roll_name,
                unassigned_assets=total_assets,
                unassigned_nests=total_nests,
                unassigned_printfiles=total_pfs,
            )
        )
    return UnassignRollsResponse(rolls_info=roll_responses)


@api_router.post(
    f"/{PROCESS_NAME}/ppu/reassign",
    status_code=200,
)
def reassign_roll_to_ppu(
    roll_name: str = Body(embed=True),
    token: str = Security(get_current_token),
):
    try:
        return ppu_replace_roll(roll_name)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=repr(ex))


@api_router.post(
    f"/{PROCESS_NAME}/ppu/renest",
    status_code=200,
)
def renest_roll_in_ppu(
    roll_name: str,
    new_length_yards: float = None,
    token: str = Security(get_current_token),
):
    try:
        return ppu_renest_roll(roll_name, new_length_yards)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=repr(ex))


@api_router.post(
    f"/{PROCESS_NAME}/printfile/terminated",
    status_code=200,
)
def handle_terminated_printfile(
    printfile_id: str = Body(embed=True),
    token: str = Security(get_current_token),
):
    try:
        return mark_partial_printing_in_healing_app(printfile_id)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=repr(ex))


@api_router.post(
    f"/{PROCESS_NAME}/production_requests/create",
    response_model=MakeAssetStatus,
    status_code=200,
)
def create_make_production_request(
    *,
    request: MakeAssetRequest,
    token: str = Security(get_current_token),
) -> MakeAssetStatus:
    make_one_info = None
    try:
        make_one_info = create_make_one_request_directly_in_airtable(
            request,
        )
        sync_make_one_request_to_mongo(make_one_info["id"])
        one_number = make_one_info["fields"]["Order Number V2"]
        # we may have munged this when making the one.
        priority_type = make_one_info["fields"]["Order Priority Type"]
        (
            _,
            blocking_dxa_request_id,
            is_dxa_blocking_request,
        ) = try_place_apply_color_request(make_one_info)
        res.utils.logger.info("Logging the event to the one bridged counter.")
        asset_status = MakeAssetStatus(
            **add_one_number_record(
                one_number=one_number,
                order_number=request.order_name,
                sku=request.sku,
                body_version=request.body_version,
                airtable_rid=make_one_info["id"],
                created_at=utc_now_iso_string(),
                ordered_at=request.ordered_at,
                sales_channel=request.sales_channel,
                brand_code=request.brand_code,
                dxa_assets_ready_at=(
                    None if is_dxa_blocking_request else utc_now_iso_string()
                ),
                blocking_dxa_request_id=blocking_dxa_request_id,
                order_line_item_id=request.order_line_item_id,
                order_priority_type=priority_type,
            ),
            status="REQUEST_CREATED",
        )
        ping_slack(
            f"[MAKE-ONE] Created production request {one_number} ({request.sku})",
            "autobots",
        )
    except Exception as ex:
        if "already exists" in str(ex):
            res.utils.logger.warn(
                f"Request already exists for line item {request.order_line_item_id}"
            )
            mopr_status = MakeAssetStatus(
                **get_one_number_record_by_order_line_item_id(
                    request.order_line_item_id
                ),
                status="ALREADY_CREATED",
            )
            try:
                # sometimes there is a race condition in the bots which leads to the unlinking of the mopr from the line item.
                add_mopr_to_line_item(
                    mopr_status.airtable_rid,
                    mopr_status.one_number,
                    request.order_line_item_id,
                )
            except:
                res.utils.logger.warn(
                    f"Failed to update line item {request.order_line_item_id} with request {mopr_status}"
                )
            return mopr_status
        else:
            res.utils.logger.error(f"failed to generate prod request: {repr(ex)}")
            if make_one_info is not None and "id" in make_one_info:
                PRODUCTION_REQUESTS.delete(make_one_info["id"])
            raise

    res.utils.logger.info(
        f"Saved one number {one_number} for sku {request.sku} in customer order {request.order_name} to the bridge table"
    )

    # dont add print assets for stuff with TT in the sku.
    if not is_dxa_blocking_request:
        if request.sku.startswith("TT"):
            ping_slack(
                f"[MAKE-ONE] Not adding print assets for testing one {one_number} with sku {request.sku}",
                "autobots",
            )
        else:
            run_job_async_with_context(
                add_to_assembly_queue,
                mopr_id=asset_status.airtable_rid,
                one_number=asset_status.one_number,
            )

    return asset_status


@api_router.post(
    f"/{PROCESS_NAME}/production_requests/cancel",
    status_code=200,
)
def cancel_prod_request(
    *,
    request_id: str,
    cancel_reason: str = None,
    token: str = Security(get_current_token),
):
    # mark the one as cancelled in hasura
    cancelled_ones = mark_ones_cancelled([request_id])
    res.utils.logger.info(f"Cancelled ones in hasura {cancelled_ones}")

    # mark the prod request as cancelled and closed in airtable / graph api
    process_mopr(
        request_id,
        {
            "isOpen": False,
            "allocationStatus": "Closed",
            "cancelReasonTags": cancel_reason,
            "cancelledAt": utc_now_iso_string(),
        },
    )

    # mark any print assets as cancelled
    if len(cancelled_ones) > 0:
        cancel_print_request(cancelled_ones[0]["one_number"])

    return cancelled_ones


@api_router.post(
    f"/{PROCESS_NAME}/production_requests/reproduce",
    status_code=200,
)
def reproduce_prod_request(
    *,
    reproduced_request_id: str = Body(embed=True),
    token: str = Security(get_current_token),
) -> MakeAssetStatus:
    try:
        new_one = reproduce_one(reproduced_request_id)
        sync_make_one_request_to_mongo(new_one["id"])

        (
            _,
            blocking_dxa_request_id,
            is_dxa_blocking_request,
        ) = try_place_apply_color_request(new_one)

        stat = MakeAssetStatus(
            **add_one_number_record(
                one_number=new_one["fields"]["Order Number V2"],
                order_number=new_one["fields"]["Belongs to Order"],
                sku=new_one["fields"]["SKU"],
                body_version=new_one["fields"]["Body Version"],
                airtable_rid=new_one["id"],
                created_at=utc_now_iso_string(),
                ordered_at=new_one["fields"]["_original_request_placed_at"],
                sales_channel=new_one["fields"]["Sales Channel"],
                brand_code=new_one["fields"]["__brandcode"],
                dxa_assets_ready_at=(
                    None if is_dxa_blocking_request else utc_now_iso_string()
                ),
                blocking_dxa_request_id=blocking_dxa_request_id,
                order_line_item_id=new_one["fields"]["_order_line_item_id"],
            ),
            status="REQUEST_CREATED",
        )

        if not is_dxa_blocking_request:
            run_job_async_with_context(
                add_to_assembly_queue,
                mopr_id=new_one["id"],
                one_number=new_one["fields"]["Order Number V2"],
            )
        ping_slack(f"[MAKE-ONE] Reproduced {reproduced_request_id}", "autobots")
        return stat
    except Exception as ex:
        ping_slack(
            f"Failed to reproduce {reproduced_request_id} ```{traceback.format_exc()}```",
            "autobots",
        )
        raise


# bump
@api_router.post(
    f"/{PROCESS_NAME}/print_assets/{{one_number}}",
    response_model=MakeAssetStatus,
    status_code=200,
)
def create_make_asset_request(
    *,
    one_number: str,
    create_assets: bool = True,
    piece_requests: List[PieceAssetRequest] = None,
    add_to_existing_print_request: bool = False,
    token: str = Security(get_current_token),
) -> MakeAssetStatus:
    gql = ResGraphQLClient()

    mone_prod_data = get_one_number_record(one_number)
    if not len(mone_prod_data):
        raise HTTPException(
            status_code=404, detail=f"Prod request not found for {one_number}"
        )
    res.utils.logger.info(f"Found make one prod info: {mone_prod_data}")

    # check the airtable record just in case.
    pr_airtable = PRODUCTION_REQUESTS.get(mone_prod_data["airtable_rid"])
    if pr_airtable is None or not pr_airtable["fields"].get("_open", False):
        mark_ones_cancelled([mone_prod_data["airtable_rid"]])
        return MakeAssetStatus(**mone_prod_data, status="CANCELED")

    if mone_prod_data["cancelled_at"] is not None:
        res.utils.logger.info(f"Prod request was cancelled for {one_number}")
        return MakeAssetStatus(**mone_prod_data, status="CANCELED")

    if mone_prod_data["dxa_assets_ready_at"] is None:
        res.utils.logger.info(f"One {one_number} is pending DXA")
        ping_slack(
            f"[MAKE-ONE] <@U0361U4B84X> One {one_number} claims to still be blocked on dxa request {mone_prod_data.get('blocking_dxa_request_id')}",
            "autobots",
        )
        return MakeAssetStatus(**mone_prod_data, status="PENDING_DXA")

    if (
        mone_prod_data.get("print_request_airtable_id")
        and not add_to_existing_print_request
    ):
        res.utils.logger.info(
            f"Print request already exists for {one_number} ({mone_prod_data.get('print_request_airtable_id')})"
        )
        return MakeAssetStatus(**mone_prod_data, status="ALREADY_CREATED")

    if not create_assets:
        res.utils.logger.info(f"Only doing asset info lookup for {one_number}")
        return MakeAssetStatus(**mone_prod_data, status="PENDING_CREATION")

    sku = mone_prod_data["sku"]
    make_one_request_id = mone_prod_data["airtable_rid"]
    m1 = MetaOne(sku)

    if m1.body_version < mone_prod_data["body_version"]:
        res.utils.logger.warn(
            f"Trying to fulfil a request for {one_number} {sku} body version {mone_prod_data['body_version']} but loaded {m1.body_version} -- doing it anyway",
        )

    print_request_id, _ = create_print_asset_requests(
        one_number,
        sku,
        m1.get_ppp_spec(),
        original_order_timestamp=mone_prod_data["ordered_at"]
        .date()
        .strftime("%Y-%m-%d"),
        make_one_prod_request_id=make_one_request_id,
        factory_request_name=mone_prod_data["order_number"],
        brand_name=mone_prod_data["brand_code"],
        factory_order_pdf_url=get_factory_order_s3_path(one_number, presign=True),
        plan=False,
        ppp_flow="v2",
        add_to_existing_print_request=add_to_existing_print_request,
        sales_channel=mone_prod_data["sales_channel"],
        order_priority_type=mone_prod_data["order_priority_type"],
    )

    set_one_status_fields(
        one_number,
        print_request_airtable_id=print_request_id,
        make_assets_checked_at=utc_now_iso_string(),
    )

    PRODUCTION_REQUESTS.update(
        make_one_request_id,
        {
            "__print_app_request_id": print_request_id,
            "Number of Printable Body Pieces": len(m1.printable_piece_codes),
        },
    )

    """
    CUT
    create the cut request with the assistance of the meta one data
    we have / can extend the make one data above to use here
    """
    ## TODO geovanny
    # try:
    #     pieces_at_cut = (
    #         m1.get_pieces_info()
    #     )  # TODO find out we actually need in the cut app
    #     # get cut one request if exists or just return a payload without record id
    #     cut_one_request = get_cut_request_for_one(
    #         mone_prod_data, get_factory_order_s3_path(one_number, presign=True)
    #     )
    #     # get piece cut asset records if exist / merge or just return payloads without record id
    #     pieces_at_cut = get_cut_pieces_for_one(
    #         mone_prod_data,
    #         current_pieces=pieces_at_cut,
    #         cut_one_record_id=cut_one_request["record_id"],
    #     )
    #     # get handle to the two tables and upsert
    #     get_cut_one_request_table().update_record(cut_one_request)
    #     get_cut_pieces_request_table().update_records_many(pieces_at_cut)
    # except Exception as ex:
    #     message = f"Failing to update cut assets for the one {one_number}: {ex}"
    #     res.utils.logger.warn(message)
    #     # ping slack here

    """
    END of cut
    """

    run_job_async_with_context(
        reconcile_order_for_sku, mone_prod_data["order_number"], sku
    )

    # return the latest data about the thing.
    status = MakeAssetStatus(**get_one_number_record(one_number), status="CREATED")

    return status


@api_router.get(
    f"/{PROCESS_NAME}/ones/make_status/",
    response_model=OneOrderStatusNodeAndExpDate,
    status_code=200,
)
def get_one_make_status(
    *,
    one_number: int,
    token: str = Security(get_current_token),
) -> OneOrderStatusNodeAndExpDate:
    """

    ***
    Gets info for a One, namely expected delivery date and current make/assembly node info
    ***
    Args:
        one_number (str): the one number

    Returns:
        OneOrderStatusNodeAndExpDate: see the schema comments

    """

    from res.flows.make.production.queries import get_one_make_status_query

    d = get_one_make_status_query(one_number)

    return OneOrderStatusNodeAndExpDate(**d)


@api_router.get(
    f"/{PROCESS_NAME}/material_info/{{material_code}}",
    response_model=MaterialInfo,
    status_code=200,
)
def get_material_info(
    *,
    material_code: str,
    token: str = Security(get_current_token),
) -> MaterialInfo:
    if roll_loader is None:
        raise Exception("Unable to load material info")
    return get_material_info_for_code_or_name(material_code, roll_loader)


@api_router.get(
    f"/{PROCESS_NAME}/order/can_place_order/",
    response_model=OrderMaterialInfoResponse,
    status_code=200,
)
def get_can_place_order(
    *, skus: OrderInfoRequest, token: str = Security(get_current_token)
):
    if roll_loader is None:
        raise Exception("Unable to load material info")
    return get_can_place_order_for_request(skus, roll_loader)


@api_router.get(
    f"/{PROCESS_NAME}/order/make_status/",
    response_model=List[OneOrderStatusNodeAndExpDate],
    status_code=200,
)
def get_order_make_status(
    *,
    order_number: str,
    token: str = Security(get_current_token),
) -> List[OneOrderStatusNodeAndExpDate]:
    """

    ***
    Gets list of OneOrderStatusNodeAndExpDate for a Order, namely expected delivery date and current make/assembly node info
    ***
    Args:
        one_number (str): the one number

    Returns:
        List[OneOrderStatusNodeAndExpDate]: see the schema comments

    """
    from pydantic import parse_obj_as

    from res.flows.make.production.queries import get_order_make_status_query

    one_list = get_order_make_status_query(order_number)

    return parse_obj_as(List[OneOrderStatusNodeAndExpDate], one_list)


@api_router.get(
    f"/{PROCESS_NAME}/ones/pieces_status/",
    response_model=OneOrderStatus,
    status_code=200,
)
def one_pieces_status(
    *,
    one_number: int,
    piece_code: Optional[str] = None,
    signed_s3_urls: bool = True,
    token: str = Security(get_current_token),
) -> OneOrderStatus:
    """

    ***
    Get the status of each piece in the order including node and contracts (WIP) - for example one number 10333357
    ***

    Args:
        one_number (str): the one number
        signed_s3_urls: the image uris are signed by default
        piece_code: (optional) specify the piece specifically that you want
    Returns:
        OneOrderStatus: see the schema comments


    """

    from res.flows.make.production.queries import get_one_piece_status_with_healing

    d = get_one_piece_status_with_healing(one_number)

    s3 = res.connectors.load("s3")
    if signed_s3_urls:
        for p in d["one_pieces"]:
            p["uri"] = s3.generate_presigned_url(p["uri"])

    if piece_code:
        pieces = [p for p in d["one_pieces"] if piece_code in p["code"]]
        d["one_pieces"] = pieces

    return OneOrderStatus(**d)


def get_current_token_airtable(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    This API key will be hardcoded in airtable for API calls from there - so this should not be shared with the main API key which is stored in AWS Secrets
    as they will likely have different schedules on when they change.  This API key is also a lot more exposed sitting in airtable script..
    """
    token = credentials.credentials
    RES_MAKE_API_KEY_FOR_AIRTABLE_USE = secrets_client.get_secret(
        "RES_MAKE_API_KEY_FOR_AIRTABLE_USE"
    )["RES_MAKE_API_KEY_FOR_AIRTABLE_USE"]
    if not token == RES_MAKE_API_KEY_FOR_AIRTABLE_USE:
        raise HTTPException(
            status_code=401,
            detail="Invalid  RES_MAKE_API_KEY_FOR_AIRTABLE_USE in token check. Check AWS Secrets for this value. ",
        )
    return token


class SharedLockedSettingsCache:
    def __init__(self):
        self.locked_validator_cache = None
        self.locked_validator_cache_printer = None
        self.updated_at = None


shared_locked_settings_cache = SharedLockedSettingsCache()


def get_shared_locked_settings_cache():
    return shared_locked_settings_cache


# bump


@api_router.post(f"/{PROCESS_NAME}/refresh_locked_validator_cache/")
def refresh_locked_validator_cache(
    cache: SharedLockedSettingsCache = Depends(get_shared_locked_settings_cache),
    token: str = Security(get_current_token_airtable),
):
    airtable = res.connectors.load("airtable")
    res.utils.logger.info(
        "reloading from airtable for refresh_locked_validator_cache - tbllUsoGfM117fcvI "
    )

    df_main = airtable["apprcULXTWu33KFsh"]["tbllUsoGfM117fcvI"].to_dataframe()

    cache.locked_validator_cache = df_main
    cache.updated_at = datetime.now()
    return {"status": f"cache updated. size:{cache.locked_validator_cache.shape} "}


@api_router.post(f"/{PROCESS_NAME}/validate_locked_settings/")
def validate_locked_settings(
    locked_settings_dict: Dict[str, Any],
    cache: SharedLockedSettingsCache = Depends(get_shared_locked_settings_cache),
    token: str = Security(get_current_token_airtable),
):
    if (
        not cache
        or (cache.locked_validator_cache is None)
        or cache.locked_validator_cache.empty
    ):
        refresh_locked_validator_cache(cache=cache, token=token)

    if cache.updated_at is None or (
        datetime.now() - cache.updated_at > timedelta(minutes=30)
    ):
        refresh_locked_validator_cache(cache=cache, token=token)

    run_job_async_with_context(
        locked_settings_validator.validate_dict,
        json_dict_from_api=locked_settings_dict,
        main_settings_df=cache.locked_validator_cache.copy(),
    )
    return "ok"


@api_router.post(f"/{PROCESS_NAME}/validate_locked_settings_gpt/")
def validate_locked_settings_gpt(
    record_id,
    cache: SharedLockedSettingsCache = Depends(get_shared_locked_settings_cache),
    token: str = Security(get_current_token_airtable),
):
    if (
        not cache
        or (cache.locked_validator_cache is None)
        or cache.locked_validator_cache.empty
    ):
        refresh_locked_validator_cache(cache=cache, token=token)

    if cache.updated_at is None or (
        datetime.now() - cache.updated_at > timedelta(minutes=30)
    ):
        refresh_locked_validator_cache(cache=cache, token=token)

    locked_settings_validator.validate_dict_experimental_gpt(
        record_id, cache.locked_validator_cache.copy()
    )


@api_router.post(f"/{PROCESS_NAME}/register_contract_variables/")
def register_contract_variables(
    record_id,
    token: str = Security(get_current_token),
):
    """
    once a "Contract Variable" is tagged on a production request, this endpoint will serve to inspect the contract variables and trigger any action that may be needed,
    e.g. invalidate the meta.one, create apply color requets, etc. - registering the contract violations.


    we can and should simplify this fn but expediting for v1
    """
    from res.utils import logger

    logger.info(f"make-one/register_contract_variables record_id={record_id}")

    from res.flows.make.production.contract_variable import register_contract_variables

    try:
        return register_contract_variables(record_id)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@api_router.post(f"/{PROCESS_NAME}/add_contract_variables_fixed/")
def add_contract_variables_fixed(
    production_request_ids: list[str],
    contract_variables_codes_fixed: list[str],
    token: str = Security(get_current_token),
):
    from res.utils import logger

    logger.info(
        f"make-one/unblock_requests_contract_variables_fixed record_id={production_request_ids} contract_variables_fixed_codes={contract_variables_codes_fixed}"
    )

    from res.flows.make.production.contract_variable import add_contract_variables_fixed

    try:
        return add_contract_variables_fixed(
            production_request_ids, contract_variables_codes_fixed
        )

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@api_router.get(
    f"/{PROCESS_NAME}/rolls/pieces",
    status_code=200,
)
def get_pieces_on_roll(
    *,
    roll_number: str,  # R_1234: MATERIAL_CODE
    token: str = Security(get_current_token),
) -> List[dict]:
    """

    this returns pieces of ONES on rolls ...
    """
    try:
        # from res.flows.make.rolls import get_pieces_by_roll
        return
    except:
        # ping slack
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail="Error on server",
        )


app.include_router(api_router)

if __name__ == "__main__":
    # Use this for debugging purposes only
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=5001, log_level="debug", reload=True)
