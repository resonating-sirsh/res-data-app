"""nudge............................."""

from __future__ import annotations
import traceback
from typing import List, Optional, Union
from typing import Sequence
from warnings import filterwarnings

from fastapi import APIRouter, Depends, FastAPI, HTTPException, status
from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import res
from res.flows.meta.ONE.body_node import BodyMetaOneNode
from res.flows.meta.ONE.material_categories import fetch_material_categories_from_hasura
from res.flows.meta.ONE.meta_one import MetaOne, BodyMetaOne
from res.flows.meta.ONE.style_node import MetaOneNode
from res.flows.meta.ONE.trims import fetch_trims_from_hasura
import res.flows.meta.apply_color_request.apply_color_request as apply_color_request
import res.flows.meta.body.body as body
import res.flows.meta.body_one_ready_request.body_one_ready_request as body_one_ready_request
from res.utils import secrets_client
import schemas.pydantic.body as body_schemas
import schemas.pydantic.body_one_ready_request as body_one_ready_request_schemas
from schemas.pydantic.make import ConsumptionAtNode, OneOrder
from schemas.pydantic.meta import MetaOnePrintAsset, MetaOneTrim
from schemas.pydantic.meta import UpsertBodyResponse
from schemas.pydantic.meta import BodyMetaOneStatus, BodyMetaOneResponse
from schemas.pydantic.meta import MetaOneMaterialCategory
from fastapi.responses import StreamingResponse
from source.routes import PROCESS_NAME, set_meta_one_routes
from res.connectors.redis import EndPointKeyCache

filterwarnings("ignore")


def verify_api_key(key: str):
    RES_META_ONE_API_KEY = secrets_client.get_secret("RES_META_ONE_API_KEY")

    if key == RES_META_ONE_API_KEY:
        return True
    else:
        raise HTTPException(
            status_code=401,
            detail="Unauthorized. Invalid RES_META_ONE_API_KEY.  Please refresh this value from AWS secrets manager",
        )


class ConsumptionAtNodeCollection(BaseModel):
    results: Sequence[ConsumptionAtNode]


app = FastAPI(
    title="Meta ONE API",
    openapi_url="/meta-one/openapi.json",
    docs_url="/meta-one/docs",
    # TODO:https://python.plainenglish.io/fastapi-redoc-with-try-it-out-a597c8702bb9
    redoc_url="/meta-one/documentation",
)
security = HTTPBearer()
api_router = APIRouter()


def get_current_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if not verify_api_key(token):
        raise HTTPException(
            status_code=401,
            detail="Invalid  RES_META_ONE_API_KEY in token check. Check AWS Secrets for this value",
        )
    return token


@app.get("/")
@app.get("/healthcheck", include_in_schema=False)
async def healthcheck():
    return {"status": "ok"}


"""
THE BODY INTERFACE
"""


@api_router.put(f"/{PROCESS_NAME}/bodies/", status_code=200)
def post_body_response(*, data: UpsertBodyResponse) -> dict:
    """

    Send a body upload request with body code, version  upload date - for example


    {'body_code': 'TT-4025',
    'body_version': 18,
    'user_email': 'sirsh@a.b.io',
    'process_id': 'test',
    'body_file_uploaded_at': '2023-06-23T11:55:41.987213+00:00'}
    added in bor id ...
    """

    data = BodyMetaOneNode.upsert_response(data)
    return data


"""

"""


@api_router.get(
    f"/{PROCESS_NAME}/bodies/points_of_measure",
    status_code=200,
)
def get_body_points_of_measure(
    *,
    body_code: str,
    body_version: Optional[int] = None,
    token: str = Security(get_current_token),
) -> dict:
    """
    Provides the points of measures for a body.  These are the critical measurements (in units: cm) along key points in the body such as the length across the chest etc.
    The measurements are provided for each size that the body is made in.
    "Petite" versions of each of the size-specific measurements may be given in some cases and prefixed with `P_`
    ...
    """
    from res.flows.meta.ONE.meta_one import BodyMetaOne
    from res.flows.meta.bodies import get_body_asset_status

    try:
        if not body_version:
            res.utils.logger.warn(f"No body provided assuming latest")
            status = get_body_asset_status(body_code=body_code)
            body_version = status["body_version"]
        m = BodyMetaOne.for_sample_size(body_code=body_code, body_version=body_version)
        poms = m.get_points_of_measure()
        return {
            "body_code": body_code,
            "body_version": body_version,
            "points_of_measure": poms,
        }
    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@api_router.get(
    f"/{PROCESS_NAME}/bodies/get_body_by_code",
    response_model=BodyMetaOneResponse,
    status_code=200,
)
def get_body_by_code(
    *,
    body_code: str,
    body_version: Optional[int] = None,
    token: str = Security(get_current_token),
) -> dict:
    # todo infer body version
    m1 = BodyMetaOne.for_sample_size(body_code=body_code, body_version=body_version)

    return m1.status_at_node()


@api_router.get(
    f"/{PROCESS_NAME}/bodies/status", response_model=BodyMetaOneStatus, status_code=200
)
def get_body_status(
    *,
    body_code: str,
    body_version: Optional[int] = None,
) -> dict:
    """

    ***
    Check the status of the body in the queue. As it exits the queue, this becomes the status of the live body without contracts.
    At any point someone could add contracts to invalidate a body but we have not yet established that flow.
    For now this has been implemented as a queue status in practice but in future we will develope the flow further for body lifecycle.
    This development will require a deeper understanding of state on the body itself, during or after its time on the queue.

    ***

    ## params
    - body code: the body code in the format KT-2011
    - body_version: the optional version of the body required. The latest version is supplied by default

    ## sketch of body status has some items to do
    - todo move necessary data to hasura for the complete picture since this version pulls together data from multiple places
    - we need to back fill data
    - in future as things go through the queue we need to update hasura
    """

    return BodyMetaOneNode.get_body_status(body_code, body_version)


@api_router.get(f"/{PROCESS_NAME}/bodies/get-nest-by-material", status_code=200)
def get_body_nest_by_material(
    *,
    body_code: str,
    material_codes: Optional[str],
    body_version: Optional[int] = None,
    size_codes: Optional[str] = None,
    token: str = Security(get_current_token),
) -> dict:
    """
    Given a Body Code and optional other parameters, this Nests the printable pieces and provides link to cut files in DXF format
    nest the body by materials. returns a collection of nested DXF file uris on S3

    **Args**
        body_code: the body code in a format like KT-2011
        material_codes: : a comma separated list of one or more material codes to generate body nests for - the onboarding material is used by default but if the user asks for specific codes use them
        body_version: (optional) body version - the latest will be chosen by default
        size_codes: (optional) a comma separated list of one or more size codes. The base size will be chosen by default

    """

    def check_string_list(s):
        if s is None or isinstance(s, list):
            return s
        s = s.split(",")
        return s if isinstance(s, list) else [s]

    try:
        if material_codes is None or body_code is None:
            raise HTTPException(
                400,
                "You need to specify both the body code and one or more material codes at a minimum",
            )

        material_codes = check_string_list(material_codes)
        size_codes = check_string_list(size_codes)

        return BodyMetaOneNode.nest_body_into_material(
            body_code=body_code,
            body_version=body_version,
            material_codes=material_codes,
            size_codes=size_codes,
        )
    except:
        raise HTTPException(
            500,
            f"The nesting process failed for body {body_code} in materials {material_codes}. you supplied size {size_codes} and body version {body_version}",
        )


"""

"""


@api_router.get(f"/{PROCESS_NAME}/meta-one/active_style_piece_info", status_code=200)
def get_active_style_pieces_for_style_code(*, style_code: str) -> dict:
    """
    Get the style pieces for all sizes that are not in a deleted state at body or style
    If there are multiple styles for a BMC with same name we return the latest
    these are pieces that should represent the current meta one's active piece and whatever body version
    each pieces shows the full piece code which has the body version in it, but we also explicitly return the linked body version as a field
    dates are also supplied when the body and style pieces respectively were updated - these could be compared with other dates such as BW exports

    the failing contracts are returned

    **Args**
       style_code: three part bmc e.g. TH-1002 PIMA7 LIGHKZ
    """
    from res.flows.meta.ONE.queries import get_active_style_pieces_for_sku

    return get_active_style_pieces_for_sku(style_code)


@api_router.get(
    f"/{PROCESS_NAME}/meta-one/nest", response_class=StreamingResponse, status_code=200
)
def get_nest_plot_for_sku(
    sku: str,
    token: str = Security(get_current_token),
) -> StreamingResponse:
    """
    Get a nest given a sku using the example format `NA-1001 LSC19 TENCVT 3ZZMD` which is the Body Material Color and Size for the SKU
    """

    import io
    from PIL import Image
    from cairosvg import svg2png

    m1 = MetaOne(sku)
    df = m1.nest(should_plot=False)
    df["size_code"] = m1.size_code
    from res.flows.meta.ONE.geometry_ex import geometry_df_to_svg

    svg = geometry_df_to_svg(
        df[["key", "nested.original.outline", "size_code"]].rename(
            columns={"nested.original.outline": "outline"}
        )
    )

    im_bytes = svg2png(svg, output_width=1200, output_height=400)
    bytes_io = io.BytesIO(im_bytes)
    # im = Image.open(bytes_io) ###

    return StreamingResponse(content=bytes_io, media_type="image/jpeg")


@api_router.get(
    f"/{PROCESS_NAME}/meta-one/nest_image",
    status_code=200,
)
def get_nest_image_link_for_sku(
    sku: str,
    token: str = Security(get_current_token),
) -> dict:
    """
    Get a nest given a sku using the example format `NA-1001 LSC19 TENCVT 3ZZMD` which is the Body Material Color and Size for the SKU
    """

    import io
    from PIL import Image
    from cairosvg import svg2png

    res.utils.logger.info(f"Received request to nest {sku}")

    m1 = MetaOne(sku)
    df = m1.nest(should_plot=False)
    df["size_code"] = m1.size_code
    from res.flows.meta.ONE.geometry_ex import geometry_df_to_svg

    svg = geometry_df_to_svg(
        df[["key", "nested.original.outline", "size_code"]].rename(
            columns={"nested.original.outline": "outline"}
        )
    )

    im_bytes = svg2png(svg, output_width=1200, output_height=400)
    bytes_io = io.BytesIO(im_bytes)
    im = Image.open(bytes_io)  ###
    s3 = res.connectors.load("s3")
    uri = f"s3://res-data-platform/images/cached/nest_plots/{sku.replace(' ','_')}.png"

    s3.write(uri, im)

    res.utils.logger.info(f"Wrote nest plot {uri}")

    return {"nest_plot_image_uri": s3.generate_presigned_url(uri), "sku": sku}


@api_router.get(f"/{PROCESS_NAME}/meta-one/request_copy_color_files", status_code=200)
def request_copy_color_files(*, style_code: str, body_version: int = None) -> dict:
    """
    This is used to simply copy self i.e. touch or copy files from an older version to the latest version

    """
    from res.flows.meta.ONE.style_node import request_touch_files

    res.utils.logger.info(
        f"Requesting touch files for style code {style_code} and body version {body_version}"
    )
    request_touch_files(
        payloads=[{"style_code": style_code, "body_version": body_version}]
    )

    return {"message": "queued"}


@api_router.get(f"/{PROCESS_NAME}/meta-one/enqueue_refresh", status_code=200)
def refresh_meta_one(*, style_code: str, request_id: str = None) -> dict:
    """
    Refresh the meta one from placed color given the style code

    **Args**
    style_code: the three part BMC style code
    request_id: (deprecate) currently needed to say which request id to send updates to on ACQ
    """
    from res.flows.meta.ONE.style_node import MetaOneNode

    # TODO lookup the the latest style request in apply color queue
    MetaOneNode.refresh(style_code, use_record_id=request_id, clear_cache=True)

    return {"message": "queued"}


@api_router.get(f"/{PROCESS_NAME}/meta-one/costs-table", status_code=200)
def get_meta_one_costs_table(*, sku: str) -> List[dict]:
    """
    Fetch costs table for a sku using this format "TH-3016 CTNBA PINKIC 0ZZXX" which is the Body, Color Material and Size of the SKU
    """
    from res.flows.meta.ONE.queries import get_bms_style_costs_for_sku

    # a record set is returned - the 0 index gives the dict - costs are a list of dicts... ...
    try:
        cache = EndPointKeyCache("/meta-one/costs-table", ttl_minutes=60)
        cached = cache[sku]
        if cached:
            res.utils.logger.warn(
                f"using cached value for item /meta-one/costs-table/{sku}"
            )
            return cached
        result = get_bms_style_costs_for_sku(sku)
        cache[sku] = result
        return result
    except Exception:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            404,
            "Failed to get the costs table - please check the format of the sku you entered is correct  ",
        )


@api_router.get(f"/{PROCESS_NAME}/meta-one/is-cost-api-enabled", status_code=200)
def get_meta_one_is_cost_api_enabled(
    *, token: str = Security(get_current_token)
) -> List[dict]:
    """
    Fetch is_const_api_enabled for a brand code "TT" always because is a single row.
    """
    from res.flows.meta.ONE.queries import get_is_cost_api_enabled_for_brand

    return get_is_cost_api_enabled_for_brand("TT")


@api_router.get(f"/{PROCESS_NAME}/meta-one/status/", status_code=200)
def get_full_style_status(
    *,
    style_sku: str,
    body_version: int,
    brand_code: str = None,
    include_full_status: bool = True,
    include_cost: bool = False,
    token: str = Security(get_current_token),
) -> dict:
    """
     Get the deep status of the meta one as used in pre make checks
     - do we have the style in the correct body version
     - what are the BMC costs for the style as last cached
     -
    ```json
     {
         "style_id": "30c7b856-590c-259d-1a04-3df849267fd5",
         "sku": "LA-6009 CHRST NEWDKG",
         "name": "Laura Dress - Long - NEW DOTS WHITE BLACK in Silk Stretch Charmeuse",
         "body_version": 1,
         "contracts_failing": null,
         "piece_version_delta_test_results": {},
         "audit_history": []
         }
     ```

    """

    try:
        return MetaOneNode.get_full_style_status(
            style_code=style_sku,
            body_version=body_version,
            brand_code=brand_code,
            include_full_status=include_full_status,
            include_cost=include_cost,
        )
    except Exception as ex:
        raise HTTPException(
            500,
            f"Could not handle the request for {style_sku} - {ex}",
        )


@api_router.get(f"/{PROCESS_NAME}/contracts/", status_code=200)
def get_meta_one_failing_contracts(
    *,
    style_sku: str,
    token: str = Security(get_current_token),
) -> dict:
    """
    Fetch a meta one failing contracts list on the meta one (if any) using the three part style sku using the example format `TT-3072 CTNBA BLACWG` - This format is the Body Material Color of the style
    """

    response = MetaOne.get_style_status_history(style_sku)

    res.utils.logger.info(response)
    if not response:
        raise HTTPException(
            204,
            f"There is no style {style_sku} in the saved meta ones in this environment",
        )

    return response


@api_router.patch(f"/{PROCESS_NAME}/contracts/", status_code=200)
def patch_meta_one_failing_contracts(
    *,
    style_sku: str,
    contracts: List[str],
    token: str = Security(get_current_token),
) -> dict:
    """
    Merge a contract failing onto the existing set of contracts failing on the meta one using the three part style sku using this example format `TT-3072 CTNBA BLACWG` i.e. the Body, Material and Color of the style and the payload of contracts list to add
    - when you patch, a contract is merged into the existing contracts to create a unique set
    - we do not delete any existing contracts
    see also `put`
    """
    if len(contracts) == 0:
        contracts = None
    return MetaOne.invalidate_meta_one(
        style_sku, contracts=contracts, merge=True, with_audit=True
    )


@api_router.put(f"/{PROCESS_NAME}/contracts/", status_code=200)
def put_meta_one_failing_contracts(
    *,
    style_sku: str,
    contracts: List[str],
    token: str = Security(get_current_token),
) -> dict:
    """
    Specify the full possibly empty list of failing contracts on the meta one using the three part style sku e.g. TT-3072 CTNBA BLACWG and the payload of contracts list to add
    - when we `put` a list of contracts we replace the existing set unlike `patch` which merges
    - typically you should want to just add contracts without deciding if the existing contracts should be removed
    """

    if len(contracts) == 0:
        contracts = None
    return MetaOne.invalidate_meta_one(
        style_sku, contracts=contracts, merge=False, with_audit=True
    )


@api_router.delete(f"/{PROCESS_NAME}/contracts/", status_code=200)
def del_meta_one_failing_contracts(
    *,
    style_sku: str,
    contracts: List[str],
    token: str = Security(get_current_token),
) -> dict:
    """
    Specify the full possibly empty list of failing contracts on the meta one using the three part style sku e.g. TT-3072 CTNBA BLACWG and the payload of contracts list to add
    - when we `put` a list of contracts we replace the existing set unlike `patch` which merges
    - typically you should want to just add contracts without deciding if the existing contracts should be removed
    """

    if len(contracts) == 0:
        contracts = None
    return MetaOne.remove_contracts(style_sku)


@api_router.get(
    f"/{PROCESS_NAME}/style/{{sku}}",
    status_code=200,
    # response_model=MetaOneRequest,
)
def get_meta_one_style(*, style_sku: str) -> dict:
    """
    Fetch a meta one style detail by three part style BODY MATERIAL COLOR e.g. CC-3069 TNSPJ LADYHK
    """

    return [f for f in MetaOneNode.get_style_as_request(style_sku) if f.is_sample_size][
        0
    ].dict()


@api_router.get(
    f"/{PROCESS_NAME}/style_pieces/",
    status_code=200,
    # response_model=MetaOneRequest,...
)
def get_meta_one_style_pieces(sku: str) -> List[dict]:
    """
    Fetch a meta one style size detail by four part style BODY MATERIAL COLOR SIZE_CODE using this example format `CC-3069 TNSPJ LADYHK 2ZZSM` which is the body, material, color and size of the SKU
    we pre-sign the s3 URL and this will time out in 30 minutes
    """
    res.utils.logger.info(f"Sku {sku}")
    try:
        s3 = res.connectors.load("s3")
        a = MetaOne.safe_load(sku)
        pcs = a._data[
            [
                "key",
                "uri",
                "piece_set_size",
                "piece_ordinal",
                "normed_size",
                "body_piece_type",
                "style_sizes_size_code",
                "garment_piece_material_code",
            ]
        ].rename(columns={"style_sizes_size_code": "size_code"})
        pcs["meta_key"] = pcs["key"].map(lambda x: f"-".join(x.split("-")[-2:]))
        # add a presigned url with the default timeout for the caller to add things to airtable or whatever
        pcs["presigned_uri"] = pcs["uri"].map(s3.generate_presigned_url)
        return pcs.to_dict("records")
    except:
        res.utils.logger.warn(traceback.format_exc())
        return []


@api_router.get(
    f"/{PROCESS_NAME}/style-size/{{sku}}",
    status_code=200,
    # response_model=MetaOneResponse,
)
def get_meta_one_style_sized(*, sku: str) -> dict:
    """
    Fetch a meta one style size detail by four part style BODY MATERIAL COLOR SIZE_CODE using this example format `CC-3069 TNSPJ LADYHK 2ZZSM` which is the Body Material Color and Size of the style
    """

    return MetaOne.safe_load(sku).as_asset_response().dict()


@api_router.get(
    f"/{PROCESS_NAME}/order/{{one_number}}", status_code=200, response_model=OneOrder
)
def get_one_by_one_number(*, one_number: str) -> dict:
    """
    Fetch a ONE by one number
    """

    return {}


@api_router.get(
    f"/{PROCESS_NAME}/order-by-label/{{label_one_number}}",
    status_code=200,
    response_model=OneOrder,
)
def get_one_by_one_code_label_number(*, label_one_number: str) -> dict:
    """
    Fetch a ONE by label one number
    """

    return {}


@api_router.get(
    f"/{PROCESS_NAME}/consumption-by-one{{one_number}}",
    status_code=200,
    response_model=ConsumptionAtNodeCollection,
)
def get_one_consumption_by_one_number(*, one_number: str) -> dict:
    """
    Fetch a ONE by label one number
    """
    duck = res.connectors.load("duckdb")
    cs = [
        ConsumptionAtNode(**c)
        for c in duck.execute(
            f"SELECT * FROM 's3://res-data-platform/samples/consumption_data.parquet' where one_number == '{one_number}'"
        ).to_dict("records")
    ]

    return ConsumptionAtNodeCollection(results=cs)


@api_router.get(
    f"/{PROCESS_NAME}/consumption-by-label{{label_one_number}}",
    status_code=200,
    response_model=ConsumptionAtNodeCollection,
)
def get_one_consumption_by_label_one_number(*, label_one_number: str) -> dict:
    """
    Fetch a ONE consumption dataset by one number
    TODO: we need to integrate the new piece model with the one label number and ONLY applies if the label exists
    For testing better to use the possibly existing label to resolve the ONE number and the call the ONE consumption for now
    """
    duck = res.connectors.load("duckdb")
    cs = [
        ConsumptionAtNode(**c)
        for c in duck.execute(
            f"SELECT * FROM 's3://res-data-platform/samples/consumption_data.parquet' where label_one_number == '{label_one_number}'"
        ).to_dict("records")
    ]

    return ConsumptionAtNodeCollection(results=cs)


@api_router.get(
    f"/{PROCESS_NAME}/meta-one",
    status_code=200,
    response_model=MetaOnePrintAsset,
)
def get_meta_one_asset(
    *,
    sku: str,
    presign_urls: bool = True,
    token: str = Security(get_current_token),
) -> MetaOnePrintAsset:
    """
    Fetch a meta one with all piece info for make
    for example pass LH-3003 LN135 CREABM 4ZZLG

    presign urls can be used to make sure that certain s3 urls are presigned as (temporary) downloadable links

    """
    from res.flows.meta.ONE.controller import get_meta_one

    try:
        # by default use presigned urls - because we can then download them
        return get_meta_one(sku, presigned_urls=presign_urls)
    except:
        res.utils.logger.info(traceback.format_exc())
        raise HTTPException(404, "Failed to load meta one see log")


""" BODY """


@api_router.post(
    f"/{PROCESS_NAME}/body", status_code=status.HTTP_201_CREATED, response_model=dict
)
def create_body(
    *, input: body_schemas.CreateBodyInput, token: str = Security(get_current_token)
) -> dict:
    """
    Create a new Body

    input:

    ```
    {
        "name": "Tech Top",
        "brand_code": "TT",
        "category_id": "recm8VCp8L2Ht0z6H",
        "cover_images_files_ids": [
            "620ade84549f1c0009269bad"
        ],
        "campaigns_ids": [
            "recw14OjsR4Jpff2y"
        ],
        "onboarding_materials_ids": [
            "rec7tgCGeJ2wueOUm"
        ],
        "created_by_email": "techpirates@resonance.nyc"
    }
    ```
    """

    try:
        return body.create_body(input)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


""" END BODY """

""" BODY ONE READY REQUEST """


@api_router.post(
    f"/{PROCESS_NAME}/body-one-ready-request",
    status_code=status.HTTP_201_CREATED,
    response_model=dict,
)
def create_body_one_ready_request(
    *,
    input: body_one_ready_request_schemas.CreateBodyOneReadyRequestInput,
    token: str = Security(get_current_token),
) -> dict:
    """
    Create a new Body ONE Ready Request

    input:

    ```
    {
        "body_id": "recaio5fMPP47kpu1",
        "parent_body_id": "rec9FRM58jHJUwbXS",
        "one_sketch_files_ids": [
            "631faecb868b1e0009efd67d"
        ],
        "reference_images_files_ids": [
            "63223fcb6b41810009738c0c"
        ],
        "base_size_id": "recXRiegGmNCjfLJx",
        "body_operations_ids": [],
        "requested_by_email": "techpirates@resonance.nyc",
        "was_parent_body_selected_by_brand": true,
        "body_design_notes": "body design notes...",
        "sizes_notes": "sizes notes...",
        "trim_notes": "trim notes...",
        "label_notes": "label notes...",
        "prospective_launch_date": null,
        "body_one_ready_request_type": "Body Onboarding",
        "changes_requested": [],
        "areas_to_update": [],
        "requestor_layers": [
            "Brand"
        ]
    }
    ```
    """

    try:
        return body_one_ready_request.create_body_one_ready_request(input)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@api_router.post(
    f"/{PROCESS_NAME}/body-one-ready-request/order-first-one",
    status_code=status.HTTP_200_OK,
    response_model=dict,
)
def order_first_one_for_body_one_ready_request(
    *, id: str, token: str = Security(get_current_token)
) -> dict:
    """
    Prepare & order a style as First ONE for a given Body ONE Ready Request.

    input:

    ```
    id="rec7qAWKJiYFyV2Tm"
    ```
    """

    try:
        return body_one_ready_request.order_first_one_for_body_one_ready_request(id)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


@api_router.post(
    f"/{PROCESS_NAME}/body-one-ready-request/register-new-body-version",
    status_code=status.HTTP_200_OK,
    response_model=dict,
)
def register_new_body_version(
    *, id: str, token: str = Security(get_current_token)
) -> dict:
    """

    This endpoint aims to formally register/rollout a new developed body version once the associated Body ONE Ready Request gets to Done.
    Initially 'register new body version' stands for:
    - reproducing any apply color request for the body, in order to use the new version

    input:

    ```
    id="reck5iZOiWxveBITK"
    ```

    NOTE:
    - to initialize the requests as we rollout this endpoint, we've set "Register Body Version Upgrade Status"='N/A' for any DONE requests created before the implementation of this enpoint
    """

    try:
        return body_one_ready_request.register_new_body_version(id)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


""" END BODY ONE READY REQUEST """

""" APPLY COLOR REQUEST """


@api_router.post(
    f"/{PROCESS_NAME}/apply-color-request/validate-apply-color-request-inputs",
    status_code=status.HTTP_200_OK,
    response_model=dict,
)
def validate_apply_color_request_inputs(
    *, id: str, token: str = Security(get_current_token)
) -> dict:
    """
    Validate the validity/readiness of apply color request inputs & requirements as en entry point in the apply color queue.
    In case the inputs are not ready, the request will be flagged. otherwise, it may proceed in the flow (and will try to unflag in case it might have been flagged in the past).

    input:

    ```
    id="recY2Ht5Tdb8LSllc"
    ```
    """

    try:
        return apply_color_request.validate_apply_color_request_inputs(id)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


""" END APPLY COLOR REQUEST """

""" BEGIN MATERIAL CATEGORIES REQEST """


@api_router.get(
    f"/{PROCESS_NAME}/material-categories",
    status_code=status.HTTP_200_OK,
    response_model=MetaOneMaterialCategory,
)
def material_categories_request(
    *, code: str, token: str = Security(get_current_token)
) -> dict:
    try:
        return fetch_material_categories_from_hasura(code)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


""" END MATERIAL CATEGORIES REQEST """

""" BEGIN TRIMS REQUEST """


@api_router.get(
    f"/{PROCESS_NAME}/trims",
    status_code=status.HTTP_200_OK,
    response_model=MetaOneTrim,
)
def trims_request(
    *, body_code: str, version: int, token: str = Security(get_current_token)
) -> dict:
    try:
        return fetch_trims_from_hasura(body_code, version)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


""" END TRIMS REQUEST """

""" BEGIN BILL OF MATERIAL REQUEST """


@api_router.get(
    f"/{PROCESS_NAME}/style-bill-of-material", status_code=status.HTTP_200_OK
)
def bill_of_material_request(
    *,
    style_code: str = None,
    style_id: str = None,
    token: str = Security(get_current_token),
) -> List[dict]:
    from res.flows.meta.style.style import get_style_bill_of_material

    try:
        return get_style_bill_of_material(style_code, style_id)

    except Exception as e:
        res.utils.logger.warn(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )


""" END BILL OF MATERIAL REQUEST """

origins = [
    "http://localhost:3000",
    "http://localhost:3001",
    "http://localhost:5000",
    "https://strictly-mutual-newt.ngrok-free.app",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
set_meta_one_routes(app)

if __name__ == "__main__":
    # Use this for debugging purposes only
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=5001, log_level="debug", reload=True)
