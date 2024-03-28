from __future__ import annotations

import inspect
import json
import os
import traceback
from datetime import datetime
from typing import Any, List, Optional
from warnings import filterwarnings

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    Header,
    HTTPException,
    Request,
    Security,
)
from fastapi.responses import JSONResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from flask import request
from jsonschema import ValidationError
from starlette.concurrency import run_in_threadpool

from schemas.pydantic.payments import *

import res
from res.flows.finance import payments_api_utils

filterwarnings("ignore")
# nudge..

PROCESS_NAME = "payments-api"
postgres = None


class PrettyJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        # as we move to production away from dev, change this from indented json which is a performance hit
        # .. nudge
        return json.dumps(content, ensure_ascii=False, indent=4).encode("utf-8")


app = FastAPI(
    title="Payments API",
    openapi_url=f"/{PROCESS_NAME}/openapi.json",
    docs_url=f"/{PROCESS_NAME}/docs",
    default_response_class=PrettyJSONResponse,
)


security = HTTPBearer()


def handle_exception(e: Exception) -> HTTPException:
    """
    Helper function to log and handle unexpected exceptions.   As this in an internal API - we are exposing the stack trace for productivy

    IMPORTANT: If using this example for a public API, DO NOT include the full stack trace like done below
    """

    if isinstance(e, HTTPException):
        return e  # its already formatted by our code, throw as is
    else:
        traceback_str = traceback.format_exc()
        res.utils.logger.error(f"Unexpected error:  {traceback_str}")

        payments_api_utils.ping_slack(
            f" {payments_api_utils.slack_id_to_notify} Global unexpected error payments API:  {traceback_str}",
            "payments_api_notification",
        )
        return HTTPException(
            status_code=500,
            detail=f"An unexpected error occurred:  {str(traceback_str)}",
        )


def log_increment_metrics(*args) -> None:
    """log the method call, increment statsd metrics"""

    res.utils.logger.info(f"PaymentsAPI Call: {args[0]}, params: {args[1:]}")
    log_hit(args[0], args[1:])


def log_hit(func_name, other_args: tuple):
    list_args = list(other_args)
    while len(list_args) < 4:
        list_args.append("None")

    list_args = [str(i) for i in list_args]

    res.utils.logger.metric_node_state_transition_incr(
        list_args[0],
        list_args[1],
        list_args[2],
        flow=func_name,
        process="PaymentsAPI",
    )


def get_current_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    if not payments_api_utils.verify_res_payments_api_key(token):
        raise HTTPException(
            status_code=401,
            detail="Invalid  RES_PAYMENTS_API_KEY in token check. Check AWS Secrets for this value. ",
        )
    return token


api_router = APIRouter()


@app.get("/")
@app.get("/healthcheck")
async def healthcheck():
    return {"status": "ok"}


@api_router.get(
    f"/{PROCESS_NAME}/GetTransactions/",
    response_model=List[PaymentsTransaction],
)
def get_transactions(
    start_date: Optional[datetime.date] = datetime.date(2015, 1, 1),
    end_date: Optional[datetime.date] = datetime.date(2099, 1, 1),
    token: str = Security(get_current_token),
    brand_code: Optional[str] = None,
):
    """
    Get a list of payment transactions from sell.transactions.  You can optionally filter by brand code (eg "BG") and optionally start and end dates
    <br>
    https://data.resmagic.io/payments-api/GetTransactions/?start_date=2023-01-01&end_date=2025-01-01&by_brand_code=BG
    """
    try:
        log_increment_metrics(
            inspect.currentframe().f_code.co_name, start_date, end_date, brand_code
        )
        payments_api_transactions = payments_api_utils.get_transactions(
            start_date, end_date, postgres, brand_code
        )

    except Exception as e:
        raise handle_exception(e)

    return payments_api_transactions


@api_router.get(
    f"/{PROCESS_NAME}/GetBrandDetails/",
    response_model=BrandAndSubscription,
)
def get_brand(
    brand_code: str,
    token: str = Security(get_current_token),
):
    """
    Gets Brand Information based upon 2 letter brand code that the PaymentsAPI knows about. Returns PaymentBrand
    <br>
    https://data.resmagic.io/payments-api/GetBrandDetails/?brand_code=BG
    """
    try:
        log_increment_metrics(inspect.currentframe().f_code.co_name, brand_code)
        brand_details = payments_api_utils.get_brand(brand_code, postgres)

        if not brand_details:
            raise HTTPException(
                status_code=404,
                detail="Ensure that the brand code is correct",
                headers={
                    "error": "brand_not_found",
                    "message": f"two letter brand code {brand_code} not found",
                },
            )
    except ValidationError as e:
        # Handle validation error
        error_str = f"pydantic validation error while parsing Brand from database: {e}"
        res.utils.logger.error(f"Validation error: {e}")
        raise HTTPException(
            status_code=500,
            detail=error_str,
        )
        brand = None

    except Exception as e:
        raise handle_exception(e)

    return brand_details


@api_router.post(
    f"/{PROCESS_NAME}/UpdateBrand/",
)
def update_brand(
    brand: BrandCreateUpdate,
    token: str = Security(get_current_token),
):
    """
    For an existing brand (by BrandCode already exists), all other attributes supplied will be assumed to need updating.
    If brand code does not exists, this will error.  It will not create the brand - there is a dedicated function to add a brand

    <br>
    https://data.resmagic.io/payments-api/UpdateBrand/


     {

        "meta_record_id": "recXXX",
        "fulfill_record_id": "recXXX",
        "brand_code": "TT",
        "is_brand_whitelist_payment": true,
        "name": "myName",
        "shopify_storename": 'shopify_storename',
        "order_delayed_email_v2": null,
        "created_at_airtable": "2020-07-22",
        "homepage_url": "myurl.com",
        "sell_enabled": true,
        "shopify_location_id_nyc": "999",
        "contact_email": "info@me.com",
        "payments_revenue_share_ecom": 0,
        "created_at_year": 2024,
        "payments_revenue_share_wholesale": 0,
        "subdomain_name": "sub",
        "brands_create_one_url": "https://test.create.one",
        "shopify_shared_secret": null,
        "quickbooks_manufacturing_id": "999",
        "address": "line1, line 2, whatever",
        "is_direct_payment_default": null,
        "brand_success_active": true,
        "shopify_api_key": "my_key",
        "shopify_api_password": "my_password",
        "register_brand_email_address": null,
        "start_date": "2020-01-01",
        "end_date": "9999-12-31",
        "is_exempt_payment_setup_for_ordering_createone": false
    }

    """
    try:
        log_increment_metrics(inspect.currentframe().f_code.co_name, brand)
        retval = payments_api_utils.insert_or_update_brand(
            brand, is_update=True, postgres=postgres
        )
        if retval != "OK":
            return HTTPException(
                status_code=403,
                detail=retval,
            )
        return retval
    except Exception as e:
        raise handle_exception(e)


@api_router.post(
    f"/{PROCESS_NAME}/CreateBrand/",
)
def create_brand(
    brand: BrandCreateUpdate,
    create_stripe_customer=True,
    token: str = Security(get_current_token),
):
    """
    For a new brand ( BrandCode cannot already exist).
    If brand code does not exists, this will error.  It will not update an existingbrand - there is a dedicated function to update a brand

    <br>
    https://data.resmagic.io/payments-api/CreateBrand/


    {

        "meta_record_id": "recXXX",
        "fulfill_record_id": "recXXX",
        "brand_code": "TT",
        "is_brand_whitelist_payment": true,

        "name": "myName",
        "shopify_storename": null,
        "order_delayed_email_v2": null,
        "created_at_airtable": "2020-07-22",
        "shopify_store_name": "my Store",
        "homepage_url": "myurl.com",
        "sell_enabled": true,
        "shopify_location_id_nyc": "999",
        "contact_email": "info@me.com",
        "payments_revenue_share_ecom": 0,
        "created_at_year": 2024,
        "payments_revenue_share_wholesale": 0,
        "subdomain_name": "sub",
        "brands_create_one_url": "https://test.create.one",
        "shopify_shared_secret": null,
        "quickbooks_manufacturing_id": "999",
        "address": "line1, line 2, whatever",
        "is_direct_payment_default": null
        "brand_success_active": true,
        "shopify_api_key": "my_key",
        "shopify_api_password": "my_password",
        "register_brand_email_address": null,
        "start_date": "2020-01-01",
        "end_date": "9999-12-31",
        "is_exempt_payment_setup_for_ordering_createone": false
    }

    """
    try:
        log_increment_metrics(inspect.currentframe().f_code.co_name, brand)
        retval = payments_api_utils.insert_or_update_brand(
            brand,
            is_update=False,
            create_stripe_customer=create_stripe_customer,
            postgres=postgres,
        )

        if retval != "OK":
            return HTTPException(
                status_code=403,
                detail=retval,
            )
        return retval
    except Exception as e:
        raise handle_exception(e)


@api_router.get(
    f"/{PROCESS_NAME}/get-stripe-products/",
)
def get_stripe_products(
    token: str = Security(get_current_token),
):
    try:
        res.utils.logger.info(f"in function: {inspect.currentframe().f_code.co_name}")
        return payments_api_utils.stripe_load_products()
    except Exception as e:
        raise handle_exception(e)


@api_router.get(
    f"/{PROCESS_NAME}/get-stripe-product-by-id/",
)
def get_stripe_product_by_id(
    stripe_product_id: str,
    token: str = Security(get_current_token),
):
    try:
        res.utils.logger.info(f"in function: {inspect.currentframe().f_code.co_name}")
        return payments_api_utils.stripe_load_product(stripe_product_id)
    except Exception as e:
        raise handle_exception(e)


# nudge
@api_router.get(
    f"/{PROCESS_NAME}/get-customer-stripe-billing-session-url/",
)
def get_customer_stripe_billing_session_url(
    stripe_customer_id: str = None,
    brand_code: str = None,
    token: str = Security(get_current_token),
):
    """use this to request a billing session URL to allow the brand to manage their payment details on stripe.  You can request using either a brand code or a stripe customer ID. stripe customer ID is prioritized if both supplied"""
    try:
        res.utils.logger.info(
            f"in function: {inspect.currentframe().f_code.co_name} stripe customer id: {stripe_customer_id}"
        )
        session = payments_api_utils.get_customer_stripe_billing_session_url(
            stripe_customer_id, brand_code
        )
    except Exception as e:
        raise handle_exception(e)

    return session


async def get_payload_body(request: Request):
    return await request.body()


# nudge
@api_router.post(f"/{PROCESS_NAME}/stripe-webhooks")
def stripe_webhooks(
    payloadbody: dict = Depends(get_payload_body),
    stripe_signature: str = Header(None),
):
    try:
        from res.utils import secrets_client

        endpoint_secret_dict = secrets_client.get_secret(
            "STRIPE_WEBHOOK_SECRET_NEW_MAR24", return_entire_dict=True
        )
        endpoint_secret = endpoint_secret_dict.get("production")
        return handle_webhook(
            payloadbody, stripe_signature, endpoint_secret, is_dev=False
        )

    except Exception as e:
        raise handle_exception(e)


#


@api_router.post(f"/{PROCESS_NAME}/stripe-webhooks-dev")
def stripe_webhooks_dev(
    payloadbody: dict = Depends(get_payload_body),
    stripe_signature: str = Header(None),
):
    try:
        from res.utils import secrets_client

        endpoint_secret_dict = secrets_client.get_secret(
            "STRIPE_WEBHOOK_SECRET_NEW_MAR24", return_entire_dict=True
        )
        endpoint_secret = endpoint_secret_dict.get("development")
        return handle_webhook(payloadbody, stripe_signature, endpoint_secret, True)

    except Exception as e:
        raise handle_exception(e)


def handle_webhook(payloadbody, stripe_signature, endpoint_secret, is_dev):

    from starlette.responses import JSONResponse

    payments_api_utils.stripe_webhook(
        payloadbody, stripe_signature, endpoint_secret, is_dev
    )

    return JSONResponse(content={"success": True})


@api_router.post(
    f"/{PROCESS_NAME}/UpdateOrder/",
)
def update_order(
    order_details: PaymentsGetOrUpdateOrder,
    token: str = Security(get_current_token),
):
    """
    Allows an update of a pre-existing Order, eg update shipped date or received date
    <br>
    http://localhost:8000/payments-api/UpdateOrder/
    <br>
    https://data.resmagic.io/payments-api/UpdateOrder/

    {


        "one_number": 0,
        "current_sku": "KT-2011 LTCSL OCTCLW 3ZZMD",
        "order_number": "KT-45694",
        "quantity": 1,
        "shipping_notification_sent_cust_datetime": null,
        "ready_to_ship_datetime": null,
        "shipped_datetime": "2023-09-20T13:58:12.761Z",
        "received_by_cust_datetime": null,
        "process_id": "my_bot_name",
        "metadata": {},
        "fulfilled_from_inventory": "True",
        "hand_delivered":"False",
        "updated_at": "2023-09-20T13:58:12.761Z"
    }

    """
    try:
        log_increment_metrics(inspect.currentframe().f_code.co_name, order_details)
        return payments_api_utils.update_order(order_details, postgres)
    except Exception as e:
        raise handle_exception(e)


app.include_router(api_router)

if __name__ == "__main__":
    import uvicorn

    postgres = res.connectors.load("postgres")
    uvicorn.run(app, host="0.0.0.0", port=8000)
