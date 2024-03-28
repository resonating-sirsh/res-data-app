"""
FULFILLMENT-ONE.......
"""

import datetime
import os
import time
import traceback
import uuid

import src.fulfillment_one_utils as fulfillment_one_utils
import src.order_procesor as order_processor
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pyparsing import Optional
from structlog.contextvars import bound_contextvars

from schemas.pydantic.fulfillment_one import CreateOrderPayload, FulfillmentOrderPayload

import res
from res.utils import logger, ping_slack, secrets_client
from res.utils.logging.ResLogger import add_contextvar_to_log_message

PROCESS_NAME = "fulfillment-one"

# set up some logging stuff.
res.utils.logger.reset_processors()
res.utils.logger.prepend_processor(add_contextvar_to_log_message("app_name"))
res.utils.logger.prepend_processor(add_contextvar_to_log_message("request_id"))

app = FastAPI(
    title="Fulfillment Order Line Item API",
    openapi_url=f"/{PROCESS_NAME}/openapi.json",
    docs_url=f"/{PROCESS_NAME}/docs",
)
security = HTTPBearer()
api_router = APIRouter()


@app.middleware("http")
async def handle_with_logs(request: Request, call_next):
    request_id = request.headers.get("X-Request-Id", str(uuid.uuid4()))
    with bound_contextvars(request_id=request_id, app_name=PROCESS_NAME):
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
                f"[FULFILL] <@U018UETQEH1> Long running request {request_id} took {dt:.2f} seconds for path {request.scope['path']}",
                "autobots",
            )
        response.headers["X-Request-Id"] = request_id
        return response


def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    RES_FULFILLMENT_API_KEY = secrets_client.get_secret("FULFILLMENT_ONE_API_KEY")
    return token == RES_FULFILLMENT_API_KEY


@app.get(f"/{PROCESS_NAME}/healthcheck")
async def healthcheck():
    return {"status": "ok"}


# get line item endpoint that receives line_item_id: str and returns line item details
@app.get(f"/{PROCESS_NAME}/line-items/{{line_item_id}}")
async def getLineItem(
    *,
    line_item_id: str,
    api_key: str = Security(verify_api_key),
):
    """
    Get line item details that receives line_item_id: str and returns line item details
    """
    try:
        if not line_item_id:
            raise HTTPException(
                status_code=400,
                detail="Bad Request.  Either line_item_id must be provided",
            )
        return fulfillment_one_utils.get_line_item(line_item_id)
    except Exception as ex:
        print(ex)
        raise HTTPException(
            status_code=500,
            detail="Internal Server Error",
        )


@app.get(f"/{PROCESS_NAME}/orders/{{order_id}}")
async def getOrder(
    *,
    order_id: str,
    api_key: str = Security(verify_api_key),
):
    """
    Get order details using graphQL query
    """
    try:
        if not order_id:
            raise HTTPException(
                status_code=400,
                detail="Bad Request.  Either order_id must be provided",
            )
        return fulfillment_one_utils.get_order(order_id)
    except Exception as ex:
        logger.info(f"Failing in getOrder: {traceback.format_exc()}")

        raise HTTPException(
            status_code=500,
            detail="Internal Server Error",
        )


import datetime
from typing import Optional


@app.get(f"/{PROCESS_NAME}/orders_by_brand/")
async def getOrderByBrand(
    *,
    start_date: Optional[datetime.date] = datetime.date(2015, 1, 1),
    end_date: Optional[datetime.date] = datetime.date(2099, 1, 1),
    brand_code: Optional[str] = None,
    api_key: str = Security(verify_api_key),
):
    """
    Get all orders for a brand
    """
    import os

    try:
        if not brand_code:
            raise HTTPException(
                status_code=400,
                detail="Bad Request. brand_code must be provided",
            )
        retval = fulfillment_one_utils.get_orders_by_brand(
            brand_code, start_date, end_date
        )
        return retval
    except Exception as ex:
        print(ex)
        raise HTTPException(
            status_code=500,
            detail="Internal Server Error",
        )


@app.get(f"/{PROCESS_NAME}/production_requests_by_order/")
async def getProductionRequestsByOrder(
    *,
    order_number: str,
    include_cancelled: Optional[bool] = False,
    api_key: str = Security(verify_api_key),
):
    """
    Get all production requests by order....
    """

    try:
        if not order_number:
            raise HTTPException(
                status_code=400,
                detail="Bad Request. order_number must be provided",
            )
        retval = fulfillment_one_utils.get_production_requests_for_order(
            order_number=order_number, include_cancelled=include_cancelled
        )
        return retval
    except Exception as ex:
        logger.warn(
            f"Failing in getProductionRequestsByOrder: {traceback.format_exc()}"
        )
        raise HTTPException(
            status_code=500,
            detail="Internal Server Error",
        )


# nudge ......
@app.post(
    f"/{PROCESS_NAME}/orders", response_model=FulfillmentOrderPayload, status_code=201
)
async def createOrder(
    order_payload: CreateOrderPayload,
    api_key: str = Security(verify_api_key),
):
    """
    This endpoint receives a payload for create the orders, line items and if the line item are good in payment and style is 3D, place a production request.
    """
    try:
        return order_processor.create_order(order_payload)
    except Exception as ex:
        res.utils.logger.error(ex)
        ping_slack(
            f"[FULFILL] <@U018UETQEH1> Error creating order: {ex}",
            "fulfillment_api_alerts",
        )
        raise HTTPException(
            status_code=500,
            detail="Internal Server Error",
        )


app.include_router(api_router)

if __name__ == "__main__":
    # Use this for debugging purposes only
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5001, log_level="debug")
