from res.utils import logger
from fastapi import APIRouter, Path, Query
from fastapi_utils.cbv import cbv
import typing
from pydantic import BaseModel, root_validator
from . import get_current_token, Security, determine_brand_context, ping_slack_sirsh
import res
from schemas.pydantic.make import OneOrderStatusNodeAndExpDate, OneOrderStatus
from fastapi import HTTPException
import datetime
from res.flows.sell.orders import queries
import traceback
from res import get_meta_one_endpoint
from fastapi.responses import JSONResponse


class ShippingInfo(BaseModel):
    name: str
    email: str
    address_line_one: str
    address_line_two: typing.Optional[str]
    city: str
    zip_code: str
    country: str
    province: typing.Optional[str]
    phone: typing.Optional[str]


class LineItemInfo(BaseModel):
    sku: str
    quantity: int

    @root_validator
    def _ids(cls, values):
        sku = values.get("sku")
        if sku[2] == "-":
            sku = f"{sku[:2]}{sku[3:]}"
        values["sku"] = sku
        return values


class BrandOrder(BaseModel):
    brand_code: str
    shipping_details: ShippingInfo
    line_items: typing.List[LineItemInfo]

    def from_santiago_address(
        line_items: typing.List[LineItemInfo],
        brand_code="TT",
        email="techpirates@resonance.nyc",
    ):

        si = ShippingInfo(
            name="Resonance Manufacturing LTD",
            email=email,
            address_line_one="Corporacion Zona Franca Santiago",
            address_line_two="Segunda Etapa, Calle Navarrete No.4",
            city="Santiago",
            province="Santiago",
            phone="8095555555",
            zip_code="51000",
            country="Dominican Republic",
        )
        return BrandOrder(
            brand_code=brand_code,
            line_items=line_items,
            shipping_details=si,
        )


def _make_order(order: BrandOrder, order_channel="resmagic.io", sales_channel="ECOM"):
    return {
        "request_name": f"{order.brand_code}+{res.utils.res_hash()}",
        "brand_code": order.brand_code,
        "email": order.shipping_details.email,
        "order_channel": order_channel,
        "sales_channel": sales_channel,
        "shipping_name": order.shipping_details.name,
        "shipping_address1": order.shipping_details.address_line_one,
        "shipping_address2": order.shipping_details.address_line_two,
        "shipping_city": order.shipping_details.city,
        "shipping_country": order.shipping_details.country,
        "shipping_province": order.shipping_details.province,
        "shipping_zip": order.shipping_details.zip_code,
        "shipping_phone": order.shipping_details.phone,
        "line_items_info": [
            {
                "sku": oi.sku,
                "quantity": oi.quantity,
            }
            for oi in order.line_items
        ],
    }


def get_sell_routes() -> APIRouter:
    router = APIRouter()

    @cbv(router)
    class _Router:
        @router.post(
            f"/orders/placeOrder", name="Place orders for ONEs", status_code=201
        )
        async def placeOrder(
            self, order: BrandOrder, token: str = Security(get_current_token)
        ):
            """

            Place a custom order for a ONE using the SKU (Body Material Color Size)
            The address information for the custom is required for fulfillment
            Add one or more sku line items specifying the SKU and the quantity
            """

            ###
            # {
            # "brand_code": "TT",
            # "shipping_details": {
            #     "name": "Resonance Manufacturing LTD",
            #     "email": "techpirates@resonance.nyc",
            #     "address_line_one": "Corporacion Zona Franca Santiago'",
            #     "address_line_two": "Segunda Etapa, Calle Navarrete No.4",
            #     "city": "Santiago",
            #     "zip_code": "51000",
            #     "country": "DR",
            #     "province": "Santiago",
            #     "phone": "000000"
            # },
            # "line_items": [
            #     {
            #     "sku": "SI5000 CLTWL KENTLA 2ZZSM",
            #     "quantity": 1
            #     }
            # ]
            # }
            #
            try:

                if (
                    order.brand_code != determine_brand_context(token)
                    and "TT" != order.brand_code
                ):
                    return HTTPException(
                        401, "Not authorized to request data for this brand"
                    )

                """translate into the format used by the api"""
                o = _make_order(order=order)

                res.utils.logger.info(o)
                ep = get_meta_one_endpoint("/fulfillment-one/orders", "post")

                response = ep(json=o)

                res.utils.logger.info(response)

                if response.status_code in [200, 201]:
                    return JSONResponse(
                        status_code=201,
                        content={"message": "order created", "ref": response.json()},
                    )

                else:
                    raise HTTPException(
                        status_code=500,
                        detail="Internal Server Error",
                    )
            except:
                res.utils.logger.warn(traceback.format_exc())
                raise HTTPException(
                    status_code=500,
                    detail="Internal Server Error",
                )

        @router.get(f"/orders/", name="Get orders by brand code")
        async def getOrderByBrand(
            self,
            start_date: typing.Optional[str] = res.utils.dates.relative_to_now(
                2
            ).isoformat(),
            end_date: typing.Optional[str] = None,
            brandCode: typing.Optional[str] = Query(
                ...,
                example="TT",
                description="The brand code",
            ),
            token: str = Security(get_current_token),
        ) -> typing.List[dict]:
            """
            Get all orders for a brand by two character brand code
            The default dates are for the last two days.
            """
            import os

            end_date = end_date or res.utils.dates.utc_now_iso_string()

            try:
                if not brandCode:
                    return HTTPException(
                        status_code=400,
                        detail="Bad Request. brand_code must be provided",
                    )

                if brandCode != determine_brand_context(token):
                    return HTTPException(
                        401, "Not authorized to request data for this brand"
                    )

                # temp pagination
                return queries.get_orders_by_brand(brand_code=brandCode)[:100]
            except Exception as ex:
                res.utils.logger.warn(traceback.format_exc())
                ping_slack_sirsh(f"Public ONE /orders/ {traceback.format_exc()} ")
                raise HTTPException(
                    status_code=500,
                    detail="Internal Server Error",
                )

        @router.get(f"/order", name="Get orders by order name")
        async def getOrderByName(
            self,
            orderName: str = Query(
                ...,
                example="TT-12345",
                description="The order number from e-commerce including the brand prefix",
            ),
            token: str = Security(get_current_token),
        ):
            """
            ***
            Get order by name
            ***
            """
            import os

            try:
                bcode = determine_brand_context(token)

                o = queries.get_order_by_name(name=orderName, validate=False)
                if o:
                    o = o[0]
                    if o["brand_code"] != bcode:
                        return HTTPException(
                            401, "Not authorized to request data for this brand"
                        )

                return o
            except Exception as ex:
                res.utils.logger.warn(traceback.format_exc())
                ping_slack_sirsh(f"Public ONE /order/ {traceback.format_exc()} ")
                raise HTTPException(
                    status_code=500,
                    detail="Internal Server Error",
                )

    return router


router = get_sell_routes()
