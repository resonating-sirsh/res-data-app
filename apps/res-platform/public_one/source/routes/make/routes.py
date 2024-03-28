from res.utils import logger
from fastapi import APIRouter, Path, Query
from fastapi_utils.cbv import cbv
import typing
from pydantic import BaseModel
from . import get_current_token, Security, determine_brand_context, ping_slack_sirsh
import res
from schemas.pydantic.make import OneOrderStatusNodeAndExpDate, OneOrderStatus
from fastapi import HTTPException
import traceback


def get_make_routes() -> APIRouter:
    router = APIRouter()

    @cbv(router)
    class _Router:
        @router.get(
            f"/sell/order/make_status/",
            response_model=typing.List[OneOrderStatusNodeAndExpDate],
            status_code=200,
        )
        def get_order_make_status(
            self,
            order_number: str,
            token: str = Security(get_current_token),
        ) -> typing.List[OneOrderStatusNodeAndExpDate]:
            """

            ***
            Gets list of status details for an order, namely expected delivery date and current make/assembly node information
            ***

            """
            try:
                from pydantic import parse_obj_as
                from res.flows.make.production.queries import (
                    get_order_make_status_query,
                )

                brand_code = determine_brand_context(token)
                one_list = get_order_make_status_query(
                    order_number, brand_code=brand_code
                )

                return parse_obj_as(typing.List[OneOrderStatusNodeAndExpDate], one_list)
            except:
                ping_slack_sirsh(
                    f"Public ONE /sell/order/make_status/ {traceback.format_exc()} "
                )
                raise HTTPException(501, "Server Error")

        @router.get(
            f"/make/ones/pieces/status/",
            response_model=OneOrderStatus,
            status_code=200,
        )
        def one_pieces_status(
            self,
            one_number: int = Query(
                ...,
                example="19010101",
                description="The production request number for the ONE",
            ),
            # signed_s3_urls: bool = True,
            token: str = Security(get_current_token),
        ) -> OneOrderStatus:
            """

            ***
            Get the status of each piece in the ONE production request including node and contracts
            ***

            """
            signed_s3_urls = True

            try:
                from res.flows.make.production.queries import (
                    get_one_piece_status_with_healing,
                )

                d = get_one_piece_status_with_healing(one_number)

                if d["sku"][:2] != determine_brand_context(token):
                    return HTTPException(
                        status_code=401,
                        detail="Not Authorized to look at assets outside of brand",
                    )

                s3 = res.connectors.load("s3")
                if signed_s3_urls:
                    for p in d["one_pieces"]:
                        p["uri"] = s3.generate_presigned_url(p["uri"])

                return OneOrderStatus(**d)
            except:
                ping_slack_sirsh(
                    f"Public ONE /make/ones/pieces/status/ {traceback.format_exc()} "
                )
                raise HTTPException(501, "Server Error")

    return router


router = get_make_routes()
