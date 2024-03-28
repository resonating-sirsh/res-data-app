from http import HTTPStatus
from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, Query
from fastapi_utils.cbv import cbv
from res.flows.dxa.bertha import logger
from schemas.pydantic.body_requests.body_request_asset import (
    BodyRequestAsset,
)
from source.routes.body_request_assets.models import (
    BodyRequestAssetResponse,
    BodyRequestAssetInput,
    UpsertBodyRequestAsset,
)

from source.routes.context import BodyRequestRouteContext, get_body_request_context

router = APIRouter()


@cbv(router)
class Router:
    context: BodyRequestRouteContext = Depends(get_body_request_context)

    @router.post("/", status_code=HTTPStatus.CREATED)
    def create_assets(
        self,
        input: BodyRequestAssetInput,
    ) -> BodyRequestAssetResponse:
        logger.info(f"Create asset for body request {input.body_request_id}")
        logger.debug(f"body_request_id: {input.body_request_id}")
        logger.debug(f"input: {input}")
        logger.incr_endpoint_requests(
            "body_request.create_assets",
            "POST",
            HTTPStatus.CREATED,
        )

        return BodyRequestAssetResponse(
            asset=self.context.asset_controller.create_asset(input)
        )

    @router.put("/{asset_id}", status_code=HTTPStatus.OK)
    def update_asset(
        self,
        asset_id: UUID,
        input: BodyRequestAssetInput,
    ) -> BodyRequestAssetResponse:
        logger.info(f"Update asset {asset_id}")
        logger.debug(f"asset_id: {asset_id}")
        logger.debug(f"input: {input}")
        logger.incr_endpoint_requests(
            "body_request.update_asset",
            "PUT",
            HTTPStatus.OK,
        )
        self.context.asset_controller.update_assets(
            assets=[
                UpsertBodyRequestAsset(
                    id=asset_id,
                    **input.dict(),
                )
            ]
        )

        return BodyRequestAssetResponse(
            asset=self.context.asset_controller.get_asset(asset_id)
        )

    @router.delete("/{asset_id}", status_code=HTTPStatus.OK)
    def delete_asset(
        self, asset_id: UUID, permanently: bool = Query(default=False)
    ) -> bool:
        logger.info(f"Delete asset {asset_id}")
        logger.debug(f"asset_id: {asset_id}")
        logger.debug(f"permanently: {permanently}")
        logger.incr_endpoint_requests(
            "body_request.delete_asset",
            "DELETE",
            HTTPStatus.OK,
        )
        self.context.asset_controller.delete_assets(
            ids=[asset_id],
            permanently=permanently,
        )
        return True

    @router.get("/{asset_id}", status_code=HTTPStatus.OK)
    def get_asset(self, asset_id: UUID) -> BodyRequestAssetResponse:
        logger.info(f"Get asset by id {asset_id}")
        logger.debug(f"asset_id: {asset_id}")
        logger.incr_endpoint_requests("body_request.get_asset", "POST", HTTPStatus.OK)
        return BodyRequestAssetResponse(
            asset=self.context.asset_controller.get_asset(asset_id)
        )

    @router.get("/", status_code=HTTPStatus.OK)
    def get_assets(
        self,
        ids: Optional[List[UUID]] = Query(None),
        body_request_id: Optional[UUID] = Query(None),
    ) -> List[BodyRequestAsset]:
        logger.info(f"Get assets")
        logger.debug(f"asset_ids: {ids}")
        logger.debug(f"body_request_id: {body_request_id}")
        logger.incr_endpoint_requests("body_request.get_assets", "POST", HTTPStatus.OK)
        return self.context.asset_controller.get_assets(
            ids=[str(id) for id in ids] if ids else [],
            body_request_id=str(body_request_id) if body_request_id else None,
        )
