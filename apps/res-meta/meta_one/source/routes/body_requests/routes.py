from http import HTTPStatus
from uuid import UUID
from fastapi import APIRouter, Depends
from fastapi_utils.cbv import cbv

from schemas.pydantic.body_requests.body_request_asset import BodyRequestAssetType

from ..context import (
    BodyRequestRouteContext,
    get_body_request_context,
)

from .models import (
    BodyRequestResponse,
    BodyRequestAssetInput,
    CreateBodyRequestInput,
    UpdateBodyRequestInput,
)


router = APIRouter()


@cbv(router)
class Router:
    context: BodyRequestRouteContext = Depends(get_body_request_context)

    @router.get(
        "/{id}",
        status_code=HTTPStatus.OK,
        responses={HTTPStatus.OK: {"model": BodyRequestResponse}},
    )
    def get_body_request_by_id(self, id: UUID) -> BodyRequestResponse:
        return BodyRequestResponse(
            body_request=self.context.bbr_controller.get_brand_body_request_by_id(id=id)
        )

    @router.post(
        "/",
        status_code=HTTPStatus.CREATED,
        responses={HTTPStatus.CREATED: {"model": BodyRequestResponse}},
    )
    def create_body_request(
        self,
        input: CreateBodyRequestInput,
    ) -> BodyRequestResponse:
        bbr_record = self.context.bbr_controller.create_brand_body_requests(input)
        self.context.asset_controller.create_assets(
            body_request_id=bbr_record.id,
            asset_type=BodyRequestAssetType.ASSET_2D,
            assets=input.assets,
        )
        return BodyRequestResponse(
            body_request=self.context.bbr_controller.get_brand_body_request_by_id(
                id=UUID(bbr_record.id)
            ),
        )

    @router.put(
        "/{id}",
        status_code=HTTPStatus.OK,
        responses={
            HTTPStatus.OK: {"model": BodyRequestResponse},
        },
    )
    def update_body_request(
        self,
        id: UUID,
        input: UpdateBodyRequestInput,
    ) -> BodyRequestResponse:
        to_create_assets = [
            BodyRequestAssetInput(name=asset.name, uri=asset.uri, type=asset.type)
            for asset in input.assets
            if not asset.id
        ]

        to_update_assets = [asset for asset in input.assets if asset.id and asset.uri]

        if to_create_assets:
            self.context.asset_controller.create_assets(
                body_request_id=str(id),
                asset_type=BodyRequestAssetType.ASSET_2D,
                assets=to_create_assets,
            )

        if to_update_assets:
            self.context.asset_controller.update_assets(
                assets=to_update_assets,
            )
        if input.delete_assets_ids:
            self.context.asset_controller.delete_assets(
                ids=input.delete_assets_ids,
            )
        return BodyRequestResponse(
            body_request=self.context.bbr_controller.update_brand_body_requests(
                id,
                input,
            )
        )

    @router.delete(
        "/{id}",
        status_code=HTTPStatus.OK,
    )
    def delete_body_request(
        self,
        id: UUID,
        permanently: bool = False,
    ) -> bool:
        self.context.asset_controller.delete_assets(
            body_request_id=str(id),
            permanently=permanently,
        )
        return self.context.bbr_controller.delete_brand_body_request(
            id=id,
            permanently=permanently,
        )
