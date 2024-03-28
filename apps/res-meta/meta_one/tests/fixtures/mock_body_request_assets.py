from datetime import datetime
from typing import List
from uuid import UUID, uuid4

from fastapi.exceptions import HTTPException
from schemas.pydantic.body_requests.body_request_asset import (
    BodyRequestAssetType,
    BodyRequestAsset,
    Optional,
)

from source.routes.body_request_assets.controllers import BodyRequestAssetController
from source.routes.body_requests.models import (
    BodyRequestAssetInput,
    UpsertBodyRequestAsset,
)


class MockBodyRequestAssetsController(BodyRequestAssetController):
    assets: List[BodyRequestAsset] = []

    def __init__(self, **kwargs):
        print(f"MockBodyRequestAssetsController: {kwargs}")
        pass

    def get_asset(self, id: UUID) -> BodyRequestAsset:
        print(id)
        for asset in self.assets:
            if str(asset.id) in str(id):
                return asset
        raise HTTPException(detail="Asset not found", status_code=404)

    def create_asset(
        self,
        input: BodyRequestAssetInput,
    ) -> BodyRequestAsset:
        body_request_asset = BodyRequestAsset(
            id=str(uuid4()),
            name=input.name,
            uri=input.uri,
            type=input.type,
            brand_body_request_id=str(input.body_request_id),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            deleted_at=None,
        )

        self.assets.append(body_request_asset)

        return body_request_asset

    def create_assets(
        self,
        asset_type: BodyRequestAssetType,
        body_request_id: str,
        assets: List[BodyRequestAssetInput],
    ) -> None:
        for asset in assets:
            body_request_asset = BodyRequestAsset(
                id=str(uuid4()),
                name=asset.name,
                uri=asset.uri,
                type=asset_type,
                brand_body_request_id=body_request_id,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                deleted_at=None,
            )

            self.assets.append(body_request_asset)

    def get_assets(
        self,
        body_request_id: Optional[str] = None,
        ids: List[str] = [],
    ) -> List[BodyRequestAsset]:
        if ids:
            return list(
                filter(
                    lambda x: x.id in ids and x.deleted_at == None,
                    self.assets.copy(),
                )
            )
        return list(
            filter(
                lambda x: x.brand_body_request_id == body_request_id
                and x.deleted_at == None,
                self.assets.copy(),
            )
        )

    def update_assets(self, assets: List[UpsertBodyRequestAsset]) -> None:
        print(assets)
        for asset in assets:
            for index, body_request_asset in enumerate(self.assets):
                if body_request_asset.id == asset.id:
                    self.assets[index].name = asset.name
                    self.assets[index].uri = asset.uri
                    break

    def delete_assets(
        self,
        ids: List[UUID] = [],
        body_request_id: Optional[str] = None,
        permanently: Optional[bool] = False,
    ):
        for index, body_request_asset in enumerate(self.assets):
            if (
                body_request_id
                and body_request_asset.brand_body_request_id == body_request_id
            ):
                if permanently:
                    del self.assets[index]
                else:
                    self.assets[index].deleted_at = datetime.now()
                continue
            if body_request_asset.id in ids:
                if permanently:
                    del self.assets[index]
                else:
                    self.assets[index].deleted_at = datetime.now()
                break
