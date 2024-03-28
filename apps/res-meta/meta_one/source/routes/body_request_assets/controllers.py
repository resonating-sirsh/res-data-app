import abc
from http import HTTPStatus
from typing import List, Optional
from uuid import UUID, uuid4
from fastapi import HTTPException
from res.connectors.postgres.PostgresConnector import PostgresConnector

from schemas.pydantic.body_requests.body_request_asset import (
    BodyRequestAsset,
    BodyRequestAssetType,
)

from ..base import Controller
from .models import UpsertBodyRequestAsset
from .sql_query import (
    DELETE_BODY_REQUEST_ASSETS_BY_BBR,
    DELETE_BODY_REQUEST_ASSETS_BY_IDS,
    DELETE_BODY_REQUEST_ASSETS_PERMANENTLY_BBR,
    DELETE_BODY_REQUEST_ASSETS_PERMANENTLY_IDS,
    SELECT_BODY_REQUEST_ASSET_BY_ID,
    SELECT_BODY_REQUEST_ASSET_BY_IDS,
    SELECT_BODY_REQUEST_ASSETS,
    UPDATE_BODY_REQUEST_ASSETS,
)
from res.utils import logger

from source.routes.body_requests.models import BodyRequestAssetInput


class BodyRequestAssetController(Controller, abc.ABC):
    """
    Body Request Asset Controller
    """

    @abc.abstractmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def get_asset(self, id: UUID) -> BodyRequestAsset:
        """
        Get Body Request Asset
        """
        pass

    @abc.abstractmethod
    def create_asset(
        self,
        input: BodyRequestAssetInput,
    ) -> BodyRequestAsset:
        """
        Create Body Request Asset
        """
        pass

    @abc.abstractmethod
    def create_assets(
        self,
        asset_type: BodyRequestAssetType,
        body_request_id: str,
        assets: List[BodyRequestAssetInput],
    ) -> None:
        """
        Create Body Request Assets
        """
        pass

    @abc.abstractmethod
    def get_assets(
        self,
        body_request_id: Optional[str] = None,
        ids: List[str] = [],
    ) -> List[BodyRequestAsset]:
        pass

    @abc.abstractmethod
    def update_assets(
        self,
        assets: List[UpsertBodyRequestAsset],
    ) -> None:
        """
        Update Body Request Assets
        """
        pass

    @abc.abstractmethod
    def delete_assets(
        self,
        ids: List[UUID] = [],
        body_request_id: Optional[str] = None,
        permanently: Optional[bool] = False,
    ) -> None:
        """
        Delete Body Request Assets
        """
        pass


class BodyRequestAssetPostgresController(BodyRequestAssetController):
    def __init__(self, pg_conn: PostgresConnector):
        self.db_connector = pg_conn

    def get_asset(self, id: UUID) -> BodyRequestAsset:
        logger.info(f"Getting Body Request Asset with id {id}")
        df = self.db_connector.run_query(
            query=SELECT_BODY_REQUEST_ASSET_BY_ID,
            data={"id": str(id)},
        )

        if df.empty:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f"Body Request Asset with id {id} not found",
            )

        logger.debug(df)

        row = df.iloc[0]

        # iterate over the rows and create the objects
        return BodyRequestAsset(
            id=row["id"],
            name=row["name"],
            uri=row["uri"],
            type=row["type"],
            brand_body_request_id=row["brand_body_request_id"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            deleted_at=None,
        )

    def create_asset(
        self,
        input: BodyRequestAssetInput,
    ) -> BodyRequestAsset:
        logger.info(f"Creating Body Request Asset with name {input.name}")
        logger.debug(f"input: {input}")
        logger.debug(f"body_request_id: {input.body_request_id}")
        id = str(uuid4())

        self.db_connector.insert_records(
            table="meta.body_request_assets",
            records=[
                {
                    "id": id,
                    "brand_body_request_id": input.body_request_id,
                    "type": input.type,
                    "name": input.name,
                    "uri": input.uri,
                }
            ],
        )

        return self.get_asset(id=UUID(id))

    def create_assets(
        self,
        asset_type: BodyRequestAssetType,
        body_request_id: str,
        assets: List[BodyRequestAssetInput],
    ) -> None:
        self.db_connector.insert_records(
            table="meta.body_request_assets",
            records=[
                {
                    "id": str(uuid4()),
                    "brand_body_request_id": body_request_id,
                    "type": asset_type,
                    "name": asset.name,
                    "uri": asset.uri,
                }
                for asset in assets
            ],
        )
        return

    def get_assets(
        self,
        body_request_id: Optional[str] = None,
        ids: List[str] = [],
    ) -> List[BodyRequestAsset]:
        logger.info(f"Getting Body Request Assets with ids {ids}")
        logger.debug(f"asset_ids: {ids}")
        logger.debug(f"body_request_id: {body_request_id}")

        response = []

        if body_request_id:
            df = self.db_connector.run_query(
                query=SELECT_BODY_REQUEST_ASSETS,
                data={"body_request_id": body_request_id},
            )

            if df.empty:
                return []

            logger.debug(df)
            response.extend(
                [
                    BodyRequestAsset(
                        id=row["id"],
                        name=row["name"],
                        uri=row["uri"],
                        type=row["type"],
                        brand_body_request_id=row["brand_body_request_id"],
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                        deleted_at=None,
                    )
                    for row in df.iloc
                ]
            )

        if ids:
            df = self.db_connector.run_query(
                query=SELECT_BODY_REQUEST_ASSET_BY_IDS,
                data={"ids": tuple(ids)},
            )

            if df.empty:
                return []

            logger.debug(df)
            response.extend(
                [
                    BodyRequestAsset(
                        id=row["id"],
                        name=row["name"],
                        uri=row["uri"],
                        type=row["type"],
                        brand_body_request_id=row["brand_body_request_id"],
                        created_at=row["created_at"],
                        updated_at=row["updated_at"],
                        deleted_at=None,
                    )
                    for row in df.iloc
                ]
            )

        # iterate over the rows and create the objects
        return response

    def update_assets(
        self,
        assets: List[UpsertBodyRequestAsset],
    ) -> None:
        for asset in assets:
            self.db_connector.run_update(
                query=UPDATE_BODY_REQUEST_ASSETS,
                data={
                    "id": str(asset.id),
                    "name": asset.name,
                    "uri": asset.uri,
                },
            )

    def delete_assets(
        self,
        ids: List[UUID] = [],
        body_request_id: Optional[str] = None,
        permanently: Optional[bool] = False,
    ):
        if not body_request_id:
            query = (
                DELETE_BODY_REQUEST_ASSETS_BY_IDS
                if not permanently
                else DELETE_BODY_REQUEST_ASSETS_PERMANENTLY_IDS
            )
            self.db_connector.run_update(
                query=query,
                data={"ids": tuple(str(id) for id in ids), "body_request_id": None},
            )
        else:
            query = (
                DELETE_BODY_REQUEST_ASSETS_BY_BBR
                if not permanently
                else DELETE_BODY_REQUEST_ASSETS_PERMANENTLY_BBR
            )
            self.db_connector.run_update(
                query=query,
                data={"body_request_id": body_request_id},
            )
