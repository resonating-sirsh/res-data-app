import abc
from uuid import UUID, uuid4

from fastapi import HTTPException
from res.connectors.postgres.PostgresConnector import PostgresConnector
from res.utils import logger
from schemas.pydantic.body_requests.body_requests import BodyRequest

from ..base import Controller

from .models import CreateBodyRequestInput, UpdateBodyRequestInput
from .sql_queries import (
    DELETE_BRAND_BODY_REQUEST,
    DELETE_BRAND_BODY_REQUEST_PERMANENTLY,
    GET_BRAND_BODY_REQUEST_BY_ID_AND_BRAND_CODE,
    INSERT_BRAND_BODY_REQUESTS,
    UPDATE_BRAND_BODY_REQUESTS,
)


class BodyRequestController(Controller, abc.ABC):
    """
    Brand body request controller interface
    """

    @abc.abstractmethod
    def __init__(self, **kwargs):
        pass

    @abc.abstractmethod
    def create_brand_body_requests(
        self,
        input: CreateBodyRequestInput,
    ) -> BodyRequest:
        """
        Create a brand body request
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def update_brand_body_requests(
        self,
        id: UUID,
        input: UpdateBodyRequestInput,
    ) -> BodyRequest:
        """
        Update a brand body request
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_brand_body_request_by_id(self, id: UUID) -> BodyRequest:
        """
        Get a brand body request by id
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_brand_body_request(self, id: UUID, permanently: bool = False) -> bool:
        """
        Delete a brand body request by id
        """
        raise NotImplementedError()


class BodyRequestPostgresController(BodyRequestController):
    """
    Brand body request controller implementation for postgres
    """

    db_connector: PostgresConnector

    def __init__(
        self,
        pg_conn: PostgresConnector,
    ):
        self.db_connector = pg_conn

    def create_brand_body_requests(
        self,
        input: CreateBodyRequestInput,
    ) -> BodyRequest:
        logger.info(f"Creating brand body request for brand: {input.brand_code}")
        logger.info(f"Input: {input}")
        # Why? we are defining the UUID on the application layer
        id = str(uuid4())

        meta_body = str(input.meta_body_id) if input.meta_body_id else None

        self.db_connector.run_update(
            query=INSERT_BRAND_BODY_REQUESTS,
            data={
                "id": id,
                "brand_code": input.brand_code,
                "name": input.name,
                "status": input.status,
                "request_type": input.request_type,
                "body_category_id": input.body_category_id,
                "body_onboarding_material_ids": ",".join(
                    (input.body_onboarding_material_ids or []).copy()
                ),
                "combo_material_id": input.combo_material_id,
                "lining_material_id": input.lining_material_id,
                "body_code": input.body_code,
                "meta_body_id": meta_body,
                "fit_avatar_id": input.fit_avatar_id,
                "size_scale_id": input.size_scale_id,
                "base_size_code": input.base_size_code,
                "created_by": input.created_by,
            },
        )

        return self.get_brand_body_request_by_id(UUID(id))

    def update_brand_body_requests(
        self,
        id: UUID,
        input: UpdateBodyRequestInput,
    ) -> BodyRequest:
        """
        Update a brand body request
        """
        logger.info(f"Updating brand body request with id: {id}")
        logger.info(f"Input: {input}")
        record = self.get_brand_body_request_by_id(id)

        self.db_connector.run_update(
            query=UPDATE_BRAND_BODY_REQUESTS,
            data={
                "id": str(id),
                "name": input.name or record.name,
                "status": input.status or record.status,
                "body_category_id": input.body_category_id or record.body_category_id,
                "fit_avatar_id": input.fit_avatar_id or record.fit_avatar_id,
                "body_onboarding_material_ids": ",".join(
                    (input.body_onboarding_material_ids or []).copy()
                )
                if input.body_onboarding_material_ids
                else record.body_onboarding_material_ids,
                "combo_material_id": input.combo_material_id
                or record.combo_material_id,
                "lining_material_id": input.lining_material_id
                or record.lining_material_id,
                "size_scale_id": input.size_scale_id or record.size_scale_id,
                "base_size_code": input.base_size_code or record.base_size_code,
            },
        )

        return self.get_brand_body_request_by_id(id)

    def get_brand_body_request_by_id(
        self,
        id: UUID,
    ) -> BodyRequest:
        """
        Get a brand body request
        """
        df = self.db_connector.run_query(
            query=GET_BRAND_BODY_REQUEST_BY_ID_AND_BRAND_CODE,
            data={"id": str(id)},
        )

        if df.empty:
            raise HTTPException(status_code=404, detail="Brand Body Request not found")

        row = df.iloc[0]

        logger.debug(row)

        return BodyRequest(
            id=row["id"],
            name=row["name"],
            brand_code=row["brand_code"],
            meta_body_id=row["meta_body_id"],
            body_category_id=row["body_category_id"],
            body_code=row["body_code"],
            fit_avatar_id=row.get("fit_avatar_id", None),  # type: ignore
            body_onboarding_material_ids=[
                x
                for x in (row["body_onboarding_material_ids"] or "").split(",")
                if x != ""
            ],
            size_scale_id=row["size_scale_id"],
            combo_material_id=row["combo_material_id"],
            base_size_code=row["base_size_code"],
            lining_material_id=row["lining_material_id"],
            status=row["status"],
            request_type=row["request_type"],
            created_by=row["created_by"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def delete_brand_body_request(self, id: UUID, permanently: bool = False) -> bool:
        """
        Delete a brand body request
        """
        if permanently:
            self.db_connector.run_update(
                query=DELETE_BRAND_BODY_REQUEST_PERMANENTLY,
                data={"id": str(id)},
            )
        else:
            self.db_connector.run_update(
                query=DELETE_BRAND_BODY_REQUEST,
                data={"id": str(id)},
            )
        return True
