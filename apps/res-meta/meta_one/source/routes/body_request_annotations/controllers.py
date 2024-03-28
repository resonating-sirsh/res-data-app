import abc
from typing import List, Optional
from uuid import UUID

from pandas import DataFrame
from res.connectors.postgres.PostgresConnector import PostgresConnector
from res.utils import logger

import source.routes.body_request_annotations.sql_query as query
from schemas.pydantic.body_requests.body_request_asset_annotations import (
    BodyRequestAnnotation,
    BodyRequestAssetAnnotationType,
)

from ..base import Controller


class BodyRequestAnnotationController(Controller, abc.ABC):
    @abc.abstractmethod
    def save(self, record: BodyRequestAnnotation) -> BodyRequestAnnotation:
        """
        Save a body request annotation and save it to the database
         - Update in case of the body_request_annotation already exists in the DB.
         - Create in case of the body_request_annotation does not exist in the DB.
        """
        ...

    @abc.abstractmethod
    def find_by_id(self, id: UUID) -> Optional[BodyRequestAnnotation]:
        """
        Find a body request annotation by id in the db.
        """
        ...

    @abc.abstractmethod
    def find(
        self,
        ids: Optional[List[UUID]] = None,
        asset_id: Optional[UUID] = None,
        annotation_type: Optional[BodyRequestAssetAnnotationType] = None,
        limit: int = 100,
    ) -> List[BodyRequestAnnotation]:
        """
        Find a body request annotation by id in the db.
        """
        ...

    @abc.abstractmethod
    def delete(
        self,
        ids: List[UUID],
        hard: bool = False,
    ) -> bool:
        """
        Delete a body request annotation by id in the db.
        """
        ...


class BodyRequestAnnotationPostgresController(BodyRequestAnnotationController):
    pg_conn: PostgresConnector

    def __init__(self, pg_conn):
        self.pg_conn = pg_conn

    def save(self, record: BodyRequestAnnotation) -> BodyRequestAnnotation:
        logger.debug(f"Saving body request annotation: {record}")
        self.pg_conn.run_update(
            query=query.UPSERT_BODY_REQUEST_ANNOTATION,
            data={
                "id": str(record.id),
                "asset_id": str(record.asset_id),
                "label_style_id": str(record.label_style_id)
                if record.label_style_id
                else None,
                **record.dict(exclude={"id", "asset_id", "label_style_id"}),
            },
        )

        return record

    def find_by_id(self, id: UUID) -> Optional[BodyRequestAnnotation]:
        df = self.pg_conn.run_query(
            query=query.FIND_BY_ID_BODY_REQUEST_ANNOTATION,
            data={"id": str(id)},
        )
        logger.debug(f"Found body request annotation: {df}")
        response = self._from_df(df)
        if not response:
            return None
        return response[0]

    def find(
        self,
        ids: Optional[List[UUID]] = [],
        asset_id: Optional[UUID] = None,
        annotation_type: Optional[BodyRequestAssetAnnotationType] = None,
        limit: int = 100,
    ) -> List[BodyRequestAnnotation]:
        data = {
            "ids": None,
            "asset_id": None,
            "annotation_type": None,
            "limit": limit,
        }
        if ids:
            data["ids"] = ", ".join(tuple(str(id) for id in ids))
        if asset_id:
            data["asset_id"] = str(asset_id) if asset_id else None
        if annotation_type:
            data["annotation_type"] = annotation_type.value if annotation_type else None

        df = self.pg_conn.run_query(
            query=query.FIND_BODY_REQUEST_ANNOTATION,
            data=data,
        )
        logger.debug(f"Found body request annotations: {df}")
        response = self._from_df(df)
        if not response:
            return []
        return response

    def delete(
        self,
        ids: List[UUID],
        hard: bool = False,
    ) -> bool:
        data = {"ids": tuple(str(id) for id in ids)}
        statement = query.DELETE_BODY_REQUEST_ANNOTATION_SOFT
        if hard:
            statement = query.DELETE_BODY_REQUEST_ANNOTATION_HARD

        self.pg_conn.run_update(
            query=statement,
            data=data,
        )
        return True

    def _from_df(self, df: DataFrame) -> Optional[List[BodyRequestAnnotation]]:
        if df.empty:
            return None
        for col in ["asset_id"]:
            df[col] = df[col].apply(lambda x: UUID(x))
        df["annotation_type"] = df["annotation_type"].apply(
            lambda x: BodyRequestAssetAnnotationType[x]
        )
        return [BodyRequestAnnotation(**record) for record in df.to_dict("records")]
