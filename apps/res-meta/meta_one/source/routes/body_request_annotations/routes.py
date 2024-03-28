from http import HTTPStatus
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi_utils.cbv import cbv
from res.utils import logger
from .models import (
    BodyRequestAnnotation,
    BodyRequestAssetAnnotationType,
    BodyRequestAnnotationResponse,
    BodyRequestAnnotationInput,
    BodyRequestAnnotationUpdateInput,
)
from source.routes.context import BodyRequestRouteContext, get_body_request_context


router = APIRouter()


@cbv(router)
class Router:
    context: BodyRequestRouteContext = Depends(get_body_request_context)

    @router.get(
        "/{id}",
        response_model=BodyRequestAnnotationResponse,
        status_code=HTTPStatus.OK,
    )
    def get_annotation_by_id(self, id: UUID) -> BodyRequestAnnotationResponse:
        logger.info("Getting body request asset annotation by id...")
        logger.debug(f"Id: {id}")
        record = self.context.annotation_controller.find_by_id(id)

        if not record:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
            )

        logger.incr_endpoint_requests(
            endpoint_name="get_body_request_asset_annotation_by_id",
            http_method="GET",
            response_code=HTTPStatus.OK,
        )
        return BodyRequestAnnotationResponse(
            body_request_annotation=record,
        )

    @router.get(
        "/",
        response_model=List[BodyRequestAnnotation],
        status_code=HTTPStatus.OK,
    )
    def get_annotations(
        self,
        asset_id: Optional[UUID] = Query(None),
        ids: List[UUID] = Query([]),
        type_: Optional[BodyRequestAssetAnnotationType] = Query(None, alias="type"),
        limit: int = Query(100),
    ) -> List[BodyRequestAnnotation]:
        logger.info("Getting body request asset annotations...")
        logger.debug(f"Asset id: {asset_id}")
        logger.debug(f"Ids: {ids}")
        logger.debug(f"Type: {type}")
        response = self.context.annotation_controller.find(
            asset_id=asset_id,
            ids=ids,
            annotation_type=type_,
            limit=limit,
        )
        logger.incr_endpoint_requests(
            "get_body_request_asset_annotations", "GET", "200"
        )
        return response

    @router.post(
        "/",
        response_model=BodyRequestAnnotationResponse,
        status_code=HTTPStatus.CREATED,
    )
    def create_annotations(
        self,
        input: BodyRequestAnnotationInput,
    ) -> BodyRequestAnnotationResponse:
        from psycopg2.errors import ForeignKeyViolation

        try:
            logger.info("Creating body request asset annotations...")
            logger.debug(f"Input: {input}")
            response = []
            new_annotation = BodyRequestAnnotation(**input.dict())
            logger.incr_endpoint_requests(
                "create_body_request_asset_annotations", "POST", "201"
            )
            response = self.context.annotation_controller.save(new_annotation)
            return BodyRequestAnnotationResponse(
                body_request_annotation=response,
            )
        except ForeignKeyViolation as e:
            logger.error(f"Foreign Key Violation in {input}", exception=e)
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail="Asset id does not exist",
            )

    @router.delete(
        "/",
        response_model=bool,
        status_code=HTTPStatus.OK,
    )
    def delete_annotations(
        self,
        ids: List[UUID] = Query([]),
        permanent: bool = Query(False),
    ) -> bool:
        logger.info(
            f"Deleting body request asset annotation ids {ids} permanent: {permanent}..."
        )
        logger.debug(f"Ids: {ids}")
        logger.debug(f"Permanent: {permanent}")
        self.context.annotation_controller.delete(
            ids=ids,
            hard=permanent,
        )
        logger.incr_endpoint_requests(
            "delete_body_request_asset_annotations", "DELETE", "200"
        )
        return True

    @router.put(
        "/{id}",
        status_code=HTTPStatus.OK,
        response_model=BodyRequestAnnotationResponse,
    )
    def update_body_request(
        self, id: UUID, input: BodyRequestAnnotationUpdateInput
    ) -> BodyRequestAnnotationResponse:
        logger.info("Updating body request asset annotation...")
        logger.debug(f"Id: {id}")
        logger.debug(f"Input: {input}")
        record = self.context.annotation_controller.find_by_id(id)
        if not record:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
            )
        only_set = input.dict(exclude_none=True)
        new_record = BodyRequestAnnotation(
            **record.dict(exclude=only_set.keys()),
            **only_set,
        )
        if not record:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
            )
        response = self.context.annotation_controller.save(new_record)
        logger.incr_endpoint_requests(
            "update_body_request_asset_annotation", "PUT", "200"
        )
        return BodyRequestAnnotationResponse(
            body_request_annotation=response,
        )
