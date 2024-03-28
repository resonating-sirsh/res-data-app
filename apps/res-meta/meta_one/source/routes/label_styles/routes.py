from typing import List
from fastapi import APIRouter, Depends, Query
from fastapi_utils.cbv import cbv
from ..context import get_body_request_context, BodyRequestRouteContext
from schemas.pydantic.body_requests.label_styles import LabelStyle
from http import HTTPStatus

router = APIRouter()


@cbv(router)
class Routes:
    context: BodyRequestRouteContext = Depends(get_body_request_context)

    @router.get("/", response_model=List[LabelStyle], status_code=HTTPStatus.OK)
    def get_label_styles(self, limit: int = Query(100)) -> List[LabelStyle]:
        return self.context.label_style_controller.find(limit)
