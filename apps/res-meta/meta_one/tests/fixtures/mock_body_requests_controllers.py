import datetime
from uuid import UUID, uuid4

from fastapi import HTTPException
from res.utils import logger
from schemas.pydantic.body_requests.body_requests import BodyRequest

from source.routes.body_requests.controllers import BodyRequestController
from source.routes.body_requests.models import (
    CreateBodyRequestInput,
    UpdateBodyRequestInput,
)


class MockController(BodyRequestController):
    brand_body_requests = []

    def __init__(self, **kwargs):
        logger.info(f"Initializing MockController, kwargs: {kwargs}")

    def get_brand_body_request_by_id(self, id: UUID) -> BodyRequest:
        logger.info(f"Getting brand body request by id belongs to {id}")
        logger.debug(f"Brand body requests: {self.brand_body_requests}")
        for bbr in self.brand_body_requests:
            if bbr.id == str(id):
                return bbr
        raise HTTPException(status_code=404, detail="Brand body request not found")

    def create_brand_body_requests(
        self,
        input: CreateBodyRequestInput,
    ) -> BodyRequest:
        logger.info("Creating brand body request")
        data = BodyRequest(
            id=str(uuid4()),
            name=input.name,
            brand_code=input.brand_code,
            status=input.status,
            request_type=input.request_type,
            body_category_id=input.body_category_id,
            body_onboarding_material_ids=input.body_onboarding_material_ids,
            body_code=input.body_code,
            meta_body_id=str(input.meta_body_id) if input.meta_body_id else None,
        )

        self.brand_body_requests.append(data)
        return data

    def update_brand_body_requests(
        self,
        id: UUID,
        input: UpdateBodyRequestInput,
    ) -> BodyRequest:
        logger.info(f"Updating brand body request for id {id}")
        for bbr in self.brand_body_requests:
            if bbr.id == str(id):
                input_dict = input.dict(exclude_unset=True)
                data_dict = bbr.dict()
                for key, value in input_dict.items():
                    data_dict[key] = value
                data_dict["updated_at"] = datetime.datetime.utcnow()
                if not input_dict["status"]:
                    data_dict["status"] = bbr.status
                bbr = BodyRequest(**data_dict)
                return bbr
        raise HTTPException(status_code=404, detail="Brand Body Request not found")

    def delete_brand_body_request(self, id: UUID, permanently: bool = False) -> bool:
        logger.info(f"Deleting brand body request for id {id}")
        for bbr in self.brand_body_requests:
            if bbr.id == str(id):
                if permanently:
                    self.brand_body_requests.remove(bbr)
                else:
                    # replace the bbr

                    new_bbr = BodyRequest(
                        deleted_at=datetime.datetime.utcnow(),
                        **bbr.dict(exclude={"deleted_at"}),
                    )
                    self.brand_body_requests.remove(bbr)
                    self.brand_body_requests.append(new_bbr)
                return True
        raise HTTPException(status_code=404, detail="Brand Body Request not found")
