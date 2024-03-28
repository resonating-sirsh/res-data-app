from typing import Optional
from uuid import UUID
from fastapi import HTTPException
from pydantic import EmailStr
import pytest
from schemas.pydantic.body_requests.body_request_asset import BodyRequestAssetType
from schemas.pydantic.body_requests.body_requests import (
    BodyRequest,
    BodyRequestStatus,
    BodyRequestType,
)
from source.routes.body_requests.controllers import (
    BodyRequestController,
    BodyRequestPostgresController,
)
from source.routes.body_requests.models import (
    BodyRequestAssetInput,
    CreateBodyRequestInput,
    UpdateBodyRequestInput,
)
from source.routes.context import BodyRequestRouteContext
from ..fixtures.mock_body_request_assets import MockBodyRequestAssetsController
from ..fixtures.mock_label_style_controller import MockLabelStyleController
from ..fixtures.mock_body_request_asset_annotations import (
    MockBodyRequestAssetAnnotationController,
)


@pytest.mark.skip(reason="Having issue running this Test CI")
class TestBodyRequestPostgresController:
    """
    Test the BrandBodyRequestPostgresController

    This meant to be a Integration test, but since I'm having issue running this Test CI.

    This test class is skipped for now.

    Using the BrandBodyRequestController to avoid, violating the Liskov Substitution Principle.
    """

    record: Optional[BodyRequest]

    @pytest.fixture
    def controller(self) -> BodyRequestController:
        from res.connectors import load

        controller = BodyRequestPostgresController(
            pg_conn=load(
                "postgres",
                conn_string="postgres://postgres:postgrespassword@localhost:5432/postgres",
            ),
        )

        controller.set_context(
            context=BodyRequestRouteContext(
                asset_controller=MockBodyRequestAssetsController(),
                bbr_controller=controller,
                annotation_controller=MockBodyRequestAssetAnnotationController(),
                label_style_controller=MockLabelStyleController(),
            ),
        )

        return controller

    @pytest.fixture(autouse=True)
    def run_before_and_after_tests(self, controller: BodyRequestController):
        input = CreateBodyRequestInput(
            name="Test Brand Body Request",
            brand_code="TT",
            request_type=BodyRequestType.CREATE_FROM_SCRATCH,
            status=BodyRequestStatus.DRAFT,
        )
        self.record = controller.create_brand_body_requests(input=input)

        yield

        try:
            if self.record:
                controller.delete_brand_body_request(
                    id=UUID(self.record.id),
                    permanently=True,
                )
        except HTTPException:
            pass

    @pytest.mark.parametrize(
        "input",
        [
            CreateBodyRequestInput(
                name="Test Brand Body Request",
                brand_code="TT",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                status=BodyRequestStatus.DRAFT,
                fit_avatar_id="test_id",
                body_category_id="test_id",
                body_code="test_id",
                assets=[
                    BodyRequestAssetInput(
                        name="Test Asset",
                        uri="s3://test/test.png",
                        type=BodyRequestAssetType.ASSET_2D,
                    ),
                    BodyRequestAssetInput(
                        name="Test Asset Ai",
                        uri="s3://test/test.ai",
                        type=BodyRequestAssetType.ASSET_2D,
                    ),
                    BodyRequestAssetInput(
                        name="Test Asset pd",
                        uri="s3://test/test.pd",
                        type=BodyRequestAssetType.ASSET_2D,
                    ),
                ],
            ),
            CreateBodyRequestInput(
                name="Test Brand Body Request",
                brand_code="TT",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                status=BodyRequestStatus.DRAFT,
                body_onboarding_material_ids=["test_id"],
                combo_material_id="Test Pocket",
                lining_material_id="Test Lining",
                size_scale_id="Test Size Range",
                base_size_code="CRTEST",
            ),
            CreateBodyRequestInput(
                name="Test Brand Body Request",
                brand_code="TT",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                status=BodyRequestStatus.DRAFT,
                body_onboarding_material_ids=["test_id"],
                combo_material_id="Test Pocket",
            ),
            CreateBodyRequestInput(
                name="Test Brand Body Request",
                brand_code="TT",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                status=BodyRequestStatus.DRAFT,
                body_onboarding_material_ids=["test_id"],
                lining_material_id="Test Lining",
            ),
            CreateBodyRequestInput(
                name="Test Brand Body Request",
                brand_code="TT",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                status=BodyRequestStatus.DRAFT,
                body_onboarding_material_ids=["test_id"],
                combo_material_id="Test Pocket",
                lining_material_id="Test Lining",
                created_by=EmailStr("test@test.com"),
            ),
        ],
    )
    def test_create_brand_body_requests(
        self,
        input: CreateBodyRequestInput,
        controller: BodyRequestController,
    ):
        record = controller.create_brand_body_requests(input=input)
        assert record.name == input.name
        assert record.brand_code == input.brand_code
        assert record.request_type == input.request_type
        assert record.status == input.status
        assert record.fit_avatar_id == input.fit_avatar_id

        assert len(record.body_onboarding_material_ids or []) == len(
            input.body_onboarding_material_ids or []
        )

        assert record.id is not None
        assert record.created_at is not None
        assert record.updated_at is not None
        assert record.deleted_at is None
        assert record.combo_material_id == input.combo_material_id
        assert record.lining_material_id == input.lining_material_id
        assert record.size_scale_id == input.size_scale_id
        assert record.base_size_code == input.base_size_code

        if input.created_by:
            assert EmailStr(record.created_by) == input.created_by

        controller.delete_brand_body_request(id=UUID(record.id), permanently=True)

    def test_update_brand_body_requests(self, controller: BodyRequestController):
        input = UpdateBodyRequestInput(
            name="Test Brand Body Request1",
            combo_material_id="Test Pocket",
            lining_material_id="Test Lining",
            size_scale_id="Test Size Range",
        )
        if not self.record:
            raise Exception("Record is not created")

        record = controller.update_brand_body_requests(
            id=UUID(self.record.id),
            input=input,
        )

        assert record.name == input.name
        assert record.brand_code == self.record.brand_code
        assert record.request_type == self.record.request_type
        assert record.status == self.record.status

        assert record.id is not None
        assert record.id == self.record.id
        assert record.updated_at != self.record.updated_at

        assert record.combo_material_id == input.combo_material_id
        assert record.lining_material_id == input.lining_material_id
        assert record.size_scale_id == input.size_scale_id

    def test_get_brand_body_request_by_id(self, controller: BodyRequestController):
        if not self.record:
            raise Exception("Record is not created")

        record = controller.get_brand_body_request_by_id(id=UUID(self.record.id))
        assert record.id == self.record.id
        assert record.name == self.record.name
        assert record.brand_code == self.record.brand_code
        assert record.request_type == self.record.request_type
        assert record.status == self.record.status

    def test_delete_brand_body_request(self, controller: BodyRequestController):
        if not self.record:
            raise Exception("Record is not created")

        assert (
            controller.delete_brand_body_request(
                id=UUID(self.record.id),
                permanently=True,
            )
            is True
        )

        try:
            controller.get_brand_body_request_by_id(id=UUID(self.record.id))
        except HTTPException as e:
            assert e.status_code == 404
            assert e.detail == "Brand Body Request not found"
            self.record = None
