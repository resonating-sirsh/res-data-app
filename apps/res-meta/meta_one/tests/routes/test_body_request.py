from uuid import uuid4

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from httpx import Response
import pytest
from pytest_mock import MockerFixture
from schemas.pydantic.body_requests.body_requests import (
    BodyRequestStatus,
    BodyRequestType,
)

from source.routes.body_requests.models import (
    BodyRequestResponse,
    CreateBodyRequestInput,
    UpdateBodyRequestInput,
)
from source.routes.body_requests.routes import router
from source.routes.context import BodyRequestRouteContext, get_body_request_context
from tests.fixtures.mock_body_request_asset_annotations import (
    MockBodyRequestAssetAnnotationController,
)

from ..fixtures.mock_body_request_assets import MockBodyRequestAssetsController
from ..fixtures.mock_body_requests_controllers import MockController
from ..fixtures.mock_label_style_controller import MockLabelStyleController

asset_controller = MockBodyRequestAssetsController()
bbr_controller = MockController()


class TestBodyRequest:
    """
    Test the BrandBodyRequestPostgresController

    This meant to be a Integration test, but since I'm having issue running this Test CI.

    This test class is skipped for now.
    """

    @pytest.fixture(autouse=True)
    def before_each(self, mocker: MockerFixture):
        value = mocker.patch(
            "res.connectors.postgres.PostgresConnector.connect_to_postgres",
        )
        value.__enter__.return_value = object()
        self.app = FastAPI()
        self.app.include_router(router)

        def mock_func():
            record = BodyRequestRouteContext(
                asset_controller=asset_controller,
                bbr_controller=bbr_controller,
                annotation_controller=MockBodyRequestAssetAnnotationController(),
                label_style_controller=MockLabelStyleController(),
            )
            return record

        self.app.dependency_overrides[get_body_request_context] = mock_func
        yield

    @pytest.fixture
    def server(self) -> TestClient:
        return TestClient(self.app)

    def test_cannot_get_brand_body_requests(self, server: TestClient):
        bbr_id = uuid4()
        with server as client:
            response: Response = client.get(f"/{bbr_id}")
            print(response.json())
            assert response.status_code == 404
            response_body = HTTPException(
                **response.json(), status_code=response.status_code
            )
            assert response_body.detail == "Brand body request not found"

    def test_create_brand_body_requests(self, server: TestClient):
        input = CreateBodyRequestInput(
            brand_code="TT",
            name="Test Body",
            status=BodyRequestStatus.DRAFT,
            request_type=BodyRequestType.CREATE_FROM_SCRATCH,
        )

        brand_code = "TT"

        with server as client:
            response: Response = client.post(
                f"/",
                json=input.dict(),
            )
            response_json = response.json()
            print(response_json)

            assert response.status_code == 201

            response_body = BodyRequestResponse(**response_json)
            assert response_body.body_request.name == input.name
            assert response_body.body_request.status == input.status
            assert response_body.body_request.request_type == input.request_type
            assert response_body.body_request.brand_code == brand_code
            assert response_body.body_request.id is not None

    def test_update_brand_body_requests(self, server: TestClient):
        with server as client:
            brand_code = "TT"

            input = CreateBodyRequestInput(
                name="Test Body",
                brand_code=brand_code,
                status=BodyRequestStatus.DRAFT,
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
            )

            with server as client:
                response: Response = server.post("/", json=input.dict())
                response_json = response.json()

                if response.status_code != 201:
                    raise Exception("Failed to create brand body request")

                response_body = BodyRequestResponse(**response_json)

                update_input = UpdateBodyRequestInput(
                    name="Test Body Updated",
                )
                response: Response = client.put(
                    f"/{response_body.body_request.id}",
                    json=update_input.dict(),
                )

                assert response.status_code == 200
                updated_response_body = BodyRequestResponse(**response.json())

                assert (
                    updated_response_body.body_request.id
                    == response_body.body_request.id
                )
                assert updated_response_body.body_request.name == update_input.name
                assert updated_response_body.body_request.status == input.status
                assert (
                    updated_response_body.body_request.request_type
                    == input.request_type
                )
                assert updated_response_body.body_request.brand_code == brand_code
                assert updated_response_body.body_request.id is not None

    def test_delete_brand_body_request(self, server: TestClient):
        with server as client:
            input = CreateBodyRequestInput(
                name="Test Body",
                brand_code="TT",
                status=BodyRequestStatus.DRAFT,
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
            )

            response: Response = server.post("/", json=input.dict())
            response_json = response.json()

            if response.status_code != 201:
                raise Exception("Failed to create brand body request")

            response_body = BodyRequestResponse(**response_json)

            response: Response = client.delete(
                f"/{response_body.body_request.id}",
            )

            assert response.status_code == 200
            assert response.json() == True

    def test_delete_brand_body_request_permanently(self, server: TestClient):
        with server as client:
            input = CreateBodyRequestInput(
                name="Test Body",
                brand_code="TT",
                status=BodyRequestStatus.DRAFT,
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
            )

            response: Response = server.post("/", json=input.dict())
            response_json = response.json()

            if response.status_code != 201:
                raise Exception("Failed to create brand body request")

            response_body = BodyRequestResponse(**response_json)

            response: Response = client.delete(
                f"/{response_body.body_request.id}",
                params={"permanently": True},
            )

            assert response.status_code == 200
            assert response.json() == True
