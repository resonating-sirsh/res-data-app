from uuid import uuid4
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from pytest_mock import MockerFixture
from source.routes.body_request_assets.models import (
    BodyRequestAssetInput,
    BodyRequestAsset,
    BodyRequestAssetType,
)
from source.routes.context import BodyRequestRouteContext, get_body_request_context
from tests.fixtures.mock_body_request_asset_annotations import (
    MockBodyRequestAssetAnnotationController,
)

from ..fixtures.mock_body_request_assets import MockBodyRequestAssetsController
from ..fixtures.mock_body_requests_controllers import MockController
from ..fixtures.mock_label_style_controller import MockLabelStyleController

controller = MockBodyRequestAssetsController()
bbr_controller = MockController()

bbr_id = str(uuid4())


class TestBodyRequestAssetRoute:
    client: TestClient
    record: BodyRequestAsset

    @pytest.fixture(autouse=True)
    def before_and_after_each_run(self, mocker: MockerFixture):
        from source.routes.body_request_assets.routes import router

        value = mocker.patch(
            "res.connectors.postgres.PostgresConnector.connect_to_postgres",
        )
        value.__enter__.return_value = object()

        app = FastAPI()

        app.include_router(router)

        self.client = TestClient(app)

        app.dependency_overrides[
            get_body_request_context
        ] = lambda: BodyRequestRouteContext(
            asset_controller=controller,
            bbr_controller=bbr_controller,
            annotation_controller=MockBodyRequestAssetAnnotationController(),
            label_style_controller=MockLabelStyleController(),
        )

        self.record = controller.create_asset(
            input=BodyRequestAssetInput(
                name="test",
                uri="s3://test",
                type=BodyRequestAssetType.ASSET_2D,
                body_request_id=uuid4(),
            ),
        )

        yield

    def test_cannot_get_body_request_asset(self):
        with self.client as client:
            response = client.get(f"/{bbr_id}")

            assert response.status_code == 404
            assert response.json() == {"detail": "Asset not found"}

    def test_create_body_request_asset(self):
        with self.client as client:
            input = {
                "name": "test",
                "uri": "s3://test",
                "type": "2D",
            }

            response = client.post(f"?body_request_id={bbr_id}", json=input)

            assert response.status_code == 201
            response = response.json()
            response = response["asset"]

            assert response["name"] == input["name"]
            assert response["uri"] == input["uri"]
            assert response["type"] == input["type"]
            assert response["id"] is not None

    def test_update_body_request_asset(self):
        with self.client as client:
            input = {
                "name": "test",
                "uri": "s3://test",
                "type": "2D",
            }
            response = client.put(f"/{self.record.id}", json=input)

            assert response.status_code == 200
            response = response.json()
            response = response["asset"]

            assert response["name"] == input["name"]
            assert response["uri"] == input["uri"]
            assert response["type"] == input["type"]
            assert response["id"] is not None

    def test_delete_body_request_asset(self):
        with self.client as client:
            response = client.delete(f"/{self.record.id}")

            assert response.status_code == 200
            assert response.json() == True

    def test_get_body_request_assets_by_ids(self):
        with self.client as client:
            response = client.get(f"/?ids={self.record.id}")

            assert response.status_code == 200
            response = response.json()

            assert len(response) == 1
            assert response[0]["id"] == self.record.id

    def test_get_body_request_assets_by_body_request_id(self):
        with self.client as client:
            response = client.get(
                f"/?body_request_id={self.record.brand_body_request_id}"
            )

            assert response.status_code == 200
            response = response.json()

            assert len(response) == 1
            assert response[0]["id"] == self.record.id
