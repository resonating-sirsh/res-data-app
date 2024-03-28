"""
Test BodyRequestAssetAnnotation

For some reason when I switch to postgres sql the asset_id in the input is not string is UUID. not idea why.

"""
from uuid import UUID, uuid4
from fastapi.testclient import TestClient
from fastapi import FastAPI

from pytest_mock import MockerFixture
import pytest
from schemas.pydantic.body_requests.body_request_asset import (
    BodyRequestAsset,
    BodyRequestAssetType,
)
from source.routes.body_request_annotations.routes import router
from ..fixtures.mock_body_request_asset_annotations import (
    MockBodyRequestAssetAnnotationController,
)
from source.routes.body_request_annotations.models import (
    BodyRequestAnnotation,
    BodyRequestAssetAnnotationType,
    BodyRequestAnnotationInput,
    BodyRequestAnnotationUpdateInput,
)
from ..fixtures.mock_label_style_controller import MockLabelStyleController

from schemas.pydantic.body_requests.label_styles import LabelStyle

from source.routes.context import BodyRequestRouteContext, get_body_request_context
from ..fixtures.mock_body_request_assets import MockBodyRequestAssetsController
from ..fixtures.mock_body_requests_controllers import MockController

controller = MockBodyRequestAssetsController()
bbr_controller = MockController()
annotation_controller = MockBodyRequestAssetAnnotationController()


class TestBodyRequestAssetAnnotation:
    client: TestClient
    asset: BodyRequestAsset
    record: BodyRequestAnnotation

    @pytest.fixture(autouse=True)
    def before_and_after_each_run(self, mocker: MockerFixture):
        value = mocker.patch(
            "res.connectors.postgres.PostgresConnector.connect_to_postgres",
        )

        value.__enter__.return_value = object()

        app = FastAPI()

        app.include_router(router)

        app.dependency_overrides[
            get_body_request_context
        ] = lambda: BodyRequestRouteContext(
            asset_controller=controller,
            bbr_controller=bbr_controller,
            annotation_controller=annotation_controller,
            label_style_controller=MockLabelStyleController(),
        )

        self.asset = BodyRequestAsset(
            name="test",
            uri="s3://test/test.jpg",
            type=BodyRequestAssetType.ASSET_2D,
            brand_body_request_id=str(uuid4()),
        )

        self.label_style_record = LabelStyle(
            id="test",
            name="test",
            thumbnail_url="s3://test/test.jpg",
            dimensions='1" x 1"',
            body_code="test",
        )

        self.record = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset.id)
            if type(self.asset.id) == str
            else self.asset.id,  # type: ignore
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            coordinate_x=0.0,
            coordinate_y=0.0,
            value="Neck's T-Shirt 1 \" ",
        )

        annotation_controller.save(self.record)

        self.client = TestClient(app)

    def test_get_record_by_id(self):
        with self.client as test:
            response = test.get(f"/{self.record.id}")
            assert response.status_code == 200
            response_json = response.json()
            assert response_json["bodyRequestAnnotation"]["id"] == str(self.record.id)

    def test_get_record_by_id_not_found(self):
        with self.client as test:
            response = test.get(f"/{uuid4()}")
            assert response.status_code == 404

    def test_get_records(self):
        with self.client as test:
            response = test.get("/")
            assert response.status_code == 200
            response_json = response.json()
            assert len(response_json) <= 100

    def test_get_records_filter_by_asset_id(self):
        with self.client as test:
            response = test.get(f"/?asset_id={self.asset.id}")
            assert response.status_code == 200
            response_json = response.json()
            for record in response_json:
                assert record["assetId"] == str(self.asset.id)

    def test_get_records_filter_by_asset_id_not_found(self):
        response = self.client.get(f"/?asset_id={uuid4()}")
        assert response.status_code == 200
        response_json = response.json()
        assert len(response_json) == 0

    def test_get_records_filter_by_ids(self):
        with self.client as test:
            response = test.get(f"/?ids={self.record.id}")
            assert response.status_code == 200
            response_json = response.json()
            assert len(response_json) >= 1

    def test_get_records_filter_by_ids_not_found(self):
        response = self.client.get(f"/?ids={uuid4()}")
        assert response.status_code == 200
        response_json = response.json()
        assert len(response_json) == 0

    def test_get_records_limit(self):
        with self.client as test:
            response = test.get(f"/?limit=1")
            assert response.status_code == 200
            response_json = response.json()
            assert len(response_json) == 1

    def test_get_records_multiple_filter(self):
        with self.client as test:
            response = test.get(
                f"/?asset_id={self.asset.id}&ids={self.record.id}&limit=1"
            )
            assert response.status_code == 200
            response_json = response.json()
            assert len(response_json) == 1

    def test_create_PRINT_LABEL_ON_BODY(self):
        with self.client as test:
            input = BodyRequestAnnotationInput(
                name="test",
                coordinate_x=0.0,
                coordinate_y=0.0,
                asset_id=UUID(self.asset.id)
                if type(self.asset.id) == str
                else self.asset.id,  # type: ignore
                annotation_type=BodyRequestAssetAnnotationType.PRINT_LABEL,
                artwork_uri="s3://test/test.jpg",
            )

            response = test.post(
                "/",
                json={
                    "assetId": str(self.asset.id),
                    **input.dict(exclude={"asset_id"}),
                },
            )
            assert response.status_code == 201
            data = response.json()["bodyRequestAnnotation"]

            assert (
                data["annotationType"]
                == BodyRequestAssetAnnotationType.PRINT_LABEL.value
            )
            assert data["coordinateX"] == 0.0
            assert data["coordinateY"] == 0.0
            assert data["coordinateZ"] is None
            assert data["assetId"] == str(self.asset.id)
            assert data["artworkUri"] == input.artwork_uri
            assert data["value"] is None
            assert data["sizeLabelUri"] is None

    def test_create_ADD_LABEL_DESIGN_MY_LABEL(self):
        with self.client as test:
            input = BodyRequestAnnotationInput(
                name="test",
                coordinate_x=0.0,
                coordinate_y=0.0,
                asset_id=UUID(self.asset.id)
                if type(self.asset.id) == str
                else self.asset.id,  # type: ignore
                annotation_type=BodyRequestAssetAnnotationType.ADD_LABEL_DESIGN_MY_LABEL,
                label_style_id=self.label_style_record.id,
                value="test",
            )

            response = test.post(
                "/",
                json={
                    **input.dict(exclude={"asset_id", "label_style_id"}),
                    "assetId": str(self.asset.id),
                    "labelStyleId": str(self.label_style_record.id),
                },
            )

            assert response.status_code == 201
            data = response.json()["bodyRequestAnnotation"]

            print(data, self.label_style_record.id)

            assert (
                data["annotationType"]
                == BodyRequestAssetAnnotationType.ADD_LABEL_DESIGN_MY_LABEL.value
            )
            assert data["coordinateX"] == 0.0
            assert data["coordinateY"] == 0.0
            assert data["coordinateZ"] is None
            assert data["assetId"] == str(self.asset.id)
            assert data["labelStyleId"] == str(input.label_style_id)
            assert data["value"] == input.value

    def test_create_ADD_LABEL_SUPPLY_LABEL(self):
        with self.client as test:
            input = BodyRequestAnnotationInput(
                name="test",
                coordinate_x=0.0,
                coordinate_y=0.0,
                asset_id=UUID(self.asset.id)
                if type(self.asset.id) == str
                else self.asset.id,  # type: ignore
                annotation_type=BodyRequestAssetAnnotationType.ADD_LABEL_SUPPLY_LABEL,
                main_label_uri="s3://test/test.jpg",
                size_label_uri="s3://test/test.jpg",
            )

            response = test.post(
                "/",
                json={
                    **input.dict(exclude={"asset_id"}),
                    "assetId": str(self.asset.id),
                },
            )

            assert response.status_code == 201
            data = response.json()["bodyRequestAnnotation"]

            assert (
                data["annotationType"]
                == BodyRequestAssetAnnotationType.ADD_LABEL_SUPPLY_LABEL.value
            )
            assert data["coordinateX"] == 0.0
            assert data["coordinateY"] == 0.0
            assert data["coordinateZ"] is None
            assert data["assetId"] == str(self.asset.id)
            assert data["mainLabelUri"] == input.main_label_uri
            assert data["sizeLabelUri"] == input.size_label_uri

    def test_soft_delete_record(self):
        with self.client as test:
            response = test.delete(f"/?ids={self.record.id}")
            assert response.status_code == 200
            response_json = response.json()
            assert response_json == True

            record = annotation_controller.find_by_id(UUID(self.record.id))
            if not record:
                raise Exception("Record not found")
            assert record.deleted_at is not None

    def test_hard_delete_record(self):
        with self.client as test:
            response = test.delete(f"/?ids={str(self.record.id)}&permanent=true")
            assert response.status_code == 200
            response_json = response.json()
            assert response_json == True

            result = annotation_controller.find_by_id(UUID(self.record.id))
            assert result is None

    def test_update_record(self):
        with self.client as test:
            input = BodyRequestAnnotationUpdateInput(
                name="test",
            )

            response = test.put(
                f"/{self.record.id}",
                json=input.dict(exclude_unset=True),
            )
            response_json = response.json()
            assert response.status_code == 200
            data = response_json["bodyRequestAnnotation"]
            assert data["name"] == input.name
