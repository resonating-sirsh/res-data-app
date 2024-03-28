from uuid import UUID
import pytest

from res.connectors.postgres.PostgresConnector import (
    PostgresConnector,
    connect_to_postgres,
)
from schemas.pydantic.body_requests.body_request_asset import BodyRequestAssetType
from schemas.pydantic.body_requests.body_requests import (
    BodyRequest,
    BodyRequestStatus,
    BodyRequestType,
)
from source.routes.body_request_assets.controllers import (
    BodyRequestAssetController,
    BodyRequestAssetPostgresController,
)

from source.routes.body_requests.controllers import (
    BodyRequestPostgresController,
)
from source.routes.body_requests.models import (
    BodyRequestAssetInput,
    CreateBodyRequestInput,
    UpsertBodyRequestAsset,
)
from source.routes.context import BodyRequestRouteContext
from ..fixtures.mock_label_style_controller import MockLabelStyleController
from ..fixtures.mock_body_request_asset_annotations import (
    MockBodyRequestAssetAnnotationController,
)


@pytest.mark.skip(reason="Having issue running this Test CI")
class TestBodyRequestAssetController:
    pg_conn: PostgresConnector
    bbr_record: BodyRequest
    asset_controller: BodyRequestAssetController

    @pytest.fixture(autouse=True)
    def before_and_after_each_test(self):
        with connect_to_postgres(
            conn_string="postgres://postgres:postgrespassword@localhost:5432/postgres"
        ) as pg_conn:
            self.pg_conn = pg_conn
            self.asset_controller = BodyRequestAssetPostgresController(
                pg_conn=self.pg_conn
            )
            body_request_controller = BodyRequestPostgresController(
                pg_conn=self.pg_conn,
            )
            context = BodyRequestRouteContext(
                asset_controller=self.asset_controller,
                bbr_controller=body_request_controller,
                annotation_controller=MockBodyRequestAssetAnnotationController(),
                label_style_controller=MockLabelStyleController(),
            )
            body_request_controller.set_context(context=context)
            self.asset_controller.set_context(context=context)

            input = CreateBodyRequestInput(
                name="Test Brand Body Request",
                brand_code="TT",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                status=BodyRequestStatus.DRAFT,
            )

            self.bbr_record = body_request_controller.create_brand_body_requests(
                input=input
            )

            yield

            body_request_controller.delete_brand_body_request(
                id=UUID(self.bbr_record.id),
                permanently=True,
            )

    def test_create_a_body_request_asset(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
            body_request_id=UUID(self.bbr_record.id),
        )

        record = self.asset_controller.create_asset(
            input=input,
        )

        response = self.asset_controller.get_asset(
            id=UUID(record.id),
        )

        assert response.name == input.name
        assert response.uri == input.uri
        assert response.type == BodyRequestAssetType.ASSET_2D

    def test_create_body_request_assets(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
            body_request_id=UUID(self.bbr_record.id),
        )

        self.asset_controller.create_assets(
            body_request_id=self.bbr_record.id,
            asset_type=BodyRequestAssetType.ASSET_2D,
            assets=[input],
        )

        assets = self.asset_controller.get_assets(
            body_request_id=self.bbr_record.id,
        )

        assert len(assets) == 1
        assert assets[0].name == input.name
        assert assets[0].uri == input.uri
        assert assets[0].type == BodyRequestAssetType.ASSET_2D

    def test_update_body_request_asset(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
        )

        self.asset_controller.create_assets(
            body_request_id=self.bbr_record.id,
            asset_type=BodyRequestAssetType.ASSET_2D,
            assets=[input],
        )
        assets = self.asset_controller.get_assets(
            body_request_id=self.bbr_record.id,
        )

        update_input = UpsertBodyRequestAsset(
            id=UUID(assets[0].id),
            name="Test Asset Updated",
            uri="s3://bucket-name/folder-name/asset-name-updated",
        )

        self.asset_controller.update_assets(
            assets=[update_input],
        )

        assets = self.asset_controller.get_assets(
            body_request_id=self.bbr_record.id,
        )

        assert len(assets) == 1
        assert assets[0].name == update_input.name
        assert assets[0].uri == update_input.uri
        assert assets[0].type == BodyRequestAssetType.ASSET_2D

    def test_delete_body_request_asset(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
        )

        self.asset_controller.create_assets(
            body_request_id=self.bbr_record.id,
            asset_type=BodyRequestAssetType.ASSET_2D,
            assets=[input],
        )
        assets = self.asset_controller.get_assets(
            body_request_id=self.bbr_record.id,
        )

        self.asset_controller.delete_assets(
            ids=[UUID(assets[0].id)],
            permanently=True,
        )

        assets = self.asset_controller.get_assets(
            body_request_id=self.bbr_record.id,
        )

        assert len(assets) == 0

    def test_get_body_requests_asset(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
            body_request_id=UUID(self.bbr_record.id),
        )

        record = self.asset_controller.create_asset(
            input=input,
        )

        asset = self.asset_controller.get_asset(
            id=UUID(record.id),
        )

        assert asset.name == input.name
        assert asset.uri == input.uri
        assert asset.type == BodyRequestAssetType.ASSET_2D

    def test_get_body_request_assets_by_body_request_id(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
        )

        self.asset_controller.create_assets(
            body_request_id=self.bbr_record.id,
            asset_type=BodyRequestAssetType.ASSET_2D,
            assets=[input],
        )

        assets = self.asset_controller.get_assets(
            body_request_id=self.bbr_record.id,
        )

        assert len(assets) == 1
        assert assets[0].name == input.name
        assert assets[0].uri == input.uri
        assert assets[0].type == BodyRequestAssetType.ASSET_2D

    def test_get_body_requests_assets_by_ids(self):
        input = BodyRequestAssetInput(
            name="Test Asset",
            uri="s3://bucket-name/folder-name/asset-name",
            type=BodyRequestAssetType.ASSET_2D,
            body_request_id=UUID(self.bbr_record.id),
        )

        record = self.asset_controller.create_asset(
            input=input,
        )

        assets = self.asset_controller.get_assets(ids=[record.id])

        assert len(assets) == 1
        assert assets[0].name == input.name
        assert assets[0].uri == input.uri
        assert assets[0].type == BodyRequestAssetType.ASSET_2D
