from uuid import UUID, uuid4

import pytest
from res.connectors.postgres.PostgresConnector import connect_to_postgres
from schemas.pydantic.body_requests.body_request_asset import BodyRequestAssetType
from schemas.pydantic.body_requests.body_request_asset_annotations import (
    BodyRequestAssetAnnotationSupplierType,
    BodyRequestAssetAnnotationType,
)
from schemas.pydantic.body_requests.body_requests import (
    BodyRequestStatus,
    BodyRequestType,
)
from source.routes.body_request_annotations.controllers import (
    BodyRequestAnnotation,
    BodyRequestAnnotationPostgresController,
    BodyRequestAnnotationController,
)

from source.routes.label_styles.controller import LabelStyleAirtableConnector

from source.routes.body_request_assets.controllers import (
    BodyRequestAsset,
    BodyRequestAssetPostgresController,
)
from source.routes.body_request_assets.models import BodyRequestAssetInput

from source.routes.body_requests.controllers import (
    BodyRequestPostgresController,
)

from source.routes.body_requests.models import CreateBodyRequestInput
from source.routes.context import BodyRequestRouteContext


@pytest.mark.skip(reason="Not run CI")
class TestBodyRequestAssetNode:
    controller: BodyRequestAnnotationController
    label_controller: LabelStyleAirtableConnector

    asset_record: BodyRequestAsset

    @pytest.fixture(autouse=True)
    def before_and_after_each(self):
        with connect_to_postgres(
            conn_string="postgres://postgres:postgrespassword@localhost:5432/postgres"
        ) as pg_conn:
            bbr_controller = BodyRequestPostgresController(pg_conn=pg_conn)
            asset_controller = BodyRequestAssetPostgresController(pg_conn=pg_conn)
            self.controller = BodyRequestAnnotationPostgresController(
                pg_conn=pg_conn,
            )

            self.label_controller = LabelStyleAirtableConnector()

            context = BodyRequestRouteContext(
                asset_controller=asset_controller,
                bbr_controller=bbr_controller,
                annotation_controller=self.controller,
                label_style_controller=self.label_controller,
            )

            asset_controller.set_context(context)
            bbr_controller.set_context(context)
            self.controller.set_context(context)

            bbr_input = CreateBodyRequestInput(
                name="test",
                request_type=BodyRequestType.CREATE_FROM_SCRATCH,
                brand_code="TT",
                status=BodyRequestStatus.DRAFT,
            )

            bbr_record = bbr_controller.create_brand_body_requests(input=bbr_input)

            asset_input = BodyRequestAssetInput(
                name="test",
                uri="s3://test/test,jpg",
                type=BodyRequestAssetType.ASSET_2D,
                body_request_id=UUID(bbr_record.id),
            )

            self.asset_record = asset_controller.create_asset(input=asset_input)

            yield

    def test_create_annotation(self):
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
            bill_of_material_id="test",
            supplier_type=BodyRequestAssetAnnotationSupplierType.BRAND,
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        assert annotation_record.asset_id == annotation.asset_id
        assert annotation_record.value == annotation.value
        assert annotation_record.supplier_type == annotation.supplier_type
        assert annotation_record.bill_of_material_id == annotation.bill_of_material_id

    def test_find_by_id_annotation(self):
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        annotation_found = self.controller.find_by_id(id=UUID(annotation_record.id))

        if annotation_found is None:
            raise Exception("Annotation not found")

        assert annotation_record.id == annotation_found.id
        assert annotation_record.value == annotation_found.value

    def test_save_annotation_foreign_key(self):
        labels = self.label_controller.find()
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
            label_style_id=labels[0].id if labels else None,
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        annotation_found = self.controller.find_by_id(id=UUID(annotation_record.id))

        if annotation_found is None:
            raise Exception("Annotation not found")

        assert annotation_record.id == annotation_found.id
        assert annotation_record.value == annotation_found.value

    def test_update_annotation(self):
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        annotation_record = BodyRequestAnnotation(
            **annotation_record.dict(exclude={"value"}),
            value="Test2",
        )

        annotation_record = self.controller.save(
            record=annotation_record,
        )

        annotation_found = self.controller.find_by_id(id=UUID(annotation_record.id))

        if annotation_found is None:
            raise Exception("Annotation not found")

        assert annotation_record.value == "Test2"
        assert annotation_found.value == "Test2"
        assert annotation_record.id == annotation_found.id

    def test_find_annotations(self):
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        annotations_found = self.controller.find(
            asset_id=annotation_record.asset_id,
            limit=5,
        )

        assert len(annotations_found) <= 5

        # annotations_found = self.controller.find(
        #     ids=[UUID(annotation_record.id)],
        #     limit=5,
        # )

        # assert len(annotations_found) == 1
        assert annotations_found[0].id == annotation_record.id

        annotations_found = self.controller.find(
            annotation_type=annotation_record.annotation_type,
            limit=5,
        )

        assert len(annotations_found) <= 5

        for annotation_found in annotations_found:
            assert annotation_found.annotation_type == annotation_record.annotation_type

        annotations_found = self.controller.find()

        assert len(annotations_found) <= 100

    def test_delete_annotation(self):
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        self.controller.delete(ids=[UUID(annotation_record.id)])

        annotation_found = self.controller.find_by_id(id=UUID(annotation_record.id))

        assert annotation_found is None

        self.controller.delete(ids=[UUID(annotation_record.id)], hard=True)

        annotation_found = self.controller.find_by_id(id=UUID(annotation_record.id))

        assert annotation_found is None

    def test_delete_annotation_already_deleted(self):
        annotation = BodyRequestAnnotation(
            id=str(uuid4()),
            name="test",
            asset_id=UUID(self.asset_record.id),
            coordinate_x=0.1,
            coordinate_y=0.2,
            coordinate_z=0.3,
            annotation_type=BodyRequestAssetAnnotationType.MEASUREMENT_VALUE,
            value="Test",
        )

        annotation_record = self.controller.save(
            record=annotation,
        )

        self.controller.delete(ids=[UUID(annotation_record.id)], hard=True)
        self.controller.delete(ids=[UUID(annotation_record.id)], hard=True)

        annotation_found = self.controller.find_by_id(id=UUID(annotation_record.id))
        assert annotation_found is None
