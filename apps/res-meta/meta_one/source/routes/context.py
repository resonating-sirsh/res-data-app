from res.connectors.postgres.PostgresConnector import (
    connect_to_postgres,
)
from res.utils import logger

from .body_requests.controllers import (
    BodyRequestController,
    BodyRequestPostgresController,
)
from .body_request_assets.controllers import (
    BodyRequestAssetController,
    BodyRequestAssetPostgresController,
)
from .body_request_annotations.controllers import (
    BodyRequestAnnotationController,
    BodyRequestAnnotationPostgresController,
)

from .label_styles.controller import LabelStyleController, LabelStyleAirtableConnector


class BodyRequestRouteContext:
    """
    Context for the body request route
    """

    def __init__(
        self,
        asset_controller: BodyRequestAssetController,
        bbr_controller: BodyRequestController,
        annotation_controller: BodyRequestAnnotationController,
        label_style_controller: LabelStyleController,
    ):
        self.asset_controller = asset_controller
        self.bbr_controller = bbr_controller
        self.annotation_controller = annotation_controller
        self.label_style_controller = label_style_controller


def get_body_request_context():
    logger.info("Connecting to progess database...")
    with connect_to_postgres(
        # conn_string="postgres://postgres:postgrespassword@localhost:5432/postgres",
    ) as pg_conn:
        logger.info("Connected to postgres database")
        asset_controller = BodyRequestAssetPostgresController(
            pg_conn=pg_conn,
        )
        controller = BodyRequestPostgresController(
            pg_conn=pg_conn,
        )
        asset_node_controller = BodyRequestAnnotationPostgresController(
            pg_conn=pg_conn,
        )
        label_controller = LabelStyleAirtableConnector()

        context = BodyRequestRouteContext(
            asset_controller=asset_controller,
            bbr_controller=controller,
            annotation_controller=asset_node_controller,
            label_style_controller=label_controller,
        )

        controller.set_context(context)
        asset_controller.set_context(context)
        asset_node_controller.set_context(context)

        yield context
