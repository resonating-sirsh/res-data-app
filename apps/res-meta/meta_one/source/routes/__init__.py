from fastapi import FastAPI

from .bertha.routes import router as bertha_router
from .body_request_annotations.routes import (
    router as body_request_annotations_router,
)
from .body_request_assets.routes import router as body_request_assets_router
from .body_requests.routes import router as body_requests_router
from .design.routes import router as design_router
from .observability.routes import router as observability_router
from .trims.routes import router as trims_router
from .label_styles.routes import router as label_styles_router

PROCESS_NAME = "meta-one"


def set_meta_one_routes(app: FastAPI):
    app.include_router(bertha_router, prefix=f"/{PROCESS_NAME}", tags=["Bertha"])
    app.include_router(
        body_requests_router,
        prefix=f"/{PROCESS_NAME}/body-requests",
        tags=["Body Requests"],
    )
    app.include_router(
        body_request_assets_router,
        prefix=f"/{PROCESS_NAME}/body-request-assets",
        tags=[
            "Body Requests",
            "Body Request Assets",
        ],
    )

    app.include_router(
        body_request_annotations_router,
        prefix=f"/{PROCESS_NAME}/body-request-asset-annotations",
        tags=[
            "Body Requests",
            "Body Request Asset Annotations",
        ],
    )

    app.include_router(design_router, prefix=f"/{PROCESS_NAME}", tags=["Design"])

    app.include_router(
        observability_router, prefix=f"/{PROCESS_NAME}", tags=["Observability Tools"]
    )

    app.include_router(trims_router, prefix=f"/{PROCESS_NAME}/trims", tags=["Trims"])
    app.include_router(
        label_styles_router,
        prefix=f"/{PROCESS_NAME}/label-styles",
        tags=["Label Styles"],
    )
