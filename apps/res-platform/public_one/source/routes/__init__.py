from fastapi import FastAPI

from .design.routes import router as design_router
from .sell.routes import router as sell_router
from .make.routes import router as make_router


PROCESS_NAME = "public-one"


def set_meta_one_routes(app: FastAPI):

    app.include_router(design_router, prefix=f"/{PROCESS_NAME}", tags=["Design"])
    app.include_router(sell_router, prefix=f"/{PROCESS_NAME}", tags=["Sell"])
    app.include_router(make_router, prefix=f"/{PROCESS_NAME}", tags=["Make"])
