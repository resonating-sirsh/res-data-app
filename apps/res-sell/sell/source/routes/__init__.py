from fastapi import APIRouter, FastAPI
from source.routes.stores_webooks.routes import router as webhooks_router
from source.routes.app_webhook.routes import router as app_webhook_routers


def add_routes(app: FastAPI):
    api_router = APIRouter(
        prefix="/sell",
    )
    api_router.include_router(
        webhooks_router,
        prefix="/webhooks",
        tags=[
            "Store",
            "Webhook",
            "Shopify",
        ],
    )
    api_router.include_router(
        app_webhook_routers,
        prefix="/shopify/webhooks/app",
        tags=[
            "Webhook",
            "Shopify",
            "App",
        ],
    )

    app.include_router(api_router)
