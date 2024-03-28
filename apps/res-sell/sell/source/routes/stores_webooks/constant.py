from pydantic import BaseModel
from os import getenv


class _Topic(BaseModel):
    topic: str
    route: str


# WEBHOOK_API_URL = "https://data.resmagic.io/shopify-webhooks"
WEBHOOK_API_URL = (
    "https://data.resmagic.io/shopify-webhooks"
    if getenv("RES_ENV") == "production"
    else "https://datadev.resmagic.io/shopify-webhooks"
)

TOPICS = [
    _Topic(topic="orders/create", route=WEBHOOK_API_URL + "/create-order"),
    _Topic(topic="orders/update", route=WEBHOOK_API_URL + "/update-order"),
    _Topic(topic="orders/cancel", route=WEBHOOK_API_URL + "/cancel-order"),
    _Topic(topic="products/create", route=WEBHOOK_API_URL + "/create-product"),
    _Topic(topic="products/update", route=WEBHOOK_API_URL + "/update-product"),
    _Topic(topic="products/delete", route=WEBHOOK_API_URL + "/delete-product"),
]
