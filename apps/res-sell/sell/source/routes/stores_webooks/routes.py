import re

from fastapi import APIRouter, Depends, Path, Security
from fastapi_utils.cbv import cbv
from pyactiveresource.connection import ClientError
from pymongo.database import Database
from shopify.collection import PaginatedCollection
from shopify.resources import Webhook
from starlette import status

from res.connectors.shopify.ShopifyConnector import Shopify
from res.utils import logger

from source.utils.dependecies import get_mongo_connector
from source.utils.security import get_api_key
from .constant import TOPICS, WEBHOOK_API_URL
from .models import BrandWebhook

router = APIRouter(dependencies=[Security(get_api_key)])


@cbv(router)
class Routes:
    db: Database = Depends(get_mongo_connector)

    @router.get(
        "/{brand_code}",
        status_code=status.HTTP_200_OK,
        response_model=BrandWebhook,
    )
    def get_brand_webhooks(self, brand_code: str = Path()):
        brand_webhooks = self.db.brandShopifyWebhooks.find_one(
            {"brandCode": brand_code}
        )

        with Shopify(brand_id=brand_code):
            webhooks = [webhook.address for webhook in Webhook.find()]  # type: ignore
            logger.info(",".join(webhooks))

        if not brand_webhooks:
            return BrandWebhook(brand_code=brand_code, topics=[])

        return BrandWebhook(brand_code=brand_code, topics=brand_webhooks["topics"])

    @router.post("/{brand_code}", status_code=status.HTTP_201_CREATED)
    def create_brand_webhooks(self, brand_code: str = Path()):
        logger.info("Creating webhooks for brand", brand_code=brand_code)
        with Shopify(brand_id=brand_code):
            for topic in TOPICS:
                try:
                    logger.info(
                        f"Creating webhook for topic {topic.topic}, route {topic.route}, brand {brand_code}"
                    )
                    webhook = Webhook.create(
                        {
                            "topic": topic.topic,
                            "address": topic.route + f"?brand_code={brand_code}",
                            "format": "json",
                        }
                    )
                    webhook.save()

                    if not self.db.brandShopifyWebhooks.find_one(
                        {"brandCode": brand_code}
                    ):
                        self.db.brandShopifyWebhooks.insert_one(
                            {
                                "brandCode": brand_code,
                                "topics": [topic.topic for topic in TOPICS],
                            }
                        )
                    else:
                        self.db.brandShopifyWebhooks.update_one(
                            {"brandCode": brand_code},
                            {"$set": {"topics": [topic.topic for topic in TOPICS]}},
                        )
                except ClientError as err:
                    if err.response.code == status.HTTP_422_UNPROCESSABLE_ENTITY:
                        logger.error("Error creating webhook", exception=err)
                        continue

            return BrandWebhook(
                brand_code=brand_code, topics=[topic.topic for topic in TOPICS]
            )

    @router.delete("/{brand_code}", status_code=status.HTTP_200_OK, response_model=bool)
    def delete_brand_webhooks(self, brand_code: str = Path()):
        logger.info(f"Deleting webhooks for brand {brand_code}")
        if not self.db.brandShopifyWebhooks.find_one({"brandCode": brand_code}):
            return False

        with Shopify(brand_id=brand_code):
            regex = re.compile(f"{WEBHOOK_API_URL}/.*")
            webhooks = Webhook.find()

            if type(webhooks) == PaginatedCollection:
                for webhook in webhooks:
                    if webhook.topic in [
                        topic.topic for topic in TOPICS
                    ] and regex.match(webhook.address):
                        logger.info(
                            f"Destroying webhook for topic {webhook.topic} and address {webhook.address} for {brand_code}"
                        )
                        webhook.destroy()
            self.db.brandShopifyWebhooks.delete_one({"brandCode": brand_code})
            return True
