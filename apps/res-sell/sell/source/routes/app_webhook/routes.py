from fastapi import APIRouter, Body, Depends, Security, status
from fastapi_utils.cbv import cbv
from pymongo.database import Database
from starlette.status import HTTP_200_OK
from res.utils import logger
from source.utils.dependecies import get_mongo_connector
from source.utils.security import validate_shopify_hmac_header

router = APIRouter(dependencies=[Security(validate_shopify_hmac_header)])


@cbv(router)
class ShopifyAppWebhooks:
    """
    This endpoint are related to the Shopify Mandatory Webhook for Apps
    [Link]: https://shopify.dev/docs/apps/webhooks/configuration/mandatory-webhooks
    """

    db: Database = Depends(get_mongo_connector)

    @router.post("/customers/redact", status_code=status.HTTP_200_OK)
    def redact_customer(self, event=Body()):
        logger.info(
            "Receive Message customer/redact from Shopify", extra={"raw": event}
        )
        self.db.shopifyAppEvents.insert_one(
            {
                "topic": "customer/redact",
                "event": event,
            }
        )

    @router.post("/customers/data_request", status_code=status.HTTP_200_OK)
    def request_customer_data(self, event=Body()):
        logger.info("Receive Message customers/data_request", extra={"raw": event})
        self.db.shopifyAppEvents.insert_one(
            {
                "topic": "customers/data_request",
                "event": event,
            }
        )

    @router.post("/shop/redact", status_code=HTTP_200_OK)
    def redact_shop_data(self, event=Body()):
        logger.info("Receive Message shop/redact", extra={"raw": event})
        self.db.shopifyAppEvents.insert_one(
            {
                "topic": "shop/redact",
                "event": event,
            }
        )
