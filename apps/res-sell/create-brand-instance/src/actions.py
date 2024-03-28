import arrow
import os

from res.utils import logger
from src.query_definitions import (
    CREATE_BRAND,
    CREATE_USER,
    CREATE_SHIPPING_ADDRESS,
    CREATE_DATASOURCE_INSTANCE,
    CREATE_STYLE,
    CREATE_COMPOSITION,
    UPDATE_BRAND,
    UPDATE_BRAND_LABEL,
    SYNC_BRAND_WITH_META,
)

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient


class CreateBrandAction:
    def __init__(self, input) -> None:
        self.client = ResGraphQLClient()
        self.input = input
        self.brand = None
        self.user = None
        self.brandAddress = None
        self.brandSpace = None
        self.style = None

    def create_brand_record(self):
        logger.info("Creating brand record")
        response = self.client.query(CREATE_BRAND, {"input": self.input["brand"]})
        logger.info(response)
        self.brand = response["data"]["createBrand"]["brand"]

    def sync_brand_with_meta(self):
        logger.info("Syncing brand with res.Meta")
        response = self.client.query(SYNC_BRAND_WITH_META, {"id": self.brand["id"]})
        logger.info(response)

    def create_user_record(self):
        logger.info("Creating Brand User Record")
        STATUS = "Active"
        response = self.client.query(
            CREATE_USER,
            {
                "email": self.input["brand"]["contactEmail"],
                "input": {**self.input["user"], "brandIds": [self.brand["id"]]},
                "layerId": os.getenv("BRAND_LAYER_ID"),
                "status": STATUS,
            },
        )
        logger.info(response)
        self.user = response["data"]["addUserToBrand"]["user"]

    def create_brand_address(self):
        logger.info("Creating Shipping Address")
        response = self.client.query(
            CREATE_SHIPPING_ADDRESS,
            {
                "input": {
                    **self.input["brandAddress"],
                    "brand": [self.brand["id"]],
                    "brandCode": self.brand["code"],
                }
            },
        )
        logger.info(response)
        self.brandAddress = response["data"]["createShippingAddress"]["shippingAddress"]

    def create_shopify_data_source_instance(self):
        logger.info("Creating Shopify DataSourceInstance")
        response = self.client.query(
            CREATE_DATASOURCE_INSTANCE,
            {
                "input": {
                    "instanceId": self.brand["id"],
                    "dataSourceId": os.getenv("SHOPIFY_DATASOURCE_ID"),
                }
            },
        )
        logger.info(response)

    def create_brand_space(self):
        instance_id = self.brand["id"]
        logger.info("Creating Brand Space")
        response = self.client.query(
            CREATE_COMPOSITION,
            {
                "input": {
                    "isFirst": True,
                    "entityId": instance_id,
                    "name": "home",
                    "entityType": "brand",
                    "brandId": instance_id,
                },
            },
        )
        self.brandSpace = response["data"]["createComposition"]["composition"]
        logger.info(response)
        logger.info("Updating Brand: Set the brandSpace under the brand")
        response = self.client.query(
            UPDATE_BRAND,
            {
                "id": self.brand["id"],
                "input": {
                    "brandSpaceId": self.brandSpace["id"],
                },
            },
        )
        logger.info(response)

    def create_style_space(self):
        brand_id = self.brand["id"]
        instance_id = self.style["id"]
        logger.info("Creating Style Space")
        response = self.client.query(
            CREATE_COMPOSITION,
            {
                "input": {
                    "isFirst": True,
                    "name": "My First Style",
                    "brandId": brand_id,
                    "entityType": "style",
                    "startDate": str(arrow.now("US/Eastern").isoformat()),
                    "canvasPosition": [5, 7, 14, 18],
                    "parentLink": self.brandSpace["id"],
                    "entityId": instance_id,
                },
            },
        )
        self.brandSpace = response["data"]["createComposition"]["composition"]
        logger.info(response)

    def create_style(self):
        logger.info("Creating first style")
        response = self.client.query(
            CREATE_STYLE,
            {
                "input": {
                    "approvedForChannels": ["ECOM"],
                    "brandCode": self.brand["code"],
                }
            },
        )
        logger.info(response)
        self.style = response["data"]["createStyle"]["style"]

    def update_brand_label(self):
        logger.info("Updating brand label...")
        response = self.client.query(
            UPDATE_BRAND_LABEL,
            {
                "code": self.brand["code"],
                "input": {
                    "sizeLabel": "White with Black Lettering",
                    "mainLabelType": "Main Label With Size",
                    "isMainLabelWithSize": True,
                },
            },
        )
        logger.debug(response)
