from typing import Optional
from pydantic import BaseModel
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger


class BrandInNotification(BaseModel):
    id: str
    code: str
    name: str
    contact_email: str
    slack_channel: Optional[str]


class BrandController:
    def __init__(self):
        self.graphql_client = ResGraphQLClient()

    def get_by_brand_code(self, brand_code: str) -> Optional[BrandInNotification]:
        logger.debug("Requesting brand", extra={"brand_code": brand_code})
        brand_query_response = self.graphql_client.query(
            """
            query brandByCode($code: String) {
              brand(code: $code) {
                id
                code
                name
                contactEmail
                slackChannel
              }
            }
            """,
            {"code": brand_code},
        )

        logger.debug(
            f"Getting response from the graphql api",
            extra=brand_query_response,
        )

        brand = brand_query_response.get("data", {}).get("brand", {})
        return BrandInNotification(
            id=brand.get("id", ""),
            code=brand.get("code", ""),
            name=brand.get("name", ""),
            contact_email=brand.get("contactEmail", ""),
            slack_channel=brand.get("slackChannel"),
        )
