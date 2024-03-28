from typing import List
from fastapi_utils.api_model import APIModel


class BrandWebhook(APIModel):
    brand_code: str
    topics: List[str]
