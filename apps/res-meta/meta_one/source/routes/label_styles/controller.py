from abc import ABC, abstractmethod
from typing import List
from res.utils import secrets_client

from airtable import Airtable

from ..base import Controller
from schemas.pydantic.body_requests.label_styles import LabelStyle


class LabelStyleController(Controller, ABC):
    @abstractmethod
    def find(
        self,
        limit: int = 100,
    ) -> List[LabelStyle]:
        ...


class LabelStyleAirtableConnector(LabelStyleController):
    def __init__(self):
        secret = secrets_client.get_secret("AIRTABLE_API_KEY")
        if not type(secret) == str:
            raise Exception("AIRTABLE_API_KEY not found in environment variables")

        self.airtable_table = Airtable(
            api_key=secret, base_id="appa7Sw0ML47cA8D1"
        ).table(table_name="Label Options")

    def find(self, limit: int = 100) -> List[LabelStyle]:
        response = self.airtable_table.get(
            limit=limit, filter_by_formula="displayable=1"
        )
        if not "records" in response:
            return []

        if not response["records"]:
            return []

        records = response["records"]

        response_list = [
            LabelStyle(
                id=record["id"],
                name=record["fields"]["name"],
                dimensions=record["fields"]["dimensions"],
                thumbnail_url=record["fields"].get("thumbnail", [{}])[0].get("url", ""),
                body_code=record["fields"].get("bodyCode", ""),
            )
            for record in records
        ]
        return response_list
