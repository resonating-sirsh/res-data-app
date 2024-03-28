from typing_extensions import Optional
from schemas.pydantic.notification_metadata import NotificationMetadata

from res.connectors.airtable.AirtableClient import ResAirtableClient
from res.utils import logger

BASE_ID = "appEm3sHF7BTTzgyq"
TABLE_ID = "tbltRVfgzkzYNvf3q"


class NotificationMetadataController:
    def __init__(self):
        self.airtable_client = ResAirtableClient()
        self.table_id = "tbltRVfgzkzYNvf3q"
        self.base_id = "appEm3sHF7BTTzgyq"

    def get_by_id(self, id: str) -> Optional[NotificationMetadata]:
        logger.info(f"Getting notification metadata with id: {id}")

        record = self.airtable_client.get_record(
            base_id=self.base_id, table_id=self.table_id, record_id=id
        )

        logger.info("Notification metadata record", extra={"record": record})

        if "error" in record:
            return None

        response = NotificationMetadata(
            id=record["id"],
            name=record["fields"].get("Name", ""),
            topic=record["fields"].get("Topic", ""),
            message_template=record["fields"].get("Message", ""),
            destination_layer=record["fields"].get("Destination Layer"),
            channels=record["fields"]["Channels"],
            space=record["fields"].get("Space"),
            sub_space=record["fields"].get("Sub Space"),
            action_link=record["fields"].get("Action URL"),
            table_id=record["fields"].get("Table ID"),
            template_id=record["fields"].get("Template ID"),
        )

        logger.info(
            f"Notification metadata parsed: {response}",
            extra={"response": response},
        )

        return response
