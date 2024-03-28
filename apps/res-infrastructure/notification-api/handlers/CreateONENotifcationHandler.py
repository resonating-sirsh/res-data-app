from schemas.pydantic.notification import NotificationMessage

from query import INSERT_MESSAGE
from res.connectors.graphql.hasura import Client
from res.utils import logger

from . import (
    NotificationHandler,
    NotificationRequest,
    NotificationMetadata,
    BrandInNotification,
)


class CreateONENotificationHanderl(NotificationHandler):
    """
    Handler for Create One notifications
    """

    def __init__(self, hasura_client: Client):
        self.hasura_client = hasura_client

    def is_procesable(self, notification_template: NotificationMetadata) -> bool:
        return "Create One" in notification_template.channels

    def handle(
        self,
        notification_template: NotificationMetadata,
        notification_request: NotificationRequest,
        message: str,
        title: str,
        action_link: str,
        brand: BrandInNotification,
    ) -> bool:
        logger.info(f"Handling Create One notification")
        logger.debug(f"Notification template: {notification_template}")
        logger.debug(f"Notification request: {notification_request}")
        payload = NotificationMessage(
            destination=brand.code,
            message=message,
            source_table_id=notification_template.table_id or "",
            topic=title,
            channels=notification_template.channels,
            subchannel=notification_template.sub_space or "",
            source_record_id=None,
            should_send=True,
            read=False,
            received=False,
            links=action_link,
            notification_id=notification_request.notification_id,
            payload=notification_request.data,
        )

        self.hasura_client.execute(
            INSERT_MESSAGE,
            payload.dict(),
        )
        return True
