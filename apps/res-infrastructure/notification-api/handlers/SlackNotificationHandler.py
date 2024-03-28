import traceback

from schemas.pydantic.notification import SlackNotificationPayload

from . import (
    NotificationHandler,
    NotificationRequest,
    NotificationMetadata,
    BrandInNotification,
)
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger


class SlackNotificationHandler(NotificationHandler):
    """
    Handler for Slack notifications
    """

    graphql_client: ResGraphQLClient

    def __init__(
        self,
        graphql_client: ResGraphQLClient,
        slack_connector,
    ):
        self.graphql_client = graphql_client
        self.slack = slack_connector

    def is_procesable(self, notification_template: NotificationMetadata) -> bool:
        return "Slack" in notification_template.channels

    def handle(
        self,
        notification_template: NotificationMetadata,
        notification_request: NotificationRequest,
        message: str,
        title: str,
        action_link: str,
        brand: BrandInNotification,
    ) -> bool:
        logger.info(f"Handling Slack notification {title}")
        logger.debug(f"Notification template: {notification_template}")
        logger.debug(f"Notification Reuqest: {notification_request}")

        try:
            if not brand.slack_channel:
                raise Exception("Brand does not include slack_channel")
            channels = [brand.slack_channel]
            gen_message = f"*{message}*"
            if action_link != "":
                gen_message += f"\n 👉 `<{action_link}|REVIEW>`"

            slack_message = SlackNotificationPayload(
                slack_channels=channels,
                message=gen_message,
                attachments=[],
            )
            logger.info("Starting to process slack notifications.")
            self.slack(slack_message.dict())
            return True
        except Exception as e:
            err = "Error notifying in Slack Channel: {}".format(traceback.format_exc())
            logger.critical(
                err,
                exception=e,
            )
            return False
