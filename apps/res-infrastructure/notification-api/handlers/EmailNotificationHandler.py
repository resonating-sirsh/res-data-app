from pydantic import EmailStr
from controllers.email import _EmailController, EmailAddress
from handlers import (
    NotificationHandler,
    NotificationRequest,
    NotificationMetadata,
    BrandInNotification,
)
from res.utils import logger
from res.utils.strings import is_html


class EmailNotificationHandler(NotificationHandler):
    def __init__(self, controller: _EmailController):
        self.controller = controller

    def is_procesable(self, notification_template: NotificationMetadata) -> bool:
        return "Email" in notification_template.channels

    def handle(
        self,
        notification_template: NotificationMetadata,
        notification_request: NotificationRequest,
        message: str,
        title: str,
        action_link: str,
        brand: BrandInNotification,
    ) -> bool:
        logger.info(f"Handling email notification {title}")
        logger.debug(
            f"Arguments",
            extra={
                "notification_template": notification_template,
                "notification_request": notification_request,
                "message": message,
                "title": title,
                "action_link": action_link,
                "brand": brand,
            },
        )

        from_email = EmailAddress(
            name="Resonance",
            address="techpirates@resonance.nyc",
        )

        if notification_request.data.get("sourceName"):
            from_email.name = notification_request.data["sourceName"]

        if notification_request.data.get("sourceEmail"):
            from_email.address = notification_request.data["sourceEmail"]

        to_emails = []

        if notification_request.data.get("targetEmails"):
            to_emails = notification_request.data["targetEmails"]
        else:
            to_emails = [EmailStr(brand.contact_email)]

        self.controller.send(
            from_email=from_email,
            to_emails=to_emails,
            subject=title,
            content=message,
            html_content=bool(
                is_html(message) or notification_template.template_id is not None
            ),
            template_id=notification_template.template_id,
            data=notification_request.data,
        )

        return True
