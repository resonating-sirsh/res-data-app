import pytest

from schemas.pydantic.notification_metadata import (
    NotificationChannel,
    NotificationMetadata,
)

from ..fixtures.controllers.email import FakeEmailController

from handlers.EmailNotificationHandler import EmailNotificationHandler

from schemas.pydantic.notification import NotificationRequest
from controllers.brand import BrandInNotification


test_brand = BrandInNotification(
    id="test",
    code="test",
    name="test",
    contact_email="test",
    slack_channel="test",
)


class TestEmailController:
    @pytest.fixture(autouse=True)
    def before_each(self):
        self.fake_controller = FakeEmailController()
        self.email_handler = EmailNotificationHandler(self.fake_controller)

        yield

    def test_is_procesable(self):
        notification_template = NotificationMetadata(
            id="rec1",
            name="test",
            topic="test",
            channels=[NotificationChannel.EMAIL],
            table_id="test",
            message_template="test",
            action_link="test",
            destination_layer="test",
            space="test",
            sub_space="test",
            template_id="test",
        )
        assert self.email_handler.is_procesable(notification_template) == True

    def test_is_not_procesable(self):
        notification_template = NotificationMetadata(
            id="rec1",
            name="test",
            topic="test",
            channels=[],
            table_id="test",
            message_template="test",
            action_link="test",
            destination_layer="test",
            space="test",
            sub_space="test",
            template_id="test",
        )
        assert self.email_handler.is_procesable(notification_template) == False

    def test_handle(self):
        request = NotificationRequest(
            notificationId="rec1",
            brandCode="test",
            data={
                "sourceName": "test",
                "sourceEmail": "brand@test.com",
                "targetEmails": ["a@test.com", "b@test.com", "b@test.com"],
            },
        )

        notification_template = NotificationMetadata(
            id="rec1",
            name="test",
            topic="test",
            channels=[NotificationChannel.EMAIL],
            table_id="test",
            message_template="test",
            action_link="test",
            destination_layer="test",
            space="test",
            sub_space="test",
            template_id="test",
        )

        message = "test test"

        title = "test, test"

        action_link = "test"

        self.email_handler.handle(
            notification_template=notification_template,
            notification_request=request,
            message=message,
            title=title,
            action_link=action_link,
            brand=test_brand,
        )

        assert self.fake_controller.from_email.name == request.data["sourceName"]
        assert self.fake_controller.from_email.address == request.data["sourceEmail"]
        assert self.fake_controller.to_emails == request.data["targetEmails"]
        assert self.fake_controller.subject == title
        assert self.fake_controller.content == message
