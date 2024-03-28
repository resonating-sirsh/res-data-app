import pytest

from pytest_mock import MockerFixture
from query import INSERT_MESSAGE

from controllers.brand import BrandInNotification
from handlers.CreateONENotifcationHandler import (
    CreateONENotificationHanderl,
    NotificationRequest,
    Client,
)
from schemas.pydantic.notification import NotificationMessage
from schemas.pydantic.notification_metadata import (
    NotificationChannel,
    NotificationMetadata,
)

test_brand = BrandInNotification(
    id="test",
    code="test",
    name="test",
    contact_email="test",
    slack_channel="test",
)


class TestCreateONENotificationHandler:
    hasura_client: Client

    @pytest.fixture(autouse=True)
    def before_each(self, mocker: MockerFixture):
        self.hasura_client = mocker.MagicMock()
        self.handler = CreateONENotificationHanderl(self.hasura_client)
        yield

    def test_is_procesable(self):
        notification_template = NotificationMetadata(
            id="rec1",
            name="test",
            topic="test",
            channels=[NotificationChannel.CREATE_ONE],
            table_id="test",
            message_template="test",
            action_link="test",
            destination_layer="test",
            space="test",
            sub_space="test",
            template_id="test",
        )
        assert self.handler.is_procesable(notification_template) == True

    def test_is_not_procesable(self):
        notification_template = NotificationMetadata(
            id="rec1",
            name="test",
            topic="test",
            channels=[NotificationChannel.SLACK],
            table_id="test",
            message_template="test",
            action_link="test",
            destination_layer="test",
            space="test",
            sub_space="test",
            template_id="test",
        )
        assert self.handler.is_procesable(notification_template) == False

    def test_handle(self):
        request = NotificationRequest(
            notificationId="rec1",
            brandCode="test",
            data={"name": "test"},
        )

        notification_template = NotificationMetadata(
            id="rec1",
            name="test",
            topic="test",
            channels=[NotificationChannel.CREATE_ONE],
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

        self.handler.handle(
            notification_template=notification_template,
            notification_request=request,
            message=message,
            title=title,
            action_link=action_link,
            brand=test_brand,
        )

        saved_value = NotificationMessage(
            destination="test",
            message="test test",
            source_table_id="test",
            topic="test, test",
            channels=["Create One"],
            subchannel="test",
            source_record_id=None,
            should_send=True,
            read=False,
            received=False,
            links="test",
            notification_id="rec1",
            payload={"name": "test"},
        )

        assert self.hasura_client.execute.called == True
        assert self.hasura_client.execute.call_count == 1

        assert self.hasura_client.execute.call_args[0][0] == INSERT_MESSAGE
        assert self.hasura_client.execute.call_args[0][1] == saved_value.dict()
