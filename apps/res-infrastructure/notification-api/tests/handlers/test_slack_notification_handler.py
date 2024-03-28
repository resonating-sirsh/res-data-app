import pytest
from pytest_mock import MockerFixture
from handlers.SlackNotificationHandler import (
    SlackNotificationHandler,
    NotificationRequest,
    NotificationHandler,
)
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from schemas.pydantic.notification_metadata import (
    NotificationMetadata,
    NotificationChannel,
)
from controllers.brand import BrandInNotification

test_brand = BrandInNotification(
    id="test",
    code="test",
    name="test",
    contact_email="test",
    slack_channel="test",
)

# from res.connectors.slack.SlackConnector import SlackConnector


class TestSlackNotificationHandler:
    graphql_client: ResGraphQLClient
    # Removed because of Pyright complaining missing methods and fields
    # slack_connector: SlackConnector
    handler: NotificationHandler

    @pytest.fixture(autouse=True)
    def before_each(self, mocker: MockerFixture):
        mocker.patch("boto3.client").return_value = mocker.MagicMock()
        self.graphql_client = mocker.MagicMock()
        self.slack_connector = mocker.MagicMock()
        self.handler = SlackNotificationHandler(
            self.graphql_client,
            self.slack_connector,
        )

    def test_is_procesable(self):
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

        assert self.handler.is_procesable(notification_template) == True

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
            channels=[NotificationChannel.SLACK],
            table_id="test",
            message_template="test",
            action_link="test",
            destination_layer="test",
            space="test",
            sub_space="test",
            template_id="test",
        )

        self.graphql_client.query.return_value = {
            "data": {
                "brand": {
                    "slackChannel": "test",
                }
            }
        }

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

        # Pyright is complaining about this line said that called and call_count doesn't exist but it does
        # just because they Mock objects
        assert self.slack_connector.called == True
        assert self.slack_connector.call_count == 1
