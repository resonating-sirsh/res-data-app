from dependencies import get_airtable_client, get_handlers
import pytest
from fastapi.testclient import TestClient
from pytest_mock import MockerFixture
from schemas.pydantic.notification import NotificationRequest


@pytest.mark.skip()
class TestNotificationRoutes:
    @pytest.fixture
    def client(self, mocker: MockerFixture) -> TestClient:
        from main import app

        mocker.patch(
            "res.connectors.slack.SlackConnector.SlackConnector"
        ).return_value = mocker.MagicMock()
        return TestClient(app)

    def test_submit_notification(self, client: TestClient, mocker: MockerFixture):
        from main import app

        with client:
            handler = mocker.MagicMock()
            handler.is_procesable.return_value = True
            handler.handle.return_value = True
            airtable_conn = mocker.MagicMock()
            airtable_conn.get_record.return_value = {
                "fields": {
                    "Message": "test {name}",
                    "Topic": "test, {name}",
                    "Action URL": "test",
                    "Channels": ["test"],
                }
            }

            app.dependency_overrides[get_handlers] = lambda: [handler]
            app.dependency_overrides[get_airtable_client] = lambda: airtable_conn

            request = NotificationRequest(
                notificationId="rec1", brandCode="test", data={"name": "test"}
            )

            response = client.post(
                "/notification-api/submit",
                json=request.dict(),
            )

            assert response.status_code == 200

            assert handler.is_procesable.called
            assert handler.handle.called
            assert airtable_conn.get_record.called

            assert handler.handle.call_args[0][0] == {
                "Message": "test {name}",
                "Topic": "test, {name}",
                "Action URL": "test",
                "Channels": ["test"],
            }

            assert handler.handle.call_args[0][1] == request
            assert handler.handle.call_args[0][2] == "test test"
            assert handler.handle.call_args[0][3] == "test, test"
            assert handler.handle.call_args[0][4] == "test"
