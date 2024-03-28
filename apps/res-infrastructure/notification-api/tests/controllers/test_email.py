import pytest
from pytest_mock import MockerFixture
from controllers.email import _EmailController, SendgridEmailController, EmailAddress

to_emails = ["test@test.com"]
from_email = EmailAddress(name="Adrison Work", address="abc@test.com")


@pytest.mark.skip()
class TestEmailController:
    @pytest.fixture
    def email_controller(self):
        return SendgridEmailController()

    @pytest.fixture(autouse=True)
    def before_each(self, mocker: MockerFixture):
        yield mocker
        # mocker.patch(
        #     "controllers.email.secrets_client.get_secret",
        #     return_value="SG.1234567890",
        # )

        # self.send_spy = mocker.patch(
        #     "controllers.email.SendGridAPIClient.send",
        #     return_value=None,
        # )
        ...

    def test_send_email(self, email_controller: _EmailController):
        email_controller.send(
            from_email=from_email,
            to_emails=to_emails,
            subject="test",
            content="Email test",
        )

        # self.send_spy.assert_called_once_with(
        #     {
        #         "from": {
        #             "email": from_email.address,
        #             "name": from_email.name,
        #         },
        #         "html_content": None,
        #         "plain_text_content": "Email test",
        #         "subject": "test",
        #         "to": to_emails,
        #     },
        # )

        # self.send_spy.reset_mock()

    def test_send_email_with_html(self, email_controller: _EmailController):
        email_controller.send(
            from_email=EmailAddress(
                name="Adrison Work",
                address="agomez@resonance.nyc",
            ),
            to_emails=["adrison.gomez@hotmail.com"],
            subject="Test 2",
            content="<h1>Email test</h1>",
            html_content=True,
        )

        # self.send_spy.assert_called_once_with(
        #     {
        #         "from": {
        #             "email": from_email.address,
        #             "name": from_email.name,
        #         },
        #         "html_content": "<h1>Email test</h1>",
        #         "plain_text_content": None,
        #         "subject": "Test 2",
        #         "to": to_emails,
        #     },
        # )
