import abc
from typing import List, Optional
from sendgrid import SendGridAPIClient, SendGridException
from sendgrid.helpers.mail import Mail, From
from res.utils import logger, secrets_client
from pydantic import BaseModel, EmailStr


class EmailAddress(BaseModel):
    name: Optional[str]
    address: str


class _EmailController(abc.ABC):
    @abc.abstractmethod
    def send(
        self,
        from_email: EmailAddress,
        to_emails: List[EmailStr],
        subject: str,
        content: str,
        html_content: bool = False,
        template_id: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        pass


class SendgridEmailController(_EmailController):
    def __init__(self, api_key: Optional[str] = None):
        if not api_key:
            secret = secrets_client.get_secret("SENDGRID_API_KEY")
            if type(secret) == dict:
                api_key = secret["SENDGRID_API_KEY"]
            elif type(secret) == str:
                api_key = secret
            else:
                raise Exception("Invalid secret")

        self.client = SendGridAPIClient(api_key=api_key)

    def send(
        self,
        from_email: EmailAddress,
        to_emails: List[EmailStr],
        subject: str,
        content: str,
        html_content: bool = False,
        template_id: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        try:
            request_email = Mail(
                subject=subject,
                from_email=From(
                    name=from_email.name,
                    email=from_email.address,
                ),
                to_emails=to_emails,
                html_content=content if html_content or template_id == None else None,
                plain_text_content=content
                if not html_content or template_id == None
                else None,
            )

            if template_id and data:
                request_email.template_id = template_id
                request_email.dynamic_template_data = data

            logger.info(
                f"Sending email: {request_email}",
                extra={"request_email": request_email},
            )

            self.client.send(request_email)
        except SendGridException as e:
            logger.error(
                f"Error sending email",
                exception=e,
                traceback=e.__traceback__,
                extra={
                    "to": to_emails,
                    "from": from_email,
                    "subject": subject,
                },
            )
