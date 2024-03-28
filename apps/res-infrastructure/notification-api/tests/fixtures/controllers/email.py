from pydantic import EmailStr
from res.utils import logger
from typing import List, Optional
from controllers.email import _EmailController, EmailAddress


class FakeEmailController(_EmailController):
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
        logger.info(
            f"Sending email: from_email={from_email}, to_emails={to_emails}, subject={subject}, content={content}, html_content={html_content}",
        )

        self.from_email = from_email
        self.to_emails = to_emails
        self.subject = subject
        self.content = content
        self.html_content = html_content
        self.template_id = template_id
        self.data = data
