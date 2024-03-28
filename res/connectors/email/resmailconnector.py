from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.audio import MIMEAudio
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
import mimetypes
import os
import smtplib
import ssl
from res.utils import secrets_client


class ResEmailClient:
    def __init__(self):
        email, password = self.get_creds()
        self.email = email
        self.password = password

    def get_creds(self):
        email = secrets_client.get_secret("ANALYTICS_EMAIL")
        password = secrets_client.get_secret("ANALYTICS_EMAIL_PASSWORD")

        return email, password

    def send_email(self, receiver_email, subject, emailMsg, file=None, **kwargs):
        content_type = kwargs.get("content_type", "plain")
        sender_email = self.email
        mimeMessage = MIMEMultipart()
        if type(receiver_email) == list:
            mimeMessage["to"] = ", ".join(receiver_email)
        else:
            mimeMessage["to"] = receiver_email

        mimeMessage["subject"] = subject
        mimeMessage.attach(MIMEText(emailMsg, content_type))

        if file:
            content_type, encoding = mimetypes.guess_type(file)
            if content_type is None or encoding is not None:
                content_type = "application/octet-stream"
            main_type, sub_type = content_type.split("/", 1)
            if main_type == "text":
                fp = open(file, "r")
                msg = MIMEText(fp.read(), _subtype=sub_type)
                fp.close()
            elif main_type == "image":
                fp = open(file, "rb")
                msg = MIMEImage(fp.read(), _subtype=sub_type)
                fp.close()
            elif main_type == "audio":
                fp = open(file, "rb")
                msg = MIMEAudio(fp.read(), _subtype=sub_type)
                fp.close()
            elif main_type == "application":
                fp = open(file, "rb")
                msg = MIMEApplication(fp.read(), _subtype=sub_type)
                fp.close()
            else:
                fp = open(file, "rb")
                msg = MIMEBase(main_type, sub_type)
                msg.set_payload(fp.read())
                fp.close()

            filename = os.path.basename(file)
            msg.add_header("Content-Disposition", "attachment", filename=filename)
            mimeMessage.attach(msg)

        text = mimeMessage.as_string()
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, self.password)
            server.sendmail(sender_email, receiver_email, text)
