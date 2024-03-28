import os
from res.connectors.email.resmailconnector import ResEmailClient


# Simple hello world print function testing the ResEmailClient functionality
if __name__ == "__main__":
    RECEIVER_EMAIL = os.getenv("RECEIVER_EMAIL")
    MESSAGE = os.getenv("MESSAGE")

    client = ResEmailClient()
    client.send_email(RECEIVER_EMAIL, "TEST", MESSAGE)
