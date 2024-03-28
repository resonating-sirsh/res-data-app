from .common import CommonBaseModel
from fastapi_utils.enums import StrEnum
from typing import List, Optional, Union


class NotificationChannel(StrEnum):
    EMAIL = "Email"
    CREATE_ONE = "Create One"
    SLACK = "Slack"


class NotificationMetadata(CommonBaseModel):
    id: str
    name: str
    topic: str
    message_template: str
    action_link: Optional[str]
    destination_layer: Optional[str]
    channels: List[Union[NotificationChannel, str]]
    space: Optional[str]
    sub_space: Optional[str]
    table_id: Optional[str]
    template_id: Optional[str]
