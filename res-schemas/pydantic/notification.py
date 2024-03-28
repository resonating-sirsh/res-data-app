from typing import Any, Dict, List, Optional
from pydantic.fields import Field
from .common import CommonBaseModel


class NotificationRequest(CommonBaseModel):
    notification_id: str = Field(
        alias="notificationId",
    )

    brand_code: str = Field(
        alias="brandCode",
    )

    data: Dict[str, Any] = Field(
        alias="data",
    )


class NotificationMessage(CommonBaseModel):
    destination: str
    message: str
    topic: str
    channels: List[str]
    subchannel: str
    source_table_id: str
    source_record_id: Optional[str] = Field(
        default=None,
    )
    should_send: bool = Field(
        default=True,
    )
    read: bool = Field(
        default=False,
    )
    received: bool = Field(
        default=False,
    )
    links: str
    notification_id: Optional[str]
    payload: Optional[Dict[str, Any]]


class SlackNotificationPayload(CommonBaseModel):
    slack_channels: List[str]
    message: str
    attachments: List[Dict[str, Any]] = Field(
        default=[],
    )
