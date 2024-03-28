from typing import List

from fastapi import APIRouter, Depends

from dependencies import get_handlers
from handlers import NotificationHandler, NotificationRequest
from controllers.notification_metadata import NotificationMetadataController
from controllers.brand import BrandController
from res.utils import logger

router = APIRouter(
    prefix="/notification-api",
)

BASE_ID = "appEm3sHF7BTTzgyq"
TABLE_ID = "tbltRVfgzkzYNvf3q"


@router.post("/submit", status_code=200)
def submit_notifcation(
    input: NotificationRequest,
    handlers: List[NotificationHandler] = Depends(get_handlers),
) -> bool:
    brand_controller = BrandController()
    controller = NotificationMetadataController()
    logger.info(f"Received notification request: {input}")
    record = controller.get_by_id(input.notification_id)
    brand_record = brand_controller.get_by_brand_code(input.brand_code)
    logger.debug("Brand response", extra={"brand": brand_record})
    if not brand_record:
        raise Exception(f"Brand not found {input.brand_code}")

    if not record:
        raise Exception(
            f"Notification metadata with id {input.notification_id} not found"
        )
    logger.debug(f"Channels: {record.channels}")

    message = record.message_template or "No message"
    title = record.topic or "Notification Title"
    action_link = record.action_link or ""

    for key, value in input.data.items():
        valueStr = str(value)
        message = message.replace("{" + key + "}", valueStr)
        title = title.replace("{" + key + "}", valueStr)
        action_link = (
            action_link.replace("{" + key + "}", valueStr) if action_link else ""
        )

    for handler in handlers:
        try:
            if handler.is_procesable(record):
                handler.handle(record, input, message, title, action_link, brand_record)
        except Exception as e:
            logger.error(
                "Error in a notification handler",
                extra={
                    "error": e,
                    "args": {
                        "record": record,
                        "input": input,
                        "message": message,
                        "title": title,
                        "action_link": action_link,
                        "brand_record": brand_record,
                    },
                },
            )

    logger.incr_endpoint_requests("submit_notifcation", "POST", 200)

    return True
