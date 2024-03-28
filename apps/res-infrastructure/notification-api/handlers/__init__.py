import abc

from schemas.pydantic.notification import NotificationRequest
from schemas.pydantic.notification_metadata import NotificationMetadata
from controllers.brand import BrandInNotification


class NotificationHandler(abc.ABC):
    """
    Abstract class for notification handlers
    """

    @abc.abstractmethod
    def __init__(self, *args, **kwargs):
        pass

    @abc.abstractmethod
    def is_procesable(
        self,
        notification_template: NotificationMetadata,
    ) -> bool:
        pass

    @abc.abstractmethod
    def handle(
        self,
        notification_template: NotificationMetadata,
        notification_request: NotificationRequest,
        message: str,
        title: str,
        action_link: str,
        brand: BrandInNotification,
    ) -> bool:
        pass
