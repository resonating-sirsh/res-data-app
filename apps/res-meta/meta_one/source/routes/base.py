import abc


class Controller(abc.ABC):
    """
    Base class for all controllers
    """

    def set_context(self, context):
        self.context = context
