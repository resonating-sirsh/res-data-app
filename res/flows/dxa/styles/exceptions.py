import stringcase


class InvalidStyleException(Exception):
    def __init__(self, message, tags=None, data=None):
        """
        Capture context for the flow
        """
        super().__init__(message)
        self._tags = tags
        self._data = data

    @property
    def flag(cls):
        return (
            stringcase.snakecase(cls.__class__.__name__)
            .upper()
            .replace("EXCEPTION", "")
            .rstrip("_")
        )


class BodyAssetMissingException(InvalidStyleException):
    def __init__(self, message, tags=None):
        """
        Capture context for the flow
        """
        super().__init__(message, tags)


class AssetVersionMismatchException(InvalidStyleException):
    def __init__(self, message, tags=None):
        """
        Capture context for the flow
        """
        super().__init__(message, tags)


class FailedPieceNamingException(InvalidStyleException):
    def __init__(self, message, tags=None):
        """
        Capture context for the flow
        """
        super().__init__(message, tags)
