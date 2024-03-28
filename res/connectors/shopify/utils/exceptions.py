import typing


class ShopifyException(Exception):
    errors: typing.Any

    def __init__(self, message, errors=None, *args: object) -> None:
        self.errors = errors
        self.message = message
        super().__init__(message, *args)
