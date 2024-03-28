from enum import Enum


class StripeErrorCodes(Enum):
    """
    Stripe Error Codes
    """

    INSUFFICIENT_FUNDS = "CARD_NO_FUNDS"
    """
    Raised when the card has insufficient funds to complete the purchase.
    """

    EXPIRED_CARD = "CARD_EXPIRED"
    """
    Raised when the card has expired.
    """

    CARD_DECLINE = "CARD_DECLINED"
    """
    Raised when the card has been declined.
    """

    CHARGE_BLOCKED = "CHARGE_BLOCKED"
    """
    Raised when the card has been blocked.
    """


class StripeException(Exception):
    """
    Stripe Exception
    """

    code: StripeErrorCodes

    def __init__(self, code: StripeErrorCodes, *args: object) -> None:
        self.code = code
        super().__init__(*args)
