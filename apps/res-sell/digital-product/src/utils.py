import requests

from res.utils import logger


def get_matic_price():
    url = "https://min-api.cryptocompare.com/data/price?fsym=MATIC&tsyms=USD"
    request = requests.get(url)
    data = request.json()
    logger.debug(data)
    return data["USD"]


def calculate_matic_to_usd(gas_amount, matic_to_usd_rate):
    return gas_amount * matic_to_usd_rate * 0.000000001


class DigitialProductRequestType:
    CREATED = "created"
    TRANSFER = "transfer"


class DigitalProductRequestStatus:
    PROCESSING = "processing"
    DONE = "done"
    ERROR = "error"
    HOLD = "hold"
    RETRY = "retry"
