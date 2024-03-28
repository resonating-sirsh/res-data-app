from logging import debug

from tenacity.wait import wait_random_exponential
from res.utils.logging import logger
from .KafkaConnector import KafkaConnector
from .KGatewayClient import KGatewayClient


kgateway_client = KGatewayClient()



