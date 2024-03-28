import requests, os
from res.utils import logger
# This is a Resonance KGateway Client, which sends formatted data to KGateway

KGATEWAY_TEMPLATE = {
    'version': '1.0.0',
    'process_name': 'default',
    'topic': 'default',
    'data': {}
}

class KGatewayException(Exception):
    pass

class KGatewayClient:
    def __init__(
        self,
        process_name=os.getenv("RES_APP_NAME", "unlabeled"),
        kgateway_host=os.getenv("KAFKA_KGATEWAY_URL", "datadev.resmagic.io"),
        environment="development",
    ):
        self._environment = environment
        self._process_name = process_name
        url_scheme = "https" if environment == "development" else "http"
        self._kgateway_url = kgateway_url = '{}://{}/kgateway/submitevent'.format(url_scheme, kgateway_host)

    def send_message(self, topic, data, throw_error=False):
        temp_template = KGATEWAY_TEMPLATE
        temp_template['topic'] = topic
        temp_template['data'] = data
        response = requests.post(self._kgateway_url, json=temp_template)
        if response.status_code != 200:
            msg = "Error sending data to KGateway! {}".format(response.text)
            logger.critical(msg)
            if throw_error:
                raise KGatewayException(msg)
        else:
            logger.info("Message sent to KGateway topic: {}".format(topic))
