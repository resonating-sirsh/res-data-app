import os

from res.utils import logger


PROCESS_NAME = os.environ.get("RES_PROCESS_NAME", "optimus-printer-assignment")
ENVIRONMENT = os.getenv("RES_ENV", "development")

GRAPH_API = os.environ.get("GRAPH_API", "https://api.resmagic.io/graphql")
GRAPH_API_KEY = os.environ.get("GRAPH_API_KEY", "")
GRAPH_API_HEADERS = {
    "x-api-key": GRAPH_API_KEY,
    "apollographql-client-name": "fargate",
    "apollographql-client-version": "inject_print_asset",
}


def statsd_incr(metric, increment):
    logger.incr("_".join([PROCESS_NAME, ENVIRONMENT, metric]), increment)
