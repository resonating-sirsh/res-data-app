import time
from flask import request, g
from res.utils import logger
from uuid import uuid4

# res.utils.logger, flask.request, and flask.g are all globals/singletons
# so they'll be the same as what's imported in the flask application itself.

TRACE_HEADER_NAME = "Res-Trace-Id"
FLASK_TRACE_ATTR = "trace_id"


def get_trace_id():
    return g.trace_id if FLASK_TRACE_ATTR in g else None


def make_trace_id_header():
    return {TRACE_HEADER_NAME: get_trace_id()}


def attach_metrics_exporter(app):
    @app.before_request
    def before_request():
        g.start = time.time()

    @app.after_request
    def after_request(response):
        try:
            diff_ms = (time.time() - g.start) * 1000
            logger.metric_set_endpoint_latency_ms(
                diff_ms,
                endpoint_name=request.endpoint,
                http_method=request.method,
                response_code=response.status_code,
            )

            logger.incr_endpoint_requests(
                endpoint_name=request.endpoint,
                http_method=request.method,
                response_code=response.status_code,
            )

        except Exception as e:
            # Avoiding logger.error here for now, as it's probably not worth paging over
            # or some other alarm will probably be firing.
            # The intent for now is to never interfere with requests.
            logger.warn("Unable to send latency metrics to statsd!", error=e)
        return response


def enable_trace_logging(app):
    @app.before_request
    def before_request():
        g.trace_id = request.headers.get(TRACE_HEADER_NAME) or str(uuid4())

    @app.after_request
    def after_request(response):
        g.trace_id = None
        return response

    logger.trace_getter = get_trace_id
