from uuid import uuid4
from fastapi import FastAPI, Request, Response
from res.utils import logger
import time


def add_commons(app: FastAPI):
    @app.get(
        "/healthcheck",
        tags=["commons"],
        status_code=200,
        summary="To check service is up",
    )
    async def check_health():
        return "OK"

    # Adding logging and metrics
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        """
        Add a header with the time taken to process the request

        :param request: The request object
        :param call_next: The next function to call
        :return: The response object
        """

        start_time = time.time()
        response: Response = await call_next(request)

        # Add the trace id to the logger
        logger.trace_getter = lambda: request.headers.get("Res-Trace-Id") or str(
            uuid4()
        )

        try:
            process_time = time.time() - start_time
            response.headers["X-Process-Time"] = str(process_time)
            logger.metric_set_endpoint_latency_ms(
                process_time,
                endpoint_name=request.url.path,
                http_method=request.method,
                response_code=response.status_code,
            )
            logger.incr_endpoint_requests(
                endpoint_name=request.url.path,
                http_method=request.method,
                response_code=response.status_code,
            )
        except Exception as e:
            logger.warn("Unable to send latency metrics to statsd", error=e)

        return response
