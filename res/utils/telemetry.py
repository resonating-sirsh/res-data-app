from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from res.utils.env import GET_RES_NODE, GET_RES_APP, GET_RES_NAMESPACE
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import res


def get_observed_resource(**kwargs):
    attributes = {
        "namespace": kwargs.get("namespace", GET_RES_NAMESPACE()) or "no-namespace",
        "service.name": kwargs.get("node", GET_RES_NODE()) or "no-node",
        "application": kwargs.get("process", GET_RES_APP()) or "no-app",
    }

    res.utils.logger.debug(f"Resource attributes for telemetry {attributes}")
    resource = Resource(attributes=attributes)

    return resource


def get_app_tracer(**kwargs):
    """
    get tracer:>
    ctx=None
    tracer = get_app_tracer()
    with tracer.start_as_current_span("some_span", ctx) as span:
    """
    span_exporter = OTLPSpanExporter()
    tp = TracerProvider(resource=get_observed_resource(**kwargs))
    # trace.set_tracer_provider(tp)
    span_processor = BatchSpanProcessor(span_exporter)
    tp.add_span_processor(span_processor)
    return tp.get_tracer(__name__)


# eg. carrier = {"traceparent": X}
# ctx = TraceContextTextMapPropagator().extract(carrier)
# with tracer.start_as_current_span("child_span", ctx) as span:
#     with tracer.start_as_current_span("bar"):
#         with tracer.start_as_current_span("baz"):
#             span.add_event("event message",  {"event_attributes": 1})
#             trace.get_current_span().set_attribute("foo", "bar")
