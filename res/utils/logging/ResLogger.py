"""
This is a Resonance Logger, which wraps the logging class in python or other logger. using structlog which has nice properties
We will use other logger extensions for writing errors and metrics. For example sentry sdk writes errors and statds writes metrics

Examples

from res.utils import logger

# Logging Levels
logger.info('just a message')
logger.warn('a warning')
logger.error('a serious problem - sends error to sentry')
logger.critical('app is not functioning - sends "fatal" to sentry, triggers pagerduty immediately')

# Metrics, sent to Prometheus + Grafana
# Note: this requires that you are opening ports so not safe
logger.metrics.incr("a_counter", 42)
#instead we can wrap calls one by one and catch exceptions and log to debug that we cannot connect
logger.incr("a_counter", 42)

"""

from __future__ import annotations
from functools import lru_cache, wraps
import os
import logging
from traceback import format_exception, format_tb
from types import TracebackType
from typing import Any, Dict, Optional, Tuple, Type, TypeVar, Union
import uuid
import orjson
from enum import EnumMeta
import sentry_sdk
import structlog
from structlog.contextvars import get_contextvars
from sentry_sdk.api import set_context
from sentry_sdk import capture_exception, capture_message, set_tag
from statsd import StatsClient, TCPStatsClient

from statsd.client import StatsClientBase
from typing_extensions import Literal

# pylint: disable=undefined-all-variable
__all__ = [  # noqa
    "ENVS_EXCLUDE_FROM_SENTRY",
    "DEFAULT_STATSD_PORT",
    "SENTRY_PROJECT_DSNS",
    "DummyMetricsProvider",
    "ResLogger",
    "logger",  # type: ignore
    "add_contextvar_to_log_message",
]

T = TypeVar("T")

Exc = TypeVar("Exc", bound=BaseException)
ExcInfo = Union[Tuple[None, None, None], Tuple[Type[Exc], Exc, TracebackType]]


def __getattr__(name) -> ResLogger:
    """Provide the logger lazily."""
    if name == "logger":
        return ResLogger.get_default_logger()
    raise AttributeError(f"module '{__name__!r}' has no attribute '{name!r}'")


# if there are envs we do not want to include for `errors()` that get sent to sentry add
# them here going to leave this empty for awhile as folks get used to using the sentry
# feature / lib stabilizes
ENVS_EXCLUDE_FROM_SENTRY = ["local"]
DEFAULT_STATSD_PORT = 9125


# structlog is cool but also insane.
def add_contextvar_to_log_message(context_var_name):
    def preprocessor(logger, log_method, log_event):
        context_var = get_contextvars().get(context_var_name)
        if context_var is not None:
            msg = log_event.pop("event")
            log_event["event"] = f"[{context_var}] {msg}"
        return log_event

    return preprocessor


class DSN:
    """A class to represent sentry DSNs."""

    __slots__ = "key", "project", "org_id", "__dict__", "__weakref__"

    def __init__(self, key: str, project: int, *, org_id="o571782"):
        """Format the sentry DSN from the inputs."""
        super().__init__()
        self.key = key
        self.project = project
        self.org_id = org_id

    def __str__(self) -> str:
        """Return the DSN's URI."""
        return f"https://{self.key}@{self.org_id}.ingest.sentry.io/{self.project}"


# Add any new sentry projects (aka Teams) here
SENTRY_PROJECT_DSNS = {
    "res-general": DSN("abc09144c02a4812b203785b22251772", 5752654),
    "res-make": DSN("dbccb7e7f8f24c4aa12ff063bea5cc51", 5893438),
    "res-infrastructure": DSN("c518a55c41304253a3d59776725ad761", 6751977),
    "optimus": DSN("53dca6f0b89247d8b46904e8d9889366", 6087959),
    "iamcurious": DSN("df5ec693f10245c09f7959ae1d15f2d4", 6089610),
    "res-sell": DSN("8a28de20a2554d90a7d3d6a93d5ad983", 6118350),
    "res-premises": DSN("c7a6b62e015f4350a1f120eced0115c1", 6182683),
    "dxa": DSN("a2e7c395fbfe44ce9dcde4c754bd2019", 6273280),
}


class DummyMetricsProvider(StatsClientBase):
    """Dummy subclass to provide noop methods."""

    @lru_cache(maxsize=1)
    def __new__(cls):
        """Call the singleton ctor function to get the same object every time."""
        return super().__new__(cls)

    def incr(self, *_, **__):
        """Do nothing placeholder for incr call."""

    def gauge(self, *_, **__):
        """Do nothing placeholder for gauge call."""

    def decr(self, *_, **__):
        """Do nothing placeholder for decr call."""

    def set(self, *_, **__):
        """Do nothing placeholder for set call."""

    def timing(self, *_, **__):
        """Do nothing placeholder for timing call."""

    def timer(self, *_, **__):
        """Do nothing placeholder for timer call."""

    def _send(self, *_, **__):
        """Do nothing placeholder for _send call."""

    def pipeline(self, *_, **__):
        """Do nothing placeholder for pipeline call."""


def coalesce(*args: Any, default: Any = None):
    """Return the first non-None arg, or the default."""
    for arg in args:
        if arg is not None:
            return arg
    return default


def coalesce_bool(*args: Any, default: Optional[bool] = None):
    """Return the first non-None arg, or the default."""
    arg = coalesce(*args, default=default)
    if isinstance(arg, str):
        return str(arg).lower() not in ["f", "false", "n", "no"]
    return None if arg is None else True if arg else False


def with_trace(fn):
    """Evaluate trace_getter to silently inject the trace_id into the kwargs."""

    @wraps(fn)
    def outer(self, *args, **kwargs):
        if self.trace_getter is not None:
            trace_id = self.trace_getter()
            if trace_id:
                kwargs["trace_id"] = trace_id
        fn(self, *args, **kwargs)

    return outer


def sentry_extras_encoder(obj):
    if isinstance(obj, bytes):
        return obj.decode(encoding="utf-8", errors="replace")
    raise TypeError


class ResLogger:
    """Logger implementation with sentry event tracking and metrics via statsd."""

    _default_logger: Optional[ResLogger] = None

    @classmethod
    def get_default_logger(cls) -> ResLogger:
        """Return the default logger."""
        if cls._default_logger is None:
            cls._default_logger = cls(enable_sentry_integration=False)
        return cls._default_logger

    @classmethod
    def set_default_logger(cls: Type[ResLogger], new_logger: ResLogger) -> None:
        """Set the default logger impl returned by the logger module attr."""
        if not isinstance(new_logger, ResLogger):
            raise TypeError(
                f"Refusing to set '{type(new_logger).__qualname__}' object as the "
                f"default logger! (got '{new_logger!r})"
            )
        cls._default_logger = new_logger

    def __init__(self, **kwargs):
        # pylint: disable=invalid-envvar-default
        app_name: Optional[str] = kwargs.get("app_name", None)
        cluster_name: Optional[str] = kwargs.get("cluster_name", None)
        environment: Optional[str] = kwargs.get("environment", None)
        namespace: Optional[str] = kwargs.get("namespace", None)
        log_level: Optional[str] = kwargs.get("log_level", None)
        team: Optional[str] = kwargs.get("team", None)
        enable_sentry_integration: Optional[bool] = coalesce_bool(
            kwargs.get("enable_sentry_integration"),
            os.getenv("ENABLE_SENTRY_INTEGRATION"),
        )

        enable_metrics_integration: Optional[bool] = coalesce_bool(
            kwargs.get("enable_metrics_integration"),
            os.getenv("ENABLE_METRICS_INTEGRATION"),
        )

        self._app_name: str = coalesce(
            app_name, os.getenv("RES_APP_NAME"), default="res-general"
        )
        self._namespace: Optional[str] = coalesce(namespace, os.getenv("RES_NAMESPACE"))
        self._cluster_name = coalesce(
            cluster_name, os.getenv("RES_CLUSTER"), default="localhost"
        )
        self._environment: str = coalesce(
            environment, os.getenv("RES_ENV"), default="local"
        )
        self._log_level: int = getattr(
            logging,
            coalesce(
                log_level,
                kwargs.get("res_log_level"),
                os.getenv("LOG_LEVEL"),
                os.getenv("RES_LOG_LEVEL"),
                default=(
                    "DEBUG" if self._environment in ["local", "development"] else "INFO"
                ),
            ).upper(),
            logging.INFO,
        )
        default_team = (
            "res-general"
            if self._environment != "local" and self._cluster_name != "localhost"
            else None
        )
        self._team = coalesce(team, os.getenv("RES_TEAM"), default=default_team)

        self._logger = ResLogger.configure_logging(self._log_level)
        self._initial_processors = structlog.get_config()["processors"]
        self.trace_getter: Any = None
        self.debug(f"Log level set to {self._log_level}")

        if enable_sentry_integration is True or (
            enable_sentry_integration is None and self._environment != "local"
        ):
            self.init_sentry_integration(**kwargs)
        else:
            self.debug("Sentry integration not enabled, skipping sentry init.")
            self._sentry_dsn = None
            self._sentry_enabled = False
            # Set Sentry project URI based on the appropriate team

        self._metrics_provider = DummyMetricsProvider()
        if enable_metrics_integration is True or (
            enable_metrics_integration is None and self._environment != "local"
        ):
            self.setup_metric_provider(**kwargs)
        else:
            self.warn(
                "Metrics integration not enabled, skipping petrics provider setup."
            )
            self._statsd_host = kwargs.get("statsd_host")
            self._statsd_port = kwargs.get("statsd_port")

    @property
    @lru_cache(maxsize=None)
    def team(self):
        """Return self._team if recognized or warn and return 'res-general'."""
        if self._team in SENTRY_PROJECT_DSNS:
            return self._team
        if not self._team:
            self.warn(
                "Please set RES_TEAM environment variable in your app. This should "
                "correspond to a team name in Sentry (e.g. 'res-general'). See Coda "
                "for more details."
            )
        else:
            self.warn(
                f"Team '{self._team}' not found in ResLogger ()! You may need to set "
                "up your sentry project URI. See Coda for details. Defaulting to "
                "'res-general' team."
            )
        return "res-general"

    def init_sentry_integration(self, sentry_dsn=None, **kwargs):
        """Initialize the sentry integration."""
        self._team = coalesce(
            sentry_dsn if sentry_dsn in SENTRY_PROJECT_DSNS else None,
            kwargs.get("team"),
            os.getenv("RES_TEAM"),
            getattr(self, "_team", None),
        )
        self._sentry_dsn = coalesce(
            kwargs.get("sentry_dsn"),
            os.getenv("SENTRY_DSN"),
            SENTRY_PROJECT_DSNS.get(str(self._team)),
            getattr(self, "_sentry_dsn", None),
        )

        if not self._sentry_dsn:
            self.warn("Encountered empty Sentry DSN, skipping sentry init.")
            return

        sentry_dsn_team = None
        for team, dsn in SENTRY_PROJECT_DSNS.items():
            if dsn == self._sentry_dsn:
                sentry_dsn_team = team
                break

        sentry_dsn_source_label = (
            f"the Sentry DSN for the '{sentry_dsn_team}' team"
            if sentry_dsn_team
            else "a custom (non-team) Sentry DSN"
        )
        if not sentry_dsn_team or sentry_dsn_team != self.team:
            self.warn(
                f"Using {sentry_dsn_source_label}, not the one configured for the "
                f"current res-team '{self.team}'!"
            )

        # pylint: disable=abstract-class-instantiated
        sentry_sdk.init(
            str(self._sentry_dsn),
            environment=self._environment,
            traces_sample_rate=1.0,
            before_send=ResLogger.before_send_sentry,
            attach_stacktrace=True,
        )
        self.info(
            f"Sentry sdk set up for environment '{self._environment}' with "
            f"{sentry_dsn_source_label}"
        )
        self.sentry_enabled = True

    def setup_metric_provider(self, statsd_host=None, statsd_port=None, **kwargs):
        """Attach the metrics provider."""
        # pylint: disable=invalid-envvar-default
        _warnings = []
        try:
            self._statsd_host = coalesce(
                statsd_host,
                os.getenv("STATSD_HOST"),
                getattr(self, "_statsd_host", None),
            )
            if self._statsd_host is None:
                if self._cluster_name == "localhost":
                    self._statsd_host = "localhost"
                else:
                    self._statsd_host = (
                        "statsd-exporter-prometheus-statsd-exporter"
                        ".statsd-exporter.svc.cluster.local"
                    )
                self.warn(
                    "Did not find STATSD_HOST in the environment, defaulted to "
                    f"{self._statsd_host} which may not be what you want"
                )

            self._statsd_port = int(
                str(
                    coalesce(
                        statsd_port,
                        os.getenv("STATSD_PORT"),
                        getattr(self, "_statsd_port", None),
                        default=DEFAULT_STATSD_PORT,
                    )
                )
            )

            if self._statsd_host == "DUMMY":
                self._metrics_provider = DummyMetricsProvider()

            elif self._statsd_host == "localhost":
                # locally use TCP but remote use UDP on the K8s cluster. This is because
                # UDP is better but i dont know how to do UDP -> K8s locally port
                # forward using kube-forwarder or you get a warning. Also add a timeout
                # on TCP constructor.
                # Also, we omit the statsd prefix because it interferes with metrics
                # dimension parsing in the statsd-exporter

                self._metrics_provider = TCPStatsClient(
                    host="localhost", port=self._statsd_port, timeout=2.0
                )
            else:
                self._metrics_provider = StatsClient(
                    host=self._statsd_host, port=self._statsd_port
                )

            self.debug(f"Set up metrics provider: {self._metrics_provider!r}")
        except Exception as ex:  # pylint: disable=broad-except
            self.warn(
                f"Unable to setup metric provider at {statsd_host}: {repr(ex)} - this "
                "is likely due to a corrupt env or connectivity issues"
            )

    @staticmethod
    def before_send_sentry(event, hint):
        if "exc_info" in hint:
            exc_type, exc_value, tb = hint["exc_info"]
            # need to look into how this works
            if "pytest" in exc_value:
                return None
        return event

    @property
    def metrics(self):
        """
        Use statsd interface e.g.
        logger.metrics.inc
        logger.metrics.gauge

        #problem with using this is we can not safely called statsd in all environments
        #unless we fire and forget udp
        """
        return self._metrics_provider

    # This a generic increment. Specific metrics are implemented below
    def incr(self, *args, **kwargs):
        try:
            self._metrics_provider.incr(*args, **kwargs)
        except Exception as ex:
            host = os.environ.get("STATSD_HOST", "localhost")
            # SA - removing this for now until statds envs are properly fleshed out
            # this message is annoying
            if host != "localhost":
                self.debug(
                    f"failed to call op incr on metrics provider at {host}. If local, "
                    "are you forwarding to statsd port (e.g. locally port forward and "
                    f"reload res) : {repr(ex)}"
                )

    def set_gauge(self, *args, **kwargs):
        try:
            self._metrics_provider.gauge(*args, **kwargs)
        except Exception as ex:  # pylint: disable=broad-except
            self.debug(ex)
            host = os.environ.get("STATSD_HOST", "localhost")
            # SA - removing this for now until statds envs are properly fleshed out
            # this message is annoying
            if host != "localhost":
                self.debug(
                    f"failed to call op gauge on metrics provider at {host}. If local, "
                    "are you forwarding to statsd port (e.g. locally port forward and "
                    f"reload res) : {repr(ex)}"
                )

    def metric_contract_failure_incr(
        self, node, asset_key, contract, flow="primary", process="res-data"
    ):
        """
        -match: "contract_fail.*.*.*.*"
        name: "contract_fail"
        labels:
          flow: "$1"
          node: "$2"
          asset_key: "$3"
          contract: "$4
         process: "$5
        """
        node = node.replace(".", "_")
        asset_key = asset_key.replace(".", "_")
        metric_name = f"contract_fail.{flow}.{node}.{asset_key}.{contract}.{process}"
        self.incr(metric_name, 1)
        return metric_name

    def metric_exception_log_incr(self, node, sub_node, process="res-data"):
        return self.metric_node_state_transition_incr(
            node=node, asset_key=sub_node, status="EXCEPTION_EVENT", process=process
        )

    def metric_node_state_transition_incr(
        self, node, asset_key, status, flow="primary", process="res-data", inc_value=1
    ):
        """
        - match: "flow_state.*.*.*.*.*"
        name: "flow_state"
        labels:
          flow: "$1"
          node: "$2"
          asset_key: "$3"
          status: "$4
          process: "$5
        """
        node = node.replace(".", "_")
        asset_key = asset_key.replace(".", "_")
        status = status.value if isinstance(type(status), EnumMeta) else status
        metric_name = f"flow_state.{flow}.{node}.{asset_key}.{status}.{process}"
        self.incr(metric_name, inc_value)
        return metric_name

    def metric_node_state_transition_gauge(
        self, node, asset_key, status, flow="primary", process="res-data", value=1
    ):
        """
        - match: "flow_state.*.*.*.*.*"
        name: "flow_state"
        labels:
          flow: "$1"
          node: "$2"
          asset_key: "$3"
          status: "$4
          process: "$5
        """
        node = node.replace(".", "_")
        asset_key = asset_key.replace(".", "_")
        status = status.value if isinstance(type(status), EnumMeta) else status
        metric_name = f"flow_state.{flow}.{node}.{asset_key}.{status}.{process}"
        self.gauge(metric_name, value)
        return metric_name

    def metric_set_runtime_ms(self, value):
        self.timing(f"jobs.runtimes_ms.{self._namespace}.{self._app_name}", value)

    def metric_set_records_processed(self, value, status="success"):
        valid_statuses = ["success", "failure", "warning"]
        if status not in valid_statuses:
            statuses_str = ",".join(valid_statuses)
            self.error(
                f"Invalid status for metric_set_records_processed in Logger: {status}. "
                f"Must be one of {statuses_str}"
            )
        else:
            self.incr(
                f"jobs.records_processed.{self._namespace}.{self._app_name}.{status}",
                value,
            )

    def endpoint_latency_name(self, endpoint_name, http_method, response_code):
        return (
            f"services.latency_ms.{self._namespace}.{self._app_name}"
            f".{endpoint_name}.{http_method}.{response_code}"
        )

    def metric_set_endpoint_latency_ms(
        self, value, endpoint_name, http_method, response_code=200, **kwargs
    ):
        """
        example  metric_set_endpoint_latency_ms(2100, 'shopify-webhooks', 'post')
        """
        self.timing(
            self.endpoint_latency_name(endpoint_name, http_method, response_code),
            value,
            **kwargs,
        )

    def endpoint_requests_name(self, endpoint_name, http_method, response_code):
        return (
            f"services.total_requests.{self._namespace}.{self._app_name}"
            f".{endpoint_name}.{http_method}.{response_code}"
        )

    def incr_endpoint_requests(
        self, endpoint_name, http_method, response_code, total_requests=1, **kwargs
    ):
        self.incr(
            self.endpoint_requests_name(endpoint_name, http_method, response_code),
            total_requests,
            **kwargs,
        )

    def incr_data_flow(self, asset_type, asset_group, status, inc_value=1):
        metric_name = (
            f"data_flows.{self._namespace}.{self._app_name}"
            f".{asset_type}.{asset_group}.{status}"
        )
        self.incr(metric_name, inc_value)

    def gauge_cohort_flow(self, node, asset_group, label, status="ok", value=1):
        metric_name = f"data_flows.cohorts.{node}.{asset_group}.{label}.{status}"
        self.gauge(metric_name, value)
        return metric_name

    def gauge(self, *args, **kwargs):
        try:
            self._metrics_provider.gauge(*args, **kwargs)
        except:  # noqa; pylint: disable=bare-except
            self.info(
                "failed to call op gauge on metrics provider - are you connecting to "
                "statsd port (e.g. locally port forward and reload res)"
            )

    def decr(self, *args, **kwargs):
        try:
            self._metrics_provider.decr(*args, **kwargs)
        except:  # noqa; pylint: disable=bare-except
            self.debug(
                "failed to call op decr on metrics provider - are you connecting to "
                "statsd port (e.g. locally port forward and reload res)"
            )

    def set(self, *args, **kwargs):
        try:
            self._metrics_provider.set(*args, **kwargs)
        except:  # noqa; pylint: disable=bare-except
            self.debug(
                "failed to call op set on metrics provider - are you connecting to "
                "statsd port (e.g. locally port forward and reload res) - {repr(ex)}"
            )

    def timing(self, *args, **kwargs):
        try:
            self._metrics_provider.timing(*args, **kwargs)
        except:  # noqa; pylint: disable=bare-except
            self.debug(
                "failed to call op on metrics provider - are you connecting to statsd "
                "port (e.g. locally port forward)"
            )

    def timer(self, *args, **kwargs):
        try:
            self._metrics_provider.timer(*args, **kwargs)
        except:  # noqa; pylint: disable=bare-except
            self.debug(
                "failed to call op on metrics provider - are you connecting to statsd "
                "port (e.g. locally port forward)"
            )

    def debug(self, msg, **kwargs):
        if self._log_level <= logging.DEBUG:
            self._logger.debug(msg, **kwargs)

    @with_trace
    def info(self, msg, **kwargs):
        if self._log_level <= logging.INFO:
            self._logger.info(msg, **kwargs)

    @with_trace
    def warn(self, msg, **kwargs):
        if self._log_level <= logging.WARN:
            self._logger.warn(msg, **kwargs)

    # synonym
    @with_trace
    def warning(self, msg, **kwargs):
        if self._log_level <= logging.WARN:
            self._logger.warn(msg, **kwargs)

    @with_trace
    def error(
        self,
        msg: Optional[str] = None,
        exception: Optional[Union[ExcInfo[Exc], Exc, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        errors: Any = None,
        *,
        traceback: Optional[Union[TracebackType, str]] = None,
        exception_type: Optional[Union[Type[Exc], str]] = None,
        **kwargs,
    ) -> None:
        logger_kwargs = kwargs.copy()
        if exception:
            if isinstance(exception, tuple):
                exception_type, exception, traceback = exception
            if isinstance(exception, BaseException):
                logger_kwargs["exception"] = "".join(
                    format_exception(
                        exception_type if isinstance(exception_type, type) else None,
                        exception,
                        (
                            traceback
                            if isinstance(traceback, TracebackType)
                            else exception.__traceback__
                        ),
                    )
                )
            elif isinstance(exception, str):
                logger_kwargs["exception"] = exception
        if isinstance(traceback, TracebackType):
            traceback = "".join(format_tb(traceback))
            logger_kwargs["traceback"] = traceback
        if isinstance(exception_type, type):
            exception_type = exception_type.__name__
            logger_kwargs["exception_type"] = exception_type
        if tags:
            logger_kwargs["tags"] = tags
        if errors:
            logger_kwargs["errors"] = errors
        self._logger.error(msg, **logger_kwargs)
        # should we really just send all logger.errors to sentry??
        self._send_to_sentry(
            msg,
            exception=exception,
            tags=kwargs.pop("tags", None),
            errors=kwargs.pop("errors", None),
            traceback=traceback,
            exception_type=exception_type,
            **kwargs,
        )

    @with_trace
    def critical(self, msg, priority="p5", exception=None, tags=None, **kwargs):
        self._logger.critical(msg, priority=priority, **kwargs)
        # Critical is called "fatal" in Sentry
        self._send_to_sentry(
            msg,
            exception=exception,
            tags=tags,
            priority=priority,
            level="fatal",
            **kwargs,
        )

    def _send_to_sentry(
        self,
        msg: Optional[str] = None,
        exception: Optional[Union[ExcInfo[Exc], Exc, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        errors: Any = None,
        level: Literal["error", "fatal", "warning"] = "error",
        trace_id: Any = None,
        priority: Optional[str] = None,
        traceback: Optional[Union[TracebackType, str]] = None,
        exception_type: Optional[Union[Type[Exc], str]] = None,
        **extras,
    ):
        # not sure best practice but setting a scope
        if ENVS_EXCLUDE_FROM_SENTRY and self._environment in ENVS_EXCLUDE_FROM_SENTRY:
            self.debug(
                f"Skipping writing error to sentry env '{self._environment}' as per "
                f"configured list '{ENVS_EXCLUDE_FROM_SENTRY}'"
            )
            return

        with sentry_sdk.configure_scope() as scope:  # type: ignore
            if tags:
                for k, v in tags.items():
                    set_tag(k, v)
            set_tag("app_name", self._app_name)
            set_tag("namespace", self._namespace)
            set_tag("cluster_name", self._cluster_name)
            if trace_id:
                set_tag("Res-Trace-Id", trace_id)
            if priority:
                set_tag("priority", priority)
                set_context("priority", {"priority": priority})

            if errors:
                set_context("errors", {"errors": errors})
            if exception and isinstance(exception, tuple):
                exception_type, exception, traceback = exception
            if traceback or exception_type:
                set_context(
                    "exc_info",
                    {
                        "exception_type": repr(exception_type),
                        "exception": repr(exception),
                        "traceback": traceback,
                    },
                )
            if extras:
                try:
                    encoded = orjson.dumps(extras, default=sentry_extras_encoder)
                    logged_kwargs = orjson.loads(encoded)
                except Exception:
                    logged_kwargs = None
                if logged_kwargs:
                    set_context("logged_kwargs", logged_kwargs)

            scope.level = level
            if exception and isinstance(exception, BaseException):
                set_context("message", {"message": msg})

                capture_exception(exception)
            else:
                set_context("exception", {"exception": repr(exception)})

                capture_message(msg or repr(exception))

    @staticmethod
    def configure_logging(level: Union[int, str] = "DEBUG"):
        return structlog.get_logger(ResLogger, level=level)

    def prepend_processor(self, processor):
        processors = structlog.get_config()["processors"]
        structlog.configure(processors=[processor, *processors])
        self._logger = structlog.get_logger(ResLogger, level=self._log_level)

    def reset_processors(self):
        structlog.configure(processors=self._initial_processors)
        self._logger = structlog.get_logger(ResLogger, level=self._log_level)

    @staticmethod
    def _log_level_to_int(level):
        _id = str(uuid.uuid4())
        logger = logging.getLogger(_id)
        logger.setLevel(level)
        rval = logger.level
        logging.Logger.manager.loggerDict.pop(_id)
        return rval

    def send_message_to_slack_channel(self, channel_name, message, **kwrags):
        """
        There are various monitoring slack channels which is often better than pager duty and sentry
        so we might as well build in something to send critical warnings here

        for example sending certain things to `sirsh-test` slack channel if there are some exceptions in backend processes
        """
        import res

        slack = res.connectors.load("slack")
        slack({"slack_channels": [channel_name], "message": message})

        return True
