"""Lazy-load and instantiate the logger client."""
from . import ResLogger as ResLoggerModule

ResLogger = ResLoggerModule.ResLogger

# pylint: disable=undefined-all-variable
__all__ = [
    "ResLogger",  # type: ignore
    "logger",  # type: ignore
]


logger: ResLogger


def __getattr__(name) -> ResLogger:
    """Provide the logger lazily."""
    if name == "logger":
        return ResLogger.get_default_logger()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
