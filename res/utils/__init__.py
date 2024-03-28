"""Res library utils module."""
from typing import overload
from typing_extensions import Literal

from ._utils import *  # noqa
from . import logging
from . import secrets
from . import env
from . import safe_http
from . import dates
from . import dataframes
from . import resources
from . import schema
from . import dev
from .logging import ResLogger
from .secrets import ResSecretsClient

from warnings import filterwarnings


def numpy_json_dumps(d):
    import json
    import numpy as np

    class NpEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            return super(NpEncoder, self).default(obj)

    return json.dumps(d, cls=NpEncoder)


def ignore_warnings():
    filterwarnings("ignore")


logger: ResLogger
secrets_client: ResSecretsClient

__all__ = [
    "logging",
    "secrets",
    "safe_http",
    "dates",
    "dataframes",
    "resources",
    "schema",
    "env",
    "secrets",
    "dev",
    "get_res_root",
    "flatten_dict",
    "ex_repr",
    "res_hash",
    "hash_of",
    "hash_dict",
    "uuid_str_from_dict",
    "hash_dict_short",
    "post_to_lambda",
    "read",
    "intersection",
    "disjunction",
    "rflatten",
    "delete_from_dict",
    "group_by",
    "slack_exceptions",
    "ping_slack",
    "run_job_async_with_context",
]


@overload
def __getattr__(name: Literal["logger"]) -> ResLogger:
    """Return the default ResLogger."""


@overload
def __getattr__(name: Literal["secrets_client"]) -> ResSecretsClient:
    """Return the default ResSecretsClient."""


def __getattr__(name):
    """Provide logger and secrets_client lazily."""
    if name == "logger":
        return ResLogger.get_default_logger()
    if name == "secrets_client":
        return ResSecretsClient.get_default_secrets_client()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
