"""Lazy-load and instantiate the secrets client."""
from __future__ import annotations

from . import ResSecretsClient as ResSecretsClientModule

ResSecretsClient = ResSecretsClientModule.ResSecretsClient

# pylint: disable=undefined-all-variable
__all__ = [
    "ResSecretsClient",
    "secrets",  # type: ignore
    "secrets_client",  # type: ignore
]

secrets: ResSecretsClient
secrets_client: ResSecretsClient


def __getattr__(name) -> ResSecretsClient:
    """Provide the secrets client lazily."""
    if name in ["secrets", "secrets_client"]:
        return ResSecretsClient.get_default_secrets_client()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
