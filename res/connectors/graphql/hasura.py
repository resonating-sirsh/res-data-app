"""Resonance connector for Hasura."""

from enum import Enum
import os
from typing import Any, AsyncGenerator, Callable, Dict, Optional, Union
from gql import gql, Client as _GqlClient
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.websockets import WebsocketsTransport
from graphql import DocumentNode
from tenacity import retry, wait_fixed, stop_after_attempt

from res.utils import logger, secrets_client

__all__ = ["Client"]

Query = Union[str, DocumentNode]

RES_ENV = os.environ.get("RES_ENV")

env_slug = "" if RES_ENV == "production" else "-dev"

DEFAULT_ENDPOINT = f"https://hasura{env_slug}.resmagic.io"

EMPTY_API_KEY = object()


def default_endpoint(env=None):
    env = env or os.environ.get("RES_ENV")
    env_slug = "" if env == "production" else "-dev"
    logger.debug(f"Using hasura environment {env}")
    return f"https://hasura{env_slug}.resmagic.io"


def normalize_query(query: Query) -> DocumentNode:
    """Pass query to gql if its not already a graphql.language.ast.DocumentNode."""
    return query if isinstance(query, DocumentNode) else gql(query)


class Client:
    """
    GraphQL client for Hasura.

    Intended to enable easy use of Hasura queries and subscriptions.
    """

    def __init__(
        self, *, api_url=None, api_key=EMPTY_API_KEY, use_env=None, timeout=10
    ):
        if use_env:
            _domain = default_endpoint(use_env)
        else:
            _domain = api_url or os.getenv("HASURA_ENDPOINT") or default_endpoint()
        self._timeout = timeout

        self.custom_headers = {}

        self.api_url = (
            _domain + "/v1/graphql" if not _domain.endswith("/v1/graphql") else _domain
        )

        self.wss_url = self.api_url.replace("https", "wss")

        if api_key is not EMPTY_API_KEY:
            self._api_key = api_key or ""
        elif "localhost" in _domain:
            self._api_key = ""
        else:
            self._api_key = secrets_client.get_secret("HASURA_API_SECRET_KEY")
            if not self._api_key:
                raise Exception("No Hasura API Key provided!")

        logger.debug(f"Configured Hasura Connector for {self.api_url}")

    def _get_headers(self):
        return {
            "content-type": "application/json",
            "x-hasura-admin-secret": self._api_key,
            **self.custom_headers,
        }

    def _make_client(self):
        return _GqlClient(
            fetch_schema_from_transport=False,
            transport=AIOHTTPTransport(
                url=self.api_url,
                headers=self._get_headers(),
            ),
            execute_timeout=self._timeout,
        )

    def _make_sync_client(self):
        return _GqlClient(
            transport=RequestsHTTPTransport(
                url=self.api_url,
                headers=self._get_headers(),
                verify=True,
                retries=5,
            ),
            fetch_schema_from_transport=False,
        )

    def _make_subscription_client(self):
        wss_url = self.api_url.replace("https", "wss")
        return _GqlClient(
            fetch_schema_from_transport=False,
            transport=WebsocketsTransport(
                url=wss_url,
                headers=self._get_headers(),
            ),
        )

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def tenacious_execute_with_kwargs(self, query, **kwargs):
        """
        provide an interface that looks like the graphql one
        """
        return self.execute(query, params=kwargs)

    # SA remove this - we need a retry but this is probably to low level - will add another one with tenacity above
    # note we need a stop attempt ^ - i had code hanging for ages in multiple places that confused me greatly until i keyboard interrupted and saw this
    # @retry(wait=wait_fixed(5))
    def execute_with_kwargs(self, query, **kwargs):
        """
        provide an interface that looks like the graphql one
        """
        return self.execute(query, params=kwargs)

    def execute(self, query: Query, params: Optional[Dict[str, Any]] = None):
        """
        Execute a synchronous query or mutation against Hasura.

        Throws TransportQueryError for most server-side errors
        (e.g. invalid data)
        """
        client = self._make_sync_client()
        return client.execute(normalize_query(query), params or {})

    async def execute_async(
        self, query: Query, params: Optional[Dict[str, Any]] = None
    ):
        """
        Execute an async query or mutation against Hasura.

        Throws TransportQueryError for most server-side errors
        (e.g. invalid data)
        """
        client = self._make_client()
        query_doc = normalize_query(query)
        return await client.execute_async(query_doc, params or {})

    async def subscribe(self, query: Query, params, *, result_handler: Callable):
        """
        Subscribe to Hasura.

        kwargs:
        result_handler: executes when updates are detected in Hasura
        """
        logger.info(f"Subscribing to {self.wss_url}")
        async for result in self.subscribe_gen(query, params):
            await result_handler(result)

    async def subscribe_gen(self, query: Query, params: Dict[str, Any]):
        """Subscribe to Hasura and get a generator."""
        # pylint: disable=not-an-iterable
        logger.info(f"Subscribing to {self.wss_url}")
        client = self._make_subscription_client()
        query_doc = normalize_query(query)
        gen: AsyncGenerator[Dict[str, Any], None] = client.subscribe_async(
            query_doc, params
        )
        async for result in gen:
            yield result


class ChangeDataTypes(Enum):
    CREATE = "CREATE"
    UPDATE = "UPDATE"
    MANUAL = "MANUAL"
