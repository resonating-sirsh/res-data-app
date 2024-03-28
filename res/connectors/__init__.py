import os

try:
    from ..utils import logger, env
    from res.utils import secrets

    alist = [DatabaseConnector, DatabaseConnectorSchema, DatabaseConnectorTable, env]

except:
    pass
from .DatabaseConnector import (
    DatabaseConnector,
    DatabaseConnectorSchema,
    DatabaseConnectorTable,
)


# test singleton
singletons = {}


class _loader(object):
    def __init__(self, **kwargs):
        self._options = kwargs

    def __getitem__(self, key):
        try:
            # we could do this dynamically but for now just be explicit
            # i think metaclasses can avoid this mess

            if key == "argo":
                from .argo_tools import ArgoConnector

                return ArgoConnector.ArgoWorkflowsConnector()

            if key == "airtable":
                from .airtable import AirtableConnector

                for key in ["AIRTABLE_API_KEY"]:
                    if key not in os.environ:
                        secrets.secrets_client.get_secret(key)

                return AirtableConnector(**self._options)

            if key == "kafka":
                from .kafka import KafkaConnector

                return KafkaConnector(**self._options)

            if key == "mongo":
                from .mongo import MongoConnector

                for key in ["MONGODB_USER", "MONGODB_PASSWORD"]:
                    if key not in os.environ:
                        secrets.secrets_client.get_secret(key)

                return MongoConnector()

            if key == "looker":
                from res.connectors.looker import ResLookerClient

                return ResLookerClient()

            if key == "dgraph":
                from .dgraph import DgraphConnector

                return DgraphConnector()

            if key == "druid":
                from .druid import DruidConnector

                return DruidConnector()

            if key == "dynamo":
                from .dynamo import DynamoConnector

                return DynamoConnector()

            if key == "glue":
                from .glue import GlueConnector

                return GlueConnector()

            if key == "redis":
                from .redis import RedisConnector

                return RedisConnector()

            if key == "pinot":
                from .pinot import PinotConnector

                return PinotConnector()

            if key == "snowflake":
                from .snowflake import SnowflakeConnector

                # for key in ["SNOWFLAKE_PRIVATE_KEY"]:
                #     if key not in os.environ:
                #         secrets.secrets_client.get_secret(key)

                for key in [
                    "SNOWFLAKE_USER",
                    "SNOWFLAKE_PASSWORD",
                    "SNOWFLAKE_PRIVATE_KEY",
                ]:
                    if key not in os.environ:
                        secrets.secrets_client.get_secret(key)

                # TODO - it might not be SNOWFLAKE but some sort of ENV like CTX_ARGO__ and
                # SECRET_CONTEXT = ""
                # PREFIX = f"{SECRET_CONTEXT}SNOWFLAKE"
                # for key in secrets.secrets_client.get_secrets_by_prefix(PREFIX):
                #     if key not in os.environ:
                #         secrets.secrets_client.get_secret(key)

                return SnowflakeConnector(**self._options)

            if key == "s3":
                from .s3 import S3Connector

                return S3Connector()

            if key == "box":
                from .box import BoxConnector

                return BoxConnector()

            if key == "quickbooks":
                from .quickbooks.QuickbooksConnector import QuickbooksConnector

                return QuickbooksConnector()

            if key in ["presto", "trino"]:
                from .presto import PrestoConnector

                return PrestoConnector()

            if key == "graphql":
                from .graphql.ResGraphQLClient import ResGraphQLClient

                for secret in ["GRAPH_API_KEY"]:
                    if secret not in os.environ:
                        secrets.secrets_client.get_secret(secret)

                return ResGraphQLClient()

            if key == "hasura":
                from .graphql.hasura import Client
                from res.connectors import singletons

                hasura_singleton = singletons.get(key)
                if hasura_singleton is None:
                    singletons[key] = hasura_singleton = Client(**self._options)
                return hasura_singleton

            if key == "email":
                from .email import ResEmailClient

                return ResEmailClient()

            if key == "slack":
                from .slack.SlackConnector import SlackConnector

                return SlackConnector()

            if key == "shortcut":
                from .shortcut.ShortcutConnector import ShortcutConnector

                return ShortcutConnector()

            if key == "shopify":
                from .shopify.ShopifyConnector import ShopifyConnector

                return ShopifyConnector()

            if key == "weaviate":
                from .weaviate.WeaviateConnector import WeaviateConnectorClient

                return WeaviateConnectorClient()

            if key == "duckdb":
                from .duckdb.DuckDbConnector import DuckDBClient

                return DuckDBClient(**self._options)

            if key == "lancedb":
                from .lancedb import LanceDBConnector

                return LanceDBConnector()

            if key == "postgres":
                from .postgres.PostgresConnector import PostgresConnector

                return PostgresConnector(
                    rds_server=self._options.get(
                        "rds_server", os.environ.get("RDS_SERVER")
                    ),
                    conn_string=self._options.get("conn_string"),
                )

            if key == "coda":
                from .coda import CodaConnector

                return CodaConnector()

            if key == "neo4j":
                from .neo4j import Neo4jConnector

                return Neo4jConnector()

        except Exception as ex:
            # logger.debug(f"Unable to load service {key} - because {ex}")
            print(ex, "failing to load service")
            raise ex


def load(name, **kwargs):
    return _loader(**kwargs)[name]


try:
    # for backwards compatability silently try to load all connectors
    pass
except:
    pass
