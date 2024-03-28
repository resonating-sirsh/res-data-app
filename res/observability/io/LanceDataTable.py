"""
A wrapper around to get to know the interface and do some opinionated stuff

https://github.com/lancedb/lance
https://blog.lancedb.com/
https://lancedb.github.io/lance/read_and_write.html
https://lancedb.github.io/lancedb/basic/#creating-an-empty-table
https://lancedb.github.io/lance/notebooks/quickstart.html

"""


import lance
import lancedb
import pyarrow as pa
import typing
import res
from res.utils import logger
from . import VECTOR_STORE_ROOT_URI
from res.connectors.lancedb.LanceDBConnector import lance_connect


class LanceDataTable:
    def __init__(
        self,
        namespace,
        name,
        space=None,
        schema=None,
        create_if_not_found: bool = False,
    ):
        self._name = name
        self._db_root = f"{VECTOR_STORE_ROOT_URI}/{namespace}"
        self._uri = f"{self._db_root}/{name}.lance"  #
        # the connectors needs to do some security stuff
        self._db = lance_connect(self._db_root)
        self._s3 = res.connectors.load("s3")
        self._duck_client = res.connectors.load("duckdb")

        # Attempt to open a LanceTable using the LanceDBConnection
        try:
            self._table = self._db.open_table(self._name)

        # lancedb.table.open() returns a FileNotFoundError if the table does not
        # exist within the specified database / namespace
        except FileNotFoundError:
            if create_if_not_found:
                logger.warning(
                    f"Table does not exist - creating {self._db_root}/{self._name} from schema {schema}"
                )
                self._table = self.table_from_schema(self._name, schema=schema)

            else:
                logger.error(
                    f"Store {name} with namespace {namespace} not found \
                    and create_if_not_found parameter set to False"
                )

                raise

        except Exception as e:
            logger.error(e)
            raise

    @staticmethod
    def load_dataset(name, namespace="default"):
        uri = f"{VECTOR_STORE_ROOT_URI}/{namespace}/{name}.lance"
        logger.debug(f"loading {uri}")
        return lance.dataset(uri)

    @property
    def name(self):
        return self._name

    @property
    def table(self):
        return self._table

    @property
    def uri(self):
        return self._uri

    @property
    def database_uri(self):
        return self._db_root

    @property
    def dataset(self):
        return lance.dataset(self._uri)

    def __repr__(self):
        return f"LanceDataSet({self._name}): {self._uri}"

    def table_from_schema(self, name, schema, space=None):
        """
        not sure how i want to do this yet but we can create tables from schema
        might be easier to just assume we have some data at some point and then add or upsert to the table
        """

        if not isinstance(schema, pa.Schema):
            # this is because we are assuming it has this e.g. an abstract entity
            schema = schema.pyarrow_schema()
            logger.debug(f"{schema=}")
        return self._db.create_table(name=name, schema=schema)

    def upsert_records(self, records: typing.List[dict], key="id", mode="append"):
        """
        add to the table and remove anything that had the same id
        """
        if len(records):
            keys = set(r[key] for r in records)
            in_list = ",".join([f'"{k}"' for k in keys])
            try:
                self._table.delete(f"{key} IN ({in_list})")
            except:
                logger.warning(
                    f"Failed in a delete transaction - this can lead to duplicates"
                )

            return self._table.add(data=records, mode=mode)
        return self.dataset

    def query_dataset(self, query):
        dataset = lance.dataset(self._uri)
        return self._duck_client.execute(query)

    def load(self, limit=None):
        """
        returns the polars data for the records
        """
        dataset = lance.dataset(self._uri)
        logger.debug(f"Fetching from {self._uri}")
        limit = f"LIMIT {limit}" if limit else ""
        return self._duck_client.execute(f"SELECT * FROM dataset {limit}")
