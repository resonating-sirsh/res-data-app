from .. import DatabaseConnector, DatabaseConnectorTable, DatabaseConnectorSchema
import pandas as pd
from trino.dbapi import connect
import res


class PrestoConnector(DatabaseConnector):
    def __init__(self):
        self._catalog = "hive"
        self._schema = "default"

    def get_client(self):
        """ """
        # TODO set up presto env
        res.utils.logger.debug(f"Connecting with {self._catalog}.{self._schema}")
        conn = connect(
            host="localhost",
            port=18080,
            user="the-user",
            catalog=self._catalog,
            schema=self._schema,
        )

        return conn

    @property
    def catalogs(self):
        return self.execute("SHOW CATALOGS")

    @property
    def schemas(self):
        return self.execute("SHOW SCHEMAS")

    @property
    def tables(self):
        return self.execute("SHOW TABLES")

    @property
    def get_tables_in_schema(self, schema):
        return self.execute(f"SHOW TABLES in {schema}")

    def execute(self, query):
        conn = self.get_client()
        cur = conn.cursor()
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=[t[0] for t in cur.description])

    def __getitem__(self, catalog):
        return PrestoConnectorSchema(catalog)


class PrestoConnectorSchema(DatabaseConnectorSchema, PrestoConnector):
    def __init__(self, catalog):
        self._catalog = catalog
        self._schema = "default"

    def __getitem__(self, schema):
        return PrestoConnectorTable(self._catalog, schema)


class PrestoConnectorTable(DatabaseConnectorTable, PrestoConnector):
    def __init__(self, catalog, schema):
        """
        Constructor sets up the session for a particular account and endpoint
        """
        self._catalog = catalog
        self._schema = schema
