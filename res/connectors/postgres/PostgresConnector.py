import re
from ast import Dict
from contextlib import contextmanager
from typing import Any, Dict, Optional

import pandas as pd
import psycopg2
import sqlalchemy as sa
from tenacity import retry, stop_after_attempt, wait_fixed

import res.utils
from res.connectors.postgres.utils import (
    build_insert_columns,
    build_insert_values,
    build_set_clause,
    quote_value,
)
from res.utils import secrets


@contextmanager
def connect_to_postgres(conn_string: Optional[str] = None):
    pg_conn = PostgresConnector(
        conn_string=conn_string,
        keep_conn_open=True,
    )
    try:
        yield pg_conn
    finally:
        pg_conn.conn.close()


def PostgresConnection(conn_string: Optional[str] = None):
    """
    This can be use by the dependecy injection of fast api to load a postgres connection
    https://fastapi.tiangolo.com/tutorial/dependencies/dependencies-with-yield/
    """

    def wrapper():
        with connect_to_postgres(conn_string) as conn:
            yield conn

    return wrapper


class PostgresConnector:
    connectionstring: str

    def __init__(
        self,
        rds_server: Optional[str] = None,
        conn_string: Optional[str] = None,
        keep_conn_open=False,
    ):
        self.keep_conn_open = keep_conn_open
        self.rds_server = rds_server
        if conn_string is not None:
            psql_conn_str = conn_string
        else:
            temp = secrets.secrets_client.get_secret(
                "HASURA_SECRET_DATA", load_in_environment=False
            )

            if not temp or type(temp) != dict:
                raise Exception("Unable to load HASURA_SECRET_DATA")

            psql_conn_str = temp["HASURA_GRAPHQL_DATABASE_URL"]

        if rds_server is not None:
            psql_conn_str = re.sub("@[^/]+", "@" + rds_server, psql_conn_str)

        self.connectionstring = psql_conn_str

        # for localhost we tend to intermittently see a blocking connection that never finishes. open_connection_with_retry will retry X times for this after waiting a few seconds
        if self.running_on_localhost():
            self.open_connection_with_retry(self.connectionstring)
        else:
            self.conn = psycopg2.connect(self.connectionstring)

    @retry(wait=wait_fixed(1), stop=stop_after_attempt(4), reraise=True)
    def open_connection_with_retry(self, conn_string):
        self.conn = psycopg2.connect(conn_string, connect_timeout=6)

    def __getattr__(self, attr):
        return getattr(self.conn, attr)

    def running_on_localhost(self):
        return self.rds_server and "localhost" in self.rds_server.lower()

    @retry(wait=wait_fixed(5), stop=stop_after_attempt(2), reraise=True)
    def tenacious_execute_with_kwargs(self, query, **kwargs):
        """
        provide an interface that looks like the graphql one
        """
        return self.execute(query, params=kwargs)

    def execute_with_kwargs(self, query, **kwargs):
        """
        provide an interface that looks like the graphql one
        """
        return self.execute(query, params=kwargs)

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None):
        """hasura Connector version of execute"""
        return self.run_query(query, params)

    def run_query(self, query, data=None, keep_conn_open=False):
        # keep_conn_open = True will mean client needs to manage conn closing. Useful if running multiple queries
        try:
            # define a function that parses the connection's poll() response
            self.reopen_connection()

            cursor = self.conn.cursor()

            if isinstance(query, sa.sql.expression.ClauseElement):
                query = str(query)

            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)

            query_results = cursor.fetchall()

            column_names = [desc[0] for desc in cursor.description or []]

            # Close the cursor and the database connection

            df = pd.DataFrame(query_results, columns=column_names)

            cursor.close()
        finally:
            # to get around our annoying issue where postgres connector hangs on localhost, we are going to try permanently leaving connection open for a localhost debug/excection session, to prevent timeouts
            if not self.running_on_localhost():
                if not keep_conn_open and not self.keep_conn_open:
                    self.conn.close()
        return df

    def reopen_connection(self):
        try:
            self.conn.poll()
        except psycopg2.InterfaceError as error:
            res.utils.logger.debug(f"{error}")
            res.utils.logger.warn("Postgres connection was closed - reopening..")
            self.open_connection_with_retry(self.connectionstring)

    def run_update(self, query, data=None, keep_conn_open=False):
        # keep_conn_open = True will mean client needs to manage conn closing. Useful if running multiple queries
        try:
            df = None
            self.reopen_connection()

            cursor = self.conn.cursor()

            if data:
                cursor.execute(query, data)
            else:
                cursor.execute(query)
            try:
                result = cursor.fetchall()
            except psycopg2.ProgrammingError:

                result = None

            column_names = [desc[0] for desc in cursor.description or []]

            df = pd.DataFrame(result, columns=column_names)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()  # Rollback should be before cursor.close()
            raise e
        finally:
            cursor.close()
            if not self.running_on_localhost():
                if not keep_conn_open and not self.keep_conn_open:
                    self.conn.close()
        return df

    def get_next_entity_id(self, entity_name: str):
        """
        Request the next integer based DB ID for a given entity name. If your entity name does not exist, the DB will create it and start a counter for you. Useful where you need unique, human friendly entity IDs before writing an entity to the DB
        example usage:
        my_id = postgresconnector.get_next_entity_id("my_brand_new_entity")
        if you want to start your ID at something other than 0, you can create an entry in infraestructure.entity_ids for your given entity_name
        """
        id = self.run_update(
            "SELECT infraestructure.get_next_entity_id(%s)", (entity_name,)
        )["get_next_entity_id"][0]
        return id

    #
    def insert_records(self, table: str, records: list[dict], keep_conn_open=False):
        """
        Insert all records into a table
        """
        if len(records) == 0:
            return

        insert_columns = build_insert_columns(records)
        insert_values = build_insert_values(records)
        self.run_update(
            f"INSERT INTO {table} {insert_columns} VALUES {insert_values}",
            keep_conn_open=keep_conn_open,
        )

    def update_records(self, table: str, set_: dict, where: dict, keep_conn_open=False):
        """
        Update all records that match the query
        """
        set_clause = build_set_clause(set_)
        where_clause = " AND ".join(
            [f"{k} = {quote_value(v)}" for k, v in where.items()]
        )
        self.run_update(
            f"UPDATE {table} SET {set_clause} WHERE {where_clause}",
            keep_conn_open=keep_conn_open,
        )
