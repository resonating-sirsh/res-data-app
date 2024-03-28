from res.connectors.postgres.PostgresConnector import connect_to_postgres
from pytest_mock import MockerFixture
import pytest


class TestPostgresConnector:
    """
    Test the PostgresConnector class
    """

    def test_connect_to_postgres(self):
        """
        Test that we can connect to postgres using a context manager
        """

        with connect_to_postgres(
            # conn_string="postgres://postgres:postgrespassword@localhost:5432/postgres" # use this if you want to run the test without mocking
        ) as pg_conn:
            df = pg_conn.execute(
                """SELECT * FROM unnest(ARRAY['value1', 'value2', 'value3']) AS my_column;"""
            )

        assert df is not None

    def test_conenect_to_postgres_and_keep_connection_alive(self):
        with connect_to_postgres(
            # conn_string="postgres://postgres:postgrespassword@localhost:5432/postgres",
        ) as pg_conn:
            df = pg_conn.execute(
                """SELECT * FROM unnest(ARRAY['value1', 'value2', 'value3']) AS my_column;"""
            )

            assert df is not None
            assert pg_conn.conn.closed == 0

            df = pg_conn.execute(
                """SELECT * FROM unnest(ARRAY['value1', 'value2', 'value3', 'value4']) AS my_column_2;"""
            )

            assert df is not None
            assert pg_conn.conn.closed == 0

        assert pg_conn.conn.closed == 1

    @pytest.fixture(autouse=True)
    def before_each(self, mocker: MockerFixture):
        """
        Mock psycopg2 so we don't actually connect to postgres

        Mock the psycopg2.connection object

        method use:
            close
            poll
            cursor
            commit
        property use:
            closed

        Mock the psycopg2.cursor object

        method use
            execute
            fetchall

        properties use:
            description

        """

        cursor_object = mocker.MagicMock()
        cursor_object.execute = mocker.MagicMock()
        cursor_object.fetchall = mocker.MagicMock()
        connection_object = mocker.MagicMock()
        connection_object.closed = 0

        def close():
            connection_object.closed = 1

        connection_object.close = close
        connection_object.poll = mocker.MagicMock()
        connection_object.cursor = mocker.MagicMock()
        connection_object.cursor.return_value = cursor_object
        connection_object.commit = mocker.MagicMock()

        self.connection_object = connection_object
        self.cursor_object = cursor_object

        mocker.patch(
            "res.connectors.postgres.PostgresConnector.psycopg2.connect",
            return_value=connection_object,
        )

        mocker.patch(
            "res.connectors.postgres.PostgresConnector.secrets.secrets_client.get_secret",
            return_value={
                "HASURA_GRAPHQL_DATABASE_URL": "postgres://postgres:postgrespassword@localhost:5432/postgres"
            },
        )
