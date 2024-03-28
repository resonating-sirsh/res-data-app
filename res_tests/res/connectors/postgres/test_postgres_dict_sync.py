from res.connectors.postgres.PostgresDictSync import PostgresDictSync
import pandas as pd


def test_additions(mocker):
    class PostgresConnectorMock:
        def __init__(self):
            self.update_called = False

        def run_query(self, query):
            return pd.DataFrame()

        def run_update(self, query):
            self.update_called = True
            assert (
                query
                == "INSERT INTO target_table (id, name) VALUES ('1', 'test1'), ('2', 'test2')"
            )

    pg = PostgresConnectorMock()

    PostgresDictSync(
        "target_table",
        [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
        on="id",
        pg=pg,
    ).sync()

    assert pg.update_called


def test_deletions(mocker):
    class PostgresConnectorMock:
        def __init__(self):
            self.update_called = False

        def run_query(self, query):
            return pd.DataFrame(
                [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}]
            )

        def run_update(self, query):
            self.update_called = True
            assert query == "DELETE FROM target_table WHERE id IN ('2')"

    pg = PostgresConnectorMock()

    PostgresDictSync(
        "target_table",
        [{"id": 1, "name": "test1"}],
        on="id",
        pg=pg,
    ).sync()

    assert pg.update_called


def test_updates(mocker):
    class PostgresConnectorMock:
        def __init__(self):
            self.update_called = False

        def run_query(self, query):
            return pd.DataFrame(
                [{"id": 1, "name": "test1"}, {"id": 2, "name": "test2"}],
            )

        def run_update(self, query):
            self.update_called = True
            assert (
                query
                == "UPDATE target_table SET id = '2', name = 'test' WHERE id = '2'"
            )

    pg = PostgresConnectorMock()
    PostgresDictSync(
        "target_table",
        [{"id": 1, "name": "test1"}, {"id": 2, "name": "test"}],
        on="id",
        pg=pg,
    ).sync()
    assert pg.update_called
