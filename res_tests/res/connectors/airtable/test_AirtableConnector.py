from res.connectors.airtable.AirtableConnector import Airtable
import pytest
import res
import pandas as pd

TEST_BASE = "appaaqq2FgooqpAnQ"
TEST_TABLE = "tblt9jo5pmdo0htu3"
TEST_WEBHOOK_ID = ""

# TODO - create a test data tier or stubs - currently we are using a test table in a live system
# TODO: many of the below are just stubs to make sure that we can call functions basically
# TODO: use markers to avoid running slow tests in CI/CD


@pytest.mark.service
def test_get_table():
    srv = res.connectors.airtable.AirtableConnector()
    base = srv[TEST_BASE]
    table = base[TEST_TABLE]

    assert (
        table._table_id == TEST_TABLE
    ), "the table was not loaded correctly from the connector"

    return table


@pytest.mark.service
def test_get_table_dataframe():
    table = test_get_table()

    df = table.to_dataframe(fields=["UnitKey", "DxaNodeId", "Flag"])

    assert len(df) > 0, "unable to load data for the test table"


@pytest.mark.service
def test_get_diff_dataframe():
    # TODO rethink this test
    assert 1 > 0, "False"


def test_recreate_from_existing():
    # TODO rethink this test
    assert 1 > 0, "False"


def test_get_changes():
    # TODO rethink this test
    assert 1 > 0, "False"


def test_invalidate_webhook():
    # TODO rethink this test
    assert 1 > 0, "False"


@pytest.mark.slow
def test_load_fields_metadata():
    # TODO rethink this test
    assert 1 > 0, "False"


@pytest.mark.slow
@pytest.mark.data
def test_table_parse():
    assert True, "not true"
