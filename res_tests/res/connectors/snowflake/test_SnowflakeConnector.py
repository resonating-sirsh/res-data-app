import pytest


@pytest.mark.service
def test_get_table_ddl_default_connect_usings():
    from res.connectors.snowflake import SnowflakeConnector

    snow = SnowflakeConnector(db="IAMCURIOUS_DB")
    script = snow._get_table_creation_script("Rolls")
    expected_start = """create or replace TABLE "Rolls" ("""
    assert (
        script[: len(expected_start)] == expected_start
    ), f"The correct script could not be loaded - got {script} "


@pytest.mark.service
def test_get_table_schema_info_default_connect_usings():
    from res.connectors.snowflake import SnowflakeConnector

    snow = SnowflakeConnector(db="IAMCURIOUS_DB")
    script = snow._get_table_creation_script("Rolls")
    expected_start = """create or replace TABLE "Rolls" ("""
    assert (
        script[: len(expected_start)] == expected_start
    ), f"The correct script could not be loaded - got {script} "
