import pytest
import res
import pandas as pd


def test_duck():
    import duckdb
    import pandas as pd

    cursor = duckdb.connect()
    Q = """
    INSTALL httpfs;
    LOAD httpfs
    """
    df = cursor.execute(Q)
    assert 1 == 1, "nope"
