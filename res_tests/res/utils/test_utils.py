from res.utils import group_by

from typing import Type
from pydantic import BaseModel
import re


def test_group_by():
    KEY = "A"
    elements = [
        "A",
        "B",
        "C",
        "A",
    ]

    data = group_by(lambda x: x, elements)

    assert KEY in data
    assert len(data[KEY]) is 2


def verify_sqlquery_satisfies_pydantic(
    sql_query: str,
    pydantic_type: Type[BaseModel],
    exclude_sql_cols: list = None,
    exclude_pydantic_attribs: list = None,
):
    sqlcols = re.findall(r"%\((\w+)\)s", sql_query)
    pydantic_fields = pydantic_type.__annotations__.keys()

    if exclude_sql_cols:
        sqlcols = [p for p in sqlcols if p not in exclude_sql_cols]

    if exclude_pydantic_attribs:
        pydantic_fields = [p for p in sqlcols if p not in exclude_pydantic_attribs]

    for pydantic_field in pydantic_fields:
        assert (
            pydantic_field in sqlcols
        ), f"Missing this pydantic field in sqlcols: {pydantic_field}"

    for sqlcol in sqlcols:
        assert (
            sqlcol in pydantic_fields
        ), f"Unused sql column in Pydantic model: {sqlcol}"
