import json
from psycopg2.extensions import QuotedString
import sqlalchemy as sa
from typing import Any, List, Union


def get_schemas(conn: sa.engine.Connection) -> List[str]:
    """
    Returns a list of schemas in the database
    """
    return [
        x[0]
        for x in conn.execute(
            sa.text("SELECT schema_name FROM information_schema.schemata")
        ).fetchall()
    ]


def get_tables(conn: sa.engine.Connection, schema: str) -> List[str]:
    """
    Returns a list of tables in the database
    """
    return [
        x[0]
        for x in conn.execute(
            sa.text(
                f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'"
            )
        ).fetchall()
    ]


def get_columns(conn: sa.engine.Connection, schema: str, table: str) -> List[str]:
    """
    Returns a list of columns in the database
    """
    return [
        x[0]
        for x in conn.execute(
            sa.text(
                f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table}'"
            )
        ).fetchall()
    ]


def build_insert_columns(to_insert: List[dict]) -> str:
    """
    Builds the column names for an insert statement
    """
    value_names = f"({', '.join(to_insert[0].keys())})"
    return value_names


def quote_value(value: Any) -> str:
    """
    Returns a quoted value for a postgres query
    """
    if isinstance(value, str):
        return QuotedString(value).getquoted().decode("utf8")
    elif isinstance(value, dict) or isinstance(value, list):
        json_string = json.dumps(value)
        # This fixes an error where the json is interpreted as a
        # bind parameter
        json_string = json_string.replace('":', '": ')
        return f"'{json_string}'"
    elif value is None:
        return "NULL"
    else:
        return QuotedString(str(value)).getquoted().decode("utf8")


def build_insert_values(to_insert: List[dict]) -> str:
    """
    Builds the values for an insert statement
    """
    value_items = []
    for row in to_insert:
        values = [quote_value(value) for value in row.values()]
        value_items.append(f"({', '.join(values)})")

    value_items = ", ".join(value_items)
    return value_items


def build_set_clause(d: dict) -> str:
    """
    Builds the values for an update statement

    Example:
        build_set_clause({"name": "test", "code": "test2"})
        # => "name = 'test', code = 'test2'"
    """
    return ", ".join([f"{k} = {quote_value(v)}" for k, v in d.items()])


def build_json_column(path: Union[str, dict, tuple, list]) -> str:
    """
    Builds a json column path for a where clause e.g.: meta->>'bodies'
    """
    if isinstance(path, str):
        return path
    if isinstance(path, dict):
        k = list(path.keys())[0]
        v = list(path.values())[0]
        return f"{k}->>'{v}'"
    return f"{path[0]}->>'{path[1]}'"
