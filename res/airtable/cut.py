from res.utils.airtable import get_table
from functools import lru_cache

CUT_APP_ID = "appyIrUOJf8KiXD1D"

REQUESTS = get_table(
    CUT_APP_ID,
    "tblwIFbHo4PsZbDgz",
)

ONES = get_table(
    CUT_APP_ID,
    "tbld6v3AJUk7IVS4p",
)

ROLLS = get_table(
    CUT_APP_ID,
    "tblXHLe3lm4zGvfZX",
)

EXPECTED_TIMING = get_table(
    CUT_APP_ID,
    "tblMD1ILg15v4Eoqe",
)

PIECES = get_table(
    CUT_APP_ID,
    "tblAqWxhkV30UFTUW",
)

SCHEDULING_VARIABLES = get_table(
    CUT_APP_ID,
    "tblH6NHgXH4FK8Jbj",
)

MATERIALS_PRINTED_PIECES = get_table(
    CUT_APP_ID,
    "tblGjdxDmKZHoT48p",
)

LOCKED_SETTINGS_REFRAME = get_table(
    CUT_APP_ID,
    "tbl7OBfUoT9DNcZRe",
)
CUT_NODE_RESOURCES = get_table(
    CUT_APP_ID,
    "tblDdqL0Dt0ypJwj4",
)
