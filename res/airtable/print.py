from res.utils.airtable import get_table
from functools import lru_cache

PRINT_APP_ID = "apprcULXTWu33KFsh"

PRETREAT_INFO = get_table(
    PRINT_APP_ID,
    "tbllUsoGfM117fcvI",
)

MATERIAL_INFO = get_table(
    PRINT_APP_ID,
    "tblJAhXttUzwxdaw5",
)

ROLLS = get_table(
    PRINT_APP_ID,
    "tblSYU8a71UgbQve4",
)

NESTS = get_table(
    PRINT_APP_ID,
    "tbl7n4mKIXpjMnJ3i",
)

ASSETS = get_table(
    PRINT_APP_ID,
    "tblwDQtDckvHKXO4w",
)

SOLUTIONS = get_table(
    PRINT_APP_ID,
    "tblGcPHb5oXA6crN5",
)

MACHINE_INFO = get_table(
    PRINT_APP_ID,
    "tbl4DbNHQPMwlLrGY",
)

PRINTFILES = get_table(
    PRINT_APP_ID,
    "tblAQcPuKUDVfU7Fx",
)

REQUESTS = get_table(
    PRINT_APP_ID,
    "tblaNDuo1nylMVplQ",
)

COLOR_CHIPS = get_table(
    PRINT_APP_ID,
    "tblGq5CwAaidojHdM",
)

SUBNESTS = get_table(
    PRINT_APP_ID,
    "tblMvfJeCOlauzb8v",
)

@lru_cache(maxsize=1)
def print_material_id_map():
    return {r["fields"]["Key"]: r["id"] for r in MATERIAL_INFO.all(fields=["Key"])}


def print_material_id(material_code):
    return print_material_id_map().get(material_code)
