from res.utils.airtable import get_table
from functools import lru_cache

HEALING_APP_ID = "appWxYpvDIs8JGzzr"

REQUESTS = get_table(
    HEALING_APP_ID,
    "tblgVQe404mNwsTDs",
)

ONES = get_table(
    HEALING_APP_ID,
    "tblbu7EUKZ6DEwHz9",
)

REQUEST_PIECES = get_table(
    HEALING_APP_ID,
    "tblbRvw7OzX1929Qi",
)

PIECES_WITH_ZONES = get_table(
    HEALING_APP_ID,
    "tblJgBId024pDSwBH",
)

ZONE_X_DEFECT = get_table(
    HEALING_APP_ID,
    "tbl9WS30YRA6m2xSu",
)

HEALING_ROLLS = get_table(
    HEALING_APP_ID,
    "tblG5x0hWNr3YQ0ge",
)

HEALING_NESTS = get_table(
    HEALING_APP_ID,
    "tblFkEL9RvNHXlnea",
)

HEALING_NESTED_PIECES = get_table(
    HEALING_APP_ID,
    "tbl9yZ27vHIb1t9XV",
)

# aka defects....
HEALING_ACTIVE_TAGS = get_table(
    HEALING_APP_ID,
    "tblFVgsGr6nC20equ",
)


@lru_cache(maxsize=1)
def get_defect_id_to_name_map():
    return {
        r["id"]: r["fields"]["Name"] for r in HEALING_ACTIVE_TAGS.all(fields=["Name"])
    }
