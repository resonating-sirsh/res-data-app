from res.utils.airtable import get_table

SCHEDULE_APP_ID = "appHqaUQOkRD8VbzX"

SCHEDULE_PPU_CREATION = get_table(
    SCHEDULE_APP_ID,
    "tblsQ6YiweQ5oPiak",
)

SCHEDULE_PPU_CONSTRUCTION_ORDER = get_table(
    SCHEDULE_APP_ID,
    "tblgUIevlJMrP28VT",
)

SCHEDULE_PPU_SCHEDULE = get_table(
    SCHEDULE_APP_ID,
    "tblNG5tFbNppt2fFL",
)

SCHEDULE_CUT_SCHEDULE = get_table(
    SCHEDULE_APP_ID,
    "tblT0URaDyJNeQjwS",
)

SCHEDULE_MATRIX = get_table(
    SCHEDULE_APP_ID,
    "tblqKSvjCSVLtlXxy",
)

SCHEDULE_HEADERS_FOOTERS = get_table(
    SCHEDULE_APP_ID,
    "tblsqbZxOQfM5Y3QS",
)

SCHEDULE_LEADERS = get_table(
    SCHEDULE_APP_ID,
    "tbl51AOLZayNHS1Na",
)

SCHEDULE_LOCATIONS = get_table(
    SCHEDULE_APP_ID,
    "tblVrFqdzZJTkCVNf",
)

SCHEDULE_WAREHOUSE = get_table(
    SCHEDULE_APP_ID,
    "tbl2fcpiVqsInaguB",
)

SCHEDULE_ASSETS = get_table(
    SCHEDULE_APP_ID,
    "tblW2CrGVcZzU86Hi",
)

SCHEDULE_MATERIALS = get_table(
    SCHEDULE_APP_ID,
    "tblYLmTsQlWNoOxBg",
)

SCHEDULE_PRINTER_IDS = [
    ("luna", "recT81pAYXiEc02L1"),
    ("flash", "recykOsUpSXTVy3VH"),
    ("nova", "rec5htHZ1fHcMr5pg"),
]
