from res.utils.airtable import get_table

# BASES ID
FULFILLMENT_BASE_ID = "appfaTObyfrmPHvHc"
RES_META_BASE_ID = "appc7VJXjoGsSOjmw"


HOLIDAYS_TABLE = get_table(
    "applaK5MLcPfcgCla",
    "tbl8wU1TVBUnWPMzI",
)

CUT_MARKERS = get_table(
    "appyIrUOJf8KiXD1D",
    "tbld6v3AJUk7IVS4p",
)

PRODUCTION_REQUESTS = get_table(
    "appH5S4hIuz99tAjm",
    "tblptyuWfGEUJWwKk",
    secret_name="RESBOT_GAMMA_PAT",
)

MATERIAL_PROP = get_table(
    "app1FBXxTRoicCW8k",
    "tblD1kPG5jpf6GCQl",
)

ORDER_LINE_ITEMS = get_table(
    FULFILLMENT_BASE_ID,
    "tblUcI0VyLs7070yI",
    secret_name="RESBOT_GAMMA_PAT",
)

ORDERS = get_table(
    "appfaTObyfrmPHvHc",
    "tblhtedTR2AFCpd8A",
    secret_name="RESBOT_GAMMA_PAT",
)

BRANDS = get_table(
    "appc7VJXjoGsSOjmw",
    "tblMnwRUuEvot969I",
)

ORDERS_TABLE = get_table(
    FULFILLMENT_BASE_ID,
    "tblhtedTR2AFCpd8A",
)

FULFILLMENT_TABLE = get_table(
    FULFILLMENT_BASE_ID,
    "tblZ6JDrAtMhxqFnT",
)

FULFILLMENT_BRAND_TABLE = get_table(
    FULFILLMENT_BASE_ID,
    "tblp1sXLJjZl3uGSt",
)

RES_META_BRAND_TABLE = get_table(
    RES_META_BASE_ID,
    "tblMnwRUuEvot969I",
)
