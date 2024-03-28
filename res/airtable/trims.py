from res.utils.airtable import get_table

TRIMS_BASE_ID = "appd9qqPELi5zOBrx"

TRIMS = get_table(
    TRIMS_BASE_ID,
    "tbl4eujDZmnfdIuNr",
)

TRIM_TAXONOMY = get_table(
    TRIMS_BASE_ID,
    "tblWcDuANqJnlXOCu",
)

TRIM_COLOR_MANAGEMENT = get_table(
    TRIMS_BASE_ID,
    "tbl0rSBcTbgV5Qfqy",
)

TRIM_BRAND = get_table(
    TRIMS_BASE_ID,
    "tbl7s8M2oukG7QOBS",
)

TRIM_VENDOR = get_table(
    TRIMS_BASE_ID,
    "tblk01jEFRNiqZwAl",
)

BIN_LOCATION = get_table(
    TRIMS_BASE_ID,
    "tblqQBjAvHPKo3px5",
)

TRIM_SIZE = get_table(
    TRIMS_BASE_ID,
    "tbl0RVcPpNzABP8M5",
)

BILL_OF_MATERIALS = get_table("appa7Sw0ML47cA8D1", "tblnnt3vhPmPuBxAF")
