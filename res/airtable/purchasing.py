from res.utils.airtable import get_table

PURCHASING_APP_ID = "appoaCebaYWsdqB30"

MATERIALS = get_table(
    PURCHASING_APP_ID,
    "tblJOLE1yur3YhF0K",
)
