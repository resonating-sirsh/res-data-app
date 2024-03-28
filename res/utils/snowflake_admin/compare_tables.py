from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient

new_client = ResSnowflakeClient(schema="IAMCURIOUS_DEVELOPMENT")
old_client = ResSnowflakeClient(schema="IAMCURIOUS_SCHEMA")

new_table = "AIRTABLE___RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS"
old_table = "resMagicFulfillment_Order_Line_Items"

new_cols = [row[2] for row in new_client.execute(f"SHOW COLUMNS IN TABLE {new_table}")]
old_cols = [
    row[2] for row in old_client.execute(f'SHOW COLUMNS IN TABLE "{old_table}"')
]

cols_to_ignore = ["__exists_in_airtable__", "__record_id__", "__FILE_DATE__"]

# Look for cols in old but not in new
for old_col in old_cols:
    if old_col not in new_cols:
        print(old_col)
