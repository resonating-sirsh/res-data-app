import os, json, sys
from res.utils import logger
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient

# This runs queries that patch bad data

RES_ENV = os.getenv("RES_ENV", "development")
# Update style IDs for latest -- these were corrupted due to material swaps
PATCH_LINE_ITEMS_QUERY = f"""
MERGE INTO IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS
USING (
    WITH 
    latest_styles AS (
        SELECT OBJECT_ID
        FROM (
            SELECT
            OBJECT_ID,
            Row_number() OVER (PARTITION BY "Resonance_code" ORDER BY "Trashed" DESC, "__createdat" DESC) rn
            FROM IAMCURIOUS_{RES_ENV}."AIRTABLE__RES_MAGIC_BY_RESONANCE__CREATE_STYLES_FLOW"
            WHERE __EXISTS_IN_AIRTABLE__ = TRUE
        )
        WHERE rn = 1
    ),
    style_history AS (
        SELECT res_code, record_id
        FROM (
            SELECT
                VARIANT_DATA:fields:"Resonance_code"::varchar as res_code,
                VARIANT_DATA:id::varchar as record_id,
                Row_number() OVER (
                    PARTITION BY VARIANT_DATA:fields:"Resonance_code"::varchar
                    ORDER BY FILE_DATE DESC) rn
            FROM IAMCURIOUS_{RES_ENV}_STAGING.appjmzNPXOuynj6xP_tblmszDBvO1MvJrlJ
            WHERE VARIANT_DATA:id::varchar IN (
                SELECT OBJECT_ID FROM latest_styles
            )
        )
        WHERE rn = 1
    ),
    line_items_matches as (
        SELECT
            items.OBJECT_ID, style_history.record_id
        FROM IAMCURIOUS_DB.IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS items
        JOIN style_history 
        ON items."RESCODE" = style_history.res_code
        WHERE items."styleId" IS NULL
    )
    SELECT object_id, record_id
    FROM line_items_matches
    GROUP BY 1,2
) AS line_items_matches
ON IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS.OBJECT_ID = line_items_matches.object_id
WHEN MATCHED THEN
UPDATE SET AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS."styleId" = line_items_matches.record_id;
"""

# There is some issue with a bot that forgets to update the lineitem_id in this table
# Here we find the correct ID from the orders table and update
PATCH_LINE_ITEM_IDS = f"""
MERGE INTO IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS
USING (
    WITH latest_fulfillment_orders AS (
        SELECT * FROM (
            SELECT
                Row_number() OVER (PARTITION BY variant_data:"fields":"ORDER_ID" ORDER BY variant_data:"fields":"created_at" DESC) rn,
                variant_data:"fields":"ORDER_ID" AS id,
                variant_data:"fields":"lineItemsJson" AS line_items_json,
                variant_data:"fields":"created_at" as created_at
            FROM IAMCURIOUS_{RES_ENV}_STAGING.appfaTObyfrmPHvHc_tblhtedTR2AFCpd8A
        ) WHERE rn = 1
    ),
    parsed_data as (
        SELECT
            created_at,
            value:"id"::varchar as record_id,
            value:"channelOrderLineItemId"::varchar as lineitem_id
        FROM latest_fulfillment_orders
        , lateral flatten(input => try_parse_json(line_items_json))
    )
    -- some bug in snowflake requires this ignore column to get everything to match properly
    SELECT concat('_',record_id) as ignore,* FROM (
        SELECT
            Row_number() OVER (PARTITION BY record_id ORDER BY created_at DESC) rn,
            record_id::varchar record_id,
            lineitem_id
        FROM parsed_data
    ) where rn = 1
) AS line_items_matches
ON IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS.OBJECT_ID = line_items_matches.record_id
WHEN MATCHED THEN
UPDATE SET AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS."lineitem_id" = line_items_matches.lineitem_id;
"""

PATCH_LINE_ITEMS_BASED_ON_SKU = f"""
MERGE INTO IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS
USING (
    
    with missing_items as (
        select sku,order_id,id as line_item_id
        from IAMCURIOUS_{RES_ENV}.shopify_order_line_items
        where id not in (select "lineitem_id" from IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS where "lineitem_id" is not null)
    ),
    existing_fulfillment_items as (
        select sku,order_id,object_id
        from IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS
        where "lineitem_id" is null
        and order_id is not null
    )
    select object_id, line_item_id from
    missing_items
    join existing_fulfillment_items
    on missing_items.order_id::string = existing_fulfillment_items.order_id
    and missing_items.sku = existing_fulfillment_items.sku
) AS line_items_matches
ON IAMCURIOUS_{RES_ENV}.AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS.OBJECT_ID = line_items_matches.object_id
WHEN MATCHED THEN
UPDATE SET AIRTABLE__RES_MAGIC_FULFILLMENT__ORDER_LINE_ITEMS."lineitem_id" = line_items_matches.line_item_id;
"""

# A manual archive was applied around 2/19/22, where the print base requests table (and others) was archived but
# not corresponding rows in the print assets table... so the link to requests was broken.
PATCH_BAD_PRINT_ASSETS_ARCHIVE = f"""
MERGE INTO IAMCURIOUS_{RES_ENV}.airtable__res_magic_print__print_assets_flow
USING (
    SELECT
        variant_data:id::varchar AS record_id,
        max(variant_data:fields:"__order_number"::varchar) AS order_number,
        max(get(to_array(variant_data:fields:"Request"), 0)::varchar) AS request
    FROM iamcurious_{RES_ENV}_staging.apprcULXTWu33KFsh_tblwDQtDckvHKXO4w 
    WHERE file_date >= '2022_02_16__00_00_00' AND file_date < '2022_02_23__00_00_00'
    AND variant_data:fields:"__order_number"::varchar IS NOT NULL
    GROUP BY 1
) AS matches
ON IAMCURIOUS_{RES_ENV}.airtable__res_magic_print__print_assets_flow.OBJECT_ID = matches.record_id
WHEN MATCHED THEN
UPDATE SET airtable__res_magic_print__print_assets_flow."__order_number" = matches.order_number,
airtable__res_magic_print__print_assets_flow."Request" = matches.request
; """
PATCH_BAD_ROLLS_ARCHIVE = f"""
MERGE INTO IAMCURIOUS_{RES_ENV}.airtable__res_magic_print__rolls
USING (
    SELECT
        variant_data:id::varchar AS record_id,
        max(variant_data:fields:"Nested Asset Sets"::varchar) AS nested_asset_sets,
        max(variant_data:fields:"Total Nested Length"::float) as total_nested_length
    FROM iamcurious_{RES_ENV}_staging.apprcULXTWu33KFsh_tblSYU8a71UgbQve4 
    WHERE file_date >= '2022_02_16__00_00_00' AND file_date < '2022_02_23__00_00_00'
    AND variant_data:fields:"Nested Asset Sets"::varchar IS NOT NULL
    GROUP BY 1
) AS matches
ON IAMCURIOUS_{RES_ENV}.airtable__res_magic_print__rolls.OBJECT_ID = matches.record_id
WHEN MATCHED THEN
UPDATE SET airtable__res_magic_print__rolls."Nested Asset Sets" = matches.nested_asset_sets,
airtable__res_magic_print__rolls."Total Nested Length" = matches.total_nested_length"""


if __name__ == "__main__":
    try:
        event = json.loads(sys.argv[1])
        # Only need to run this if Order Line Items has been updated
        if (
            "base" not in event
            or (event["base"] == "appjmzNPXOuynj6xP" and "table" not in event)
            or (
                event["base"] == "appjmzNPXOuynj6xP"
                and event["table"] == "tblmszDBvO1MvJrlJ"
            )
        ):
            snow_client = ResSnowflakeClient(schema=f"IAMCURIOUS_{RES_ENV}_STAGING")
            logger.info("Running patch line items...")
            snow_client.execute(PATCH_LINE_ITEMS_QUERY)
            logger.info("Running patch line item IDs...")
            snow_client.execute(PATCH_LINE_ITEM_IDS)
            logger.info("Running patch line item IDs based on SKUs...")
            snow_client.execute(PATCH_LINE_ITEMS_BASED_ON_SKU)
            logger.info("Running patch bad print assets archive...")
            snow_client.execute(PATCH_BAD_PRINT_ASSETS_ARCHIVE)
            logger.info("Running patch bad rolls archive...")
            snow_client.execute(PATCH_BAD_ROLLS_ARCHIVE)
            logger.info("Done!")
    except Exception as e:
        logger.error("Error running line items patch query in Snowflake!")
        raise
