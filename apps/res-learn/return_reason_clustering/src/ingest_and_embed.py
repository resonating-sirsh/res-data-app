import os
import sys
from datetime import datetime

import pandas as pd
import snowflake.connector
from helper_functions import (
    add_p_key_and_sync_time,
    get_latest_snowflake_timestamp,
    to_snowflake,
)

from res.observability.entity import ReturnReasonsVectorStoreEntry
from res.observability.io import VectorDataStore
from res.utils import logger, secrets_client

RETURNS_QUERY = """
        select
        
            return_lines.source_return_line_item_id,            
            nullif(nullif(return_lines.return_reason_notes, ''), ' ') as return_reason_notes,
            order_lines.ordered_at,
            insert(order_lines.body_full_code, 3, 0, '-') as body_code,
            order_lines.brand_code,
            order_lines.brand_name,    
            order_lines.fulfillment_completed_at,  
            order_lines.style_code,
            order_lines.size as size_code      

        from iamcurious_db.iamcurious_production_models.fact_latest_return_line_items as return_lines
        left join iamcurious_db.iamcurious_production_models.fact_latest_returns as returns
            on return_lines.source_return_id = returns.source_return_id
        left join iamcurious_db.iamcurious_production_models.fact_latest_order_line_items as order_lines
            on order_lines.external_order_line_item_id = return_lines.external_order_line_item_id
        where return_lines.return_reason_notes is not null      
    """

# Retrieve args
SYNC_TYPE = sys.argv[1].lower()
DEV_OVERRIDE = (sys.argv[2]).lower() == "true"
# Checks if the python file name (retrieved programmatically given known length
# of the flow_name input relative path) is in steps_to_run input or if
# steps_to_run = all
IS_INCLUDED_STEP = sys.argv[0][9:-3] in sys.argv[3] or sys.argv[3].lower() == "all"
RES_ENV = os.getenv("RES_ENV", "development").lower()

if (
    __name__ == "__main__"
    and IS_INCLUDED_STEP
    and (RES_ENV == "production" or DEV_OVERRIDE)
):
    # Retrieve Snowflake and OpenAI credentials
    os.environ["OPENAI_API_KEY"] = secrets_client.get_secret("DATA_TEAM_OPENAI_API_KEY")
    snowflake_creds = secrets_client.get_secret("SNOWFLAKE_AWS_LAMBDA_CREDENTIALS")
    ts = None

    if SYNC_TYPE == "test":
        # Test syncs will just retrieve 100 records
        RETURNS_QUERY += "\nlimit 100"
        logger.info("Beginning test sync")

    elif SYNC_TYPE == "incremental":
        try:
            ts = get_latest_snowflake_timestamp(
                "machine_learning",
                "return_reasons_clustering",
                "return_reasons_in_lancedb",
                snowflake_creds["user"],
                snowflake_creds["password"],
                snowflake_creds["account"],
                "synced_at",
                "%Y-%m-%d %H:%M:%S.%f %z",
            )

        except Exception as e:
            logger.error(e)
            raise

        else:
            RETURNS_QUERY += f"\nand returns.updated_at >= '{ts}'::timestamp_tz"
            logger.info("Beginning incremental sync")

    elif SYNC_TYPE == "full":
        logger.info("Beginning full sync")

    else:
        e_str = "Null or invalid sync type provided; sync type must be full, incremental, or test"
        logger.error(e_str)
        raise ValueError(e_str)

    # retrieve data from snowflake
    conn = snowflake.connector.connect(
        user=snowflake_creds["user"],
        password=snowflake_creds["password"],
        account=snowflake_creds["account"],
        warehouse="loader_wh",
    )
    cur = conn.cursor()

    try:
        data = []
        synced_at = datetime.utcnow()
        cur.execute(RETURNS_QUERY)

        for (
            source_return_line_item_id,
            return_reason_notes,
            ordered_at,
            body_code,
            brand_code,
            brand_name,
            fulfillment_completed_at,
            style_code,
            size_code,
        ) in cur:
            data.append(
                {
                    "source_return_line_item_id": source_return_line_item_id,
                    "return_reason_notes": return_reason_notes,
                    "brand_code": brand_code,
                    "brand_name": brand_name,
                    "body_code": body_code,
                    "style_code": style_code,
                    "size_code": size_code,
                    "ordered_at": ordered_at,
                    "fulfillment_completed_at": fulfillment_completed_at,
                }
            )

        if len(data) == 0:
            logger.info("No data to sync" + (f" since {ts}" if ts else ""))

        else:
            logger.info("Retrieved data from Snowflake")

    except Exception as e:
        logger.error(e)

        raise

    finally:
        cur.close()

    Model = ReturnReasonsVectorStoreEntry.create_model(
        name="return_reasons", namespace="sell"
    )
    store = VectorDataStore(
        Model,
        description="A store for return reasons given by customers when returning ONE garments",
    )

    # Create dataframe of records
    df = pd.DataFrame.from_dict(data)
    records = [
        Model(
            name=str(record["source_return_line_item_id"]),
            text=record["return_reason_notes"],
            source_return_line_item_id=record["source_return_line_item_id"],
            return_reason_notes=record["return_reason_notes"],
            brand_code=record["brand_code"],
            brand_name=record["brand_name"],
            body_code=record["body_code"],
            style_code=record["style_code"],
            size_code=record["size_code"],
            ordered_at=record["ordered_at"],
            fulfillment_completed_at=record["fulfillment_completed_at"],
            synced_at=synced_at,
        )
        for record in df.dropna().to_dict("records")
    ]
    store.add(records)

    # Add added records to a Snowflake table to track what has been stored in S3
    # and what has not
    data = add_p_key_and_sync_time(data, synced_at)
    logger.info(f"Adding records stored in LanceDB to Snowflake table")
    to_snowflake(
        data,
        snowflake_creds,
        "machine_learning",
        "return_reasons_clustering",
        "return_reasons_in_lancedb",
    )
    logger.info(f"Ingesting and embedding complete")
