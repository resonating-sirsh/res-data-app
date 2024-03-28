from adwords_client import ResAdwordsClient, ClientJobConfig, ClientJobSecrets

from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils import secrets_client, logger
from typing import List, Dict
from datetime import datetime
import time, json, os, arrow

SECRET_NAME = "GOOGLE_ADWORDS"
DAILY_START = int(arrow.utcnow().shift(days=-2).timestamp())
CLIENT_CONFIGS = [
    {"brand_code": "TK", "account_id": "341-769-4381"},
    {"brand_code": "RS", "account_id": "189-600-7152"},
    {"brand_code": "JR", "account_id": "597-306-1088"},
    {"brand_code": "KT", "account_id": "387-398-8025"},
]

if __name__ == "__main__":
    logger.info("Starting job...")
    if os.getenv("BACKFILL", "False").lower() == "false":
        # Don't backfill, just pull in prior 2 days
        start = DAILY_START
        extract_type = "Deltas"
    else:
        start = None
        extract_type = None
    start_time = time.time()
    snow_client = ResSnowflakeClient()
    adwords_creds_json = secrets_client.get_secret(SECRET_NAME)
    job_secrets = ClientJobSecrets(adwords_creds_yaml=adwords_creds_json)

    for i, config in enumerate(CLIENT_CONFIGS):
        job_config = ClientJobConfig(
            account_id=config["account_id"],
            last_updated_timestamp=start,
            brand_code=config["brand_code"],
            data_extract_type=extract_type,
        )

        client = ResAdwordsClient(job_config, job_secrets)

        logger.info(f"Pulling Adwords data for brand: {config['brand_code']}")
        rows = client.pull_adwords_data()
        logger.info(
            f"Pulled {len(rows)} Rows of Adwords Data for brand: {config['brand_code']}. Sending to Snowflake"
        )
        logger.info("Setting primary keys for upsert")
        for row in rows:
            row["primary_key"] = "-".join(
                [row["CampaignId"], row["Date"], row["brand_code"]]
            )

        logger.info("Loading to snowflake...")
        snow_client.load_json_data(rows, f"google_adwords", "primary_key")
        logger.incr("rows_sent_to_snowflake", len(rows))
        logger.info("All Rows Sent to Snowflake")

    logger.timing("total_time_elapsed_ms", int((time.time() - start_time) * 1000))
