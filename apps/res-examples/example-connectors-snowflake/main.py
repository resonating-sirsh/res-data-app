from res.utils import logger
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient

QUERY = 'SELECT "created_at", "fulfillment_status" FROM SHOPIFY_ORDERS limit 10'

# Basic query functionality
if __name__ == "__main__":
    client = ResSnowflakeClient(schema="IAMCURIOUS_SCHEMA")
    logger.info("Executing query...")
    results = client.execute(QUERY)
    for row in results:
        logger.info(row)
    logger.info("Done!")
