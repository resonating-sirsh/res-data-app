from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils import logger

# Starting with a 24 hour alarm
DEFAULT_MAX_MINUTES_THRESHOLD = 24 * 60

THRESHOLD_OVERRIDES = {}

if __name__ == "__main__":
    try:
        snowflake_connector = ResSnowflakeClient()
        with open("query.sql") as query_file:
            results = snowflake_connector.execute(query_file.read(), return_type="list")
            for row in results:
                logger.debug(str(row))
                alert = False
                if row[0] in THRESHOLD_OVERRIDES:
                    if int(row[1]) > THRESHOLD_OVERRIDES[row[0]]:
                        alert = True
                else:
                    if int(row[1]) > DEFAULT_MAX_MINUTES_THRESHOLD:
                        alert = True

                if alert:
                    # Table hasn't been updated, raise alarm
                    logger.error(
                        "Mongo table hasn't updated in {} minutes: {}".format(
                            DEFAULT_MAX_MINUTES_THRESHOLD, row[0]
                        )
                    )
                else:
                    logger.info("Table is up to date: {}".format(row[0]))
    except Exception as e:
        logger.error("Error running query for Mongo Alerts: {}".format(str(e)))
