from ResGoogleAnalytics import ResGoogleAnalyticsClient
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils import logger
import json, requests, os, datetime

BRAND_CONFIGS = [
    {"brand_code": "KT", "view_id": "132376190"},
    {"brand_code": "TK", "view_id": "61042386"},
    {"brand_code": "JR", "view_id": "121475933"},
    {"brand_code": "BR", "view_id": "219825341"},
]
START_DATE = datetime.date(2020, 1, 1)
END_DATE = datetime.datetime.now().date() - datetime.timedelta(days=1)


if __name__ == "__main__":
    if os.getenv("BACKFILL", "False").lower() == "false":
        # Don't backfill
        days_back = int(os.getenv("DAYS_BACK", 1))
        START_DATE = END_DATE - datetime.timedelta(days=days_back)

    # Initialize client
    gac = ResGoogleAnalyticsClient()
    snow_client = ResSnowflakeClient()
    rows_added = 0

    # Loop thru brands and get data, send to kgateway
    for brand_config in BRAND_CONFIGS:
        try:
            date_iterator = START_DATE
            brand_code = brand_config["brand_code"]
            logger.info(f"Running reports for: {brand_code}")
            while date_iterator <= END_DATE:
                for type in ["orders", "metrics"]:
                    data = gac.run_query_for_day(
                        view_id=brand_config["view_id"], date=date_iterator, type=type
                    )
                    converted_data = gac.convert_query_results(
                        data, brand_code, type=type
                    )
                    logger.info(
                        f"\tBrand: {brand_code}, Type: {type}, Rows loaded: {str(len(converted_data))}"
                    )
                    snow_client.load_json_data(
                        converted_data, f"google_analytics_{type}", "primary_key"
                    )
                date_iterator += datetime.timedelta(days=1)
        except Exception as e:
            logger.error(
                f"Error processing google analytics for brand: {brand_config}, {e}"
            )
            continue
