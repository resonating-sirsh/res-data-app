from res.connectors.quickbooks.QuickbooksClient import QuickBooksClient
from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
import os, datetime

START_DATE = datetime.datetime.now().date() - datetime.timedelta(
    days=4
)  # datetime.date(2020, 1, 1)
END_DATE = datetime.datetime.now().date() - datetime.timedelta(days=2)

if __name__ == "__main__":
    if os.getenv("BACKFILL", "False").lower() == "false":
        # Don't backfill
        days_back = int(os.getenv("DAYS_BACK", 1))
        START_DATE = END_DATE - datetime.timedelta(days=days_back)

    # Initialize clients
    qb_client = QuickBooksClient("Resonance_Manufacturing")
    snow_client = ResSnowflakeClient()

    # try:
    date_iterator = START_DATE
    data = qb_client.get_report("ProfitAndLoss")
    print(data)
    # snow_client.load_json_data(data, "quickbooks_profit_and_loss", "primary_key")
    date_iterator += datetime.timedelta(days=1)
    # except Exception as e:
    #     logger.error("Error processing quickbooks report")
    #     print(f"{e!r}")
