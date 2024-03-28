import os, json, requests, time
from res.utils import logger, secrets_client
from res.utils.logging.ResLogger import ResLogger
from src.alert_config import ALERTS
from airtable import Airtable as AirtableWrapper

# This long running process runs a series of queries against airtable
# Any query that returns results will fire off an error to Sentry

sleep_seconds = 60


def airtable_query(base, table, filter, api_key):
    results = []
    at = AirtableWrapper(base, api_key)
    for record in at.iterate(table, filter_by_formula=filter):
        results.append(dict(record["fields"]))
    return results


def run_alerts():
    api_key = secrets_client.get_secret("AIRTABLE_API_KEY")
    environment = os.getenv("RES_ENV", "local")
    while True:
        for team in ALERTS:
            logger.info(f"Running alerts for team {team}")
            for alert in ALERTS[team]:
                alert_name = alert["alert_name"]
                logger.info(f"...Checking alert {alert_name}")
                query_results = airtable_query(
                    alert["base"], alert["table"], alert["filter"], api_key
                )
                logger.debug(query_results)
                if len(query_results) > 0:
                    # Set team env var an initialize new logger
                    os.environ["RES_TEAM"] = team
                    team_logger = ResLogger()
                    if environment == "production":
                        team_logger.error(
                            f"Airtable Alert: {alert_name} failed, returned rows: {query_results}"
                        )
                    else:
                        team_logger.warn(
                            "Non-production environment, not sending alert"
                        )
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    run_alerts()
