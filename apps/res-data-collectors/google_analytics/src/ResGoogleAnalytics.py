import json, datetime
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
from res.utils import logger, secrets_client

API_NAME = "analyticsreporting"
API_VERSION = "v4"
API_SCOPE = ["https://www.googleapis.com/auth/analytics.readonly"]
API_JSON_SECRET_NAME = "GOOGLE_ANALYTICS_KEYFILE"
DIMENSIONS = {
    "orders": [
        {"name": "ga:campaign"},
        {"name": "ga:date"},
        {"name": "ga:transactionId"},
        {"name": "ga:channelGrouping"},
        {"name": "ga:source"},
        {"name": "ga:medium"},
        {"name": "ga:adDistributionNetwork"},
    ],
    "metrics": [
        {"name": "ga:campaign"},
        {"name": "ga:date"},
        {"name": "ga:channelGrouping"},
        {"name": "ga:source"},
        {"name": "ga:medium"},
        {"name": "ga:adDistributionNetwork"},
        {"name": "ga:deviceCategory"},
        {"name": "ga:userType"},
        {"name": "ga:hasSocialSourceReferral"},
    ],
}
METRICS = [
    {"expression": "ga:transactionRevenue"},
    {"expression": "ga:sessions"},
    {"expression": "ga:users"},
    {"expression": "ga:newUsers"},
    {"expression": "ga:transactions"},
    {"expression": "ga:bounces"},
]


class ResGoogleAnalyticsClient:
    def __init__(self, object_override=None):
        self.set_analytics_service_object(object_override)

    def set_analytics_service_object(self, object_override=None):
        if object_override:
            # Use to inject a fake service object for testing
            logger.debug("Overriding analytics service object for testing")
            self.analytics_service_object = object_override
        else:
            # Retrieve credentials and create a GA service object
            logger.info("Retrieving JSON keyfile secret")
            json_secret = secrets_client.get_secret(API_JSON_SECRET_NAME)
            key_dict = json.loads(json_secret)
            logger.info("Building GA Service object")
            credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                key_dict, scopes=API_SCOPE
            )
            self.analytics_service_object = build(
                API_NAME, API_VERSION, credentials=credentials
            )

    def run_query_for_day(
        self,
        view_id,
        date=datetime.datetime.today() - datetime.timedelta(days=2),
        allow_incomplete=False,
        type="orders",
    ):
        # Queries are always for a single day.
        # Data from GA takes 24-48 hours for processing, so default to 2 days ago
        # type can be orders or metrics
        start_date = date.strftime("%Y-%m-%d")
        end_date = start_date
        logger.info("Retrieving GA report for date: {}".format(end_date))
        report = (
            self.analytics_service_object.reports()
            .batchGet(
                body={
                    "reportRequests": [
                        {
                            "viewId": view_id,
                            "dateRanges": [
                                {"startDate": start_date, "endDate": end_date}
                            ],
                            "metrics": METRICS,
                            "dimensions": DIMENSIONS[type],
                            "samplingLevel": "LARGE",
                        }
                    ]
                }
            )
            .execute()
        )
        # logger.debug(json.dumps(report, indent=4))
        # "Golden" data means the data is finalized, this report/day's data will never change
        if (
            allow_incomplete
            or "isDataGolden" not in report["reports"][0]["data"]
            or report["reports"][0]["data"]["isDataGolden"]
        ):
            return report
        else:
            # Data isn't ready yet
            logger.critical(
                "Data returned from Google Analytics isn't ready yet (isn't Golden)"
            )
            return None

    def convert_query_results(self, query_results, brand_code, type="orders"):
        # Converts native google analytics query results to simple rows/columns in JSON format
        return_data = []
        if "rows" not in query_results["reports"][0]["data"]:
            # No results
            return return_data
        for row in query_results["reports"][0]["data"]["rows"]:
            new_row = {}
            for i, dim in enumerate(row["dimensions"]):
                new_row[DIMENSIONS[type][i]["name"].replace(":", "_")] = dim
            for i, metric in enumerate(row["metrics"][0]["values"]):
                new_row[METRICS[i]["expression"].replace(":", "_")] = float(metric)
            new_row["brand_code"] = brand_code
            new_row["primary_key"] = "-".join(row["dimensions"]) + "-" + brand_code
            return_data.append(new_row)
        return return_data
