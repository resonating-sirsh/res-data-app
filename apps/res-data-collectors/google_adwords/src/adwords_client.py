import json
import csv
import tempfile
from googleads import adwords, AdWordsClient
from datetime import datetime, timedelta
from res.utils import logger
from typing import Callable

DELTA_JOB_TYPE = "Deltas"


class ClientJobConfig:
    def __init__(
        self,
        account_id: str,
        last_updated_timestamp: str,
        data_extract_type: str,
        brand_code: str,
    ):
        self.account_id = account_id
        self.brand_code = brand_code
        self.last_updated_timestamp = last_updated_timestamp
        self.data_extract_type = data_extract_type

    def get_report_start_datetime(self) -> datetime:
        start_date = None
        if self.data_extract_type == DELTA_JOB_TYPE and self.last_updated_timestamp:
            last_updated_time = datetime.fromtimestamp(int(self.last_updated_timestamp))
            time_difference = datetime.today() - last_updated_time
            start_date = datetime.now() - time_difference
        else:
            # build start date from the 'beginning'
            start_date = datetime.now() - timedelta(days=365 * 2)
        return start_date


class ClientJobSecrets:
    def __init__(self, adwords_creds_yaml: str):
        self.key_yaml_string = adwords_creds_yaml


class ResAdwordsClient:
    ADWORDS_FIELDS = (
        "CampaignId",
        "CampaignName",
        "CampaignStatus",
        "Date",
        "Amount",
        "HasRecommendedBudget",
        "Impressions",
        "Clicks",
        "Cost",
        "AllConversions",
        "AllConversionRate",
        "AverageCost",
        "AverageCpc",
        "AverageCpe",
        "AverageCpm",
        "AverageCpv",
        "AverageTimeOnSite",
        "CostPerAllConversion",
        "Ctr",
        "Engagements",
        "EngagementRate",
        "GmailForwards",
        "Interactions",
        "InteractionRate",
        "Conversions",
        "ConversionValue",
        "CostPerConversion",
    )

    def __init__(self, job_config: ClientJobConfig, secrets: ClientJobSecrets):
        self._config = job_config
        self._client = self._get_adwords_client(secrets.key_yaml_string)

    def _get_adwords_client(self, key_yaml_string) -> AdWordsClient:
        # I'm not really sure how this is working in the OG implementation:
        # `adwords_client = adwords.AdWordsClient.LoadFromString(brand.get('key_json'))``
        # Adwords really wants to load this from a file-like resource, so I'm tricking
        # it until I figure this out
        logger.info("Getting AdWords client")
        with tempfile.SpooledTemporaryFile() as fake_yaml_file:
            fake_yaml_file.write(bytearray(key_yaml_string, "utf8"))
            fake_yaml_file.seek(0)  # rewind
            adwords_client = adwords.AdWordsClient.LoadFromString(fake_yaml_file)
            adwords_client.SetClientCustomerId(self._config.account_id)
            return adwords_client

    def _make_report_query(self, start_date, end_date):
        return (
            adwords.ReportQueryBuilder()
            .Select(*self.ADWORDS_FIELDS)
            .From("CAMPAIGN_PERFORMANCE_REPORT")
            .During(start_date=start_date, end_date=end_date)
            .Build()
        )

    def _execute_query(self, query):
        return self._client.GetReportDownloader(
            version="v201809"
        ).DownloadReportAsStringWithAwql(
            query,
            "CSV",
            skip_report_header=True,
            skip_column_header=True,
            skip_report_summary=True,
            include_zero_impressions=True,
        )

    @staticmethod
    def _make_covering_date_ranges(
        start_date: datetime, end_date: datetime, max_day_interval: int
    ):
        """returns a list of date-ranges covering the start_date to end_date, with no intervals exceeding the max day
        interval. (well, technically max day interval + 1)
        e.g. if the max interval is 2 days:
        [(2021-06-01, 2021-06-03), (2021-06-04, 2021-06-06), ... ]

        FYI, the reason for this odd interval structure is that Google appears to only allows queries by
        date, not datetime.

        """
        ranges = []
        range_start = start_date
        while True:
            range_end = min(end_date, range_start + timedelta(days=max_day_interval))
            ranges.append((range_start, range_end))
            if range_end == end_date:
                break
            else:
                range_start = range_end + timedelta(days=1)
        return ranges

    def pull_adwords_data(self):
        start_date_range = self._config.get_report_start_datetime()
        end_date_range = datetime.today()
        max_report_date_range = 21
        date_ranges = self._make_covering_date_ranges(
            start_date_range, end_date_range, max_report_date_range
        )

        csv_temp_file = tempfile.NamedTemporaryFile(mode="w+t")
        for start_date, end_date in date_ranges:
            logger.info(
                f"Fetching AdWords data from: {start_date.strftime('%Y-%m-%d')}\tto: {end_date.strftime('%Y-%m-%d')}"
            )
            query = self._make_report_query(start_date, end_date)
            report_string = self._execute_query(query)
            csv_temp_file.write(report_string)

        # 'Rewind' .csv file to read from the beginning and write to the .json file
        csv_temp_file.seek(0)

        logger.info("Data Collected!")
        logger.info("Converting .CSV to a .JSON file")

        # parse the .csv file to create a temporary .json file
        reader = csv.DictReader(csv_temp_file, fieldnames=self.ADWORDS_FIELDS)
        json_temp_file = tempfile.NamedTemporaryFile(mode="w+t")

        # TODO - this duplicate removal should be achieved across brands and across data sources
        # TODO - not sure this jsonification is really necessary. It works, so I (AR) didn't want to mess with it
        # using a set to store all rows and easily remove duplicates
        no_duplicates = set()
        for row in reader:
            # create json from orderedDict
            json_row = json.dumps(row)
            # add json row to set
            no_duplicates.add(json_row)

        items = []
        for item in no_duplicates:
            py_item = json.loads(item)
            py_item["brand_code"] = self._config.brand_code
            items.append(py_item)

        # close temporary files
        json_temp_file.close()
        csv_temp_file.close()
        return items
