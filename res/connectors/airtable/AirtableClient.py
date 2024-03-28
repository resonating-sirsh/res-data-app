from typing import Dict
from res.utils import logger, secrets_client, safe_http, ping_slack
import requests
import re

API_URI = "https://api.airtable.com/v0"


class ResAirtableClient:
    def __init__(self):
        self.api_key = secrets_client.get_secret("AIRTABLE_API_KEY")
        self.bearer_header = {"Authorization": f"Bearer {self.api_key}"}

    @staticmethod
    def clean_name(raw_name):
        # Replace camel case with snake case, convert to lower
        clean_name = re.sub(
            "((?<=[a-z0-9])[A-Z]|(?!^)[A-Z](?=[a-z]))", r"_\1", raw_name
        ).lower()
        # Replace all non alphanumeric characters with underscore
        clean_name = re.sub("[^0-9a-zA-Z]+", "_", clean_name)
        # Strip leading and trailing underscores
        clean_name = clean_name.lstrip("_").rstrip("_")
        return clean_name

    @staticmethod
    def clean_field_name(raw_name):
        # Different cleaning logic for fields
        regex_replaced = re.sub("[^A-Za-z0-9 _]+", "", raw_name)
        if raw_name == regex_replaced:
            return regex_replaced
        return regex_replaced.replace("  ", "_").replace(" ", "_")

    def _get_airtable_request_header(self):
        return {"Authorization": f"Bearer {self.api_key}"}

    def get_bases(self):
        url = f"{API_URI}/meta/bases"
        json_data = []
        offset = None
        while True:
            iter_json_data = requests.get(
                url=f"{url}?offset={offset}" if offset is not None else url,
                headers=self.bearer_header,
            ).json()
            json_data.extend(iter_json_data["bases"])

            offset = iter_json_data.get("offset")
            if offset is None:
                break
            else:
                logger.info("Enterprise has over 1000 bases; iterating to retrieve all")

        return json_data

    def get_base_schema(self, base_id):
        url = f"{API_URI}/meta/bases/{base_id}/tables"

        try:
            json_data = safe_http.request_get(
                url, headers=self.bearer_header, timeout=10
            ).json()
            return json_data

        except Exception as e:
            logger.error(e.message)
            raise

    def get_record(
        self,
        table_id,
        base_id,
        record_id,
    ) -> Dict:
        uri = f"{API_URI}/{base_id}/{table_id}/{record_id}"
        response = requests.get(uri, headers=self.bearer_header).json()
        return response

    def get_records(
        self,
        base_id,
        table_id,
        view_id=None,
        filter_by_formula=None,
        limit=None,
        get_all=True,
        log_errors=True,
        fields=None,
        send_errors_to_slack=False,
        slack_channel=None,
    ):
        url = f"{API_URI}/{base_id}/{table_id}"
        params = {}
        if view_id:
            params["view"] = view_id
        if filter_by_formula:
            params["filterByFormula"] = filter_by_formula
        if limit:
            params["limit"] = limit
        if fields:
            params["fields"] = fields
        offset = None
        while True:
            if offset:
                params["offset"] = offset
                logger.debug(f"...offset: {offset}")
            response = safe_http.request_get(
                headers=self.bearer_header, url=url, params=params, timeout=None
            ).json()
            if "records" not in response:
                e_str = f"Bad response while retrieving records for base {base_id} table {table_id}: {str(response)}"
                if log_errors:
                    logger.error(e_str)
                if send_errors_to_slack:
                    ping_slack(
                        # Pings Jay Gibbs
                        "<@U043JU9F4EN> - " + e_str,
                        slack_channel,
                    )

                break
            for record in response["records"]:
                yield record
            if "offset" in response and get_all:
                offset = response["offset"]
            else:
                break

    def set_record(self, base_id, table_id, record_id, field, value):
        # Sets a specific field's value
        url = f"{API_URI}/{base_id}/{table_id}/{record_id}"
        result = requests.patch(
            headers=self.bearer_header,
            url=url,
            json={"fields": {field: value}},
        )
        if result.status_code != 200:
            logger.error(
                f"Airtable client set_record returned an error: {result.text} for url {url}"
            )
        return result

    """
    Receives a list of field-value pairs. We formatted so all these are under a 'fields' dictionary as Airtable expect it.
    """

    def create_many_records(self, base_id, table_id, data):
        url = f"{API_URI}/{base_id}/{table_id}"
        formatted_data = [{"fields": d} for d in data if "fields" not in d]
        result = requests.post(
            url,
            headers=self.bearer_header,
            json={"records": formatted_data},
        )
        return result

    def delete_record(
        self,
        base_id,
        table_id,
        record_id,
        send_errors_to_slack=False,
        slack_channel=None,
    ):
        url = f"{API_URI}/{base_id}/{table_id}/{record_id}"
        result = requests.delete(url=url, headers=self.bearer_header)
        if result.status_code != 200:

            e_str = (
                f"Airtable client delete returned an error: {result.text} for url {url}"
            )
            logger.error(e_str)
            if send_errors_to_slack:
                ping_slack(
                    # Pings Jay Gibbs
                    "<@U043JU9F4EN> - Error deleting a record - " + e_str,
                    slack_channel,
                )
        return result
