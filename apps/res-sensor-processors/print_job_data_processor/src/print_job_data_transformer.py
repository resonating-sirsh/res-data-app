import urllib.parse, os
from pyairtable import Table
from res.utils import secrets_client, logger
import re
from tenacity import retry, stop_after_attempt, wait_random_exponential

AIRTABLE_API_SECRET = "AIRTABLE_API_KEY"
STATUSES = {
    "0": "PROCESSING",
    "1": "PRINTED",
    "2": "PRINTED",
    "3": "PRINTED",
    "4": "PRINTED",
    "5": "PRINTED",
}

RESULTS = {
    "0": "PENDING",
    "1": "COMPLETED",
    "2": "PARTIALLY PRINTED",
    "3": "PARTIALLY PRINTED",
    "4": "PARTIALLY PRINTED",
    "5": "PARTIALLY PRINTED",
}

PRINT_BASE = "apprcULXTWu33KFsh"
PRINTFILES_TABLE = "tblAQcPuKUDVfU7Fx"
MACHINES_TABLE = "tbl4DbNHQPMwlLrGY"
# PRINTFILES_COLUMN = "Print Queue"
NESTS_TABLE = "tbl7n4mKIXpjMnJ3i"
NESTS_COLUMN = "Print Queue"

# Payload for sending to Kafka Topic to update Airtable
KAFKA_AIRTABLE_TEMPLATE = {
    "submitting_app": os.getenv("RES_APP_NAME", "testing"),
    "submitting_app_namespace": os.getenv("RES_NAMESPACE", "testing"),
    "airtable_base": PRINT_BASE,
    "airtable_table": PRINTFILES_TABLE,
    "airtable_record": "<placeholder>",
    "payload": "<placeholder>",
}

record_id_re = re.compile(r"^rec[0-9a-zA-Z]{14}$")


class PrintJobDataTransformer:
    def __init__(self):
        self.print_files = Table(
            secrets_client.get_secret(AIRTABLE_API_SECRET), PRINT_BASE, PRINTFILES_TABLE
        )
        self.machines = Table(
            secrets_client.get_secret(AIRTABLE_API_SECRET), PRINT_BASE, MACHINES_TABLE
        )
        self.machine_record_ids = {
            r["fields"]["Name"].replace("JP7 ", "").replace("JP5 ", "").lower(): r["id"]
            for r in self.machines.all(
                fields=["Name"], formula="{Machine Type} = 'Printer'"
            )
        }

    def normalize_status(self, raw_data):
        if raw_data["Status"] in STATUSES:
            return STATUSES[raw_data["Status"]]
        else:
            # Invalid Status, dont need to update
            logger.warn(f"Invalid status: {raw_data['Status']}")
            return None

    def normalize_result(self, raw_data):
        if raw_data["Status"] in RESULTS:
            return RESULTS[raw_data["Status"]]
        else:
            # Invalid Status, dont need to update
            logger.warn(f"Invalid status: {raw_data['Status']}")
            return None

    @retry(
        wait=wait_random_exponential(multiplier=5, max=600),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def get_print_file(self, raw_data):
        job_str = urllib.parse.unquote(raw_data["Name"])
        print_file = None
        rec_ids_in_job = [seg for seg in job_str.split("_") if record_id_re.match(seg)]
        if rec_ids_in_job:
            rec_id = rec_ids_in_job[0]
            logger.info(f"Saw record id {rec_id!r} in job string.")
            try:
                return self.print_files.get(rec_id)
            except Exception as e:
                logger.warn(f"No record found for {rec_id!r}! {e!r}")

        filter = f'{{Local File Basename Without Extension}} = "{job_str}"'
        logger.info(f"No record id in job string, searching for {filter!r}")
        print_file = self.print_files.first(formula=filter)
        if not print_file:
            logger.warn(f"No records returned from Airtable! {filter!r}")
        return print_file

    def get_nest_ids(self, print_file):
        nest_ids = print_file["fields"].get("Nests", [])
        if print_file.get("Print Queue", "") != "CANCELED":
            return nest_ids
        unprinted_nests = print_file["fields"].get("Unprinted Nests", "")
        unprinted_nest_ids = set(unprinted_nests.split("."))
        return list(set(nest_ids) & unprinted_nest_ids)

    def canceled_at_unload(self, print_file):
        fields = print_file["fields"]
        if fields.get("Print Queue") == "CANCELED":
            cancelation_reason = fields.get("Cancellation Bot / Reason")
            if cancelation_reason == "ROLL UNLOADED BEFORE FILE PRINTED":
                return True
        return False

    def print_file_updates(self, status, print_file, job_details):
        print_file_id = print_file["id"]
        print_file_update = {
            **KAFKA_AIRTABLE_TEMPLATE,
            "airtable_record": print_file_id,
            "payload": {"Print Queue": status},
        }
        print_file_update["payload"].update(job_details)
        if status == "PRINTED" and self.canceled_at_unload(print_file):
            nest_ids = self.get_nest_ids(print_file)
            assigned_printer = print_file["fields"].get("Assigned Printer")
            if not assigned_printer:
                assigned_printer = print_file["fields"].get("Unassigned Printer")

            logger.debug(f"Uncancelling printed print file {print_file['id']}")

            print_file_update["payload"].update(
                {
                    "Nests": nest_ids,
                    "Unprinted Nests": None,
                    "Unprinted Nests": None,
                    "Assigned Printer": assigned_printer,
                    "Unassigned Printer": None,
                    "Cancellation Bot / Reason": None,
                }
            )

        yield {**print_file_update}

    def update_payload_generator(self, status, print_file, job_details, machine_name):
        print_file_id = print_file["id"]
        nest_ids = self.get_nest_ids(print_file)
        logger.debug(f"{status =}, {print_file_id =}, {nest_ids =}")
        yield from self.print_file_updates(status, print_file, job_details)
        for nest_id in nest_ids:
            nest_update = KAFKA_AIRTABLE_TEMPLATE.copy()
            nest_update.update(
                {
                    "airtable_record": nest_id,
                    "airtable_table": NESTS_TABLE,
                    "payload": {NESTS_COLUMN: status},
                }
            )
            yield nest_update
        machine_update = KAFKA_AIRTABLE_TEMPLATE.copy()
        machine_update.update(
            {
                "airtable_record": self.machine_record_ids[machine_name],
                "airtable_table": MACHINES_TABLE,
                "payload": {"Processing Roll Name": print_file["fields"]["Roll Name"]},
            }
        )
        yield machine_update

    # Transform data for easier use in Snowflake
    def get_kafka_snowflake_payload(self):
        return {}
