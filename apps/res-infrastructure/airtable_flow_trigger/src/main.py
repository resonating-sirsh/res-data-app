import os
import time
from operator import attrgetter as ag, methodcaller as mc
from pathlib import Path
from typing import Dict, Generator, NamedTuple, Text

import yaml

import res
from res.utils import logger
from res.connectors.airtable.AirtableClient import ResAirtableClient

RES_ENV = os.getenv("RES_ENV", "development")
CONFIG_PATH = "configs"
DELAY_BETWEEN_AIRTABLE_CALLS = 5  # In seconds


def make_flow_payload(data, job_key="test123", metadata_name="flow_node_example"):
    return {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {"name": metadata_name, "version": "v1"},
        "assets": data if isinstance(data, list) else [data],
        "task": {"key": job_key},
        "args": {},
    }


class ActiveEnvs(NamedTuple):
    development: bool
    production: bool


class QueryTrigger(NamedTuple):
    """Python class representation of event trigger yaml configuration"""

    base: Text
    table: Text
    flow: Text
    formula: Text
    active: ActiveEnvs

    @classmethod
    def parse(cls, trig: Dict) -> "QueryTrigger":
        """Returns QueryTrigger class populated with values from trig"""
        active = ActiveEnvs(**trig.pop("active"))
        non_string = {k: v for k, v in trig.items() if not isinstance(v, str)}
        if non_string:
            raise TypeError(f"Expected string args, got {non_string}!")
        return cls(**trig, active=active)

    @property
    def is_active(self) -> bool:
        return getattr(self.active, RES_ENV, False)


def get_trigger_data(airtable_client, trigger):
    """Queries Airtable for trigger results"""
    # Get data from Airtable (if any)
    airtable_results = list(
        airtable_client.get_records(
            trigger.base, trigger.table, filter_by_formula=trigger.formula
        )
    )
    if not airtable_results:
        # No results, nothing to do
        logger.info("\tNo rows returned!")
        return None
    else:
        logger.info(f"{len(airtable_results)} records found")
        return [result["id"] for result in airtable_results]


def get_unique_job_name(flow):
    # Job names are max 65 characters in kubernetes
    # And must conform to other standards
    clean_flow = flow.replace("_", "-").replace(".", "-")
    short_flow = clean_flow[:52]
    return short_flow


def job_instances_running(argo_client, clean_flow_name):
    # Checks if another instance of this job is running on argo
    running_jobs = argo_client.get_workflows().to_dict()
    for job_num in running_jobs.get("name"):
        # Check if job is still running
        if running_jobs.get("phase").get(job_num) == "Running":
            # Strip out the timestamp at the end of the job
            running_job_name = running_jobs.get("name")[job_num][:-11]
            if running_job_name == clean_flow_name:
                return True
    return False


def handle_trigger(argo_client, airtable_client, trigger: QueryTrigger):
    """Calls get_trigger_data to get trigger results and executes Argo flow to process"""
    logger.info("Processing config: " + trigger.flow)
    # Check if another instance is running
    if job_instances_running(argo_client, get_unique_job_name(trigger.flow)):
        logger.warning("Another copy of the job is running, skipping this run")
        return None
    airtable_results = get_trigger_data(airtable_client, trigger)
    if not airtable_results:
        return None
    data = {"record_ids": airtable_results}
    # Construct flow payload
    job_key = f"{str(int(time.time()))}"
    unique_job_name = f"{get_unique_job_name(trigger.flow)}-{job_key}"
    payload = make_flow_payload(job_key=job_key, metadata_name=trigger.flow, data=data)
    # Submit argo job
    logger.info(f"\tsubmitting job: {unique_job_name}")
    argo_client.handle_event(payload, unique_job_name=unique_job_name)


def read_config_file(config_path: Path) -> Generator[QueryTrigger, None, None]:
    """Parses and validates a single config yaml file"""
    try:
        yaml_data = yaml.safe_load(Path.read_text(config_path))
    except yaml.YAMLError:
        logger.error(f"Error opening airtable_event_trigger config file: {config_path}")
    else:
        for trig_data in yaml_data["event_triggers"]:
            try:
                yield QueryTrigger.parse(trig_data)
            except (TypeError, ValueError) as exc:
                logger.error(
                    f"Error parsing query trigger config! {exc} "
                    f"(input was '{trig_data}')"
                )


def read_triggers(configs_path=None) -> Generator[QueryTrigger, None, None]:
    """Iterates through all files in the configs_path and parses configs"""
    for config_file in filter(mc("is_file"), Path(configs_path).iterdir()):
        yield from read_config_file(config_file)


if __name__ == "__main__":
    # Load all files in configs
    active_triggers = list(filter(ag("is_active"), read_triggers(CONFIG_PATH)))
    # Endless loop to process configs
    argo = res.connectors.load("argo")
    airtable = ResAirtableClient()
    while True:
        for trigger in active_triggers:
            try:
                handle_trigger(argo, airtable, trigger)
                time.sleep(DELAY_BETWEEN_AIRTABLE_CALLS)
            except Exception as e:
                logger.warn(f"Error handling QueryTrigger: {trigger!r}. Error: {e!r}")
                logger.incr_data_flow(
                    "airflow_flow_trigger", asset_group="any", status="FAILED"
                )
                time.sleep(DELAY_BETWEEN_AIRTABLE_CALLS)
