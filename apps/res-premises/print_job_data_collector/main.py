import sys
import os
import requests
from xml.etree import ElementTree as ET
from res.utils.logging.ResLogger import ResLogger
from res.utils.configuration.ResConfigClient import ResConfigClient
import res.utils.premises.premtools as premtools
from tenacity import retry, wait_fixed, stop_after_attempt, wait_exponential
from airtable import Airtable

printer_name = None
printer_ip_address = None
jobs_list_url = ""
print_job_url = ""
DEVICE_NAME = os.getenv("DEVICE_NAME")
RES_ENV = os.getenv("RES_ENV", "production")
RES_NAMESPACE = os.getenv("RES_NAMESPACE", "res_premises")
RES_APP_NAME = os.environ["RES_APP_NAME"] = "printjob_data_collector"
logger = ResLogger()
configs_cient = ResConfigClient(
    environment=RES_ENV, app_name=RES_APP_NAME, namespace=RES_NAMESPACE
)


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def update_number_of_active_rows(active_rows):
    data = {"Active Rows At Printer": active_rows}
    at_print_machines = Airtable(
        "apprcULXTWu33KFsh", "tbl4DbNHQPMwlLrGY", os.environ.get("AIRTABLE_API_KEY")
    )
    at_print_machines.update(
        os.environ.get("AT_MACHINE_RECORD_ID"), fields=data, typecast=True
    )


def get_printer_env_variables():
    printer_name = os.getenv("PRINTER_NAME")
    printer_ip_address = os.getenv("PRINTER_IP_ADDRESS")

    return printer_name, printer_ip_address


def get_last_processed_printjob():
    # UPDATED ON NOVEMBER 3TH, 2021 to use Configs Client.
    # Gets last processed jobid and status from configs manager.
    # RETURNS: last processed jobid and its status-> {"jobid": "#####", "status": "#"}
    printjob = configs_cient.get_config_value(
        "last_processed_job_at_{}".format(printer_name)
    )

    set_new_starting_job = False

    if set_new_starting_job:
        logger.info("Setting new 'last processed job'")
        printjob = {"jobid": "36750", "status": "0"}
        configs_cient.set_config_value(
            "last_processed_job_at_{}".format(printer_name), printjob
        )
        printjob = configs_cient.get_config_value(
            "last_processed_job_at_{}".format(printer_name)
        )
        set_new_starting_job = False
        return printjob

    elif printjob is None:
        logger.error("No 'last processed job' found on configs client. Exiting")

        sys.exit()

    else:
        return printjob


def get_unprocessed_job_ids(printer_ip_adress, last_job_id):  # TESTED OK'
    # Request jobslist from last_processed_jobid
    # Create URL from params and last processed job-id
    logger.info("Creating List of unprocessed jobs")
    jobs_list_url = "http://{}/xml/joblist?from_id={}".format(
        printer_ip_adress, last_job_id
    )
    jobs_list_xml = requests.get(jobs_list_url)

    jobslist_xml_root = ET.fromstring(jobs_list_xml.content)

    # Create list of unprocessed jobs
    unprocessed_jobs_list = []
    for Job in jobslist_xml_root:
        unprocessed_jobs_list.append(Job[0].text)

    logger.info("There are {} unprocessed jobs".format(len(unprocessed_jobs_list)))
    logger.info("Done getting list of unprocessed jobs")
    return unprocessed_jobs_list


def update_last_processed_printjob(new_job_id, new_status):  # TESTED OK
    # Updates the config last_processed_printjob_at_{printer} with the latest processed printjob

    printjob = configs_cient.get_config_value(
        "last_processed_job_at_{}".format(printer_name)
    )

    printjob["jobid"] = new_job_id
    printjob["status"] = new_status

    configs_cient.set_config_value(
        "last_processed_job_at_{}".format(printer_name), printjob
    )


def get_printjob_from_printer(printer_ip_adress, job_id):
    # Gets print-job data and inserts the printer name into the XML.
    print_job_url = "http://{}/xml/jobid-{}".format(printer_ip_adress, job_id)
    print_job_data_xml = requests.get(print_job_url)

    # Read XML File
    try:
        print_job_data_tree = ET.fromstring(print_job_data_xml.content)
        logger.info(f"Getting data for job-{job_id} -> Ok")
    except:
        logger.info(f"Getting data for job-{job_id} -> Error")
        return "fail", "fail"

    job_status = print_job_data_tree.find("Status").text
    # Create Element "Printer" on index 0
    printer_name_node = ET.Element("Printer")
    printer_name_node.text = printer_name
    print_job_data_tree.insert(0, printer_name_node)

    # Create Element "Jobid" on index 1
    print_job_node = ET.Element("Jobid")
    print_job_node.text = job_id
    print_job_data_tree.insert(1, print_job_node)

    job_data_with_printer_name = ET.tostring(
        print_job_data_tree, encoding="utf8"
    ).decode("utf8")
    return job_data_with_printer_name, job_status


@retry(wait=wait_exponential(multiplier=1, min=4, max=30), stop=stop_after_attempt(5))
def push_printjob_data_to_kafka(job_xml):
    if RES_ENV == "production":
        url = "https://data.resmagic.io/kgateway/submitevent"
        data = {
            "version": "1.0.0",
            "process_name": "res-premises.print_job_data",
            "topic": "res_premises.printer_data_collector.print_job_data",
            "data": job_xml,
            "data_format": "xml",
        }
        response = requests.post(url, json=data)

        if response.text == "success":
            return True
        else:
            raise Exception
    else:
        url = "https://datadev.resmagic.io/kgateway/submitevent"
        data = {
            "version": "1.0.0",
            "process_name": "res-premises.print_job_data",
            "topic": "res_premises.printer_data_collector.print_job_data",
            "data": job_xml,
            "data_format": "xml",
        }
        response = requests.post(url, json=data)

        if response.text == "success":
            return True
        else:
            raise Exception


def printer_is_on(printer_ip_address):
    # Requests the current status of the printer.
    # If no response, we'll close teh collector.
    logger.info("Getting Printer Status")
    printer_status_url = "http://{}/json/status".format(printer_ip_address)

    try:
        printer_status_response = requests.get(printer_status_url)
    except:
        return False

    try:
        number_of_active_rows = printer_status_response.json()["MSjobinfo"][
            "ActiveRows"
        ]
        update_number_of_active_rows(number_of_active_rows)
    except Exception as e:
        logger.error(f"Unable to update number of active rows. Got {e!r}")

    # update airtable.
    if printer_status_response.status_code == 200:
        logger.info(f"Printer: {printer_name} is powered on!")
        return True

    return False


def start():
    global printer_name
    global printer_ip_address

    # 1. Get printer variables
    printer_name, printer_ip_address = get_printer_env_variables()

    if not printer_is_on(printer_ip_address):
        logger.warning(f"Printer: {printer_name} not powered on!")
        exit()

    # 2. Get last processed job from Configs Manager
    last_processed_job = get_last_processed_printjob()

    # 3.Request the list of unprocessed jobs from printer

    pjobs_list = get_unprocessed_job_ids(
        printer_ip_address, last_processed_job["jobid"]
    )

    n = 1
    for job_id in pjobs_list:
        logger.info(f"Now processing - {job_id}")
        printjob_data_to_push, current_job_status = get_printjob_from_printer(
            printer_ip_address, job_id
        )

        if not (printjob_data_to_push == "fail"):
            if (
                n == 1
                and job_id == last_processed_job["jobid"]
                and current_job_status == last_processed_job["status"]
            ):
                logger.info("Same job and status as last time....")
            else:
                result = push_printjob_data_to_kafka(printjob_data_to_push)
                if result:
                    logger.info(f"{n}- Jobid-{job_id}   Processed Successfully")
                    last_processed = job_id
                    update_last_processed_printjob(last_processed, current_job_status)
                else:
                    logger.info(f"{n}- Jobid-{job_id}   Failed or in progress")
                    exit()

        n += 1
    logger.info("Collector Done Running")


if __name__ == "__main__":
    if not premtools.internet_is_up():
        logger.info("No internet... Closing collector")
        exit()
    try:
        start()
    except Exception as e:
        logger.error(
            f"An unexpected error has ocurred.\n {DEVICE_NAME}-{RES_APP_NAME}\n{e!r}"
        )
        raise e
