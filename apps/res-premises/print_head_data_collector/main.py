import sys
import os, json, yaml
import requests
from res.utils.logging.ResLogger import ResLogger
from res.utils.configuration.ResConfigClient import ResConfigClient
import res.utils.premises.premtools as premtools

printer_name = None
printer_ip_address = None
RES_APP_NAME = os.environ["RES_APP_NAME"] = "print_head_data_collector"
DEVICE_NAME = os.getenv("DEVICE_NAME")
RES_ENV = os.getenv("RES_ENV", "production")
RES_NAMESPACE = os.getenv("RES_NAMESPACE", "res_premises")
logger = ResLogger()


def get_printer_env_variables():
    printer_name = os.getenv("PRINTER_NAME")
    printer_ip_address = os.getenv("PRINTER_IP_ADDRESS")

    return printer_name, printer_ip_address


def import_params_from_descriptor(descriptor_file_name):  # TESTED OK
    print("Getting parameters from descriptor file....")
    with open(descriptor_file_name, "r") as descriptor:
        printer_variables = yaml.safe_load(descriptor)
        descriptor.close()

    print("Done")
    return printer_variables


def get_head_data_from_printer(printer_ip_adress, printer_name):
    print("Getting print-head-data")
    # Gets print-head data and inserts the printer name
    print_job_url = "http://{}/json/heads".format(printer_ip_adress)
    print_head_data = requests.get(print_job_url).json()
    print_head_data["Printer"] = printer_name
    return print_head_data


def get_latest_head_data():
    pass


def push_head_data_to_kafka(print_head_data, data_type):
    global printer_name
    # print('Pushing Event into Kgateway')
    if data_type == "FULL":
        url = "https://data.resmagic.io/kgateway/submitevent"
        event_data = print_head_data

        data = {
            "version": "1.0.0",
            "process_name": "print_head_data",
            "topic": "res_data_collectors.print_head_data.full_print_head_data",
            "data": event_data,
        }
        response = requests.post(url, json=data)

    elif data_type == "INSTALLED":
        url = "https://data.resmagic.io/kgateway/submitevent"
        event_data = print_head_data
        event_data["Printer"] = printer_name

        data = {
            "version": "1.0.0",
            "process_name": "print_head_data",
            "topic": "res_data_collectors.print_head_data.installed_print_head_data",
            "data": event_data,
        }
        response = requests.post(url, json=data)

    elif data_type == "UNINSTALLED":
        url = "https://data.resmagic.io/kgateway/submitevent"
        event_data = print_head_data
        event_data["Printer"] = printer_name

        data = {
            "version": "1.0.0",
            "process_name": "print_head_data",
            "topic": "res_data_collectors.print_head_data.uninstalled_print_head_data",
            "data": event_data,
        }
        response = requests.post(url, json=data)


def printer_is_on(printer_ip_address):
    # Requests the current status of the printer.
    # If no response, we'll close teh collector.
    logger.info("Getting Printer Status")
    printer_status_url = "http://{}/json/status".format(printer_ip_address)

    try:
        printer_status_response = requests.get(printer_status_url)
    except:
        return False

    if printer_status_response.status_code == 200:
        logger.info(f"Printer: {printer_name} is powered on!")
        return True

    return False


def start():
    global printer_name
    global printer_ip_address

    # 1.Get Printer Variables
    printer_name, printer_ip_address = get_printer_env_variables()

    if not printer_is_on(printer_ip_address):
        logger.info(f"Printer: {printer_name} not powered on!")
        exit()

    # 2.Get Head_Data and insert printer name
    head_data = get_head_data_from_printer(printer_ip_address, printer_name)

    # 3.Compare Head_Data
    # Future Enhancement

    # 4.Push Head_Data into Kafka
    push_head_data_to_kafka(head_data, "FULL")

    for installed_head in head_data["Installed"]:
        push_head_data_to_kafka(installed_head, "INSTALLED")

    for uninstalled_head in head_data["Uninstalled"]:
        push_head_data_to_kafka(uninstalled_head, "UNINSTALLED")


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
