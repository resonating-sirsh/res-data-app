import re
import os
import pandas as pd
from datetime import datetime, timedelta
from io import BytesIO, StringIO
from queries import *
from typing import TypedDict

from res.connectors.quickbooks.QuickbooksClient import QuickBooksClient
from res.connectors.box import BoxConnector
from res.utils import logger, secrets_client, ping_slack


def get_enabled_sales_form_custom_fields(qbo_client: QuickBooksClient):
    """
    Queries Quickbooks to retrieve a preferences object. Retrieves custom
    sales form fields that are enabled within the account and returns as a
    dict where each field's definition ID is a key and the field name is
    a value

    Currently, Quickbooks mapping to each created custom field is immutable and
    only the first three will be returned, whether they are active or not. The
    only way to "remove" an inactive custom field in the first three created
    custom fields is to replace its information with something else
    """

    # Get custom fields from the account preferences object. This is a list
    # containing a dict indicating which custom fields are enabled for sales
    # forms (invoices are sales forms).
    custom_field_dict_list = [
        i
        for i in qbo_client.client.query("select * from preferences")["QueryResponse"][
            "Preferences"
        ]
        if "SalesFormsPrefs" in i
    ][0]["SalesFormsPrefs"]["CustomField"]

    custom_fields = {}
    excluded_fields = []
    always_included_field_names = [
        "Total Orders",
        "Total Items",
        "Billing Period",
    ]

    for i in custom_field_dict_list:

        if i["CustomField"][0]["Type"] == "BooleanType":

            for custom_field_dict in i["CustomField"]:

                # A custom field's definition ID is the numeric string at the
                # end of its Name
                definition_id = re.findall(r"\d+$", custom_field_dict["Name"])[0]
                is_enabled = custom_field_dict["BooleanValue"]

                # Check if definition id has been added, then remove it if the
                # field is not enabled
                if definition_id in custom_fields:

                    if is_enabled:

                        continue

                    else:

                        # Override for field names we know we always want
                        # regardless
                        if (
                            custom_fields[definition_id]
                            not in always_included_field_names
                        ):

                            del custom_fields[definition_id]

                else:

                    if is_enabled:

                        custom_fields[definition_id] = None

                    else:

                        excluded_fields.append(definition_id)

        elif i["CustomField"][0]["Type"] == "StringType":

            for custom_field_dict in i["CustomField"]:

                # A custom field's definition ID is the numeric string at the
                # end of its Name
                definition_id = re.findall(r"\d+$", custom_field_dict["Name"])[0]
                field_name = custom_field_dict["StringValue"]

                if definition_id in custom_fields:

                    custom_fields[definition_id] = field_name

                else:

                    if (
                        definition_id not in excluded_fields
                        or field_name in always_included_field_names
                    ):

                        custom_fields[definition_id] = field_name

    return custom_fields


def get_invoice_by_doc_number(doc_number: str, qbo_client: QuickBooksClient):
    """
    Looks up an invoice by its document number using a Quickbooks query. If
    there is not a matching invoice this function returns None
    """

    resp = qbo_client.client.query(
        f"select * from invoice where DocNumber = '{doc_number}'"
    )
    result_list = resp["QueryResponse"].get("Invoice", [None])
    invoice = result_list[0]

    # Multiple invoices with the same doc_number shouldn't be possible but
    # adding just in case
    if len(result_list) > 1:

        logger.warning(
            "Warning, DocNumber matches multiple invoices. Returning first in" " list"
        )

    return invoice


def validate_brand_folder_structure(
    box_connector: BoxConnector, utilized_date_str, start_folder_id=None
):
    """
    Checks brand folder to make sure there exists a folder for the current
    month with required Resonance Companies / Manufacturing + csv and pdf
    sub-folders. Returns a dict containing the IDs for those folders
    """

    if not start_folder_id:
        if os.getenv("RES_ENV", "development") == "production":

            # Brands folder
            start_folder_id = 143847228389

        else:

            # Dev folder within Brands folder
            start_folder_id = 250831797064

    utilized_date = datetime.strptime(utilized_date_str, "%Y-%m-%d")
    utilized_month = utilized_date.strftime("%B")
    utilized_year = utilized_date.strftime("%Y")

    # Create dict to store eventual folder IDs for pdf and csv files
    target_folders = {
        "Resonance Companies": {
            "pdf": None,
            "csv": None,
        },
        "Resonance Manufacturing": {
            "pdf": None,
            "csv": None,
        },
    }

    def create_sub_folders(start_folder_id, utilized_folder_type, year, month_name):
        """
        Creates sub-folders beneath a folder and returns a dict with the
        resultant file folder ids at the deepest folder directory
        """

        output = None

        if utilized_folder_type == "root":

            output = {
                "Resonance Companies": {
                    "pdf": None,
                    "csv": None,
                },
                "Resonance Manufacturing": {
                    "pdf": None,
                    "csv": None,
                },
            }

            # Create Resonance folders and all of their sub_folders. Returns a whole
            # target dict
            res_folders = [
                box_connector.client.folder(start_folder_id).create_subfolder(
                    "Resonance Companies"
                ),
                box_connector.client.folder(start_folder_id).create_subfolder(
                    "Resonance Manufacturing"
                ),
            ]

            for res_folder in res_folders:

                # Create sub-folders
                year_folder = box_connector.client.folder(
                    res_folder.id
                ).create_subfolder(year)
                month_folder = year_folder.create_subfolder(month_name)
                pdf_folder = month_folder.create_subfolder("pdf")
                csv_folder = month_folder.create_subfolder("csv")

                # Add folder IDs to output dict
                output[res_folder.name]["pdf"] = pdf_folder.id
                output[res_folder.name]["csv"] = csv_folder.id

        elif utilized_folder_type == "res_folder":

            year_folder = box_connector.client.folder(start_folder_id).create_subfolder(
                year
            )
            month_folder = year_folder.create_subfolder(month_name)
            pdf_folder = month_folder.create_subfolder("pdf")
            csv_folder = month_folder.create_subfolder("csv")
            output = {"pdf": pdf_folder.id, "csv": csv_folder.id}

        elif utilized_folder_type == "year":

            month_folder = box_connector.client.folder(
                start_folder_id
            ).create_subfolder(month_name)
            pdf_folder = month_folder.create_subfolder("pdf")
            csv_folder = month_folder.create_subfolder("csv")
            output = {"pdf": pdf_folder.id, "csv": csv_folder.id}

        elif utilized_folder_type == "month":

            pdf_folder = box_connector.client.folder(start_folder_id).create_subfolder(
                "pdf"
            )
            csv_folder = box_connector.client.folder(start_folder_id).create_subfolder(
                "csv"
            )
            output = {"pdf": pdf_folder.id, "csv": csv_folder.id}

        return output

    # Check if the start folder contains folders for Resonance Manufacturing and
    # Resonance Companies
    res_folders = [
        i
        for i in box_connector.get_folder_items(start_folder_id)
        if i.type == "folder"
        and (i.name == "Resonance Companies" or i.name == "Resonance Manufacturing")
    ]
    res_folders_names = [i.name for i in res_folders]

    if (
        "Resonance Companies" in res_folders_names
        and "Resonance Manufacturing" in res_folders_names
    ):

        # Check if each of those folders contains a folder with the utilized_date year
        for res_folder in res_folders:

            year_folders = [
                i
                for i in box_connector.get_folder_items(res_folder.id)
                if i.name == utilized_year
            ]
            year_folder = year_folders[0] if 0 < len(year_folders) else None

            if year_folder:

                # Check if that folder contains a folder for the current month
                month_folders = [
                    i
                    for i in box_connector.get_folder_items(year_folder.id)
                    if i.type == "folder" and i.name == utilized_month
                ]
                month_folder = month_folders[0] if 0 < len(month_folders) else None

                if month_folder:

                    # Check if that folder contains a pdf and csv folder
                    file_folders = [
                        i
                        for i in box_connector.get_folder_items(month_folder.id)
                        if i.type == "folder" and (i.name == "pdf" or i.name == "csv")
                    ]
                    file_folders_names = [i.name for i in file_folders]

                    if "pdf" in file_folders_names and "csv" in file_folders_names:

                        pdf_folder_id = [i.id for i in file_folders if i.name == "pdf"][
                            0
                        ]
                        csv_folder_id = [i.id for i in file_folders if i.name == "csv"][
                            0
                        ]

                        # Add identified folder IDs to target dict
                        logger.info(
                            f"Correct folder format identified for"
                            f" {res_folder.name}; adding folder IDs to output"
                            " dictionary"
                        )
                        target_folders[res_folder.name]["pdf"] = pdf_folder_id
                        target_folders[res_folder.name]["csv"] = csv_folder_id

                    # If both names are not present, check if one is present
                    elif "pdf" in file_folders_names or "csv" in file_folders_names:

                        # Uses a list to get the missing type programmatically
                        needed_folder_names = ["pdf", "csv"]
                        found_folder_name = file_folders_names[0]
                        needed_folder_names.remove(found_folder_name)
                        needed_folder_name = needed_folder_names[0]

                        logger.info(
                            f"Missing {needed_folder_name} in year folder; adding"
                        )

                        # Create needed folder
                        needed_folder = month_folder.create_subfolder(
                            needed_folder_name
                        )

                        # Add to target folder
                        target_folders[res_folder.name][found_folder_name] = (
                            file_folders[0]
                        ).id
                        target_folders[res_folder.name][
                            needed_folder_name
                        ] = needed_folder.id

                    else:

                        logger.info(
                            f"Creating both file folders in {utilized_month} month folder"
                        )
                        target_folders[res_folder.name] = create_sub_folders(
                            month_folder.id, "month", utilized_year, utilized_month
                        )

                else:

                    logger.info(
                        f"Creating month folder and sub-folders for"
                        f" {res_folder.name} year {utilized_year}"
                    )
                    target_folders[res_folder.name] = create_sub_folders(
                        year_folder.id, "year", utilized_year, utilized_month
                    )

            else:

                logger.info(
                    f"Creating year folder and sub-folders for {res_folder.name}"
                )
                target_folders[res_folder.name] = create_sub_folders(
                    res_folder.id, "res_folder", utilized_year, utilized_month
                )

    # If both names are not present, check if one is present
    elif (
        "Resonance Companies" in res_folders_names
        or "Resonance Manufacturing" in res_folders_names
    ):

        # Uses a list to get the missing type programmatically
        needed_res_folder_names = ["Resonance Companies", "Resonance Manufacturing"]
        found_res_folder_name = res_folders_names[0]
        needed_res_folder_names.remove(found_res_folder_name)
        needed_res_folder_name = needed_res_folder_names[0]

        # Identify found Resonance company folder and create needed Resonance
        # company folder
        found_res_folder = res_folders[0]
        needed_res_folder = box_connector.client.folder(
            start_folder_id
        ).create_subfolder(needed_res_folder_name)

        # Create sub-folders in created folder
        target_folders[needed_res_folder_name] = create_sub_folders(
            needed_res_folder.id, "res_folder", utilized_year, utilized_month
        )

        # Check found folder for format
        year_folders = [
            i
            for i in box_connector.get_folder_items(found_res_folder.id)
            if i.name == utilized_year
        ]
        year_folder = year_folders[0] if 0 < len(year_folders) else None

        if year_folder:

            # Check if that folder contains a folder for the current month
            month_folders = [
                i
                for i in box_connector.get_folder_items(year_folder.id)
                if i.type == "folder" and i.name == utilized_month
            ]
            month_folder = month_folders[0] if 0 < len(month_folders) else None

            if month_folder:

                # Check if that folder contains a pdf and csv folder
                file_folders = [
                    i
                    for i in box_connector.get_folder_items(month_folder.id)
                    if i.type == "folder" and (i.name == "pdf" or i.name == "csv")
                ]
                file_folders_names = [i.name for i in file_folders]

                if "pdf" in file_folders_names and "csv" in file_folders_names:

                    pdf_folder_id = [i.id for i in file_folders if i.name == "pdf"][0]
                    csv_folder_id = [i.id for i in file_folders if i.name == "csv"][0]

                    logger.info(
                        f"Correct folder format identified for"
                        f" {found_res_folder_name}; adding folder IDs to output"
                        " dictionary"
                    )
                    target_folders[found_res_folder_name]["pdf"] = pdf_folder_id
                    target_folders[found_res_folder_name]["csv"] = csv_folder_id

                # If both names are not present, check if one is present
                elif "pdf" in file_folders_names or "csv" in file_folders_names:

                    # Uses a list to get the missing type programmatically
                    needed_file_folder_names = ["pdf", "csv"]
                    found_file_folder_name = file_folders_names[0]
                    needed_file_folder_name = (
                        needed_file_folder_names.remove(found_file_folder_name)
                    )[0]

                    # Create needed folder
                    needed_file_folder = month_folder.create_subfolder(
                        needed_file_folder_name
                    )

                    # Assign IDs
                    found_file_folder = file_folders[0]
                    target_folders[found_res_folder_name][
                        found_file_folder_name
                    ] = found_file_folder.id
                    target_folders[needed_res_folder_name][
                        needed_file_folder_name
                    ] = needed_file_folder.id

                else:

                    pdf_folder = month_folder.create_subfolder("pdf")
                    target_folders[needed_res_folder_name]["pdf"] = pdf_folder.id
                    csv_folder = month_folder.create_subfolder("csv")
                    target_folders[needed_res_folder_name]["csv"] = csv_folder.id

            else:

                logger.info(
                    f"Creating month folder and sub-folders for"
                    f" {found_res_folder_name} year {utilized_year}"
                )
                target_folders[found_res_folder_name] = create_sub_folders(
                    year_folder.id, "year", utilized_year, utilized_month
                )

        else:

            print(f"Creating year folder and sub-folders for {found_res_folder_name}")
            target_folders[found_res_folder_name] = create_sub_folders(
                found_res_folder.id, "res_folder", utilized_year, utilized_month
            )

    else:

        print(
            "Creating folders for Resonance Companies and Resonance Manufacturing"
            " as well as their sub-folders"
        )
        target_folders = create_sub_folders(
            start_folder_id, "root", utilized_year, utilized_month
        )

    return target_folders


def low_key(input: list[dict]):

    output = []
    for i in input:

        output.append({k.lower(): v for k, v in i.items()})

    return output


def get_last_complete_week_dates() -> tuple[str, str]:
    """
    Gets last complete week's dates using the current date and some deltas
    """

    today = datetime.today()
    weekday = today.weekday()
    start_delta = timedelta(days=weekday)
    start_of_week = (today - start_delta - timedelta(weeks=1)).date()
    end_of_week = start_of_week + timedelta(days=6)

    # Return start and end dates
    return str(start_of_week), str(end_of_week)


class UninvoicedLineItem(TypedDict):

    app_name: str
    order_line_item_ids: str
    order_id: str
    order_number: str
    sku: str
    is_in_airtable: bool
    error: str


def add_and_alert_uninvoiced_line_items(
    box_connector: BoxConnector, uninvoiced_items: list[UninvoicedLineItem]
) -> None:
    """
    Adds missing costs to a Box file and alerts the DR Shipping channel
    with information about which line item records need to have their
    shipping costs added. Also alerts the alerts_data_quality channel
    with all errors
    """

    # Retrieve Box file for uninvoiced line items and add new rows
    if os.getenv("RES_ENV", "development") == "production":

        file_id = "1474713616208"

    else:

        file_id = "1474751250296"

    uninvoiced_line_items_stream = box_connector.client.file(file_id).content()
    df = pd.read_csv(BytesIO(uninvoiced_line_items_stream), index_col=False)
    df = df._append(uninvoiced_items, ignore_index=True)
    df.drop_duplicates(keep="first", inplace=True)

    # Add updated data to Box
    in_memory_csv = StringIO()
    df.to_csv(in_memory_csv, index=False)
    in_memory_csv.seek(0)
    box_connector.upload_new_version_from_stream(file_id, in_memory_csv)

    # Alert either dr_shipping or alerts_data_quality Slack channels based on
    # error type
    shipping_errors = [
        f"Order line item ids - {i.get('order_line_item_ids')} - Order number {i.get('order_number')} - Order record {i.get('order_id')} - SKU {i.get('sku')}"
        for i in uninvoiced_items
        if i.get("error").lower() == "missing fulfillment shipping cost"
        and i.get("is_in_airtable")
    ]

    logger.warning("\n".join(shipping_errors))

    # For production environments also ping slack
    if os.getenv("RES_ENV", "development") == "production" and len(shipping_errors) > 0:

        ping_slack(
            # Pings Carmen Ayala (DR Shipping Owner)
            "<@U7K1N5KRQ>\n"
            + "Fulfilled line items missing shipping costs:\n\n"
            + "\n".join(shipping_errors),
            "drshipping",
        )

    all_errors = [
        f"App {i.get('app_name')} - Order line item ids - {i.get('order_line_item_ids')} - Order record {i.get('order_id')} - SKU {i.get('sku')} - error {i.get('error')}"
        for i in uninvoiced_items
    ]

    logger.warning("\n".join(all_errors))

    # For production environments also ping slack
    if os.getenv("RES_ENV", "development") == "production" and len(all_errors) > 0:

        ping_slack(
            # Pings Jay Gibbs (Data Team Lead)
            "<@U043JU9F4EN>\n"
            + "Invoice Generation Errors:\n\n"
            + "\n".join(all_errors),
            "alerts-data-quality",
        )
