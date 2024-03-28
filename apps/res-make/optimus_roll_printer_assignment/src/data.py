import json
import os
import numpy as np
import pandas as pd
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache

from pyairtable import Table
from tenacity import retry, wait_fixed, stop_after_attempt

from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.utils import logger
from res.utils.airtable import format_filter_conditions, multi_option_filter
from res.utils.dataframes import dataframe_from_airtable

ENV = os.getenv("RES_ENV", "development")

today = date.today().strftime("%Y-%m-%d")
now = datetime.now().strftime("%H:%M:%S")

PRINT_BASE_ID = "apprcULXTWu33KFsh"
FULFILLMENT_BASE_ID = "appfaTObyfrmPHvHc"

PRINTER_COLUMNS = {
    "Name": "printer_name",
    "Material Capabilities": "material_capabilities",
    "Status": "printer_status",
    "Model": "printer_model",
    "Machine Type": "machine_type",
    "Active Roll Queue": "active_roll_queue",
}

INACTIVE_PRINTER_STATUSES = (
    "FAILURE",
    "OFF",
    "PLANNED MAINTENANCE",
    "UNPLANNED MAINTENANCE",
)

ROLL_COLUMNS = {
    "Name": "roll_name",
    "Material Code": "material_code",
    "Pretreated Length": "roll_length_yards",
    "Total Nested Length": "total_nested_length_yards",
    "pretreated_at": "pretreated_at_manual",
    "Pretreatment Exit Date At": "pretreated_at_flow",
    "Nested Asset Sets": "assigned_nests",
    "Print Assets (flow) 3": "assigned_print_assets",
    "Print Priority": "roll_print_priority",
    "Prioritize Roll for Print": "is_prioritized",
    "Roll Utilization (Nested Assets)": "roll_utilization_nested_assets",
    "Assigned Printer Name v3": "assigned_printer_name",
    "Roll Status": "roll_status",
    "Printer": "currently_printing_on_printer",
}

PRINT_FILE_COLUMNS = {
    "Key": "print_file_key",
    "Material Code": "material_code",
    "Nested Length in Yards": "nested_length_yards",
    "Nests": "nest_record_ids",
    "Rolls": "assigned_roll",
    "Rank": "rank_on_roll",
    "Assigned Printer": "assigned_printer",
    "Print Queue": "print_queue",
    "Print Asset Downloaded?": "is_downloaded",
    "Asset Path": "asset_path",
}

NEST_COLUMNS = {
    "Material Code": "material_code",
    "Asset Ids": "asset_ids",
    "Nested Length in Yards": "nested_length",
    "Asset Set Id": "id",
    "Key": "nest_jobkey",
    "Assets": "assets",
    "Asset Path": "asset_path",
    "Rolls": "assigned_roll",
    "Assigned Printer": "assigned_printer",
    "Print Queue": "print_queue",
}

ASSET_COLUMNS = {
    "Key": "asset_key",
    "Material Code": "material_code",
    "Days Since Customer Ordered (days)": "days_since_ordered",
    "Emphasis": "emphasis",
    "Belongs to Order": "order_key",
    "Rank": "rank",
    "Pieces Type": "piece_type",
}


@lru_cache(maxsize=1)
def cnx_machines():
    # TODO: better way of handling this via env vars or dep injection
    return Table(os.environ.get("AIRTABLE_API_KEY"), PRINT_BASE_ID, "tbl4DbNHQPMwlLrGY")


@lru_cache(maxsize=1)
def cnx_print_rolls():
    return Table(os.environ.get("AIRTABLE_API_KEY"), PRINT_BASE_ID, "tblSYU8a71UgbQve4")


@lru_cache(maxsize=1)
def cnx_nests():
    return Table(os.environ.get("AIRTABLE_API_KEY"), PRINT_BASE_ID, "tbl7n4mKIXpjMnJ3i")


@lru_cache(maxsize=1)
def cnx_print_files():
    return Table(os.environ.get("AIRTABLE_API_KEY"), PRINT_BASE_ID, "tblAQcPuKUDVfU7Fx")


@lru_cache(maxsize=1)
def cnx_print_assets():
    return Table(os.environ.get("AIRTABLE_API_KEY"), PRINT_BASE_ID, "tblwDQtDckvHKXO4w")


@retry(wait=wait_fixed(5), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    response = getattr(cnx, method)(*args, **kwargs)
    logger.debug(json.dumps(response))
    return response


@lru_cache(maxsize=1)
def kafka_client():
    return ResKafkaClient()


def get_input_data(event):
    solver_settings = event["solver_settings"]

    printers_to_consider = event.get("printers")
    if "all" not in printers_to_consider:
        logger.info(
            f"considering only the following printers: {', '.join(printers_to_consider)}"
        )

    materials_to_consider = event.get("materials")
    if "all" not in materials_to_consider:
        logger.info(
            f"considering only the following materials: {', '.join(materials_to_consider)}"
        )

    print_files = get_print_files(materials_to_consider)
    rolls_to_filter = get_rolls_with_incomplete_printfiles(print_files)

    rolls = get_rolls(materials_to_consider, printers_to_consider, rolls_to_filter)

    assets = get_print_assets(materials_to_consider)

    # nests = get_nests(materials_to_consider)

    printers = get_printers(printers_to_consider).pipe(
        compute_print_queues, rolls, solver_settings
    )
    logger.info(
        printers[
            [
                "printer_status",
                "printer_model",
                "material_capabilities",
                "current_print_queue_yards",
                "print_queue_remaining_yards",
            ]
        ]
    )

    return {
        "rolls": rolls.to_dict(orient="records"),
        "printers": printers.to_dict(orient="records"),
        "assets": assets.to_dict(orient="records"),
    }


def get_printers(printers_to_consider):
    filter_formula = format_filter_conditions(
        ["{Machine Type}='Printer'"] + multi_option_filter("Name", printers_to_consider)
    )

    logger.info("fetching printer info")
    printers = dataframe_from_airtable(
        call_airtable(
            cnx_machines(), formula=filter_formula, fields=list(PRINTER_COLUMNS)
        ),
        fields=list(PRINTER_COLUMNS),
    )
    logger.info("done, got %d printers" % len(printers))

    return printers.rename(
        columns={**{"id": "printer_record_id"}, **PRINTER_COLUMNS}
    ).assign(
        is_accepting_rolls=lambda df: ~df.printer_status.isin(
            INACTIVE_PRINTER_STATUSES
        ),
        active_roll_queue=lambda df: df.active_roll_queue.map(eval),
        material_capabilities=lambda df: df.material_capabilities.map(
            lambda x: x if isinstance(x, list) else []
        ),
    )


def get_rolls(materials_to_consider, printers_to_consider, rolls_to_filter):
    """
    Print ONE Ready:
    AND({Roll Status}!='5. Done',
        {Roll Status}!='4. Error',
        {Current Substation}='Print',
        {Flag for Review}!=TRUE(),
        {Nested Asset Sets}=BLANK(),
        {Roll Length (yards)}>=5)
    """
    roll_filter_formula = format_filter_conditions(
        [
            "{Current Substation}='Print'",
            "{Ready for Print}=TRUE()",
            "{Nested Asset Set Count}>0",
            "{Print Assets (flow) 3}!=''",
            "{Asset Print Queue}='TO DO'",
            "{Roll Status}!='5. Done'",
            "{Roll Status}!='4. Error'",
            "{Flag for Review}!=TRUE()",
        ]
        + multi_option_filter(
            "Material Code", materials_to_consider, is_list_column=True
        )
        + multi_option_filter("Assigned Printer Name v3", printers_to_consider + [""])
        + multi_option_filter("_record_id", rolls_to_filter, is_exclude_filter=True)
    )

    logger.info("fetching roll info")

    rolls = (
        dataframe_from_airtable(
            call_airtable(
                cnx_print_rolls(),
                formula=roll_filter_formula,
                fields=list(ROLL_COLUMNS),
            ),
            fields=list(ROLL_COLUMNS),
        )
        .rename(columns={**{"id": "roll_record_id"}, **ROLL_COLUMNS})
        .fillna({"is_prioritized": False})
        .assign(
            roll_length_yards=lambda df: df.roll_length_yards.map(lambda x: x.pop()),
            roll_print_priority_unit_score=lambda df: df.roll_print_priority.map(
                lambda x: 1 / (1 + np.exp(-x))
            ),
            material_code=lambda df: df.material_code.map(lambda x: x[0]),
            is_currently_printing=lambda df: df[
                "currently_printing_on_printer"
            ].notnull(),
            pretreated_at=lambda df: df.pretreated_at_flow.fillna(
                df.pretreated_at_manual
            ).map(pd.to_datetime),
        )
        .assign(
            days_since_pretreatment=lambda df: (
                datetime.now(timezone(timedelta(0))) - df.pretreated_at
            ).dt.days
        )
    )

    logger.info(
        "done, got %d rolls (%d not assigned to a printer)"
        % (rolls.shape[0], rolls.query("assigned_printer_name.isnull()").shape[0])
    )
    logger.debug(rolls)

    return rolls


def _remaining_yards_to_print(printer, max_print_queue_yards):
    if not printer["is_accepting_rolls"]:
        return 0
    else:
        return max(0, max_print_queue_yards - printer["current_print_queue_yards"])


def compute_print_queues(printers, rolls, solver_settings):
    print_queues = (
        rolls.sort_values(
            by="is_currently_printing", ascending=False
        )  # to do: sort by rank?
        # .query("roll_status.isin(('3. Unassigned', '2. Printing'))")
        .reset_index()
        .groupby("assigned_printer_name")
        .agg(
            {
                "total_nested_length_yards": "sum",
                "material_code": list,
                "roll_record_id": list,
            }
        )
        .rename(
            columns={
                "total_nested_length_yards": "current_print_queue_yards",
                "material_code": "current_print_queue_materials",
                "roll_record_id": "current_print_queue_roll_ids",
            }
        )
    )

    return (
        printers.join(print_queues, on="printer_name")
        .fillna({"current_print_queue_yards": 0})
        .assign(
            print_queue_remaining_yards=lambda df: df.apply(
                lambda x: _remaining_yards_to_print(
                    x, solver_settings["constraints"]["max_print_queue_yards"]
                ),
                axis=1,
            ),
            current_print_queue_materials=lambda df: df.current_print_queue_materials.map(
                lambda x: x if isinstance(x, list) else []
            ),
            current_print_queue_roll_ids=lambda df: df.current_print_queue_roll_ids.map(
                lambda x: x if isinstance(x, list) else []
            ),
        )
    )


def get_print_files(materials_to_consider):
    filter_formula = format_filter_conditions(
        [
            "{Rolls}!=BLANK()",
            "{Rolls}!=''",
            "{Print Queue}!='CANCELED'",
            "{Print Queue}!='PRINTED'",
        ]
        + multi_option_filter("Material", materials_to_consider, is_list_column=True)
    )

    logger.info("fetching print file data")
    return (
        dataframe_from_airtable(
            call_airtable(
                cnx_print_files(),
                formula=filter_formula,
                fields=list(PRINT_FILE_COLUMNS),
            ),
            fields=list(PRINT_FILE_COLUMNS),
        )
        .rename(columns={**{"id": "print_file_record_id"}, **PRINT_FILE_COLUMNS})
        .assign(
            material_code=lambda x: x.material_code.apply(lambda t: t.pop()),
            assigned_roll=lambda x: x.assigned_roll.apply(lambda t: t.pop()),
            is_waiting_for_printfile=lambda x: x.asset_path.isnull(),
        )
    )


def get_rolls_with_incomplete_printfiles(print_files):
    rolls_to_filter = list(
        print_files.query("is_waiting_for_printfile").assigned_roll.unique()
    )
    logger.info(f"filtering out {len(rolls_to_filter)} roll(s)")
    logger.debug(rolls_to_filter)

    return rolls_to_filter


def get_nests(materials_to_consider):
    filter_formula = format_filter_conditions(
        ["{Rolls}!=BLANK()", "{Rolls}!=''", "{Asset Path}!=BLANK()", "{Asset Ids}!=''"]
        + multi_option_filter(
            "Material Code", materials_to_consider, is_list_column=True
        )
    )

    logger.info("fetching nest data")
    return (
        dataframe_from_airtable(
            call_airtable(cnx_nests(), formula=filter_formula, fields=fields),
            fields=fields,
        )
        .rename(columns={**{"id": "nest_record_id"}, **{NEST_COLUMNS}})
        .assign(
            asset_ids=lambda x: x.asset_ids.apply(lambda t: tuple(sorted(t))),
            material_code=lambda x: x.material_code.apply(lambda t: t.pop()),
            assigned_roll=lambda x: x.Rolls.apply(lambda t: t.pop()),
        )
    )


def get_print_assets(materials_to_consider):
    filter_formula = format_filter_conditions(
        [
            "{Rolls Assignment v3}!=BLANK()",
            "{Rolls Assignment v3}!=''",
            "{Print Queue}!='PRINTED'",
        ]
        + multi_option_filter(
            "Material Code", materials_to_consider, is_list_column=True
        )
    )

    logger.info("fetching print assets")

    assets = (
        dataframe_from_airtable(
            call_airtable(
                cnx_print_assets(), formula=filter_formula, fields=ASSET_COLUMNS.keys()
            )
        )
        .rename(columns={**{"id": "print_asset_id"}, **ASSET_COLUMNS})
        .fillna({"emphasis": 1.0})
    )

    logger.info(f"done. found {len(assets)} print assets")
    logger.debug(assets)

    return assets


def construct_printer_queue_message(
    printer_record_id, printer_name, solution_info, task_key, timestamp
):
    existing_queue_length = solution_info.query(
        "~is_new_assignment"
    ).total_nested_length_yards.sum()
    new_queue_length = solution_info.query(
        "is_new_assignment"
    ).total_nested_length_yards.sum()

    return {
        "printer_record_id": printer_record_id,
        "printer_name": printer_name,
        "roll_record_ids": solution_info["roll_record_id"].to_list(),
        "roll_names": solution_info["roll_name"].to_list(),
        "print_length_existing_queue_yards": existing_queue_length,
        "print_length_additional_queue_yards": new_queue_length,
        "print_length_total_queue_yards": existing_queue_length + new_queue_length,
        "task_key": task_key,
        "created_at": timestamp,
    }


def construct_output_messages(
    solver_status,
    objective_value,
    solution_info,
    solver_settings,
    reason_infeasible=None,
):
    timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")
    task_key = f"optimus-roll-printer-assignment-{timestamp}"

    messages = [
        {
            "topic": "res_make.optimus.test_roll_printer_assignment_metadata",
            "message": {
                "objective_value": objective_value,
                "solver_status": solver_status,
                "reason_infeasible": reason_infeasible,
                "solver_settings": str(solver_settings),
                "solution_message_count": len(solution_info),
                "task_key": task_key,
                "created_at": timestamp,
            },
        }
    ]

    for (printer_record_id, printer_name), assignments in solution_info.groupby(
        ["printer_record_id", "printer_name"]
    ):
        printer_assigned_roll_count = len(assignments)

        topic_printer_queue = "res_make.optimus.test_printer_queue"
        msg_printer_queue = construct_printer_queue_message(
            printer_record_id, printer_name, assignments, task_key, timestamp
        )
        messages.append(
            {
                "topic": topic_printer_queue,
                "message": msg_printer_queue,
            }
        )

        for assignment in assignments[["roll_name", "rank"]].itertuples():
            # TO DO: add roll record id, printer record id, is_new_assignment, printer_assigned_yards
            topic_roll = "res_make.optimus.test_roll_printer_assignment"
            msg_roll = {
                "printer_name": printer_name,
                "roll_key": assignment.roll_name,
                "rank": assignment.rank,
                "printer_assigned_roll_count": printer_assigned_roll_count,
                "task_key": task_key,
                "created_at": timestamp,
            }
            messages.append(
                {
                    "topic": topic_roll,
                    "message": msg_roll,
                }
            )

    return messages


def send_messages(output):
    for data in output:
        send_kafka_message(kafka_client(), data["topic"], data["message"])


def send_kafka_message(client, topic, message):
    try:
        logger.info(f"Sending message to Kafka {topic}")
        logger.debug(message)
        with ResKafkaProducer(client, topic) as kafka_producer:
            kafka_producer.produce(message)
    except Exception as e:
        logger.error("Failed to send message to Kafka")
        logger.info(e)
