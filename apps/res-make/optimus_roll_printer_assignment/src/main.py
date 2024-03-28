import pandas as pd
import json
import os
import sys
import time

from res.utils import logger, secrets

from src.utils import statsd_incr
from src.data import (
    get_input_data,
    construct_output_messages,
    send_messages,
)
from src.solver import roll_printer_assignment


def handler(event, context):
    start_time = time.time()

    dry_run = event.get("dry_run", False)
    if dry_run:
        logger.info("running in dry run mode")

    else:
        logger.info("running with output to kafka")

    verbose_solver_logging = event.get("verbose_solver_logging", False)

    solver_settings = event["solver_settings"]
    logger.info(f"solver settings: {solver_settings}")

    rolls = pd.DataFrame.from_records(context["rolls"]).set_index("roll_record_id")
    printers = pd.DataFrame.from_records(context["printers"]).set_index("printer_name")
    assets = pd.DataFrame.from_records(context["assets"]).set_index("print_asset_id")

    if (
        rolls.roll_length_yards.max()
        > solver_settings["constraints"]["max_print_queue_yards"]
    ):
        logger.info(
            f"Longest roll ({rolls.roll_length_yards.max()} yds exceeds max allowed print queue ({solver_settings['constraints']['max_print_queue_yards']} yds)! Increasing allowed print queue."
        )
        solver_settings["constraints"][
            "max_print_queue_yards"
        ] = rolls.roll_length_yards.max()

    solver_status, objective_value, roll_printer_assignments = roll_printer_assignment(
        rolls,
        printers,
        assets,
        solver_settings,
        verbose_solver_logging,
    )

    solution_info = (
        pd.DataFrame(
            roll_printer_assignments, columns=["roll_record_id", "printer_name", "rank"]
        )
        .join(printers, on="printer_name")
        .set_index("roll_record_id")
        .join(
            rolls[
                [
                    "roll_name",
                    "material_code",
                    "roll_length_yards",
                    "total_nested_length_yards",
                    "assigned_printer_name",
                ]
            ]
        )
        .assign(is_new_assignment=lambda df: df.assigned_printer_name.isnull())
        .reset_index()
    )
    logger.info(
        solution_info[
            [
                "printer_name",
                "rank",
                "is_new_assignment",
                "roll_name",
                "material_code",
                "roll_length_yards",
            ]
        ]
    )

    # To do, investigate reasons the problem becomes infeasible if it occurs
    reason_infeasible = None

    output = construct_output_messages(
        solver_status,
        objective_value,
        solution_info,
        solver_settings,
        reason_infeasible,
    )

    if not dry_run:
        send_messages(output)

    end_time = time.time()
    process_time = end_time - start_time
    logger.info(f"{process_time} Seconds to Complete Process")

    logger.info("Done")


def initialize():
    if "AIRTABLE_API_KEY" not in os.environ:
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY")


if __name__ == "__main__":
    event = json.loads(sys.argv[1])
    if len(sys.argv) > 2:
        data = json.loads(sys.argv[2])
    else:
        initialize()
        data = get_input_data(event)

    logger.info("beginning to run optimus roll-printer assignment")
    statsd_incr("optimus_loading", 1)

    handler(
        event,
        data,
    )

    statsd_incr("optimus_complete", 1)
