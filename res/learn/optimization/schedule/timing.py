import res
import os
from functools import lru_cache
from pyairtable import Table
from res.airtable.print import MACHINE_INFO, PRETREAT_INFO
from res.flows import flow_node_attributes, FlowContext
from res.utils import secrets_client, logger

MM_PER_YD = 914.4


def normalize_printer_name(p):
    return p.replace("JP7 ", "").replace("JP5 ", "").lower()


@lru_cache(maxsize=1)
def load_machine_info():
    machines_info = MACHINE_INFO.all(
        formula=(
            "AND({Machine Type}='Printer', {Machine.Model}='MS JP7')"
            if os.environ["RES_ENV"] != "production"
            else "AND({Machine Type}='Printer', {Machine.Model}='MS JP7', {Enable Roll Assignment}=TRUE())"
        ),
        fields=[
            "Name",
            "Material Capabilities",
            "Status",
            "Active Rows At Printer",
            "Processing Roll Name",
        ],
    )
    machine_props = {
        normalize_printer_name(r["fields"]["Name"]): r["fields"] for r in machines_info
    }
    current_processing = set(
        r["fields"]["Processing Roll Name"]
        for r in machines_info
        if "Processing Roll Name" in r["fields"]
    )
    return (machine_props, current_processing)


@lru_cache(maxsize=1)
def material_print_modes():
    material_info = PRETREAT_INFO.all(
        fields=["Name", "Print Mode"],
    )
    return {
        r["fields"]["Name"]: (
            (lambda r: r if "HQ" not in r else r.replace("HQ", "") + " HQ")(
                r["fields"]["Print Mode"].replace("HighSpeed", "HiSpeed")
            )
        )
        for r in material_info
        if "Print Mode" in r["fields"]
    }


@lru_cache(maxsize=1)
def printer_speed_info():
    d = res.connectors.load("s3").read(
        "s3://res-data-development/experimental/printer_speed/data.json"
    )
    # javier slightly changed how we write the modes and removed the colon.
    d["base_rates"] = {k.replace(":", ""): v for k, v in d["base_rates"].items()}
    return d


def estimate_print_time(
    material_code,
    length_yards,
    ignore_machine_capabilities=True,
):
    speed_info = printer_speed_info()
    print_mode = material_print_modes()[material_code]
    machine_props = load_machine_info()[0]
    est_times = {}
    for machine, props in machine_props.items():
        if (
            ignore_machine_capabilities
            or material_code in props["Material Capabilities"]
        ):
            speed_mm_s = (
                speed_info["base_rates"][print_mode]
                * speed_info["printer_multipliers"][machine]
                * props["Active Rows At Printer"]
            )
            est_times[machine] = MM_PER_YD * length_yards / speed_mm_s
    return est_times
