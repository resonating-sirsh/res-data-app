import random
import math
import numpy as np
import pandas as pd
import pytz
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import time
import res
from collections import Counter
from matplotlib.pyplot import cm
from datetime import datetime, timedelta
from res.airtable.misc import HOLIDAYS_TABLE
from res.airtable.print import (
    ROLLS,
    PRETREAT_INFO,
    MATERIAL_INFO,
)
from res.airtable.schedule import (
    SCHEDULE_ASSETS,
    SCHEDULE_MATRIX,
    SCHEDULE_PPU_SCHEDULE,
    SCHEDULE_PRINTER_IDS,
    SCHEDULE_PPU_CONSTRUCTION_ORDER,
    SCHEDULE_PPU_CREATION,
    SCHEDULE_HEADERS_FOOTERS,
    SCHEDULE_WAREHOUSE,
    SCHEDULE_LEADERS,
    SCHEDULE_LOCATIONS,
    SCHEDULE_MATERIALS,
)
from res.utils import logger, ping_slack
from res.flows.make.print.rebalance_print_queues import reassign_roll
from res.flows.make.nest.clear_nest_roll_assignments import clear_nest_roll_assignments
from .timing import estimate_print_time
from .station_times import get_station_runtimes

PRINTER_MAINTAIN_HOURS = 4

# -- PHYSICAL PARAMETERS
# - unit conversion
METERS_PER_YD = 0.9144
# - speed of rolling maching for PPU assembly
PPU_ROLLING_METERS_PER_SECOND = 9 / 60.0
# - time taken to sew one join in a PPU (seconds)
CONCAT_PPU_TIME = 360
# - time to change over the chemicals in the pretreatment maching
PRETREAT_CHANGEOVER_TIME = 1800
MIN_SILK_YARDS_TO_PRETREAT = 30
SILK_YARDS_FOR_FULL_BATCH = 50
# - steamer speed
STEAMER_GROUP_1_YARDS_PER_SECOND = 2.2 / (60 * METERS_PER_YD)
STEAMER_GROUP_2_YARDS_PER_SECOND = 1.5 / (60 * METERS_PER_YD)
# - how many yards are in the printer dryer (power-d)
PRINT_DRYER_YARDS = 12
# -- SLOP PARAMETERS
# - some slop for ppu creation
PPU_CREATION_SETUP_TIME = 600
# - estimated time needed between successive PPUs in pretreat
PRETREAT_SETUP_TIME = 1 * 60
PRETREAT_INITIAL_SETUP_TIME = 600
PRETREAT_SETTING_CHANGEOVER_TIME = 8 * 60
STENTOR_CLEANING_TIME = 3600
STENTOR_LENGTH_YARDS = 14
# - estimated time needed between successive PPUs in print.
PRINT_SETUP_TIME = 300
STEAMER_SETUP_TIME = 210
STEAMER_CLEANING_TIME = 510
STEAMER_CLEANING_MATERIALS = [
    "LY115",
    "RYJ01",
    "GGT16",
    "CTW70",
    "LSG19",
    "SLCTN",
    "CTNBA",
    "CFT97",
]
WASHER_SETUP_TIME = 120
WASHER_LENGTH_YARDS = 24
DRYER_SETUP_TIME = 120
DRYER_LENGTH_YARDS = 55
# - starting with the assumption that roll inspection takes 10 min.
INSPECT_SETUP_TIME = 180
INSPECT_TIME_YPM = 5.45
PRINTERS = ["luna", "flash", "nova"]
LEADER_LENGTH_YD = 12
STENTOR_LENGTH_YD = 10
# for now the steamer dwell time is just a constant irrespective of the material speed
STEAMER_DWELL_TIME = 13.5 * 60

MAX_YARDAGE_SKIP_TO_SCHEDULE_MAINTENANCE = 25

# https://airtable.com/apprcULXTWu33KFsh/tbldADFytseIivUcO/viwahE5ntI1TsLeZ6/recT1kSbfsx86d6W8?blocks=hide
INSUFFICIENT_SILK_CV = "recT1kSbfsx86d6W8"

# https://airtable.com/apprcULXTWu33KFsh/tbldADFytseIivUcO/viwahE5ntI1TsLeZ6/recyYCpHLjbuLufcG?blocks=hide
TOO_MUCH_YARDAGE_CV = "recyYCpHLjbuLufcG"


def material_info():
    """
    Get properties of materials as well as the adjacency list which describes the material pairs which may be involved
    in the same PPU.
    """
    pretreat_df = PRETREAT_INFO.df(
        field_map={
            "Name": "material_code",
            "Material Type": "material_type",
            "Pre-treatment Type": "pretreat_type",
            "Pretreatment Speed": "pretreat_speed",
            "Pretreatment Width 1 (mm)": "pretreat_width",
            "Pretreatment Temperature": "pretreat_temp",
            "Steamer Group - MS Steamer": "steamer_group",
            "Production Wash Speed (m/min)": "washer_speed_mpm",
            "Stenter Dry Speed": "stentor_dry_speed_mpm",
            "Pilot Speed (m/min) Relaxed Dryer": "dryer_speed_mpm",
            "Weight (GSM)": "weight_gsm",
        },
        formula="{Approved Material}=TRUE()",
        include_id=False,
    )
    material_df = MATERIAL_INFO.df(
        field_map={
            "Key": "material_code",
            "Printers Capable of Printing this Material": "printers",
        },
        formula="{Printers Capable of Printing this Material}!=''",
        include_id=False,
    )
    df = (
        material_df.set_index("material_code")
        .join(pretreat_df.set_index("material_code"), how="inner")
        .sort_index()
        .dropna()  # dont break due to busted airtable data.
    )
    df = df.astype({"washer_speed_mpm": "float"})
    df["material_type"] = df["material_type"].apply(lambda l: l[0])
    df["weight_gsm"] = df["weight_gsm"].apply(lambda l: l[0])
    mat_info = df.to_dict("index")
    adj_list = []
    for i in range(df.shape[0]):
        ri = df.iloc[i]
        for j in range(i + 1, df.shape[0]):
            rj = df.iloc[j]
            if (
                ri.material_type == rj.material_type
                and ri.pretreat_type == rj.pretreat_type
                and ri.steamer_group == rj.steamer_group
                and ri.pretreat_temp == rj.pretreat_temp
                and ri.pretreat_speed == rj.pretreat_speed
                and ri.pretreat_width == rj.pretreat_width
            ):
                adj_list.append((ri.name, rj.name))
    return df, mat_info, set(adj_list)


def adjacent_materials(material, adj_list):
    return set(
        [material]
        + [w for u, v in adj_list if u == material or v == material for w in [u, v]]
    )


def current_rolls_df(mat_info, roll_names=None):
    rolls_df = ROLLS.df(
        field_map={
            "Name": "roll_name",
            "Material Code": "material_code",
            "Latest Measurement": "roll_length",
            "Cut To Length (yards)": "cut_length_yards",
            "Current Substation": "current_substation",
            "Pretreated Length": "pretreated_length",
        },
        formula="""
            AND(
                {Allocated for PPU}=TRUE(),
                {Carts}='',
                {Assigned Printer Name v3}='',
                {Assigned PPU Name}='',
                OR(
                    {Current Substation} = 'Material Node',
                    {Current Substation} = 'Pretreatment',
                    {Current Substation} = 'Straighten',
                    {Current Substation} = 'Print',
                    {Current Substation} = 'Material Prep'
                )
            )
        """
        if roll_names is None
        else f"AND({{Name}}!='', FIND({{Name}}, '{','.join(roll_names)}'))",
        include_id=True,
    )
    if rolls_df.shape[0] == 0:
        raise ValueError("No rolls to schedule.")
    rolls_df.material_code = rolls_df.material_code.apply(lambda l: l[0])
    rolls_df["print_yards"] = rolls_df.cut_length_yards.where(
        pd.notna, rolls_df.roll_length
    ).astype("float")
    rolls_df["needs_pretreat"] = rolls_df["current_substation"] != "Print"
    if not all(rolls_df.needs_pretreat):
        rolls_df.loc[~rolls_df.needs_pretreat, "print_yards"] = rolls_df[
            ~rolls_df.needs_pretreat
        ].apply(
            lambda r: r["pretreated_length"][0]
            if pd.isna(r["print_yards"])
            else r["print_yards"],
            axis=1,
        )
    for prop in [
        "pretreat_speed",
        "pretreat_width",
        "pretreat_type",
        "pretreat_temp",
        "steamer_group",
        "washer_speed_mpm",
        "dryer_speed_mpm",
        "stentor_dry_speed_mpm",
        "weight_gsm",
        "material_type",
    ]:
        rolls_df[prop] = rolls_df.material_code.apply(lambda c: mat_info[c].get(prop))
    rolls_df["ppu_time_s"] = (
        rolls_df.print_yards * METERS_PER_YD / PPU_ROLLING_METERS_PER_SECOND
    )
    rolls_df["pretreat_time_s"] = (
        60 * rolls_df.print_yards * METERS_PER_YD / rolls_df.pretreat_speed
    )
    rolls_df["print_time_s"] = rolls_df.apply(
        lambda r: estimate_print_time(r.material_code, r.print_yards), axis=1
    )
    rolls_df["power_d_time_s"] = rolls_df.apply(
        lambda r: estimate_print_time(r.material_code, PRINT_DRYER_YARDS), axis=1
    )
    rolls_df["steam_time_s"] = rolls_df.apply(
        lambda r: r.print_yards
        / (
            STEAMER_GROUP_1_YARDS_PER_SECOND
            if r.steamer_group == "Group 1"
            else STEAMER_GROUP_2_YARDS_PER_SECOND
        ),
        axis=1,
    )
    rolls_df["steam_clean_time_s"] = rolls_df.material_code.apply(
        lambda m: STEAMER_CLEANING_TIME if m in STEAMER_CLEANING_MATERIALS else 0
    )
    rolls_df["wash_time_s"] = rolls_df.apply(
        lambda r: METERS_PER_YD * r.print_yards / (r.washer_speed_mpm / 60.0),
        axis=1,
    )
    rolls_df["dry_time_s"] = rolls_df.apply(
        lambda r: METERS_PER_YD * r.print_yards / (r.dryer_speed_mpm / 60.0),
        axis=1,
    )
    rolls_df["inspect_time_s"] = rolls_df.apply(
        lambda r: 60 * r.print_yards / INSPECT_TIME_YPM,
        axis=1,
    )
    rolls_df["stentor_dry_time_s"] = rolls_df.apply(
        lambda r: METERS_PER_YD * r.print_yards / (r.stentor_dry_speed_mpm / 60.0),
        axis=1,
    )
    return (
        rolls_df.drop(columns=["pretreated_length", "roll_length"])
        .dropna()
        .reset_index(drop=True)
    )


def order_rolls_for_physical_constraints(rolls_df, ppu_list):
    return sorted(
        ppu_list,
        key=lambda p: [
            1 if rolls_df.iloc[p[0]].material_code == "CFT97" else 0,
            0 if rolls_df.iloc[p[0]].pretreat_type == "Silk" else 1,
            0 if rolls_df.iloc[p[0]].material_type == "Woven" else 1,
            -rolls_df.iloc[p[0]].pretreat_temp,
            -rolls_df.iloc[p[0]].pretreat_width,
        ],
    )


def plan_ppus(
    rolls_df,
    ppu_list,
    station_runtimes,
    output_sched=False,
    active_printers=PRINTERS,
    maintain_printers=[],
):
    """
    From the ordered list of PPUs - build up the timestamps when each PPU goes on each substation.
    """
    pretreat_temp = None
    stentor_t = station_runtimes.pretreat.start + timedelta(
        seconds=PRETREAT_INITIAL_SETUP_TIME
    )
    printer_t = {p: station_runtimes.print.start for p in active_printers}
    steam_t = station_runtimes.steam.start
    wash_t = station_runtimes.wash.start
    dry_t = station_runtimes.dry.start
    inspect1_t = station_runtimes.inspect_1.start
    plan = []
    pretreat_type = None
    ppu_print_finish = []
    silk_yards = 0
    for p in ppu_list:
        ppu_pretreat_temp = rolls_df.iloc[p[0]].pretreat_temp
        pretreat_time = (
            (
                PRETREAT_SETUP_TIME
                + sum(rolls_df.iloc[i].pretreat_time_s for i in p)
                + (
                    60
                    * (
                        LEADER_LENGTH_YD
                        if rolls_df.iloc[p[0]].material_type != "Knit"
                        else 0
                    )
                    * METERS_PER_YD
                    / rolls_df.iloc[p[0]].pretreat_speed
                )
                + (
                    PRETREAT_SETTING_CHANGEOVER_TIME
                    if pretreat_temp is not None and pretreat_temp != ppu_pretreat_temp
                    else 0
                )
            )
            if rolls_df.iloc[p[0]].needs_pretreat
            else 0
        )
        pretreat_temp = ppu_pretreat_temp
        print_time = {
            printer: PRINT_SETUP_TIME
            + sum(rolls_df.iloc[i].print_time_s[printer] for i in p)
            for printer in active_printers
        }
        print_dry_time = {
            printer: max(rolls_df.iloc[i].power_d_time_s[printer] for i in p)
            for printer in active_printers
        }
        if pretreat_type is None:
            pretreat_type = rolls_df.iloc[p[0]].pretreat_type
        elif pretreat_type != rolls_df.iloc[p[0]].pretreat_type:
            pretreat_type = rolls_df.iloc[p[0]].pretreat_type
            stentor_t += timedelta(
                seconds=PRETREAT_CHANGEOVER_TIME + PRETREAT_INITIAL_SETUP_TIME
            )
        if rolls_df.iloc[p[0]].pretreat_type == "Silk":
            silk_yards += sum(rolls_df.iloc[i].print_yards for i in p)
        pretreat_finish = stentor_t + timedelta(seconds=pretreat_time)
        printer, printer_start = min(
            [
                t
                for t in printer_t.items()
                if t[0] not in maintain_printers
                or t[1] + timedelta(seconds=print_time[t[0]])
                < station_runtimes.print.end - timedelta(hours=PRINTER_MAINTAIN_HOURS)
            ],
            key=lambda t: [t[1], t[0]],
        )
        print_t = max(printer_start, pretreat_finish)
        print_finish = print_t + timedelta(seconds=print_time[printer])
        if output_sched:
            plan.append(
                {
                    "pretreat_start": stentor_t,
                    "pretreat_finish": pretreat_finish,
                    "printer": printer,
                    "print_start": print_t,
                    "print_finish": print_finish,
                    "pretreat_type": pretreat_type,
                    "dry_on_stentor": pretreat_type == "Silk",
                }
            )
        stentor_t = pretreat_finish
        printer_t[printer] = print_t + timedelta(seconds=print_time[printer])
        ppu_print_finish.append(
            printer_t[printer] + timedelta(seconds=print_dry_time[printer])
        )
    stentor_t += timedelta(seconds=STENTOR_CLEANING_TIME)
    for print_finish, (i, p) in sorted(
        zip(ppu_print_finish, list(enumerate(ppu_list)))
    ):
        steam_time = (
            STEAMER_SETUP_TIME
            + sum(rolls_df.iloc[i].steam_time_s for i in p)
            + max(rolls_df.iloc[i].steam_clean_time_s for i in p)
            + STEAMER_DWELL_TIME
        )
        wash_time = (
            WASHER_SETUP_TIME
            + sum(rolls_df.iloc[i].wash_time_s for i in p)
            + (
                METERS_PER_YD
                * WASHER_LENGTH_YARDS
                / (rolls_df.iloc[p[-1]].washer_speed_mpm / 60.0)
            )
        )
        dry_on_stentor = rolls_df.iloc[p[0]].pretreat_type == "Silk"
        dry_time = (
            (
                DRYER_SETUP_TIME
                + sum(rolls_df.iloc[i].stentor_dry_time_s for i in p)
                + (
                    METERS_PER_YD
                    * STENTOR_LENGTH_YARDS
                    / (rolls_df.iloc[p[-1]].stentor_dry_speed_mpm / 60.0)
                )
            )
            if dry_on_stentor
            else (
                DRYER_SETUP_TIME
                + sum(rolls_df.iloc[i].dry_time_s for i in p)
                + (
                    METERS_PER_YD
                    * DRYER_LENGTH_YARDS
                    / (rolls_df.iloc[p[-1]].dryer_speed_mpm / 60.0)
                )
            )
        )
        inspect_time = INSPECT_SETUP_TIME + sum(
            rolls_df.iloc[i].inspect_time_s for i in p
        )
        steam_t = max(steam_t, print_finish)
        steam_finish = steam_t + timedelta(seconds=steam_time)
        wash_t = max(wash_t, steam_finish)
        wash_finish = wash_t + timedelta(seconds=wash_time)
        dry_start = max(wash_finish, stentor_t if dry_on_stentor else dry_t)
        dry_finish = dry_start + timedelta(seconds=dry_time)
        inspect1_t = max(inspect1_t, dry_finish)
        inspect1_finish = inspect1_t + timedelta(seconds=inspect_time)
        if output_sched:
            plan[i] = {
                **plan[i],
                "steam_start": steam_t,
                "steam_finish": steam_finish,
                "wash_start": wash_t,
                "wash_finish": wash_finish,
                "dry_start": dry_start,
                "dry_finish": dry_finish,
                "inspect1_start": inspect1_t,
                "inspect1_finish": inspect1_finish,
            }
        steam_t = steam_finish
        wash_t = wash_finish
        if dry_on_stentor:
            stentor_t = dry_finish
        else:
            dry_t = dry_finish
        inspect1_t = inspect1_finish
    if output_sched:
        return plan
    else:
        valid = (
            stentor_t <= station_runtimes.pretreat.end
            and max(printer_t.values()) <= station_runtimes.print.end
            # RH 2024-03-11 allegedly we dont care if we go into overtime on the steamer
            # and steam_t <= station_runtimes.steam.end
            and wash_t <= station_runtimes.wash.end
            and dry_t <= station_runtimes.dry.end
            and inspect1_t <= station_runtimes.inspect_1.end
        )
        return (inspect1_t - station_runtimes.pretreat.start).seconds, valid


def anneal_ppus(
    rolls_df,
    adj_list,
    station_runtimes,
    max_iters=25000,
    active_printers=PRINTERS,
    try_maintain=[],
):
    ppu_list = []
    # what rolls can go together on PPUs.
    roll_adj = set(
        (i, j)
        for i in rolls_df.index
        for j in rolls_df.index
        if (
            (
                rolls_df.iloc[i].material_code == rolls_df.iloc[j].material_code
                or (rolls_df.iloc[i].material_code, rolls_df.iloc[j].material_code)
                in adj_list
                or (rolls_df.iloc[j].material_code, rolls_df.iloc[i].material_code)
                in adj_list
            )
            # we only make PPUs out of raw rolls not leftovers that are already pretreated.
            and rolls_df.iloc[i].needs_pretreat
            and rolls_df.iloc[j].needs_pretreat
            # for now we never want knits on a PPU.
            and rolls_df.iloc[i].material_type != "Knit"
            and rolls_df.iloc[j].material_type != "Knit"
        )
    )
    # to start with just try to maximize yardage assigned
    total_yards = rolls_df.print_yards.sum()
    score = total_yards * 100
    best_score = score
    best_ppu_list = list(ppu_list)
    for iteration in range(max_iters):
        if iteration % 5000 == 0:
            logger.info(
                f"Anneal iteration {iteration}: {len(ppu_list)} PPUs with score {score}"
            )
        roll_idx = random.choice(rolls_df.index)
        # remove from ppu.
        ppus_new = [
            [j for j in p if j != roll_idx] for p in ppu_list if p != [roll_idx]
        ]
        # find a ppu where it can go.
        possible_ppus = [
            i
            for i, p in enumerate(ppus_new)
            if all((roll_idx, j) in roll_adj for j in p)
        ]
        new_pos = random.randint(0, len(possible_ppus) + 1)
        if new_pos == 1:
            ppus_new.append([roll_idx])
        if new_pos > 1:
            ppus_new[possible_ppus[new_pos - 2]].append(roll_idx)
        ppus_new = order_rolls_for_physical_constraints(rolls_df, ppus_new)
        end_time, valid = plan_ppus(
            rolls_df,
            ppus_new,
            station_runtimes,
            active_printers=active_printers,
            maintain_printers=try_maintain,
        )
        if valid:
            assigned_yards = sum(
                rolls_df.iloc[i].print_yards for p in ppus_new for i in p
            )
            assigned = set(i for p in ppu_list for i in p)
            unassigned = set(rolls_df.index) - assigned
            unassigned_yards = total_yards - assigned_yards
            unassigned_silk_yards = sum(
                rolls_df.iloc[i].print_yards
                for i in unassigned
                if rolls_df.iloc[i].pretreat_type == "Silk"
            )
            new_score = (
                10 * unassigned_yards + 100 * unassigned_silk_yards + 0.0001 * end_time
            )
            delta = new_score - score
            if delta < 0 or random.random() > (
                2 / (1 + math.exp(-delta * 1 * iteration / max_iters)) - 1
            ):
                score = new_score
                ppu_list = ppus_new
                if score < best_score:
                    best_score = score
                    best_ppu_list = list(ppu_list)
    # sort the PPUs of the final schedule into width and speed order.
    best_ppu_list = [
        sorted(
            p,
            key=lambda j: [
                -rolls_df.iloc[j]["pretreat_width"],
                -rolls_df.iloc[j]["pretreat_speed"],
                rolls_df.iloc[j]["material_code"],
            ],
        )
        for p in best_ppu_list
    ]
    plan = plan_ppus(
        rolls_df,
        best_ppu_list,
        station_runtimes,
        output_sched=True,
        active_printers=active_printers,
        maintain_printers=try_maintain,
    )
    unassigned_yards = total_yards - sum(
        rolls_df.iloc[i].print_yards for p in best_ppu_list for i in p
    )
    return best_ppu_list, plan, unassigned_yards


def anneal_ppus_and_printer_maintainance(
    rolls_df,
    adj_list,
    station_runtimes,
    max_iters=25000,
    active_printers=PRINTERS,
):
    # figure out what could use maintainance:
    printers_to_maintain = SCHEDULE_ASSETS.all(
        fields=["Assets Name"],
        formula="""
        AND(
            {Maintenance Action} = 'Empty Queue',
            {Asset Type} = 'Printers',
            {Days Since Last Maintenance} >= 4
        )
        """,
    )
    printer_names_to_maintain = [
        n
        for n, i in SCHEDULE_PRINTER_IDS
        if i in [r["id"] for r in printers_to_maintain]
    ]
    # if some printers are not active - try to maintain on them first.
    printer_names_to_maintain = sorted(
        printer_names_to_maintain, key=lambda n: 1 if n in active_printers else 0
    )
    # rh - 2/20/24 - for now dont try to do maintenance on printers.
    # we can revisit this when print gang decides wtf.
    printer_names_to_maintain = []
    logger.info(f"Attempting to maintain printers {printer_names_to_maintain}")
    no_maintain_plan = anneal_ppus(
        rolls_df,
        adj_list,
        station_runtimes,
        max_iters=max_iters,
        active_printers=active_printers,
        try_maintain=[],
    )
    current_plan = no_maintain_plan
    maintaining_printers = []
    while len(printer_names_to_maintain) > 0:
        updated = False
        for maintain_printer in printer_names_to_maintain:
            try_maintain = maintaining_printers + [maintain_printer]
            if all(a in try_maintain for a in active_printers):
                break
            maintain_plan = anneal_ppus(
                rolls_df,
                adj_list,
                station_runtimes,
                max_iters=max_iters,
                active_printers=active_printers,
                try_maintain=try_maintain,
            )
            if (
                maintain_plan[2]
                < no_maintain_plan[2] + MAX_YARDAGE_SKIP_TO_SCHEDULE_MAINTENANCE
            ):
                logger.info(f"Can schedule maintenance on {maintain_printer}")
                maintaining_printers += [maintain_printer]
                current_plan = maintain_plan
                printer_names_to_maintain = [
                    p for p in printer_names_to_maintain if p != maintain_printer
                ]
                updated = True
                break
        if not updated:
            break
    ppu_list, plan, _ = current_plan
    return ppu_list, plan, maintaining_printers


def plot_sched(
    rolls_df,
    ppu_list,
    plan,
    schedule_date_str,
    station_runtimes,
    printers_to_maintain,
    filename=None,
):
    stations = {
        "pretreat": 8,
        "luna": 7,
        "flash": 6,
        "nova": 5,
        "steam": 4,
        "wash": 3,
        "dry": 2,
        "stentor_dry": 1,
        "inspect1": 0,
    }
    station_labels = [
        "Stentor - pretreat",
        "P2 (luna)",
        "P3 (flash)",
        "P4 (nova)",
        "Steam",
        "Wash",
        "Dry",
        "Stentor - dry",
        "Inspection 1",
    ]
    ppu_colors = cm.rainbow(np.linspace(1, 0, len(ppu_list)))
    fig, ax = plt.subplots(figsize=(20, 20))
    ppu_labels = []
    for i, (p, pp) in enumerate(zip(ppu_list, plan)):
        color = ppu_colors[i]
        ax.barh(
            stations[pp["printer"]],
            left=(pp["print_start"] - station_runtimes.day_start).seconds,
            width=(pp["print_finish"] - pp["print_start"]).seconds,
            color=color,
        )
        for s in ["pretreat", "steam", "wash", "dry", "inspect1"]:
            station_name = s
            if s == "dry" and pp["dry_on_stentor"]:
                station_name = "stentor_dry"
            ax.barh(
                stations[station_name],
                left=(pp[f"{s}_start"] - station_runtimes.day_start).seconds,
                width=(pp[f"{s}_finish"] - pp[f"{s}_start"]).seconds,
                color=color,
            )
        ppu_labels.append(
            f"PPU {i}: {sum(rolls_df.iloc[j]['print_yards'] for j in p):.1f}yd ({', '.join(rolls_df.iloc[j]['roll_name'] for j in p)}){' -- already pretreated' if not rolls_df.iloc[p[0]].needs_pretreat else ''}"
        )

    for p in printers_to_maintain:
        ax.barh(
            stations[p],
            left=3600 * (station_runtimes.print.end.hour - PRINTER_MAINTAIN_HOURS),
            width=3600 * PRINTER_MAINTAIN_HOURS,
            color="gray",
            hatch="+",
        )

    max_t = (max(p["dry_finish"] for p in plan) - station_runtimes.day_start).seconds
    end_hour = max(20, math.ceil(max_t / 3600))
    xax = np.arange(math.floor(station_runtimes.pretreat.start.hour), end_hour + 1)
    ax.set_xticks([x * 3600 for x in xax])
    ax.set_xticklabels(xax, fontsize=20)
    ax.set_xlim(
        math.floor(station_runtimes.pretreat.start.hour) * 3600 - 100,
        end_hour * 3600 + 100,
    )
    ax.set_xlabel("Hour", fontsize=18)
    ax.set_yticks([y for _, y in stations.items()])
    ax.set_yticklabels(station_labels, fontsize=20)
    ax.set_ylim(-0.5, 8.5 + len(ppu_list) * 0.45)
    ax.legend(
        handles=[
            mpatches.Patch(color=c, label=p) for p, c in zip(ppu_labels, ppu_colors)
        ],
        loc=2,
        mode="expand",
        fontsize=18,
    )
    ax.set_title(schedule_date_str, fontdict={"fontsize": 26})
    if filename:
        plt.savefig(filename)
    else:
        plt.show()


def airtable_date(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def schedule_maintenance(
    ppu_plan, schedule_date, printers_to_maintain, station_runtimes
):
    day_name = schedule_date.strftime("%A")
    assets = SCHEDULE_ASSETS.all(formula=f"{{Maintenance Day}}='{day_name}'")
    for r in assets:
        start_time = station_runtimes.day_start + timedelta(hours=8)
        if r["fields"]["Maintenance Action"] == "After Shift":
            if r["fields"]["Assets Name"] == "Stenter":
                start_time = max(pl["pretreat_finish"] for pl in ppu_plan)
        SCHEDULE_MATRIX.create(
            {
                "Assets": [r["id"]],
                "Process SVOT": ["recGjYGHdvh1XMeDL"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(start_time),
                "Planned - Completed Time": airtable_date(
                    start_time + timedelta(hours=2)
                ),
            }
        )
    for p in printers_to_maintain:
        SCHEDULE_MATRIX.create(
            {
                "Assets": [i for n, i in SCHEDULE_PRINTER_IDS if n == p],
                "Process SVOT": ["recGjYGHdvh1XMeDL"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    station_runtimes.print.end - timedelta(hours=PRINTER_MAINTAIN_HOURS)
                ),
                "Planned - Completed Time": airtable_date(station_runtimes.print.end),
            }
        )


def erase_schedule(schedule_date, erase_ppu_creation=True):
    ppu_records = get_exising_ppu_schedule(schedule_date)
    # erase schedule matrix records
    schedule_records = SCHEDULE_MATRIX.all(
        formula=f"{{Scheduled Date}}='{schedule_date.strftime('%d-%m-%Y')}'"
    )
    SCHEDULE_MATRIX.batch_delete([r["id"] for r in schedule_records])
    # erase ppu schedule.
    SCHEDULE_PPU_SCHEDULE.batch_delete(
        [ri for r in ppu_records for ri in r["fields"].get("PPU Scheduling", [])]
    )
    # delete ppu definitions
    if erase_ppu_creation:
        SCHEDULE_PPU_CREATION.batch_delete([r["id"] for r in ppu_records])
        # erase construction order records.
        SCHEDULE_PPU_CONSTRUCTION_ORDER.batch_delete(
            [
                ci
                for p in ppu_records
                for ci in p["fields"].get("PPU Construction Order", [])
            ]
        )
    # TODO: delete the other schedule matrix stuff that happens the day before (chemical batch etc)


def get_exising_ppu_schedule(schedule_date):
    return SCHEDULE_PPU_CREATION.all(
        formula=f"""{{Scheduled Print Date}} = '{schedule_date.strftime("%Y-%m-%d")}'""",
        sort=["PPU Index"],
    )


def get_ppus_and_plan(schedule_date):
    ppu_records = get_exising_ppu_schedule(schedule_date)
    station_runtimes = get_station_runtimes(schedule_date)
    if len(ppu_records) == 0:
        logger.warn(
            f"Unable to find ppu records for date {schedule_date.strftime('%Y-%m-%d')}"
        )
    rolls = [n for r in ppu_records for n in r["fields"]["Roll Names"].split(",")]
    _, mat_info, _ = material_info()
    rolls_df = current_rolls_df(mat_info, roll_names=rolls)
    ppu_list = [
        [
            rolls_df[rolls_df.roll_name == n].index[0]
            for n in r["fields"]["Roll Names"].split(",")
        ]
        for r in ppu_records
    ]
    ppu_list = order_rolls_for_physical_constraints(rolls_df, ppu_list)
    plan = plan_ppus(
        rolls_df,
        ppu_list,
        station_runtimes,
        output_sched=True,
    )
    return rolls_df, ppu_list, plan, station_runtimes


def reschedule_ppus(schedule_date):
    """
    Convenience for when something happens and we have to redo the schedule but preserve the same ppus.
    should add methods to specify the printers etc.
    """
    ppu_records = get_exising_ppu_schedule(schedule_date)
    station_runtimes = get_station_runtimes(schedule_date)
    if len(ppu_records) == 0:
        logger.warn(
            f"Unable to find ppu records for date {schedule_date.strftime('%Y-%m-%d')}"
        )
    rolls = [n for r in ppu_records for n in r["fields"]["Roll Names"].split(",")]
    _, mat_info, _ = material_info()
    rolls_df = current_rolls_df(mat_info, roll_names=rolls)
    ppu_list = [
        [
            rolls_df[rolls_df.roll_name == n].index[0]
            for n in r["fields"]["Roll Names"].split(",")
        ]
        for r in ppu_records
    ]
    ppu_records_dict = dict(zip([str(sorted(p)) for p in ppu_list], ppu_records))
    # ppu_list = order_rolls_for_physical_constraints(rolls_df, ppu_list)
    ppu_records = [ppu_records_dict[str(sorted(p))] for p in ppu_list]
    erase_schedule(schedule_date, erase_ppu_creation=False)
    plan = plan_ppus(
        rolls_df,
        ppu_list,
        station_runtimes,
        output_sched=True,
    )
    schedule_to_airtable_with_ppu_records(
        rolls_df,
        ppu_records,
        ppu_list,
        plan,
        station_runtimes,
        [],
        schedule_date,
        datetime.now(),
        False,
        add_connectors_and_leaders=False,
        add_ppu_construction_order=False,
    )


def schedule_to_airtable(
    rolls_df,
    ppu_list,
    plan,
    station_runtimes,
    printers_to_maintain,
    schedule_date,
    today_date,
    full_silk_batch,
):
    if len(get_exising_ppu_schedule(schedule_date)) > 0:
        return False
    # generate PPU records
    ppu_payload = []
    for i, p in enumerate(ppu_list):
        pretreat_settings = {
            k: rolls_df.iloc[p[0]][k]
            for k in [
                "pretreat_speed",
                "pretreat_width",
                "pretreat_type",
                "pretreat_temp",
            ]
        }
        ppu_payload.append(
            {
                "PPU Index": i,
                "Status": "Todo",
                "Total Length": sum(rolls_df.iloc[k]["print_yards"] for k in p),
                "Roll Order At Construction": "\n".join(
                    f"""{j}: {rolls_df.iloc[k]["roll_name"]} ({f'cut to {rolls_df.iloc[k]["cut_length_yards"]:.1f} yards' if not rolls_df.isna().iloc[k]["cut_length_yards"] else "whole roll"})"""
                    for j, k in enumerate(reversed(p))
                ),
                "Roll Names": ",".join(rolls_df.iloc[k]["roll_name"] for k in p),
                "Pretreat Type": rolls_df.iloc[p[0]]["pretreat_type"]
                if rolls_df.iloc[p[0]]["needs_pretreat"]
                else "Child Roll",
                "Requires Construction": str(rolls_df.iloc[p[0]]["needs_pretreat"]),
                "Notes": None
                if rolls_df.iloc[p[0]]["needs_pretreat"]
                else "Rolls are already in print - do not pretreat or make a PPU.",
                "Print Date": airtable_date(station_runtimes.day_start),
                "Pretreat Settings": "\n".join(
                    f"{k}: {v}" for k, v in pretreat_settings.items()
                ),
                "Material Type": rolls_df.iloc[p[0]]["material_type"],
            }
        )
    ppu_records = SCHEDULE_PPU_CREATION.batch_create(ppu_payload, typecast=True)
    logger.info(f"Created {len(ppu_records)} ppu records")
    return schedule_to_airtable_with_ppu_records(
        rolls_df,
        ppu_records,
        ppu_list,
        plan,
        station_runtimes,
        printers_to_maintain,
        schedule_date,
        today_date,
        full_silk_batch,
    )


def get_connector_rolls(material_code, needed_yards, chunk_size=None):
    rolls = ROLLS.all(
        fields=[
            "Name",
            "Latest Measurement",
            "Roll Length (yards)",
        ],
        formula=f"""
            AND(
                {{Current Substation}}!='Done',
                {{Location V1.1}}!='',
                {{Inventory Cycle Count Status}}!='Not Found',
                {{Material Code}}='{material_code}',
                {{Print Assets (flow) 3}}='',
                {{Allocated for PPU}}=FALSE()
            )
        """,
    )
    for r in rolls:
        if (
            "Latest Measurement" not in r["fields"]
            or r["fields"]["Latest Measurement"] == ""
        ):
            r["fields"]["Latest Measurement"] = r["fields"]["Roll Length (yards)"]
        else:
            r["fields"]["Latest Measurement"] = float(r["fields"]["Latest Measurement"])
    if chunk_size is not None:
        # for leaders - we cant split a leader between multiple rolls.  also we should just use the longest rolls first.
        rolls = [r for r in rolls if r["fields"]["Latest Measurement"] >= chunk_size]
        for r in rolls:
            r["fields"]["Latest Measurement"] = chunk_size * math.floor(
                r["fields"]["Latest Measurement"] / chunk_size
            )
        rolls = sorted(rolls, key=lambda r: -r["fields"]["Latest Measurement"])
    else:
        # for connectors we can just break up tiny rolls because we cant use them for antyhing else.
        rolls = sorted(rolls, key=lambda r: r["fields"]["Latest Measurement"])
    assignment = {}
    remaining = list(rolls)
    while sum(assignment.values()) < needed_yards and len(remaining) > 0:
        required_yards = needed_yards - sum(assignment.values())
        rolls_yards = remaining[0]["fields"]["Latest Measurement"]
        assignment[remaining[0]["fields"]["Name"]] = math.floor(
            min(required_yards, rolls_yards)
        )
        remaining = remaining[1:]
    assigned_ids = [
        next(r["id"] for r in rolls if r["fields"]["Name"] == n)
        for n in assignment.keys()
    ]
    try:
        ROLLS.batch_update(
            [
                {
                    "id": id,
                    "fields": {
                        "Manual Pick Pretreat": True,
                        "Picking  Status pretreat": "To Do",
                        "Checkout Type": "Headers/Connectors",
                    },
                }
                for id in assigned_ids
            ],
            typecast=True,
        )
    except:
        pass
    return assignment


def assign_connectors(material, min_count, max_count, rack_record):
    if rack_record is None:
        logger.warn(f"No rack for {material}")
        return []
    schedule = []
    stock = rack_record["fields"]["Stock"]
    if stock < min_count:
        logger.info(
            f"Scheduling connectors in {material} - current stock {stock} min {min_count}"
        )
        to_create = max_count - stock
        roll_assignment = get_connector_rolls(material, to_create)
        logger.info(roll_assignment)
        if sum(roll_assignment.values(), 0) < to_create:
            ping_slack(f"Failed to schedule connectors for {material}", "make_one_team")
        for roll, count in roll_assignment.items():
            schedule.append(
                {
                    "Type": "HEADER/FOOTER",
                    "Headers/Footer Rack": [rack_record["id"]],
                    "Count Requested": count,
                    "Roll Name": roll,
                }
            )
    return schedule


def schedule_connectors(today_date, needed_connectors_light, needed_connectors_heavy):
    current_connectors = SCHEDULE_HEADERS_FOOTERS.all()
    connector_racks = {r["fields"]["Material"]: r for r in current_connectors}
    schedule = []
    schedule.extend(
        assign_connectors(
            "CLTWL",
            needed_connectors_heavy,
            max(12, needed_connectors_heavy),
            connector_racks.get("CLTWL"),
        )
    )
    schedule.extend(
        assign_connectors(
            "RYJ01",
            needed_connectors_light,
            max(25, needed_connectors_light),
            connector_racks.get("RYJ01"),
        )
    )
    if len(schedule) > 0:
        day_name = today_date.strftime("%A")
        warehouse_rows = SCHEDULE_WAREHOUSE.batch_create(schedule, typecast=True)
        SCHEDULE_MATRIX.create(
            {
                "Assets": ["rec8DBT5emVvPYkaq"],
                "Process SVOT": ["recya1xLjZ5K9Zxiz"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    today_date + timedelta(hours=13)
                ),
                "Planned - Completed Time": airtable_date(
                    today_date + timedelta(hours=14)
                ),
                "Warehouse Scheduling": [r["id"] for r in warehouse_rows],
            }
        )
    return connector_racks


def schedule_leaders(today_date):
    usable_leaders = SCHEDULE_LEADERS.all(
        formula="""
            AND(
                {State} = 'IN STOCK',
                {Length} > 0,
                {Flag for Review} = FALSE()
            )
        """
    )
    leader_locations = SCHEDULE_LOCATIONS.all(formula="{Warehouse}='Leaders Rack'")
    if len(usable_leaders) < 10:
        needed_leaders = 10 - len(usable_leaders)
        roll_assignment = get_connector_rolls(
            "RYJ01", LEADER_LENGTH_YD * needed_leaders, chunk_size=LEADER_LENGTH_YD
        )
        warehouse_rows = []
        for roll_name, yards in roll_assignment.items():
            leader_payload = []
            while yards >= LEADER_LENGTH_YD:
                location = min(
                    leader_locations, key=lambda r: r["fields"]["Leader Count"]
                )
                location["fields"]["Leader Count"] += 1
                leader_payload.append(
                    {
                        "Material": "RYJ01",
                        "Roll Name": roll_name,
                        "Length": LEADER_LENGTH_YD,
                        "Location": location["id"],
                    }
                )
                yards -= LEADER_LENGTH_YD
            leader_records = SCHEDULE_LEADERS.batch_create(
                leader_payload, typecast=True
            )
            warehouse_rows.append(
                SCHEDULE_WAREHOUSE.create(
                    {
                        "Type": "LEADER",
                        "Leaders": [r["id"] for r in leader_records],
                        "Count Requested": len(leader_records),
                        "Roll Name": roll_name,
                    }
                )
            )
        if len(warehouse_rows) > 0:
            day_name = today_date.strftime("%A")
            SCHEDULE_MATRIX.create(
                {
                    "Assets": ["rec8DBT5emVvPYkaq"],
                    "Process SVOT": ["rec5xcYZjAoZFOmeE"],
                    "Day of the Week": day_name,
                    "Planned - Started Time": airtable_date(
                        today_date + timedelta(hours=13)
                    ),
                    "Planned - Completed Time": airtable_date(
                        today_date + timedelta(hours=14)
                    ),
                    "Warehouse Scheduling": [r["id"] for r in warehouse_rows],
                }
            )


def schedule_to_airtable_with_ppu_records(
    rolls_df,
    ppu_records,
    ppu_list,
    plan,
    station_runtimes,
    printers_to_maintain,
    schedule_date,
    today_date,
    full_silk_batch,
    add_connectors_and_leaders=True,
    add_ppu_construction_order=True,
):
    ppu_plan = list(
        zip(
            ppu_list,
            plan,
            ppu_records,
        )
    )
    toff = lambda x: airtable_date(schedule_date + timedelta(seconds=x))
    day_name = schedule_date.strftime("%A")
    # schedule pretreatment
    silk_ppus = [
        (i, t)
        for i, t in enumerate(ppu_plan)
        if t[1]["pretreat_type"] == "Silk" and rolls_df.iloc[t[0][0]].needs_pretreat
    ]
    cotton_ppus = [
        (i, t)
        for i, t in enumerate(ppu_plan)
        if t[1]["pretreat_type"] == "Cotton" and rolls_df.iloc[t[0][0]].needs_pretreat
    ]
    if len(silk_ppus) > 0:
        pretreat_schedule_record = SCHEDULE_MATRIX.create(
            {
                "Assets": ["recfZpqjX9ATTGuUs"],
                "Process SVOT": ["recHKsN5dcvhGVRRj"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    silk_ppus[0][1][1]["pretreat_start"]
                    - timedelta(seconds=PRETREAT_INITIAL_SETUP_TIME)
                ),
                "Planned - Completed Time": airtable_date(
                    silk_ppus[-1][1][1]["pretreat_finish"]
                ),
            }
        )
        silk_payload = []
        for i, (p, pl, r) in silk_ppus:
            silk_payload.append(
                {
                    "PPU": [r["id"]],
                    "Scheduling": [pretreat_schedule_record["id"]],
                    "Planned Start Time": airtable_date(pl["pretreat_start"]),
                    "Planned End Time": airtable_date(pl["pretreat_finish"]),
                }
            )
        SCHEDULE_PPU_SCHEDULE.batch_create(silk_payload, typecast=True)
        # chemical batch
        SCHEDULE_MATRIX.create(
            {
                "Assets": ["recZqKFxPuE5KpBti"],
                "Process SVOT": [
                    "recw68AxM8A9tZw3e" if full_silk_batch else "recWl5F7DCHLbm1ab"
                ],
                "Day of the Week": datetime.now().strftime("%A"),
                "Planned - Started Time": airtable_date(
                    station_runtimes.day_start + timedelta(hours=14)
                ),
                "Planned - Completed Time": airtable_date(
                    station_runtimes.day_start + timedelta(hours=14.5)
                ),
            }
        )
        if len(cotton_ppus) > 0:
            SCHEDULE_MATRIX.create(
                {
                    "Assets": ["recfZpqjX9ATTGuUs"],
                    "Process SVOT": ["recSvHqQ81hW0PQlE"],
                    "Day of the Week": day_name,
                    "Planned - Started Time": airtable_date(
                        silk_ppus[-1][1][1]["pretreat_finish"]
                    ),
                    "Planned - Completed Time": airtable_date(
                        cotton_ppus[0][1][1]["pretreat_start"]
                    ),
                }
            )
    if len(cotton_ppus) > 0:
        pretreat_schedule_record = SCHEDULE_MATRIX.create(
            {
                "Assets": ["recfZpqjX9ATTGuUs"],
                "Process SVOT": ["recas2fvAlVY4X8oC"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    cotton_ppus[0][1][1]["pretreat_start"]
                    - timedelta(seconds=PRETREAT_INITIAL_SETUP_TIME)
                ),
                "Planned - Completed Time": airtable_date(
                    cotton_ppus[-1][1][1]["pretreat_finish"]
                ),
            }
        )
        cotton_payload = []
        for i, (p, pl, r) in cotton_ppus:
            cotton_payload.append(
                {
                    "PPU": [r["id"]],
                    "Scheduling": [pretreat_schedule_record["id"]],
                    "Planned Start Time": airtable_date(pl["pretreat_start"]),
                    "Planned End Time": airtable_date(pl["pretreat_finish"]),
                }
            )
        SCHEDULE_PPU_SCHEDULE.batch_create(cotton_payload, typecast=True)
        SCHEDULE_MATRIX.create(
            {
                "Assets": ["recfZpqjX9ATTGuUs"],
                "Process SVOT": ["recSvHqQ81hW0PQlE"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    cotton_ppus[-1][1][1]["pretreat_finish"]
                ),
                "Planned - Completed Time": airtable_date(
                    cotton_ppus[-1][1][1]["pretreat_finish"]
                    + timedelta(seconds=PRETREAT_CHANGEOVER_TIME)
                ),
            }
        )
        # chemical batch
        boff = 0.5 if len(silk_ppus) > 0 else 0
        SCHEDULE_MATRIX.create(
            {
                "Assets": ["recZqKFxPuE5KpBti"],
                "Process SVOT": ["recHHr4tcu5RBcJIf"],
                "Day of the Week": datetime.now().strftime("%A"),
                "Planned - Started Time": airtable_date(
                    station_runtimes.day_start + timedelta(hours=14 + boff)
                ),
                "Planned - Completed Time": airtable_date(
                    station_runtimes.day_start + timedelta(hours=14.5 + boff)
                ),
            }
        )
    # schedule printing
    for printer, asset in SCHEDULE_PRINTER_IDS:
        ppus = [(i, t) for i, t in enumerate(ppu_plan) if t[1]["printer"] == printer]
        if len(ppus) == 0:
            continue
        schedule_record = SCHEDULE_MATRIX.create(
            {
                "Assets": [asset],
                "Process SVOT": ["recBVP9knwlBx0g00"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(ppus[0][1][1]["print_start"]),
                "Planned - Completed Time": airtable_date(
                    ppus[-1][1][1]["print_finish"]
                ),
            }
        )
        SCHEDULE_PPU_SCHEDULE.batch_create(
            [
                {
                    "PPU": [r["id"]],
                    "Scheduling": [schedule_record["id"]],
                    "Planned Start Time": airtable_date(pl["print_start"]),
                    "Planned End Time": airtable_date(pl["print_finish"]),
                }
                for i, (p, pl, r) in ppus
            ],
            typecast=True,
        )
    # schedule other stuff
    for name, asset in [
        ("steam", "recZ31SdVvM8mwFjN"),
        ("wash", "recdNeau7wWXaq8oY"),
        ("inspect1", "reckwtom9v4BvKrnM"),
    ]:
        schedule_record = SCHEDULE_MATRIX.create(
            {
                "Assets": [asset],
                "Process SVOT": ["recBVP9knwlBx0g00"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    ppu_plan[0][1][f"{name}_start"]
                ),
                "Planned - Completed Time": airtable_date(
                    ppu_plan[-1][1][f"{name}_finish"]
                ),
            }
        )
        SCHEDULE_PPU_SCHEDULE.batch_create(
            [
                {
                    "PPU": [r["id"]],
                    "Scheduling": [schedule_record["id"]],
                    "Planned Start Time": airtable_date(pl[f"{name}_start"]),
                    "Planned End Time": airtable_date(pl[f"{name}_finish"]),
                }
                for i, (p, pl, r) in enumerate(ppu_plan)
            ],
            typecast=True,
        )
    # schedule drying
    stentor_dry = [p for p in ppu_plan if p[1]["dry_on_stentor"]]
    relax_dry = [p for p in ppu_plan if not p[1]["dry_on_stentor"]]
    if len(stentor_dry) > 0:
        # stentor cleaning
        SCHEDULE_MATRIX.create(
            {
                "Assets": ["recfZpqjX9ATTGuUs"],
                "Process SVOT": ["recSvHqQ81hW0PQlE"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(
                    max(p[1][f"pretreat_finish"] for p in ppu_plan),
                ),
                "Planned - Completed Time": airtable_date(
                    max(p[1][f"pretreat_finish"] for p in ppu_plan)
                    + timedelta(seconds=STENTOR_CLEANING_TIME)
                ),
            }
        )
        schedule_record = SCHEDULE_MATRIX.create(
            {
                "Assets": ["recfZpqjX9ATTGuUs"],
                "Process SVOT": ["recCQ3Svqwld8AM8p"],
                "Day of the Week": day_name,
                "Planned - Started Time": airtable_date(stentor_dry[0][1]["dry_start"]),
                "Planned - Completed Time": airtable_date(
                    stentor_dry[-1][1]["dry_finish"]
                ),
            }
        )
        SCHEDULE_PPU_SCHEDULE.batch_create(
            [
                {
                    "PPU": [r["id"]],
                    "Scheduling": [schedule_record["id"]],
                    "Planned Start Time": airtable_date(pl["dry_start"]),
                    "Planned End Time": airtable_date(pl["dry_finish"]),
                }
                for i, (p, pl, r) in enumerate(stentor_dry)
            ],
            typecast=True,
        )
    # relax dryer
    schedule_record = SCHEDULE_MATRIX.create(
        {
            "Assets": ["rec5Z8RnjYMJI3bKP"],
            "Process SVOT": ["recBVP9knwlBx0g00"],
            "Day of the Week": day_name,
            "Planned - Started Time": airtable_date(relax_dry[0][1]["dry_start"]),
            "Planned - Completed Time": airtable_date(relax_dry[-1][1]["dry_finish"]),
        }
    )
    SCHEDULE_PPU_SCHEDULE.batch_create(
        [
            {
                "PPU": [r["id"]],
                "Scheduling": [schedule_record["id"]],
                "Planned Start Time": airtable_date(pl["dry_start"]),
                "Planned End Time": airtable_date(pl["dry_finish"]),
            }
            for i, (p, pl, r) in enumerate(relax_dry)
        ],
        typecast=True,
    )

    # ppu creation.
    SCHEDULE_MATRIX.create(
        {
            "Assets": ["rec8DBT5emVvPYkaq"],
            "Process SVOT": ["recy8zgq0zhcWwaVo"],
            "Day of the Week": day_name,
            "Planned - Started Time": airtable_date(
                station_runtimes.day_start + timedelta(hours=14)
            ),
            "Planned - Completed Time": airtable_date(
                station_runtimes.day_start + timedelta(hours=17)
            ),
        }
    )
    # update rolls with the pretreat flag.
    assigned = list(set(i for p in ppu_list for i in p))
    to_pretreat = rolls_df.iloc[assigned].copy()
    to_pretreat = to_pretreat[to_pretreat.needs_pretreat]
    ROLLS.batch_update(
        [
            {
                "id": r["id"],
                "fields": {
                    "Schedule to pretreat": "True",
                    "Manual Pick Pretreat": "True",
                },
            }
            for _, r in to_pretreat.iterrows()
        ],
        typecast=True,
    )
    if add_connectors_and_leaders:
        schedule_leaders(today_date)
        total_connectors = sum(
            2 for pi in ppu_list if rolls_df.iloc[pi[0]].material_type != "Knit"
        )
        total_connectors_heavy = sum(
            (1 if rolls_df.iloc[pi[0]].weight_gsm >= 250 else 0)
            + (1 if rolls_df.iloc[pi[-1]].weight_gsm >= 250 else 0)
            for pi in ppu_list
            if rolls_df.iloc[pi[0]].material_type != "Knit"
        )
        connector_racks = schedule_connectors(
            today_date,
            total_connectors - total_connectors_heavy,
            total_connectors_heavy,
        )
    else:
        connector_racks = {
            r["fields"]["Material"]: r for r in SCHEDULE_HEADERS_FOOTERS.all()
        }

    # build up a dictionary of materials for linkage.
    material_record_id = {
        r["fields"]["Name"]: [r["id"]] for r in SCHEDULE_MATERIALS.all(fields=["Name"])
    }
    material_record = lambda r: material_record_id.get(r.split(": ")[1])

    records = []
    # add in records for child rolls.
    for i, p in enumerate(ppu_list):
        if not rolls_df.iloc[p[0]].needs_pretreat:
            records.append(
                {
                    "Type": "ROLL",
                    "PPU": [ppu_records[i]["id"]],
                    "Roll Name": rolls_df.iloc[p[0]].roll_name,
                    "Material": material_record(rolls_df.iloc[p[0]].roll_name),
                    "Requires Construction": False,
                    "Length Yards": rolls_df.iloc[p[0]].print_yards,
                    "Requires Cutting": False,
                }
            )
    # remove ppus which wont be part of the giant chain.
    ppu_idx = [i for i, p in enumerate(ppu_list) if rolls_df.iloc[p[0]].needs_pretreat]
    for i in ppu_idx:
        pi = ppu_list[i]
        ppuid = ppu_records[i]["id"]
        if rolls_df.iloc[pi[0]].material_type == "Knit":
            if i > 0 and rolls_df.iloc[ppu_list[i - 1][0]].material_type != "Knit":
                records.append(
                    {
                        "Type": "LEADER",
                        "PPU": [ppuid],
                        "Roll Name": "LEADER",
                        "Requires Construction": True,
                    }
                )
            for j in pi:
                records.append(
                    {
                        "Type": "ROLL",
                        "PPU": [ppuid],
                        "Roll Name": rolls_df.iloc[j].roll_name,
                        "Material": material_record(rolls_df.iloc[j].roll_name),
                        "Requires Construction": False,
                        "Length Yards": rolls_df.iloc[j].print_yards,
                        "Requires Cutting": not pd.isna(
                            rolls_df.iloc[j].cut_length_yards
                        ),
                    }
                )
            continue
        if (
            i == 0
            or rolls_df.iloc[pi[0]].pretreat_temp
            != rolls_df.iloc[ppu_list[i - 1][0]].pretreat_temp
            or rolls_df.iloc[pi[0]].pretreat_type
            != rolls_df.iloc[ppu_list[i - 1][0]].pretreat_type
            or abs(
                rolls_df.iloc[pi[0]].pretreat_width
                - rolls_df.iloc[ppu_list[i - 1][-1]].pretreat_width
            )
            > 30
            or abs(
                rolls_df.iloc[pi[0]].pretreat_speed
                - rolls_df.iloc[ppu_list[i - 1][-1]].pretreat_speed
            )
            > 1
        ):
            if (
                i > 0
                and rolls_df.iloc[pi[0]].pretreat_type
                != rolls_df.iloc[ppu_list[i - 1][0]].pretreat_type
            ):
                # add a trailing leader for silk...
                records.append(
                    {
                        "Type": "LEADER",
                        "PPU": [ppu_records[i - 1]["id"]],
                        "Roll Name": "LEADER",
                        "Requires Construction": True,
                    }
                )

            records.append(
                {
                    "Type": "LEADER",
                    "PPU": [ppuid],
                    "Roll Name": "LEADER",
                    "Requires Construction": True,
                }
            )
        header_material = "CLTWL" if rolls_df.iloc[pi[0]].weight_gsm > 250 else "RYJ01"
        footer_material = "CLTWL" if rolls_df.iloc[pi[-1]].weight_gsm > 250 else "RYJ01"
        header_rack = (
            [connector_racks[header_material]["id"]]
            if header_material in connector_racks
            else []
        )
        footer_rack = (
            [connector_racks[footer_material]["id"]]
            if footer_material in connector_racks
            else []
        )
        records.append(
            {
                "Type": "HEADER",
                "PPU": [ppuid],
                "Roll Name": "HEADER",
                "Requires Construction": True,
                "Header/Footer": header_rack,
            }
        )
        for j in pi:
            records.append(
                {
                    "Type": "ROLL",
                    "PPU": [ppuid],
                    "Roll Name": rolls_df.iloc[j].roll_name,
                    "Material": material_record(rolls_df.iloc[j].roll_name),
                    "Requires Construction": True,
                    "Length Yards": rolls_df.iloc[j].print_yards,
                    "Requires Cutting": not pd.isna(rolls_df.iloc[j].cut_length_yards),
                }
            )
        records.append(
            {
                "Type": "FOOTER",
                "PPU": [ppuid],
                "Roll Name": "FOOTER",
                "Requires Construction": True,
                "Header/Footer": footer_rack,
            }
        )
    # if there were no knits then add the trailing leader now.
    if rolls_df.iloc[ppu_list[-1][0]].material_type != "Knit":
        records.append(
            {
                "Type": "LEADER",
                "PPU": [ppu_records[-1]["id"]],
                "Roll Name": "LEADER",
                "Requires Construction": True,
            }
        )
    records = [{**r, "Order": len(records) - i} for i, r in enumerate(records)]
    if add_ppu_construction_order:
        SCHEDULE_PPU_CONSTRUCTION_ORDER.batch_create(records, typecast=True)
    # Schedule maintenance
    schedule_maintenance(plan, schedule_date, printers_to_maintain, station_runtimes)
    return True


def get_schedule_date(today_date):

    today_str = today_date.strftime("%Y-%m-%d")

    # temp hack for anaida
    if today_str == "2024-03-22":
        return today_date + timedelta(days=1), None
    if today_str == "2024-03-23":
        return today_date + timedelta(days=2), None

    if today_date.isoweekday() in [6, 7]:
        return None, "Not Scheduling since it is the weekend"

    next_holidays = HOLIDAYS_TABLE.all(
        fields=["Event", "Date"],
        formula=f"AND({{Date}}>='{today_str}', {{Category}}='Holiday - Office Closed', FIND('DR', {{Office}}))",
        sort=["Date"],
        max_records=10,
    )

    for r in next_holidays:
        if today_str == r["fields"]["Date"]:
            return None, f"Not Scheduling since it is {r['fields']['Event']}"

    holiday_dates = set(r["fields"]["Date"] for r in next_holidays)

    schedule_date = today_date + timedelta(days=1)

    while (
        schedule_date.isoweekday() in [6, 7]
        or schedule_date.strftime("%Y-%m-%d") in holiday_dates
    ):
        schedule_date = schedule_date + timedelta(days=1)

    return schedule_date, None


def schedule_ppus(plot_filename=None, active_printers=PRINTERS):
    today_date = datetime.now()
    today_date = datetime(today_date.year, today_date.month, today_date.day)
    schedule_date, no_schedule_reason = get_schedule_date(today_date)
    if no_schedule_reason is not None:
        return (False, no_schedule_reason)
    schedule_date_str = schedule_date.strftime("%d-%m-%Y")
    if len(get_exising_ppu_schedule(schedule_date)) > 0:
        return (
            False,
            f"Already scheduled PPUs for date {schedule_date_str} - see: https://airtable.com/appHqaUQOkRD8VbzX/tblsQ6YiweQ5oPiak/viwz4st6LZItkcjlj?blocks=hide",
        )
    logger.info("loading data")
    _, mat_info, adj_list = material_info()
    rolls_df = current_rolls_df(mat_info)
    total_silk_yards = rolls_df[rolls_df.pretreat_type == "Silk"].print_yards.sum()
    logger.info(f"Total silk yards: {total_silk_yards:.1f}")
    unassigned_rolls = []
    if total_silk_yards < MIN_SILK_YARDS_TO_PRETREAT or schedule_date.weekday() not in [
        1,  # tues
        4,  # fri
    ]:
        logger.info("Not scheduling silks")
        unassigned_rolls = list(rolls_df[rolls_df.pretreat_type == "Silk"].roll_name)
        rolls_df = rolls_df[rolls_df.pretreat_type != "Silk"].reset_index(drop=True)
        clear_nest_roll_assignments(
            ["all"],
            unassigned_rolls,
            False,
            notes=f"Roll not used for PPU schedule for {schedule_date_str}",
            asset_contract_variables=[INSUFFICIENT_SILK_CV]
            if total_silk_yards < MIN_SILK_YARDS_TO_PRETREAT
            else [],
            deallocate_ppu=True,
        )
    logger.info("annealing schedule")
    station_runtimes = get_station_runtimes(schedule_date)
    ppu_list, plan, printers_to_maintain = anneal_ppus_and_printer_maintainance(
        rolls_df, adj_list, station_runtimes, active_printers=active_printers
    )
    assigned = set(i for p in ppu_list for i in p)
    unassigned = set(rolls_df.index) - assigned
    unassigned_yards = sum(rolls_df.iloc[i].print_yards for i in unassigned)
    assigned_yards = rolls_df.print_yards.sum() - unassigned_yards
    plot_sched(
        rolls_df,
        ppu_list,
        plan,
        schedule_date_str,
        station_runtimes,
        printers_to_maintain,
        filename=plot_filename,
    )
    total_time = (
        max(p["dry_finish"] for p in plan) - station_runtimes.pretreat.start
    ).seconds
    scheduled_airtable = schedule_to_airtable(
        rolls_df,
        ppu_list,
        plan,
        station_runtimes,
        printers_to_maintain,
        schedule_date,
        today_date,
        total_silk_yards >= SILK_YARDS_FOR_FULL_BATCH,
    )
    if not scheduled_airtable:
        return (
            False,
            f"Already scheduled PPUs for date {schedule_date_str} - see: https://airtable.com/appHqaUQOkRD8VbzX/tblsQ6YiweQ5oPiak/viwz4st6LZItkcjlj?blocks=hide",
        )
    if len(unassigned) > 0:
        clear_nest_roll_assignments(
            ["all"],
            [rolls_df.iloc[i].roll_name for i in unassigned],
            False,
            notes=f"Roll not used for PPU schedule for {schedule_date_str}",
            asset_contract_variables=[TOO_MUCH_YARDAGE_CV],
            deallocate_ppu=True,
        )
        unassigned_rolls.extend([rolls_df.iloc[i].roll_name for i in unassigned])
    return (
        True,
        f"Scheduled {assigned_yards:.1f} yards and {rolls_df.shape[0]-len(unassigned)} rolls into {len(ppu_list)} PPUs taking {total_time/3600:.2f} hours in the material node on {schedule_date_str},  assuming active printers {active_printers}.  Unassigning rolls {unassigned_rolls}.  Scheduling maintenance on {printers_to_maintain}.",
    )


def get_roll_names_for_ppu(ppu_name):
    ppu_record = SCHEDULE_PPU_CREATION.all(
        fields=["Roll Names"], formula=f"{{Name}}='{ppu_name}'"
    )
    if len(ppu_record) == 1:
        return ppu_record[0]["fields"]["Roll Names"].split(",")
    else:
        raise ValueError(f"Failed to load ppu {ppu_name}")


def assign_ppus_to_printers():
    scheduled_ppus = SCHEDULE_PPU_CREATION.all(
        formula=f"""{{Scheduled Print Date}} >= '{datetime.now().strftime("%Y-%m-%d")}'""",
    )
    logger.info(f"Got {len(scheduled_ppus)} ppus")
    for p in scheduled_ppus:
        roll_names = p["fields"]["Roll Names"].split(",")
        printer_id = p["fields"]["Printer"][0]
        printer_name = next(p for p, i in SCHEDULE_PRINTER_IDS if i == printer_id)
        printer_name = "JP7 " + printer_name.upper()
        for roll_name in roll_names:
            try:
                logger.info(f"assigning: {roll_name} to {printer_name}")
                reassign_roll(roll_name, printer_name)
            except Exception as ex:
                logger.warn(f"Failed to assign roll {repr(ex)}")


# helper method to kick off nesting jobs.
def start_nesting_job(job_name, **kwargs):
    res.connectors.load("argo").handle_event(
        {
            "apiVersion": "resmagic.io/v1",
            "args": {
                "slack_channel": "make_one_team",
                **kwargs,
            },
            "assets": [],
            "kind": "resFlow",
            "metadata": {
                "name": "make.nest.progressive.construct_rolls",
                "version": "primary",
            },
            "task": {
                "key": job_name,
            },
        },
        context={},
        template="res-flow-node",
        unique_job_name=job_name,
    )


def ppu_allocate_length(material_code, needed_length):
    candidate_rolls = ROLLS.all(
        formula=f"""
        AND(
            {{Print ONE Ready}}=0,
            {{Material Code}}='{material_code}',
            {{Current Substation}}!='Done',
            {{Current Substation}}!='Write Off',
            {{Current Substation}}!='Expired',
            {{Current Substation}}!='Print',
            {{Current Substation}}!='Steam',
            {{Current Substation}}!='Wash',
            {{Current Substation}}!='Dry',
            {{Current Substation}}!='Inspection',
            {{Current Substation}}!='Soft',
            {{Current Substation}}!='Scour: Wash',
            {{Current Substation}}!='Scour: Dry',
            {{Flag for Review}}=FALSE(),
            {{Print Assets (flow) 3}}='',
            {{Latest Measurement}}!=''
        )
        """
    )
    target_roll = next(
        (
            r
            for r in candidate_rolls
            if float(r["fields"]["Latest Measurement"]) > needed_length + 10
            or (
                float(r["fields"]["Latest Measurement"]) > needed_length
                and float(r["fields"]["Latest Measurement"]) < needed_length + 5
            )
        ),
        None,
    )

    if target_roll is not None:
        ROLLS.update(
            target_roll["id"],
            {
                "Schedule to pretreat": "True",
                "Manual Pick Pretreat": "True",
                "Cut To Length (yards)": needed_length,
                "Allocated for PPU": True,
            },
            typecast=True,
        )

    return target_roll


def ppu_replace_roll(roll_name):
    roll_record = ROLLS.all(formula=f"{{Name}}='{roll_name}'", max_records=1)[0]
    needed_length = roll_record["fields"]["Cut To Length (yards)"]
    material_code = roll_record["fields"]["Material Code"][0]
    target_roll = ppu_allocate_length(material_code, needed_length)

    # unassign previous roll
    clear_nest_roll_assignments(
        ["all"],
        [roll_name],
        False,
        notes="Roll unassigned due to PPU process failure",
        asset_contract_variables=["reccyLMIhbma0MfBn"],
        deallocate_ppu=True,
    )

    target_roll_name = None if target_roll is None else target_roll["fields"]["Name"]
    update_ppu_def(roll_name, target_roll_name)

    if target_roll is not None:
        # make a reassignment
        start_nesting_job(
            f"reassign-ppu-{int(time.time())}",
            materials=[material_code],
            fill_allocations=True,
            roll_names=[target_roll_name],
        )
        return target_roll_name

    else:
        return "NO_ROLL"


def ppu_renest_roll(roll_name, new_length_yards=None):
    if new_length_yards is not None:
        # update the rolls length in the print app and the schedule.
        today = datetime.now().strftime("%m/%d/%Y")
        ppu_construction_record = SCHEDULE_PPU_CONSTRUCTION_ORDER.all(
            formula=f"AND({{Roll Name}}='{roll_name}', {{PPU Creation Day}}>='{today}')",
            max_records=1,
        )[0]
        SCHEDULE_PPU_CONSTRUCTION_ORDER.update(
            ppu_construction_record["id"], {"Length Yards": new_length_yards}
        )
        roll_record = ROLLS.all(
            formula=f"{{Name}}='{roll_name}'",
            max_records=1,
        )[0]
        ROLLS.update(roll_record["id"], {"Cut To Length (yards)": new_length_yards})
    clear_nest_roll_assignments(
        ["all"],
        [roll_name],
        False,
        notes="Roll unassigned due to PPU process failure",
        asset_contract_variables=["reccyLMIhbma0MfBn"],
    )
    start_nesting_job(
        f"reassign-ppu-{int(time.time())}",
        materials=[roll_name.split(": ")[1]],
        fill_allocations=True,
        roll_names=[roll_name],
    )


def update_ppu_def(roll_name, target_roll_name=None):
    """
    When unassigning a roll or changing a roll on a ppu - edit the corresponding schedule record.
    """
    today = datetime.now().strftime("%m/%d/%Y")

    # update ppu definitions
    ppu_construction_record = SCHEDULE_PPU_CONSTRUCTION_ORDER.all(
        formula=f"AND({{Roll Name}}='{roll_name}', {{PPU Creation Day}}>='{today}')",
        max_records=1,
    )[0]

    ppu_definition_record = SCHEDULE_PPU_CREATION.all(
        formula=f"AND(FIND('{roll_name}', {{Roll Names}}), {{Created At}}>='{today}')",
        max_records=1,
    )[0]

    if target_roll_name is not None:
        SCHEDULE_PPU_CONSTRUCTION_ORDER.update(
            ppu_construction_record["id"], {"Roll Name": target_roll_name}
        )
        SCHEDULE_PPU_CREATION.update(
            ppu_definition_record["id"],
            {
                "Roll Names": ppu_definition_record["fields"]["Roll Names"].replace(
                    roll_name, target_roll_name
                ),
                "Roll Order At Construction": ppu_definition_record["fields"][
                    "Roll Order At Construction"
                ].replace(roll_name, target_roll_name),
            },
        )
    else:
        SCHEDULE_PPU_CONSTRUCTION_ORDER.delete(ppu_construction_record["id"])
        SCHEDULE_PPU_CREATION.update(
            ppu_definition_record["id"],
            {
                "Roll Names": ppu_definition_record["fields"]["Roll Names"].replace(
                    roll_name, ""
                ),
                "Roll Order At Construction": ppu_definition_record["fields"][
                    "Roll Order At Construction"
                ].replace(roll_name, ""),
            },
        )
