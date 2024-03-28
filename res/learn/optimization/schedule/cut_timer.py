from datetime import datetime, timedelta
import io
import math
import random

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from shapely.affinity import scale, translate
from shapely.geometry import box, LineString
from shapely.ops import unary_union
from shapely.wkt import loads as shapely_loads

from res.airtable.cut import (
    MATERIALS_PRINTED_PIECES,
    LOCKED_SETTINGS_REFRAME,
    SCHEDULING_VARIABLES,
)
from res.airtable.cut import CUT_NODE_RESOURCES, EXPECTED_TIMING, ROLLS as CUT_ROLLS
from res.airtable.print import ROLLS as PRINT_ROLLS
from res.airtable.schedule import (
    SCHEDULE_CUT_SCHEDULE,
    SCHEDULE_PPU_SCHEDULE,
)
from res.airtable.purchasing import MATERIALS
from res.airtable.print import NESTS
from res.learn.optimization.packing.annealed.progressive import (
    SEPERATOR_HEIGHT_PX_UNCOMPENSATED,
)
import res

# everything in shapely we store as pixels and it's 300 px to an inch
PIXELS_PER_INCH = 300
# 1 inch is 25.4 mm. a lot of things is measured in mm or mm/sec
MM_PER_INCH = 25.4
# easy to just use one conversion factor for mm to px
PIXELS_PER_MM = PIXELS_PER_INCH / MM_PER_INCH

# 88 inches
WINDOW_SIZE_INCHES = 88
WINDOW_SIZE = WINDOW_SIZE_INCHES * PIXELS_PER_INCH

# daily start time in hours for cutting
DAILY_START_TIME_HOURS = 8
DAILY_START_TIME_MINUTES = 30
OVERTIME_START = 17

# making up some numbers here
# suppose it takes 2 minutes to separate the fuse from the self
SEPARATION_TIME_SEC = 120
# buffer time between actions for stations and rolls. Shows up as white space on schedule
BUFFER_TIME_SEC = 60

# these could all be enums
# roll statuses:
TO_FUSE = "to_fuse"
TO_CUT = "to_cut"
TO_SEPARATE = "to_separate"
DONE = "done"

# stations:
LASER = "Laser"
SEPARATION = "Separation"
FUSING = "Fusing"

# nest types
FUSE = "fuse"
SELF = "self"
RUNNING = "running"


class CutMachineInfoLoader:
    def __init__(self):
        self.machine_speeds = self._load_machine_speeds()
        self.laser_to_belt_speed = self._load_belt_speeds()
        (
            self.available_lasers,
            self.unavailable_lasers_to_reason,
        ) = self._load_machine_status()

    def _load_machine_status(self):
        df = CUT_NODE_RESOURCES.df(
            formula="FIND('LASER CUTTER',{Name})",
            field_map={"Name": "name", "Resource Status": "status"},
        )

        df["laser_number"] = df["name"].str.strip().str[-1]
        available_lasers = (
            df[df["status"].isin(["Production", "Idle", "OFF"])]["laser_number"]
            .astype(int)
            .tolist()
        )
        unavailable_lasers_to_reason = (
            df[~df["status"].isin(["Production", "Idle", "OFF"])]
            .set_index("laser_number")["status"]
            .to_dict()
        )
        return available_lasers, unavailable_lasers_to_reason

    def _load_machine_speeds(self):
        return MATERIALS_PRINTED_PIECES.df(
            field_map={
                "Material Code": "material_code",
                "Laser Cutter 1: Speed (mm/s)": "laser_cutter_1_speed",
                "Laser Cutter 2: Speed (mm/s)": "laser_cutter_2_speed",
                "Laser Cutter 3: Speed (mm/s)": "laser_cutter_3_speed",
                "Laser Cutter 4: Speed (mm/s)": "laser_cutter_4_speed",
            },
            include_id=False,
        )

    def _load_belt_speeds(self):
        # filtering on array fields doesn't seem to work in airtable formula so we do it in pandas
        locked_settings_df = LOCKED_SETTINGS_REFRAME.df(
            field_map={
                "Locked Value": "value",
                "Machine (from Machine Settings)": "machine_from_machine_settings",
                "Units (from Machine Settings)": "units",
                "Name (from Machine Settings)": "name",
            }
        )
        locked_settings_df["name"] = locked_settings_df["name"].apply(
            lambda x: x[0] if isinstance(x, list) else x
        )
        locked_settings_df = locked_settings_df[
            locked_settings_df["name"].str.contains("Belt Speed")
        ]
        locked_settings_df["machine_from_machine_settings"] = locked_settings_df[
            "machine_from_machine_settings"
        ].apply(lambda x: x[0] if isinstance(x, list) else x)

        # verify machine_from_machine_settings is of the form "Laser N" where N is the number using regex
        locked_settings_df = locked_settings_df[
            locked_settings_df["machine_from_machine_settings"].str.match(r"Laser \d")
        ]
        # verify all units are "mm/sec"
        locked_settings_df["units"] = locked_settings_df["units"].apply(
            lambda x: x[0] if isinstance(x, list) else x
        )
        locked_settings_df["units"] = locked_settings_df["units"].str.strip()
        if locked_settings_df["units"].nunique() != 1:
            raise ValueError(
                "There are multiple units for the fusing belt speeds. This is unexpected."
            )
        if locked_settings_df["units"].iloc[0] != "mm/sec":
            raise ValueError(
                "The units for the fusing belt speeds are not mm/sec. This is unexpected."
            )
        # machine_from_machine_settings for the relevant rows is "Laser N" where N is the number
        locked_settings_df["laser_number"] = (
            locked_settings_df["machine_from_machine_settings"]
            .str.extract(r"Laser (\d)")
            .astype(int)
        )
        # return the dictionary of laser number to belt speed
        return locked_settings_df.set_index("laser_number")["value"].to_dict()

    def get_belt_speed(self, laser_number):
        """Gets the belt speed of a given laser in px/sec

        Args:
            laser_number (int): The laser number we want the speed for. Note this is 1 indexed since it is named 1,2,3,4

        Returns:
            int: Belt speed in px/sec
        """
        return self.laser_to_belt_speed[laser_number] * PIXELS_PER_MM

    def get_laser_speed(self, material_code, laser_number):
        """Gets the speed of a given laser for a given material code. Note that some material codes have
        different speeds for different lasers.

        Args:
            material_code (str): The material code we want the speed for
            laser_number (int): The laser number we want the speed for. Note this is 1 indexed since it is named 1,2,3,4

        Returns:
            int: Laser speed in px/sec
        """
        return (
            self.machine_speeds[
                (self.machine_speeds["material_code"] == material_code)
            ][f"laser_cutter_{laser_number}_speed"].iloc[0]
            * PIXELS_PER_MM
        )  # mm/s to px/s


class FuseMachineInfoLoader:
    def __init__(self):
        self._fusable_materials = self._load_fusable_materials()
        self._locked_settings = self._load_locked_settings()
        self._code_to_machine_speeds = self._load_machine_speeds()
        self.fusing_block_set_up_time = self._load_fusing_block_set_up_time()

    def _load_machine_speeds(self):
        # locked settings has the actual speeds, and we join with
        # the fusable materials to get the res_pack_code
        machine_speeds_by_code_df = self._locked_settings.merge(
            self._fusable_materials, left_on="key_from_fusing_material", right_on="key"
        )
        return machine_speeds_by_code_df.set_index("res_pack_code")["value"].to_dict()

    def _load_locked_settings(self):
        locked_settings_df = LOCKED_SETTINGS_REFRAME.df(
            formula="{Name (from Machine Settings)}='Fusing Machine - 1 [Belt Speed]'",
            field_map={
                "Scheduling Value": "value",
                "Scheduling Units (from Machine Settings)": "units",
                "Machine Settings": "machine_settings",
                "Name (from Machine Settings)": "name_from_machine_settings",
                "Key (from Fusing Material)": "key_from_fusing_material",
            },
        )

        # confirm that each "units" is "mm/sec"
        locked_settings_df["units"] = locked_settings_df["units"].apply(
            lambda x: x[0] if isinstance(x, list) else x
        )
        locked_settings_df["units"] = locked_settings_df["units"].str.strip()
        if locked_settings_df["units"].nunique() != 1:
            raise ValueError(
                "There are multiple units for the fusing belt speeds. This is unexpected."
            )
        if locked_settings_df["units"].iloc[0] != "mm/sec":
            raise ValueError(
                "The units for the fusing belt speeds are not mm/sec. This is unexpected."
            )

        # this is the key we will use to join with the fusable materials code
        locked_settings_df = locked_settings_df.explode("key_from_fusing_material")
        locked_settings_df["key_from_fusing_material"] = locked_settings_df[
            "key_from_fusing_material"
        ].str.strip()
        return locked_settings_df

    def _load_fusable_materials(self):
        fusable_materials_df = MATERIALS.df(
            formula="""
            AND(
                {SubCategory Name}='Fusing',
                {Development Status}='Approved'
            )
            """,
            field_map={
                "Key": "key",
                "res.Pack Code": "res_pack_code",
            },
        )
        fusable_materials_df["key"] = fusable_materials_df["key"].str.strip()
        # res_pack_code is the code nesting uses. Key is the record in the Locked Settings table (see column "Fusing Material")
        return fusable_materials_df

    def get_fuse_speed(self, res_pack_code):
        """
        This function returns the fusing machine speed in px/s for the
        associated res_pack_code (AKA fusing material code).
        Not every fusing has a known material code (res_pack_code == "none"). We return the slowest speed in that case.
        Not every res_pack_code has a locked setting, so we return the slowest speed if it's not found.
        Ideally we'd be able to fix this, but our data is not perfect. We do not want to fail if we can't find the speed.
        """
        if res_pack_code.upper() == "NONE":
            return self.get_slowest_fuse_speed()
        if res_pack_code.upper() not in self._code_to_machine_speeds:
            res.utils.logger.info(
                f"WARNING: res_pack_code {res_pack_code} is not associated with any locked settings fusing machine speed. Using slowest speed."
            )
            return self.get_slowest_fuse_speed()

        return (
            self._code_to_machine_speeds[res_pack_code.upper()] * PIXELS_PER_MM
        )  # mm/s to px/s

    def get_slowest_fuse_speed(self):
        """
        This function returns the slowest fusing machine speed known for a fusing material in px/s.
        """
        return (
            min(self._code_to_machine_speeds.values()) * PIXELS_PER_MM
        )  # mm/s to px/s

    def _load_fusing_block_set_up_time(self):
        scheduling_variables_df = SCHEDULING_VARIABLES.df(
            formula="{Name}='Set Up Block Fuse Section in Fusing Machine'",
            field_map={
                "Fixed Value": "value",
                "Specified Units": "units",
            },
        )
        units = scheduling_variables_df["units"].iloc[0]
        if units != "sec":
            raise ValueError(
                f"Expected units to be 'sec' but got {units} for Set Up Block Fuse Section in Fusing Machine"
            )
        time = scheduling_variables_df["value"].iloc[0]
        if time is None:
            raise ValueError(
                "Expected a time for Set Up Block Fuse Section in Fusing Machine"
            )
        return time

    def get_block_prep_time(self):
        return self.fusing_block_set_up_time


class RollsLoader:
    def __init__(
        self, lookback_window_days: int = 7, include_future_rolls: bool = False
    ):
        self.include_future_rolls = include_future_rolls
        self.lookback_window_days = lookback_window_days
        self._rolls_to_estimated_print = self._get_lookback_estimated_print_rolls()
        # Note that we are still pulling from two different airtable tables to get the rolls.
        # I don't trust the cut table to have all the rolls that have been printed, so we also pull from the print table
        # However, if this ends up being a problem, we can just pull from cut
        self._cut_rolls = self._get_cut_rolls()
        self._failed_rolls = self._get_failed_rolls()
        self._print_exit_rolls = self._get_print_exit_rolls()
        self.rolls_to_cut_to_estimated_print = self._get_rolls_to_cut()

    def _get_lookback_estimated_print_rolls(self) -> dict[str, str]:
        """Get the roll names that were printed in the lookback window"""
        ppu_schedule_df = SCHEDULE_PPU_SCHEDULE.df(
            formula=f"""{{Planned End Time}}>='{pd.to_datetime("today") - pd.Timedelta(days=self.lookback_window_days)}'""",
            field_map={
                "PPU Roll Names": "roll_names",
                "Planned End Time": "planned_end_time",
                "Asset Type": "asset_type",
            },
        )
        ppu_schedule_df = ppu_schedule_df.explode("roll_names").rename(
            columns={"roll_names": "roll_name"}
        )
        # sometimes the roll names are just comma separated but not actually a list
        ppu_schedule_df["roll_name"] = ppu_schedule_df["roll_name"].apply(
            lambda x: x.split(",")
        )
        ppu_schedule_df = ppu_schedule_df.explode("roll_name")
        ppu_schedule_df["roll_name"] = ppu_schedule_df["roll_name"].str.strip()
        ppu_schedule_df = ppu_schedule_df.groupby("roll_name")["planned_end_time"].max()
        return ppu_schedule_df.to_dict()

    def _get_print_exit_rolls(self) -> list[str]:
        """Get the roll names that have exited print in the lookback window"""
        rolls_df = PRINT_ROLLS.df(
            formula=f"""AND(
                {{Current Substation}}='Done',
                {{Print Exit Date At}}>='{pd.to_datetime("today") - pd.Timedelta(days=self.lookback_window_days)}'
            )
            """,
            field_map={
                "Name": "roll_name",
            },
        )
        if rolls_df.empty:
            return []
        return rolls_df["roll_name"].tolist()

    def _get_cut_rolls(self) -> list[str]:
        """Get the roll names that were cut in the lookback window"""
        cut_schedule_df = CUT_ROLLS.df(
            formula=f"""{{Started Cutting Time}}>='{pd.to_datetime("today") - pd.Timedelta(days=self.lookback_window_days)}'""",
            field_map={
                "key": "roll_name",
            },
        )
        if cut_schedule_df.empty:
            return []
        return cut_schedule_df["roll_name"].tolist()

    def _get_failed_rolls(self) -> list[str]:
        """Get the roll names that failed color inspection on the way to cut in the lookback window"""
        cut_schedule_df = CUT_ROLLS.df(
            formula=f"""
                AND(
                    {{Scheduled Print Date}}>='{pd.to_datetime("today") - pd.Timedelta(days=self.lookback_window_days)}',
                    OR(
                        {{Color Inspection Status}}='FAIL',
                        {{Color Inspection Status}}='FAIL - Engineering Resolution',
                        {{Color Inspection Status}}='Engineering Review'
                    )
                )
            """,
            field_map={
                "key": "roll_name",
            },
        )
        if cut_schedule_df.empty:
            return []
        return cut_schedule_df["roll_name"].tolist()

    def _get_rolls_to_cut_using_future_rolls(self) -> dict[str, datetime | None]:
        """Get the roll names that are expected to be printed in the lookback window that still need to be cut

        Returns:
            dict[str, datetime]: a map from the roll name to the estimated print time
        """
        rolls_to_cut = {}
        for (
            roll_name,
            estimated_print_time_str,
        ) in self._rolls_to_estimated_print.items():
            if roll_name not in self._cut_rolls and roll_name not in self._failed_rolls:
                rolls_to_cut[roll_name] = datetime.strptime(
                    estimated_print_time_str, "%Y-%m-%dT%H:%M:%S.%fZ"
                )
        return rolls_to_cut

    def _get_rolls_to_cut_already_printed(self) -> dict[str, datetime | None]:
        """Get the roll names that have exited print in the lookback window that still need to be cut

        Returns:
            dict[str, datetime]: a map from the roll name to None (there's probably a neater way making a class to do this)
        """
        rolls_to_cut = {}
        for roll_name in self._print_exit_rolls:
            if roll_name not in self._cut_rolls and roll_name not in self._failed_rolls:
                rolls_to_cut[roll_name] = None
        return rolls_to_cut

    def _get_rolls_to_cut(self) -> dict[str, datetime | None]:
        """Get the roll names that need to be cut depending on the strategy"""
        if self.include_future_rolls:
            return self._get_rolls_to_cut_using_future_rolls()
        else:
            return self._get_rolls_to_cut_already_printed()

    def get_rolls_to_cut(self) -> dict[str, datetime | None]:
        """Get the roll names that need to be cut"""
        return self.rolls_to_cut_to_estimated_print


def get_fuse_material_code(nest_s3_path):
    """
    Returns the fuse material code from the s3 path. This is the string after the last "_fuse_" and before the "."
    If it is not a "fuse" type, it returns None.
    Note that sometimes the fuse material code can be the string "none". This is a valid unknown fuse material code
    that is separate from not returning a fuse material code at all.
    """
    if "_fuse_" not in nest_s3_path:
        return None
    return nest_s3_path.split("/")[-1].split("_fuse_")[-1].split(".")[0]


def get_concatenated_df(existing_df, new_df):
    """
    Concatenates the new_df to the existing_df, shifting the nested_shapes in new_df up by the highest y value in existing_df
    Returns the concatenated df. If the existing_df is None, it just returns the new_df
    """
    if existing_df is None:
        return new_df
    else:
        # first shift the nested_shapes from df to be translated up by the highest y value in existing_df
        highest_y = existing_df["nested_shapes"].apply(lambda x: x.bounds[3]).max()
        new_df["nested_shapes"] = new_df["nested_shapes"].apply(
            lambda x: translate(x, yoff=highest_y + SEPERATOR_HEIGHT_PX_UNCOMPENSATED)
        )
        return pd.concat([existing_df, new_df])


def get_fuse_dfs_from_roll_name(roll_name) -> dict:
    """
    Load the dfs from the roll name, and filter by the nest type == fuse.
    Then add a column for the nested shapes, which are the shapes from
    the nested_geometry column, scaled by the stretch_x and stretch_y columns.
    These shapes are then translated up and concatenated into a single df.
    Return a dictionary for each fuse material code to the concatenated df for that fuse material code.
    """
    nests = NESTS.all(
        formula=f"""
        AND(
            {{Roll Name}}='{roll_name}',
            FIND('_fuse',{{Argo Jobkey}})
        )
        """,
        fields=["Dataframe Path", "Rank"],
        sort=["-Rank"],
    )
    fuse_material_to_df = {}
    for nest in nests:
        s3_path = nest["fields"]["Dataframe Path"]
        fuse_material_code = get_fuse_material_code(s3_path)
        df = res.connectors.load("s3").read(s3_path)
        df["nested_shapes"] = df.apply(
            lambda x: scale(
                shapely_loads(x["nested_geometry"]),
                1.0 / x["stretch_x"],
                1.0 / x["stretch_y"],
            ),
            axis=1,
        )
        fuse_material_df = fuse_material_to_df.get(fuse_material_code, None)
        fuse_material_df = get_concatenated_df(fuse_material_df, df)
        fuse_material_to_df[fuse_material_code] = fuse_material_df

    return fuse_material_to_df


def get_self_df_from_roll_name(roll_name):
    """
    Load the df from the roll name, and filter by the nest type == self.
    Then add a column for the nested shapes, which are the shapes from
    the nested_geometry column, scaled by the stretch_x and stretch_y columns.
    These shapes are then translated up and concatenated into a single df.
    """
    nests = NESTS.all(
        formula=f"""
        AND(
            {{Roll Name}}='{roll_name}',
            FIND('_self',{{Argo Jobkey}})
        )
        """,
        fields=["Dataframe Path", "Rank"],
        sort=["-Rank"],
    )
    concatenated_df = None
    for nest in nests:
        s3_path = nest["fields"]["Dataframe Path"]
        df = res.connectors.load("s3").read(s3_path)
        df["nested_shapes"] = df.apply(
            lambda x: scale(
                shapely_loads(x["nested_geometry"]),
                1.0 / x["stretch_x"],
                1.0 / x["stretch_y"],
            ),
            axis=1,
        )
        concatenated_df = get_concatenated_df(concatenated_df, df)
    return concatenated_df


def get_length_within_window(polygon, window):
    """
    get the length of the polygon within the window
    note this function only works for box windows due to the bottom edge assumption where
    we are assuming we do not want to double count the bottom edge of the window
    """
    polygon_view = polygon.intersection(window)
    # this is the full perimeter, including the edges on the window
    perimeter_in_view = polygon_view.length
    # any edges on the window must be removed, unless it was already on the original boundary
    # if it was on the original boundary, we don't want to double count it,
    # so we remove it if it is on the bottom edge of the window (we assume the window starts fully below the polygon)

    # these are the perimeters that are on the window edges prior to the window intersection
    original_boundary_intersection_length = polygon.boundary.intersection(
        window.boundary
    ).length

    # here are the perimeters that are on the window edges after the window intersection
    polygon_boundary_on_window = window.boundary.intersection(
        polygon_view.boundary
    ).length
    # ignore any boundary on the bottom edge of the window
    minx, miny, maxx, _ = window.bounds
    bottom_edge_of_window = LineString([(minx, miny), (maxx, miny)])
    polygon_boundary_on_bottom_edge_of_window = polygon.boundary.intersection(
        bottom_edge_of_window
    ).length
    polygon_boundary_on_window = (
        polygon_boundary_on_window - polygon_boundary_on_bottom_edge_of_window
    )
    # finally we can return the perimeter in view minus the edges on the window that were not on the original boundary except for the bottom edge
    return perimeter_in_view - (
        polygon_boundary_on_window - original_boundary_intersection_length
    )


def get_windows_lengths(geometry, window_size):
    """
    Returns the a list of lengths (perimeters) of the nested geometries in geometry within each window
    """
    start_y = 0
    end_y = start_y + window_size
    start_y = 0
    end_y = start_y + window_size
    sliding_window = box(0, start_y, 20000, end_y)

    windows_lengths = []
    while end_y < geometry.bounds[3] + window_size:
        length = get_length_within_window(geometry, sliding_window)
        windows_lengths.append(length)
        start_y += window_size
        end_y += window_size
        sliding_window = box(0, start_y, 20000, end_y)
    return windows_lengths


def get_geometry_from_nested_shapes_df(df):
    """
    Creates and returns the unioned geometry from the "nest_shapes" column in the df
    """
    return unary_union(df["nested_shapes"])


def get_lasers_to_time_from_df(roll_df, cut_machine_info_loader: CutMachineInfoLoader):
    """_summary_

    Args:
        roll_df (DataFrame): The df containing the roll information - notably the "nested_shapes" column and material codes in "material" column
        cut_machine_info_loader (CutMachineInfoLoader): an instance of the CutMachineInfoLoader

    Returns:
        dict: a dictionary from laser number to a list of times it takes to cut up to each window
    """
    roll_geom = get_geometry_from_nested_shapes_df(roll_df)
    material_code = roll_df["material"].iloc[0]
    windows_lengths = get_windows_lengths(roll_geom, WINDOW_SIZE)

    lasers_to_time = {}
    for i in range(1, 5):
        laser_speed_px_sec = cut_machine_info_loader.get_laser_speed(material_code, i)

        window_times = []
        total_time = 0
        for window_length in windows_lengths:
            laser_cut_time = window_length / laser_speed_px_sec
            total_time += laser_cut_time
            # add the amount of time it takes to move the belt to the this window
            belt_moving_time = WINDOW_SIZE / cut_machine_info_loader.get_belt_speed(i)
            total_time += belt_moving_time
            window_times.append(total_time)
        lasers_to_time[i] = window_times

    return lasers_to_time


def get_window_index_for_piece(piece_geom):
    """
    Returns the index of the window that the piece is in.
    If the piece is in multiple windows it returns the highest window index it is in.
    """
    return int(piece_geom.bounds[3] / WINDOW_SIZE)


def get_time_to_window_cut(window_index, lasers_to_time):
    """
    get a dict of laser number to the time it takes to cut up to and including to that window
    """
    laser_to_window_time = {}
    for laser_number in lasers_to_time.keys():
        laser_to_window_time[laser_number] = lasers_to_time[laser_number][window_index]
    return laser_to_window_time


def get_df_with_timing_from_df(df, cut_machine_info_loader: CutMachineInfoLoader):
    """
    takes a df and adds the following columns:
    - lasers_to_time: a dictionary from laser number to a list of times it takes to cut up to each window
    - window_index: the index of the window that the piece is in
    - lasers_to_time_to_window_cut: a dictionary from laser number to the time it takes to cut up to and including to that window
    - roll_max_y: the top y value of the roll (or really of the unioned nested shapes in the df)
    """
    if df is None:
        return None
    lasers_to_time = get_lasers_to_time_from_df(df, cut_machine_info_loader)
    df["lasers_to_time"] = [lasers_to_time] * len(df)
    df["window_index"] = df["nested_shapes"].apply(get_window_index_for_piece)
    df["lasers_to_time_to_window_cut"] = df.apply(
        lambda x: get_time_to_window_cut(x["window_index"], x["lasers_to_time"]), axis=1
    )
    df["top_y"] = df["nested_shapes"].apply(lambda x: x.bounds[3])
    df["roll_max_y"] = df["top_y"].max()
    return df


def get_fuse_dfs_with_timing_from_roll(
    roll_name, cut_machine_info_loader: CutMachineInfoLoader
) -> dict:
    """Takes a roll name and pulls the dict of fuse material codes to their concatenated df and adds the timing information to each df

    Args:
        roll_name (str): the roll name
        cut_machine_info_loader (CutMachineInfoLoader): an instance of the CutMachineInfoLoader

    Returns:
        dict: fuse material codes to the dfs with the timing information added
    """
    fuse_material_code_to_df = get_fuse_dfs_from_roll_name(roll_name)
    for fuse_material_code, fuse_df in fuse_material_code_to_df.items():
        fuse_material_code_to_df[fuse_material_code] = get_df_with_timing_from_df(
            fuse_df, cut_machine_info_loader
        )
    return fuse_material_code_to_df


def get_self_df_with_timing_from_roll(
    roll_name, cut_machine_info_loader: CutMachineInfoLoader
):
    """
    Gets the self df from the roll name and returns that with the timing information added
    """
    df = get_self_df_from_roll_name(roll_name)
    return get_df_with_timing_from_df(df, cut_machine_info_loader)


def get_lasers_to_total_time(df):
    """
    Get the laser numbers to the total time it takes to cut the roll
    """
    lasers_to_all_window_times = df["lasers_to_time"].iloc[0]
    lasers_to_total_time = {
        laser: max(times) for laser, times in lasers_to_all_window_times.items()
    }
    return lasers_to_total_time


# as we schedule each roll should have an action priority
# which is relative to the other rolls' action priorities
class RollState:
    def __init__(
        self,
        roll_name: str,
        roll_index: str,
        roll_state: str,  # TODO: this should be an enum
        # it's ok for this to be an empty dict if the roll's state is to be separated
        lasers_to_total_time: dict[int, float],
        fuse_time: float | None = None,
        nest_type: str | None = None,
        roll_busy_until: float = 0,
    ):
        self.roll_name = roll_name
        self.roll_index = roll_index
        self.roll_state = roll_state
        self.roll_busy_until = roll_busy_until
        self.lasers_to_total_time = lasers_to_total_time
        self.nest_type = nest_type
        self.fuse_time = fuse_time
        self.required_laser = None
        self.associated_roll = None

    def __str__(self):
        return f"RollInfo(roll_index={self.roll_index}, roll_state={self.roll_state}, roll_busy_until={self.roll_busy_until}, nest_type={self.nest_type})"

    def is_roll_free(self, time):
        return time >= self.roll_busy_until

    def add_associated_roll(self, roll):
        self.associated_roll = roll

    def set_associated_required_laser(self, laser_number):
        if self.associated_roll is not None:
            self.associated_roll.set_required_laser(laser_number)
            return self.associated_roll

    def set_required_laser(self, laser_number):
        self.required_laser = laser_number

    def get_fuse_time(self):
        if self.fuse_time is None:
            if self.roll_state == TO_FUSE:
                raise ValueError(f"Fuse time is None for roll {self.roll_index}")
            else:
                raise ValueError(
                    f"Calling get_fuse_time when roll_state is {self.roll_state} for {self.roll_index}"
                )
        return self.fuse_time

    def is_queue_valid(self):
        return self.roll_state == TO_CUT or self.roll_state == TO_FUSE


class SeparatedRollInfo:
    def __init__(
        self,
        roll_name: str,
        modifier: str,
        lasers_to_time: dict[int, float],
        piece_df: pd.DataFrame,
        fusing_time: float | None = None,
    ):
        self.roll_name: str = roll_name
        self.modifier: str = modifier
        self.lasers_to_time: dict[int, float] = lasers_to_time
        self.piece_df: pd.DataFrame = piece_df
        self.fusing_time: float | None = fusing_time

    def generate_post_separation_initial_roll_state(
        self, roll_busy_until: float
    ) -> RollState:
        roll_index = self.roll_name + self.modifier
        state = TO_CUT if self.modifier == "_self" else TO_FUSE
        nest_type = SELF if self.modifier == "_self" else FUSE
        return RollState(
            self.roll_name,
            roll_index,
            state,
            self.lasers_to_time,
            nest_type=nest_type,
            fuse_time=self.fusing_time,
            roll_busy_until=roll_busy_until,
        )


class RollInfo:
    def __init__(
        self,
        roll_name: str,
        modifier_to_separated_info: dict[str, SeparatedRollInfo],
        separation_required: bool,
        # fuse_material_to_time: dict[str, float],
        # roll_modifiers: list[str],
        # modifiers_to_piece_df: dict[str, pd.DataFrame],
        estimated_print_time: datetime | None,
    ):
        self.roll_name = roll_name
        # the time it takes to cut the entire roll for each laser number
        self.modifier_to_separated_info: dict[
            str, SeparatedRollInfo
        ] = modifier_to_separated_info
        # does this roll require separation
        self.separation_required: bool = separation_required
        # estimated time the roll will be done being printed and will be available for cut. None if already printed for sure
        self.estimated_print_time: datetime | None = estimated_print_time

    def generate_separation_roll_state(
        self, roll_busy_until: float
    ) -> RollState | None:
        if self.separation_required:
            return RollState(
                self.roll_name,
                self.roll_name,
                TO_SEPARATE,
                {},
                roll_busy_until=roll_busy_until,
            )
        return None


class RollsAndTimesInfo:
    def __init__(
        self,
        available_lasers: list[int],
        roll_name_to_roll_info: dict[str, RollInfo],
    ):
        self.available_lasers: set[int] = set(available_lasers)
        self.roll_name_to_roll_info: dict[str, RollInfo] = roll_name_to_roll_info


def get_rolls_and_times(
    roll_lookback_window_days: int = 7, include_future_rolls: bool = False
):
    """
    Get the args for the scheduler in advance so we can use them during optimization
    """
    # 1) pull the roll names from airtable
    # 2) for each roll name, get the self and fuse df
    # 3) for each df, get the total cut time per laser (get the max of the window times)
    # 4) if a roll has both a fuse and a self, we add to the separation_required dictionary

    cut_machine_info_loader = CutMachineInfoLoader()
    fuse_machine_info_loader = FuseMachineInfoLoader()
    rolls_loader = RollsLoader(roll_lookback_window_days, include_future_rolls)
    roll_name_to_roll_info: dict[str, RollInfo] = {}
    rolls_to_cut = rolls_loader.get_rolls_to_cut()
    df_columns = [
        "asset_id",
        "piece_id",
        "piece_code",
        "asset_key",
        "window_index",
        "lasers_to_time_to_window_cut",
    ]
    for roll_name in rolls_to_cut:
        modifier_to_separated_info: dict[str, SeparatedRollInfo] = {}
        self_df = get_self_df_with_timing_from_roll(roll_name, cut_machine_info_loader)
        fuse_material_to_df = get_fuse_dfs_with_timing_from_roll(
            roll_name, cut_machine_info_loader
        )
        roll_modifiers = []
        if self_df is not None:
            lasers_to_total_time = get_lasers_to_total_time(self_df)
            roll_modifiers.append("_self")

            modifier_to_separated_info["_self"] = SeparatedRollInfo(
                roll_name, "_self", lasers_to_total_time, self_df[df_columns]
            )
        for fuse_material_code, fuse_df in fuse_material_to_df.items():
            lasers_to_total_time = get_lasers_to_total_time(fuse_df)
            fuse_time = (
                2
                * fuse_df["roll_max_y"].iloc[0]
                / fuse_machine_info_loader.get_fuse_speed(fuse_material_code)
            ) + (2 * fuse_machine_info_loader.get_block_prep_time())

            roll_modifiers.append("_fuse_" + fuse_material_code)

            modifier_to_separated_info[
                "_fuse_" + fuse_material_code
            ] = SeparatedRollInfo(
                roll_name,
                "_fuse_" + fuse_material_code,
                lasers_to_total_time,
                fuse_df[df_columns],
                fusing_time=fuse_time,
            )

        separation_required = (
            self_df is not None and len(fuse_material_to_df) > 0
        ) or len(fuse_material_to_df) > 1
        roll_name_to_roll_info[roll_name] = RollInfo(
            roll_name,
            modifier_to_separated_info,
            separation_required,
            rolls_to_cut[roll_name],
        )

    return RollsAndTimesInfo(
        cut_machine_info_loader.available_lasers,
        roll_name_to_roll_info,
    )


class Schedule:
    def __init__(
        self,
        roll_name_to_roll_info: dict[str, RollInfo],
        overall_start_time: datetime,
    ):
        self.roll_name_to_roll_info: dict[str, RollInfo] = roll_name_to_roll_info
        self.overall_start_time = overall_start_time
        self.separations = []
        self.fusions = []
        self.laser_cuts = {1: [], 2: [], 3: [], 4: []}
        self.overall_schedule = []
        self.finish_time = 0
        self.unschedule_rolls_to_times = {}

    def set_unscheduled_rolls(self, unschedule_rolls_to_times: dict[str, str]):
        """
        Set the unscheduled rolls and the times they will be finished printing for keeping track of stuff
        """
        self.unschedule_rolls_to_times = unschedule_rolls_to_times

    def set_finish_time(self, start_time, duration):
        """
        Update when the schedule's finish time will be
        Should be run after every action is added to the schedule
        """
        if start_time + duration > self.finish_time:
            self.finish_time = start_time + duration

    def add_separation(self, start_time, roll: RollState, duration=SEPARATION_TIME_SEC):
        """
        Adds a roll separation event to the schedule
        """
        self.separations.append((start_time, roll.roll_name, duration))
        self.overall_schedule.append(
            (start_time, roll.roll_index, SEPARATION, duration)
        )
        self.set_finish_time(start_time, duration)

    def add_fusion(self, start_time, rollInfo: RollState):
        """
        Adds a fusion event to the schedule
        """
        duration = rollInfo.fuse_time
        if duration == 0:
            raise ValueError(f"Fuse time is None for roll {rollInfo.roll_index}")
        self.fusions.append((start_time, rollInfo, duration))
        self.overall_schedule.append(
            (start_time, rollInfo.roll_index, FUSING, duration)
        )
        self.set_finish_time(start_time, duration)

    def add_cut(self, start_time, rollInfo: RollState, laser_number):
        """
        Adds a cut for the specified laser number to the schedule
        """
        duration = rollInfo.lasers_to_total_time[laser_number]
        self.laser_cuts[laser_number].append((start_time, rollInfo, duration))
        self.overall_schedule.append(
            (
                start_time,
                rollInfo.roll_index,
                LASER + f" {laser_number}",
                duration,
            )
        )
        self.set_finish_time(start_time, duration)

    def get_finish_time(self):
        """
        Returns the finish time of the schedule for all actions that have currently been added
        """
        return self.finish_time

    def get_schedule_entries(self):
        """Maps the schedule into the format needed for airtable.
        This is a list of dictionaries where each dictionary has the following keys:
        - "Planned Start Time": the start time of the action (in the format "%Y-%m-%d %H:%M:%S")
        - "Planned End Time": the end time of the action (in the format "%Y-%m-%d %H:%M:%S")
        - "Roll Name": the name of the roll
        - "Roll Action Identifier": the roll action identifier (aka roll_index)
        - "Station": the station the action is occurring at
        - "Planned Time (s)": the planned time of the action in seconds
        - "Scheduled At": the current time (when we created this schedule)

        Returns:
            list[dict]: the list of dictionaries of the schedule entries
        """
        result = []
        current_time = airtable_date(datetime.now())
        for start_time, roll_index, action, duration in self.overall_schedule:
            start_time_str = airtable_date(
                self.overall_start_time + timedelta(seconds=start_time)
            )
            end_time_str = airtable_date(
                self.overall_start_time + timedelta(seconds=start_time + duration)
            )
            result.append(
                {
                    "Scheduled Cut Date": self.overall_start_time.strftime("%Y-%m-%d"),
                    "Planned Start Time": start_time_str,
                    "Planned End Time": end_time_str,
                    "Roll Name": roll_index.split("_")[0],
                    "Roll Action Identifier": roll_index,
                    "Station": action,
                    "Planned Time (s)": duration,
                    "Scheduled At": current_time,
                }
            )
        return result

    def get_pieces_df_with_end_times(self):
        """
        Returns the pieces df with the end times added
        """
        # start off with an empty df
        concatenated_df = pd.DataFrame()
        for start_time, roll_index, action, duration in self.overall_schedule:
            if action == SEPARATION or action == FUSING:
                continue
            laser_index = action.split(" ")[-1]
            # get the roll name TODO: we should just have the schedule pass in the rollinfo/separatedRollInfo object or something
            roll_name = get_real_roll_name(roll_index)
            roll_info = self.roll_name_to_roll_info[roll_name]
            # get the text after the roll_name in the roll_index
            roll_modifier = roll_index[len(roll_name) :]
            separated_roll_info = roll_info.modifier_to_separated_info[roll_modifier]
            df = separated_roll_info.piece_df
            df["roll_cut_start_time"] = start_time
            df["roll_cut_end_time"] = start_time + duration
            df["window_cut_end_time"] = df.apply(
                lambda x: x.lasers_to_time_to_window_cut[int(laser_index)] + start_time,
                axis=1,
            )
            concatenated_df = pd.concat([concatenated_df, df])
        return concatenated_df


def airtable_date(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def load_schedule_to_airtable(schedule: Schedule):
    """
    Load the schedule to airtable
    """
    existing_cut_schedule = get_existing_cut_schedule(schedule.overall_start_time)
    if len(existing_cut_schedule) > 0:
        return False
    schedule_entries = schedule.get_schedule_entries()
    SCHEDULE_CUT_SCHEDULE.batch_create(schedule_entries)
    return True


def get_existing_cut_schedule(date: datetime):
    """
    Get the scheduled entries from airtable for the given date.
    """
    formatted_date = date.strftime("%Y-%m-%dT00:00:00.000Z")
    return SCHEDULE_CUT_SCHEDULE.all(
        formula=f"{{Scheduled Cut Date}}='{formatted_date}'"
    )


def delete_existing_cut_schedule(date: datetime):
    """
    Delete the scheduled entries from airtable for the given date.
    """
    existing_entries = get_existing_cut_schedule(date)
    if len(existing_entries) > 0:
        SCHEDULE_CUT_SCHEDULE.batch_delete([entry["id"] for entry in existing_entries])


def delete_existing_piece_timing_from_airtable(date: datetime):
    """
    Delete the scheduled entries from airtable for the given date.
    """
    existing_entries = get_existing_expected_timing_from_airtable(date)
    if len(existing_entries) > 0:
        EXPECTED_TIMING.batch_delete([entry["id"] for entry in existing_entries])


def get_existing_expected_timing_from_airtable(date: datetime):
    """Get the pieces and their expected scheduled cut times from airtable"""
    formatted_date = date.strftime("%Y-%m-%dT00:00:00.000Z")
    return EXPECTED_TIMING.all(formula=f"{{Scheduled Cut Date}}='{formatted_date}'")


def load_piece_timing_to_airtable(schedule: Schedule):
    existing_pieces = get_existing_expected_timing_from_airtable(
        schedule.overall_start_time
    )
    if len(existing_pieces) > 0:
        return False
    df = schedule.get_pieces_df_with_end_times().copy()
    if df.empty:
        return False
    df["scheduled_cut_date"] = schedule.overall_start_time.strftime("%Y-%m-%d")

    column_map = {
        "asset_id": "Asset Id",
        "piece_id": "Piece Id",
        "piece_code": "Piece Code",
        "asset_key": "Asset Key",
        "roll_cut_start_time": "Roll Cut Start Time Scheduled",
        "roll_cut_end_time": "Roll Cut End Time Scheduled",
        "window_cut_end_time": "Piece Cut End Time Scheduled",
        "scheduled_cut_date": "Scheduled Cut Date",
    }
    filtered_df = df.filter(items=list(column_map.keys())).rename(columns=column_map)
    filtered_df["Roll Cut Start Time Scheduled"] = filtered_df[
        "Roll Cut Start Time Scheduled"
    ].apply(lambda x: airtable_date(schedule.overall_start_time + timedelta(seconds=x)))
    filtered_df["Roll Cut End Time Scheduled"] = filtered_df[
        "Roll Cut End Time Scheduled"
    ].apply(lambda x: airtable_date(schedule.overall_start_time + timedelta(seconds=x)))
    filtered_df["Piece Cut End Time Scheduled"] = filtered_df[
        "Piece Cut End Time Scheduled"
    ].apply(lambda x: airtable_date(schedule.overall_start_time + timedelta(seconds=x)))
    filtered_df = filtered_df[filtered_df["Asset Id"].notna()]

    list_of_dicts = filtered_df.to_dict(orient="records")
    EXPECTED_TIMING.batch_create(list_of_dicts)
    return True


class Scheduler:
    def __init__(
        self,
        rolls_and_times_info: RollsAndTimesInfo,
        day_to_schedule: datetime,
        enforce_roll_proximity: bool = False,
    ):
        self.roll_name_to_roll_info: dict[
            str, RollInfo
        ] = rolls_and_times_info.roll_name_to_roll_info
        self.unscheduled_rolls: dict[str, str] = {}
        self.available_lasers: set[int] = rolls_and_times_info.available_lasers
        self.roll_name_to_future_cut_rolls: dict[str, list[RollState]] = {}
        self.enforce_roll_proximity: bool = enforce_roll_proximity
        self.rolls: list[RollState] = []
        self.time: float = 0
        self.sparation_busy_until: float = 0
        self.fusion_machine_busy_until: float = 0
        self.lasers_busy_until: list[float] = [0, 0, 0, 0]
        self.day_to_schedule = day_to_schedule
        self.start_time = self.get_start_time()
        self.schedule = Schedule(
            self.roll_name_to_roll_info,
            self.start_time,
        )

        self.overtime_time_limit = (
            self.get_end_time().timestamp() - self.start_time.timestamp()
        )
        # we have a priority queue if we are enforcing roll proximity
        self.laser_queue: dict[int, list[RollState]] = {
            1: [],
            2: [],
            3: [],
            4: [],
        }

    def get_end_time(self) -> datetime:
        """the typical factory close time. after this point it is overtime

        Returns:
            datetime: the time the factory closes
        """
        return self.day_to_schedule.replace(
            hour=OVERTIME_START, minute=0, second=0, microsecond=0
        )

    def get_start_time(self) -> datetime:
        return self.day_to_schedule.replace(
            hour=DAILY_START_TIME_HOURS,
            minute=DAILY_START_TIME_MINUTES,
            second=0,
            microsecond=0,
        )

    def _add_post_separation_rolls(self, roll_name: str, roll_busy_until: float):
        """
        Prepare a roll for either cut or fuse.
        This is to be called after separation if separation is necessary (has more than 1 fuse or has self and fuse)

        Args:
            roll_name (str): the name of the roll that is ready for cut or fuse
            roll_busy_until (float, optional): The amount of time the roll is busy for (if separation is occurring it wil be the time of separation).
                                             Defaults to 0 which is if the roll is free to be cut or fused immediately without separation.
        """
        cut_rolls = []

        for separated_roll_info in self.roll_name_to_roll_info[
            roll_name
        ].modifier_to_separated_info.values():
            roll_info = separated_roll_info.generate_post_separation_initial_roll_state(
                roll_busy_until
            )

            self._add_roll(roll_info)
            cut_rolls.append(roll_info)
        self.roll_name_to_future_cut_rolls[roll_name] = cut_rolls

    def get_roll_initial_busy_until(self, roll_info: RollInfo):
        """
        Return the busy until time for the roll based on the roll name compared to the initial schedule cut start time.
        If it is not busy, return 0.
        """
        # roll will be busy until print completion time - end time of cut + ~1hour for final inspection and error margin
        print_completion_time = roll_info.estimated_print_time
        if print_completion_time is None:
            return 0
        busy_until = (
            print_completion_time.timestamp() - self.get_end_time().timestamp() + 3600
        )
        return max(busy_until, 0)

    def add_rolls(self, roll_names):
        """Add the rolls to the scheduler

        Args:
            roll_names (list): list of roll names to be scheduled. This should match the keys in the fields in rolls_and_times_info.
            The order of this input list will determine the schedule
        """
        for roll_name in roll_names:
            roll_info = self.roll_name_to_roll_info[roll_name]
            busy_until = self.get_roll_initial_busy_until(roll_info)
            # any roll that isn't ready to be cut by typical factory close time will be unscheduled for that day
            if busy_until > self.overtime_time_limit:
                if roll_info.estimated_print_time is None:
                    self.unscheduled_rolls[roll_name] = "Already printed. weird state"
                else:
                    self.unscheduled_rolls[
                        roll_name
                    ] = roll_info.estimated_print_time.strftime("%Y-%m-%d %H:%M:%S")
                self.schedule.set_unscheduled_rolls(self.unscheduled_rolls)
                continue

            separation_roll_state = roll_info.generate_separation_roll_state(
                self.get_roll_initial_busy_until(roll_info)
            )
            if separation_roll_state:
                self._add_roll(separation_roll_state)
            else:
                self._add_post_separation_rolls(
                    roll_name,
                    busy_until,
                )

    def _add_roll(self, roll: RollState):
        """Add a roll to the scheduler"""
        self.rolls.append(roll)

    def process(self) -> Schedule:
        """Process the rolls and return the schedule

        Returns:
            Schedule: The schedule object that dictates when each roll will be cut, fused, and separated
        """

        # while we are processing, we are adding a lot of rolls with various states.
        while not self._all_rolls_cut():
            self._process_separation()
            self._process_fusion()
            self.process_cut()
            # ideally we'd increment by 1 second but that's slow af. 30 sec error margin is fine.
            # we can probably make it as big as the longest action time (maybe 60s for the buffer time?)
            self.time += 60  # 60 seconds
        return self.schedule

    def _all_rolls_cut(self):
        """
        Returns True if all rolls are in the DONE state, False otherwise
        """
        # we are attempting to remove DONE rolls for efficiency but in case we missed it, we will check for it here
        return len(self.rolls) == 0 or all(
            roll.roll_state == DONE for roll in self.rolls
        )

    def _process_separation(self):
        """
        Check if the separation station is busy.
        If it is not busy, schedule a separation for the next available roll that needs it
        """
        if self.sparation_busy_until <= self.time:
            for roll in self.rolls.copy():
                if roll.roll_state == TO_SEPARATE and roll.is_roll_free(self.time):
                    self.schedule.add_separation(self.time, roll)
                    self.sparation_busy_until = (
                        self.time + SEPARATION_TIME_SEC + BUFFER_TIME_SEC
                    )
                    roll.roll_state = DONE
                    # now we have at least two rolls that we need to set appropriately
                    self._add_post_separation_rolls(
                        roll.roll_name,
                        self.time + SEPARATION_TIME_SEC + BUFFER_TIME_SEC,
                    )
                    # we are done processing this roll, we can remove it
                    self.rolls.remove(roll)
                    break

    def _process_fusion(self):
        """
        Check if the fusing machine is busy.
        If it is not busy, schedule a fusion for the next available roll that needs it.
        Then set the roll to be busy until the fusion is done and change the roll state to TO_CUT
        """
        if self.fusion_machine_busy_until <= self.time:
            for roll in self.rolls:
                if roll.roll_state == TO_FUSE and roll.is_roll_free(self.time):
                    self.schedule.add_fusion(self.time, roll)
                    self.fusion_machine_busy_until = (
                        self.time + roll.get_fuse_time() + BUFFER_TIME_SEC
                    )
                    roll.roll_busy_until = (
                        self.time + roll.get_fuse_time() + BUFFER_TIME_SEC
                    )
                    # can only get_fuse_time before changing the state to cut or we get an error
                    roll.roll_state = TO_CUT
                    break

    def laser_queue_is_empty(self, laser_number_index):
        for roll in self.laser_queue[laser_number_index + 1]:
            if roll.is_queue_valid():
                return False
        return True

    # to enforce cuts next to each other we have to keep track of a priority queue rolls
    # 1) if a roll is cut, we check for the associated fuse/self roll and add it to that laser's queue
    #     a) that roll is now also no longer allowed to be cut on another roll so shall be marked as such
    # 2) when a laser is free, we check it's specific queue for the next roll to cut first if it has availability
    def process_cut(self):
        """
        First checks to see if there is something in the queue for each laser if we are enforcing roll proximity.
        If there is, we attempt to cut that roll.
        Next, we loop through every laser again and if we are not enforcing roll proximity or the queue is empty, we attempt to cut
        the first available roll.
        """
        # we have to check the queue first if we are enforcing roll proximity
        if self.enforce_roll_proximity:
            for laser_number_index in range(4):
                if laser_number_index + 1 not in self.available_lasers:
                    # don't use any lasers that aren't available
                    continue
                for roll in self.laser_queue[laser_number_index + 1].copy():
                    did_cut = self._attempt_cut(roll, laser_number_index)
                    if did_cut:
                        self.laser_queue[laser_number_index + 1].remove(roll)
                        # we don't need to check the other rolls if we cut
                        break
        for roll in self.rolls.copy():
            for laser_number_index in range(4):
                if laser_number_index + 1 not in self.available_lasers:
                    # don't use any lasers that aren't available
                    continue
                # only do a cut if we are not enforcing roll proximity or the queue is empty
                if not self.enforce_roll_proximity or self.laser_queue_is_empty(
                    laser_number_index
                ):
                    did_cut = self._attempt_cut(roll, laser_number_index)
                    if did_cut:
                        self.rolls.remove(roll)
                        # we don't need to check the other lasers if we cut
                        break

    def _attempt_cut(self, roll: RollState, laser_number_index: int):
        """
        Processes the actual cut for the roll. Returns True if the cut was successful, False otherwise.
        Sets roll state to done if the cut was successful.
        Will not cut a roll if it is not free or if it is required to be cut on a different laser.
        """
        # this is fucking stupid. i wonder if i should change it all to be 0 indexed and make a mapping for the name
        laser_number = laser_number_index + 1
        if roll.roll_state == TO_CUT and roll.is_roll_free(self.time):
            # if enforcing roll proximity we will not cut a roll on a different laser
            if (
                self.enforce_roll_proximity
                and roll.required_laser is not None
                and roll.required_laser != laser_number
            ):
                return False
            if self.lasers_busy_until[laser_number_index] <= self.time:
                self.schedule.add_cut(self.time, roll, laser_number)
                self.lasers_busy_until[laser_number_index] = (
                    self.time
                    + roll.lasers_to_total_time[laser_number]
                    + BUFFER_TIME_SEC
                )
                roll.roll_state = DONE
                roll.roll_busy_until = (
                    self.time
                    + roll.lasers_to_total_time[laser_number]
                    + BUFFER_TIME_SEC
                )
                # some logic for enforcing roll proximity
                associated_rolls = self.roll_name_to_future_cut_rolls.get(
                    roll.roll_name, []
                )
                for associated_roll in associated_rolls:
                    associated_roll.set_required_laser(laser_number)
                    self.laser_queue[laser_number].append(associated_roll)
                return True


class ScheduleOptimizer:
    def __init__(
        self,
        rolls_and_times_info: RollsAndTimesInfo,
        day_to_schedule: datetime,
        enforce_roll_proximity=False,
        debug_statements=False,
    ):
        self.rolls_and_times_info = rolls_and_times_info
        self.day_to_schedule = day_to_schedule
        self.default_roll_names = self.get_default_roll_name_list(
            rolls_and_times_info.roll_name_to_roll_info
        )
        self.enforce_roll_proximity = enforce_roll_proximity
        self.debug_statements = debug_statements

    def get_default_roll_name_list(self, roll_name_to_roll_info: dict[str, RollInfo]):
        """
        Get the default roll name list for simulated annealing.
        We sort it by what would take the most time first because we assume
        that a greedy algorithm scheduling the longest first would be pretty good
        """
        result = []
        for roll_name, roll_info in roll_name_to_roll_info.items():
            total_self_time = 0
            total_fuse_time = 0
            for separated_roll_info in roll_info.modifier_to_separated_info.values():
                total_self_time += max(separated_roll_info.lasers_to_time.values())
                total_fuse_time += separated_roll_info.fusing_time or 0
            result.append((roll_name, total_self_time + total_fuse_time))
        result.sort(key=lambda x: x[1], reverse=True)
        return [x[0] for x in result]

    def cost(self, rolls):
        """
        Get the cost of a list of rolls
        """
        scheduler = Scheduler(
            self.rolls_and_times_info, self.day_to_schedule, self.enforce_roll_proximity
        )
        scheduler.add_rolls(rolls)
        schedule = scheduler.process()
        return schedule.get_finish_time()

    def neighbor(self, roll_name_list):
        """
        Get the neighbor of a roll_name_list
        """
        roll_name_list_copy = roll_name_list[:]
        a, b = random.sample(range(len(roll_name_list_copy)), 2)
        roll_name_list_copy[a], roll_name_list_copy[b] = (
            roll_name_list_copy[b],
            roll_name_list_copy[a],
        )
        return roll_name_list_copy

    def acceptance_probability(self, old_cost, new_cost, temperature):
        # If the new cost is lower, always accept
        if new_cost < old_cost:
            return 1.0
        # If the new cost is higher, accept with a probability that decreases with temperature
        return math.exp((old_cost - new_cost) / temperature)

    def simulated_annealing(self, initial_temperature, cooling_rate):
        current_rolls = self.default_roll_names[:]
        if len(current_rolls) == 0:
            return []
        current_cost = self.cost(current_rolls)
        temperature = initial_temperature
        num_iterations = 0
        num_acceptances = 0
        while temperature > 1e-3:
            new_rolls = self.neighbor(current_rolls)
            new_cost = self.cost(new_rolls)
            if (
                self.acceptance_probability(current_cost, new_cost, temperature)
                > random.random()
            ):
                current_rolls, current_cost = new_rolls, new_cost
                num_acceptances += 1
            temperature *= cooling_rate
            num_iterations += 1

        if self.debug_statements:
            res.utils.logger.info(
                f"Simulated annealing took {num_iterations} iterations with {num_acceptances} acceptances. Final cost: {current_cost}"
            )
        return current_rolls


def get_real_roll_name(schedule_roll_name):
    """
    Get the roll name from the schedule roll name
    """
    if "_fuse" in schedule_roll_name:
        return schedule_roll_name.split("_fuse")[0]
    elif "_self" in schedule_roll_name:
        return schedule_roll_name.split("_self")[0]
    return schedule_roll_name


def generate_large_palette(size):
    colormaps = ["tab20", "tab20b", "tab20c"]  # List of colormaps to cycle through
    colors = []
    for i in range(size):
        cmap = plt.get_cmap(colormaps[i % len(colormaps)])
        colors.append(cmap(i % 20))  # Assumes each cmap can provide up to 20 colors
    return colors


def plot_schedule(schedule: Schedule):
    """
    Plot the schedule. The arg is the overall schedule from the scheduler
    """
    # let's make a copy of the schedule so we don't screw the original
    overall_schedule = schedule.overall_schedule.copy()
    # Convert start times and durations to datetime objects for plotting
    for i, schedule_tuple in enumerate(overall_schedule):
        start_seconds, roll_name, action, duration_seconds = schedule_tuple
        start_time = schedule.overall_start_time + timedelta(seconds=start_seconds)
        duration = timedelta(seconds=duration_seconds)
        overall_schedule[i] = (start_time, roll_name, action, duration)

    # Assign a unique color to each roll_name
    roll_names = list(set([get_real_roll_name(item[1]) for item in overall_schedule]))
    res.utils.logger.info(f"length of roll_names: {len(roll_names)}")
    colors = generate_large_palette(len(roll_names))
    color_dict = dict(zip(roll_names, colors))

    # Prepare figure for plotting
    fig, ax = plt.subplots(figsize=(15, 8))
    actions = list(set([item[2] for item in overall_schedule]))
    actions.sort()  # Ensure actions are in a consistent order

    # Plot each item
    for start_time, roll_name, action, duration in overall_schedule:
        action_idx = actions.index(action)
        color = color_dict[get_real_roll_name(roll_name)]
        start_datetime = mdates.date2num(start_time)
        end_datetime = mdates.date2num(start_time + duration)
        ax.barh(
            action_idx,
            end_datetime - start_datetime,
            left=start_datetime,
            color=color,
            edgecolor="black",
            label=get_real_roll_name(roll_name),
        )

    # Customize the y-axis
    ax.set_yticks(range(len(actions)))
    ax.set_yticklabels(actions)

    max_finish_time = (
        max([item[0] + item[3] for item in overall_schedule])
        if overall_schedule
        else schedule.overall_start_time
    )
    # Set the x-axis limits to the start and finish times of the schedule (plus a little extra for padding)
    ax.set_xlim(
        [
            mdates.date2num(schedule.overall_start_time),
            mdates.date2num(max_finish_time + timedelta(minutes=1)),
        ]
    )

    # Format the x-axis to display time
    ax.xaxis_date()
    fig.autofmt_xdate()
    ax.xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 500, 15)))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    # Add legend (remove duplicates)
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(
        by_label.values(), by_label.keys(), bbox_to_anchor=(1.05, 1), loc="upper left"
    )

    plt.title("Cut Schedule Visualization")
    plt.xlabel("Time from Schedule Start")
    plt.ylabel("Station")

    plt.tight_layout()
    plt.show()
    return fig


class ScheduleManager:
    def __init__(
        self,
        day_to_schedule: datetime,
        lookback_window: int = 7,
        include_future_rolls: bool = False,
        enforce_roll_proximity=True,
        debug_statements=False,
        person_to_ping=None,
    ):
        # include rolls that aren't yet ready to be cut by the time the schedule runs but should be later in the day
        self.include_future_rolls = include_future_rolls
        self.day_to_schedule = day_to_schedule
        self.lookback_window = lookback_window
        self.enforce_roll_proximity = enforce_roll_proximity
        self.debug_statements = debug_statements
        self.person_to_ping = person_to_ping

        self.rolls_and_times: RollsAndTimesInfo = get_rolls_and_times(
            self.lookback_window, self.include_future_rolls
        )
        self.best_schedule = self._generate_schedule()

    def _generate_schedule(self):
        optimizer = ScheduleOptimizer(
            self.rolls_and_times,
            self.day_to_schedule,
            enforce_roll_proximity=self.enforce_roll_proximity,
            debug_statements=self.debug_statements,
        )
        best_roll_input = optimizer.simulated_annealing(2400, 0.999)
        scheduler = Scheduler(
            self.rolls_and_times, self.day_to_schedule, enforce_roll_proximity=True
        )
        scheduler.add_rolls(best_roll_input)
        schedule = scheduler.process()
        return schedule

    def post_to_slack(self, channel: str):
        fig = plot_schedule(self.best_schedule)
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        data = buf.getvalue()
        buf.close()
        slack = res.connectors.load("slack")
        slack.post_file(
            data,
            "schedule.png",
            channel,
            initial_comment=f"Cut schedule for {self.best_schedule.overall_start_time.strftime('%Y-%m-%d')}",
        )
        if self.person_to_ping and len(self.best_schedule.overall_schedule) == 0:
            slack.post_message(
                f"{self.person_to_ping} - No rolls were scheduled for {self.best_schedule.overall_start_time.strftime('%Y-%m-%d')}",
                channel,
            )

    def load_to_airtable(self) -> bool:
        return load_schedule_to_airtable(self.best_schedule)

    def load_piece_timing_to_airtable(self) -> bool:
        return load_piece_timing_to_airtable(self.best_schedule)
