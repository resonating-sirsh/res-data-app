import res
import numpy as np
import pandas as pd
import datetime
import json
from datetime import timedelta
from tabulate import tabulate


def get_healing_pieces():
    """
    Attempt to track the observation of unique physical pieces exiting the inspection
    there are two roll inspections for each piece and the resolution can be some combination of pass and fail at each
    the raw data here could be used to show if the piece still needs to be healed or not but for now just the raw data are returned

    curious about some cases where healing pieces get cancelled or otherwise forgotten and never "exit" / complete the loop. How do we know we are no longer pending the piece
    """
    HEALED_PIECE_TRACKING = """
    select a.*, 
    all_piece.code, 
    b.node_id, 
    b.set_key, 
    b.observed_at, 
    b.make_instance, 
    n.name as node_name, 
    ri.fail, 
    ri.created_at as inspected_at, 
    ri.defect_name,
    b.status, 
    b.roll_key
    from make.one_piece_healing_requests a
    left join make.roll_inspection ri on ri.piece_id = a.print_asset_piece_id
    left join make.one_pieces all_piece on a.piece_id = all_piece.oid
    left join make.one_pieces b on a.piece_id = b.oid and b.status='Exit' and b.node_id='8be3ef2f-eb9b-381e-a0cb-01fbcc6348f0' and b.observed_at > a.created_at
    left join flow.nodes n on n.id = b.node_id  
    
    """

    postgres = res.connectors.load("postgres")
    data = postgres.run_query(HEALED_PIECE_TRACKING)
    data["one_number"] = data["metadata"].map(lambda x: x.get("one_number"))
    data = data.sort_values("created_at")

    return data


def get_healing_history_for_one(one_number=None, piece_ids=None, postgres=None):
    """
    Attempt to track the observation of unique physical pieces exiting the inspection
    there are two roll inspections for each piece and the resolution can be some combination of pass and fail at each
    the raw data here could be used to show if the piece still needs to be healed or not but for now just the raw data are returned

    curious about some cases where healing pieces get cancelled or otherwise forgotten and never "exit" / complete the loop. How do we know we are no longer pending the piece
    """
    HEALED_PIECE_TRACKING = """
    select a.*, 
    ri.fail, 
    ri.created_at as inspected_at,
    ri.defect_name
    from make.one_piece_healing_requests a
    left join make.roll_inspection ri on ri.piece_id = a.print_asset_piece_id
    
    where metadata->>'one_number' = %s
   
    """

    postgres = postgres or res.connectors.load("postgres")
    data = postgres.run_query(HEALED_PIECE_TRACKING, (str(one_number),))
    data["one_number"] = data["metadata"].map(lambda x: x.get("one_number"))
    data = data.sort_values("created_at")
    if len(data):
        data["piece_code"] = data["metadata"].map(lambda x: x["piece_code"])

    return data


def get_healing_history_for_one_by_piece(one_number, postgres=None):
    """
    examples; 10333357
    """
    H = get_healing_history_for_one(str(one_number), postgres=postgres)
    if not len(H):
        return {}
    H = H.sort_values("piece_code")
    H = (
        H.groupby(["piece_code", "print_asset_piece_id"])
        .agg({"defect_name": set, "fail": set, "inspected_at": max, "created_at": max})
        .reset_index()
    )
    H = H.rename(columns={"created_at": "healing_print_asset_at"})
    H = res.utils.dataframes.replace_nan_with_none(H)

    def with_pruning_sets(df):
        for k in ["defect_name", "fail"]:
            df[k] = df[k].map(lambda x: x - {None}).map(lambda x: x if len(x) else None)
        for k in ["inspected_at", "healing_print_asset_at"]:
            df[k] = df[k].map(str)
        # show the most recent healing at the top
        return df.drop("piece_code", 1).sort_values(
            "healing_print_asset_at", ascending=False
        )

    return {
        key: with_pruning_sets(data).to_dict("records")
        for key, data in H.groupby("piece_code")
    }


def get_healing_pieces_per_one_from_data(data):
    """
    compare the set of pieces healed with the ones that have reached roll inspection
    a smarter thing could check if they have succeeded at inspection but because of the possible contention
    we dont bother to do this yet since we need a clear view if the piece will progress
    """

    if data is None:
        data = get_healing_pieces()

    healed_pieces = (
        data[data["code"].notnull()]
        .groupby(["one_number"])
        .agg({"code": set, "created_at": np.nanmin})
    )
    exited = data[data["observed_at"].notnull()]
    exited = exited.groupby(["one_number"]).agg({"code": set, "observed_at": np.nanmax})
    healed_pieces = healed_pieces.join(exited, rsuffix="_exited")
    healed_pieces["code_exited"] = healed_pieces["code_exited"].fillna(
        healed_pieces["code_exited"].map(lambda x: set([]))
    )
    healed_pieces["total"] = healed_pieces["code"].map(len)
    healed_pieces["total_exited"] = healed_pieces["code_exited"].map(len)
    healed_pieces["number_pieces_pending"] = (
        healed_pieces["total"] - healed_pieces["total_exited"]
    )
    healed_pieces["pending_healing_pieces"] = healed_pieces.apply(
        lambda row: row["code"] - row["code_exited"], axis=1
    )
    healed_pieces = healed_pieces.reset_index().sort_values("created_at")
    healed_pieces["one_number"] = healed_pieces["one_number"].map(int)
    healed_pieces["code"] = healed_pieces["code"].map(list)
    healed_pieces["code_exited"] = healed_pieces["code_exited"].map(list)
    healed_pieces["pending_healing_pieces"] = healed_pieces[
        "pending_healing_pieces"
    ].map(list)

    return healed_pieces


class HealingAlertDataProcessor:
    def __init__(self):
        self.airtable = res.connectors.load("airtable")
        self.assets = self._get_assets()
        self.mones = self._get_ones()
        self.ppu_schedule = self._get_ppu_schedule()
        self.rolls = self._get_rolls()
        self.pa_table_joined = self._get_joined_pa_table()
        self.pa_table_non_exited = self._get_non_exited_pa_table(self.pa_table_joined)
        self.lagging_print_assets = self.pa_table_non_exited[
            (
                self.pa_table_non_exited["first_asset_exit_lag_time_hours"].isnull()
                == False
            )
            & (self.pa_table_non_exited["first_asset_exit_lag_time_hours"] > 24)
        ]
        self.healing_assets = self.pa_table_non_exited[
            self.pa_table_non_exited["rank"] == "Healing"
        ]
        self.lagging_combo_assets = self.pa_table_non_exited[
            (
                self.pa_table_non_exited["first_asset_exit_lag_time_hours"].isnull()
                == False
            )
            & (self.pa_table_non_exited["first_asset_exit_lag_time_hours"] > 24)
            & (self.pa_table_non_exited["rank"] != "Healing")
        ]
        self.old_assets = self.pa_table_non_exited[
            self.pa_table_non_exited["order_lag_time_days"] >= 14
        ]

    def _get_joined_pa_table(self):
        # first need to join the order information into the asset
        assets_with_mones = self._get_pa_join_mo(self.assets, self.mones)
        assets_with_schedules = self._get_pa_join_ppu(assets_with_mones)
        with_earliest_exit_timestamps = self._add_earliest_exit_timestamp_from_assets(
            assets_with_schedules
        )
        with_num_assets = self._add_num_assets_from_assets(
            with_earliest_exit_timestamps
        )
        # add roll count info
        roll_counts = self.rolls["material_code"].value_counts().reset_index()
        roll_counts.columns = ["material_code", "material_roll_count"]
        roll_counts["material_code"] = roll_counts["material_code"].apply(
            lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
        )
        with_roll_counts = pd.merge(
            with_num_assets, roll_counts, on="material_code", how="left"
        )
        with_roll_counts["material_roll_available"] = with_roll_counts[
            "material_roll_count"
        ].apply(lambda x: True if pd.notna(x) and x > 0 else False)
        # add lag times
        self._get_update_lag_time(with_roll_counts)
        self._get_exit_lag_time(with_roll_counts)
        return with_roll_counts

    def _get_non_exited_pa_table(self, pa_table_joined):
        # we are defining a print asset as not exited if it is not printed, canceled or terminated
        non_exited_assets = pa_table_joined[
            (pa_table_joined["status"] != "PRINTED")
            & (pa_table_joined["status"] != "CANCELED")
            & (pa_table_joined["status"] != "TERMINATED")
            & (pa_table_joined["print_exit"] != "DONE")
            & (pa_table_joined["make_node"] != "Done")
            & (pa_table_joined["make_node"] != "Cancelled")
            & (pa_table_joined["one_number"].isnull() == False)
        ]
        non_exited_assets.loc[
            non_exited_assets["prepared_piece_count"] == 0, "ppu_scheduled_date"
        ] = "ppc=0, broken asset"
        return non_exited_assets

    def _get_pa_join_ppu(self, pa_table):
        pa_table = pd.merge(pa_table, self.ppu_schedule, on="roll_names", how="left")
        return pa_table

    def _get_pa_join_mo(self, pa_table, mo_table):
        # join pa_table and mo_table on one_number
        pa_table = pd.merge(pa_table, mo_table, on="one_number", how="left")
        # use customer_ordered_at to calculate lag time in hours
        now = datetime.datetime.now(datetime.timezone.utc)  # Get current time in UTC
        now = now.replace(tzinfo=None)  # Convert to tz-naive datetime
        pa_table["order_lag_time_hours"] = (now - pd.to_datetime(pa_table["customer_ordered_at"]).dt.tz_convert(None)).dt.total_seconds() * 1.0 / 60 / 60  # type: ignore
        pa_table["order_lag_time_days"] = (now - pd.to_datetime(pa_table["customer_ordered_at"]).dt.tz_convert(None)).dt.total_seconds() * 1.0 / 60 / 60 / 24  # type: ignore
        pa_table["customer_ordered_at_NYC"] = pd.to_datetime(
            pa_table["customer_ordered_at"]
        ).dt.tz_convert("America/New_York")
        pa_table["order_date"] = pa_table["customer_ordered_at_NYC"].dt.date
        return pa_table

    def _add_num_assets_from_assets(self, pa_table):
        # get the number of assets in each ONE. Don't include canceled or terminatd assets
        num_assets = (
            pa_table[
                (pa_table["status"] != "CANCELED")
                & (pa_table["status"] != "TERMINATED")
            ]
            .groupby("one_number")
            .agg(
                {
                    "asset_name": "nunique",
                    "material_code": "nunique",
                }
            )
            .reset_index()
        )
        num_assets = num_assets.rename(
            columns={"asset_name": "body_assets", "material_code": "body_materials"}
        )
        # join the num_assets to the pa_table
        pa_table = pd.merge(pa_table, num_assets, on="one_number", how="left")
        return pa_table

    def _add_earliest_exit_timestamp_from_assets(self, pa_table):
        # lag time is the time between the first part of the ONE exiting printing and now
        # get the first part of the ONE exiting printing
        pa_exited = pa_table[
            (pa_table["status"] == "PRINTED")
            & (pa_table["exit_timestamp"].isnull() == False)
        ]

        earliest_exit_timestamps = (
            pa_exited.groupby("one_number")["exit_timestamp"].min().reset_index()
        )
        earliest_exit_timestamps = earliest_exit_timestamps.rename(
            columns={"exit_timestamp": "earliest_exit_timestamp"}
        )[["one_number", "earliest_exit_timestamp"]]
        # join the earliest_exit_timestamps to the pa_table
        pa_table = pd.merge(
            pa_table, earliest_exit_timestamps, on="one_number", how="left"
        )
        return pa_table

    def _get_update_lag_time(self, pa_table):
        now = datetime.datetime.now(datetime.timezone.utc)  # Get current time in UTC
        now = now.replace(tzinfo=None)  # Convert to tz-naive datetime
        pa_table["last_updated_at_datetime"] = pd.to_datetime(
            pa_table["last_updated_at"]
        ).dt.tz_convert(
            None
        )  # Convert to tz-naive datetime
        pa_table["update_lag_time_hours"] = (
            (now - pa_table["last_updated_at_datetime"]).dt.total_seconds()
            * 1.0
            / 60
            / 60
        )
        pa_table["update_lag_time_days"] = (
            (now - pa_table["last_updated_at_datetime"]).dt.total_seconds()
            * 1.0
            / 60
            / 60
            / 24
        )
        pa_table["last_updated_at_NYC"] = pd.to_datetime(
            pa_table["last_updated_at"]
        ).dt.tz_convert("America/New_York")
        last_update_days, last_update_hours = divmod(
            pa_table["update_lag_time_hours"], 24
        )
        pa_table["update_lag"] = (
            last_update_days.astype(int).astype(str)
            + "d "
            + last_update_hours.round(2).astype(str)
            + "h"
        )
        pa_table["update_lag"] = pa_table["update_lag"].apply(
            lambda x: x.split(" ")[1] if x.split(" ")[0] == "0d" else x
        )
        yesterday_noon = (datetime.datetime.now() - timedelta(days=1)).replace(
            hour=12, minute=0, second=0, microsecond=0
        )
        pa_table["updated_after_last_optimus_run"] = (
            pa_table["last_updated_at_datetime"] > yesterday_noon
        )

    def _get_exit_lag_time(self, pa_table):
        now = datetime.datetime.now(datetime.timezone.utc)  # Get current time in UTC
        now = now.replace(tzinfo=None)  # Convert to tz-naive datetime
        pa_table["first_asset_exit_at_datetime"] = pd.to_datetime(
            pa_table["earliest_exit_timestamp"]
        ).dt.tz_convert(
            None
        )  # Convert to tz-naive datetime
        pa_table["first_asset_exit_lag_time_hours"] = (
            (now - pa_table["first_asset_exit_at_datetime"]).dt.total_seconds()
            * 1.0
            / 60
            / 60
        )
        pa_table["first_asset_exit_lag_time_days"] = (
            (now - pa_table["first_asset_exit_at_datetime"]).dt.total_seconds()
            * 1.0
            / 60
            / 60
            / 24
        )
        pa_table["first_asset_exit_at_NYC"] = pd.to_datetime(
            pa_table["earliest_exit_timestamp"]
        ).dt.tz_convert("America/New_York")
        pa_table["first_asset_exit_date"] = pa_table["first_asset_exit_at_NYC"].dt.date
        exit_days, exit_hours = divmod(pa_table["first_asset_exit_lag_time_hours"], 24)
        pa_table["first_asset_exit_lag"] = (
            exit_days.fillna(0).astype(int).astype(str)
            + "d "
            + exit_hours.fillna(0).round(2).astype(str)
            + "h"
        )
        pa_table["first_asset_exit_lag"] = pa_table["first_asset_exit_lag"].apply(
            lambda x: x.split(" ")[1] if x.split(" ")[0] == "0d" else x
        )

    @staticmethod
    def get_make_sequence_number(json_str):
        if isinstance(json_str, float):
            return None

        # Parse the JSON string
        data = json.loads(json_str)

        # Return the value of "make_sequence_number", or None if it doesn't exist
        return (
            data[0].get("make_sequence_number")
            if data and isinstance(data, list) and isinstance(data[0], dict)
            else None
        )

    def _get_ppu_schedule(self):
        fields = {
            "Name": "ppu",
            "Scheduled Print Date": "ppu_scheduled_date",
            "Status": "ppu_schedule_status",
            "Total Length": "ppu_total_length",
            "Roll Names": "roll_names",
        }
        ppu_creation = self.airtable["appHqaUQOkRD8VbzX"]["tblsQ6YiweQ5oPiak"]
        ppu_creation = ppu_creation.to_dataframe(fields=list(fields.keys())).rename(
            columns=fields
        )
        # Split the 'roll_names' column on commas
        ppu_creation["roll_names"] = ppu_creation["roll_names"].str.split(",")
        # Explode the 'roll_names' column
        roll_names_scheduled = ppu_creation.explode("roll_names")
        return roll_names_scheduled

    def _get_ones(self):
        # make one prod
        # https://airtable.com/appH5S4hIuz99tAjm/tblptyuWfGEUJWwKk/
        fields = {
            "Order Number v3": "one_number",
            "Current Assembly Node": "assembly_node",
            "Current Make Node": "make_node",
            "_original_request_placed_at": "customer_ordered_at",
            "Body Pieces": "body_pieces",
        }
        mone = self.airtable["appH5S4hIuz99tAjm"]["tblptyuWfGEUJWwKk"]
        mone = mone.to_dataframe(fields=list(fields.keys())).rename(columns=fields)
        return mone

    def _get_rolls(self):
        # res.Magic.Print Rolls
        # https://airtable.com/apprcULXTWu33KFsh/tblSYU8a71UgbQve4
        fields = {
            "Name": "roll_name",
            "Material Code": "material_code",
            "Latest Measurement": "latest_measurement",
            "Allocated for PPU": "allocated_for_ppu",
            "Print ONE Ready": "print_one_ready",
            "Current Substation": "current_substation",
            "📏Roll Length Data": "roll_length_data",
            "Print Assets (flow) 3": "print_assets_flow_3",
            "Flag for Review": "flag_for_review",
        }
        roll_table = self.airtable["apprcULXTWu33KFsh"]["tblSYU8a71UgbQve4"]
        roll_table = roll_table.to_dataframe(fields=list(fields.keys())).rename(
            columns=fields
        )
        # Copying logic from construct_rolls.py... oof
        roll_table = roll_table[roll_table["allocated_for_ppu"].isna()]
        roll_table = roll_table[roll_table["print_one_ready"] == 0]
        roll_table = roll_table[roll_table["current_substation"] != "Done"]
        roll_table = roll_table[roll_table["current_substation"] != "Write Off"]
        roll_table = roll_table[roll_table["current_substation"] != "Expired"]
        roll_table = roll_table[roll_table["current_substation"] != "Print"]
        roll_table = roll_table[roll_table["current_substation"] != "Steam"]
        roll_table = roll_table[roll_table["current_substation"] != "Wash"]
        roll_table = roll_table[roll_table["current_substation"] != "Dry"]
        roll_table = roll_table[roll_table["current_substation"] != "Inspection"]
        roll_table = roll_table[roll_table["current_substation"] != "Soft"]
        roll_table = roll_table[roll_table["current_substation"] != "Scour: Wash"]
        roll_table = roll_table[roll_table["current_substation"] != "Scour: Dry"]
        roll_table = roll_table[roll_table["flag_for_review"].isna()]
        roll_table = roll_table[roll_table["roll_length_data"].isna() == False]
        roll_table = roll_table[roll_table["print_assets_flow_3"].isna()]
        roll_table["latest_measurement"] = pd.to_numeric(
            roll_table["latest_measurement"], errors="coerce"
        )
        roll_table = roll_table[roll_table["latest_measurement"] >= 10]
        return roll_table

    def _get_assets(self):
        # print assets
        fields = {
            # "key" is the print asset name. This is what we use as the identifier
            "Key": "key",
            # some fields that are useful about the order/asset
            "Rank": "rank",
            "Print Queue": "status",
            "Prepared Pieces Count": "prepared_piece_count",
            "Assigned Rolls": "roll_assignment",
            "_print_exit_at": "exit_timestamp",
            "Last Updated At": "last_updated_at",
            "Asset Name": "asset_name",
            "Material Code": "material_code",
            "Number of Pieces": "asset_pieces",
            "Last Optimus Status": "optimus_status",
            "PRINT_EXIT": "print_exit",
            "Prep Pieces Spec Json": "prep_pieces_spec_json",
            "SKU": "sku",
            # needed to join to the order
            "__order_number": "one_number",
        }

        pa_table = self.airtable["apprcULXTWu33KFsh"]["tblwDQtDckvHKXO4w"]
        pa_table = pa_table.to_dataframe(fields=list(fields.keys())).rename(
            columns=fields
        )
        # hacky way to get the roll assignment since it is a list and it is always either [], [foo] or [None, foo]
        pa_table["roll_names"] = pa_table["roll_assignment"].apply(
            lambda x: x[len(x) - 1] if len(x) > 0 else None
        )
        # Apply the function to the 'prep_pieces_spec_json' column
        pa_table["make_sequence_number"] = pa_table["prep_pieces_spec_json"].apply(
            HealingAlertDataProcessor.get_make_sequence_number
        )
        return pa_table


class HealingAlertSlackPoster:
    def __init__(self, data_processor: HealingAlertDataProcessor, slack_channel: str):
        self.slack_channel = slack_channel
        self.data_processor = data_processor
        self.slack = res.connectors.load("slack")
        self.kafka = res.connectors.load("kafka")

    def post_long_laggers_to_kafka_alerts(self):
        for _, row in self.data_processor.lagging_print_assets.iterrows():
            # for now we'll have the id be the key to make only one record per lagging print asset
            message = {
                "id": f"lagging-{row['key']}",
                "one_number": str(row["one_number"]),
                "sku": row["sku"],
                "order_number": str(row["one_number"]),
                "order_date": str(row["customer_ordered_at"]),
                "event_date": str(row["last_updated_at"]),
                "event_type": f"lagging-{row['rank']}",
            }
            self.kafka["res_platform.sla_events.one_alerts"].publish(
                message, use_kgateway=True
            )
        # also post the recently printed healings
        pa_table = self.data_processor.pa_table_joined
        recently_printed_healings = pa_table[
            (pa_table["rank"] == "Healing")
            & (pa_table["update_lag_time_hours"] < 24)
            & (pa_table["print_exit"] == "DONE")
        ]
        for _, row in recently_printed_healings.iterrows():
            message = {
                "id": f"healing-complete-{row['key']}",
                "one_number": str(row["one_number"]),
                "sku": row["sku"],
                "order_number": str(row["one_number"]),
                "order_date": str(row["customer_ordered_at"]),
                "event_date": str(row["last_updated_at"]),
                "event_type": "healing-complete",
            }
            self.kafka["res_platform.sla_events.one_alerts"].publish(
                message, use_kgateway=True
            )

    def post_long_laggers_to_slack(self, sorted_df, template, additional_text):
        selected_columns = sorted_df[
            [
                "key",
                "make_node",
                "rank",
                "optimus_status",
                "status",
                "roll_names",
                "update_lag",
                "order_date",
                "first_asset_exit_lag",
                "first_asset_exit_date",
                "ppu_scheduled_date",
                "make_sequence_number",
                "body_assets",
                "asset_pieces",
                "body_pieces",
                "body_materials",
                "material_roll_available",
                "updated_after_last_optimus_run",
            ]
        ]
        selected_columns.loc[
            (selected_columns["optimus_status"].isna())
            & (selected_columns["material_roll_available"] == False),
            "optimus_status",
        ] = "no roll available"
        selected_columns.loc[
            (selected_columns["optimus_status"].isna())
            & (selected_columns["updated_after_last_optimus_run"]),
            "optimus_status",
        ] = "waiting for optimus"
        # this adds owner field to selected_columns df and returns a dict of owner to message
        owner_message = self.set_owners_by_optimus(selected_columns)

        top_10 = selected_columns[
            [
                "key",
                "make_node",
                "rank",
                "optimus_status",
                "update_lag",
                "order_date",
                "first_asset_exit_lag",
                "roll_names",
                "ppu_scheduled_date",
                "make_sequence_number",
                "owner",
            ]
        ].head(10)
        top_10_result = tabulate(
            top_10, headers="keys", tablefmt="pipe", showindex=False
        )
        message = template.format(top_10_result=top_10_result)
        if owner_message:
            message = message + "\n" + owner_message

        if selected_columns.shape[0] > 10:
            message = message + additional_text

        result_csv = selected_columns.to_csv(index=False)
        if selected_columns.shape[0] > 10:
            self.slack.post_file(
                result_csv.encode(),
                "all_assets.csv",
                channel=self.slack_channel,
                initial_comment=message,
            )
        else:
            self.slack.post_message(message, self.slack_channel)

    def post_healings_to_slack(self):
        sorted_healing_assets = self.data_processor.healing_assets.sort_values(
            by=["first_asset_exit_lag_time_hours"], ascending=False
        )
        self.post_long_laggers_to_slack(
            sorted_healing_assets,
            ":raichu-faint: *The following are the 10 laggiest healing assets* :raichu-faint: \n```{top_10_result}```",
            "\nALL healing assets:",
        )

    def post_lagging_non_healings_to_slack(self):
        sorted_lagging_combo_assets = (
            self.data_processor.lagging_combo_assets.sort_values(
                by=["first_asset_exit_lag_time_hours"], ascending=False
            )
        )
        self.post_long_laggers_to_slack(
            sorted_lagging_combo_assets,
            ":pepeturtleq: *The following print assets are non-healing that are lagging behind another asset in the same ONE for more than 24 hours* :pepeturtleq: \n```{top_10_result}```",
            "\nOther lagging non-healing assets:",
        )

    def post_recently_printed_healings(self):
        pa_table = self.data_processor.pa_table_joined
        recently_printed_healings = pa_table[
            (pa_table["rank"] == "Healing")
            & (pa_table["update_lag_time_hours"] < 24)
            & (pa_table["print_exit"] == "DONE")
        ]
        sorted_df = recently_printed_healings.sort_values(
            by=["order_date"], ascending=True
        )
        selected_columns = sorted_df[
            ["key", "order_date", "update_lag", "make_sequence_number"]
        ].rename(columns={"make_sequence_number": "make_seq_num"})
        results = tabulate(
            selected_columns, headers="keys", tablefmt="pipe", showindex=False
        )
        self.slack.post_message(
            f":chansey: *Recently printed healings (print_exit = Done and updated within last 24 hours)* :chansey: \n```{results}``` @ anaida these healings are recently completed",
            self.slack_channel,
        )

    def post_make_node_finished_assets_todo(self):
        pa_table = self.data_processor.pa_table_joined
        relevant_columns = [
            "key",
            "status",
            "make_node",
            "print_exit",
            "rank",
            "order_date",
            "update_lag",
        ]
        # these are print assets that have the node as cancelled or done but have status = to do
        todo_assets_finished_make = pa_table[
            ((pa_table["make_node"] == "Done") | (pa_table["make_node"] == "Cancelled"))
            & (pa_table["status"] == "TO DO")
        ]
        if todo_assets_finished_make.shape[0] > 0:
            todo_assets_finished_make_results = tabulate(
                todo_assets_finished_make[relevant_columns].sort_values(
                    by=["order_date"], ascending=True
                ),
                headers="keys",
                tablefmt="pipe",
                showindex=False,
            )
            self.slack.post_message(
                f":error_prompt: *Print assets with make node = Done|Cancelled but status = TO DO* :error_prompt: \n```{todo_assets_finished_make_results}``` @ sirsh @ aerskine - please clean",
                self.slack_channel,
            )

    def post_old_print_assets(self):
        pa_table = self.data_processor.old_assets.sort_values(
            by=["order_date"], ascending=True
        )
        pa_table["owner"] = None
        pa_table.loc[
            pa_table["optimus_status"] == "INSUFFICIENT_ASSET_LENGTH", "owner"
        ] = "anaida"
        self.post_long_laggers_to_slack(
            pa_table,
            ":fossil: *The following print assets are the oldest* :fossil: \n```{top_10_result}```",
            "\nALL assets older than 14 days:",
        )

    # specifically for the lagging tables
    def set_owners_by_optimus(self, filtered_pa_table):
        filtered_pa_table["owner"] = None
        owners_to_message = {}
        filtered_pa_table.loc[
            filtered_pa_table["optimus_status"] == "INSUFFICIENT_ASSET_LENGTH", "owner"
        ] = "anaida"
        any_insufficient_assets = (
            filtered_pa_table[
                filtered_pa_table["optimus_status"] == "INSUFFICIENT_ASSET_LENGTH"
            ].shape[0]
            > 0
        )
        if any_insufficient_assets:
            self._append_message(
                "@ anaida",
                "Some assets have insufficient asset length",
                owners_to_message,
            )
        filtered_pa_table.loc[
            filtered_pa_table["optimus_status"] == "no roll available", "owner"
        ] = "anaida"
        any_insufficient_assets = (
            filtered_pa_table[
                filtered_pa_table["optimus_status"] == "no roll available"
            ].shape[0]
            > 0
        )
        if any_insufficient_assets:
            self._append_message(
                "@ anaida",
                "Some assets are an out of stock material",
                owners_to_message,
            )
        filtered_pa_table.loc[
            filtered_pa_table["optimus_status"].isna(), "owner"
        ] = "rob, aerskine"
        any_missing_optimus_status = (
            filtered_pa_table[filtered_pa_table["optimus_status"].isna()].shape[0] > 0
        )
        if any_missing_optimus_status:
            self._append_message(
                "@ rob, @ aerskine",
                "Some assets are missing optimus status",
                owners_to_message,
            )

        message = ""
        for owner in owners_to_message:
            messages = owners_to_message[owner]
            message += f"\n{owner}: {'. '.join(messages)}"
        return message

    def _append_message(self, owner, message, owners_to_message):
        if owner not in owners_to_message:
            owners_to_message[owner] = []
        messages = owners_to_message[owner]
        messages.append(message)
        owners_to_message[owner] = messages
