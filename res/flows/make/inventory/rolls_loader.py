from res.airtable.print import ROLLS
from res.airtable.misc import MATERIAL_PROP
from datetime import datetime
import pandas as pd


class RollLoader:
    def __init__(self) -> None:
        self.material_info_df = self._get_avilable_material_info()
        self.calculated_at = datetime.now()
        (
            self.material_code_to_name,
            self.material_name_to_code,
        ) = self._get_material_names()

    def get_ready_to_print_yards(self, material_code: str) -> float:
        if material_code not in self.material_info_df.index:
            return 0
        return self.material_info_df.loc[material_code].ready_to_print_yards_sum

    def get_scouring_or_printable_yards_unflagged(self, material_code: str) -> float:
        if material_code not in self.material_info_df.index:
            return 0
        return self.material_info_df.loc[
            material_code
        ].scouring_or_printable_yards_unflagged_sum

    def get_after_scouring_not_printed_flagged_yards(self, material_code: str) -> float:
        if material_code not in self.material_info_df.index:
            return 0
        return self.material_info_df.loc[
            material_code
        ].after_scouring_not_printed_flagged_yards_sum

    def get_not_yet_printed_flagged_yards(self, material_code: str) -> float:
        if material_code not in self.material_info_df.index:
            return 0
        return self.material_info_df.loc[
            material_code
        ].not_yet_printed_flagged_yards_sum

    def get_material_code(self, material_name):
        if material_name in self.material_name_to_code:
            return self.material_name_to_code[material_name]
        return None

    def get_material_name(self, material_code):
        if material_code in self.material_code_to_name:
            return self.material_code_to_name[material_code]
        return None

    def _get_material_names(self):
        material_info = MATERIAL_PROP.df(
            field_map={
                "Material Name": "material_name",
                "Material Code": "material_code",
            },
            formula="""{Development Status}='Approved'""",
        )
        return (
            material_info.set_index("material_code")["material_name"].to_dict(),
            material_info.set_index("material_name")["material_code"].to_dict(),
        )

    def _get_avilable_material_info(self):
        rolls_info = ROLLS.df(
            field_map={
                "Name": "name",
                "Material Code": "material_code",
                "Current Substation": "current_substation",
                "Latest Measurement": "latest_measurement",
                "📏Roll Length Data": "roll_length_data",
                "Roll Length (yards)": "roll_length_yards",
                "Flag for Review": "flag_for_review",
            },
            formula=f"""
            AND(
                {{Allocated for PPU}}=FALSE(),
                {{Print ONE Ready}}=0,
                {{Current Substation}}!='Done',
                {{Current Substation}}!='Write Off',
                {{Current Substation}}!='Expired',
                {{Current Substation}}!='Steam',
                {{Current Substation}}!='Wash',
                {{Current Substation}}!='Dry',
                {{Current Substation}}!='Inspection',
                {{Current Substation}}!='Soft',
                {{Print Assets (flow) 3}}=''
            )""",
        )
        # why is material code a list? anyway it only makes sense to count a roll when it's a single material
        rolls_info = rolls_info[
            rolls_info["material_code"].apply(
                lambda x: isinstance(x, list) and len(x) == 1
            )
        ]
        rolls_info["material_code"] = rolls_info["material_code"].apply(lambda x: x[0])
        rolls_info["latest_measurement"] = pd.to_numeric(
            rolls_info["latest_measurement"], errors="coerce"
        )
        rolls_info["roll_length_yards"] = pd.to_numeric(
            rolls_info["roll_length_yards"], errors="coerce"
        )

        # if it hasn't been inspected yet, the roll is still good but can't be printed on yet
        # so we only filter if n/a or less than 10
        rolls_info["post_scour_yards"] = (
            (rolls_info["roll_length_data"].notna())
            & (rolls_info["latest_measurement"] >= 10)
            & (rolls_info["current_substation"] != "Scour: Wash")
            & (rolls_info["current_substation"] != "Scour: Dry")
        )

        rolls_info["ready_to_print_yards"] = (
            rolls_info["post_scour_yards"] & rolls_info["flag_for_review"].isna()
        )

        rolls_info["after_scouring_not_printed_flagged"] = (
            rolls_info["post_scour_yards"] & rolls_info["flag_for_review"] == True
        )

        rolls_info["not_yet_printed_yards"] = (
            rolls_info["latest_measurement"].isna()
        ) | (rolls_info["latest_measurement"] >= 10)

        rolls_info["scouring_or_printable_yards"] = (
            rolls_info["not_yet_printed_yards"] & rolls_info["flag_for_review"].isna()
        )

        rolls_info["not_yet_printed_flagged_yards"] = (
            rolls_info["not_yet_printed_yards"] & rolls_info["flag_for_review"] == True
        )

        rolls_info["coalesce_roll_length_yards"] = rolls_info[
            "latest_measurement"
        ].fillna(rolls_info["roll_length_yards"])
        result = rolls_info.groupby("material_code").agg(
            ready_to_print_yards_sum=(
                "coalesce_roll_length_yards",
                lambda x: x[rolls_info["ready_to_print_yards"]].sum(),
            ),
            scouring_or_printable_yards_unflagged_sum=(
                "coalesce_roll_length_yards",
                lambda x: x[rolls_info["scouring_or_printable_yards"]].sum(),
            ),
            after_scouring_not_printed_flagged_yards_sum=(
                "coalesce_roll_length_yards",
                lambda x: x[rolls_info["after_scouring_not_printed_flagged"]].sum(),
            ),
            not_yet_printed_flagged_yards_sum=(
                "coalesce_roll_length_yards",
                lambda x: x[rolls_info["not_yet_printed_flagged_yards"]].sum(),
            ),
        )
        return result
