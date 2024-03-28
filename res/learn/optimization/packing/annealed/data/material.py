from ..progressive import (
    PX_PER_INCH,
    LASER_CUTTER_MAX_LENGTH_PX,
    SEPERATOR_HEIGHT_PX_UNCOMPENSATED,
    DISTORTION_GRID_HEIGHT_PX_UNCOMPENSATED,
)

# todo: refactor this out of flows.
from res.flows.make.nest.utils import transform_piece_for_nesting


class Material(object):
    def __init__(
        self,
        material_code,
        stretch_x,
        stretch_y,
        paper_stretch_x,
        paper_stretch_y,
        max_width_in,
        buffer,
        fabric_type,
        pretreat_type,
    ):
        self.material_code = material_code
        self.stretch_x = stretch_x
        self.stretch_y = stretch_y
        self.paper_stretch_x = paper_stretch_x
        self.paper_stretch_y = paper_stretch_y
        self.max_width_in = max_width_in
        self.max_width_px = max_width_in * PX_PER_INCH
        self.buffer = buffer
        self.fabric_type = fabric_type
        # large laser cutter = 88"
        self.max_guillotinable_length = (
            None if fabric_type == "Woven" else LASER_CUTTER_MAX_LENGTH_PX * stretch_y
        )
        self.guillotine_spacer = (
            None if fabric_type == "Woven" else (3 * 300 * stretch_y + 2 * 75)
        )  # 3" + buffered
        self.seperator_height_px = SEPERATOR_HEIGHT_PX_UNCOMPENSATED * stretch_y
        self.distortion_grid_height_px = (
            DISTORTION_GRID_HEIGHT_PX_UNCOMPENSATED * stretch_y
        )
        self.pretreat_type = pretreat_type

    def transform_for_nesting(self, p):
        return transform_piece_for_nesting(
            p, self.stretch_x, self.stretch_y, self.buffer, self.max_width_px
        )

    def add_props(self, df):
        df["stretch_x"] = self.stretch_x
        df["stretch_y"] = self.stretch_y
        df["paper_marker_stretch_x"] = self.paper_stretch_x
        df["paper_marker_stretch_y"] = self.paper_stretch_y
        if "buffer" not in df.columns:
            df["buffer"] = self.buffer
        df["buffer"] = df["buffer"].fillna(self.buffer)
        df["output_bounds_width"] = self.max_width_px
        df["material"] = self.material_code
        return df
