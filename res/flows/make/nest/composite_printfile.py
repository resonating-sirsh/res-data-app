import res
from res.flows import flow_node_attributes, FlowContext
from res.learn.optimization.nest import nest_transformations_apply
from .utils import s3_path_to_image_supplier

s3 = res.connectors.load("s3")


def _get_nest_df(fc):
    return s3.read(fc.args.get("nest_df_path"))


def _get_output_path(fc):
    return fc.get_node_path_for_key("composite", fc.args.get("job_key"))


def _get_piece_label_text(image_path):
    image_filename = image_path.split("/")[-1].split(".")[0]
    if "-" in image_filename:
        # meta 1 which is like: DJ-6000-V1-BODFTFACLF-BF_L - and the thing we care about is the BODFTFACLF
        parts = image_filename.split("-")
        if len(parts) == 5:
            return parts[3]
    return image_filename


def _get_image_label_info(df):
    """
    Creating some labels so we can make a labeled version of hte printfile for use for roll inspection.
    Basically so that people can know what piece they are looking at when they have the actual printed roll in front of them.
    The labels are just the one number and the piece name and we try to get the angle right for the diagonal pieces.
    """
    return [
        (
            f"{r.asset_key} ({_get_piece_label_text(r.s3_image_path)})",
            r.nested_centroid,
            r.nested_label_angle,
        )
        for _, r in df.iterrows()
    ]


@flow_node_attributes(
    "composite_printfile",
    description="Build the giant image corresponding to a nested set of pieces.",
    memory="64Gi",
    disk="20G",
    mapped=False,
    allow24xlg=True,
)
def handler(event, context):
    """
    Composites a print file based on a nesting.
    The nesting itself records everything we needed to generate the file, so all that this thing needs
    to know is where to get the nesting.
    The event should have:
    "args": {
        "job_key": the key of the pack_pieces job the output of which needs composititing.
        "header_data": data to encode into header qr code.
        "header_image_path": path to header base image.
    }
    """
    with FlowContext(event, context) as fc:
        df = _get_nest_df(fc)

        composite_args = {
            **fc.args,
            "output_path": _get_output_path(fc),
            "stretch_x": df.stretch_x[0],
            "stretch_y": df.stretch_y[0],
            "output_bounds_width": df.output_bounds_width[0],
            "output_bounds_length": df.composition_y.max() + 10,
            "x_coord_field": "composition_x",
            "y_coord_field": "composition_y",
            "image_labels": _get_image_label_info(df),
        }
        nest_transformations_apply(
            df,
            [s3_path_to_image_supplier(p) for p in df.s3_image_path.values],
            **composite_args,
        )
