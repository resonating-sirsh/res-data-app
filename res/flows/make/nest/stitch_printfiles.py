import res
import math
import pandas as pd
import numpy as np
import subprocess
import random
import qrcode
import os
import time
from res.airtable.print import COLOR_CHIPS, PRINTFILES, NESTS
from res.flows import flow_node_attributes, FlowContext
from res.flows.make.nest.utils import write_guillotine_seperator_to_file
from res.flows.meta.ONE.controller import MetaOnePieceManager
from res.flows.meta.ONE.meta_one import MetaOne
from res.learn.optimization.packing.annealed.progressive import (
    HEADER_HEIGHT_INCHES_UNCOMPENSATED,
    SEPERATOR_HEIGHT_INCHES_UNCOMPENSATED,
)
from res.media.images.geometry import is_bias_piece
from res.utils import logger, res_hash, secrets_client
from res.learn.optimization.nest import nest_transformations_apply
from shapely.wkt import loads as shapely_loads
from shapely.affinity import scale
from .pack_pieces import DPI_SETTING
from .composite_printfile import _get_image_label_info
from .utils import (
    s3_path_to_image_supplier,
    nest_to_dxf,
    zero_waste_patch_image_supplier,
)

s3 = res.connectors.load("s3")

HEADER_IMAGE_TEMPLATE_PATH = "s3://res-data-development/flows/v1/dxa-printfile/expv2/nesting/resources/headers/Color Chips V3_CHRST IN Gamut_Reference Points Added-01.png"
PIXEL_PER_MILLIMETER_SETTING = DPI_SETTING / 25.4


def _make_seperator_image(width, stretch_y, index, nest_pre, nest_post):
    from pyvips import Image as vips_image

    filename = f"seperator_{index}.png"
    filename_comp = f"seperator_{index}_comp.png"
    single_label = nest_pre is None or nest_post is None
    top_label = nest_post if nest_pre is None else nest_pre
    bottom_label = None if single_label else nest_post

    write_guillotine_seperator_to_file(
        top_label,
        bottom_label,
        int(
            SEPERATOR_HEIGHT_INCHES_UNCOMPENSATED
            * DPI_SETTING
            * (0.5 if single_label else 1.0)
        ),
        width,
        filename,
    )
    vips_image.pngload(filename).affine((1, 0, 0, stretch_y)).pngsave(filename_comp)
    return filename_comp


def _make_text_image(text, width, height, transparent=True):
    from pyvips import Image as vips_image

    label_image = vips_image.text(
        text,
        width=width,
        height=height,
        autofit_dpi=True,
    )[0]
    rgba_label = (
        label_image.new_from_image([0, 0, 0])
        .bandjoin(label_image)
        .copy(interpretation="srgb")
    )
    if transparent:
        return rgba_label
    else:
        return (
            rgba_label.new_from_image([255, 255, 255, 255])
            .copy(interpretation="srgb")
            .composite(rgba_label, "over", x=0, y=0)
        )


# TODO: put a qr code with some info.
def _make_header_image(
    roll_name,
    index,
    stretch_x,
    stretch_y,
    roll_width_px,
    image_start_px,
    roll_offset_px,
    output_path,
    printfile_record_id,
    nest_name_post,
):
    from pyvips import Image as vips_image

    filename = f"header_{index}.png"
    s3._download(
        HEADER_IMAGE_TEMPLATE_PATH,
        target_path=".",
        filename=f"pre_{filename}",
    )
    chip_name = f"{roll_name} ({index})"
    label_image = _make_text_image(chip_name, 900, 200, transparent=False)
    qr_code = np.asarray(qrcode.make(chip_name, border=0).convert("RGB"))
    qr_code_vips = vips_image.new_from_memory(
        qr_code.data, qr_code.shape[1], qr_code.shape[0], bands=3, format="uchar"
    ).copy(interpretation="srgb")

    base_image = vips_image.pngload(f"pre_{filename}").addalpha()
    base_image = base_image.composite(label_image, "over", x=40, y=1540)
    base_image = base_image.composite(qr_code_vips, "over", x=940, y=1540)

    # compensate
    compensated_image = base_image.affine((stretch_x, 0, 0, stretch_y))

    grid_line_weight = 8  # 8 px is close to 0.027" which is what ivan wants

    grid_height = (
        300 * int(math.ceil(compensated_image.height / 300)) + grid_line_weight
    )

    # randomize position.
    roll_width_background = (
        vips_image.black(
            int(roll_width_px),
            int(HEADER_HEIGHT_INCHES_UNCOMPENSATED * 300 * stretch_y),
        )
        + (0, 0, 0, 0)
    ).copy(
        interpretation="srgb",
        xres=PIXEL_PER_MILLIMETER_SETTING,
        yres=PIXEL_PER_MILLIMETER_SETTING,
    )

    # add on an uncompensated 1 inch grid pattern on the background.
    for x in range(0, int(math.ceil(roll_width_px / 300)) + 1):
        red = 255 if x % 6 == 0 else 0
        roll_width_background = roll_width_background.draw_rect(
            [red, 0, 0, 255],
            max(0, 300 * x - grid_line_weight / 2),
            0,
            grid_line_weight,
            grid_height,
            fill=True,
        )
    for y in range(0, int(math.ceil(grid_height / 300)) + 1):
        red = 255 if y % 6 == 0 else 0
        roll_width_background = roll_width_background.draw_rect(
            [red, 0, 0, 255],
            0,
            max(0, 300 * y - grid_line_weight / 2),
            roll_width_px,
            min(grid_line_weight, grid_height - 300 * y + grid_line_weight / 2),
            fill=True,
        )
    # go back and draw on labels after over the grid lines.
    for x in range(0, int(math.ceil(roll_width_px / 300)) + 1):
        if x % 6 == 0:
            roll_width_background = roll_width_background.composite(
                _make_text_image(
                    chip_name + " " + str(x // 6),
                    1700,
                    200,
                    transparent=False,
                ),
                "over",
                x=50 + 300 * x,
                y=30,
            )
    pos_idx = random.randrange(0, 3)
    x_pos = [
        0,
        1800 * math.floor((roll_width_px - compensated_image.width) / 3600),
        roll_width_px - compensated_image.width,
    ][pos_idx]

    final_image = roll_width_background.composite(
        compensated_image, "over", x=int(x_pos), y=0
    )

    label = _make_text_image(nest_name_post, 10000, 300).affine(
        (stretch_x, 0, 0, stretch_y)
    )

    final_image = final_image.composite(
        label,
        "over",
        x=450,
        y=grid_height + 75,
    )

    final_image.pngsave(filename)

    # write record into airtable.
    if os.environ["RES_ENV"] == "production":
        # we want to have the qr code in airtable for some reason.
        qr_s3_path = f"{output_path}/qr{index}.png"
        s3.write(qr_s3_path, qr_code)
        COLOR_CHIPS.create(
            {
                "Name": chip_name,
                "Horizontal Position": ["Left", "Center", "Right"][pos_idx],
                "Location X": x_pos / 300,
                "Print File Location Y": image_start_px / 300,
                "Roll Location Y": (image_start_px + roll_offset_px) / 300,
                "QR Code Img": [{"url": s3.generate_presigned_url(qr_s3_path)}],
                "Print File": [printfile_record_id]
                if printfile_record_id is not None
                else [],
            },
            typecast=True,
        )

    return filename


def _concatenate_printfiles_imagemagick(
    input_file_paths, output_file_path, thumbnail_path
):
    from pyvips import Image as vips_image

    logger.info("Concatenating printfiles")
    # sad
    if len(input_file_paths) > 1:
        subprocess.run(["convert"] + input_file_paths + ["-append", output_file_path])
    else:
        subprocess.run(["mv"] + input_file_paths + [output_file_path])
    logger.info("done stitching")
    # just read header info to check.
    img = vips_image.pngload(output_file_path)
    if (
        abs(img.yres - PIXEL_PER_MILLIMETER_SETTING) > 0.1
        or abs(img.xres - PIXEL_PER_MILLIMETER_SETTING) > 0.1
    ):
        raise ValueError(
            f"Generated an image with invalid resolution: {img.xres, img.yres} and size {img.width, img.height}"
        )
    file_width_px = img.width
    file_height_px = img.height
    logger.info(
        f"stitched a printfile with: {file_width_px} width and {file_height_px} height"
    )
    logger.info("making thumbnail")
    subprocess.run(["convert", output_file_path, "-thumbnail", "5%", thumbnail_path])
    return (file_width_px, file_height_px)


def _download_printfile(asset_path, local_file_name):
    file_name = f"{asset_path}/printfile_composite_noheader.png"
    if s3.exists(file_name):
        s3._download(file_name, target_path=".", filename=local_file_name)
    else:
        # some old flow created the thing so just have the header in there for now.
        file_name = f"{asset_path}/printfile_composite.png"
        s3._download(file_name, target_path=".", filename=local_file_name)


def concatenate_printfiles(
    roll_name,
    asset_df,
    output_path,
    num_headers,
    roll_offset_px,
    header_offset_idx,
    printfile_record_id,
):
    from pyvips import Image as vips_image

    material_prop_df = s3.read(_fix_nest_path(asset_df["nest_df_path"][0]))
    stretch_y = material_prop_df.stretch_y.max()
    stretch_x = material_prop_df.stretch_x.max()
    roll_width_px = material_prop_df.output_bounds_width.max()
    logger.info(f"Downloading image files {asset_df.asset_path.values}")
    # when stitching we only want to have 3 headers per roll
    # TODO: composite on demand so we know when to put the header in the image etc.  see comment in nest_transformations_apply
    header_stride = (
        0 if num_headers == 0 else max(1, math.ceil(asset_df.shape[0] / num_headers))
    )
    asset_df["temp_file_name"] = asset_df.apply(lambda r: f"{r.name}.png", axis=1)
    asset_df.apply(
        lambda r: _download_printfile(r.asset_path, r.temp_file_name),
        axis=1,
    )
    asset_df["printfile_height_px"] = asset_df.temp_file_name.apply(
        lambda p: vips_image.pngload(p, access="sequential").height
    )
    asset_df["image_start_px"] = (
        asset_df.printfile_height_px.cumsum() - asset_df.printfile_height_px
    )
    asset_df["nest_name_simple"] = asset_df["nest_job_key"].apply(
        lambda k: "_".join(k.split("_")[1:]).replace("-", "_")
    )
    asset_df["nest_name_pre"] = asset_df.apply(
        lambda r: r["nest_name_simple"]
        if r["nests_parts"] == 1
        else r["nest_name_simple"] + "_part_" + str(r["nests_parts"] - 1),
        axis=1,
    )
    asset_df["nest_name_post"] = asset_df.apply(
        lambda r: r["nest_name_simple"]
        if r["nests_parts"] == 1
        else r["nest_name_simple"] + "_part_0",
        axis=1,
    )
    asset_df["header_file_name"] = asset_df.apply(
        lambda r: _make_header_image(
            roll_name,
            header_offset_idx + (r.name // header_stride),
            stretch_x,
            stretch_y,
            roll_width_px,
            r.image_start_px,
            roll_offset_px,
            output_path,
            printfile_record_id,
            r.nest_name_pre,
        )
        if num_headers > 0 and r.name % header_stride == 0
        else _make_seperator_image(
            roll_width_px,
            stretch_y,
            r.name,
            None if r.name == 0 else asset_df.loc[r.name - 1, "nest_name_post"],
            r.nest_name_pre,
        ),
        axis=1,
    )
    width = vips_image.pngload("0.png", access="sequential").width
    logger.info(f"Got printfiles with width {width}")
    logger.info("Making header and seperator images")
    concat_paths = [
        p
        for _, r in asset_df.iterrows()
        for p in [r.header_file_name, r.temp_file_name]
    ]
    concat_paths.append(
        _make_seperator_image(
            roll_width_px,
            stretch_y,
            "footer",
            asset_df.loc[asset_df.shape[0] - 1, "nest_name_post"],
            None,
        )
    )
    if num_headers == 0:
        concat_paths = concat_paths[1:]
    logger.info(f"Concatenating {len(concat_paths)} images: {concat_paths}")
    combined_temp_path = "printfile_composite.png"
    thumbnail_temp_path = "printfile_composite_thumbnail.png"
    file_width_px, file_height_px = _concatenate_printfiles_imagemagick(
        concat_paths, combined_temp_path, thumbnail_temp_path
    )
    logger.info(f"Uploading images to {output_path}")
    s3.upload(combined_temp_path, f"{output_path}/printfile_composite.png")
    s3.upload(thumbnail_temp_path, f"{output_path}/printfile_composite_thumbnail.png")
    return (file_width_px, file_height_px, asset_df)


def _nest_df_row_to_printfile_piece(
    row, offset, nest_height, file_height_px, nest_job_key
):
    piece_name = row.piece_name
    # fix kafkaesque y coord into something less insane
    dist_from_nest_top_px = nest_height - row.max_nested_y
    dist_from_printfile_top_px = offset + dist_from_nest_top_px
    dist_from_printfile_bottom_px = (
        file_height_px - dist_from_nest_top_px - (row.max_nested_y - row.min_nested_y)
    )
    return {
        "asset_id": row.asset_id,
        "asset_key": row.asset_key,
        "piece_id": row.piece_id,
        "sku": row.get(
            "sku", ""
        ),  # note -- sku will be present for nests made after this is committed.
        "nest_job_key": nest_job_key,
        "piece_name": piece_name,
        "min_y_inches": dist_from_printfile_top_px / DPI_SETTING,
        "max_y_inches": (
            dist_from_printfile_top_px + row.max_nested_y - row.min_nested_y
        )
        / DPI_SETTING,
        "min_x_inches": row.min_nested_x / DPI_SETTING,
        "max_x_inches": row.max_nested_x / DPI_SETTING,
        "inches_from_printfile_top": dist_from_printfile_top_px / DPI_SETTING,
        "inches_from_printfile_bottom": dist_from_printfile_bottom_px / DPI_SETTING,
        "piece_file_path": row.s3_image_path
        if row.s3_image_path is not None
        else "n/a",
    }


def _fix_nest_path(nest_path):
    if nest_path.endswith(".feather"):
        return nest_path
    else:
        return f"{nest_path}/output.feather"


def _build_piece_info(nest_job_key, nest_df, offset, nest_image_height, file_height_px):
    # make into printfilePiece records
    printfile_pieces = nest_df[nest_df.asset_id.notnull()].apply(
        lambda r: _nest_df_row_to_printfile_piece(
            r, offset, nest_image_height, file_height_px, nest_job_key
        ),
        axis=1,
    )
    return printfile_pieces


def _get_file_height(filename):
    from pyvips import Image as vips_image

    img = vips_image.pngload(filename)
    return img.height


def _dump_piece_info(
    fc,
    asset_df,
    num_headers,
    printfile_record_id,
    output_path,
    file_width_px,
    file_height_px,
):
    if "nest_job_key" not in asset_df.columns:
        asset_df["nest_job_key"] = asset_df.asset_path.apply(lambda x: x.split("/")[-1])
    piece_records = []
    offset = 0
    stretch_y = None
    for i, r in asset_df.iterrows():
        nest_df = s3.read(_fix_nest_path(r.nest_df_path))
        if stretch_y is None:
            stretch_y = nest_df.stretch_y.max()
        if i > 0 or num_headers > 0:
            offset += _get_file_height(r.header_file_name)
        pieces = _build_piece_info(
            r.nest_job_key, nest_df, offset, r.printfile_height_px, file_height_px
        )
        piece_records.extend(pieces)
        offset += r.printfile_height_px
    printfile_record = {
        "printfile_name": _get_printfile_name(printfile_record_id),
        "airtable_record_id": fc.args.get("print_file_record_id", "unkn"),
        "optimus_task_key": fc.args.get("optimus_task_key", "unkn"),
        "roll_name": fc.args.get("roll_name", "unkn"),
        "num_headers": num_headers,
        "nest_job_keys": asset_df.nest_job_key.values.tolist(),
        "stitching_job_key": fc.key,
        "printfile_s3_path": output_path,
        "printfile_width_px": file_width_px,
        "printfile_height_px": file_height_px,
        "printfile_width_inches": file_width_px / float(DPI_SETTING),
        "printfile_height_inches": file_height_px / float(DPI_SETTING),
        "header_height_inches": HEADER_HEIGHT_INCHES_UNCOMPENSATED * stretch_y,
        "seperator_height_inches": SEPERATOR_HEIGHT_INCHES_UNCOMPENSATED * stretch_y,
        "piece_info": piece_records,
    }
    logger.info(f"Sending kafka message: {printfile_record}")
    res.connectors.load("kafka")["res_make.optimus.printfile_pieces"].publish(
        printfile_record, use_kgateway=True
    )
    # also make a df on s3 containing this info.
    df_info = [{**p, **printfile_record} for p in printfile_record.pop("piece_info")]
    fc._write_output(
        pd.DataFrame(df_info), "concat", fc.key, output_filename="manifest.feather"
    )


def _add_composited_printfile_path_to_nest(nest_record_id, nest_output_path):
    """
    To make life easier on people who expect the nests printfile path to be there in airtable.
    """
    # lame retry logic
    for attempt in range(3):
        try:
            NESTS.update(
                nest_record_id,
                {
                    "Asset Path": nest_output_path,
                    "DXF Files": [
                        {"url": s3.generate_presigned_url(p)}
                        for p in s3.ls(nest_output_path)
                        if p.endswith(".dxf")
                    ],
                },
            )
            break
        except Exception as ex:
            logger.warn(f"Failed to write asset path to airtable: {repr(ex)}")
            time.sleep(10 * (attempt + 1))


def _get_printfile_name(record_id):
    try:
        record = PRINTFILES.get_record(record_id)
        return record["fields"]["Key"]
    except:
        return ""


def _intermediate_nest_path(fc, nest_key):
    return fc.get_node_path_for_key("composite", key=fc.key) + "/" + nest_key


def _row_to_image_supplier(row):
    if row.piece_id.startswith("patch_"):
        return zero_waste_patch_image_supplier(row.s3_image_path)
    # the column piece_info is just the unadulterated PPP output.
    piece_info = row.get("piece_info") or {}

    if piece_info.get("annotation_json") is not None:
        return lambda: np.asarray(
            MetaOnePieceManager.annotate_cached_image_from_ppp(piece_info)
        )

    if not pd.isna(row.s3_image_path):
        return s3_path_to_image_supplier(row.s3_image_path)

    sku = row.sku
    one_number = int(row.one_number)
    material_code = row.material_code
    piece_code = row.piece_code

    def load_piece_from_meta_one():
        logger.info(
            f"Loading piece image from meta one {sku} {one_number} {material_code} {piece_code}"
        )
        m1 = MetaOne(
            sku,
            one_number,
            material_code=material_code,
        )
        for piece_key, piece in m1._pieces.items():
            if piece_key.endswith(piece_code):
                return np.asarray(piece.labelled_piece_image)
        raise ValueError(
            f"Unknown how to get image for piece {piece_code} with pieces {list(m1)}"
        )

    return load_piece_from_meta_one


@flow_node_attributes(
    "stitch_printfiles.generator",
)
def generator(event, context):
    with FlowContext(event, context) as fc:
        return fc.asset_list_to_payload(fc.assets)


@flow_node_attributes(
    "stitch_printfiles.handler",
    description="Build the giant image corresponding to a nested set of pieces.",
    memory="128G",
    disk="50G",
    cpu="24000m",
    allow24xlg=True,
)
def handler(event, context):
    with FlowContext(event, context) as fc:
        nest_key = fc.assets[0]["nest_job_key"]
        nest_path = fc.assets[0]["nest_df_path"]
        nest_record_id = fc.assets[0].get("nest_record_id")
        df = s3.read(_fix_nest_path(nest_path))
        output_path = _intermediate_nest_path(fc, nest_key)
        background_artwork_uri = fc.assets[0].get("background_artwork_uri")
        compensate_background = True
        uniform_mode = False
        if "uniform_mode" in df.columns and any(df["uniform_mode"]):
            uniform_mode = True
            artworks = df["artwork_uri"].dropna().unique()
            if len(artworks) > 1:
                raise ValueError(f"Got multiple artworks for uniform mode: {artworks}")
            background_artwork_uri = artworks[0]
            # the fucking res uniforms artwork is pre-compensated and nobody will ever fix it.
            compensate_background = not any(
                df.s3_image_path.apply(lambda p: "CC-3068-CTJ95-RESU" in p)
            )
        image_height = (
            df["total_nest_height"].max()
            if "total_nest_height" in df.columns
            else df["composition_y"].max()
        )
        # ensure that certain pieces are composited first.
        # this is just a hack for the case where the one labels are misplaced and floating outside of the piece bounds
        # so that the label doesnt end up covering another piece.
        df["comp_order"] = df.apply(
            lambda r: 0
            if is_bias_piece(
                scale(
                    shapely_loads(r.nested_geometry),
                    1.0 / r.stretch_x,
                    1.0 / r.stretch_y,
                )
            )
            else 1,
            axis=1,
        )
        comp_df = df.sort_values("comp_order")
        if uniform_mode:
            pieces_to_comp = comp_df.apply(
                lambda r: r.uniform_mode_placement or "guillotine_spacer" in r.piece_id,
                axis=1,
            )
            comp_df = comp_df[pieces_to_comp].reset_index(drop=True)
        composite_args = {
            **fc.args,
            "output_path": output_path,
            "stretch_x": df.stretch_x[0],
            "stretch_y": df.stretch_y[0],
            "output_bounds_width": df.output_bounds_width[0],
            "output_bounds_length": image_height + 10,
            "x_coord_field": "composition_x",
            "y_coord_field": "composition_y",
            "grid": "block_fuse" in nest_key,
            "background_artwork_uri": background_artwork_uri,
            "compensate_background": compensate_background,
            "buffers": [int(v) for v in comp_df.buffer],
            "uniform_mode": uniform_mode,
        }
        res.media.images.text.ensure_s3_fonts()
        res.media.images.icons.extract_icons()
        image_suppliers = comp_df.apply(_row_to_image_supplier, axis=1).values
        nest_transformations_apply(comp_df, image_suppliers, **composite_args)
        # try to dump a nested dxf.
        try:
            if "cutline_uri" not in df.columns:
                df["cutline_uri"] = None
            df["cutline_uri"] = df["cutline_uri"].where(
                df["cutline_uri"].notnull(),
                df["s3_image_path"].apply(
                    lambda p: p.replace(".png", ".feather").replace(
                        "extract_parts", "extract_cutlines"
                    )
                ),
            )
            nest_name_simple = "_".join(nest_key.split("_")[1:]).replace("-", "_")
            divider_idx = df[
                df.piece_id.apply(lambda x: x.startswith("guillotine_"))
            ].index.values
            if len(divider_idx) == 0:
                nest_to_dxf(df, f"{output_path}/{nest_name_simple}.dxf")
            else:
                start_idx = 0
                for i, end_idx in enumerate(divider_idx):
                    nest_to_dxf(
                        df.iloc[start_idx:end_idx],
                        f"{output_path}/{nest_name_simple}_part_{i}.dxf",
                    )
                    start_idx = end_idx + 1
                if df.shape[0] > start_idx:
                    nest_to_dxf(
                        df.iloc[start_idx:],
                        f"{output_path}/{nest_name_simple}_part_{len(divider_idx)}.dxf",
                    )
        except:
            pass
        if nest_record_id:
            _add_composited_printfile_path_to_nest(nest_record_id, output_path)


@flow_node_attributes(
    "stitch_printfiles.reducer",
    description="Combine multiple printfiles by concatenating them vertically.",
    memory="100G",  # decreasing these since we are limited in what can be fed to caldera anyway
    disk="50G",  # these should be upper bounds on what we ask for in res/docker/imagemagick/policy.xml
    cpu="24000m",
    allow24xlg=True,
)
def reducer(event, context):
    """
    Concatenates a bunch of already-made printfiles together into a giant one.
    The event should have:
    "assets": [
        # each entry represents a nesting.
        {
            "nest_record_id": id of the record.
            "asset_path": s3 directory where the composite_printfile.png is
            "nest_job_key": argo job key of nest -- so we can load up the piece info for putting in kafka
        }, ...
    ]
    "args": {
        "print_file_record_id": optional airtable record id for the printfile row.
    }
    """
    with FlowContext(event, context) as fc:
        df = fc.assets_dataframe
        df["asset_path"] = df["nest_job_key"].apply(
            lambda k: _intermediate_nest_path(fc, k)
        )
        if "nests_parts" not in df.columns:
            df["nests_parts"] = 1
        output_path = fc.get_node_path_for_key("concat", key=fc.key)
        printfile_record_id = fc.args.get("print_file_record_id")
        num_headers = int(fc.args.get("num_headers", 0))
        roll_offset_px = int(fc.args.get("roll_offset_px", 0))
        header_offset_idx = int(fc.args.get("header_offset_idx", 0))
        file_width_px, file_height_px, concat_df = concatenate_printfiles(
            fc.args.get("roll_name", "unknown_roll"),
            df,
            output_path,
            num_headers,
            roll_offset_px,
            header_offset_idx,
            printfile_record_id,
        )
        _dump_piece_info(
            fc,
            concat_df,
            num_headers,
            printfile_record_id,
            output_path,
            file_width_px,
            file_height_px,
        )
        if printfile_record_id:
            record_update = {
                "Asset Path": output_path,
                "Stitching Status": "DONE",
                "Height Px": file_height_px,
                "Width Px": file_width_px,
            }
            if fc.args.get("clear_flags", False):
                record_update["Flag For Review"] = "false"
                record_update["Flag for review Reason"] = ""
            PRINTFILES.update(printfile_record_id, record_update, typecast=True)


def on_success(event, context):
    pass


def on_failure(event, context):
    with FlowContext(event, context) as fc:
        printfile_record_id = fc.args.get("print_file_record_id")
        if printfile_record_id:
            PRINTFILES.update(
                printfile_record_id,
                {
                    "Flag For Review": "true",
                    "Flag for review Reason": f"Printfile stitching job failed: {fc._task_key}",
                    "Stitching Status": "ERROR",
                },
                typecast=True,
            )
