import res
import time
import pandas as pd
import numpy as np
import cv2
from pyairtable import Table
from res.flows import flow_node_attributes, FlowContext
from res.flows.make.nest.pack_pieces import _pack_pieces
from res.learn.optimization.packing.annealed.progressive import (
    ProgressiveNode,
    PX_PER_INCH,
)
from res.learn.optimization.packing.annealed.packing import pack_multi
from res.media.images.providers.dxf import DxfFile
from res.utils import logger
from shapely.affinity import translate, scale
from shapely.geometry import Polygon, box
from shapely.ops import unary_union
from shapely.wkt import dumps
from matplotlib import pyplot as plt
from .utils import (
    update_production_request,
    make_bbox_image,
    add_blocks_to_healing_app,
)
from ..utils import (
    transform_pieces_for_nesting,
    validate_shapes,
    piece_label_angle,
    upload_rectangle_piece,
    pixel_space_to_dxf,
    nest_to_dxf,
)

INPUT_KAFKA_TOPIC = "res_meta.dxa.prep_pieces_responses"
OUTPUT_KAFKA_TOPIC = "res_make.res_nest.one_nest"
KAFKA_CONSUMER_NAME = "pack_one"
MAP_LIMIT = 10
MAX_FUSING_WIDTH_PX = 300 * 32
FUSING_PIECE_BUFFER_PX = 450
MAX_LASER_CUTTER_HEIGHT_PX = 88 * 300

BLOCK_FUSER_MAX_WIDTH_PX = 32 * PX_PER_INCH  # 32 inches.

s3 = res.connectors.load("s3")


@flow_node_attributes(
    "pack_one.generator",
)
def generator(event, context={}):
    with FlowContext(event, context) as fc:
        if fc.assets:
            return [
                {
                    **dict(event),
                    "assets": [m],
                }
                for m in fc.assets
            ]
        kafka = res.connectors.load("kafka", group_id=KAFKA_CONSUMER_NAME)[
            INPUT_KAFKA_TOPIC
        ]
        map_tasks = []
        t1 = time.time() + 300
        while len(map_tasks) < MAP_LIMIT and time.time() < t1:
            df = kafka.consume(give_up_after_records=MAP_LIMIT)
            df = df.where(pd.notnull(df), None)
            map_tasks.extend(
                [
                    {
                        **dict(event),
                        "assets": [m],
                    }
                    for m in df.to_dict(orient="records")
                    if m.get("metadata", {}).get("rank", "").lower() != "healing"
                    or m.get("flow") == "v2"
                ]
            )
        return map_tasks


def _kafka_msg_to_df(msg):
    one_df = pd.DataFrame(msg["make_pieces"])
    one_df["piece_info"] = one_df.apply(dict, axis=1)
    one_df = one_df.rename(
        columns={"filename": "s3_image_path", "outline_uri": "s3_shape_path"}
    )
    for field in [
        "piece_type",
        "one_number",
        "material_code",
        "color_code",
        "body_code",
    ]:
        one_df[field] = msg[field]
    for field in [
        "output_bounds_width",
        "stretch_x",
        "stretch_y",
        "paper_marker_stretch_x",
        "paper_marker_stretch_y",
        "buffer",
        "cuttable_width",
        "offset_size",
    ]:
        one_df[field] = msg["metadata"].get(field, -1)
    one_df["thread_colors"] = msg["metadata"].get("thread_colors", "")
    # add in identification pieces
    if "piece_group" not in one_df.columns:
        one_df["piece_group"] = None
    return one_df


def _add_piece_info_to_df(nest_df):
    nest_df["piece_name"] = nest_df.apply(
        lambda r: (
            r.piece_name
            if isinstance(r.piece_name, str)
            else r.s3_image_path.split("/")[-1].replace(".png", "")
        ),
        axis=1,
    )
    if "piece_code" not in nest_df.columns:
        nest_df["piece_code"] = nest_df["piece_name"].apply(
            lambda n: None
            if n is None or "-" not in n or n.isnumeric()
            else "-".join(n.split("-")[3:5]).split("_")[0]
        )
    # nonsense lib -pure garbage. who wrote this
    nest_df = nest_df.where(nest_df.notnull(), None)
    zone_info = {
        r["fields"]["Generated Piece Code"]: r["fields"]
        for r in Table(
            res.utils.secrets_client.get_secret("AIRTABLE_API_KEY"),
            "appWxYpvDIs8JGzzr",
            "tblJgBId024pDSwBH",
        ).all(
            fields=[
                "Generated Piece Code",
                "Generated Piece Name",
                "Commercial Acceptability Zone (from Part Tag)",
            ],
            formula=f"AND({{Generated Piece Code}}!='', find({{Generated Piece Code}}, '{','.join(set(n for n in nest_df.piece_code.values if n is not None))}'))",
        )
    }
    nest_df["piece_description"] = nest_df["piece_code"].apply(
        lambda c: zone_info.get(c, {}).get("Generated Piece Name", "")
    )
    nest_df["zone"] = nest_df["piece_code"].apply(
        lambda c: zone_info.get(c, {}).get(
            "Commercial Acceptability Zone (from Part Tag)", [""]
        )[0]
    )
    return nest_df


def _handle_nested_pieces(msg, one_df, fc, uniform_mode_pieces=[]):
    """
    For ones where we just need to nest a bunch of pieces.
    """
    key = msg["id"]
    block_fuse = one_df["piece_type"][0] == "block_fuse"
    logger.info(f"Nesting {one_df.shape[0]} pieces")
    # prepare for nesting
    max_width = one_df.output_bounds_width.max()
    logger.info(f"Nesting with max width {max_width}")
    logger.info(f"Loading geometry")
    if len(uniform_mode_pieces) > 0:
        logger.warn(
            f"Using uniform mode for body {one_df.body_code[0]} {one_df.color_code[0]} with placement pieces {uniform_mode_pieces}"
        )
    one_df["uniform_mode"] = len(uniform_mode_pieces) > 0
    one_df["uniform_mode_placement"] = one_df.piece_code.apply(
        lambda c: c in uniform_mode_pieces
    )
    one_df["shape_path"] = one_df.apply(
        lambda r: r.s3_shape_path
        if len(uniform_mode_pieces) == 0 or r.piece_code in uniform_mode_pieces
        else r.cutline_uri,
        axis=1,
    )
    one_df["shape"] = one_df["shape_path"].apply(
        lambda p: Polygon(s3.read(p)[["column_1", "column_0"]].values)
    )
    one_df["force_clip"] = one_df["piece_code"].apply(lambda c: "MTRFXPNL" in c)
    one_df["buffer"] = one_df.apply(lambda r: 0 if r.force_clip else r.buffer, axis=1)
    logger.info(f"Transforming geometry")
    one_df["transformed_shape"] = one_df.apply(
        lambda r: transform_pieces_for_nesting(
            [r["shape"]],
            r.stretch_x,
            r.stretch_y,
            r.buffer,
            max_width,
            r.force_clip,
        )[0],
        axis=1,
    )
    # nest
    validate_shapes(one_df.transformed_shape, max_width, flag_asset=key)
    one_df["nestable_wkt"] = one_df["transformed_shape"].apply(dumps)
    nest = ProgressiveNode.from_pieces(
        one_df,
        max_width,
        max_vertical_strip_width=None
        if not block_fuse
        else BLOCK_FUSER_MAX_WIDTH_PX * one_df.stretch_x.max(),
    )
    nest.packing.compress(threads=1)
    # save info
    pieces_shapes = nest.packing.packed_shapes()
    pieces_order = nest.packing.piece_order()
    one_df["packed_shape"] = one_df["piece_id"].apply(lambda i: pieces_shapes[i])
    one_df["packing_order"] = one_df["piece_id"].apply(lambda i: pieces_order[i])
    one_df["geometry"] = one_df["shape"].apply(dumps)
    one_df["bounds"] = one_df["shape"].apply(lambda p: np.array(p.bounds))
    one_df["original_centroid"] = one_df["shape"].apply(lambda p: np.array(p.centroid))
    one_df["nested_centroid"] = one_df["packed_shape"].apply(
        lambda p: np.array(p.centroid)
    )
    one_df["nested_geometry"] = one_df["packed_shape"].apply(dumps)
    one_df["nested_bounds"] = one_df["packed_shape"].apply(lambda p: np.array(p.bounds))
    one_df["nested_area"] = one_df["packed_shape"].apply(lambda p: p.area)
    one_df["translation"] = one_df.nested_centroid - one_df.original_centroid
    one_df = one_df.join(
        pd.DataFrame(
            one_df["nested_bounds"].to_list(),
            columns=[
                "min_nested_x",
                "min_nested_y",
                "max_nested_x",
                "max_nested_y",
            ],
        )
    )
    one_df["composition_x"] = one_df.min_nested_x + one_df.buffer
    one_df["composition_y"] = one_df.max_nested_y - one_df.buffer
    one_df["nested_label_angle"] = one_df["packed_shape"].apply(piece_label_angle)
    one_df["piece_type"] = one_df.apply(
        lambda r: "block_buffer" if "block" in r.piece_id else r.piece_type, axis=1
    )
    one_df = one_df.drop(columns=["shape", "packed_shape", "transformed_shape"])
    one_df = _add_piece_info_to_df(one_df.reset_index(drop=True))
    fc._write_output(one_df, "nest", key, output_filename="output.feather")
    # dump to kafka
    height = one_df.max_nested_y.max() - one_df.min_nested_y.min()
    res.connectors.load("kafka")[OUTPUT_KAFKA_TOPIC].publish(
        {
            "piece_set_key": key,
            "piece_count": one_df.shape[0],
            "height_nest_px": height,
            "width_nest_px": max_width,
            "utilization": nest.utilization(),
            "material_code": msg["material_code"],
            "nest_file_path": fc.get_node_path_for_key("nest", key),
            "one_number": int(msg["one_number"]),
            "piece_type": msg["piece_type"],
            "created_at": res.utils.dates.utc_now_iso_string(),
            "pieces_created_at": msg["created_at"],
        },
        use_kgateway=True,
    )
    # plot the layout of the nest for looking at.
    width = 5
    height = int(width * height / max_width)
    plt.figure(figsize=(width, height))
    nest.packing.packing.plot()
    plt.savefig("nest.png", bbox_inches="tight")
    s3.upload("nest.png", fc.get_node_path_for_key("nest", key) + "/nest.png")
    # possibly construct a dxf
    if "cutline_uri" in one_df.columns:
        dxf_path = (
            fc.get_node_path_for_key("nest", key)
            + f"/{msg['one_number']}_{msg['material_code']}_{msg['piece_type']}.dxf"
        )
        if nest_to_dxf(one_df, dxf_path, one_number=msg["one_number"]):
            update_production_request(msg["one_number"], [dxf_path], False)
            s3.copy_to_make_one_production(
                msg["one_number"],
                dxf_path,
            )


def _move_shapes_to_nesting_solution(shapes, solution):
    return [
        translate(
            shapes[idx],
            *(
                box.local_offset
                + dxy
                + np.array(box.geom.centroid)
                - np.array(shapes[idx].centroid)
            ),
        )
        for idx, box, dxy in solution.boxes()
    ]


def _bbox_size(shape):
    x0, y0, x1, y1 = shape.bounds
    return x1 - x0, y1 - y0


def _remove_holes(poly):
    return Polygon(list(poly.exterior.coords))


def _handle_block_buffer_pieces(msg, one_df, fc):
    """
    For pieces that need to be block buffered we are going to do a bunch of stuff:
    1.  replace the actual printed pieces with one or more rectangular sections of a repeating pattern.
    2.  create one or more dxf files which
    """
    key = msg["id"]
    one_number = msg["one_number"]
    scale_x = one_df.iloc[0].stretch_x
    scale_y = one_df.iloc[0].stretch_y
    paper_scale_x = one_df.iloc[0].paper_marker_stretch_x
    paper_scale_y = one_df.iloc[0].paper_marker_stretch_y
    buffer_px = one_df.iloc[0].offset_size * 300
    material_width_px = one_df.iloc[0].cuttable_width * 300
    shapes_df = one_df[["piece_name", "cutline_uri"]].copy().reset_index(drop=True)
    shapes_df["geometry"] = shapes_df.cutline_uri.apply(
        lambda p: scale(Polygon(s3.read(p)[["column_1", "column_0"]].values), -1, 1)
    )
    pieces = list(shapes_df.geometry.values)
    s3_output_dir = fc.get_node_path_for_key("nest", key)
    # attempt to nest the thing into multiple bins so that they will each fit in the laser cutter.
    # note -- nesting with uncompensated pieces.
    multi_solution = None
    for buckets in range(3):
        try:
            multi_solution = pack_multi(
                [_remove_holes(n.buffer(75)) for n in pieces],
                material_width_px / scale_x - 2 * buffer_px,
                MAX_LASER_CUTTER_HEIGHT_PX,
                buckets,
            )
            break
        except:
            pass
    if multi_solution is None:
        logger.error(
            "Failed to nest one -- pieces wont fit in laser cutter.  Nesting old fashioned way"
        )
        _handle_nested_pieces(msg, one_df, fc)
    piece_groups = [
        _move_shapes_to_nesting_solution(pieces, solution)
        for solution in multi_solution
    ]
    # make bounding boxes for each block
    bboxes = [
        (
            int(material_width_px / scale_x),
            int(_bbox_size(unary_union([p.buffer(buffer_px) for p in g]))[1]),
        )
        for g in piece_groups
    ]
    # dump a picture of the nest
    aspect_ratio = sum(y for _, y in bboxes) / max(x for x, _ in bboxes)
    _, ax = plt.subplots(len(piece_groups), 1, figsize=(5, 1 + 5 * aspect_ratio))
    if len(piece_groups) == 1:
        ax = [ax]  # thanks py plot lib
    for i, p in enumerate(piece_groups):
        ax[i].set_title(f"{one_number} ({i+1})")
        for j, g in enumerate(p):
            ax[i].fill(*g.boundary.xy, color=(0.6, 0.6, 0.9))
            ax[i].plot(*g.boundary.xy, color=(0.3, 0.3, 0.45))
    plt.savefig("nest.png", bbox_inches="tight")
    s3.upload(
        "nest.png",
        s3_output_dir + "/cutlayout.png",
    )
    fusing = all(one_df.piece_name.apply(lambda n: n.endswith("-BF")))
    material_code = msg["material_code"]
    one_name = f"{one_number} {material_code}{' (block fuse)' if fusing else ''}"
    # make piece images.
    for i, (w, h) in enumerate(bboxes):
        filename = f"block_{i}.png"
        image = make_bbox_image(
            w,
            h,
            one_df.iloc[0].artwork_uri,
            f"{one_number}_{material_code}{'_block_fuse' if fusing else ''}_block_{i+1}.dxf",
        )
        cv2.imwrite(filename, image)
        s3.upload(
            filename,
            s3_output_dir + "/" + filename,
        )
        cv2.imwrite(
            f"block_{i}_small.png",
            cv2.resize(image, (int(image.shape[1] / 5), int(image.shape[0] / 5))),
        )
        s3.upload(
            f"block_{i}_small.png",
            s3_output_dir + f"/block_{i}_small.png",
        )
        upload_rectangle_piece(
            s3_output_dir + f"/block_{i}.feather",
            image.shape[0],
            image.shape[1],
        )
    # make DXFs
    dxf_paths = []
    for i, g in enumerate(piece_groups):
        dxf_file_path = f"{s3_output_dir}/{one_number}_{material_code}{'_block_fuse' if fusing else ''}_block_{i+1}.dxf"
        DxfFile.export_dataframe(
            pd.DataFrame.from_dict(
                {
                    "geometry": [
                        pixel_space_to_dxf(p, paper_scale_x, paper_scale_y) for p in g
                    ]
                }
            ),
            dxf_file_path,
            point_conversion_op=lambda p: p.boundary.coords,
        )
        dxf_paths.append(dxf_file_path)
    update_production_request(msg["one_number"], dxf_paths, True)
    for p in dxf_paths:
        s3.copy_to_make_one_production(
            one_number,
            p,
        )
    # now just act as if we have these big rectangle pieces and just nest them
    nest_msg = dict(msg)
    nest_msg["make_pieces"] = [
        {
            "asset_key": one_df.asset_key.iloc[0],
            "asset_id": one_df.asset_id.iloc[0],
            "piece_name": f"block_{i}",
            "piece_code": f"block_{i}",
            "piece_id": res.utils.res_hash(
                f"{one_df.asset_id.iloc[0]}block_{i}".encode()
            ),
            "filename": f"{s3_output_dir}/block_{i}.png",
            "outline_uri": f"{s3_output_dir}/block_{i}.feather",
            "wrapped_piece_ids": ",".join(
                one_df.piece_id[idx] for idx, _, _ in multi_solution[i].boxes()
            ),
            "wrapped_piece_codes": ",".join(
                one_df.piece_name[idx] for idx, _, _ in multi_solution[i].boxes()
            )
            # no cutline uri since we dont want to overwrite the dxf info.
        }
        for i, g in enumerate(piece_groups)
    ]
    _handle_nested_pieces(nest_msg, _kafka_msg_to_df(nest_msg), fc)
    # add_blocks_to_healing_app(
    #     msg["id"],
    #     msg["one_number"],
    #     msg["material_code"],
    #     [s3_output_dir + f"/block_{i}.png" for i in range(len(bboxes))],
    # )


def _uniform_mode_placement(body_code, color_code):
    if body_code == "CC-3068" and color_code.startswith("RESU"):
        return ["PLSFTPNL-S"]
    return []


@flow_node_attributes("pack_one.handler", memory="16G")
def handler(event, context):
    with FlowContext(event, context) as fc:
        # make a dataframe about the pieces of the one
        msg = fc.assets[0]
        one_df = _kafka_msg_to_df(msg)
        uniform_mode_pieces = _uniform_mode_placement(
            one_df.body_code[0], one_df.color_code[0]
        )
        needs_block_buffer = (
            one_df.shape[0] > 1
            and "requires_block_buffer" in one_df.columns
            and "artwork_uri" in one_df.columns
            and all(one_df.requires_block_buffer.values)
            and all(
                one_df.artwork_uri.apply(
                    lambda x: x is not None
                    and isinstance(x, str)
                    and x.lower().startswith("s3://")
                )
            )
            and len(one_df.artwork_uri.unique()) == 1
            and len(uniform_mode_pieces) == 0
        )
        if needs_block_buffer:
            logger.info("Doing block buffering plan.")
            _handle_block_buffer_pieces(msg, one_df, fc)
        else:
            _handle_nested_pieces(msg, one_df, fc, uniform_mode_pieces)


@flow_node_attributes("pack_one.reducer")
def reducer(event, context={}):
    return {}
