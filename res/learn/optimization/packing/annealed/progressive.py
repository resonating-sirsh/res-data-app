import time
import res
import random
import pandas as pd
import numpy as np
from collections import OrderedDict
from enum import Enum
from queue import PriorityQueue
from .packing_geometry import choose_geom, divider
from .packing import Packing, pack_boxes_annealed
from res.flows.make.nest.utils import write_text_label_to_s3
from res.utils import logger
from multiprocessing import Pool
from shapely.geometry import box
from shapely.wkt import dumps as shapely_dumps, loads as shapely_loads
from .nn import load_latest_model

threadpool = None

PX_PER_INCH = 300  # aka DPI
PX_PER_YARD = PX_PER_INCH * 36

# for now -- prevent using any nest where the utilization is lower than some threshold.
UTILIZATION_MINIMUM = 0.0

# how many times we will nest over the length of the roll before giving up
MAX_BACKTRACKS = 25

# how close we need to get to filling up the whole roll to constitute a valid solition
DEFAULT_SLOP_FRAC = 0.01

# so we dont dilly dally trying to get within 3" on a 10 yard roll, allow for a foot of slop
# irrespective of the slop fraction.  Note - this only has an effect for rolls less than
# 1/slop_frac feet long - by default this is only rolls < 100 feet = 33 yards.
DEFAULT_MIN_SLOP_PX = 1 * 36 * PX_PER_INCH
DEFAULT_MAX_SLOP_PX = 1 * 36 * PX_PER_INCH

# how many unnested pieces we need in order to trigger nesting
DEFAULT_PACKING_LIMIT = 50
BLOCK_FUSE_PACKING_LIMIT = 75
MIN_NEST_PIECES = 20

# roll filling parameters
ROLL_ADVANCE_INTER_PX = 0.25 * PX_PER_YARD
ROLL_ADVANCE_START_PX = 2.0 * PX_PER_YARD
DISTORTION_GRIDS_PER_ROLL = 3
# these are also used by stitching to know whats going on.
HEADER_HEIGHT_INCHES_UNCOMPENSATED = 7.5
SEPERATOR_HEIGHT_INCHES_UNCOMPENSATED = 3
DISTORTION_GRID_HEIGHT_PX_UNCOMPENSATED = (
    HEADER_HEIGHT_INCHES_UNCOMPENSATED * PX_PER_INCH
)
SEPERATOR_HEIGHT_PX_UNCOMPENSATED = SEPERATOR_HEIGHT_INCHES_UNCOMPENSATED * PX_PER_INCH
MAX_STITCHED_LENGTH_PX = 30 * PX_PER_YARD
LASER_CUTTER_MAX_LENGTH_PX = 88 * 300

QR_CODE_SIZE = 5 * PX_PER_INCH
QR_CODE_GEOM = box(0, 0, QR_CODE_SIZE, QR_CODE_SIZE)
QR_CODE_IMAGE_PATH_PLACEHOLDER = "TODO_QR"
LABEL_HEIGHT = 300
LABEL_WIDTH = 1800
BLOCK_FUSE_LABEL_GEOM = box(0, 0, LABEL_WIDTH, LABEL_HEIGHT)

s3 = res.connectors.load("s3")


def _px_to_yards(px):
    return float(px) / PX_PER_YARD


def _yards_to_px(yards):
    return int(yards * PX_PER_YARD)


def get_threadpool():
    global threadpool
    if threadpool is None:
        threadpool = Pool(8)
    return threadpool


# A node in a tree-structured nesting where the leaves corespond to nested ones, and interior nodes correspond
# to multi-one nests.
class ProgressiveNode(object):
    def __init__(
        self,
        node_id=None,
        packing=None,
        children=None,
        mergeable=True,
    ):
        self.node_id = node_id
        self.packing = packing
        self.children = children or []
        self.mergeable = mergeable
        self.name = None

    @staticmethod
    def from_pieces(
        piece_df,
        max_width,
        node_id=None,
        mergeable=True,
        max_vertical_strip_width=None,
        margin_x=0,
        margin_y=0,
    ):
        packing = ProgressivePacking(
            max_width,
            max_vertical_strip_width=max_vertical_strip_width,
            margin_x=margin_x,
            margin_y=margin_y,
        )
        for _, row in piece_df.iterrows():
            packing.add_piece(row)
        return ProgressiveNode(
            node_id=node_id,
            packing=packing,
            mergeable=mergeable,
        )

    @staticmethod
    def from_nodes(
        nodes,
        node_id=None,
        compress=True,
        compress_threads=None,
        compress_randomize=False,
        compress_iters=3000,
        max_vertical_strip_width=None,
        max_guillotinable_length=None,
        guillotine_spacer=None,
    ):
        widths = [n.packing.max_width for n in nodes]
        assert max(widths) == min(widths)
        margins_x = [n.packing.margin_x for n in nodes]
        assert max(margins_x) == min(margins_x)
        margins_y = [n.packing.margin_y for n in nodes]
        assert max(margins_y) == min(margins_y)
        max_width = widths[0]
        mergeable = all(n.mergeable for n in nodes)
        packing = ProgressivePacking(
            max_width,
            max_vertical_strip_width=max_vertical_strip_width,
            max_guillotinable_length=max_guillotinable_length,
            guillotine_spacer=guillotine_spacer,
            margin_x=margins_x[0],
            margin_y=margins_y[0],
        )
        for n in nodes:
            packing._add_all(n.packing)
        if compress:
            assert mergeable
            packing.compress(
                threads=compress_threads
                if compress_threads is not None
                else get_threadpool(),
                random_init=compress_randomize,
                iters=compress_iters,
            )
        return ProgressiveNode(
            node_id=node_id,
            packing=packing,
            children=nodes,
            mergeable=mergeable,
        )

    @staticmethod
    def concat(nodes, max_guillotinable_length=None, guillotine_spacer=None):
        original = ProgressiveNode.from_nodes(
            nodes,
            compress=False,
            max_guillotinable_length=max_guillotinable_length,
            guillotine_spacer=guillotine_spacer,
        )
        compressed = ProgressiveNode.from_nodes(
            nodes,
            max_guillotinable_length=max_guillotinable_length,
            guillotine_spacer=guillotine_spacer,
        )
        if (
            max_guillotinable_length is not None
            or compressed.utilization() > original.utilization()
        ):
            return compressed
        else:
            logger.info("Failed to improve packing")
            return original

    def covered_node_ids(self):
        if self.node_id is not None:
            yield self.node_id
        for c in self.children:
            yield from c.covered_node_ids()

    def leaves(self):
        if self.children is None or len(self.children) == 0:
            yield self
        else:
            for child in self.children:
                yield from child.leaves()

    def utilization(self):
        return self.packing.packing.utilization()

    def height(self):
        return self.packing.height()

    def plot(self):
        from matplotlib import pyplot as plt

        plt.figure(figsize=(5, 5 * (self.packing.height() / self.packing.max_width)))
        self.packing.packing.plot()
        plt.show()

    def rename(self, solution_name, roll_name, roll_index):
        self.name = f"{solution_name}_{roll_name}_{roll_index}"

    def contains_piece_id(self, piece_id):
        return piece_id in self.packing.piece_ids


# wrapper around nesting operations to let us build up a nest incrementally
class ProgressivePacking(object):
    def __init__(
        self,
        max_width,
        max_vertical_strip_width=None,
        max_guillotinable_length=None,
        guillotine_spacer=None,
        margin_x=0,
        margin_y=0,
    ):
        self.boxes = []
        self.piece_ids = []
        self.piece_df = pd.DataFrame()
        self.max_width = max_width
        self.packing = Packing(
            max_width=max_width, margin_x=margin_x, margin_y=margin_y
        )
        self.n = 0
        self.max_vertical_strip_width = max_vertical_strip_width
        self.max_guillotinable_length = max_guillotinable_length
        self.guillotine_spacer = guillotine_spacer
        self.margin_x = margin_x
        self.margin_y = margin_y

    def height(self):
        return self.packing.height

    def utilization(self):
        return self.packing.utilization()

    def _order(self):
        # packing may add some shapes like spacers etc so ignore those.
        return list(reversed([idx for idx, _, _ in self.packing.boxes()]))

    def compress(self, threads=None, random_init=False, iters=3000):
        logger.info(
            f"Compressing {len(self.boxes)} pieces, with height {int(self.packing.height)} and utilization {self.packing.utilization():.3} (max vert: {self.max_vertical_strip_width}, max guill: {self.max_guillotinable_length}, margins: {self.margin_x, self.margin_y})"
        )
        if (
            self.max_vertical_strip_width is None
            and self.max_guillotinable_length is None
            and self.margin_x == 0
            and self.margin_y == 0
        ):
            logger.info("Nesting using NN model")
            self.packing = load_latest_model().nest_boxes(self.boxes, self.max_width)
            logger.info(
                f"NN compressed {len(self._order())} pieces to height {int(self.packing.height)} and utilization {self.packing.utilization():.3}"
            )
            # for now just to be sure - follow up with 1k annealing iters.
            self.packing = pack_boxes_annealed(
                self.boxes,
                self.max_width,
                None,
                None,
                None,
                1000,
                threads if threads is not None else get_threadpool(),
                initial_order=self._order(),
                log_progress=False,
            )
        else:
            if random_init:
                self.packing = pack_boxes_annealed(
                    self.boxes,
                    self.max_width,
                    self.max_vertical_strip_width,
                    self.max_guillotinable_length,
                    self.guillotine_spacer,
                    iters,
                    threads if threads is not None else get_threadpool(),
                    log_progress=False,
                    margin_x=self.margin_x,
                    margin_y=self.margin_y,
                )
            while True:
                current_height = self.height()
                self.packing = pack_boxes_annealed(
                    self.boxes,
                    self.max_width,
                    self.max_vertical_strip_width,
                    self.max_guillotinable_length,
                    self.guillotine_spacer,
                    iters,
                    threads if threads is not None else get_threadpool(),
                    initial_order=self._order(),
                    log_progress=False,
                    margin_x=self.margin_x,
                    margin_y=self.margin_y,
                )
                if current_height - self.height() < current_height * 0.01:
                    break
        logger.info(
            f"Compressed {len(self._order())} pieces to height {int(self.packing.height)} and utilization {self.packing.utilization():.3}"
        )

    def packed_shapes(self):
        shapes = OrderedDict()
        spacers = 0
        for idx, b, t in reversed(list(self.packing.boxes(filter_hidden_boxes=False))):
            geom = b.translated(t)
            if idx >= 0:
                shapes[self.piece_ids[idx]] = geom
            elif b.type_name == "guillotine_spacer":
                shapes[f"guillotine_spacer_{spacers}"] = geom
                spacers += 1
            else:
                logger.info(f"Ignoring unknown piece {idx} {b.type_name} at {t}")
        return shapes

    def guillotined_sections(self):
        return 1 + len(
            [
                idx
                for idx, b, t in self.packing.boxes(filter_hidden_boxes=False)
                if b.type_name == "guillotine_spacer"
            ]
        )

    def nested_df(self, material_prop):
        pieces_shapes = self.packed_shapes()
        piece_order = {k: n for n, k in enumerate(pieces_shapes)}
        df = self.piece_df.copy()
        for k, g in pieces_shapes.items():
            if k.startswith("guillotine_spacer"):
                df = df.append(
                    {
                        "piece_id": k,
                        "nestable_wkt": shapely_dumps(g),
                    },
                    ignore_index=True,
                )
        if material_prop is not None:
            df = material_prop.add_props(df)
        for _, r in df.iterrows():
            if r["piece_id"] not in pieces_shapes:
                logger.info(f"Nest missing piece for df row: {r}")
                logger.info(f"Piece ids: {self.piece_ids}")
                logger.info(
                    f"Box info: {reversed(list(self.packing.boxes(filter_hidden_boxes=False)))}"
                )
        df["packed_shape"] = df["piece_id"].apply(lambda i: pieces_shapes[i])
        df["nesting_order"] = df["piece_id"].apply(lambda i: piece_order[i])
        df["nested_centroid"] = df["packed_shape"].apply(lambda p: np.array(p.centroid))
        df["nested_geometry"] = df["packed_shape"].apply(shapely_dumps)
        df["nested_bounds"] = df["packed_shape"].apply(lambda p: np.array(p.bounds))
        df["nested_area"] = df["packed_shape"].apply(lambda p: p.area)
        df["total_nest_height"] = int(self.height())
        df = df.join(
            pd.DataFrame(
                df["nested_bounds"].to_list(),
                columns=[
                    "min_nested_x",
                    "min_nested_y",
                    "max_nested_x",
                    "max_nested_y",
                ],
            )
        )
        df["composition_x"] = df.min_nested_x + df.buffer
        df["composition_y"] = df.max_nested_y - df.buffer
        df = df.drop(columns=["packed_shape"])
        return df.sort_values("nesting_order").reset_index(drop=True)

    def validate(self):
        # make sure we didnt duplicate pieces somehow.
        if any(self.piece_df.duplicated(subset=["piece_id"]).values):
            logger.error(
                f"Duplicated piece ids {self.piece_df.piece_id.value_counts().loc[lambda x: x > 1]}"
            )
            return False
        # just be sure that the nesting is in fact valid.
        packed_shapes = self.packed_shapes()
        packed = list(packed_shapes.items())
        shapes = [
            s.buffer(-1) for _, s in packed
        ]  # buffer -1 to just deal with 1 pixel rounding errors.
        for i, s in enumerate(shapes):
            x0, y0, x1, _ = s.bounds
            if x0 < 0 or y0 < 0 or x1 > self.max_width:
                logger.error(
                    f"Shape with id {packed[i][0]} is out of bounds with {s.bounds}"
                )
                return False
            for j in range(i + 1, len(shapes)):
                area = s.intersection(shapes[j]).area
                if area > 0:
                    logger.error(
                        f"Shape with id {packed[i][0]} intersects shape with id {packed[j][0]} with area {area}"
                    )
                    return False
        return True

    def piece_order(self):
        return {self.piece_ids[idx]: i for i, idx in enumerate(self._order())}

    def add_piece(self, piece_record):
        box = choose_geom(
            shapely_loads(piece_record.nestable_wkt), max_width=self.max_width
        )
        if box.width > self.max_width:
            raise ValueError(
                f"{piece_record.piece_code} of body {piece_record.body_code} has width {box.width} too large for material {self.max_width}"
            )
        self._add_box(piece_record.piece_id, box)
        self.piece_df = self.piece_df.append(piece_record, ignore_index=True)

    def _add_box(self, id, box):
        self.boxes.append(box)
        self.piece_ids.append(id)
        self.packing = self.packing.add(box, self.n)
        self.n += 1

    def _add_all(self, other):
        self.boxes.extend(other.boxes)
        self.piece_ids.extend(other.piece_ids)
        self.piece_df = self.piece_df.append(other.piece_df, ignore_index=True)
        for box in other.boxes:
            self.packing = self.packing.add(box, self.n)
            self.n += 1

    def with_pieces(self, piece_df):
        p = ProgressivePacking(self.max_width)
        combined_df = pd.Concat([self.piece_df, piece_df])
        for _, row in combined_df.iterrows():
            p.add_piece(row)
        return p


def nest_th_3000(nodes, material_prop):
    """
    Special nesting so we can have smocking in a fancy way.
    """
    all_pieces_df = pd.concat(n.packing.piece_df for n in nodes).reset_index(drop=True)
    p = ProgressiveNode(packing=ProgressivePacking(material_prop.max_width_px))
    smock_pieces = all_pieces_df[
        all_pieces_df.piece_code.apply(lambda c: "TOPBYPNL" in c)
    ].reset_index(drop=True)
    strap_pieces = all_pieces_df[
        all_pieces_df.piece_code.apply(lambda c: "TOPBYPNL" not in c)
    ].reset_index(drop=True)
    extra_pieces = smock_pieces.sample((3 - (smock_pieces.shape[0] % 3)) % 3)
    extra_pieces["piece_id"] = extra_pieces["piece_id"].apply(
        lambda c: c + "::duplicate"
    )
    all_smock_pieces = pd.concat([smock_pieces, extra_pieces])
    i = 0
    for _, r in all_smock_pieces.iterrows():
        if i % 3 == 0 and i > 0:
            p.packing.packing = p.packing.packing._add_translated(
                divider(material_prop.max_width_px),
                -1,
                [0, p.packing.packing.height + 150],
                record_box=False,
            )
        p.packing.add_piece(r)
        i += 1

    q = Packing(max_width=p.packing.packing.max_width)
    for i, b, t in reversed(list(p.packing.packing.boxes())):
        q = q._add_translated(b, i, t)
    p.packing.packing = q

    for _, r in strap_pieces.iterrows():
        p.packing.add_piece(r)

    strap_box = next(p.packing.packing.boxes())[1]

    while True:
        p0 = p.packing.packing
        p1 = p0.add(strap_box, -1)
        if p1.utilization() >= p0.utilization():
            for _, r in strap_pieces.sample(1).iterrows():
                r["piece_id"] = r["piece_id"] + "::duplicate"
                p.packing.add_piece(r)
        else:
            break

    return p


# A collection of progressive nodes that represents the subset of a one that we want to print in this material.
class OneNode:
    def __init__(self, value=0, nesting_sku=None, node_map=None):
        self.value = value
        self.nesting_sku = nesting_sku
        self.node_map = node_map if node_map is not None else {}

    def add_node(self, nest_type, node, value=0, compress=True):
        self.value += value
        if nest_type not in self.node_map:
            self.node_map[nest_type] = node
        else:
            self.node_map[nest_type] = ProgressiveNode.from_nodes(
                [self.node_map[nest_type], node],
                compress=compress,
            )

    def covered_node_ids(self):
        return [id for node in self.node_map.values() for id in node.covered_node_ids()]

    def mergeable(self):
        return all(p.mergeable for p in self.node_map.values())

    def max_piece_width(self):
        return max(
            max(b.width / 300 for _, b, _ in n.packing.packing.boxes())
            for n in self.node_map.values()
        )

    def total_height(self):
        return sum(p.height() for p in self.node_map.values())

    def total_area(self):
        return sum(p.height() * p.utilization() for p in self.node_map.values())


# Preserve nesting data betweeen successive runs of the job - so that we dont repeat work unnecessarily.
class NestCache:
    def __init__(
        self, cache, material, block_fuser_width_px_compensated, label_assets_path
    ):
        self.nest_cache = cache
        self.material = material
        self.block_fuser_width_px_compensated = block_fuser_width_px_compensated
        self.label_assets_path = label_assets_path

    def cached_concat(self, nodes, nest_type):
        key = (
            nest_type
            + ":"
            + ",".join(sorted([i for n in nodes for i in list(n.covered_node_ids())]))
        )
        if key not in self.nest_cache:
            logger.debug(f"making nest for key {key}")
            if "TH-3000" in nest_type:
                # special smocking thing.
                self.nest_cache[key] = nest_th_3000(
                    nodes,
                    self.material,
                )
            elif "block_fuse" in nest_type:
                # do something fancy for the block fuser so that we dont try to anneal the order with labels
                # todo == let the annealing alg treat all the labels as one element in the ordering.
                leaves = [n for v in nodes for n in v.leaves()]
                labels = [n for n in leaves if n.packing.n >= 50]
                # hack to try to sort labels by size so that like-labels are adjacent in the order.
                labels = [
                    n
                    for _, _, n in sorted(
                        [
                            (
                                sum(s.area for s in n.packing.packed_shapes().values()),
                                random.random(),
                                n,
                            )
                            for n in labels
                        ]
                    )
                ]
                non_labels = [n for n in leaves if n.packing.n < 50]

                nests = []
                if len(non_labels) > 0:
                    nests.append(
                        ProgressiveNode.from_nodes(
                            self.label_nest(
                                non_labels, nest_type, ["qr_botton"], ["bf_bottom"]
                            ),
                            max_vertical_strip_width=self.block_fuser_width_px_compensated,
                            max_guillotinable_length=self.material.max_guillotinable_length,
                            guillotine_spacer=self.material.guillotine_spacer,
                            compress_iters=2000,
                            compress=len(labels) > 0,
                        )
                    )
                if len(labels) > 0:
                    nests.append(
                        ProgressiveNode.from_nodes(
                            labels
                            if len(non_labels) > 0
                            else self.label_nest(
                                labels,
                                nest_type,
                                ["qr_botton"],
                                ["bf_bottom"],
                            ),
                            max_vertical_strip_width=self.block_fuser_width_px_compensated,
                            max_guillotinable_length=self.material.max_guillotinable_length,
                            guillotine_spacer=self.material.guillotine_spacer,
                            compress=False,
                        )
                    )

                nests = self.label_nest(nests, nest_type, ["qr_top"], ["bf_top"])
                combined_nest = ProgressiveNode.from_nodes(
                    nests,
                    max_vertical_strip_width=self.block_fuser_width_px_compensated,
                    max_guillotinable_length=self.material.max_guillotinable_length,
                    guillotine_spacer=self.material.guillotine_spacer,
                    compress_iters=500,
                )
                # sometimes the presence of over-width pieces totally screws the nesting of fusing pieces
                # so in this case see if we can just nest normally and it will be up to the cutters to figure out
                # how to deal with the resulting nest.
                if (
                    "uniform" not in nest_type
                    and len(labels) == 0
                    and combined_nest.utilization() < 0.5
                ):
                    combined_nest_no_gap = ProgressiveNode.from_nodes(
                        nests,
                        compress=True,
                        compress_iters=2000,
                    )
                    if (
                        combined_nest_no_gap.utilization()
                        > combined_nest.utilization() * 1.2
                    ):
                        combined_nest = combined_nest_no_gap
                self.nest_cache[key] = combined_nest
            else:
                nodes = self.label_nest(nodes, nest_type, ["qr_top", "qr_bottom"], [])
                self.nest_cache[key] = ProgressiveNode.concat(
                    nodes,
                    max_guillotinable_length=self.material.max_guillotinable_length,
                    guillotine_spacer=self.material.guillotine_spacer,
                )
        self.nest_cache.move_to_end(key, last=False)
        logger.debug(f"loading nest for key {key}")
        return self.nest_cache[key]

    def keep_mru(self, max_entries=5000):
        while len(self.nest_cache) > max_entries:
            self.nest_cache.popitem(last=True)

    def label_pieces(self, image_path, geom, piece_prefix):
        return pd.DataFrame(
            {
                "piece_id": [f"{piece_prefix}_bottom", f"{piece_prefix}_top"],
                "piece_code": [f"{piece_prefix}_bottom", f"{piece_prefix}_top"],
                "piece_group": ["anchor_bottom", "anchor_top"],
                "s3_image_path": [image_path] * 2,
                "nestable_wkt": [
                    shapely_dumps(self.material.transform_for_nesting(geom))
                ]
                * 2,
            }
        )

    def label_nest(self, nodes, nest_type, qr_types, bf_types):
        if "uniform" in nest_type:
            return nodes
        else:
            for qr_type in qr_types:
                nodes = self.ensure_qr_code(nodes, qr_type)
            for bf_type in bf_types:
                nodes = self.ensure_label(nodes, nest_type, bf_type)
            return nodes

    def ensure_qr_code(self, nodes, qr_piece_id):
        nodes_with_qr = [n for n in nodes if n.contains_piece_id(qr_piece_id)]
        if len(nodes_with_qr) == 1:
            return nodes
        elif len(nodes_with_qr) == 0:
            qr_pieces = self.label_pieces(
                QR_CODE_IMAGE_PATH_PLACEHOLDER, QR_CODE_GEOM, "qr"
            )
            return self._insert_labels(nodes, qr_piece_id, qr_pieces)
        else:
            raise ValueError(f"Multiple nodes with qr pieces being concatenated")

    def ensure_label(self, nodes, label_text, label_piece_id):
        nodes_with_bf = [n for n in nodes if n.contains_piece_id(label_piece_id)]
        if len(nodes_with_bf) == 1:
            return nodes
        elif len(nodes_with_bf) == 0:
            bf_pieces = self.label_pieces(
                self.label_s3_path(label_text), BLOCK_FUSE_LABEL_GEOM, "bf"
            )
            return self._insert_labels(nodes, label_piece_id, bf_pieces)
        else:
            raise ValueError(f"Multiple nodes with qr pieces being concatenated")

    def _insert_labels(self, nodes, piece_id, label_pieces):
        n = ProgressiveNode.from_pieces(
            label_pieces[label_pieces.piece_id == piece_id],
            self.material.max_width_px,
        )
        if piece_id.endswith("_top"):
            nodes[-1] = ProgressiveNode.from_nodes([nodes[-1], n], compress=False)
        if piece_id.endswith("_bottom"):
            nodes[0] = ProgressiveNode.from_nodes([n, nodes[0]], compress=False)
        return nodes

    def label_s3_path(self, label_text):
        path = self.label_assets_path + "/" + label_text + ".png"
        if not s3.exists(path):
            write_text_label_to_s3(label_text, LABEL_HEIGHT, LABEL_WIDTH, path)
        return path

    @staticmethod
    def s3_path(base_path, material):
        return f"{base_path}/{material.material_code}.pickle"

    @staticmethod
    def load(
        material,
        valid_ids,
        block_fuser_width_px_compensated,
        label_assets_path,
        base_path=f"s3://{res.RES_DATA_BUCKET}/progressive_nests/cache",
    ):
        cache = OrderedDict()
        valid_id_set = set(valid_ids)
        try:
            cache = s3.read(NestCache.s3_path(base_path, material))
            logger.info(f"Loaded nest cache with {len(cache)} elements")
            bad_keys = []
            for k, v in cache.items():
                if any(i not in valid_id_set for i in v.covered_node_ids()):
                    bad_keys.append(k)
            for k in bad_keys:
                del cache[k]
            logger.info(f"After filtering, nest cache contains {len(cache)} elements")
        except Exception as ex:
            logger.warn(f"Failed to read nest cache {repr(ex)}")
        return NestCache(
            cache, material, block_fuser_width_px_compensated, label_assets_path
        )

    def save(self, base_path=f"s3://{res.RES_DATA_BUCKET}/progressive_nests/cache"):
        try:
            self.keep_mru()
            s3.write(NestCache.s3_path(base_path, self.material), self.nest_cache)
        except Exception as ex:
            logger.warn(
                f"Failed to write nest cache for material {self.material}: {repr(ex)}"
            )


# possibly partial solution for a roll:
# consists of a set of nests which are "closed" meaning we reached the limit on pieces we are willing to include
# and a set of nests which are "unnested" meaning that before emitting the solution we need to merge them and re-nest the pieces.
# we incrementally add nests to the unnested set and eventually we do this flush() operation to compress them into one nest.
class NestGroup:
    def __init__(
        self, nested=[], unnested=[], pieces_to_nest=0, unnested_length=0, value=0
    ):
        self.nested = nested
        self.unnested = unnested
        self.pieces_to_nest = pieces_to_nest
        self.unnested_length = unnested_length
        self.asset_count = len(list(self.contained_node_ids()))
        self.value = value
        self.nested_solution = self.nested + (
            [] if len(self.unnested) != 1 else self.unnested
        )

    def contained_node_ids(self):
        for n in self.nested:
            yield from n.covered_node_ids()
        for n in self.unnested:
            yield from n.covered_node_ids()

    def add_node(self, node, node_value):
        if node.mergeable:
            return NestGroup(
                self.nested,
                self.unnested + [node],
                self.pieces_to_nest + node.packing.n,
                self.unnested_length + node.height(),
                self.value + node_value,
            )
        else:
            return NestGroup(
                self.nested + [node],
                self.unnested,
                self.pieces_to_nest,
                self.unnested_length,
                self.value + node_value,
            )

    def finalize(self):
        assert len(self.unnested) <= 1
        return NestGroup(
            self.nested_solution,
            [],
            0,
            0,
            self.value,
        )

    def flush(self, nest_cache, packing_limit, nest_type, force):
        if len(self.unnested) == 0 or (
            force == False and self.pieces_to_nest < packing_limit
        ):
            return self
        # make sure that on the final packing we dont leave any tiny nests left over.
        if (
            force
            and self.pieces_to_nest < MIN_NEST_PIECES
            and len(self.nested) > 0
            and self.nested[-1].mergeable
        ):
            return NestGroup(
                self.nested[:-1]
                + [
                    nest_cache.cached_concat(
                        self.unnested + [self.nested[-1]], nest_type
                    )
                ],
                [],
                0,
                0,
                self.value,
            )
        new_nest = nest_cache.cached_concat(self.unnested, nest_type)
        if new_nest.packing.n >= packing_limit:
            return NestGroup(
                self.nested + [new_nest],
                [],
                0,
                0,
                self.value,
            )
        else:
            return NestGroup(
                self.nested,
                [new_nest],
                new_nest.packing.n,
                new_nest.height(),
                self.value,
            )


def _packing_limit_for_nest_type(nest_type):
    if nest_type.startswith("block_fuse"):
        return BLOCK_FUSE_PACKING_LIMIT
    else:
        return DEFAULT_PACKING_LIMIT


class Solution:
    def __init__(self, material, roll_length, nest_groups={}, additional_reserved_px=0):
        self.material = material
        self.roll_length = roll_length
        self.nest_groups = nest_groups
        self.asset_count = sum(n.asset_count for n in nest_groups.values())
        self.value = sum(n.value for n in nest_groups.values())
        self.additional_reserved_px = additional_reserved_px
        self.nested_required_length = self.compute_nested_required_length()
        self.slop = roll_length - self.nested_required_length
        self.nested_utilization = (
            sum(
                n.height() * n.utilization()
                for g in nest_groups.values()
                for n in g.nested
            )
            / self.nested_required_length
        )
        self.nested_count = sum(len(n.nested) for n in nest_groups.values())
        self.unnested_count = max(
            (len(n.unnested) for n in nest_groups.values()), default=0
        )
        self.unnested_length = sum(n.unnested_length for n in nest_groups.values())
        self.unnested_area = sum(
            n.utilization() * n.height()
            for g in self.nest_groups.values()
            for n in g.unnested
            if len(g.unnested) > 1
        )
        self.needs_flush = any(
            n.pieces_to_nest >= _packing_limit_for_nest_type(k)
            for k, n in nest_groups.items()
        )

    def add_one_node(self, one_node):
        updated_groups = self.nest_groups.copy()
        subnode_value = one_node.value / len(one_node.node_map)
        for group, node in one_node.node_map.items():
            if group not in updated_groups:
                updated_groups[group] = NestGroup()
            updated_groups[group] = updated_groups[group].add_node(node, subnode_value)
        return Solution(self.material, self.roll_length, updated_groups)

    def flush(self, nest_cache, force=False):
        return Solution(
            self.material,
            self.roll_length,
            {
                k: v.flush(nest_cache, _packing_limit_for_nest_type(k), k, force)
                for k, v in self.nest_groups.items()
            },
        )

    def try_reserve_extra(self, reserve_px):
        return Solution(
            self.material,
            self.roll_length,
            self.nest_groups,
            int(min(reserve_px, self.roll_length - self.nested_required_length)),
        )

    def __iter__(self):
        for _, g in self.nest_groups.items():
            for n in g.nested:
                yield n

    def labeled_nests(self):
        for l, g in self.nest_groups.items():
            for n in g.nested:
                yield l, n

    def finalize(self):
        return Solution(
            self.material,
            self.roll_length,
            {k: v.finalize() for k, v in self.nest_groups.items()},
        )

    def contained_node_ids(self):
        for _, g in self.nest_groups.items():
            yield from g.contained_node_ids()

    def compute_nested_required_length(self):
        distortion_grids = min(
            DISTORTION_GRIDS_PER_ROLL,
            sum(len(g.nested_solution) for _, g in self.nest_groups.items()),
        )
        total_length = (
            ROLL_ADVANCE_START_PX
            + distortion_grids * self.material.distortion_grid_height_px
            + self.additional_reserved_px
        )
        curr_section = 0
        num_sections = 0
        for _, g in self.nest_groups.items():
            for n in g.nested_solution:
                if curr_section + n.height() > MAX_STITCHED_LENGTH_PX:
                    total_length += curr_section + ROLL_ADVANCE_INTER_PX
                    curr_section = n.height()
                    num_sections += 1
                else:
                    curr_section += n.height() + (
                        self.material.seperator_height_px if curr_section > 0 else 0
                    )
        if curr_section > 0:
            num_sections += 1
            total_length += curr_section
        if num_sections < distortion_grids:
            # we allocated too many up-front distortion grids.  Instead we need to replace some spacers with them.
            total_length -= (
                distortion_grids - num_sections
            ) * self.material.distortion_grid_height_px
            total_length += (distortion_grids - num_sections) * (
                self.material.distortion_grid_height_px
                - self.material.seperator_height_px
            )
        return int(total_length)

    def __lt__(self, other):
        return self.value > other.value

    def __hash__(self):
        return hash("/".join(sorted(list(self.contained_node_ids()))))

    def __eq__(self, other):
        sn = list(self)
        on = list(other)
        if len(sn) != len(on):
            return False
        for n, m in zip(sn, on):
            if set(n.covered_node_ids()) != set(m.covered_node_ids()):
                return False
        return set(self.contained_node_ids()) == set(other.contained_node_ids())


class SolutionState(Enum):
    VALID = 0
    INSUFFICIENT_ASSET_LENGTH = 1
    NO_ROLLS = 2
    TIME_LIMIT_EXCEEDED = 3
    INFEASIBLE = 4
    NO_ASSETS = 5
    NESTING_DISABLED = 6

    def __lt__(self, other):
        if type(other) != SolutionState:
            raise ValueError("wtf are you doing")
        return self.value < other.value


def nest_to_length(
    ones_nodes,
    material,
    length,
    nest_cache,
    node_affinity_fn,
    slop_frac=DEFAULT_SLOP_FRAC,
    ending_ts=None,
    always_finalize=True,
):
    # this is essentially a best-first search method where we take the best solution according to its "value" and then try to expand it by adding more assets.
    # eventually we need to nest the thing - either because we have collected enough pieces or because the unnested length goes over the roll length and we need
    # to see if the thing will actually fit on a roll.
    allowable_slop = min(
        max(DEFAULT_MIN_SLOP_PX, length * slop_frac), DEFAULT_MAX_SLOP_PX
    )
    backtracks = 0
    pq = PriorityQueue()
    pq.put(Solution(material, length))
    closed = set()
    while (not pq.empty()) and (ending_ts is None or time.time() < ending_ts):
        sol = pq.get()
        closed.add(sol)
        logger.info(
            f"Visiting solution with value {sol.value:.2f} and utilization {sol.nested_utilization:.3f} requiring {int(sol.nested_required_length)} (+{int(sol.unnested_area)}) of {length} with groups {', '.join([f'{k}: {len(v.nested)},{v.pieces_to_nest}({len(list(v.contained_node_ids()))})' for k, v in sol.nest_groups.items()])}"
        )
        # see if we need to nest the solution
        flush_to_length = (
            sol.unnested_count > 1
            and sol.nested_required_length + sol.unnested_length
            > length * (1 - slop_frac)
        )
        if sol.needs_flush or flush_to_length:
            sol = sol.flush(nest_cache, force=flush_to_length)
            logger.info(
                f"Flushed - solution with value {sol.value:.2f} and utilization {sol.nested_utilization:.3f} requiring {int(sol.nested_required_length)} (+{int(sol.unnested_area)}) of {length} with groups {', '.join([f'{k}: {len(v.nested)},{v.pieces_to_nest}({len(list(v.contained_node_ids()))})' for k, v in sol.nest_groups.items()])}"
            )
        dead_end = sol.nested_required_length > length
        # if the solution is still valid then work on it
        if not dead_end:
            # the solution fills the roll so output it,
            if (
                length - sol.nested_required_length
            ) < allowable_slop and sol.unnested_count <= 1:
                sol = sol.flush(nest_cache, force=True)
                if sol.nested_utilization >= UTILIZATION_MINIMUM:
                    yield sol.finalize(), SolutionState.VALID
            # figure out what assets we can add to the solution and then construct new solutions by adding each one.
            covered = set(sol.contained_node_ids())
            remaining_length = length - sol.nested_required_length
            uncovered_nodes = {
                k: v
                for k, v in ones_nodes.items()
                if all(id not in covered for id in v.covered_node_ids())
            }
            # so as to not get stuck - only add one instance of each SKU (so we dont e.g., repeatedly try to nest the same set of skus after we discover its not possible).
            included_skus = set()
            deduped_uncovered_nodes = {}
            total_candidates = len(uncovered_nodes)
            for k, v in sorted(
                uncovered_nodes.items(), key=lambda t: t[1].value, reverse=True
            ):
                sku = v.nesting_sku
                if sku is None or sku not in included_skus:
                    included_skus.add(sku)
                    deduped_uncovered_nodes[k] = v
            uncovered_nodes = deduped_uncovered_nodes
            logger.debug(f"expanding skus: {included_skus}")
            if len(uncovered_nodes) == 0:
                sol = sol.flush(nest_cache, force=True)
                logger.info(
                    f"Ran out of assets with length {sol.nested_required_length}"
                )
                if sol.nested_utilization >= UTILIZATION_MINIMUM:
                    yield (sol.finalize() if always_finalize else sol), (
                        SolutionState.VALID
                        if (length - sol.nested_required_length) < allowable_slop
                        else SolutionState.INSUFFICIENT_ASSET_LENGTH
                    )
                return
            expanded = 0
            for n in uncovered_nodes.values():
                if (
                    n.mergeable()
                    and n.total_area() + sol.unnested_area < remaining_length
                ) or (not n.mergeable() and n.total_height() < remaining_length):
                    next = sol.add_one_node(n)
                    if next not in closed:
                        pq.put(next)
                        expanded += 1
            logger.info(
                f"Expanded {expanded} nodes out of {len(uncovered_nodes)} skus and {total_candidates} candidates"
            )
            if expanded == 0:
                dead_end = True
        if dead_end:
            backtracks += 1
            if backtracks > MAX_BACKTRACKS:
                logger.info(f"Exceeded max nesting attempts.")
                return
    if not pq.empty():
        yield pq.get().flush(
            nest_cache, force=True
        ).finalize(), SolutionState.TIME_LIMIT_EXCEEDED


def nest_into_rolls(
    ones_nodes,
    rolls_info,
    material,
    nest_cache,
    node_affinity_fn,  # f(node_id, [node_ids])
    solution={},
    slop_frac=DEFAULT_SLOP_FRAC,
    ending_ts=None,
):
    if len(rolls_info) == 0:
        logger.info("No rolls available - keeping nest cache warm")
        try:
            roll_solution, _ = next(
                nest_to_length(
                    ones_nodes,
                    material,
                    _yards_to_px(5000),
                    nest_cache,
                    node_affinity_fn,
                    slop_frac,
                    ending_ts=ending_ts,
                )
            )
            yield {"NO_ROLL_NAME": roll_solution}, SolutionState.NO_ROLLS
        except:
            yield {}, SolutionState.NO_ROLLS
        return
    if ending_ts is None or time.time() < ending_ts:
        total_unnested_length = ROLL_ADVANCE_START_PX + sum(
            p.packing.height()
            + (material.seperator_height_px if not p.mergeable else 0)
            for g in ones_nodes.values()
            for p in g.node_map.values()
        )
        logger.info(f"Total unnested length {total_unnested_length/(36*300):1f} yd")
        candidate_rolls = list(rolls_info)
        logger.info(f"Operating with candidate rolls {candidate_rolls}")
        for roll_name, roll_yds, _, _ in candidate_rolls:
            roll_px = _yards_to_px(roll_yds)
            logger.info(
                f"Attempting to find solution for {roll_name} and {len(ones_nodes)} ones"
            )
            # find solutions for the current roll and the current pieces.
            for roll_solution, solution_status in nest_to_length(
                ones_nodes,
                material,
                roll_px,
                nest_cache,
                node_affinity_fn,
                slop_frac,
                ending_ts=ending_ts,
            ):
                logger.info(
                    f"Solution {roll_name} (status: {solution_status}), {roll_yds} yds, reserving {_px_to_yards(roll_solution.nested_required_length):.2} with util {100*roll_solution.nested_utilization:.2}%"
                )
                if solution_status is SolutionState.VALID:
                    yield {roll_name: roll_solution, **solution}, solution_status
                    contained_nodes = set(roll_solution.contained_node_ids())
                    # recurse into the remaining rolls with the remaining pieecs
                    remaining_nodes = {
                        k: v
                        for k, v in ones_nodes.items()
                        if all(
                            nid not in contained_nodes for nid in v.covered_node_ids()
                        )
                    }
                    remaining_rolls = [r for r in candidate_rolls if r[0] != roll_name]
                    if len(remaining_rolls) > 0 and len(remaining_nodes) > 0:
                        logger.info(
                            f"Attempting to expand solution to {remaining_rolls} with {len(remaining_nodes)} nodes."
                        )
                        for (
                            expanded_solution,
                            expanded_solution_status,
                        ) in nest_into_rolls(
                            remaining_nodes,
                            remaining_rolls,
                            material,
                            nest_cache,
                            node_affinity_fn,
                            {roll_name: roll_solution, **solution},
                            slop_frac=slop_frac,
                            ending_ts=ending_ts,
                        ):
                            yield expanded_solution, expanded_solution_status
                            # dont keep trying more rolls if we are under length - since they will all be.
                            # instead we can just backtrack and try a different roll first.
                            if (
                                expanded_solution_status
                                == SolutionState.INSUFFICIENT_ASSET_LENGTH
                            ):
                                break
                else:
                    yield {roll_name: roll_solution, **solution}, solution_status
                if solution_status in [
                    SolutionState.INSUFFICIENT_ASSET_LENGTH,
                    SolutionState.TIME_LIMIT_EXCEEDED,
                ]:
                    # dont keep searching on this roll until we come up with a solution which just gets to the roll length
                    # via worse utilization.
                    break
    if ending_ts is not None and time.time() > ending_ts:
        logger.info("Exceeded time limit finding roll solutions")
        yield solution, SolutionState.TIME_LIMIT_EXCEEDED
