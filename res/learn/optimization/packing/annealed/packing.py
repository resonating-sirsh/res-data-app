import random
import math
import numpy as np

from collections import Counter
from res.learn.optimization.packing.geometry import translate
from res.utils import logger
import multiprocessing
from multiprocessing import Pool

from .packing_geometry import (
    combine_frontier,
    choose_geom,
    divider,
    vertical_spacer,
    guillotine_divider,
)
from ..packing import PackingShape

# for pieces which require anchoring to the bottom or top of the roll
# how much weight to give them in the overall objective (see Packing.objective)
ANCHOR_PIECE_OBJECTIVE_COEFFICIENT = 0.1

GUILLOTINE_SPACER_PIECE_IDX = -2


class Packing:
    """
    Represent packings as a tree structure where each node represents adding on a
    shape to its parent in the tree.  This way we can build up packings incrementally
    and also explore the space of these packings efficiently.
    """

    def __init__(
        self,
        box=None,
        box_idx=None,
        box_translation=None,
        height=0,
        parent=None,
        frontier=None,
        record_box=True,
        max_width=55 * 300,
        x_loss_val=None,
        margin_x=0,
        margin_y=0,
    ):
        self.box = box
        self.box_idx = box_idx
        self.box_translation = box_translation
        self.parent = parent
        self.height = height
        self.max_width = max_width
        self.frontier = [(0, 0, max_width, 0)] if frontier is None else frontier
        self.children = {}
        self.record_box = record_box
        self.x_loss_val = x_loss_val
        self.margin_x = margin_x
        self.margin_y = margin_y

    def add(self, box, box_idx, x_loss_val=0.05):
        """
        Add a box to this packing -- the box should be a PackingGeometry.
        """
        if (box_idx, x_loss_val) in self.children:
            return self.children[(box_idx, x_loss_val)]
        translation = box.best_fit_coordinates(
            self.frontier, self.max_width, x_loss_val=x_loss_val, margin=self.margin_x
        )
        return self._add_translated(box, box_idx, translation, x_loss_val=x_loss_val)

    def _add_translated(
        self, box, box_idx, translation, record_box=True, x_loss_val=None
    ):
        translated = translate(box.box, translation)
        x0, _, x1, y1 = translated.bounds
        dx, dy = translation
        child = Packing(
            box=box,
            box_idx=box_idx,
            box_translation=translation,
            parent=self,
            height=max(self.height, y1) if record_box else self.height,
            max_width=self.max_width,
            frontier=combine_frontier(
                [
                    (px + dx, py + dy, qx + dx, qy + dy)
                    for px, py, qx, qy in box.frontier
                ],
                x0,
                x1,
                self.frontier,
            ),
            record_box=record_box,
            x_loss_val=x_loss_val,
            margin_x=self.margin_x,
            margin_y=self.margin_y,
        )
        if record_box:
            self.children[(box_idx, x_loss_val)] = child

        return child

    def boxes(self, filter_hidden_boxes=True):
        """
        Return a generator of tuple of (index, packing geometry, translation) of all
        the objects that are in the packing -- they will arrive in reverse order of how they
        were added.
        """
        # py isnt cool about recursion
        curr = self
        while curr is not None:
            if (
                (curr.record_box and curr.box_idx is not None and curr.box_idx >= 0)
                or not filter_hidden_boxes
            ) and curr.box is not None:
                yield (curr.box_idx, curr.box, curr.box_translation)
            curr = curr.parent

    def all_box_ids(self):
        return set(i for i, _, _ in self.boxes())

    def indices_and_x_loss(self):
        curr = self
        while curr is not None:
            if curr.record_box and curr.box is not None:
                yield (curr.box_idx, curr.x_loss_val)
            curr = curr.parent

    def to_packingshapes(self):
        """
        Convert this packing object into the same structure returned by the heuristic packing.
        """
        box_info = sorted(list(self.boxes()))
        res = []
        for _, b, t in box_info:
            # PackingShape uses the bottom left of the bounding box as its origin whereas
            # this packing used the bottom left vertex.
            coords = t + b.local_offset + np.array([b.geom.bounds[0], b.geom.bounds[1]])
            p = PackingShape(b.geom)
            p.translate(coords)
            res.append(p)
        return res

    def utilization(self):
        return sum(b.geom.area for _, b, _ in self.boxes()) / (
            self.height * self.max_width
        )

    def plot(self, ax=None, show_waste=True):
        from matplotlib import pyplot as plt

        ax = ax or plt.gca()
        ax.set_ylim(0, self.height)
        ax.set_xlim(0, self.max_width)
        for _, b, t in self.boxes():
            bounds = translate(b.box, t)
            orig = translate(b.geom, np.array(t) + b.local_offset)
            if show_waste:
                ax.fill(*bounds.boundary.xy, color=(0.6, 0.6, 0.9))
                ax.plot(*bounds.boundary.xy, color=(0.3, 0.3, 0.45))
            try:
                # sometimes the piece boundary can be fucked up.
                ax.fill(*orig.boundary.xy, color=(0.6, 0.9, 0.6))
                ax.plot(*orig.boundary.xy, color=(0.3, 0.45, 0.3))
            except:
                ax.fill(*bounds.boundary.xy, color=(0.6, 0.9, 0.6))
                ax.plot(*bounds.boundary.xy, color=(0.3, 0.45, 0.3))
        for _, b, t in self.boxes(filter_hidden_boxes=False):
            if b.type_name == "guillotine_spacer":
                bounds = translate(b.box, t)
                ax.fill(*bounds.boundary.xy, color=(0.9, 0.6, 0.6))
                ax.plot(*bounds.boundary.xy, color=(0.45, 0.3, 0.3))
            if b.type_name == "divider":
                ax.axline(
                    (0, t[1]),
                    (self.max_width, t[1]),
                    color="blue",
                    linewidth=2,
                    linestyle="--",
                )

    def objective(self):
        """
        The packing objective -- to be minimized.
        """
        height = self.height
        obj = height
        for _, b, t in self.boxes():
            if b.piece_group == "anchor_bottom":
                obj += ANCHOR_PIECE_OBJECTIVE_COEFFICIENT * (t[1] + t[0])
            elif b.piece_group == "anchor_top":
                obj += ANCHOR_PIECE_OBJECTIVE_COEFFICIENT * (
                    (height - t[1]) + (self.max_width - t[0])
                )
        return obj


def get_packing(
    boxes,
    order,
    packing_root=None,
    max_width=300 * 55,
    max_vertical_strip=None,
    vertical_strip_spacer=300 * 4,
    max_guillotinable_length=None,
    guillotine_spacer=300 * 6,
    margin_x=0,
    margin_y=0,
):
    """
    Return the packing when the boxes are added in a particular order.
    """
    packing = (
        packing_root
        if packing_root is not None
        else Packing(max_width=max_width, margin_x=margin_x, margin_y=margin_y)
    )
    max_width, margin_x, margin_y = (
        (max_width, margin_x, margin_y)
        if packing_root is None
        else (packing_root.max_width, packing_root.margin_x, packing_root.margin_y)
    )
    last_guillotine_length = 0
    if margin_y > 0:
        packing = packing._add_translated(
            divider(packing.max_width),
            -1,
            [0, margin_y],
        )
    for i in order:
        if (
            max_guillotinable_length is not None
            and packing.add(boxes[i], i).height - last_guillotine_length
            > max_guillotinable_length
        ):
            if packing.height > last_guillotine_length:
                packing = packing._add_translated(
                    guillotine_divider(packing.max_width, guillotine_spacer),
                    GUILLOTINE_SPACER_PIECE_IDX,
                    [0, packing.height + margin_y],
                )
                if margin_y > 0:
                    packing = packing._add_translated(
                        divider(packing.max_width),
                        -1,
                        [0, packing.height + margin_y],
                    )
            last_guillotine_length = packing.height
        packing = packing.add(boxes[i], i)
        if max_vertical_strip is not None and packing.height >= max_vertical_strip:
            break
    if margin_y > 0:
        packing = packing._add_translated(
            divider(packing.max_width),
            -1,
            [0, packing.height + margin_y],
        )
    if max_vertical_strip is None or packing.height < max_vertical_strip:
        # the fuser nest can just be put in sideways instead of cut.
        return packing
    packing = (
        packing_root
        if packing_root is not None
        else Packing(max_width=max_width, margin_x=margin_x, margin_y=margin_y)
    )
    if margin_y > 0:
        packing = packing._add_translated(
            divider(packing.max_width),
            -1,
            [0, margin_y],
        )
    # adjust the max vertical strip size to potentially get a better packing.
    max_vertical_strip = max(
        (packing.max_width - 2 * margin_x - vertical_strip_spacer) / 2,
        max(
            (b.width + margin_x for b in boxes if b.width <= max_vertical_strip),
            default=0,
        ),
    )
    for i in order:
        if boxes[i].width >= max_vertical_strip:
            packing = packing.add(boxes[i], i)
    if packing.height > 0:
        packing = packing._add_translated(
            divider(packing.max_width),
            -1,
            [0, packing.height + 150],
        )
    last_guillotine_length = packing.height
    packing = add_vertical_spacers(packing, max_vertical_strip, vertical_strip_spacer)
    for i in order:
        if boxes[i].width < max_vertical_strip:
            if (
                max_guillotinable_length is not None
                and packing.add(boxes[i], i).height - last_guillotine_length
                > max_guillotinable_length
            ):
                if packing.height > last_guillotine_length:
                    packing = packing._add_translated(
                        guillotine_divider(packing.max_width, guillotine_spacer),
                        GUILLOTINE_SPACER_PIECE_IDX,
                        [0, packing.height + margin_y],
                    )
                    if margin_y > 0:
                        packing = packing._add_translated(
                            divider(packing.max_width),
                            -1,
                            [0, packing.height + margin_y],
                        )
                    packing = add_vertical_spacers(
                        packing, max_vertical_strip, vertical_strip_spacer
                    )
                last_guillotine_length = packing.height
            packing = packing.add(boxes[i], i)
    if margin_y > 0:
        packing = packing._add_translated(
            divider(packing.max_width),
            -1,
            [0, packing.height + margin_y],
        )
    return packing


def add_vertical_spacers(packing, max_vertical_strip, vertical_strip_spacer=300 * 4):
    # note - now assuming only 2 strips.
    width_remaining = packing.max_width
    while width_remaining >= max_vertical_strip + vertical_strip_spacer:
        # to break the canvas into vertical strips, add in some ghost objects that takes up infinite height
        # that the nesting has to nest around.
        packing = packing._add_translated(
            vertical_spacer(vertical_strip_spacer),
            -1,
            [
                packing.max_width - width_remaining + max_vertical_strip,
                packing.height + 1,
            ],
            record_box=False,
        )
        width_remaining -= max_vertical_strip + vertical_strip_spacer
    return packing


def pack_boxes_annealed_inner(
    thread_idx,
    boxes,
    max_width,
    max_vertical_strip,
    max_guillotinable_length,
    guillotine_spacer,
    max_iters,
    initial_order=None,
    log_progress=True,
    margin_x=0,
    margin_y=0,
):
    """
    Anneal the order of adding things to the packing.
    """
    n = len(boxes)
    packing_root = Packing(max_width=max_width, margin_x=margin_x, margin_y=margin_y)
    order = list(range(n)) if initial_order is None else initial_order
    if n == 1:
        return order
    if initial_order is None:
        random.shuffle(order)
    current = get_packing(
        boxes,
        order,
        max_vertical_strip=max_vertical_strip,
        packing_root=packing_root,
        max_guillotinable_length=max_guillotinable_length,
        guillotine_spacer=guillotine_spacer,
    )
    current_obj = current.objective()
    best = current
    best_obj = current_obj
    best_order = list(order)
    total_accept = 0
    uphill_accept = 0
    total_area = sum(b.box.area for b in boxes)
    for iter in range(max_iters):
        if log_progress and iter % 1000 == 0:
            logger.info(
                f"Thread {thread_idx}, iteration {iter}, best {best_obj:.2f}, util {100*total_area / (best.height * max_width):.2f}%, total jumps {total_accept} uphill jumps {uphill_accept}"
            )
            # kill the cache so we don't oom on big nests (although current and best still hold on to it).
            packing_root = Packing(max_width=max_width)
        i, j = random.sample(range(n), k=2)
        order[i], order[j] = order[j], order[i]
        res = get_packing(
            boxes,
            order,
            max_vertical_strip=max_vertical_strip,
            packing_root=packing_root,
            max_guillotinable_length=max_guillotinable_length,
            guillotine_spacer=guillotine_spacer,
        )
        res_obj = res.objective()
        delta = res_obj - current_obj
        reject_prob = (
            0 if delta < 0 else 2 / (1 + math.exp(-delta * 100 * iter / max_iters)) - 1
        )
        if reject_prob < random.random():
            current = res
            current_obj = res_obj
            if res_obj < best_obj:
                best = res
                best_obj = res_obj
                best_order = list(order)
            total_accept += 1
            if delta > 0:
                uphill_accept += 1
        else:
            order[i], order[j] = order[j], order[i]

    return best_order


def pack_boxes_annealed(
    boxes,
    max_width,
    max_vertical_strip,
    max_guillotinable_length,
    guillotine_spacer,
    max_iters,
    threads,
    initial_order=None,
    log_progress=True,
    margin_x=0,
    margin_y=0,
):
    # if we dont want to do any iterations then just put the boxes into the strip in input order.
    if max_iters <= 0:
        return get_packing(
            boxes,
            initial_order if initial_order is not None else list(range(len(boxes))),
            max_width=max_width,
            max_vertical_strip=max_vertical_strip,
            max_guillotinable_length=max_guillotinable_length,
            guillotine_spacer=guillotine_spacer,
            margin_x=margin_x,
            margin_y=margin_y,
        )
    if threads == 1:
        orders = [
            pack_boxes_annealed_inner(
                0,
                boxes,
                max_width,
                max_vertical_strip,
                max_guillotinable_length,
                guillotine_spacer,
                max_iters,
                initial_order,
                log_progress,
                margin_x=margin_x,
                margin_y=margin_y,
            )
        ]
    else:
        threadpool = (
            threads if isinstance(threads, multiprocessing.pool.Pool) else Pool(threads)
        )
        orders = threadpool.starmap(
            pack_boxes_annealed_inner,
            (
                (
                    i,
                    boxes,
                    max_width,
                    max_vertical_strip,
                    max_guillotinable_length,
                    guillotine_spacer,
                    max_iters,
                    initial_order,
                    log_progress,
                    margin_x,
                    margin_y,
                )
                for i in range(len(threadpool._pool))
            ),
        )
    best = None
    for order in orders:
        packing = get_packing(
            boxes,
            order,
            max_width=max_width,
            max_vertical_strip=max_vertical_strip,
            max_guillotinable_length=max_guillotinable_length,
            guillotine_spacer=guillotine_spacer,
            margin_x=margin_x,
            margin_y=margin_y,
        )
        if best is None or packing.height < best.height:
            best = packing
    return best


def _get_packings_multi(boxes, order, bucket, packing_roots):
    packings = list(packing_roots)
    for b, i in zip(bucket, order):
        packings[b] = packings[b].add(boxes[i], i)
    return packings, sum(p.height for p in packings)


def pack_multi(geoms, max_width, max_height, buckets, max_iters=10000):
    # experimental code to pack into multiple strips at once so that we dont exceed some max height
    for g in geoms:
        if g.bounds[3] - g.bounds[1] > max_height:
            raise ValueError(
                f"Cannot pack due to max height {max_height} and piece with bounds {g.bounds}"
            )
    boxes = [choose_geom(g) for g in geoms]
    packing_roots = [Packing(max_width=max_width) for i in range(buckets)]
    n = len(boxes)
    order = list(range(n))
    attempt = 0
    while True:
        attempt += 1
        random.shuffle(order)
        bucket = [random.randint(0, buckets - 1) for b in boxes]
        curr, curr_height = _get_packings_multi(boxes, order, bucket, packing_roots)
        if all(p.height < max_height for p in curr):
            break
        if attempt > 1000:
            raise ValueError("No feasible initialization")
    if n == 1:
        return curr
    for iter in range(max_iters):
        i, j = random.sample(range(n), k=2)
        bucket_i = bucket[i]
        flip = buckets == 1 or random.random() < 0.5
        if flip:
            order[i], order[j] = order[j], order[i]
        else:
            bucket[i] = random.randint(0, buckets - 1)
        next, next_height = _get_packings_multi(boxes, order, bucket, packing_roots)
        delta = next_height - curr_height
        reject_prob = (
            0 if delta < 0 else 2 / (1 + math.exp(-delta * 100 * iter / max_iters)) - 1
        )
        if all(p.height < max_height for p in next) and reject_prob < random.random():
            curr, curr_height = next, next_height
        else:
            if flip:
                order[i], order[j] = order[j], order[i]
            else:
                bucket[i] = bucket_i
    return curr


def pack_annealed(
    geoms,
    groups,
    max_width,
    max_height,
    max_vertical_strip=None,
    max_iters=50000,
    threads=8,
):
    """
    Main entry point for packing with a list of geoms.
    """
    n = len(geoms)
    boxes = [choose_geom(g, gr, max_width=max_width) for g, gr in zip(geoms, groups)]
    geom_counts = Counter([b.type_name for b in boxes]).items()
    logger.info(f"Packing with {geom_counts}")
    logger.info(f"piece groups: {set(b.piece_group for b in boxes)}")
    packing = pack_boxes_annealed(
        boxes, max_width, max_vertical_strip, None, None, max_iters, threads
    )
    logger.info(f"Annealed packed into a roll of height {packing.height}")
    return packing.to_packingshapes()[0:n]
