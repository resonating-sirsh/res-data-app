import cv2
import numpy as np
import res
from shapely.ops import unary_union
from res.learn.optimization.packing.geometry import (
    translate,
    negate_shape,
    convex_minkowski,
)
from res.learn.optimization.packing.annealed.packing_geometry import bounding_polygon
from res.utils import logger


class BlockBuffer:
    def __init__(
        self,
        offset_buffer,
        shape=None,
        piece_id=None,
        children=[],
        cutter_buffer=37,
    ):
        contained = (
            shape
            if shape
            else unary_union([translate(s.shape, p) for s, p in children]).convex_hull
        )
        self.piece_id = piece_id
        self.shape = contained
        self.buffered = contained.buffer(offset_buffer)
        self.bounds = bounding_polygon(contained.buffer(cutter_buffer), 10)
        self.children = children
        self.buffered_area = self.buffered.area

    def _repr_svg_(self):
        return self._draw()._repr_svg_()

    def _draw(self):
        if len(self.children) == 0:
            return self.shape.boundary
        else:
            return unary_union(
                [self.shape.boundary]
                + [translate(s._draw(), p) for s, p in self.children]
            )

    def contained_piece_shapes(self):
        if self.piece_id is not None:
            return [self.shape]
        else:
            return [
                shape
                for child, _ in self.children
                for shape in child.contained_piece_shapes()
            ]

    def contained_piece_ids(self):
        if self.piece_id is not None:
            return [self.piece_id]
        else:
            return [
                id for child, _ in self.children for id in child.contained_piece_ids()
            ]

    @staticmethod
    def leaf(piece_shape, piece_id, offset_buffer):
        return BlockBuffer(
            shape=piece_shape, piece_id=piece_id, offset_buffer=offset_buffer
        )

    @staticmethod
    def merge_crude(a, b, offset_buffer, max_width_px):
        nfp = convex_minkowski(a.bounds, negate_shape(b.bounds))
        best = None
        vertices = list(np.array(p) for p in nfp.boundary.coords)
        for i in range(len(vertices) - 1):
            p = vertices[i]
            q = vertices[i + 1]
            for l in np.arange(0, 1, 0.1):
                r = p * l + q * (1 - l)
                br = translate(b.buffered, r)
                piece_width = max(br.bounds[2], a.buffered.bounds[2]) - min(
                    br.bounds[0], a.buffered.bounds[0]
                )
                if piece_width < max_width_px:
                    overlap_area = a.buffered.intersection(br).area
                    if best is None or overlap_area > best[1]:
                        best = (r, overlap_area)
        return (
            BlockBuffer(
                children=[
                    (a, (0, 0)),
                    (b, best[0]),
                ],
                offset_buffer=offset_buffer,
            )
            if best
            else None
        )

    @staticmethod
    def merge_and_buffer(
        id_to_shape_map, offset_buffer, max_width_px, max_piece_size=1500 * 300 * 300
    ):
        leafs = [
            BlockBuffer.leaf(shape, id, offset_buffer)
            for id, shape in id_to_shape_map.items()
        ]
        raw_buffered_area = sum(l.buffered_area for l in leafs)
        logger.info(f"buffered piece area {raw_buffered_area / 90000}")
        while True:
            best = None
            for i in range(len(leafs)):
                for j in range(i + 1, len(leafs)):
                    sij = BlockBuffer.merge_crude(
                        leafs[i], leafs[j], offset_buffer, max_width_px
                    )
                    if sij and sij.buffered_area < max_piece_size:
                        delta = sij.buffered_area - (
                            leafs[i].buffered_area + leafs[j].buffered_area
                        )
                        if delta < 0 and (best is None or best[1] > delta):
                            best = (sij, delta, i, j)
            if best is None:
                break
            sij, delta, i, j = best
            leafs[i] = None
            leafs[j] = None
            leafs = [l for l in leafs if l is not None] + [sij]
            logger.info(
                f"merged piece area {sum(l.buffered_area for l in leafs) / 90000}"
            )
        return leafs
