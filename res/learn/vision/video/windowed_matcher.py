import res
import numpy as np
import matplotlib.pyplot as plt
import cv2
from collections import defaultdict
from queue import PriorityQueue

from .printfile import Printfile
from .printfile_matcher import (
    get_frames,
    get_sift_matches,
    MATCHER_X_RES,
)


class WindowedMatcher:
    def __init__(
        self,
        printfile_id,
        video_path,
        video_stride=10,
    ):
        res.utils.logger.info(f"Loading video {video_path}")
        self.frames = get_frames(video_path, adaptive=True, spacing=video_stride)
        self.frames_orig = get_frames(
            video_path, adaptive=True, scaled=False, spacing=video_stride
        )
        self.num_frames = len(self.frames)
        self.frame_height = self.frames[0].shape[0]
        res.utils.logger.info(
            f"Loaded {self.num_frames} frames with downsampled height {self.frame_height} and {len(self.frames_orig)} frames at original dimension."
        )
        self.printfile_ref = Printfile(printfile_id)
        self.printfile = self.printfile_ref.printfile_scaled
        self.buffered = np.pad(
            self.printfile,
            [(MATCHER_X_RES, MATCHER_X_RES), (MATCHER_X_RES, MATCHER_X_RES), (0, 0)],
            constant_values=255,
        )
        self.printfile_height = self.printfile.shape[0]
        res.utils.logger.info(
            f"Loaded printfile downsampled to height {self.printfile_height}"
        )
        res.utils.logger.info(f"Getting sift matches")
        frames_matches = get_sift_matches(self.frames, self.printfile)
        self.frames_matches_sorted = [
            sorted(
                [
                    (np.array([p[0] / MATCHER_X_RES, p[1] / self.frame_height]), q)
                    for p, q, _ in f
                ],
                key=lambda t: t[1][1],
            )
            for f in frames_matches
        ]
        self.frames_matches_sorted_y = [
            [t[1][1] for t in f] for f in self.frames_matches_sorted
        ]

    def get_scaled_matches(self, fidx, x0, y0, x1, y1):
        d = 0
        n = len(self.frames_matches_sorted_y[fidx])
        idx0 = np.searchsorted(self.frames_matches_sorted_y[fidx], y0)
        idx = idx0
        matches = []
        while idx < n and self.frames_matches_sorted_y[fidx][idx] < y1:
            p, q = self.frames_matches_sorted[fidx][idx]
            u = (q[0] - x0) / (x1 - x0)
            v = (q[1] - y0) / (y1 - y0)
            if not (u < 0 or u > 1 or v < 0 or v > 1):
                matches.append((p, np.array([u, v])))
            idx += 1
        return matches

    def dist(self, fidx, x0, y0, x1, y1):
        d = 0
        n = len(self.frames_matches_sorted_y[fidx])
        scaled_matches = self.get_scaled_matches(fidx, x0, y0, x1, y1)
        d = 2 * (n - len(scaled_matches))
        for (s, t), (u, v) in scaled_matches:
            d += abs(u - s) + abs(v - t)
        return d

    def viterbi_approx(
        self, box_width, box_height, cell=5, lam_x=1e-2, lam_y=1e-3, beam_width=5000
    ):
        res.utils.logger.info(
            f"Computing alignment with beam width {beam_width} and target size {box_height, box_width}."
        )
        # forwards beam search version of viterbi since the state space is huge when the cell size
        # is small.  for now the box dimensions are constant but doing it in a way where we can relax that.
        # what i want to do is make this a kind of coarse to fine thing since now its very dependent on getting the
        # first frame right through sift alone.
        pxy = np.zeros((self.num_frames, beam_width, 6))
        dists_0 = np.array(
            [
                [
                    self.dist(0, x, y, x + box_width, y + box_height),
                    x,
                    y,
                    x + box_width,
                    y + box_height,
                    0,
                ]
                for x in range(-box_width, MATCHER_X_RES, cell)
                for y in range(-box_height, self.printfile_height, cell)
            ]
        )
        idx_0 = np.argpartition(dists_0[:, 0], beam_width)[0:beam_width]
        pxy[0] = dists_0[idx_0]

        for i in range(1, self.num_frames):
            cellq = PriorityQueue()
            cellm = {}
            for ind in range(beam_width):
                p, x0, y0, x1, y1, _ = pxy[i - 1, ind]
                cellq.put((p, x0, y0, x1, y1, ind))
            while len(cellm) <= beam_width:
                p, x0, y0, x1, y1, ind = cellq.get()
                if (x0, y0, x1, y1) not in cellm:
                    cellm[(x0, y0, x1, y1)] = (p, ind)
                    q, u0, v0, u1, v1, _ = pxy[i - 1, ind]
                    if (x0 + cell, y0, x1 + cell, y1) not in cellm:
                        cellq.put(
                            (
                                q
                                + lam_x * (x0 + cell - u0) ** 2
                                + lam_y * (y0 - v0) ** 2,
                                x0 + cell,
                                y0,
                                x1 + cell,
                                y1,
                                ind,
                            )
                        )
                    if (x0 - cell, y0, x1 - cell, y1) not in cellm:
                        cellq.put(
                            (
                                q
                                + lam_x * (x0 - cell - u0) ** 2
                                + lam_y * (y0 - v0) ** 2,
                                x0 - cell,
                                y0,
                                x1 - cell,
                                y1,
                                ind,
                            )
                        )
                    if (x0, y0 + cell, x1, y1 + cell) not in cellm:
                        cellq.put(
                            (
                                q
                                + lam_x * (x0 - u0) ** 2
                                + lam_y * (y0 + cell - v0) ** 2,
                                x0,
                                y0 + cell,
                                x1,
                                y1 + cell,
                                ind,
                            )
                        )
                    if (x0, y0 - cell, x1, y1 - cell) not in cellm:
                        cellq.put(
                            (
                                q
                                + lam_x * (x0 - u0) ** 2
                                + lam_y * (y0 - cell - v0) ** 2,
                                x0,
                                y0 - cell,
                                x1,
                                y1 - cell,
                                ind,
                            )
                        )
            dists_i = np.array(
                [
                    [p + self.dist(i, x0, y0, x1, y1), x0, y0, x1, y1, ind]
                    for (x0, y0, x1, y1), (p, ind) in cellm.items()
                ]
            )
            idx_i = np.argpartition(dists_i[:, 0], beam_width)[0:beam_width]
            pxy[i] = dists_i[idx_i]

        assignment_xy = np.zeros((self.num_frames, 5))
        ind = np.argmin(pxy[self.num_frames - 1, :, 0])
        assignment_xy[self.num_frames - 1] = pxy[self.num_frames - 1, ind, 0:5]
        bptr = int(pxy[self.num_frames - 1, ind, 5])
        for i in range(self.num_frames - 2, -1, -1):
            assignment_xy[i] = pxy[i, bptr, 0:5]
            bptr = int(pxy[i, bptr, 5])

        res.utils.logger.info(
            f"Computed an alignment with score {assignment_xy[self.num_frames - 1, 0]}"
        )
        return assignment_xy

    def get_scaled_bounding_boxes(self, x0, y0, x1, y1):
        for k, (bx0, by0, bx1, by1) in self.printfile_ref.mbb.items():
            bx0 = int(bx0 * MATCHER_X_RES)
            bx1 = int(bx1 * MATCHER_X_RES)
            by0 = int(by0 * self.printfile_height)
            by1 = int(by1 * self.printfile_height)
            if bx0 < x1 and bx1 > x0 and by1 > y0 and by0 < y1:
                sx0 = (bx0 - x0) / (x1 - x0)
                sx1 = (bx1 - x0) / (x1 - x0)
                sy0 = (by0 - y0) / (y1 - y0)
                sy1 = (by1 - y0) / (y1 - y0)
                yield k, np.array([sx0, sy0, sx1, sy1])

    def debug_frame(self, frame_idx, x0, y0, x1, y1):
        # just dump a side by side thing of the frame, printfile section, and the complete printfile.
        ps = 2 * self.frame_height / self.printfile_height
        printfile_tiny = cv2.resize(
            np.pad(self.printfile, [(0, 0), (400, 400), (0, 0)], constant_values=255),
            (0, 0),
            fx=ps,
            fy=ps,
        )
        debug_frame = np.zeros(
            (2 * self.frame_height, MATCHER_X_RES + printfile_tiny.shape[1], 3), "uint8"
        )
        debug_frame[0 : self.frame_height, 0:MATCHER_X_RES, :] = self.frames[frame_idx]
        target_region = self.buffered[
            (y0 + MATCHER_X_RES) : (y1 + MATCHER_X_RES),
            (x0 + MATCHER_X_RES) : (x1 + MATCHER_X_RES),
            :,
        ]
        target_region = cv2.resize(target_region, (MATCHER_X_RES, self.frame_height))
        # draw on bounding boxes.
        for k, (sx0, sy0, sx1, sy1) in self.get_scaled_bounding_boxes(x0, y0, x1, y1):
            target_region = cv2.rectangle(
                target_region,
                (int(sx0 * MATCHER_X_RES), int(sy0 * self.frame_height)),
                (int(sx1 * MATCHER_X_RES), int(sy1 * self.frame_height)),
                (255, 0, 0),
                2,
            )
            debug_frame = cv2.rectangle(
                debug_frame,
                (int(sx0 * MATCHER_X_RES), int(sy0 * self.frame_height)),
                (int(sx1 * MATCHER_X_RES), int(sy1 * self.frame_height)),
                (0, 0, 255),
                2,
            )
        debug_frame[
            self.frame_height : 2 * self.frame_height, 0:MATCHER_X_RES, :
        ] = target_region
        debug_frame[:, MATCHER_X_RES:, :] = printfile_tiny
        debug_frame = cv2.rectangle(
            debug_frame,
            (int(MATCHER_X_RES * (1 + ps) + x0 * ps), int(y0 * ps)),
            (int(MATCHER_X_RES * (1 + ps) + x1 * ps), int(y1 * ps)),
            (255, 0, 0),
            3,
        )
        # draw on sift matches.
        for p, q in self.get_scaled_matches(frame_idx, x0, y0, x1, y1):
            line = cv2.line(
                debug_frame.copy(),
                (int(p[0] * MATCHER_X_RES), int(p[1] * self.frame_height)),
                (
                    int(q[0] * MATCHER_X_RES),
                    int(q[1] * self.frame_height + self.frame_height),
                ),
                (255, 0, 0),
                2,
            )
            debug_frame = cv2.addWeighted(line, 0.2, debug_frame, 0.8, 0)
        return debug_frame.astype("uint8")

    def dump_debug_video(self, path, assignment_xy):
        frame = lambda i: self.debug_frame(i, *assignment_xy[i, 1:5].astype("int"))[
            :, :, [2, 1, 0]
        ]
        df = frame(0)
        out = cv2.VideoWriter(
            path, cv2.VideoWriter_fourcc(*"MP4V"), 20.0, (df.shape[1], df.shape[0])
        )
        out.write(df)
        for i in range(1, self.num_frames):
            out.write(frame(i))
        out.release()

    def get_piece_coords(self, assignment_xy, visible_frac_threshold=0.3):
        for frame_idx in range(self.num_frames):
            x0, y0, x1, y1 = assignment_xy[frame_idx, 1:5].astype("int")
            frame = self.frames_orig[frame_idx]
            frame_height, frame_width = frame.shape[0:2]
            for k, (sx0, sy0, sx1, sy1) in self.get_scaled_bounding_boxes(
                x0, y0, x1, y1
            ):
                frame_coords = np.array(
                    [
                        int(max(0, sy0) * frame_height),
                        int(min(1, sy1) * frame_height),
                        int(max(0, sx0) * frame_width),
                        int(min(1, sx1) * frame_width),
                    ]
                )
                bbox_coords = np.array(
                    [
                        (max(0, sy0) - sy0) / (sy1 - sy0),
                        (min(1, sy1) - sy0) / (sy1 - sy0),
                        (max(0, sx0) - sx0) / (sx1 - sx0),
                        (min(1, sx1) - sx0) / (sx1 - sx0),
                    ]
                )
                visible_frac = (
                    (min(1, sy1) - max(0, sy0))
                    * (min(1, sx1) - max(0, sx0))
                    / ((sy1 - sy0) * (sx1 - sx0))
                )
                if visible_frac > visible_frac_threshold:
                    yield frame_idx, k, visible_frac, frame_coords, bbox_coords

    def get_best_k_piece_images(self, assignment_xy, k=3, piece_key=None):
        coords = defaultdict(lambda: PriorityQueue())
        for fidx, key, area, frame_coords, bbox_coords in self.get_piece_coords(
            assignment_xy
        ):
            if piece_key is None or key == piece_key:
                coords[key].put((-area, fidx, frame_coords, bbox_coords))
        for key, q in coords.items():
            for i in range(k):
                try:
                    sf, fidx, (y0, y1, x0, x1), bbox_coords = q.get()
                    yield key, self.frames_orig[fidx][y0:y1, x0:x1], bbox_coords
                except:
                    pass
