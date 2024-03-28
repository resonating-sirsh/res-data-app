# unsupervised method of matching video against a known printfile.
# the plan is to just use sift matches and then kalman smooth the thing.
# where P(frame | printfile slice) ~ exp(-dist(frame, printfile slice))
# and P(printfile slice | previous slice) ~ normal

import cv2
import res
import numpy as np
import matplotlib.pyplot as plt
from PIL import Image
from res.airtable.print import PRINTFILES
from res.flows.make.nest.progressive.construct_rolls import _load_material_info

from .image import remove_transparency, scale_x_res

# scale for images when matching.
MATCHER_X_RES = 400


def scale_for_matcher(image):
    return scale_x_res(image, MATCHER_X_RES)


def get_frames(path, spacing=None, viewport=None, scaled=True, adaptive=False):
    video = cv2.VideoCapture(path)
    num_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))

    def frame(idx):
        video.set(cv2.CAP_PROP_POS_FRAMES, idx)
        _, frame = video.read()
        frame = np.asarray(frame)
        frame = (
            frame[:, :, [2, 1, 0]]
            if viewport is None
            else frame[viewport[0] : viewport[1], viewport[2] : viewport[3], [2, 1, 0]]
        )
        return scale_for_matcher(frame) if scaled else frame

    if not adaptive:
        return [
            frame(i) for i in range(0, num_frames, 1 if spacing is None else spacing)
        ]
    else:
        fl = []
        for i in range(0, num_frames, 1 if spacing is None else spacing):
            fi = frame(i)
            if len(fl) == 0 or np.abs(fi - fl[-1]).sum() > 30:
                fl.append(fi)
        return fl


def load_printfile(printfile_record_id, remove_compensation=False):
    printfile_record = PRINTFILES.get(printfile_record_id)
    material = printfile_record["fields"]["Material Code"][0]
    # we can work with the thumbnail since we are going to downsample a lot anyway.
    path = (
        printfile_record["fields"]["Asset Path"] + "/printfile_composite_thumbnail.png"
    )
    res.utils.logger.debug(f"Loading printfile {path}")
    image = res.connectors.load("s3").read(path)
    if remove_compensation:
        material_prop = _load_material_info(material)
        res.utils.logger.info(
            f"Downloading {path} and removing {material} compensation of {(material_prop.stretch_x, material_prop.stretch_y)}"
        )
        image = cv2.resize(
            image,
            (0, 0),
            fx=1 / material_prop.stretch_x,
            fy=1 / material_prop.stretch_y,
        )
    # remove transparency
    image = remove_transparency(image)
    manifest = res.connectors.load("s3").read(
        printfile_record["fields"]["Asset Path"] + "/manifest.feather"
    )
    return scale_for_matcher(image), manifest, printfile_record


def buffer_printfile(printfile):
    # add on trailing and leading white space to represent the leader
    buffer_len = 3 * printfile.shape[1]
    return np.pad(
        printfile,
        [(buffer_len, buffer_len), (0, 0), (0, 0)],
        constant_values=255,
    )


def get_sift_matches(frames, printfile):
    sift = cv2.SIFT_create()
    printfile_sift = sift.detectAndCompute(printfile, None)
    frames_sift = [sift.detectAndCompute(frame, None) for frame in frames]
    # TODO: consider keeping more matches.
    def sift_matches(src_sift, dst_sift, threshold=0.8):
        bf = cv2.BFMatcher()
        matches = bf.knnMatch(src_sift[1], dst_sift[1], k=2)
        good = []
        for (m1, m2) in matches:
            if m1.distance < threshold * m2.distance:
                src_pt = np.asarray(src_sift[0][m1.queryIdx].pt)
                dst_pt = np.asarray(dst_sift[0][m1.trainIdx].pt)
                good.append((src_pt, dst_pt, m1.distance))
        return good

    return [sift_matches(frame_sift, printfile_sift) for frame_sift in frames_sift]


def assign_frames_to_printfile(
    frames_matches, printfile_height, frame_height, invert_frames=False, lam=5e-4
):
    res.utils.logger.debug(
        f"Computing distances from frames to printfile with inverted={invert_frames}"
    )
    dists = np.array(
        [
            [
                np.mean(
                    [
                        (
                            (frame_height / 2)
                            if q[1] < o or q[1] > o + frame_height
                            else abs(
                                (frame_height - p[1] if invert_frames else p[1])
                                - (q[1] - o)
                            )
                        )
                        / frame_height
                        + (
                            abs(p[0] - q[0])
                            if not invert_frames
                            else abs(p[0] - (MATCHER_X_RES - q[0]))
                        )
                        / MATCHER_X_RES
                        for p, q, d in frame_matches
                    ]
                )
                for o in range(printfile_height - frame_height)
            ]
            if len(frame_matches) > 0
            else [0 for _ in range(printfile_height - frame_height)]
            for frame_matches in frames_matches
        ]
    )

    res.utils.logger.debug(f"Computing assignment")
    # viterbi / kalman smoothing
    log_prob = np.zeros_like(dists)
    back_ptr = np.zeros_like(dists)

    for i in range(dists.shape[0]):
        last = np.zeros(dists.shape[1]) if i == 0 else log_prob[i - 1]
        for j in range(dists.shape[1]):
            pij = last + lam * (np.arange(dists.shape[1]) - j) ** 2 + dists[i, j]
            mi = np.argmin(pij)
            back_ptr[i, j] = mi
            log_prob[i, j] = pij[mi]

    assignments = np.zeros(dists.shape[0], dtype="int")
    assignments[dists.shape[0] - 1] = np.argmin(log_prob[dists.shape[0] - 1, :])
    for i in range(dists.shape[0] - 2, -1, -1):
        assignments[i] = back_ptr[i + 1, assignments[i + 1]]

    score = np.min(log_prob[dists.shape[0] - 1, :])
    res.utils.logger.debug(f"Computed an assignment with score {score}")
    return assignments, score


def get_best_assignment(frames, printfile):
    frames_matches = get_sift_matches(frames, printfile)
    assignments_raw, score_raw = assign_frames_to_printfile(
        frames_matches, printfile.shape[0], frames[0].shape[0], invert_frames=False
    )
    assignments_inv, score_inv = assign_frames_to_printfile(
        frames_matches, printfile.shape[0], frames[0].shape[0], invert_frames=True
    )
    inverted = score_inv < score_raw
    assignments = assignments_inv if inverted else assignments_raw
    return (assignments, inverted)


def get_bounding_boxes(manifest, printfile_height):
    return {
        r["asset_key"]
        + ":"
        + r["piece_name"]: (
            int(r["min_x_inches"] / r["printfile_width_inches"] * MATCHER_X_RES),
            int(r["min_y_inches"] / r["printfile_height_inches"] * printfile_height),
            int(r["max_x_inches"] / r["printfile_width_inches"] * MATCHER_X_RES),
            int(r["max_y_inches"] / r["printfile_height_inches"] * printfile_height),
        )
        for _, r in manifest.iterrows()
    }


def annotate_printfile(printfile, manifest):
    printfile = printfile.copy()
    for key, bbox in get_bounding_boxes(manifest, printfile.shape[0]).items():
        printfile = cv2.rectangle(printfile, bbox[0:2], bbox[2:], (255, 0, 0), 1)
        # printfile = cv2.putText(printfile, key, bbox[0:2], cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 0, 0), 1, cv2.LINE_AA)
    return printfile


def linear_mapping(frames_matches, assignments, frame_height, inverted):
    matches_src = []
    matches_dst = []
    # could do arbitrary features here.
    phi = lambda x, y: np.array(
        [
            1,
            MATCHER_X_RES - x if inverted else x,
            frame_height - y if inverted else y,
        ]
    )
    for fidx in range(len(frames_matches)):
        m = frames_matches[fidx]
        j = assignments[fidx]
        for p, q, d in m:
            ry = q[1] - j
            rx = q[0]
            if ry > 0 and ry < frame_height:
                matches_src.append(p)
                matches_dst.append(phi(rx, ry))
    # want to map dst coords back to src.
    # min ||s - Md|| -> min ||s's -s'Md - d'M's - d'M'Md|| -> 0 = -2d's + 2Md'd -> M = (d'd)^-1d's
    d = np.array(matches_dst)
    s = np.array(matches_src)
    M = np.matmul(
        np.linalg.inv(np.matmul(np.transpose(d), d)), np.matmul(np.transpose(d), s)
    )
    return lambda x, y: np.matmul(np.transpose(M), phi(x, y))


def dump_alignment(frames, printfile, manifest, assignments, inverted, filename):
    res.utils.logger.debug(f"Dumping alignment to {filename}")
    frames_matches = get_sift_matches(frames, printfile)
    fourcc = cv2.VideoWriter_fourcc(*"MP4V")
    frame_height = frames[0].shape[0]
    mapping = linear_mapping(frames_matches, assignments, frame_height, inverted)
    mbb = get_bounding_boxes(manifest, printfile.shape[0] - 6 * MATCHER_X_RES)
    height = int(3 * frame_height)
    width = int(2 * MATCHER_X_RES)
    out = cv2.VideoWriter(filename, fourcc, 20.0, (width, height))
    for fidx in range(len(frames)):
        m = frames_matches[fidx]
        f0 = frames[fidx]
        j = assignments[fidx]
        ju = j - 3 * MATCHER_X_RES
        frame_bb = [
            (k, b) for k, b in mbb.items() if b[1] < ju + frame_height and b[3] > ju
        ]
        cj = min(j, printfile.shape[0] - 2 * f0.shape[0])
        combined = np.zeros((height, width, 3))
        combined[frame_height : 2 * frame_height, 0:MATCHER_X_RES, :] = f0
        combined[:, MATCHER_X_RES : 2 * MATCHER_X_RES, :] = printfile[
            cj - f0.shape[0] : cj + 2 * f0.shape[0], :, :
        ][:: (-1 if inverted else 1), :: (-1 if inverted else 1), :]
        for p, q, d in m:
            ry = int(q[1] - j + f0.shape[0])
            rx = int(q[0] + MATCHER_X_RES)
            if inverted:
                ry = 3 * frame_height - ry
                rx = int(2 * MATCHER_X_RES - q[0])
            if ry > 0 and ry < 3 * f0.shape[0]:
                annotated = combined.copy()
                cv2.line(
                    annotated,
                    [int(p[0]), int(f0.shape[0] + p[1])],
                    [rx, ry],
                    (255, 0, 0),
                    3,
                )
                combined = cv2.addWeighted(annotated, 0.1, combined, 0.9, 0)
        for k, (x0, y0, x1, y1) in frame_bb:
            u0, v0 = mapping(x0, y0 - ju)
            u1, v1 = mapping(x0, y1 - ju)
            u2, v2 = mapping(x1, y1 - ju)
            u3, v3 = mapping(x1, y0 - ju)
            color = (255, 0, 0) if "10349603" in k else (0, 0, 255)
            combined = cv2.line(
                combined,
                (int(u0), int(v0 + frame_height)),
                (int(u1), int(v1 + frame_height)),
                color,
                2,
            )
            combined = cv2.line(
                combined,
                (int(u1), int(v1 + frame_height)),
                (int(u2), int(v2 + frame_height)),
                color,
                2,
            )
            combined = cv2.line(
                combined,
                (int(u2), int(v2 + frame_height)),
                (int(u3), int(v3 + frame_height)),
                color,
                2,
            )
            combined = cv2.line(
                combined,
                (int(u3), int(v3 + frame_height)),
                (int(u0), int(v0 + frame_height)),
                color,
                2,
            )
        out.write(combined[:, :, [2, 1, 0]].astype("uint8"))
    out.release()


if __name__ == "__main__":
    frames = get_frames(
        "/Users/rhall/downloads/R23594_big.MP4",
        spacing=60,
        viewport=(600, 1000, 500, 1400),
    )
    res.utils.logger.info(f"Loaded {len(frames)} frames.")
    printfile, manifest, _ = load_printfile("recO7P8NngnfTmBQt")
    printfile = buffer_printfile(printfile)
    assignments, inverted = get_best_assignment(frames, printfile)
    dump_alignment(
        frames,
        printfile,
        manifest,
        assignments,
        inverted,
        "/Users/rhall/downloads/R23594_big_annotated.MP4",
    )
