"""
TODO: add construction input interface ingestion here

contract

use edge-ref and notch-ref to deescribed relative notches
self edges need this for start and end but for next prev edges we assume the notch ref (sa notches)


- the piece model scales to 300 dpi and we record templates in inches
- we can also use relative notch-ref for SA and facing notches e.g. 0,1 OR -1,-2 -> these are converted relative to the piece

#contract avoid notch_refer and edge_ref in places of hypens and if self make they are there
"""
import res
import re
import pandas as pd


# TODO body construction order pieces list

# these will be moved to some sort of config database but its quicker to iterate with them here
# NB: these templates are only used if we go through symbolized notch and normal seam ops in the switch - this is important
# because otherwise you only get edge operations (not notch etc.)
templates = {
    "s_fold_stitch_saeqfold_facing_1_4": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "position": "near_edge_center",
                # the seam allowance factor that we use to calc dx
                "dx-ref": 1,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
                # the seam allowance factor that we use to calc dx
                "dx-ref": 1,
            },
        ],
    },
    "s_fold_saeqfold_facing": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "position": "near_edge_center",
                # the seam allowance factor that we use to calc dx
                "dx-ref": 1,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
                # the seam allowance factor that we use to calc dx
                "dx-ref": 1,
            },
        ],
    },
    "s_sa_neq_overlap_plain_seam_ts_1": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "next",
                "operation": "overlap_plain_seam_ts",
                "dx": 300 * 1,
                "position": "near_edge_center",
            },
            {
                "edge-ref": "prev",
                "operation": "overlap_plain_seam_ts",
                "dx": 300 * 1,
                "position": "near_edge_center",
            },
        ],
    },
    "s_fold_stitch_sa_neq_fold_facing_0_25": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "dx": 300 * 0.25,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "dx": 300 * 0.25,
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
        ],
    },
    "s_fold_stitch_sa_neq_fold_facing_0_75": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "dx": 300 * 0.75,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "dx": 300 * 0.75,
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
        ],
    },
    "s_fold_stitch_sa_neq_fold_facing_1": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "dx": 300 * 1,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "dx": 300 * 1,
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/reclIKNFtAKLq8ATM?blocks=hide
    "s_fold_stitch_sa_neq_fold_facing": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "notch-ref": -2,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "notch-ref": 1,
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
        ],
    },
    "aeqb_1_4_backwards_double_fold_notch": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "notch-ref": -2,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "notch-ref": 1,
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
            },
        ],
    },
    "s_aeqb_1_4_fold_stitch_eq_fold_facing": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
                "dx": 0.0,
                # the seam allowance factor that we use to calc dx
                "dx-ref": 0.5,
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
                # the seam allowance factor that we use to calc dx
                "dx-ref": 0.5,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/rec69fuJnJNPlKCDA?blocks=hide
    "s_fold_stitch_sa_eq_fold_facing": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
                "dx": 0.25,
                # the seam allowance factor that we use to calc dx
                "dx-ref": 0.5,
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_corner",
                "dx": 0.25,
                # the seam allowance factor that we use to calc dx
                "dx-ref": 0.5,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/rec1pCuxFACGRjlh0?blocks=hide
    "s_fold_stitch_sa": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "notch-ref": 0,
                "position": "near_edge_center",
                "dx": 0,
            },
            {
                "edge-ref": "prev",
                # this will be an arrow
                "operation": "edge_stitch_fold_notch",
                "notch-ref": -1,
                "position": "near_edge_center",
                "dx": 0,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/recE6gAzcGntb6wKy?blocks=hide
    "s_fold_sa": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "notch-ref": 0,
                "position": "near_edge_center",
                "dx": 0,
            },
            {
                "edge-ref": "prev",
                # this will be an arrow
                "operation": "fold_notch",
                "notch-ref": -1,
                "position": "near_edge_center",
                "dx": 0,
            },
        ],
    },
    "s_invisible_stitch_sa": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "next",
                "operation": "invisible_stitch_notch",
                "notch-ref": 0,
                "position": "near_edge_center",
                "dx": 0,
            },
            {
                "edge-ref": "prev",
                # this will be an arrow
                "operation": "invisible_stitch_notch",
                "notch-ref": -1,
                "position": "near_edge_center",
                "dx": 0,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/recfxG5ITjpEF5p0x?blocks=hide
    "b_0_25_1n_boundary_ts": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "top_stitch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
            {
                "edge-ref": "self",
                "notch-ref": -1,
                "operation": "top_stitch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/recUA5HxYUeLXmz9F?blocks=hide
    "b_0_0625_1n_boundary_ts": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "top_stitch",
                "position": "near_edge_center",
                "dx": 0.0625,
            },
            {
                "edge-ref": "self",
                "notch-ref": -1,
                "operation": "top_stitch",
                "position": "near_edge_center",
                "dx": 0.0625,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/recRQ4z0Vlbu7FNRR?blocks=hide
    "s_0_25_2n_seam_ts": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "top_stitch",
                # "position": "near_edge_center",
                # "dx": 0.001,
            },
            {
                "edge-ref": "next",
                "operation": "top_stitch",
                # "position": "near_edge_center",
                # "dx": 0.001,
            },
            {
                "edge-ref": "prev",
                "operation": "top_stitch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
            {
                "edge-ref": "next",
                "operation": "top_stitch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
        ],
    },
    # https://airtable.com/appa7Sw0ML47cA8D1/tblrJiovxCF1QAZ2J/viwHbAPIqQcWFA3fM/recG1UNnlqF3Q0cN0?blocks=hide
    "s_sa_direction": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "sa_direction",
                "position": "near_edge_center",
                "dx": 0.115,
                "dy": -0.0625,
            },
            {
                "edge-ref": "next",
                "operation": "sa_direction",
                "position": "near_edge_center",
                "dx": 0.115,
                "dy": -0.0625,
            },
        ],
    },
    #########################################
    ##########################################
    # for the fuse block we use the ordered notches and we put symbols
    # before and after so they enclose the sectional edge
    "sa_piece_fuse": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        # these are examples where the 0 and -1 notches are relative to the notches defined on the sectional edge
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "operation": "sa_piece_fuse",
                "notch-ref": 0,
                "position": "before",
                "dx": 50,
            },
            {
                "edge-ref": "self",
                "operation": "sa_piece_fuse",
                "notch-ref": -1,
                "position": "after",
                "dx": 50,
            },
        ],
    },
    "back_apply_in_seam_main_label": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "operation": "back_apply_in_seam_main_label",
                "position": "top_center",
                "dy": 0.125 * 300,
            }
        ],
    },
    "top_apply_in_seam_main_label": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "operation": "top_apply_in_seam_main_label",
                "position": "top_center",
                "dy": 0.125 * 300,
            }
        ],
    },
    # this one is draw along the viable surface at a y offset
    # check contract for how far from the seam allowance is the sew line
    "var_offset_framing_top_stitch": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [
            {
                "edge_id": None,
                "operation": "var_offset_framing_top_stitch",
                "line": "dotted",
                "dy": 0,
            }
        ],
        "keypoint_annotations": [],
    },
    # simply an offset on the fold stitch
    "paired_0_0625_1n_i_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
                "dx": 0.06265,
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
                "dx": 0.06265,
            },
        ],
    },
    # doubles
    "paired_edge_fold_n_1_0_25_fold_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
            # add a second fold a=b ??
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "position": "near_edge_center",
                "dx": 2 * 0.25,
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
                "dx": 2 * 0.25,
            },
        ],
    },
    # this one seems to assume something rectangular ?
    "paired_fold_stitch_center_notch_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
            },
        ],
    },
    # this one seems to assume something rectangular ?
    "paired_edge_fold_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "notch_id": -1,
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
            },
        ],
    },
    # as above, assumes regular shape?
    "paired_fold_center_notch_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "notch-ref": -1,
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
            },
        ],
    },
    # fold stitch without offset
    "paired_edge_fold_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "notch-ref": -1,
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "notch-ref": 0,
                "position": "near_edge_center",
            },
        ],
    },
    "paired_0_0625_1n_u_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "notch_id": -1,
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "notch_id": 0,
                "position": "near_edge_center",
            },
        ],
    },
    "paired_edge_fold_stitch_n_0_fold_n_1_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
            },
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
                "dx": 2 * 0.25,
            },
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "position": "near_edge_center",
                "dx": 2 * 0.25,
            },
        ],
    },
    "paired_fold_n_1_fold_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "next",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
            },
            {
                "edge-ref": "prev",
                "operation": "edge_stitch_fold_notch",
                "position": "near_edge_center",
            },
            {
                "edge-ref": "next",
                "operation": "fold_notch",
                "position": "near_edge_center",
                "dx": 2 * 0.25,
            },
            {
                "edge-ref": "prev",
                "operation": "fold_notch",
                "position": "near_edge_center",
                "dx": 2 * 0.25,
            },
        ],
    },
    "paired_0_25_1n_u_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "edge_stitch_notch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
            {
                "edge-ref": "self",
                "notch-ref": -1,
                "operation": "edge_stitch_notch",
                "position": "near_edge_center",
                "dx": 0.25,
            },
        ],
    },
    "paired_0_0625_1n_u_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "edge_stitch_notch",
                "position": "near_edge_center",
                "dx": 0.06265,
            },
            {
                "edge-ref": "self",
                "notch-ref": -1,
                "operation": "edge_stitch_notch",
                "position": "near_edge_center",
                "dx": 0.06265,
            },
        ],
    },
    "paired_0_0625_1n_0_25_1n_i_stitch_n_0_seam": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "edge_stitch_notch",
                "position": "near_edge_center",
                "dx": 0.06265,
            },
            {
                "edge-ref": "self",
                "notch-ref": -1,
                "operation": "edge_stitch_notch",
                "position": "near_edge_center",
                "dx": 0.06265,
            },
        ],
    },
    "care_label_and_size_2_from_sa_notch": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "care_label_size_label_on_the_top",
                "position": "near_edge_center",
                "dx": 2 * 300,
            }
        ],
    },
    "care_label_7_5_from": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "care_label_size_label_on_the_top",
                "position": "near_edge_center",
                "dx": 7.5 * 300,
            }
        ],
    },
    "care_label_12_from": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "care_label_size_label_on_the_top",
                "position": "near_edge_center",
                "dx": 12 * 300,
            }
        ],
    },
    "care_label_18_from": {
        "edge_id": None,
        "piece_key": None,
        "edge_annotations": [],
        "keypoint_annotations": [
            {
                "edge-ref": "self",
                "notch-ref": 0,
                "operation": "care_label_size_label_on_the_top",
                "position": "near_edge_center",
                "dx": 18 * 300,
            }
        ],
    },
}


def load_symbols(reload=False):
    """
    Conventional cache and reload - until we move to database we have an S3 table lookup
    """
    return None


def get_templates():
    """
    db in future
    """
    return templates


def get_template_keys():
    return list(templates.keys())


def map_construction_keys(op_lookup):
    def f(s):
        def c(s):
            return s.lower().replace(" ", "")

        def map_names(s):
            s = c(s)

            if c("BOD+BTM") in s:
                return "bodice_pants"
            elif c("BTM") in s:
                return "pants"
            elif c("BOD") in s:
                return "bodice"
            elif c("SLV") in s:
                return "sleeve"
            elif c("collar") in s:
                return "collar"

        if isinstance(s, list):
            l = [f"{map_names(op_lookup.get(k,k))}" if k[:3] == "rec" else k for k in s]

            return l

    return f


def _split_construction_type(s):
    """
    the names usually are things like BOD-1-2 but we only care about the BOD-1 part

    construction types is a key that already has the truncation
    """
    if pd.isnull(s):
        return s

    return "-".join([c.rstrip().lstrip() for c in s.split("-")])  # [:-1]


def get_construction_sequences():
    airtable = res.connectors.load("airtable")
    ops = airtable["appa7Sw0ML47cA8D1"]["tblrJiovxCF1QAZ2J"]
    ops = ops.to_dataframe()[
        ["record_id", "Name", "Org. Structure"]  # , "Requires Additional Annotation"
    ]

    op_lookup = dict(ops[["record_id", "Name"]].values)
    op_resolver = map_construction_keys(op_lookup)

    data = airtable["appa7Sw0ML47cA8D1"]["tblIUAF6ufN2zuGpn"].to_dataframe(
        fields=["Construction Types", "Body Construction Order Symbol Sequence"]
    )

    # data = airtable["appa7Sw0ML47cA8D1"]["tblIUAF6ufN2zuGpn"].to_dataframe(
    #     fields=["Construction Types", "Body Construction Order Symbol Sequence"]
    # )
    data["body_construction_sequence"] = data[
        "Body Construction Order Symbol Sequence"
    ].map(op_resolver)

    def _clean_construction_type(s):
        if pd.isnull(s):
            return s

        return "-".join([c.rstrip().lstrip() for c in s.split("-")])  # [:-1]

    data["key"] = data["Construction Types"].map(_clean_construction_type)
    d = dict(data[["key", "body_construction_sequence"]].values)

    return d

    # we only need the first parts of the key e.g. Shirt-1-2 -> Shirt-1
    if pd.isnull(key):
        return None
    key = "-".join(key.split("-")[:2])
    d = {
        "BOD-2": ["bodice", "bodice", "sleeve", "bodice", "collar", "bodice"],
        "Shirt-2": ["bodice", "bodice", "sleeve", "bodice", "collar", "bodice"],
        "Shirt-3": ["bodice", "bodice", "sleeve", "collar", "bodice"],
        "BOD-3": ["bodice", "bodice", "sleeve", "bodice", "collar", "bodice"],
        "2BD-2": [
            "bodice",
            "sleeve",
            "bodice",
            "bodice_pants",
            "bodice",
            "collar",
            "bodice",
            "bodice",
        ],
        "2BD-BTM": ["pants", "pants", "pants", "bodice_pants"],
    }

    return d.get(key)


def load_template(name, eid, pkey, sectional_notches=None):
    def template_for_piece_edge(t, eid, pkey):
        t["edge_id"] = eid
        t["piece_key"] = pkey
        # not sure how we will want to handle them yet but we can use these explicitly to generate templates
        # for example rather than 0,-1 we now replace the first and last notch that defines the edge
        t["sectional_notches"] = sectional_notches
        for e in t.get("edge_annotations", []):
            e["edge_id"] = eid
        for e in t.get("keypoint_annotations", []):
            e["edge_id"] = eid
        return t

    # take a copy of the tep
    t = templates.get(name)

    if t is None:
        t = {"errors": ["TEMPLATE NOT FOUND"]}
    else:
        # take a copy of the template
        t = dict(t)
    t["name"] = name
    return template_for_piece_edge(t, eid, pkey)


def body_order_resolver(s):
    return None


def clean(s):
    """
    There are some special symbols uses in naming which are useful descrims
    but we want alphanumeric codes
    """

    def c(s):
        s = s.replace("≠", "neq")
        s = s.replace("=", "eq")
        s = s.replace("⟂", "B")
        s = s.replace("||", "S")

        s = s.replace("___" "", "var_")
        s = re.sub("[^0-9a-zA-Z]+", "_", s)
        s = s.lstrip("_").rstrip("_")
        return s.lower()

    if isinstance(s, list):
        return [c(i) for i in s]
    if isinstance(s, str):
        return c(s)


def sew_mapped_bodies(without_raw_preview=False):
    airtable = res.connectors.load("airtable")
    tab = airtable["appa7Sw0ML47cA8D1"]["tblUGsbA6ApGp3ZXc"]
    data = tab.to_dataframe(fields=["Name", "Body Number", "Raw Piece Geometries"])

    if without_raw_preview:
        res.utils.logger.debug("filtering cases without preview")

        data = data[data["Raw Piece Geometries"].isnull()]

    data["Body Number"] = data["Body Number"].map(
        lambda l: l[0] if pd.notnull(l) else None
    )

    return list(data["Body Number"].dropna().unique())


def load_table(body=None):
    airtable = res.connectors.load("airtable")
    ops = airtable["appa7Sw0ML47cA8D1"]["tblrJiovxCF1QAZ2J"]
    pieces = airtable["appa7Sw0ML47cA8D1"]["tbl4V0x9Muo2puF8M"]
    bodies = airtable["appa7Sw0ML47cA8D1"]["tblXXuR9kBZvbRqoU"]
    pp = bodies.to_dataframe(fields=["Body Number", "Body Name"])
    blookup = dict(pp[["record_id", "Body Number"]].values)

    ops = ops.to_dataframe()[
        ["record_id", "Name", "Org. Structure"]  # , "Requires Additional Annotation"
    ]
    op_lookup = dict(ops[["record_id", "Name"]].values)
    org_structure_lookup = dict(ops[["record_id", "Org. Structure"]].values)

    piece_lookup = dict(
        pieces.to_dataframe()[["record_id", "Generated Piece Code"]].values
    )

    def r(s):
        if isinstance(s, list):
            return [
                f"{org_structure_lookup.get(k)}::{op_lookup.get(k)}"
                if k[:3] == "rec"
                else k
                for k in s
            ]

    def piece_name_resolver(s):
        if isinstance(s, list):
            return [piece_lookup.get(k) if k[:3] == "rec" else k for k in s][0]

    def body_name_resolver(bs):
        try:
            return [blookup.get(s) for s in bs]
        except:
            return []

    # addons_lookup = dict(ops[["record_id", "Requires Additional Annotation"]].values)
    # addons_lookup = {op_lookup.get(k): r(v) for k, v in addons_lookup.items()}
    # addons_lookup = {k: v for k, v in addons_lookup.items() if v is not None}
    filters = filters = f"""OR(SEARCH("{body}",{{Body Number}}))""" if body else None
    tab = airtable["appa7Sw0ML47cA8D1"]["tblUGsbA6ApGp3ZXc"].to_dataframe(
        filters=filters
    )

    if len(tab) == 0:
        return tab

    print(f"We have {len(tab)} rows with filter {filters}")

    # tab["Body Number"] = tab["Body Number"].map(
    #     lambda x: x[0] if not pd.isnull(x) else None
    # )
    # support many mapping
    tab["Bodies Names"] = tab[
        "Body Number"
    ]  # tab["Bodies (from Piece)"].map(body_name_resolver)
    tab["relates_to_bodies"] = tab["Bodies Names"]
    tab = tab.explode("Bodies Names")
    tab["Body Number"] = tab["Bodies Names"]
    if body is not None:
        tab = tab[tab["Body Number"] == body].reset_index(drop=True)

    tab["piece_key"] = tab["Piece"].map(piece_name_resolver)
    drop_cols = [
        "Sew Symbol (Platform) (from Geometry Tag)",
        "Bodies Rollup (from Piece)",
        "Piece Identity Attributes",
        "Pieces Images",
        "__timestamp__",
        "Name",
        "Bodies (from Piece)",
        "Piece",
    ]

    # default airtable missing
    for c in ["Last Operated Edge"]:
        if c not in tab.columns:
            tab[c] = None

    drop_cols = [c for c in drop_cols if c in tab.columns]

    tab = tab.drop(
        drop_cols,
        1,
    )

    tab["Last Operated Edge"] = tab["Last Operated Edge"].map(
        lambda x: int(x.split("-")[-1]) if not pd.isnull(x) else None
    )

    tab = res.utils.dataframes.snake_case_columns(tab)

    for c in tab.columns:
        if c[:3] in ["se_", "op_"] or c[:2] == "e_":
            tab[c] = tab[c].map(r)
        # if c in ["misc"]:
        #     tab[c] = tab[c].map(r)

    tab["key"] = tab.apply(
        lambda row: f"{row['body_number']}-{row['piece_key']}", axis=1
    )

    return tab


def edge_parse(values, edge):
    items = []

    order = None
    has_seen_sa = False

    for c in values:
        d = {"edge_id": edge, "body_construction_order": None}

        cat, name = tuple(c.split("::"))
        # assumed that we see this first
        if cat == "Body Construction Order":
            order = name

        else:
            d["body_construction_order"] = order
            d["operation"] = name
            d["name"] = clean(name)
            d["category"] = clean(cat)
            if d["category"] == "sa_operation_symbol":
                has_seen_sa = True

        # when we passed the order we have a void op - but we can wait to see if we have another
        # later when we have multi we can change this logic
        if d.get("operation"):
            items.append(d)

    if not has_seen_sa and order:
        items.append(
            {
                "edge_id": edge,
                "body_construction_order": order,
                "category": "sa_operation_symbol",
                "operation": "void",
                "name": "res.void",
            }
        )

    return items


def op_parse(values):
    items = []
    edges = []
    ops = []
    for c in values:
        cat, name = tuple(c.split("::"))
        if cat == "Edge Labels":
            edges.append(int(name.split("-")[-1]))
        else:
            ops.append(name)

    for e in edges:
        for o in ops:
            items.append(
                {
                    "edge_id": e,
                    "operation": o,
                    "category": clean(cat),
                    "name": clean(o),
                }
            )
    return items


def parse_sectional_edges(values):
    """
    For example
        ['Edge Labels::E-1',
        'Derived Seam - Fold/Stitch::|| Fold/Stitch_SA',
        'Edge Labels::E-6',
        'Derived Seam - Fold/Stitch::|| Fold (Center-Notch )',
        'Derived Seam - Fold/Stitch::|| Fold_SA']

    """
    se_ops = []
    edges = []
    ops_visited = False
    notches = []
    for c in values:
        cat, name = tuple(c.split("::"))
        if cat == "Edge Labels":
            # reset after parsing ops
            # this is to allow multi edge attribution to symbols
            if ops_visited:
                edges = []
                ops_visited = False
            edges.append(int(name.split("-")[-1]))
            notches = []
        elif cat == "Notch Labels":
            notches.append(int(name.split("-")[-1]))
        else:
            # res.utils.logger.debug(
            #     f"Adding edge {edge_id} - operation {name} ({clean(name)})"
            # )
            for edge_id in edges:
                se_ops.append(
                    {
                        "operation": name,
                        "name": clean(name),
                        "category": clean(cat),
                        "edge_id": edge_id,
                        "notch1": notches[0] if len(notches) > 0 else None,
                        "notch2": notches[1] if len(notches) > 1 else None,
                    }
                )
            # flush the notch after we "use" it by smybols - this is a bot weird
            notches = []
            ops_visited = True
    return se_ops


def format_sew_dataframe(df, debug_stuff=False):
    ########
    op_cols = [ec for ec in df.columns if ec[:3] == "op_"]
    edge_cols = [ec for ec in df.columns if ec[:2] == "e_"]
    sedge_cols = [ec for ec in df.columns if ec[:3] == "se_"]

    piece_attributes = {}
    stuff = []

    res.utils.logger.info(
        f"Loading the construction sequences and formatting a dataframe of length {len(df)}"
    )
    construction_seqs = get_construction_sequences()

    for k, v in df.set_index("key").iterrows():
        # add the o-edge to the geometry_attributes
        # res.utils.logger.debug(f"Processing key {k}")
        # extract piece level stuff
        piece_attributes[k] = {"edges": []}
        # these are the piece fields we want to keep?
        for c in [
            "geometry_attributes",
            "internal_line",
            "last_operated_edge",
            "orientation_edge",
            "relates_to_bodies",
        ]:
            piece_attributes[k][c] = v.get(c)

        for c in dict(v).keys():
            # res.utils.logger.info(f"{c} to ....")
            # get the edges
            # this logic is VERY sensitive to column names so we need to refactor from this ref point
            if c in edge_cols:
                edge_id = int(c.split("_")[-1])
                val = v[c]
                if isinstance(val, list):
                    for d in edge_parse(val, edge_id):
                        d["key"] = k
                        d["body_code"] = v["body_number"]
                        d["type"] = "full_edge"
                        stuff.append(d)

            # get the ops
            elif c in op_cols:
                val = v[c]
                if isinstance(val, list):
                    # was formally op parse but not sure what is the difference
                    for d in parse_sectional_edges(val):
                        d["key"] = k
                        d["body_code"] = v["body_number"]
                        d["type"] = "full_edge_op"
                        stuff.append(d)

            # get the se edges
            elif c in sedge_cols:
                val = v[c]
                if isinstance(val, list):
                    for d in parse_sectional_edges(val):
                        d["key"] = k
                        d["body_code"] = v["body_number"]
                        d["type"] = "sectional_edge"
                        stuff.append(d)
            else:
                pass
                # res.utils.logger.debug(f"dont know what to do with {dict(v)}")

    if not len(stuff):
        res.utils.logger.debug("We did not find any sew data of interest, return None")
        return None

    df = pd.DataFrame(stuff)
    # print(df.head(1))
    res.utils.logger.info(f"Mapping the body order to a specific sequence...")

    res.utils.logger.info(f"{list(df.columns)}")

    if debug_stuff:
        return df

    def lookup_cs(s):
        if pd.isnull(s):
            return s

        k = "-".join([c.rstrip().lstrip() for c in s.split("-")][:-1])  #

        return construction_seqs.get(k)

    if "body_construction_order" not in df.columns:
        df["body_construction_order"] = None
    else:
        df["body_construction_sequence"] = df["body_construction_order"].map(lookup_cs)
    # df = df.where(pd.notnull(df), None)
    drop_list = [c for c in ["name", "edge_id", "notch1", "notch2"] if c in df.columns]
    res.utils.logger.debug(f"Updating {len(df)} records like {dict(df.iloc[0])}")
    for k, d in df.groupby("key"):
        # NB - dedup fon interface: the collection of edges fro one pieces is a frame here that we can prune
        d = d.drop_duplicates(subset=drop_list).to_dict("records")
        piece_attributes[k]["edges"] = d

    sew_df = pd.DataFrame(piece_attributes).T
    sew_df = sew_df.where(pd.notnull(sew_df), None)
    sew_df.index.name = "piece_key_no_version"
    sew_df.columns = [f"sew_{c}" for c in sew_df.columns]

    return sew_df.reset_index()


def add_body_pieces(body, plan=False):
    """ """
    from res.media.images.providers.dxf import PRINTABLE_PIECE_TYPES
    from res.connectors.airtable import AirtableConnector

    s3 = res.connectors.load("s3")
    airtable = res.connectors.load("airtable")

    def try_get_piece_names_for_body(body):
        body_lower = body.lower().replace("-", "_")
        dxf = s3.read(
            [
                d
                for d in list(
                    s3.ls(
                        f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_lower}"
                    )
                )
                if ".dxf" in d
            ][0]
        )
        res.utils.logger.debug(f"Using dxf {dxf._home_dir}")
        c = dxf.compact_layers
        l = list(c[c["type"].isin(PRINTABLE_PIECE_TYPES)]["piece_name"].unique())
        if len(l):
            cl = lambda s: "-".join(s.split("-")[3:])
            l = [cl(s) for s in l]
        return l

    def get_piece_ids(codes):
        pred = AirtableConnector.make_key_lookup_predicate(
            codes, "Generated Piece Code"
        )
        tab = airtable["appa7Sw0ML47cA8D1"]["tbl4V0x9Muo2puF8M"]
        data = tab.to_dataframe(filters=pred, fields=["Name", "Generated Piece Code"])
        rids = list(data["record_id"])
        print(rids)
        return rids

    piece_names = try_get_piece_names_for_body(body)
    data = load_table(body)

    pieces = (
        [] if len(data) == 0 else data["key"].map(lambda s: "-".join(s.split("-")[2:]))
    )
    s = set(piece_names) - set(pieces)
    res.utils.logger.debug(f"Adding pieces from set: {s}")
    if len(s) > 0:
        print(f"we need to add {s}")
        if not plan:
            vals = get_piece_ids(list(s))
            for v in vals:
                print("adding", v)
                tab = airtable["appa7Sw0ML47cA8D1"]["tblUGsbA6ApGp3ZXc"].update_record(
                    {"Piece": v, "Body Number": body}
                )

    # determine missing pieces

    return data


def get_sew_assignation_for_body(
    body=None,
    clone_as=None,
    transfer_to_datalake=True,
    debug_stuff=False,
    ensure_pieces=False,
):
    """
    Load and format the sew assignation data
    piece level result with embedded edges
    """

    if ensure_pieces and body:
        try:
            add_body_pieces(body)
        except Exception as ex:
            print(f"Failed to add pieces {ex}")

    df = load_table(body)

    # if we have nothing try add them anyway
    # if len(df) == 0:
    #     try:
    #         add_body_pieces(body)
    #     except Exception as ex:
    #         print(f"Failed to added pieces {ex}")

    res.utils.logger.debug(f"Loading {len(df)} records")

    if len(df) == 0:
        return df

    def map_key(k, clone):
        b = k.split("-")[:2]
        rest = "-".join(k.split("-")[2:])
        return f"{clone}-{rest}"

    if clone_as:
        df["key"] = df["key"].map(lambda x: map_key(x, clone_as))

    df = format_sew_dataframe(df, debug_stuff=debug_stuff)

    if transfer_to_datalake and df is not None:
        res.utils.logger.info(f"Should try sync sew dataframe for body {body}")
        if body is not None:
            s3 = res.connectors.load("s3")
            body_lower = body.replace("-", "_").lower()
            path = f"s3://meta-one-assets-prod/bodies/sew/{body_lower}/edges.feather"
            res.utils.logger.info(f"Writing data to {path}")
            s3.write(path, df)

    return df


def handle_sew_bot_requests():
    """
    Bot requests are here
    https://airtable.com/appa7Sw0ML47cA8D1/tblZrP2omIoDE7fsL/viwbQGeJW3gxXmY7L?blocks=hide

    Currently we support hard and soft resets which resync the meta one and sew interface possible with a re-export of the BW file on create one
    """
    from res.connectors.airtable import AirtableConnector
    from res.flows.meta.bodies import get_versioned_body_details

    def get_body_ids(bodies):
        if len(bodies):
            airtable = res.connectors.load("airtable")
            pr = AirtableConnector.make_key_lookup_predicate(bodies, "Body Number")
            bodies = airtable["appa7Sw0ML47cA8D1"]["tblXXuR9kBZvbRqoU"]
            bodies = bodies.to_dataframe(filters=pr)
            if len(bodies):
                return dict(bodies[["Body Number", "record_id"]].values)
        return {}

    airtable = res.connectors.load("airtable")
    kafka = res.connectors.load("kafka")
    null = None

    tab = airtable["appa7Sw0ML47cA8D1"]["tblZrP2omIoDE7fsL"]
    sdata = tab.to_dataframe(fields=["Name", "Bot Request", "Body Number"]).dropna()
    if len(sdata) == 0 or "Bot Request" not in sdata.columns:
        res.utils.logger.info(f"Nothing to do - no bot requests returned")
        return

    sdata["Body Number"] = sdata["Body Number"].map(lambda s: s[0])

    hard_refreshes = sdata[sdata["Bot Request"] == "Hard Refresh"]
    soft_refreshes = sdata[sdata["Bot Request"] == "Soft Refresh"]

    bodies = get_body_ids(
        list(hard_refreshes["Body Number"]) if len(hard_refreshes) > 0 else []
    )

    print(bodies)

    for record in soft_refreshes.to_dict("records"):
        res.utils.logger.info(f"Soft refershing {record['Body Number']}")
        get_versioned_body_details(
            record["Body Number"],
            None,
            as_body_request=True,
            sample_size_only=True,
            send_test_requests=True,
        )

        tab.update_record({"record_id": record["record_id"], "Bot Request": None})

    for record in hard_refreshes.to_dict("records"):
        res.utils.logger.info(f"Hard refreshing {record['Body Number']}")
        # lookup the body id from the map
        body_id = bodies.get(record["Body Number"])
        if body_id:
            p = {
                "event_type": "CREATE",
                "requestor_id": body_id,
                "job_type": "DXA_EXPORT_BODY_BUNDLE",
                "status": null,
                "status_detail": null,
                "result_s3_uri": null,
            }

            kafka["res_platform.job.event"].publish(p, use_kgateway=True)

            tab.update_record({"record_id": record["record_id"], "Bot Request": None})

    res.utils.logger.info("Done")


# TODO - we can push some of this stuff to the airtable connector but requires some planning

# def column_resolver_factor(table_id, name, category=None, 'sep="::'):
#     #lookup the table and use the name with or without category
#     pass

# #for each column you can specify column resolvers that do a name and possile category name lookup on another table
# #this can be added to the to_dataframe method to update the dataframe
# #add a snake care column property to data frame
# #
