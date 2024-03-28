"""Helpers for asset bundles exported by VStitcher."""


from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import json
import os


@dataclass
class _VStitcherPieceFile:
    path: str
    vstitcher_name: str
    vstitcher_num: str
    matched_vs_name: Optional[str]
    ext: str
    size: str
    size_category: Optional[str]
    airtable_size: str


def get_pattern_pieces(bundle_path: Path):
    """
    Return an inventory of found pattern pieces.

    VStitcher exports pieces by piece_number, colorway, and size, so
    we require a mapping file to retrieve the full piece name, as a VStitcher
    user would see it.
    """
    pattern_pieces_path = bundle_path / "pattern-pieces"
    if not pattern_pieces_path.exists():
        raise Exception("Could not find pattern-pieces directory!")

    piece_nums_to_names = json.loads(
        open(bundle_path / "piece_name_mapping.json", "r").read()
    )
    pieces = []
    for piece_file_name in os.listdir(pattern_pieces_path):
        """
        files will be in the format `17-Colorway1-XSM.pdf` where the num
        corresponds to the number of the piece in VStitcher
        """
        path = pattern_pieces_path / piece_file_name
        full_name, ext = piece_file_name.split(".")
        piece_num, _, size_name = full_name.split("-")

        "e.g KT-6041-V7-DRSFTPNL-S_P or KT-6041-V7-DRSFTPNL-S"
        found_vstitcher_name = piece_nums_to_names.get(piece_num)

        if found_vstitcher_name is None or " " in found_vstitcher_name:
            pieces.append(
                _VStitcherPieceFile(
                    path=str(path),
                    vstitcher_name=full_name,
                    vstitcher_num=piece_num,
                    matched_vs_name=None,
                    ext=ext,
                    size=size_name,
                    size_category=None,
                    airtable_size=size_name,
                )
            )
        else:
            vstitcher_name_parts = found_vstitcher_name.split("_")
            vstitcher_name = (
                vstitcher_name_parts[0]
                if len(vstitcher_name_parts) == 2
                else found_vstitcher_name
            )
            size_category_maybe = (
                vstitcher_name_parts[1] if len(vstitcher_name_parts) == 2 else None
            )
            airtable_size = (
                f"{size_category_maybe}-{size_name}"
                if size_category_maybe is not None
                else size_name
            )

            pieces.append(
                _VStitcherPieceFile(
                    path=str(path),
                    vstitcher_name=vstitcher_name,
                    vstitcher_num=piece_num,
                    matched_vs_name=found_vstitcher_name,
                    ext=ext,
                    size=size_name,
                    size_category=size_category_maybe,
                    airtable_size=airtable_size,
                )
            )
    return pieces


def pattern_pieces_by_airtable_size(bundle_path: Path):
    """Group pieces by category-size, e.g. SM_P or just SM ."""
    by_size = {}
    for p in get_pattern_pieces(bundle_path):
        if p.airtable_size in by_size:
            by_size[p.airtable_size].append(p)
        else:
            by_size[p.airtable_size] = [p]
    return by_size
