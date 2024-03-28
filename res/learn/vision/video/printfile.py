import res
import os
import subprocess
import numpy as np
import cv2
from pyvips import Image
from .printfile_matcher import load_printfile, MATCHER_X_RES
from .image import remove_transparency


class Printfile:
    def __init__(self, printfile_id):
        self.printfile_scaled, self.manifest, printfile_record = load_printfile(
            printfile_id
        )
        self.printfile_height = self.printfile_scaled.shape[0]
        self.printfile_record_id = printfile_id
        self.printfile_path = f"/tmp/{printfile_record['id']}.png"
        if not os.path.exists(self.printfile_path):
            res.utils.logger.info(
                f"Downloading full resolution printfile to {self.printfile_path}"
            )
            res.connectors.load("s3")._download(
                printfile_record["fields"]["Asset Path"] + "/printfile_composite.png",
                target_path="/tmp",
                filename=f"{printfile_record['id']}.png",
            )
        # figure out dimensions
        r = subprocess.run(["identify", self.printfile_path], capture_output=True)
        self.full_res_width, self.full_res_height = [
            int(x) for x in str(r.stdout).split(" ")[2].split("x")
        ]
        res.utils.logger.info(
            f"Full resolution printfile {self.printfile_path} has dimensions {self.full_res_width}, {self.full_res_height}."
        )
        self.mbb = {
            r["asset_key"]
            + ":"
            + r["piece_name"]: (
                r["min_x_inches"] / r["printfile_width_inches"],
                r["min_y_inches"] / r["printfile_height_inches"],
                r["max_x_inches"] / r["printfile_width_inches"],
                r["max_y_inches"] / r["printfile_height_inches"],
            )
            for _, r in self.manifest.iterrows()
        }

    def get_piece_image(self, key):
        piece_path = f"/tmp/{self.printfile_record_id}_{key.replace(':', '_')}.png"
        x0, y0, x1, y1 = self.mbb[key]
        if not os.path.exists(piece_path):
            sx0 = int(x0 * self.full_res_width)
            sy0 = int(y0 * self.full_res_height)
            sw = int((x1 - x0) * self.full_res_width)
            sh = int((y1 - y0) * self.full_res_height)
            Image.pngload(self.printfile_path).crop(sx0, sy0, sw, sh).flatten(
                background=[255, 255, 255]
            ).pngsave(piece_path)
        return cv2.imread(piece_path)[:, :, ::-1]  # dunno wtf is up with rgb
