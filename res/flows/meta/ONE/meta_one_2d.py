from res.flows.dxa.res_color import get_one_label
from res.media.images.geometry import Polygon, swap_points
from res.media.images.outlines import (
    place_artwork_in_shape,
    draw_outline,
    get_piece_outline,
)
from shapely.wkt import loads
from res.flows.dxa.res_color import get_viable_locations, place_label_from_options
from PIL import Image
import pandas as pd
import res
from res.media.images.icons import op_symbol
import numpy as np


def safe_loads(g):
    try:
        if pd.notnull(g) and str(g).lower() not in ["nan", "none"]:
            return loads(g)
    except:
        return g


class meta_one_2d_piece:
    def __init__(self, record, parent):
        self._row = record
        self._vs = None
        self._parent = parent

    def __getitem__(self, key):
        return self._row.get(key)

    @property
    def piece_outline(self):
        return self._row["outline"]

    @property
    def notches(self):
        return self._row["notches"]

    @property
    def key(self):
        return self._row["key"]

    @property
    def piece_image(self):
        """
        the image is either loaded from the uri
        """
        placed_color_path = self._row.get("uri")
        if placed_color_path:
            return Image.fromarray(res.connectors.load("s3").read(placed_color_path))
        artwork_uri = self._row.get("artwork_uri")
        offset_buffer = self._row.get("offset_size_inches", 0) * 300
        if artwork_uri:
            ol = self.piece_outline
            if offset_buffer:
                res.utils.logger.info(f"apply offset buffer {offset_buffer/300}")
                ol = Polygon(ol).buffer(offset_buffer).exterior
            return place_artwork_in_shape(ol, artwork_uri)

    def get_one_number_and_group_symbol(self):
        one_number, group = self._parent._one_number, self._row.get("group_icon")
        if not one_number:
            return None
        lbl = get_one_label(one_number)
        try:
            gl = op_symbol(group)
            fact = lbl.size[1] / gl.size[1]
            new_size = (int(gl.size[0] * fact), lbl.size[1])
            gl = gl.resize(new_size)
            return Image.fromarray(np.concatenate([lbl, np.zeros_like(gl), gl], axis=1))
        except:
            res.utils.logger.warn(
                f"failed to get group icon for piece with group {group}"
            )
            return lbl

    def __repr__(self):
        return self.key

    @property
    def labelled_piece_image(self):
        im = self.piece_image
        label = self.get_one_number_and_group_symbol()
        # add cutline to outline
        if label is not None:
            try:
                vs = pd.DataFrame(
                    list(
                        get_viable_locations(
                            self.piece_outline,
                            self.notches,
                            # 35 is the offset from the edge to allow for the cutline
                            inside_offset_r=35,
                            label_width=label.size[1],
                        )
                    )
                )
                if len(vs):
                    im = place_label_from_options(
                        im, label, shift_margin=0, **dict(vs.iloc[50])
                    )
                else:
                    res.utils.logger.warn(f"Unable to place label")
            except Exception as ex:
                res.utils.logger.warn(f"Unable to add label {repr(ex)} ")

        # it may be we can be more efficient and use the outline that we load but they are slightly different maybe with a little cut line
        ol = swap_points(get_piece_outline(np.asarray(im)))
        # remove castle notches from the outline
        ol = Polygon(ol).buffer(50).buffer(-50).exterior

        im = Image.fromarray(draw_outline(np.asarray(im), outline_image_space=ol))

        return im


class MetaOne2d:
    def __init__(self, path, one_number=None):
        self._home_dir = path
        df = res.connectors.load("s3").read(path)
        self._one_number = one_number
        for col in ["outline", "notches"]:
            df[col] = df[col].map(safe_loads)
        self._pieces = [meta_one_2d_piece(row, self) for row in df.to_dict("records")]
        self._pieces = {p.key: p for p in self._pieces}
        self._key = res.utils.uuid_str_from_dict(
            {"path": path, "one_number": one_number}
        )
        self._data = df

    def validate(self):
        v = {}
        d = self._data

        try:

            v["PIECE NAMING"] = list(d[d["failed_to_name_color"]]["key"])
            v["COLOR ASSET VERSION"] = list(
                d[d["color_version"] != d["body_version"]]["key"]
            )

            dups = d.groupby("key")[["body_version"]].count()[["body_version"]]
            dups = list(dups[dups["body_version"] > 1].index)
            v["DUPLICATE PIECES"] = dups
        except Exception as ex:
            # this is really to catch a compliance issue for older versions
            v["EXCEPTION"] = repr(ex)

        return {k: v for k, v in v.items() if len(v) > 0}

    @property
    def sku_versioned(self):
        a = self._home_dir.split("/")
        return f"{a[4]}-{a[5]} {a[6]} {a[7]} {a[8]}".upper().replace("_", "-")

    @property
    def sku(self):
        a = self._home_dir.split("/")
        return f"{a[4]} {a[6]} {a[7]} {a[8]}".upper().replace("_", "-")

    def _repr_html_(self):
        return self._data._repr_html_()

    def __getitem__(self, key):
        return self._pieces[key]

    def __iter__(self):
        for _, v in self._pieces.items():
            yield v

    @staticmethod
    def try_load(
        body_code,
        body_version,
        material_code,
        color_code,
        size_code,
        one_number,
        **kwargs,
    ):
        """
        If we have created a meta one for a SKU and body version we can use the ONE number ot create a meta one at order time
        The PPP request has all of this information in it: res_meta.dxa.prep_pieces_requests

        The idea here for now is that this is development mode; we can override an intelligent one marker with this
        later when happy these are stored in a database and we turn of intelligent one marker creation
        """
        p = f"s3://res-data-platform/2d-meta-ones/{body_code.replace('-', '_')}/v{body_version}/{material_code}/{color_code}/{size_code}/pieces.feather".lower()
        res.utils.logger.debug(f"Searching for {p}")
        if res.connectors.load("s3").exists(p):
            return MetaOne2d(p, one_number)
        return None

    # interface
    def get_nesting_statistics(self, plot=False):
        """
        We should see how the pieces nest in the raw at least
        for physical maybe be careful with the knits
        """
        return {"piece_count": len(self._pieces)}

    @property
    def key(self):
        return self._key

    @property
    def is_valid(self):
        return True

    @property
    def piece_images(self):
        for piece in self:
            yield piece.piece_image

    @property
    def named_piece_images(self):
        for piece in self:
            yield piece["key"], piece.piece_image

    @property
    def labelled_piece_images(self):
        for piece in self:
            yield piece.labelled_piece_image

    @property
    def named_labelled_piece_images(self):
        for piece in self:
            yield piece["key"], piece.labelled_piece_image

    @property
    def piece_outlines(self):
        for piece in self:
            yield piece.piece_outline

    @property
    def named_piece_outlines(self):
        for piece in self:
            yield piece["key"], piece.piece_outline

    @property
    def notched_piece_image_outlines(self):
        for piece in self:
            yield piece.piece_outline

    @property
    def named_notched_piece_image_outlines(self):
        for piece in self:
            yield piece["key"], piece.piece_outline
