import os
import res
from warnings import filterwarnings

filterwarnings("ignore")
# misnomer but this is where we store our dxf stuff
# simple wrapper that can find some examples in 2d or 3d based on conventions
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import math
import io
from PIL import Image
from res.media.images.geometry import geom_len
from shapely.affinity import translate

s3 = res.connectors.load("s3")

DIFF_COLORS = ["y", "b", "r", "g", "y", "b", "r", "g"]


class Diff(object):
    # TODO add some option for dealing with bodies/styles as the folder
    # structure will be different
    def __init__(
        self,
        browzwear_path,
        preload=False,
        show_progress=False,
        plot=False,
        preferred_sizes=None,
    ):
        """
        pass preferred sizes to filter the comparison e.g. if there is a change in size scale you can ignore older sizes
        """
        self.browzwear_path = browzwear_path

        self._bw_versions = s3.get_versions(browzwear_path)
        self._base_dir = os.path.dirname(browzwear_path)
        self._dxf_dir = os.path.join(self._base_dir, "extracted/dxf_by_size")
        self._preferred_sizes = preferred_sizes
        self._dxf_paths = list(
            filter(
                lambda s: s.endswith("dxf"), list(s3.ls(self._dxf_dir, suffixes=".dxf"))
            )
        )
        # if we have specified preferred sizes in the normed format then we can filter them here - conventionally the sizes is the -2 index of the path
        if self._preferred_sizes:
            res.utils.logger.debug(f"Filtering by preferred sizes {preferred_sizes}")
            self._dxf_paths = list(
                filter(
                    lambda s: s.split("/")[-2] in self._preferred_sizes,
                    list(self._dxf_paths),
                )
            )
        self._dxf_files_by_path = {}
        self._dxf_history_by_key = {}

        if preload:
            self._preload()

        if show_progress:
            from tqdm import tqdm

            self._tqdm = tqdm
        else:
            self._tqdm = lambda x: x

        self._plot = plot

    def _read_dxf(self, path):
        try:
            if path not in self._dxf_files_by_path:
                self._dxf_files_by_path[path] = s3.read(path)
        except Exception as x:
            res.utils.logger.debug(f"Could not read {path}: {x}")

        return self._dxf_files_by_path[path]

    def _preload(self):
        for path in self._tqdm(self._dxf_paths):
            self._read_dxf(path)

    # TODO probably a much better way to get e.g. 2X from extracted/dxf_by_size/2X
    def sizing_of_dxf(self, dxf_path):
        return os.path.dirname(dxf_path)[len(self._dxf_dir) + 1 :]

    def get_dxf_history(self, key="last_modified"):
        if key in self._dxf_history_by_key:
            return self._dxf_history_by_key[key]

        dxf_versions_by_filename = {
            path: s3.get_versions(path) for path in self._dxf_paths
        }
        history = pd.DataFrame(
            {"browzwear_date": [v["last_modified"] for v in self._bw_versions]}
        )

        for dxf_path in self._tqdm(self._dxf_paths):
            bw_keys = []
            dxf_keys = []

            i = 0
            for bv in self._bw_versions:
                dxf_versions = dxf_versions_by_filename[dxf_path]

                bw_keys.append(bv["last_modified"])
                dxf_key_array = []
                while (
                    i < len(dxf_versions)
                    and dxf_versions[i]["last_modified"] > bv["last_modified"]
                ):
                    dxf_key_array.append(dxf_versions[i][key])
                    i += 1

                # 0 is the newest file before we hit an older version of the bw file
                dxf_keys.append(dxf_key_array[0] if len(dxf_key_array) > 0 else None)

            size = self.sizing_of_dxf(dxf_path)
            dxf_history = pd.DataFrame({"browzwear_date": bw_keys, size: dxf_keys})

            history = history.merge(
                dxf_history, on="browzwear_date", how="outer", copy=False
            )

        self._dxf_history_by_key[key] = history

        return history

    def filter_pieces_and_notches(self, dxf):
        cl = dxf.compact_layers
        return cl[(cl["layer"] == 1) | (cl["layer"] == 4)]

    def pieces_and_notches_diff(
        self, dxf_a, dxf_b, outline_overlap_tolerance=0, save_images=False
    ):
        dxf_a = self.filter_pieces_and_notches(dxf_a)
        dxf_b = self.filter_pieces_and_notches(dxf_b)

        # if outline_overlap_tolerance > 0:
        #     # filter these dataframes to only include where the type is 'block_fuse' or 'self'
        #     dxf_a = dxf_a[dxf_a["type"].isin(["block_fuse", "self"])]
        #     dxf_b = dxf_b[dxf_b["type"].isin(["block_fuse", "self"])]

        return self.get_geom_changes(
            dxf_a,
            dxf_b,
            outline_overlap_tolerance=outline_overlap_tolerance,
            save_images=save_images,
        )

    def get_geom_changes(
        self,
        dxf_a_compact_layers,
        dxf_b_compact_layers,
        outline_overlap_tolerance=0,
        save_images=False,
    ):
        # verbose param names are a hint for the caller
        a = dxf_a_compact_layers
        b = dxf_b_compact_layers

        def merge_pieces_and_notches(df):
            pieces = df[df["layer"] == 1]
            notches = df[df["layer"] == 4]

            return pieces.merge(
                notches, on="key", how="outer", suffixes=("_piece", "_notches")
            )

        a = merge_pieces_and_notches(a)
        b = merge_pieces_and_notches(b)

        def default_geom(geom):
            if geom is None or isinstance(geom, float) or geom.is_empty:
                return None
            return geom

        diffs = 0
        results = []
        alen = len(a)
        blen = len(b)
        total = max(len(a), len(b))
        for i in range(0, total):
            a_ok = i < alen
            b_ok = i < blen

            row_a = a.iloc[i] if a_ok else None
            row_b = b.iloc[i] if b_ok else None

            piece_a_geom = default_geom(row_a["geometry_piece"]) if a_ok else None
            piece_b_geom = default_geom(row_b["geometry_piece"]) if b_ok else None

            notches_a_geom = default_geom(row_a["geometry_notches"]) if a_ok else None
            notches_b_geom = default_geom(row_b["geometry_notches"]) if b_ok else None

            if outline_overlap_tolerance > 0:
                try:
                    # the area that intersects should be similar to the area of one of the pieces
                    a_buffered = piece_a_geom.buffer(1)
                    b_buffered = piece_b_geom.buffer(1)
                    ab_intersection = a_buffered.intersection(b_buffered)

                    # if there's no intersection at all, move them both to the origin to see
                    if ab_intersection.is_empty:
                        a_bounds = piece_a_geom.bounds
                        shift_a = lambda x: translate(x, -a_bounds[0], -a_bounds[1])
                        a_buffered = shift_a(a_buffered)
                        piece_a_geom = shift_a(piece_a_geom)
                        notches_a_geom = shift_a(notches_a_geom)

                        b_bounds = piece_b_geom.bounds
                        shift_b = lambda x: translate(x, -b_bounds[0], -b_bounds[1])
                        b_buffered = shift_b(b_buffered)
                        piece_b_geom = shift_b(piece_b_geom)
                        notches_b_geom = shift_b(notches_b_geom)

                        ab_intersection = a_buffered.intersection(b_buffered)

                    is_different = not (
                        math.isclose(
                            ab_intersection.area,
                            a_buffered.area,
                            rel_tol=outline_overlap_tolerance,
                        )
                        and math.isclose(
                            ab_intersection.area,
                            b_buffered.area,
                            rel_tol=outline_overlap_tolerance,
                        )
                    )
                except:
                    is_different = True
            else:
                is_different = (
                    piece_a_geom != piece_b_geom or notches_a_geom != notches_b_geom
                )
            if is_different:
                diffs += 1

                ######### PIECES
                piece_diff_ab = None
                piece_diff_ba = None
                if piece_a_geom and piece_b_geom:
                    piece_diff_ab = default_geom(piece_a_geom.difference(piece_b_geom))
                    piece_diff_ba = default_geom(piece_b_geom.difference(piece_a_geom))

                ######### NOTCHES
                notches_diff_ab = None
                notches_diff_ba = None
                if notches_a_geom and notches_b_geom:
                    notches_diff_ab = default_geom(
                        notches_a_geom.difference(notches_b_geom)
                    )
                    notches_diff_ba = default_geom(
                        notches_b_geom.difference(notches_a_geom)
                    )

                df = pd.DataFrame(
                    {
                        "Desc": [
                            "old piece",
                            "new piece",
                            "removed from piece",
                            "added to piece",
                            "old notches",
                            "new notches",
                            "removed from notches",
                            "added to notches",
                        ],
                        "geometry": [
                            piece_b_geom,
                            piece_a_geom,
                            piece_diff_ba,
                            piece_diff_ab,
                            notches_b_geom,
                            notches_a_geom,
                            notches_diff_ba,
                            notches_diff_ab,
                        ],
                    }
                )

                title = row_a["key"] if a_ok else row_b["key"]
                if a_ok != b_ok:
                    title += f' (in {"A" if a_ok else "B"} only)'
                if self._plot or save_images:
                    from geopandas import GeoDataFrame

                    gdf = GeoDataFrame(df)
                    plt.figure(figsize=(20, 20))
                    axes = plt.subplot(1, 1, 1)
                    gdf.plot(ax=axes, color=DIFF_COLORS)
                    axes.set_title(title)

                    if not save_images:
                        plt.show()
                        results.append(title)
                    else:
                        title = row_a["key"] if a_ok else row_b["key"]
                        path = os.path.join(
                            self._base_dir, f"extracted/prev_version_diffs/{title}.png"
                        )

                        data = io.BytesIO()
                        plt.savefig(data, dpi=150, bbox_inches="tight", format="png")

                        s3.save(path, Image.open(data))
                        plt.close()
                        results.append(path)
                else:
                    results.append(title)

        return results

    # TODO must be an existing better way to do this?
    def sizing_of_dxf(self, dxf_path):
        return os.path.dirname(dxf_path)[len(self._dxf_dir) + 1 :]

    def get_notch_comparison_df(self):
        def get_notches_by_piece_df(dxf_path):
            dxf = self._read_dxf(dxf_path)
            cl = dxf.compact_layers

            def sizeless_key(key):
                return "".join(key.split("_")[:-1])

            total_pieces = cl[cl["layer"] == 1]
            total_pieces = pd.DataFrame(
                {
                    "sizeless_key": ["total pieces"],
                    "type": [""],
                    "count": [len(total_pieces)],
                }
            )

            notches = cl[cl["layer"] == 4]
            total_notches = pd.DataFrame(
                {
                    "sizeless_key": ["total notch layers"],
                    "type": [""],
                    "count": [len(notches)],
                }
            )

            notches["count"] = notches.apply(
                lambda row: geom_len(row["geometry"]), axis=1, result_type="reduce"
            )
            notches["sizeless_key"] = notches.apply(
                lambda row: sizeless_key(row["key"]), axis=1, result_type="reduce"
            )

            notches = notches[["sizeless_key", "type", "count"]]

            return pd.concat([total_pieces, total_notches, notches])

        # create one df to hold all the notches per piece
        df = pd.DataFrame({"sizeless_key": [], "type": [], "count": []})

        # for each dxf add its notch stats to the general df
        for dxf_path in self._tqdm(self._dxf_paths):
            size = self.sizing_of_dxf(dxf_path)
            stats = get_notches_by_piece_df(dxf_path)

            df = df.merge(
                stats, on=["sizeless_key", "type"], how="outer", suffixes=["", "_new"]
            )
            df = df.rename(columns={"count_new": size})

        df = df.drop(["count"], axis=1)

        return df

    def generate_notch_comparison_table(self, path=None):
        notch_comparison_df = self.get_notch_comparison_df()
        [key, piece_type, *sizes] = list(notch_comparison_df.keys())

        # sort by type then combine the type with the key
        notch_comparison_df = notch_comparison_df.sort_values(by="type")
        notch_comparison_df[key] = (
            notch_comparison_df[key] + " " + notch_comparison_df[piece_type]
        )
        notch_comparison_df = notch_comparison_df.drop("type", axis=1)

        # instead of seaborn's default heat map, transform it so each cell in a
        # row has a different value if it hasn't been seen before so for example
        # 10 10 10 11 10 12 turns into 0 0 0 1 0 2
        def changes_per_row(row):
            new_row = []
            observed_values = {}
            i = 0
            for value in row:
                if value not in observed_values:
                    observed_values[value] = i
                    i += 1
                new_row.append(observed_values[value])

            return new_row

        value_labels = notch_comparison_df[sizes].to_numpy()
        value_labels[np.isnan(value_labels)] = 0
        relative_changes = np.apply_along_axis(changes_per_row, 1, value_labels)

        y_labels = notch_comparison_df[key].to_numpy()
        x_labels = list(notch_comparison_df.columns)[1:]

        # 3/4 inch per col, 1/4 inch per row
        sns.set(
            rc={"figure.figsize": (2 + 0.75 * len(x_labels), 1 + 0.25 * len(y_labels))}
        )

        # green if there are no differences, red otherwise
        no_differences = relative_changes.sum() == 0
        palette = "light:g" if no_differences else "light:r"
        ax = sns.heatmap(
            relative_changes,
            annot=value_labels,
            xticklabels=x_labels,
            yticklabels=y_labels,
            cmap=sns.color_palette(palette),
        )
        title = str(self._bw_versions[0]["last_modified"])
        ax.set(xlabel="sizings", ylabel="pieces", title=title)
        ax.tick_params(top=True, labeltop=True)

        figure = ax.get_figure()
        if not self._plot:
            plt.close(figure)

        if path is None:
            path = os.path.join(self._base_dir, "extracted/notch_comparison.png")

        data = io.BytesIO()
        figure.savefig(data, dpi=300, bbox_inches="tight", format="png")

        s3.save(path, Image.open(data))
        return {"image_path": path, "notch_counts_valid": no_differences}

    def save_all_dxf_diff_images(self):
        dxf_history = self.get_dxf_history()
        # [bw_date, *sizes] = dxf_history_ids.columns

        for dxf_path in self._tqdm(self._dxf_paths):
            size = self.sizing_of_dxf(dxf_path)
            dxf_history_for_size = dxf_history[["browzwear_date", size]]
            dxf_dir = os.path.dirname(dxf_path)

            # compare the most recent dxf...
            newest_dxf = self._read_dxf(dxf_path)
            newest_date = dxf_history_for_size.iloc[0][size]

            # with the next *valid* dxf (we may have to go back if can't read dxf)
            previous_date = None
            i = 1  # start after the latest one
            while i < len(dxf_history_for_size):
                date_history_row = dxf_history_for_size.iloc[i]
                i += 1
                previous_date = date_history_row[size]
                if not pd.isnull(previous_date):
                    try:
                        dxf = s3.read(dxf_path, at=previous_date)
                        previous_dxf = dxf
                        previous_date
                        break
                    except Exception as x:
                        res.utils.logger.debug(
                            f"Invalid dxf at: {previous_date} path: {dxf_path} ex: {x}"
                        )

            if (newest_dxf or previous_dxf) is None:
                res.utils.logger.debug(f"Cannot compare path: {dxf_path}")
                continue

            gdfs = self.pieces_and_notches_diff(newest_dxf, previous_dxf)

            def format_date(date):
                return str(date).replace(" ", "_").replace(":", "-")

            newest_date = format_date(newest_date)
            previous_date = format_date(previous_date)
            diff_dir = os.path.join(dxf_dir, f"diffs/{newest_date}/{previous_date}")

            for gdf_and_title in self._tqdm(gdfs):
                gdf = gdf_and_title["gdf"]
                title = gdf_and_title["title"]

                ax = gdf.plot(color=DIFF_COLORS)
                ax.set_title(title)

                fig = ax.get_figure()
                # fig.set_size_inches(20,20)

                data = io.BytesIO()
                fig.savefig(data, dpi=300, bbox_inches="tight", format="png")

                image = Image.open(data)
                image_path = os.path.join(diff_dir, title + ".png")

                s3.save(image_path, image)

                # don't plot it here
                if not self._plot:
                    plt.close(fig)
