import res
from res.media.images.convert import rasterize_png
from res.flows import FlowContext
from res import utils
import pandas as pd
from pathlib import Path
from itertools import islice
import json


DEFAULT_DPI = 25


def chunk(it, size):
    "pagination tool"
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


def _placement_file_ids(fc):
    def parse_one(s):
        try:
            return json.loads(s)[0]["fileId"]
        except:
            return None

    SELECT_STYLE_FIELDS = [
        "Brand_Name",
        "Body Code",
        "Key",
        "__colorcode",
        "__materialcode",
        "__materialname",
        "_print_placement_files",
    ]

    fc = FlowContext({}, {})
    db = fc.connectors["mongo"]["resmagic"]
    select_fields = [f"legacyAttributes.{f}" for f in SELECT_STYLE_FIELDS]
    styles = pd.DataFrame(db["styles"].find({}, {k: 1 for k in select_fields}))
    styles = styles[["_id"]].join(
        [pd.DataFrame([r for r in styles["legacyAttributes"]])]
    )

    styles["sample_file"] = styles["_print_placement_files"].map(parse_one)
    return styles[styles["sample_file"].notnull()][["_id", "sample_file"]].rename(
        columns={"_id": "style_id"}
    )


def get_files_needing_thumbnails(event, context):
    """ """

    batch_size = event.get("args", {}).get("batch_size", 10)
    dpi = event.get("args").get("dpi", DEFAULT_DPI)

    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]
        db = fc.connectors["mongo"]["resmagic"]
        files = pd.DataFrame(db["files"].find())
        "illustrator files"
        files = files[files["extension"].str.lower() == ".ai"]
        files["_id"] = files["_id"].map(str)

        res.utils.logger.info(f"filtering to find samples per style")
        files = pd.merge(
            files, _placement_file_ids(fc), left_on=["_id"], right_on=["sample_file"]
        )

        # this is the res-key for saving thumbnails and checking if they exist
        files["key"] = files["style_id"].map(str)
        res.utils.logger.info(f"found {len(files)} placement files for styles")
        root = fc.get_node_path_for_key(f"thumbnails/dpi_{dpi}", "")
        res.utils.logger.info(f"Getting list of processed thumbnails at {root} ...")
        existing = pd.DataFrame(s3.ls(root), columns=["__thumbnail_path"])
        # use the dpi/key/thumbnail.png e.g. part[-2]
        existing["key"] = existing["__thumbnail_path"].map(
            lambda x: str(Path(x).parts[-2])
        )
        res.utils.logger.info(
            f"There are {len(existing)} processed thumbnails. Processing new..."
        )
        files = pd.merge(files, existing, on="key", how="left")
        print(existing[["key"]].head())
        print(files[["key"]].head())
        files = files[files["__thumbnail_path"].isnull()]
        res.utils.logger.info(
            f"There are {len(files)} files needing thumbnails. Starting to process in chunks..."
        )
        # do 100 at a time
        files = files.reset_index()[:200]

        chunks = []
        for idx in chunk(files.index, size=batch_size):
            batch = []
            for record in files.iloc[list(idx)].to_dict("records"):
                s3_info = record["s3"]
                bucket = s3_info["bucket"]
                file = s3_info["key"]
                batch.append(
                    {"key": record["style_id"], "name": f"s3://{bucket}/{file}"}
                )
                chunks.append(batch)
        return chunks


def make_thumbnails(event, context):
    """
    Currently assumes file is an adobe illustrator file and will make a thumbnail using the dpi setting

    {
       "apiVersion": "v0",
       "kind": "resFlow",
       "metadata": {"name": "dxa.process_images", "version": "dev"},
       "args":
       {
           "assets": [
               {"name":"s3://", "target":"optional", "key":"uid"}
               {"name":"s3://"},
                     ],
           "jobKey": "job1",
           "dpi":25
       },
       "nodes": null
       }
    """

    with FlowContext(event, context) as fc:
        assets = event.get("args").get("assets")
        dpi = event.get("args").get("dpi", DEFAULT_DPI)
        for a in assets:
            source = a["name"]
            key = a.get("key", utils.hash_of(source))
            target = a.get(
                "target",
                fc.get_node_path_for_key(
                    "thumbnails", key=f"dpi_{dpi}/{key}/thumbnail.png"
                ),
            )
            res.utils.logger.info(
                f"Making thumbnail: file {source} will be saved to {target} with dpi {dpi}"
            )
            rasterize_png(source, target, dpi=dpi)
