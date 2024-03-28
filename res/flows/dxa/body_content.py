"""
this flow batch syncs content files from bodies as stored as links in Airtable to S3
Some content exists on box and is moved to S3
We use the existence of the S3 target path to trigger action
- if the .dxf (astm) file does not exist in the s3 target, we copy from box to s3 target
- if the .ai setup file(s) do not exist on s3 target, we copy from the original s3 location to the target
- if the PNGs rasterized for a particular DPI value do not exist at target, we rasterize the .ai files at DPI and save to target

By the process we would like to maintain up-to-date content files for all bodies that can be used downstream
"""

from ast import literal_eval
from res.flows.dxa.process_images import DEFAULT_DPI
import res
from res.flows import FlowContext
from res.media.images.convert import rasterize_png
import pandas as pd
from pathlib import Path


def make_path(p):
    return f"s3://{p['bucket']}/{p['key']}"


def make_astm_destination(row):
    body_number = row["Body Number"]
    file_name = "file.dxf"
    return f"s3://one-meta-artifacts/v0/bodies/{body_number}/astm_files/{file_name}"


def make_setup_destination(row):
    body_number = row["Body Number"]
    file_name = row["setup_file_name"]
    return f"s3://one-meta-artifacts/v0/bodies/{body_number}/setup_files/{file_name}"


def make_setup_png_destination(row):
    body_number = row["Body Number"]
    file_name = row["setup_file_name"]
    file_name = f"{Path(file_name).stem}.png"
    return f"s3://one-meta-artifacts/v0/bodies/{body_number}/setup_files/{file_name}"


def get_body_content_metadata(fc):
    mongo = fc.connectors["mongo"]
    at = fc.connectors["airtable"]

    res.utils.logger.info("loading bodies...")
    BODY_BASE = "appa7Sw0ML47cA8D1"
    BODY_TABLE = "tblXXuR9kBZvbRqoU"
    table = at[BODY_BASE][BODY_TABLE]
    data = table.to_dataframe()

    res.utils.logger.info("Loading sizes...")
    SIZE_BASE = "appjmzNPXOuynj6xP"
    SIZE_TABLE = "tblvexT7dliEamnaK"
    sztable = at[SIZE_BASE][SIZE_TABLE]
    szdata = sztable.to_dataframe()
    sizes = dict(szdata[["record_id", "Size Chart"]].values)

    res.utils.logger.info("Loading files...")
    files_collection = mongo["resmagic"]["files"]
    files_collection = pd.DataFrame(files_collection.find())
    files_collection["_id"] = files_collection["_id"].map(str)
    files_collection["s3_path"] = files_collection["s3"].map(make_path)
    files_collection = files_collection.set_index("_id")
    files = dict(files_collection[["s3_path"]].reset_index().values)

    def make_setup_file_name(row):
        size = row["sizeId"]
        size = sizes[size]
        path = f"setup_{size}_v{row.get('bodyVersion',0)}_{row['repeatSize']}.ai"
        path = path.replace('"', "in").replace(" ", "_").lower()
        return path

    B = data[["_setup_files", "DXF ASTM Link", "Body Name", "Body Number"]]
    B = B[B["DXF ASTM Link"].notnull()]
    B["setup_files"] = B["_setup_files"].map(
        lambda x: literal_eval(x) if not pd.isnull(x) else None
    )
    B = B.explode("setup_files")
    B["file_id"] = B["setup_files"].map(
        lambda x: x["fileId"] if x is not None else None
    )
    B["file"] = B["file_id"].map(lambda x: files[x] if x is not None else None)
    B["box_file_id"] = B["DXF ASTM Link"].map(
        lambda x: x.split("/")[-1] if x is not None else None
    )
    B["setup_file_name"] = B["setup_files"].map(
        lambda x: make_setup_file_name(x) if x is not None else None
    )
    B["astm_target_file_name"] = B.apply(make_astm_destination, axis=1)
    B["s3_setup_target_file_name"] = B.apply(make_setup_destination, axis=1)

    return B


def sync_body_content(fc, body_content_meta):
    s3 = fc.connectors["s3"]
    bc = fc.connectors["box"]
    if not bc:
        raise Exception("could not load box connector")

    res.utils.logger.info(f"Copying any missing astm files to S3...")

    for _, record in (
        body_content_meta[["box_file_id", "astm_target_file_name"]]
        .drop_duplicates()
        .iterrows()
    ):
        box_file_id = int(record["box_file_id"])
        target = record["astm_target_file_name"]
        if not s3.exists(target) and box_file_id is not None:
            try:
                res.utils.logger.info(f"copying box file {box_file_id} => {target}")
                bc.copy_to_s3(box_file_id, target)
            except Exception as ex:
                res.utils.logger.warn(
                    f"Problem with copy box {box_file_id} -> {target} : {repr(ex)}"
                )

    res.utils.logger.info(
        f"Copying any missing setup files to S3 and generating the png versions..."
    )

    for _, record in (
        body_content_meta[["file", "s3_setup_target_file_name"]]
        .drop_duplicates()
        .iterrows()
    ):
        source = record["file"]
        target = record["s3_setup_target_file_name"]

        if not s3.exists(target) and source is not None:
            try:
                res.utils.logger.info(f"copying {source} => {target}")
                s3.copy(source, target)
            except Exception as ex:
                res.utils.logger.warn(
                    f"Problem with copy {source} -> {target} : {repr(ex)}"
                )

        DPI = 300
        png_target = target.replace(".ai", f"dpi_{DPI}.png")
        if not s3.exists(png_target) and source is not None:
            try:
                res.utils.logger.info(
                    f"Rasterizing setup file and saving PNG to {png_target}"
                )
                # this requires a docker image with ghostscript
                rasterize_png(target, png_target, dpi=DPI)
            except Exception as ex:
                res.utils.logger.warn(f"Problem rasterizing {source} : {repr(ex)}")


def handler(event, context):
    with FlowContext(event, context) as fc:
        B = get_body_content_metadata(fc)
        sync_body_content(fc, B)


def on_failure(event, context):
    print("got an onExit->error and we can callback or something")


def on_success(event, context):
    print("got a onExit->success and we can callback or something")
