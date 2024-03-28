import json
import res
from res.flows import FlowContext
import pandas as pd
from res.media.images.convert import rasterize_png


def try_parse(s):
    try:
        return json.loads(s)
    except:
        return None


def make_key(row):
    code = row["Resonance_code"].lower().replace(" ", "-")
    return f"{code}-{row['record_id']}"


def make_path(p):
    return f"s3://{p['bucket']}/{p['key']}"


def get_style_content_metadata(fc):
    s3 = fc.connectors["s3"]
    box = fc.connectors["box"]
    mongo = fc.connectors["mongo"]
    at = fc.connectors["airtable"]

    res.utils.logger.info("loading styles...")
    # styles
    stable = at["appjmzNPXOuynj6xP"]["tblmszDBvO1MvJrlJ"]
    data = stable.to_dataframe(fields=["Resonance_code", "_print_placement_files"])
    data["key"] = data.apply(make_key, axis=1)
    data["_print_placement_files"] = data["_print_placement_files"].map(try_parse)
    data = data.explode("_print_placement_files")
    for field in "sizeId", "fileId", "styleVersion", "bodyVersion":
        data[field] = data["_print_placement_files"].map(
            lambda x: str(x[field]) if not pd.isnull(x) and field in x else None
        )
    data = data[data["_print_placement_files"].notnull()]

    res.utils.logger.debug(list(data.columns))

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

    data["file"] = data["fileId"].map(lambda x: files[x] if x is not None else None)

    def make_file_name(row):
        size = row["sizeId"]
        code = row["Resonance_code"].lower().replace(" ", "-")
        record_id = row["record_id"]
        size = sizes[size].replace('"', "in") if size in sizes else "default"
        file_name = (
            f"{record_id}_{size}_v{row.get('bodyVersion',0)}_{row['styleVersion']}.ai"
        )
        return f"s3://one-meta-artifacts/v0/styles/{code}/placement_files/{file_name}"

    data["s3_target_file_name"] = data.apply(make_file_name, axis=1)

    return data


def sync_style_content(fc, body_content_meta):
    """
    move marker files for every style from whereever they live to a location by style code or id
    also rasterize at 3-0 dpi and create those files
    This is suppoed to be run on a job keeping targets (unique versioned file names) up-to-date

    """

    s3 = fc.connectors["s3"]

    for _, record in (
        body_content_meta[["key", "file", "s3_target_file_name"]]
        .drop_duplicates()
        .iterrows()
    ):
        source = record["file"]
        target = record["s3_target_file_name"]
        if not s3.exists(target):
            try:
                print("copying ", source, target)
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
        data = get_style_content_metadata(fc)
        sync_style_content(fc, data)


def on_failure(event, context):
    print("got an onExit->error and we can callback or something")


def on_success(event, context):
    print("got a onExit->success and we can callback or something")
