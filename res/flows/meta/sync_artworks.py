from PIL import Image
import subprocess
import requests
from io import BytesIO
import res
from res.flows import FlowContext
from res import utils
from res.flows import FlowEventProcessor

UPSERT_META_ARTWORKS = """
    mutation upsert_meta_artworks($artwork: meta_artworks_insert_input!) {
        insert_meta_artworks_one(
            object: $artwork, 
            on_conflict: {constraint: artworks_pkey, update_columns: [name, brand, description, dpi_300_uri, dpi_72_uri, dpi_36_uri, metadata]}
        ) {
            id
        }
    }
"""


def upload_external_uri_to_s3(external_uri, brand, s3_uri=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36"
    }
    response = requests.get(external_uri, headers=headers)
    if response.status_code != 200:
        utils.logger.error(f"Failed to download {external_uri}")
        return None

    end = external_uri.split("/")[-1]
    ext = end.split(".")[-1] if "." in end else "png"

    key = res.utils.hash_of(external_uri)
    destination = (
        s3_uri
        if s3_uri
        else f"s3://meta-one-assets-prod/artworks/{brand.lower()}/{key}_orig.{ext}"
    )
    utils.logger.info(f"Saving {external_uri} to {destination}")
    s3 = res.connectors.load("s3")
    img = Image.open(BytesIO(response.content))
    format = "JPEG" if ext == "jpg" else ext.upper()
    s3.save(
        destination, img, dpi=(300, 300), format=format
    )  # assume uploader has checked?

    return destination


def submit_convert_and_save(
    name, source_dpi, source, brand, source_id, redo_if_exists=True, metadata=None
):
    # create a dummy record that the job will fill in
    key = utils.hash_of(source)
    hasura = res.connectors.load("hasura")

    res.utils.logger.info("updating hasura")
    artwork = {
        "id": key,
        "name": name,
        "brand": brand,
        "description": "",
        "original_uri": source,
        "dpi_300_uri": "s3://meta-one-assets-prod/artworks/inprogress.png",
        "dpi_72_uri": "s3://meta-one-assets-prod/artworks/inprogress.png",
        "dpi_36_uri": "s3://meta-one-assets-prod/artworks/inprogress.png",
        "metadata": {
            "source_id": source_id,
            "status": "IN_PROGRESS",
            **(metadata or {}),
        },
    }
    res.utils.logger.info(f"artwork: {artwork}")
    hasura.execute_with_kwargs(UPSERT_META_ARTWORKS, artwork=artwork)

    try:
        event = FlowEventProcessor().make_sample_flow_payload_for_function(
            "meta.sync_artworks"
        )
        argo = res.connectors.load("argo")

        asset = {
            "name": name,
            "brand": brand,
            "source": source,
            "dpi": source_dpi,
            "metadata": metadata or {},
        }
        if source_id:
            asset["source_id"] = source_id

        event["assets"] = [asset]
        event["args"] = {"redo_if_exists": redo_if_exists}

        argo.handle_event(event)
    except Exception as e:
        res.utils.logger.error(f"Failed to submit argo job to convert {source}")
        artwork["metadata"]["status"] = "FAILED"
        artwork["metadata"]["error"] = str(e)
        hasura.execute_with_kwargs(UPSERT_META_ARTWORKS, artwork=artwork)
        raise e

    return key


def convert_and_save(
    name,
    source_dpi,
    source,
    brand,
    source_id,
    redo_if_exists,
    metadata={},
    s3=None,
    hasura=None,
):
    """
    Convert image to 300, 72, 36 DPI and save to S3
    """
    s3 = s3 or res.connectors.load("s3")
    hasura = hasura or res.connectors.load("hasura")

    if not s3.exists(source):
        res.utils.logger.warning(f"{source} does not exist")
        return

    # may not be necessary but just to get rid of any query params
    source = next(s3.ls(source))
    key = utils.hash_of(source)

    # only download/read the source image once
    img = None
    root = f"s3://meta-one-assets-prod/artworks/{brand.lower()}"
    for dest_dpi in [300, 72, 36]:
        target = f"{root}/{key}_{dest_dpi}.png"
        res.utils.logger.info(f"Making {target} from {source}")

        if s3.exists(target) and not redo_if_exists:
            res.utils.logger.info(f"{target} already exists")
            continue

        if source.endswith(".ai"):
            if not img:
                s3._download(source, "/tmp", "temp.ai")
                img = "/tmp/temp.ai"

            channels = "pngalpha"  # or png16m
            outfile = "/tmp/out.png"
            subprocess.check_output(
                [
                    "gs",
                    f"-sDEVICE={channels}",
                    f"-r{dest_dpi}",
                    "-o",
                    outfile,
                    "-dBufferSpace=2000000000",
                    "-dNumRenderingThreads=4",
                    "-f",
                    img,
                ]
            )

            # url_out may need to be
            bucket, out_file = s3.split_bucket_and_blob_from_path(target)
            destination = f"{bucket}/{out_file}"
            s3._s3fs.put(outfile, destination)
        else:
            if not img:
                res.utils.logger.info(f"reading {source}...")
                img = s3.read_image(source)
                if img.mode != "RGBA":
                    img = img.convert("RGBA")
                res.utils.logger.info(f"read {source}.")

            # get dpi assuming 300 if it's not there
            dpi = img.info.get("dpi", [source_dpi, source_dpi])
            dpi_x = int(dpi[0])
            dpi_y = int(dpi[1])

            scale = dest_dpi / max(dpi_x, dpi_y)
            [w, h] = [int(img.size[0] * scale), int(img.size[1] * scale)]
            resized = img.resize((w, h), Image.LANCZOS)

            s3.write(target, resized, dpi=(dest_dpi, dest_dpi))
            resized.close()

    res.utils.logger.info("updating hasura")
    if "status" in metadata:
        del metadata["status"]
    artwork = {
        "id": key,
        "name": name,
        "brand": brand,
        "description": "",
        "original_uri": source,
        "dpi_300_uri": f"{root}/{key}_300.png",
        "dpi_72_uri": f"{root}/{key}_72.png",
        "dpi_36_uri": f"{root}/{key}_36.png",
        "metadata": {**metadata, "source_id": source_id},
    }
    res.utils.logger.info(f"artwork: {artwork}")
    result = hasura.execute_with_kwargs(UPSERT_META_ARTWORKS, artwork=artwork)

    if img and not isinstance(img, str):
        img.close()

    return result["insert_meta_artworks_one"]["id"]


def generator(event, context={}):
    with FlowContext(event, context) as fc:
        return fc.asset_list_to_payload(fc.assets)


@res.flows.flow_node_attributes(
    memory="8Gi",
)
def handler(event, context):
    """
    {
        "apiVersion": "v0",
        "kind": "resFlow",
        "metadata": {"name": "meta.sync_artworks", "version": "dev"},
        "args": { "redo_if_exists": false },
        "assets": [
            {"name":"some plaid", brand="JR", "source":"s3://", "source_id":"rec123", "dpi":300},
            {"name":"another plaid", brand="JR", "source":"s3://", "source_id":"rec123", "dpi":300},
        ],
        "nodes": null
    }
    OR assets could be a list of lists:
        "assets": [
            {"multiple": [{"name":"some plaid" ...}, {"name":"another plaid" ...}, ...]},
            {"multiple": [{"name":"some flowery yoke" ...}, {"another flowery yoke" ...]}
        ],
    """

    with FlowContext(event, context) as fc:
        hasura = fc.connectors["hasura"]
        s3 = fc.connectors["s3"]

        redo_if_exists = fc.args.get("redo_if_exists", False)
        assets = []
        for asset in fc.assets:
            if asset.get("multiple", None) is None:
                res.utils.logger.info("single asset")
                assets.append(asset)
            else:
                res.utils.logger.info("multiple assets")
                assets.extend(asset["multiple"])

        for a in assets:
            try:
                res.utils.logger.info("**************")
                res.utils.logger.info(a)

                convert_and_save(
                    name=a["name"],
                    source_dpi=a.get("dpi", 300),
                    source=a["source"],
                    brand=a.get("brand", "__") or "__",
                    source_id=a.get("source_id", a.get("record_id", "__")) or "__",
                    redo_if_exists=redo_if_exists,
                    metadata=a.get("metadata", {}),
                    s3=s3,
                    hasura=hasura,
                )

            except Exception as e:
                import traceback

                res.utils.logger.error(
                    f"Error processing {a}: {traceback.format_exc()}"
                )
