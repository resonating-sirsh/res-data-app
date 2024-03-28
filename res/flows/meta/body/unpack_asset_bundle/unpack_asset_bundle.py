import os
from pathlib import Path
import shutil
import boto3
import tqdm
import datetime
from res.utils import logger
from res.utils.strings import to_snake_case
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import res
from res.flows.meta.diff import Diff
from res.media.images.providers.vstitcher_pieces import VstitcherPieces
import pandas as pd
from res.flows.meta.body.unpack_asset_bundle.body_db_sync import sync_body_to_db
import res.flows.meta.body.import_poms_from_rulers.import_poms_from_rulers as import_poms_from_rulers
from res.flows.meta.bodies import get_body_preferred_extracted_sizes
from res.flows.meta.ONE.body_node import BodyMetaOneNode
import res.flows.meta.body.graphql_queries as graphql_queries
import res.flows.meta.body_one_ready_request.graphql_queries as body_one_ready_request_graphql_queries
from res.media.images.providers.pdf import extract_images


airtable = res.connectors.load("airtable")
s3 = res.connectors.load("s3")

META_ONE_ASSETS_BUCKET = "meta-one-assets-prod"

AIRTABLE_META_ONE_BASE = "appa7Sw0ML47cA8D1"
AIRTABLE_BODIES_TABLE = "tblXXuR9kBZvbRqoU"
AIRTABLE_SEW_SYMBOL_TABLE = "tblUGsbA6ApGp3ZXc"

ADD_POM_FILE_3D = """
mutation addPomFile3d($id:ID!, $input: UpdatePomFile3dInput!){
    addPomFile3d(id:$id, input:$input){
        body {
            id
        }
    }
}
"""

ADD_BODY_3D_SIMULATION_FILE = """
mutation addBody3dSimulationFile($id: ID!, $input: AddBody3dSimulationFileInput!) {
    addBody3dSimulationFile(id: $id, input: $input) {
        body {
            id
            name
            body3dSimulationFile {
                file {
                    fileName
                    url
                    uri
                    key
                   createdAt
                }
                bodyVersion
                notValidReasons
            }
        }
    }
}
"""


def download_s3_file_to_directory(s3_uri: str, destination_path: Path, s3_client):
    bucket, key = s3_uri.split("/", 2)[-1].split("/", 1)
    kwargs = {"Bucket": bucket, "Key": key}
    file_size = s3_client.head_object(**kwargs)["ContentLength"]

    # file_name = destination_path.name
    with tqdm.tqdm(
        total=file_size, unit="B", unit_scale=True, desc=str(destination_path)
    ) as pbar:
        s3_client.download_file(
            bucket,
            key,
            str(destination_path),
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
        )


def upload_s3_file(file_location, bucket, object_name, s3_client):
    file_size = os.stat(file_location).st_size

    with tqdm.tqdm(
        total=file_size, unit="B", unit_scale=True, desc=file_location
    ) as pbar:
        s3_client.upload_file(
            file_location,
            bucket,
            object_name,
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
        )


def generate_notch_comparison_table(
    snake_case_body_code,
    body_version,
    body_code,
    body_id,
    preferred_sizes=None,
    diff_images={},
):
    try:
        res.utils.logger.info("Creating notch comparison table")
        # after all files are uploaded, get a notch comparison and add to airtable
        browzwear_path = f"bodies/3d_body_files/{snake_case_body_code}/v{body_version}/{body_code}-V{body_version}-3D_BODY.bw"
        browzwear_path = f"s3://{META_ONE_ASSETS_BUCKET}/{browzwear_path}"
        diff = Diff(browzwear_path, preferred_sizes=preferred_sizes)
        result = diff.generate_notch_comparison_table()
        path = result["image_path"]
        notch_counts_valid = result["notch_counts_valid"]

        diff_images_s3_paths = []
        try:
            for name, img in diff_images.items():
                url = f"s3://{META_ONE_ASSETS_BUCKET}/bodies/3d_body_files/{snake_case_body_code}/v{body_version}/extracted/diff_images/{name}.png"
                s3.save(url, img)
                diff_images_s3_paths.append(url)
        except:
            res.utils.logger.warn("Failed to upload diff images to s3, moving on")

        tab = airtable[AIRTABLE_META_ONE_BASE][AIRTABLE_BODIES_TABLE]
        try:
            tab.update_record(
                {
                    "record_id": body_id,
                    "Meta.ONE Diffs": [
                        {"url": s3.generate_presigned_url(path)},
                        *[
                            {"url": s3.generate_presigned_url(p)}
                            for p in diff_images_s3_paths
                        ],
                    ],
                }
            )
        except:
            res.utils.logger.warn("Failed to update airtable but moving on...")
        return notch_counts_valid
    except Exception as e:
        logger.warn("Exception creating notch comparison table", error=str(e))


def notify_meta_one_queue_status(
    body_code, body_version, body_id, zip_path, exception=None, status="FAILED"
):
    try:
        payload = {
            "created_at": res.utils.dates.utc_now_iso_string(None),
            "body_code": body_code,
            "body_version": body_version,
            "id": body_id,
            "size_code": "",
            "flow": "dxa",
            "node": "unpack",
            "meta_one_path": zip_path,
            "dxf_path": zip_path,
            "status": status,
            "unit_key": body_code,
            "color_code": "",
            "unit_type": "",
            "tags": ["EXCEPTION"] if exception else [],
            "validation_error_map": (
                {"exception": str(exception)} if exception else {}
            ),
        }

        res.connectors.load("kafka")["res_meta.meta_one.status_updates"].publish(
            payload, use_kgateway=True, coerce=True
        )
    except Exception as e:
        logger.warn("Exception notifying meta.one queue status", error=str(e))


def update_airtable_sew_symbols(body, path):
    try:
        first = lambda x: x[0] if x else None

        # get (any) pieces.json
        pieces_json = first([p for p in s3.ls(path) if p.endswith("pieces.json")])
        if pieces_json:
            vs_pieces = VstitcherPieces(pieces_json)
            data = [
                {
                    "key": str(p.name._name),  # no body code, version or size
                    "_auto_piece_identity_attributes": p.sew_identity_symbol_id_old,
                    "_auto_sew_identity_symbol_id": p.sew_identity_symbol_id,
                    "_auto_geom_attributes": p.geometry_attributes,
                }
                for p in vs_pieces.pieces.values()
            ]
            sew_attr_df = pd.DataFrame(data).drop_duplicates("key").set_index("key")

            # get the airtable record ids for this body
            tab = airtable[AIRTABLE_META_ONE_BASE][AIRTABLE_SEW_SYMBOL_TABLE]
            filters = f"""OR(SEARCH("{body}",{{Body Number}}))"""
            tab_df = tab.to_dataframe(filters=filters)

            # we need to match on "Piece Name Code (from Piece)"" but it's an array
            tab_df["key"] = tab_df["Piece Name Code (from Piece)"].apply(first)
            tab_df = tab_df[["_record_id", "key"]].set_index("key")
            tab_df = tab_df.rename({"_record_id": "record_id"}, axis=1)

            joined_df = sew_attr_df.join(tab_df).dropna().reset_index(drop=True)
            records = joined_df.to_dict("index")

            for key, record in records.items():
                try:
                    tab.update_record(record)
                except Exception as e:
                    logger.warn(f"Exception updating sew airtable record {key}: {e}")
    except Exception as e:
        logger.warn(f"Exception updating sew airtable records: {e}")


def check_for_suspicous_pieces(
    upload_s3_path_root, contract_failures, contract_failure_context, testing=False
):
    logger.info("Checking for suspicious pieces")
    BUFFER = 3 * 300  # 3 inches * 300 dpi

    def image_info_from_pdf(pdf_path):
        # download the pdf
        file_name = pdf_path.split("/")[-1]
        [piece_name, size_name] = file_name.split("_")
        size_name = size_name.split(".")[0]

        dims = None
        try:
            dest_path = f"/tmp/{file_name}"
            s3._download(pdf_path, "/tmp")

            # try to extract the image
            extracted_image = extract_images(file=dest_path).__next__()

            # get the aspect ratio
            dims = extracted_image.size

            # free image memory
            extracted_image.close()
        except Exception as e:
            res.utils.logger.warn(
                f"Failed to extract image from pdf {pdf_path}, error: {e}"
            )

        return piece_name, size_name, dims

    # check the pattern-pieces pdfs so there are no "suspicious" pieces
    # pieces that are too big so the pdf isn't an image but a vector (VS thing)
    dims_by_piece_name_by_size = {}
    fails = []
    for pdf_path in s3.ls(f"{upload_s3_path_root}/pattern-pieces-unbuffered/"):
        logger.info(f"  Trying to extract image from {pdf_path}")
        piece_name, size_name, dims = image_info_from_pdf(pdf_path)

        if dims is not None:
            dims_by_piece_name_by_size.setdefault(size_name, {})[piece_name] = dims
        else:
            logger.warn(f"  Failed to extract image from {pdf_path}")
            fails.append(f"{piece_name}_{size_name}")

    if fails:
        if testing:
            logger.warn("  Not failing contract because testing=True")
        else:
            contract = "FAILED_UNPACKING_SUSPICIOUS_PIECE"
            contract_failures.append(contract)
            contract_failure_context[contract] = ", ".join(fails)
    else:
        logger.info("  No suspicious pieces found")

    # # try to find any inconsistent grading spikes, generally the unbuffered
    # # pieces will have similar dimensions to the buffered pieces if ok
    # fails = []
    # for pdf_path in s3.ls(f"{upload_s3_path_root}/pattern-pieces-buffered/"):
    #     logger.info(f"  Checking buffered image size from {pdf_path}")
    #     piece_name, size_name, buffered_dims = image_info_from_pdf(pdf_path)

    #     original_dims = dims_by_piece_name_by_size.get(size_name, {}).get(piece_name)
    #     if buffered_dims is not None and original_dims is not None:
    #         # in VS I add 3 inches to every edge to simulate a seam allowance
    #         # buffering, if the original dims with 3 inches either side are
    #         # close it's ok. in case it's a corner I'm going to give it a very
    #         # rough sqrt 2 allowance too --- will probably need to adjust this
    #         # as the examples come in
    #         if not all(
    #             buffered_dims[i] < original_dims[i] + 2 * BUFFER * 1.5 for i in range(2)
    #         ):
    #             logger.warn(f"  Grading issue with {piece_name} {size_name}")
    #             fails.append(f"{piece_name}_{size_name}")

    # if fails:
    #     if testing:
    #         logger.warn("  Not failing contract because testing=True")
    #     else:
    #         contract = "GRADING_ISSUES"
    #         contract_failures.append(contract)
    #         contract_failure_context[contract] = ", ".join(fails)
    # else:
    #     logger.info("  No grading issues found")

    logger.info("Completed checking for suspicious pieces")


def experimental_inspection(body_code, body_version):
    import traceback

    slack_channel = "C06DVEE12JV"
    # check for any grading issues
    try:
        from res.flows.dxa.inspectors import grading

        grading.inspect_grading(body_code, body_version, slack_channel=slack_channel)
    except Exception as e:
        msg = traceback.format_exc()
        logger.warn("Exception inspecting grading", error=msg)

    # check for simulation issues
    try:
        from res.flows.dxa.inspectors import simulation

        simulation.inspect_simulation(
            body_code, body_version, slack_channel=slack_channel
        )
    except Exception as e:
        msg = traceback.format_exc()
        logger.warn("Exception inspecting simulation", error=msg)


def run_body_checks(body_id, body_code, body_version):
    snake_case_body_code = to_snake_case(body_code)

    diffs = {}
    diff_images = []
    try:
        from res.flows.dxa.inspectors import grading

        prev_version = int(body_version) - 1
        prev = f"s3://meta-one-assets-prod/bodies/3d_body_files/{snake_case_body_code}/v{prev_version}/extracted/"
        if s3.exists(prev):
            res.utils.logger.info("Prev version exists so we can compare")
            diffs, diff_images = grading.get_body_geometry_changes(
                body_code,
                body_version,
                prev_version,
                abs_tol=0.02,
                include_images=True,
            )
        else:
            res.utils.logger.warn("Prev version does not exist so we cannot compare")
    except:
        res.utils.logger.warn("Failed to get diffs with previous version")

    pref_sizes = get_body_preferred_extracted_sizes(body_code, body_version)
    is_valid = generate_notch_comparison_table(
        snake_case_body_code,
        body_version,
        body_code,
        body_id,
        preferred_sizes=pref_sizes,
        diff_images=diff_images,  # clunky but this updates the body table anyways
    )
    # some of these seem impossible to fix, ignore this as a contract violation for now...
    # if not is_valid:
    #     contract_failures.append("NOTCHES_CONSISTENT_ON_SIZES")
    #     res.utils.logger.warn(
    #         f"Body has issues: merged contract failures {contract_failures}"
    #     )

    # try AI check of grading/simulation etc.
    experimental_inspection(body_code, body_version)
    return diffs


def handler(event, context=None, plan=False):
    try:
        res.utils.logger.debug(event)

        s3 = res.connectors.load("s3")
        gql = ResGraphQLClient()

        # accumulate contracts below and they will be sent on - use the standard tags : https://airtable.com/appqtN4USHTmyC6Dv/tbl6RO3zv3xU401Qk/viwG7P2mEc3VtWZLC?blocks=hide
        contract_failures = []
        contract_failure_context = {}

        body_id = event.get("record_id", None)
        body_code = event.get("body_code", None)
        zipped_asset_bundle_uri = event.get("asset_bundle_uri", None)
        # if we saved stampers and other body assets here we would not need to do anything but the sample size in meta-one
        meta_one_sample_size_only = event.get("meta_one_sample_size_only", False)
        body_version = event.get("body_version", None)

        copy_from_body_code = event.get("copy_from_body_code", None)
        copy_from_body_version = event.get("copy_from_body_version", None)

        snake_case_body_code = to_snake_case(body_code)
        snake_case_from_body_code = (
            to_snake_case(copy_from_body_code) if copy_from_body_code else None
        )

        # if there is a .bw file in the extract, it was a rename so we should move it to the parent folder

        diffs = {}
        if copy_from_body_code:
            res.utils.logger.info(
                f"Because we are copying {copy_from_body_code} => {body_code}, we do not need to unpack the asset again"
            )
            # the asset source is the body we are copying from
            upload_s3_path_root = f"s3://{META_ONE_ASSETS_BUCKET}/bodies/3d_body_files/{snake_case_from_body_code}/v{copy_from_body_version}/extracted"
            bw_file = f"{upload_s3_path_root}/{copy_from_body_code}-V{copy_from_body_version}-3D_BODY.bw"
            # SA: what is a suitable error when we try to copy from an existing body that does not exist?
        else:
            """
            everything below here only happens when we are exporting a body for the first time
            """
            upload_s3_path_root = f"s3://{META_ONE_ASSETS_BUCKET}/bodies/3d_body_files/{snake_case_body_code}/v{body_version}/extracted"

            # before we unzip we should delete everything in the dxf_by_size folder, otherwise deleted sizes will remain
            s3.delete_files(s3.ls(f"{upload_s3_path_root}/dxf_by_size/"))

            bw_file = f"{upload_s3_path_root}/{body_code}-V{body_version}-3D_BODY.bw"
            res.utils.logger.info(
                f"unpacking archive {zipped_asset_bundle_uri} to body extract path {upload_s3_path_root}"
            )
            s3.unzip(zipped_asset_bundle_uri, upload_s3_path_root, verbose=True)
            # check the pattern-pieces pdfs so there are no "suspicious" pieces
            try:
                check_for_suspicous_pieces(
                    upload_s3_path_root,
                    contract_failures,
                    contract_failure_context,
                    # testing=True,
                )
            except Exception as e:
                logger.warn("Exception checking for suspicious pieces", error=str(e))

            diffs = run_body_checks(
                body_id=body_id, body_code=body_code, body_version=body_version
            )
            ####
            ##   NEW ASSETS EXPORT AND CHECK ENDS HERE
            #####

        # SA: this could be copy from body but not sure about the name or if it matters. bw file will always exist in copy from mode unless we choose a bad body
        is_duplicate_body = s3.exists(bw_file)
        if is_duplicate_body:

            # Duplicate bodies are generated from an origin body:
            # 1. the body file for duplicate/inherit bodies generated renaming & refactoring the body file from the original body (the filename & pieces get updated)
            # 2. the body bundle gets exported (as usual) + with the renamed file within
            # 3. here in the unpack, the file is moved to the expected original bw file location and we register it into the body_3d_simulation_file json (note: in the standard flow, step 3 happens first on file upload in create.one, there we register the body file and request the export asset bundle simultaneously)
            post_file = bw_file.replace("extracted/", "")
            s3.rename(bw_file, post_file)
            res.utils.logger.debug(f"Posting the simulation file {post_file}...")
            gql.query(
                ADD_BODY_3D_SIMULATION_FILE,
                {
                    "id": body_id,
                    "input": {
                        "uri": post_file,
                        "bodyVersion": int(body_version),
                        "requestExportAssetBundle": False,
                        "uploadedByUserEmail": "techpirates@resonance.nyc",
                        "uploadedAt": datetime.datetime.utcnow().strftime(
                            ("%Y-%m-%dT%H:%M:%SZ")
                        ),
                    },
                },
            )

        # SA: if we copy from body_code, body_version we still use these PoMs for our version
        rulers = [f for f in s3.ls(upload_s3_path_root) if "rulers/" in f]
        res.utils.logger.info(
            f"Registering POMs {rulers} for body_id {body_id} version {body_version}..."
        )
        for ruler in rulers:
            logger.info(f"Adding {ruler}....")
            logger.info(
                gql.query(
                    ADD_POM_FILE_3D,
                    {
                        "id": body_id,
                        "input": {
                            "uri": ruler,
                            "bodyVersion": int(body_version),
                        },
                    },
                )
            )

        logger.info("All files uploaded!")
        _, pom_errors = import_poms_from_rulers.handler(body_code, body_version)

        if len(pom_errors) > 0:
            contract_failures.append("POM_ASSETS")

        if is_duplicate_body:
            body = gql.query(
                graphql_queries.GET_BODY,
                variables={"number": body_code},
            )["data"]["body"]
            active_body_one_ready_request = (
                body["activeBodyOneReadyRequest"] if body else None
            )
            if active_body_one_ready_request:
                logger.info(
                    "Updating Active Body ONE Ready Request for Duplicate/Inherited Body: {body}"
                )
                gql.query(
                    body_one_ready_request_graphql_queries.UPDATE_BODY_ONE_READY_REQUEST,
                    {
                        "id": active_body_one_ready_request["id"],
                        "input": {
                            "autoGenerateBodyFileStatus": "Done",
                        },
                    },
                )

        sizes_synced = sync_body_to_db(
            body_code,
            body_version,
            # upload_s3_path_root is the copy from location if copy from body exists
            upload_s3_path_root,
            contract_failures,
            contract_failure_context,
            # SA pass the copy from assets here as a reference for renaming
            copy_from_body_code=copy_from_body_code,
            copy_from_body_version=copy_from_body_version,
        )
        if sizes_synced == 0:
            contract_failures.append("BODY_SIZES_SYNCED")
            res.utils.logger.warn("No sizes synced to db, probably no extracted files")

        # deprecate/remove
        # update_airtable_sew_symbols(body_code, upload_s3_path_root)

        if not plan:
            res.utils.logger.info(f"Posting meta one request for {event}")
            BodyMetaOneNode.refresh(
                body_code,
                body_version,
                contract_failures=contract_failures,
                contract_failure_context=contract_failure_context,
                previous_differences=diffs,
            )

            notify_meta_one_queue_status(
                body_code, body_version, body_id, zipped_asset_bundle_uri, status="OK"
            )

        return {
            "files_extracted_uri": upload_s3_path_root,
            "copy_from_body": copy_from_body_code,
            "target_body": body_code,
        }
    except Exception as e:
        import traceback

        err = traceback.format_exc()

        res.utils.logger.warn(f"Failed to unpack {event}, error: {err}")

        notify_meta_one_queue_status(
            body_code,
            body_version,
            body_id,
            zipped_asset_bundle_uri,
            status="FAILED",
            exception=err,
        )
        bor = None
        bor_status = None
        try:
            b = BodyMetaOneNode.get_body_as_request(body_code)
            bor = b["body_one_ready_request"]
            bor_status = b["body_one_ready_request_status"]
        except:
            res.utils.logger.warn(f"Failed to get the body request data")

        BodyMetaOneNode.post_status(
            body_code=body_code,
            body_version=body_version,
            contracts_failed=contract_failures,
            contract_failure_context=contract_failure_context,
            trace_log=traceback.format_exc(),
            body_one_ready_request_id=bor,
            bor_status=bor_status,
        )
        raise e


def process_on_scan_created(body_code, body_version, scan_file_uri):
    s3_client = boto3.client("s3")
    snake_case_body_code = to_snake_case(body_code)

    input_key_path = Path(f"/tmp/body/scan")

    if input_key_path.exists() and input_key_path.is_dir():
        shutil.rmtree(input_key_path)

    os.makedirs(input_key_path, exist_ok=True)
    logger.info(f"Downloading {scan_file_uri}...")

    scan_file_path = input_key_path / "scan.json"

    download_s3_file_to_directory(scan_file_uri, scan_file_path, s3_client)
    logger.info(f"File downloaded")

    s3_path = f"bodies/3d_body_files/{snake_case_body_code}/v{body_version}/extracted/scan.json"

    upload_s3_file(
        str(scan_file_path),
        META_ONE_ASSETS_BUCKET,
        s3_path,
        s3_client,
    )

    shutil.rmtree(input_key_path)
    logger.info("File uploaded!")


# if __name__ == '__main__':
#     event = {
#         "record_id": "rec9W2i16bhWlmMQo",
#         "body_code": "TH-4001",
#         "body_version": "1",
#         "asset_bundle_uri": "s3://res-temp-public-bucket/style_assets_dev/61a4e197-5c9e-4520-9955-e6edc27d59d5/asset_bundle.zip"
#     }

#     handler(event)
