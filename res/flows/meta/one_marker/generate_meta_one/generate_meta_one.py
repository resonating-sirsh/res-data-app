"""Generate a Meta ONE.

copy mode implemented and described  in ths PR: https://github.com/resonance/res-data-platform/pull/4268
"""

import os, redis, json, sys
from pathlib import Path
import shutil
import arrow
import boto3
import tqdm
import s3fs
import res
from res.utils import logger
from res.media.images import validators
from res.utils.strings import to_snake_case
from res.media.images.providers.pdf import extract_images
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import res.flows.meta.one_marker.generate_meta_one.graphql_queries as graphql_queries
from res.flows.meta.vstitcher import asset_bundle
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.utils.meta.sizes.sizes import get_sizes_from_aliases
from res.flows.meta.one_marker.apply_color_request import APPLY_COLOR_ISSUE_TYPE_CODE
from res.flows.meta.one_marker.apply_color_request import flag_apply_color_request
from res.flows.meta.one_marker.apply_color_request import unflag_apply_color_request
import numpy as np
from res.media.images.outlines import get_piece_outline_dataframe
from tenacity import retry, wait_fixed, stop_after_attempt

META_ONE_ASSETS_BUCKET = "meta-one-assets-prod"

ENVIRONMENT = os.environ.get("environment", "development")

KAFKA_TOPIC_EVENT = (
    "https://data.resmagic.io/kgateway/submitevent"
    if ENVIRONMENT == "production"
    else "https://datadev.resmagic.io/kgateway/submitevent"
)

gql = ResGraphQLClient()
kafka_client = ResKafkaClient()
os.environ["RES_APP_NAME"] = "generate-meta-one"
os.environ["RES_NAMESPACE"] = "res-meta"
redis_client = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0)


def finish_with_known_exception():
    """
    workaround to finish the workflow for known exceptions and not flag the request in the exit-handler
    """
    sys.exit(2)


def finish_with_unknown_exception(fn):
    def wrap_with_try_catch(*args, **kwargs):
        """
        when an unknown exception occurs, attempt to capture the error and flag the request
        """
        import traceback

        fn_return = {}
        try:
            fn_return = fn(*args, **kwargs)
        except Exception as e:
            msg = traceback.format_exc()
            logger.warn("Unknown exception occurred")
            logger.warn(msg)
            event = args[0]
            logger.info(event)

            record_id = (
                event.get("record_id", None) or event.get("request_id", None)
                if event
                else None
            )  # workflow steps currently use one or the other..

            try:
                flagged_request = flag_apply_color_request(
                    record_id,
                    [
                        {
                            "context": f"EXCEPTION: Failed generating meta.ONE's input: {msg}",
                            "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE[
                                "UNKNOWN_ERROR"
                            ],
                        }
                    ],
                )
                logger.info(flagged_request)

            except Exception as e:
                logger.warn("Failed to flag request")
                logger.warn(e)
            sys.exit(2)
        return fn_return

    return wrap_with_try_catch


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


def _get_redis_piece_validation_key(record_id: str):
    if not record_id:
        raise Exception("A Queue record Id is required for validation.")
    return f"{record_id}_piece_validations"


DPI = 300


def piece_mapping_to_offset_lookup(piece_mapping):
    lookup = {}
    for mapping in piece_mapping:
        piece_name = mapping.get("bodyPiece", {}).get("code")
        if piece_name:
            dpi_offset = int(
                (mapping.get("material", {}).get("offsetSizeInches") or 0) * DPI
            )
            lookup[piece_name] = dpi_offset
    return lookup


def piece_mapping_to_material_code_lookup(piece_mapping):
    lookup = {}
    for mapping in piece_mapping:
        piece_name = mapping.get("bodyPiece", {}).get("code")
        if piece_name:
            lookup[piece_name] = mapping.get("material", {}).get("code")
    return lookup


def copy_pieces(event, plan=False):
    """
    copy files from pieces in one version to the same or new version
    the same version copy is just a file touch to update metadata but its necessary

    we copy the outlines, 3d files and other assets
    """

    from res.flows.meta.ONE.style_node import MetaOne

    s3 = res.connectors.load("s3")

    # get a handle on the ACQ so we can upadte some states
    record_id = event.get("record_id")
    apply_color_request = gql.query(
        graphql_queries.QUERY_APPLY_COLOR_REQUEST, {"id": record_id}
    )["data"]["applyColorRequest"]

    body_code = event.get("body_code")
    body_version = event.get("body_version")
    # can be the same by default
    source_body_version = event.get("source_body_version")
    if not source_body_version:
        res.utils.logger.info(
            f"The source is not given - we will check for a test status for style {event['style_code']} or assume the same version"
        )
        status = MetaOne.get_style_status_history(event["style_code"])
        test_status = status.get("piece_version_delta_test_results")
        if test_status and test_status.get("style_body_version"):
            source_body_version = test_status["style_body_version"]
        else:
            source_body_version = body_version

    color_code = event.get("color_code").lower()
    material_code = event.get("material_code")
    lowered_body_code = body_code.lower().replace("-", "_")
    # copy turntable?? - i think somethings like artwork may be versionless

    source_body_version = int(source_body_version)
    body_version = int(body_version)
    res.utils.logger.info(f"The source version is {source_body_version}")

    root_source = f"s3://{META_ONE_ASSETS_BUCKET}/color_on_shape/{lowered_body_code}/v{source_body_version}/{color_code}"
    root_target = f"s3://{META_ONE_ASSETS_BUCKET}/color_on_shape/{lowered_body_code}/v{body_version}/{color_code}"
    # we could do the test here to see if for the "latest" style files we have a match

    """
    Copy the 3d assets and images first
    """
    model_files = ["3d.glb", "point_cloud.json", "front.png"]
    for model_file in model_files:
        s3_path_source = f"{root_source}/{model_file}"
        s3_path_target = f"{root_target}/{model_file}"
        if s3.exists(s3_path_source):
            res.utils.logger.info(f"Copying {s3_path_source}->{s3_path_target}")
            if not plan:
                s3.copy(s3_path_source, s3_path_target)

    """
    copy turntables
    """
    # these two links are the source and the target
    turntable_root_path_source = f"s3://{META_ONE_ASSETS_BUCKET}/color_on_shape/{lowered_body_code}/v{source_body_version}/{color_code}/turntable/snapshots"
    turntable_root_path_target = f"s3://{META_ONE_ASSETS_BUCKET}/color_on_shape/{lowered_body_code}/v{body_version}/{color_code}/turntable/snapshots"
    for f in s3.ls(turntable_root_path_source):
        # just replace the folder roots to copy
        t = f.replace(turntable_root_path_source, turntable_root_path_target)
        res.utils.logger.info(f"{f} -> {t}")
        s3.copy(f, t)

    notify_unpack_turntable(
        upload_root_path=turntable_root_path_target,
        apply_color_request=apply_color_request,
    )

    """
    copy the pieces and outlines if they exist for all the sizes
    we are careful to rename both the path part to the new version and the piece part component
    """
    res.utils.logger.info(
        f"Checking for files to copy from {root_source} to {root_target} for /pieces/ and /outlines/"
    )

    res.utils.logger.info(f"ls {root_source}")
    for source in s3.ls(f"{root_source}"):
        if "/pieces/" in source or "/outlines/" in source:
            target = source.replace(f"-V{source_body_version}-", f"-V{body_version}-")
            target = target.replace(f"/v{source_body_version}/", f"/v{body_version}/")
            res.utils.logger.info(f"Copying {source} -> {target}")
            if not plan:
                s3.copy(source, target)


def save_image_outline(image, uri):
    """
    If we cache the outlines it means we can check them without reloading large images
    he target uri should be like:> replacing both the pieces and extension

        res.connectors.load("s3").write(uri.replace('/pieces/','/outlines/').replace('.png','.feather/'), ol)
    """
    try:
        res.utils.logger.info(f"Saving piece outline {uri}")
        outline = get_piece_outline_dataframe(np.asarray(image))
        res.connectors.load("s3").write(uri, outline)
    except Exception as ex:
        res.utils.logger.warn(f"Failed to save outline {uri} - {ex}")


def can_acq_use_copied_files(event, acq_style_request):
    """
    The apply color queue can determine what mode to use and potentially other attributes like how many versions to go back - here we assume go back 1 version or stay on same

    """

    if (
        acq_style_request.get("assetsGenerationMode")
        == "Copy Assets from Previous Version"
    ):
        return True

    return False


@finish_with_unknown_exception
def start_generate_meta_one(event, debugging=False):
    logger.info(f"event: {event}")
    record_id = event.get("record_id")

    updated_request = gql.query(
        graphql_queries.UPDATE_APPLY_COLOR_REQUEST_MUTATION,
        {"id": record_id, "input": {"generateMetaOneStatus": "In Progress"}},
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    """
    in a special mode, unpack can copy existing files
    """
    use_existing_files = can_acq_use_copied_files(event, updated_request)

    style = updated_request["style"]
    zipped_raw_pieces_uri = (
        updated_request["exportedPiecesFile"]["uri"]
        if updated_request["exportedPiecesFile"]
        and updated_request["exportedPiecesFile"]["uri"]
        else None
    )

    # clear piece validations from redis from previous runs. We shouldn't generally
    # be re-running from the same record, but just in case
    if not debugging:
        redis_client.delete(_get_redis_piece_validation_key(record_id))
        logger.info(
            f"Deleted piece validation key from redis: {_get_redis_piece_validation_key(record_id)}"
        )

    mapping = style["pieceMapping"]

    if event.get("body_version"):
        res.utils.logger.warn(
            f"Note that we are spoofing the body version e.g. to advance a body version we have no BW assets for"
        )

    return {
        "record_id": record_id,
        "body_code": style["body"]["code"],
        "style_code": style["code"],
        # adding something to spoof the body version if we need it but expect this to come from queue
        "body_version": event.get("body_version") or updated_request["bodyVersion"],
        "color_code": style["color"]["code"],
        "material_code": style["material"]["code"],
        "zipped_raw_pieces_uri": zipped_raw_pieces_uri,
        "piece_material_offset_lookup": piece_mapping_to_offset_lookup(mapping),
        "piece_material_code_lookup": piece_mapping_to_material_code_lookup(mapping),
        # something for testing and something derived from the acq
        "mode": "copy" if use_existing_files or event.get("mode") == "copy" else None,
    }


@finish_with_unknown_exception
def finish_generate_meta_one(event, context=None):
    logger.info(f"event: {event}")
    s3_client = boto3.client("s3")

    logger.info(event)
    record_id = event.get("record_id")

    r = gql.query(graphql_queries.QUERY_APPLY_COLOR_REQUEST, {"id": record_id})["data"][
        "applyColorRequest"
    ]

    body_code = r["style"]["body"]["code"]
    snake_case_body_code = to_snake_case(body_code)
    body_version = r["bodyVersion"]
    lowered_color_code = r["style"]["color"]["code"].lower()

    # check piece count
    s3_expected_uri = (
        f"color_on_shape/{snake_case_body_code}/v{body_version}/{lowered_color_code}/"
    )

    objects = s3_client.list_objects_v2(
        Bucket=META_ONE_ASSETS_BUCKET, Delimiter="/", Prefix=s3_expected_uri
    )
    selected_key = None
    for key in objects["CommonPrefixes"]:
        current_key = key["Prefix"]
        if "turntable" not in current_key:
            selected_key = current_key
            break

    pieces_path = f"{selected_key}pieces"
    response = s3_client.list_objects_v2(
        Bucket=META_ONE_ASSETS_BUCKET, Prefix=pieces_path
    )

    piece_files_count = response["KeyCount"]

    updated_request = gql.query(
        graphql_queries.UPDATE_APPLY_COLOR_REQUEST_MUTATION,
        {
            "id": record_id,
            "input": {
                "generateMetaOneStatus": "Done",
                "totalPieceCount": piece_files_count,
            },
        },
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    redis_client.delete(_get_redis_piece_validation_key(record_id))

    return updated_request


def handle_failure(event):
    logger.info(f"on failure event: {event}")
    logger.info("Handle Failure")

    flagged_request = None

    exit_codes = [error for error in event.get("errors") if "exit code" in error]

    logger.info(f"exit codes: {exit_codes}")

    record_id = (
        event.get("record_id", None) or event.get("request_id", None) if event else None
    )  # workflow steps currently use one or the other..

    if "Error (exit code 2)" in exit_codes:
        # Known exception that was flagged already
        pass
    else:
        # Uncaught exception
        flagged_request = flag_apply_color_request(
            record_id,
            [
                {
                    "context": f"EXCEPTION: Failed generating meta.ONE's input: {event.get('error_details')}",
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                }
            ],
        )
        logger.info(flagged_request)

    return flagged_request


# possible retry action
def notify_unpack_turntable(upload_root_path, apply_color_request):
    style_update_payload = {
        "turntableS3UriRoot": f"s3://{META_ONE_ASSETS_BUCKET}/{upload_root_path}",
        "simulationStatus": "Simulation Ready",
    }

    if (
        apply_color_request["style"]["simulationBrandFeedback"] != "Simulation Approved"
    ):  # verify if simulation has been approved in case we are re-running the workflow for the same request and approval happened already
        if apply_color_request["turntableRequiresBrandFeedback"]:
            style_update_payload[
                "simulationBrandFeedback"
            ] = "Pending Brand Simulation Feedback"
        else:
            style_update_payload["simulationBrandFeedback"] = "N/A"

    style_id = apply_color_request["style"]["id"]
    gql.query(
        graphql_queries.UPDATE_STYLE_MUTATION,
        variables={
            "id": style_id,
            "input": style_update_payload,
        },
    )


@finish_with_unknown_exception
def unpack_turntable(event):
    logger.info(f"event: {event}")
    if event.get("mode") == "copy":
        return event

    flag_issues_input = []
    all_snapshots = None
    s3_client = boto3.client("s3")

    record_id = event.get("record_id")
    apply_color_request = gql.query(
        graphql_queries.QUERY_APPLY_COLOR_REQUEST, {"id": record_id}
    )["data"]["applyColorRequest"]
    logger.info(apply_color_request)

    """
    we need to check the apply color queue again on the workflow event
    """
    if can_acq_use_copied_files(event, apply_color_request):
        return event

    turntable_zip_uri = apply_color_request["turntableZipFile"]["uri"]

    input_key_path = Path(f"/tmp/turntable")
    os.makedirs(input_key_path, exist_ok=True)
    logger.info(f"Downloading {turntable_zip_uri}...")
    zipped_file_path = input_key_path / "turntable.zip"
    download_s3_file_to_directory(turntable_zip_uri, zipped_file_path, s3_client)
    logger.info(f"File downloaded")

    snapshots_path = input_key_path / "snapshots"

    logger.info(f"Unpacking zipped files to {input_key_path}...")
    shutil.unpack_archive(str(zipped_file_path), str(snapshots_path), "zip")
    print("Archive file unpacked successfully.")

    try:
        all_snapshots = os.listdir(snapshots_path)
    except Exception as e:
        flag_issues_input.append(
            {
                "context": "Turntable seems to be empty.",
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                    "FAILED_GENERATING_TURNTABLE"
                ],
            }
        )

    if len(flag_issues_input) > 0:
        flag_apply_color_request(
            event.get("record_id"),
            [
                {
                    "context": flag_issue_input["context"],
                    "issueTypeCode": flag_issue_input["issue_type_code"],
                }
                for flag_issue_input in flag_issues_input
            ],
        )

        finish_with_known_exception()

    logger.info(all_snapshots)

    body = apply_color_request["style"]["body"]
    color = apply_color_request["style"]["color"]
    lower_color_code = color["code"].lower()
    lower_body_code = body["code"].lower()
    snaked_body = to_snake_case(lower_body_code)

    upload_root_path = f"color_on_shape/{snaked_body}/v{apply_color_request['bodyVersion']}/{lower_color_code}/turntable/snapshots"

    for snapshot in all_snapshots:
        snapshot_path = snapshots_path / snapshot
        s3_upload_path = f"{upload_root_path}/{snapshot}"
        logger.info(f"Uploading {snapshot_path} to {s3_upload_path}")
        upload_s3_file(
            str(snapshot_path), META_ONE_ASSETS_BUCKET, s3_upload_path, s3_client
        )

    notify_unpack_turntable(
        upload_root_path=upload_root_path, apply_color_request=apply_color_request
    )


@finish_with_unknown_exception
def unpack_pieces_and_sort_by_size(event, debugging=False):
    """
    introducing a new mode for this: we can sometimes unpack from the previous and just return [] to skip the unpack in the next stage and go to the validation etc.

    """
    logger.info(f"event: {event}")

    """
    if the apply color queue can clone files we do it here - we infer that we need the latest color files which can be the same pieces
    """
    if event.get("mode") == "copy":
        copy_pieces(event)
        # we can return none because this avoids the next two size based steps in the DAF
        return [
            {
                "mode": "copy",
                "style_code": event.get("style_code"),
                "record_id": event.get("record_id"),
            }
        ]

    s3_client = boto3.client("s3")
    record_id = event.get("record_id")
    body_code = event.get("body_code")
    body_version = event.get("body_version")
    color_code = event.get("color_code")
    material_code = event.get("material_code")
    zipped_raw_pieces_uri = event.get("zipped_raw_pieces_uri")
    piece_material_offset_lookup = event.get("piece_material_offset_lookup")
    piece_material_code_lookup = event.get("piece_material_code_lookup")

    input_key_path = Path(f"/tmp/asset_bundle")
    os.makedirs(input_key_path, exist_ok=True)
    logger.info(f"Downloading {zipped_raw_pieces_uri}...")
    zipped_file_path = input_key_path / "asset_bundle.zip"
    download_s3_file_to_directory(zipped_raw_pieces_uri, zipped_file_path, s3_client)
    logger.info(f"File downloaded")

    logger.info(f"Unpacking zipped files to {input_key_path}...")
    shutil.unpack_archive(str(zipped_file_path), str(input_key_path), "zip")
    print("Archive file unpacked successfully")

    lowered_color_code = color_code.lower()
    snake_case_body_code = to_snake_case(body_code)

    # Upload .dxf and .rul file
    piece_name_mapping_path = input_key_path / "dxf"
    # generally in the 'dxf' folder we'll find the .dxf and .rul files so we will upload everything in this directory.
    dxf_files = os.listdir(piece_name_mapping_path)
    logger.info("Uploading the dxf files required...")
    dxf_file_path = None
    for dxf_file in dxf_files:
        dxf_s3_path = f"color_on_shape/{snake_case_body_code}/v{body_version}/{lowered_color_code}/dxf/{dxf_file}"
        logger.info(f"Uploading {dxf_file} to {dxf_s3_path}...")
        upload_s3_file(
            str(piece_name_mapping_path / dxf_file),
            META_ONE_ASSETS_BUCKET,
            dxf_s3_path,
            s3_client,
        )

        if ".dxf" in dxf_file:
            dxf_file_path = dxf_s3_path

    # Upload artwork folder
    artwork_path = f"{input_key_path}/artwork_bundle"
    if os.path.isdir(artwork_path):
        artwork_s3_path = f"{META_ONE_ASSETS_BUCKET}/color_on_shape/{snake_case_body_code}/{lowered_color_code}/artwork_bundle"
        logger.info(
            f"Uploading the artwork files... uploading {artwork_path} to {artwork_s3_path}"
        )
        s3fs.S3FileSystem().put(artwork_path, artwork_s3_path, recursive=True)
    else:
        logger.info("No artwork directory found to be uploaded.")

    # upload 3d model
    model_files = ["3d.glb", "point_cloud.json", "front.png"]
    if all(os.path.exists(input_key_path / model_file) for model_file in model_files):
        s3_path = f"color_on_shape/{snake_case_body_code}/v{body_version}/{lowered_color_code}"

        for model_file in model_files:
            src = str(input_key_path / model_file)
            dest = f"{s3_path}/{model_file}"
            logger.info(f"Uploading {src} to {dest}")
            upload_s3_file(src, META_ONE_ASSETS_BUCKET, dest, s3_client)

    pieces_by_size_and_category = asset_bundle.pattern_pieces_by_airtable_size(
        input_key_path
    )

    root_path_by_size = []

    for size_name in pieces_by_size_and_category.keys():
        logger.info(f"Working with size: {size_name}...")
        pieces_files = pieces_by_size_and_category[size_name]
        size_code = get_sizes_from_aliases([size_name])[0]["code"]
        lowered_size_code = size_code.lower()
        size_pieces_path = f"color_on_shape/{snake_case_body_code}/v{body_version}/{lowered_color_code}/{lowered_size_code}"
        raw_piece_path = f"{size_pieces_path}/raw_pieces"

        raw_pieces_paths = []
        for pf in pieces_files:
            # if this piece has a material offset, we need to add it to the piece name for uniqueness
            file_name = if_offset_make_unique_dir(
                pf.vstitcher_name, piece_material_offset_lookup
            )
            s3_path = f"{raw_piece_path}/{file_name}.{pf.ext}"

            logger.info(f"Uploading {pf} to {s3_path}")
            upload_s3_file(str(pf.path), META_ONE_ASSETS_BUCKET, s3_path, s3_client)
            raw_pieces_paths.append(s3_path)

        root_path_by_size.append(
            {
                "request_id": record_id,
                "body_code": body_code,
                "body_version": body_version,
                "color_code": color_code,
                "material_code": material_code,
                "size_code": size_code,
                "dxf_file_path": f"s3://{META_ONE_ASSETS_BUCKET}/{dxf_file_path}",
                "size_pieces_path": size_pieces_path,
                "raw_pieces_paths": raw_pieces_paths,
                "piece_material_offset_lookup": piece_material_offset_lookup,
                "piece_material_code_lookup": piece_material_code_lookup,
            }
        )

        if debugging:  # just get one for debugging purposes
            break
    logger.info("All files sorted and uploaded!")

    logger.info(root_path_by_size)

    return root_path_by_size


def if_offset_make_unique_dir(piece_name, piece_material_offset_lookup):
    key = "-".join(piece_name.split("-")[-2:])
    offset = piece_material_offset_lookup.get(key)
    if offset:
        return f"{offset}/{piece_name}"
    return piece_name


@finish_with_unknown_exception
def extract_images_from_pdf_pieces(event):
    logger.info(f"event: {event}")
    if event.get("mode") == "copy":
        return {
            "mode": "copy",
            "style_code": event.get("style_code"),
            "record_id": event.get("record_id"),
        }

    s3_client = boto3.client("s3")
    raw_pieces_paths = event.get("raw_pieces_paths")
    png_piece_root = f"{event.get('size_pieces_path')}/pieces"
    outline_of_piece_root = f"{event.get('size_pieces_path')}/outlines"

    piece_material_offset_lookup = event.get("piece_material_offset_lookup")
    flag_issues_input = []

    all_pdf_pieces_in_directory = raw_pieces_paths
    total_pieces = len(raw_pieces_paths)

    logger.info(f"Total pieces: {total_pieces}")

    logger.info(png_piece_root)
    png_pieces_paths = []
    for pdf_piece_key in all_pdf_pieces_in_directory:
        piece_type = pdf_piece_key.split(".")[0].split("-")[-1]
        if piece_type in ["S", "BF", "C", "LN"]:
            file_uri = f"s3://{META_ONE_ASSETS_BUCKET}/{pdf_piece_key}"
            file_name = pdf_piece_key.split("/")[-1]
            file_path = Path(f"/tmp/{file_name}")
            logger.info(f"Downloading {pdf_piece_key}")
            download_s3_file_to_directory(
                s3_uri=file_uri, destination_path=file_path, s3_client=s3_client
            )

            logger.info("File downloaded.")

            extracted_image = None
            try:
                extracted_image = extract_images(file=str(file_path)).__next__()
            except Exception as e:
                flag_issues_input.append(
                    {
                        "context": f"Suspicious piece couldn't be extracted from the piece's pdf in the asset bundle - please check the piece in the browzwear file: {str(file_uri)} \nException: {str(e)} ",
                        "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                            "FAILED_UNPACKING_SUSPICIOUS_PIECE"
                        ],
                    }
                )

            if len(flag_issues_input) > 0:
                flag_apply_color_request(
                    event.get("request_id"),
                    [
                        {
                            "context": flag_issue_input["context"],
                            "issueTypeCode": flag_issue_input["issue_type_code"],
                        }
                        for flag_issue_input in flag_issues_input
                    ],
                )

                finish_with_known_exception()

            file_name = file_name.split(".")[0]
            # if this piece has a material offset, we need to add it to the piece name for uniqueness
            new_file_name = if_offset_make_unique_dir(
                file_name, piece_material_offset_lookup
            )
            new_file_name += ".png"

            path_to_file = f"/tmp/{new_file_name}"
            os.makedirs(os.path.dirname(path_to_file), exist_ok=True)
            extracted_image.save(path_to_file)

            # Upload image to s3
            png_piece_key = f"{png_piece_root}/{new_file_name}"
            png_pieces_paths.append(png_piece_key)

            logger.info(f"Uploading piece to {png_piece_key}")
            upload_s3_file(
                path_to_file, META_ONE_ASSETS_BUCKET, png_piece_key, s3_client
            )

            save_image_outline(
                extracted_image,
                f"s3://{META_ONE_ASSETS_BUCKET}/{outline_of_piece_root}/{new_file_name.replace('.png', '.feather')}",
            )
        else:
            logger.info(
                "Piece is not a printable piece (self, block fuse, lining or combo). Skipping."
            )

    logger.info("All printable images pieces in png extracted and uploaded.")
    event["png_pieces_paths"] = png_pieces_paths
    event["png_pieces_path"] = png_piece_root
    event["total_pieces_count"] = total_pieces
    return event


@finish_with_unknown_exception
@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
def run_pieces_checks(event):
    """Check pieces for missing pixels and artwork width."""
    logger.info(f"event: {event}")
    if event.get("mode") == "copy":
        return {
            "mode": "copy",
            "style_code": event.get("style_code"),
            "record_id": event.get("record_id"),
        }

    check_missing_pixels = False  # Turn off missing pixels check - all cases we've got flagged by this speficially in 3D are false positives.. note: this check works accurately for the 2DM1 flow with the pieces we get from the rasterized .ai file
    check_cuttable_width = True

    record_id = event.get("request_id")
    s3_client = boto3.client("s3")
    piece_material_code_lookup = event.get("piece_material_code_lookup")

    png_pieces_paths = event.get("png_pieces_paths")
    logger.info(f"size_pieces_paths: {png_pieces_paths}")

    # check for missing pixels and wide pieces
    # material_code = event.get("material_code")
    pieces_with_missing_pixels = []
    pieces_exceeding_cuttable_width = []
    pieces_exceeding_cuttable_width_details = []
    for piece_uri in png_pieces_paths:
        piece_uri = f"s3://{META_ONE_ASSETS_BUCKET}/{piece_uri}"
        have_missing_pixels = (
            validators.look_for_missing_pixels(piece_uri)
            if check_missing_pixels
            else False
        )

        piece_name = "-".join(piece_uri.split(".")[-2].split("-")[-2:])
        material_code = piece_material_code_lookup.get(piece_name)
        does_piece_exceed_cuttable_width, piece_exceeds_cuttable_witdh_details = (
            validators.validate_piece_exceeds_material_cuttable_width(
                piece_uri, material_code
            )
            if check_cuttable_width
            else False
        )
        pieces_with_missing_pixels.append(piece_uri) if have_missing_pixels else None
        (
            pieces_exceeding_cuttable_width.append(piece_uri)
            if does_piece_exceed_cuttable_width
            else None
        )
        (
            pieces_exceeding_cuttable_width_details.append(
                piece_exceeds_cuttable_witdh_details
            )
            if piece_exceeds_cuttable_witdh_details
            else None
        )

    # check for pieces too wide
    missing_pixels_found = len(pieces_with_missing_pixels) > 0
    pieces_too_wide_found = len(pieces_exceeding_cuttable_width) > 0
    are_pieces_valid = not (missing_pixels_found or pieces_too_wide_found)

    lowered_size_code = event.get("size_code").lower()
    pieces_validation_result = {
        "sizeCode": lowered_size_code,
        "validations": {
            "piecesWithMissingPixels": pieces_with_missing_pixels or [],
            "piecesExceedingCuttableWidth": pieces_exceeding_cuttable_width or [],
            "piecesExceedingCuttableWidthDetails": pieces_exceeding_cuttable_width_details
            or [],
        },
    }

    if missing_pixels_found or pieces_too_wide_found:
        logger.info(
            f"Found missing pixels or too-wide pieces: {pieces_validation_result}"
        )
        redis_client.lpush(
            _get_redis_piece_validation_key(record_id),
            json.dumps(pieces_validation_result),
        )

    return are_pieces_valid


@finish_with_unknown_exception
def validate_pieces_checks(event):
    logger.info(f"event: {event}")
    if event.get("mode") == "copy":
        return "true"

    request_id = event.get("record_id")

    piece_validations = [
        json.loads(pv)
        for pv in redis_client.lrange(
            _get_redis_piece_validation_key(request_id), 0, -1
        )
    ]

    logger.info(f"Retrieved piece validations: {piece_validations}")

    missing_pixels_found = False
    pieces_too_wide_found = False
    for pv in piece_validations:
        validations = pv.get("validations", {})
        if len(validations.get("piecesWithMissingPixels", [])) > 0:
            missing_pixels_found = True
        if len(validations.get("piecesExceedingCuttableWidth", [])) > 0:
            pieces_too_wide_found = True

    invalid_pieces_exist = len(piece_validations) > 0
    if invalid_pieces_exist:
        flag_issues_input = []

        if missing_pixels_found:
            flag_issues_input.append(
                {
                    "context": f"Some pieces seem to have missing pixels:\n\n {validations.get('piecesWithMissingPixels', [])}",
                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "PIECE_HAS_MISSING_PIXELS"
                    ],
                }
            )

        if pieces_too_wide_found:
            pieces_exceed_cuttable_width_details = [
                {
                    "sizeCode": (
                        piece_validation["sizeCode"]
                        if piece_validation["sizeCode"]
                        else None
                    ),
                    "piecesExceedingCuttableWidthDetails": (
                        piece_validation["validations"][
                            "piecesExceedingCuttableWidthDetails"
                        ]
                        if piece_validation
                        and piece_validation["validations"]
                        and piece_validation["validations"][
                            "piecesExceedingCuttableWidthDetails"
                        ]
                        else None
                    ),
                }
                for piece_validation in piece_validations
            ]

            flag_issues_input.append(
                {
                    "context": f"Some pieces seem to exceed the cuttable width:\n\n {json.dumps(pieces_exceed_cuttable_width_details,indent=2)}",
                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "PIECE_EXCEEDS_CUTTABLE_WIDTH"
                    ],
                }
            )

        updated_request = gql.query(
            graphql_queries.UPDATE_APPLY_COLOR_REQUEST_MUTATION,
            {
                "id": request_id,
                "input": {
                    "invalidPiecesBySize": [
                        {
                            "sizeCode": (
                                piece_validation["sizeCode"]
                                if piece_validation and piece_validation["sizeCode"]
                                else None
                            ),
                            "validations": {
                                "piecesWithMissingPixels": (
                                    piece_validation["validations"][
                                        "piecesWithMissingPixels"
                                    ]
                                    if piece_validation
                                    and piece_validation["validations"]
                                    and piece_validation["validations"][
                                        "piecesWithMissingPixels"
                                    ]
                                    else None
                                ),
                                "piecesExceedingCuttableWidth": (
                                    piece_validation["validations"][
                                        "piecesExceedingCuttableWidth"
                                    ]
                                    if piece_validation
                                    and piece_validation["validations"]
                                    and piece_validation["validations"][
                                        "piecesExceedingCuttableWidth"
                                    ]
                                    else None
                                ),
                            },
                        }
                        for piece_validation in piece_validations
                    ]
                },
            },
        )["data"]["updateApplyColorRequest"]["applyColorRequest"]

        flag_apply_color_request(
            event.get("record_id"),
            [
                {
                    "context": flag_issue_input["context"],
                    "issueTypeCode": flag_issue_input["issue_type_code"],
                }
                for flag_issue_input in flag_issues_input
            ],
        )
        finish_with_known_exception()

    should_request_meta_one = not invalid_pieces_exist

    return should_request_meta_one


def post_style_size_update(
    sku, record_id=None, body_version=None, style_record_id=None, skip_previews=False
):
    """
    post the style update to the meta one node with ids
    the skip previews disables the slow preview generation on the sample size if its not required
    """
    from res.flows.meta.ONE.style_node import MetaOneNode, reset_cache_for_ids
    import traceback

    style = gql.query(
        graphql_queries.QUERY_STYLE_MAKE_ONE_READY_STATUS, {"id": style_record_id}
    )["data"]["style"]

    res.utils.logger.info(f"Style: {style}")

    contracts_failing = style["makeOneReadyStatusV2"]["contractsFailing"]

    res.utils.logger.info(f"Style contracts failing: {contracts_failing}")

    try:
        if "-" not in sku:
            sku = f"{sku[:2]}-{sku[2:]}"

        res.utils.logger.info(f"Make a style size change request for {sku}")
        # passing the body version will request the style payload in that body version
        # if you want to skip previews you can specify that here (its only generated on the sample size normally and we can disable entirely)

        payload = MetaOneNode.get_style_as_request(
            sku,
            body_version=body_version,
            with_initial_contracts=contracts_failing,
            skip_previews=skip_previews,
        ).dict()

        # the queue request id loaded here and sent on the wire
        if record_id:
            payload["id"] = record_id

        # we now reset the cache in redis when creating a new batch eg problems with deleting size codes from bodies that get stuck in the cache
        reset_cache_for_ids(record_id)

        res.connectors.load("kafka")[
            "res_meta.dxa.style_pieces_update_requests"
        ].publish(payload, use_kgateway=True)
    except:
        res.utils.logger.warn(
            f"Failing to send the style update request {traceback.format_exc()}"
        )


@finish_with_unknown_exception
def request_meta_one(event):
    logger.info(f"event: {event}")
    if event.get("mode") == "copy":
        from res.flows.meta.ONE.style_node import MetaOneNode

        MetaOneNode.refresh(
            sku=event.get("style_code"), use_record_id=event.get("record_id")
        )

        return {}

    record_id = event.get("request_id")
    request = gql.query(graphql_queries.QUERY_APPLY_COLOR_REQUEST, {"id": record_id})[
        "data"
    ]["applyColorRequest"]

    style = request["style"]
    style_code = style["code"]
    style_pieces = style["stylePieces"]
    pieces_mapping = style[
        "pieceMapping"
    ]  # all pieces mapping for body x material x color
    material_piece_mapping = {"default": style["material"]["code"]}
    piece_color_mapping = {"default": style["color"]["code"]}

    request_body_version = request["bodyVersion"]
    body_code = style["body"]["code"]
    size_pieces_path = f"s3://{META_ONE_ASSETS_BUCKET}/{event.get('png_pieces_path')}"

    for style_piece in style_pieces:
        piece_code = style_piece["bodyPiece"]["code"]
        piece_material = style_piece["material"]["code"]
        piece_color = style_piece["color"]["code"]

        material_piece_mapping[piece_code] = piece_material
        piece_color_mapping[piece_code] = piece_color

    pieces_mapping_refactored = [
        {
            "key": (
                piece["bodyPiece"]["code"]
                if piece["bodyPiece"] and piece["bodyPiece"]["code"]
                else None
            ),
            "color_code": (
                piece["color"]["code"]
                if piece["color"] and piece["color"]["code"]
                else None
            ),
            "material_code": (
                piece["material"]["code"]
                if piece["material"] and piece["material"]["code"]
                else None
            ),
            "artwork_uri": (
                f"s3://{piece['artwork']['file']['s3']['bucket']}/{piece['artwork']['file']['s3']['key']}"
                if piece["artwork"]
                and piece["artwork"]["file"]
                and piece["artwork"]["file"]["s3"]
                and piece["artwork"]["file"]["s3"]["key"]
                and piece["artwork"]["file"]["s3"]["bucket"]
                else None
            ),
            "offset_size_inches": (
                piece["material"]["offsetSizeInches"]
                if piece["material"] and piece["material"]["offsetSizeInches"]
                else 0
            ),
            "base_image_uri": (
                f"{size_pieces_path}/{body_code}-V{request_body_version}-{piece['bodyPiece']['code']}.png"
                if body_code
                and request_body_version
                and piece["bodyPiece"]
                and piece["bodyPiece"]["code"]
                else None
            ),
        }
        for piece in pieces_mapping
    ]

    logger.info(f"pieces mapping: {json.dumps(pieces_mapping_refactored,indent=2)}")

    if "None" in json.dumps(pieces_mapping_refactored):
        err_message = f"Null value was found in piece mapping, tech please investigate: {json.dumps(pieces_mapping_refactored,indent=2)}"
        logger.info(err_message)

        flag_apply_color_request(
            record_id,
            [
                {
                    "context": err_message,
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                }
            ],
        )

        finish_with_known_exception()

    payload = {
        "id": event.get("request_id"),
        "unit_key": style_code.replace("-", ""),
        "body_code": event.get("body_code"),
        "body_version": event.get("body_version"),
        "color_code": event.get("color_code"),
        "size_code": event.get("size_code"),
        "dxf_path": event.get("dxf_file_path"),
        "pieces_path": size_pieces_path,
        "piece_material_mapping": material_piece_mapping,
        "piece_color_mapping": piece_color_mapping,
        "piece_mapping": pieces_mapping_refactored,
        "flow": "3d",
        "created_at": str(arrow.now()),
    }

    sku = f"{style_code} {event.get('size_code')}"

    logger.info({"WERE_ERRORS_FOUND": os.getenv("WERE_ERRORS_FOUND", "False")})
    if os.getenv("WERE_ERRORS_FOUND", "False").lower() == "false":
        # clearing the flags in case the request was previously flagged in a previous run but now succeeded until this point
        unflag_apply_color_request(record_id)

    # if we want to disable previews being required for this queue item we can do so here
    skip_previews = False
    post_style_size_update(
        sku,
        record_id=record_id,
        body_version=event.get("body_version"),
        style_record_id=style.get("id"),
        skip_previews=skip_previews,
    )

    # deprecate the old flow and update from the new style node
    # with ResKafkaProducer(kafka_client, "res_meta.meta_one.requests") as producer:
    #     r = producer.produce(payload)

    # try sending to the new size model too

    return {}


# if __name__ == "__main__":
#     run_pieces_checks(
#         {
#             "body_code": "TK-6093",
#             "body_version": 8,
#             "color_code": "TOFFOQ",
#             "dxf_file_path": "s3://meta-one-assets-prod/color_on_shape/tk_6093/v8/toffoq/dxf/TK-6093-V8-3D_BODY.dxf",
#             "material_code": "CHRST",
#             "raw_pieces_path": "color_on_shape/tk_6093/v8/toffoq/3zzmd/raw_pieces",
#             "request_id": "rec6SIVid28GbYVzo",
#             "size_code": "3ZZMD",
#             "png_pieces_path": "color_on_shape/tk_6093/v8/toffoq/3zzmd/pieces",
#             "total_pieces_count": 23,
#         }
#     )
#     validate_pieces_checks(
#        {
#            "record_id": "recmnaEnBecP1AliW",
#            "body_code": "KT-6009",
#            "body_version": 10,
#            "color_code": "MDIUQG",
#            "material_code": "RYJ01",
#            "zipped_raw_pieces_uri": "s3://res-temp-public-bucket/style_assets_dev/3ebe985f-b904-4a34-8190-004dc08496cc/asset_bundle.zip",
#            "piece_material_offset_lookup": {
#                "BODFTPNLRT-S": 900,
#                "BODFTPNLLF-S": 900,
#                "BODBKPNL-S": 900,
#                "BODFTPLKRT-BF": 900,
#                "BODFTPLKLF-BF": 900,
#                "BODSLPNLRT-S": 900,
#                "BODSLPNLLF-S": 900,
#                "BODNKCLSTP-BF": 900,
##                "BODNKCLSUN-BF": 900,
#                "BODFTPLKRT-X": 900,
#                "BODSLCUFRT-S": 900,
#                "BODSLCUFLF-S": 900,
#                "SKTBKPNL-S": 900,
#                "SKTBKWBN-X": 900,
#                "SKTFTPNLLF-S": 900,
#                "SKTFTPNLRT-S": 900,
#                "SKTWTPNLTP-BF": 900,
#                "SKTWTPNLUN-BF": 900,
#                "SKTFTPKTLF-S": 900,
#                "SKTFTPKT-X": 900,
#                "SKTFTPNL-X": 900,
#                "SKTFTPKTRT-S": 900,
#            },
#            "piece_material_code_lookup": {
#                "BODFTPNLRT-S": "RYJ01",
#                "BODFTPNLLF-S": "RYJ01",
#                "BODBKPNL-S": "RYJ01",
#                "BODFTPLKRT-BF": "RYJ01",
#                "BODFTPLKLF-BF": "RYJ01",
#                "BODSLPNLRT-S": "RYJ01",
#                "BODSLPNLLF-S": "RYJ01",
#                "BODNKCLSTP-BF": "RYJ01",
#                "BODNKCLSUN-BF": "RYJ01",
#                "BODFTPLKRT-X": "RYJ01",
#                "BODSLCUFRT-S": "RYJ01",
#                "BODSLCUFLF-S": "RYJ01",
#                "SKTBKPNL-S": "RYJ01",
#                "SKTBKWBN-X": "RYJ01",
#                "SKTFTPNLLF-S": "RYJ01",
#                "SKTFTPNLRT-S": "RYJ01",
#                "SKTWTPNLTP-BF": "RYJ01",
#                "SKTWTPNLUN-BF": "RYJ01",
#                "SKTFTPKTLF-S": "RYJ01",
#                "SKTFTPKT-X": "RYJ01",
#                "SKTFTPNL-X": "RYJ01",
#                "SKTFTPKTRT-S": "RYJ01",
#            },
#        }
#    )
