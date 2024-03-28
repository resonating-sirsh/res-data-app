"""VStitcher job runner Application.



export PLATFORM_JOB_TYPES=DXA_EXPORT_BODY_BUNDLE,DXA_EXPORT_ASSET_BUNDLE,BRAND_ONBOARDING
#for example
export VSTITCHER_PLUGIN_DIR=/Users/????/code/res/browzwear 
export LOCAL_ASSET_OUTPUT_DIR=/Users/????/Downloads/job_runner
"""


from pathlib import Path
import asyncio
import json
import os
from urllib.parse import urlparse
import subprocess
import res
from res.connectors.s3 import S3Connector
from res.utils.logging import logger
import helpers

from res.flows.create import assets
from res.flows.dxa import node_run

s3c = res.connectors.load("s3")


LOCAL_ASSET_OUTPUT_DIR = os.getenv("LOCAL_ASSET_OUTPUT_DIR")

VSTITCHER_PLUGIN_DIR = os.getenv("VSTITCHER_PLUGIN_DIR")

PLATFORM_JOB_TYPES = [
    e.strip() for e in (os.getenv("PLATFORM_JOB_TYPES", "") or "").split(",")
]

VSTITCHER_PLUGIN_DIR = os.environ.get("VSTITCHER_PLUGIN_DIR")

# if VSTITCHER_PLUGIN_DIR:
#     print(f"Adding plugin dir {VSTITCHER_PLUGIN_DIR} to sys path")
#     sys.path.append(str(VSTITCHER_PLUGIN_DIR))


#
try:
    pt = Path(__file__).parents[1]
except:
    # best effort - it was as above which i think is fine remote but unless app name changes the below is fine
    pt = (
        Path(res.__file__).parent.parent
        / "apps"
        / "res-create"
        / "vstitcher-job-runner"
    )

JSON_SCHEMA_DIR = pt / "schemas"
job_details_schema = json.load(open(JSON_SCHEMA_DIR / "job_details.json"))
job_presets_schema = json.load(open(JSON_SCHEMA_DIR / "job_preset.json"))

_s3_connector = S3Connector()


ACK_INTERVAL_SECONDS = 10
ERROR_TIMEOUT_SECS = 30

VSTITCHER_JOB_TTL_SECONDS = 60


def invoke_vstitcher(*, job_id, garment_file_path, job_details_file_path: str):
    """Invoke VStitcher as a subcommand against a working directory."""
    stitcher_script_path = "run.py"

    errors = {}
    # TODO: Pipe logs into a logfile
    cmd = " ".join(
        [
            "python3",
            stitcher_script_path,
            "--garment-path",
            garment_file_path,
            "--job-details-file",
            job_details_file_path,
        ]
    )
    try:
        # 1337 haxx0r 4l3rt
        # run 7h3 bw_p4rs3r scr!p7 s0 w3 c4n g37 rul3rs d!r3c71y fr0m 7h3 bw f!l3
        logger.info("Running bw_parser.py")
        file_path = os.path.realpath(__file__)
        parent = os.path.dirname(file_path)
        parser_path = os.path.join(parent, "bw_parser.py")
        subprocess.run(["python3", parser_path, garment_file_path])

        timeout_seconds = 60 * 60
        logger.info(
            f"Running VStitcher from directory {VSTITCHER_PLUGIN_DIR}",
            command=cmd,
            cwd=VSTITCHER_PLUGIN_DIR,
        )

        error_file = open(VSTITCHER_PLUGIN_DIR + "/last_run_errors.txt", "w")
        out_file = open(VSTITCHER_PLUGIN_DIR + "/last_run_out.txt", "w")

        # this was subprocess.run but it seems to hang if the output is too large
        # dev job de21836a-0f15-4f69-bb6e-3e7d176bb340 for example
        # https://stackoverflow.com/questions/39900020/python-subprocess-not-returning
        # so we use Popen and wait with a timeout, another benefit is we can tail -f
        p = subprocess.Popen(
            cmd,
            cwd=VSTITCHER_PLUGIN_DIR,
            shell=True,
            stdout=out_file,
            stderr=error_file,
            text=True,
        )

        return_code = p.wait(timeout=timeout_seconds)

        error_file.close()
        out_file.close()

        if return_code != 0:
            cmd_msg = (
                "Nonzero return code from cmd to invoke vstitcher plugin. "
                "This probably indicates a machine config issue"
            )
            logger.warn(cmd_msg)
            errors["cmd"] = f"{cmd}: {p.returncode} {cmd_msg}"

        try:
            logger.info(f"Added output for last run to {VSTITCHER_PLUGIN_DIR}")

            date = res.utils.dates.utc_now_iso_string(None).split("T")[0]
            s3root = f"s3://res-data-platform/uploads/bertha/{date}/{job_details_file_path.split('/')[-2]}"
            logger.info(f"Done. Will try upload logs to s3 - {s3root}")
            s3c.upload(
                VSTITCHER_PLUGIN_DIR + "/last_run_errors.txt",
                f"{s3root}/err.txt",
            )
            s3c.upload(
                VSTITCHER_PLUGIN_DIR + "/last_run_out.txt",
                f"{s3root}/out.txt",
            )
        except Exception as ex:
            print("NO:", repr(ex))
        else:
            logger.info("Vstitcher exited gracefully")

    except Exception as e:
        logger.info("Exception in VStitcher Subprocess!", error=str(e))
        errors["vs exception"] = str(e)

    # VStitcher should exit gracefully, but just in case to ensure there are no
    # orphaned processes draining memory, kill anything outstanding
    # VStitcher processes will have been kicked off with arguments that include
    # the job_id. So find that, and kill it.
    logger.info("Killing VStitcher job processes")
    try:
        helpers.kill_job_processes(job_id)
    except Exception as e:
        logger.error(
            "Error killing the VStitcher job process on the runner machine!", str(e)
        )
        errors["kill exception"] = str(e)

    return errors


def download_latest_bw_file(target_body_name, target_folder="./"):
    target_body_name = target_body_name.lower().replace("-", "_")
    dir = f"s3://meta-one-assets-prod/bodies/3d_body_files/{target_body_name}/"

    files = s3c.ls_info(dir, [".bw"])

    if not files:
        logger.error(f"Unable to find body: {target_body_name}")
        raise Exception(f"Unable to find body: {target_body_name}")

    files = sorted(files, key=lambda f: f["last_modified"])
    newest_file = files[-1]

    s3c._download(newest_file["path"], target_folder)

    return os.path.join(target_folder, newest_file["path"].split("/")[-1])


async def run_asset_request_job(job_id):
    """
    Run a VStitcher job to completion.

    Given a job, it will download all required assets, set up the
    job directory, and invoke VStitcher.
    """
    logger.info(f"fetching job details for job: {job_id}")
    job = await node_run.fetch_by_id(job_id)
    logger.info("running job", job=job)
    job_details = job.get("details")
    job_presets = job.get("preset", {}).get("details")

    logger.info("Validating Job Details and Preset")
    # Should probably combine these
    # jsonschema.validate(instance=job_details, schema=job_details_schema)
    # jsonschema.validate(instance=job_presets, schema=job_presets_schema)

    job_dir = Path(LOCAL_ASSET_OUTPUT_DIR) / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    job_details["local_job_output_path"] = str(job_dir)

    for p in Path(LOCAL_ASSET_OUTPUT_DIR).iterdir():
        if p.suffix == ".done":
            p.unlink()

    # --- Artwork File ---: for default color placement
    # Grab artwork from s3
    if job_details.get("artwork_file_path"):
        job_details["remote_artwork_file_path"] = job_details.get("artwork_file_path")
        job_details.pop("artwork_file_path")

    # Grab artwork from s3
    remote_artwork_file_path = job_details.get("remote_artwork_file_path")
    if remote_artwork_file_path:
        logger.info(
            "Downloading artwork file",
            remote_artwork_file_path=remote_artwork_file_path,
        )
        local_artwork_path = _s3_connector._download(
            remote_artwork_file_path, target_path=job_dir
        )
        job_details["local_artwork_path"] = local_artwork_path
    else:
        logger.info("No remote artwork specified. Skipping download")

    # --- Artwork Bundle (directory)---: for custom color re-placement
    # Grab artwork from s3
    remote_artwork_bundle_dir_path = job_details.get("remote_artwork_bundle_dir_path")
    if remote_artwork_bundle_dir_path:
        logger.info(
            "Downloading artwork bundle directory",
            remote_artwork_bundle_dir_path=remote_artwork_bundle_dir_path,
        )

        local_artwork_bundle_dir_path = (
            Path(LOCAL_ASSET_OUTPUT_DIR) / job_id / "artwork_bundle"
        )
        local_artwork_bundle_dir_path.mkdir(parents=True, exist_ok=True)

        try:
            # download artwork_bundle dir
            _s3_connector._s3fs.download(
                rpath=remote_artwork_bundle_dir_path,
                lpath=str(local_artwork_bundle_dir_path),
                recursive=True,
            )
        except Exception as e:
            import traceback

            logger.error(traceback.format_exc())
            raise e

        job_details["local_artwork_bundle_dir_path"] = str(
            local_artwork_bundle_dir_path
        )
    else:
        logger.info("No remote artwork bundle dir specified. Skipping download")

    replacements = job_details.get("artwork_replacements")
    if replacements:
        dest_dir = Path(LOCAL_ASSET_OUTPUT_DIR) / job_id / "artwork_replacements"
        dest_dir.mkdir(parents=True, exist_ok=True)

        # keep track of remote_file_paths so we don't download the same one twice
        # but if they have the same filename, download it to a unique local path
        mapping = {}
        try:
            for replacement in replacements:
                remote_file_path = replacement.get("remote_file_path")
                if remote_file_path not in mapping:
                    filename = os.path.basename(urlparse(remote_file_path).path)
                    local_file_path = dest_dir / filename
                    i = 1
                    while local_file_path.exists():
                        local_file_path = dest_dir / f"{i}_{filename}"
                        i += 1

                    logger.info(f"Downloading {remote_file_path} to {local_file_path}")
                    _s3_connector._download(remote_file_path, target_path=dest_dir)
                    mapping[remote_file_path] = local_file_path
                else:
                    logger.info(f"Already downloaded {remote_file_path}")

                replacement["local_file_path"] = str(mapping[remote_file_path])

        except Exception as e:
            import traceback

            logger.error(traceback.format_exc())
            raise e
    else:
        logger.info("No artwork replacements specified. Skipping download")

    # Grab body file
    logger.info("Downloading body/style file")

    if job_details.get("input_file_uri"):
        body_file_name = Path(job_details.get("input_file_uri")).name.replace(" ", "")
        logger.info(
            f"Downloading body file from s3 ({job_details.get('input_file_uri')}) - Filename: {body_file_name} to {job_dir}"
        )
        bw_file_path = _s3_connector._download(
            job_details.get("input_file_uri"),
            target_path=job_dir,
            filename=body_file_name,
        )
        res.utils.logger.info(
            f"Downloaded {bw_file_path}. Exists {Path(bw_file_path).exists()}"
        )

    elif job_details.get("body_code"):
        logger.info("Downloading from S3")
        body_code = job_details.get("body_code")
        bw_file_path = download_latest_bw_file(body_code, target_folder=job_dir)
    else:
        raise Exception("No Body file specified. Can't Continue")

    job_details_file = job_dir / "job.json"

    # Maybe hide pre-existing style from end-users. This is a stupid hack that
    # should be removed when we have a sane data model for brand onboarding
    style_id = job_details.get("style_id")
    if (
        style_id is not None
        and job_presets.get("should_hide_previous_style_assets") is True
    ):
        logger.info(f"Hiding style assets for {style_id}")
        await assets.hide_style_assets_for_style(style_id)

    open(job_details_file, "w").write(json.dumps(job))

    helpers.make_job_ttl_file(job_dir, VSTITCHER_JOB_TTL_SECONDS)

    cmd_errors = invoke_vstitcher(
        garment_file_path=str(bw_file_path),
        job_details_file_path=str(job_details_file),
        job_id=job_id,
    )

    if cmd_errors:
        helpers.notify_meta_one_queue_status(
            job, error_dict=cmd_errors, status="FAILED"
        )

    if (job_dir / ".errors.txt").exists():
        e = open(job_dir / ".errors.txt").read()
        raise Exception(f"Errors running vstitcher: {str(e)}")
    if not (job_dir / ".completed.txt").exists():
        logger.info(f"VStitcher did not complete successfully, maybe killed?")
        bad_popups = ""
        pop_path = Path("/tmp/pop.txt")
        if pop_path.exists():
            bad_popups = pop_path.read_text()
            pop_path.rename(job_dir / "popups.txt")
            logger.info(bad_popups)
        raise Exception(
            f"VStitcher did not complete successfully, maybe killed?\n{bad_popups}"
        )
    if cmd_errors:
        raise Exception(f"Errors running vstitcher: {str(cmd_errors.values())}")


async def job_worker_loop():
    """
    Run VSticher job worker loop.#

    This claims jobs, invokes VStitcher, and continues on forever
    """

    logger.info(f"started worker. Job types: {PLATFORM_JOB_TYPES}")
    while True:
        try:
            jobs = await node_run.claim_next(run_types=PLATFORM_JOB_TYPES)
        except Exception as e:
            logger.error("Error attempting to claim job!", error=e)
            await asyncio.sleep(ERROR_TIMEOUT_SECS)
            continue

        if not len(jobs) > 0:
            await asyncio.sleep(3)
            continue

        job = jobs[0]
        job_id = job.get("id")
        logger.info("Claimed Job", job_id=job_id)
        logger.info(f"VSTITCHER_PLUGIN_DIR={VSTITCHER_PLUGIN_DIR}")

        errors = None
        try:
            resp = await run_asset_request_job(job_id)
            logger.info(f"ran asset job. Resp: {resp}")
        except Exception as e:
            errors = str(e)
            logger.warn(errors)

        try:
            if errors is None:
                await node_run.update_status_by_id(
                    job_id, node_run.Status.COMPLETED_SUCCESS.value
                )
            else:
                logger.warn("Errors encountered. Setting job to failed", errors=errors)
                helpers.notify_meta_one_queue_status(
                    job, error_dict={"exception": errors}, status="FAILED"
                )
                await node_run.update_status_by_id(
                    job_id, node_run.Status.COMPLETED_ERRORS.value, str(errors)
                )
        except Exception as e:
            logger.error(
                f"Unable to set jobs to failed! Check graphql connection {repr(e)}",
                errors=str(e),
            )

        # Signal to the file sync daemon that it can kill this directory after
        # it's finished uploading assets
        logger.info(f"Finished {job_id}. Adding a DONE file")
        (Path(LOCAL_ASSET_OUTPUT_DIR) / job_id / "DONE").touch()


if __name__ == "__main__":
    required_env_vars = [
        "LOCAL_ASSET_OUTPUT_DIR",
        "VSTITCHER_PLUGIN_DIR",
        "PLATFORM_JOB_TYPES",
    ]
    for v in required_env_vars:
        if not os.getenv(v):
            logger.error(f"Required ENV var missing: {v}")
            exit()

    Path(LOCAL_ASSET_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    asyncio.run(job_worker_loop())
