from res.connectors.s3 import S3Connector
from res.flows.create import assets
from res.utils.logging import logger
import helpers


from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
import asyncio
import json
import os
import shutil
import time
from tenacity import retry, wait_fixed, stop_after_attempt
import traceback


_s3_connector = S3Connector()
_s3_client = _s3_connector.get_client()

LOCAL_ASSET_OUTPUT_DIR = os.getenv("LOCAL_ASSET_OUTPUT_DIR")


def make_watcher(watched_dir, on_create_handler):
    patterns = ["*"]
    ignore_patterns = [".DS_Store"]
    ignore_directories = True
    case_sensitive = True
    event_handler = PatternMatchingEventHandler(
        patterns, ignore_patterns, ignore_directories, case_sensitive
    )
    event_handler.on_created = on_create_handler
    # event_handler.on_modified = on_create_handler

    observer = Observer()
    observer.schedule(event_handler, watched_dir, recursive=True)
    return observer


JOB_JSON_FILE = "job.json"


def job_from_file_event(event):
    try:
        path = Path(event.src_path)
        path = path.parent if not event.is_directory else path

        # go up until just before you get to LOCAL_ASSET_OUTPUT_DIR
        dir_levels = len(LOCAL_ASSET_OUTPUT_DIR.split("/")) + 1
        while len(path.parts) > dir_levels:
            path = path.parent

        job_json_path = path / JOB_JSON_FILE

        if job_json_path.exists():
            return json.load(open(job_json_path))
    except:
        pass

    return None


@retry(wait=wait_fixed(10), stop=stop_after_attempt(3))
async def upload_asset(new_file_path, job, job_id, file_instruction):
    try:
        logger.info("uploading asset", asset=new_file_path.name)

        # Sometimes VStitcher is still writing to files after creation. We
        # need to wait for that to finish to avoid incomplete files
        file_finished_writing = False
        file_size = new_file_path.stat().st_size
        await asyncio.sleep(3)  # give a few seconds for the file to write
        while not file_finished_writing:
            file_finished_writing = new_file_path.stat().st_size == file_size
            if file_finished_writing:
                break
            else:
                file_size = new_file_path.stat().st_size
                logger.debug("Waiting for file to finish")
                await asyncio.sleep(3)

        s3_output_path = job["preset"]["details"]["remote_destination"]
        s3_path = f"{s3_output_path}{job_id}/{new_file_path.name}"

        logger.info("Uploading file", local=new_file_path, dest=s3_path)

        is_public = job["details"].get("style_id") is not None

        insert_asset_resp = await assets.insert(
            {
                "path": s3_path,
                "job_id": job_id,
                "type": file_instruction.get("export_type"),
                "style_id": job["details"].get("style_id"),
                "body_id": job["details"].get("body_id"),
                "name": new_file_path.name,
                "is_public_facing": is_public,
                "status": "IN_PROGRESS",
            }
        )
        logger.info(f"Inserted Asset {insert_asset_resp}")

        artifact_record = insert_asset_resp[0]
        asset_id = artifact_record.get("id")

        logger.info(f"Created asset in db. Uploading to S3: {s3_path}")
        if is_public:
            logger.debug("create public asset. As a temporary hack")
            public_path = f"style_assets_dev/{job_id}/{new_file_path.name}"
            # This is a temporary dive down to the boto3 client, to pass along the ACL Args
            _s3_client.upload_file(
                str(new_file_path),
                "res-temp-public-bucket",
                public_path,
                ExtraArgs={"ACL": "public-read"},
            )
        else:
            logger.info(
                f"Created Artifact Record in database. Uploading to private S3 object: {s3_path}"
            )
            _s3_connector.upload(str(new_file_path), s3_path)

        await assets.update_by_id(asset_id, {"status": "UPLOADED"})
        logger.info("Upload Completed!", path=new_file_path)
        helpers.notify_meta_one_queue_status(job, status="OK")
    except:
        logger.error("Couldn't upload file", traceback.format_exc())
        raise


async def handle_file_create_event(event):
    """
    Listen for new files that are specified in the job.json, and upload
    them to their destination.

    Also listen for the creation of a DONE file, which signifies that the job
    is finished and the folder can be removed.
    """
    logger.debug("file change", e=event)
    new_file_path = Path(event.src_path)
    job_dir = new_file_path.parent
    if (
        not event.is_directory
        and event.event_type == "created"
        and new_file_path.name != JOB_JSON_FILE
        and new_file_path.exists()
    ):
        logger.debug("handling file change", e=event)
        job_json_path = new_file_path.parent / JOB_JSON_FILE

        if not job_json_path.exists():
            # No job JSON found at path {job_json_path}. Ignore
            return

        job = json.load(open(job_json_path))
        job_id = job.get("id")

        # Job is complete. We need to clean up the directory
        if new_file_path.name == "DONE" and new_file_path.parent.parent == Path(
            LOCAL_ASSET_OUTPUT_DIR
        ):
            # raise Exception("Testing...")
            logger.info(f"Removing files now")
            shutil.rmtree(new_file_path.parent)
            return
        elif new_file_path.name == ".ttl":
            timeout_secs = helpers.get_job_ttl_seconds(job_dir)
            if timeout_secs is not None:
                logger.info(
                    f"Starting timeout of {timeout_secs}. If TTL file not removed, job {job_id} will be killed"
                )
                await asyncio.sleep(timeout_secs)
                if helpers.ttl_file_exists(job_dir):
                    logger.info(f"Hit timeout, killing.")
                    helpers.notify_meta_one_queue_status(
                        job,
                        {"timeout": f"VS didn't finish after {timeout_secs}s"},
                        "FAILED",
                    )
                    helpers.kill_job_processes(job_id)
                    open(job_dir / ".errors.txt", "w").write(
                        f"Job Failed to start. Killed after {timeout_secs} seconds"
                    )
            return

        logger.info(f"Scanning preset instructions...")

        all_instructions = job["preset"]["details"]["instructions"]
        file_instructions = [
            i for i in all_instructions if i["output_file_name"] == new_file_path.name
        ]

        if len(file_instructions) == 0:
            logger.debug("file not in expected outputs. Ignoring")
            return

        await upload_asset(new_file_path, job, job_id, file_instructions[0])


if __name__ == "__main__":
    if LOCAL_ASSET_OUTPUT_DIR is None:
        raise Exception(
            "No LOCAL_ASSET_OUTPUT_DIR found! Check your environment variables"
        )

    def sync_handler(e):
        # It's quite important that this be handled sync! Otherwise we might
        # delete files before upload
        try:
            asyncio.run(handle_file_create_event(e))
        except Exception as x:
            x_details = traceback.format_exc()
            try:
                job = job_from_file_event(e)
                if job:
                    helpers.notify_meta_one_queue_status(
                        job, error_dict={"exception": str(x)}, status="FAILED"
                    )
            except Exception as ex:
                logger.error(f"Failed to notify meta one queue: {ex}")

            logger.error(
                f"VStitcher File Sync Daemon encountered error uploading create asset: {x_details}"
            )

    watcher = make_watcher(
        LOCAL_ASSET_OUTPUT_DIR,
        sync_handler,
    )
    watcher.start()
    logger.info(f"Started file watcher on {LOCAL_ASSET_OUTPUT_DIR}")
    while True:
        time.sleep(1)
