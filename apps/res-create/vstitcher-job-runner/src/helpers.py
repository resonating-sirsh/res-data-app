import os
import subprocess
from pathlib import Path

from res.utils.logging import logger
from res.utils.dates import utc_now_iso_string
from res import connectors


def kill_job_processes(job_id):
    """
    Finds OS processes for a given job number, and sends a kill signal.
    """
    logger.info(f"killing OS processes for {job_id}")
    for line in subprocess.run(
        ["ps", "-A"], stdout=subprocess.PIPE
    ).stdout.splitlines():
        if job_id in str(line):
            proc_id = int(line.split(None, 1)[0])
            logger.info(f"killing pid {proc_id}")
            os.kill(proc_id, 9)


TTL_FILENAME = ".ttl"


def make_job_ttl_file(job_dir: Path, seconds: int):
    open(job_dir / TTL_FILENAME, "w").write(str(seconds))


def ttl_file_exists(job_dir: Path) -> bool:
    return (job_dir / TTL_FILENAME).exists()


def get_job_ttl_seconds(job_dir: Path):
    seconds_str = open(job_dir / TTL_FILENAME, "r").read()
    if seconds_str is not None:
        try:
            return int(seconds_str)
        except:
            return None


def notify_meta_one_queue_status(job={}, error_dict={}, status="FAILED"):
    job_details = job.get("details", {})
    job_preset = job.get("preset", {}).get("details", {})

    try:
        payload = {
            "created_at": utc_now_iso_string(None),
            "body_code": job_details.get("body_code", ""),
            "body_version": job_details.get("body_version", ""),
            "id": job_details.get("style_id", job_details.get("body_id", "")),
            "size_code": "",
            "flow": "dxa",
            "node": "bertha",
            "meta_one_path": job_preset.get("remote_destination", ""),
            "dxf_path": job_details.get("input_file_uri", ""),
            "status": status,
            "unit_key": job_details.get("body_code", ""),
            "color_code": "",
            "unit_type": "",
            "tags": [],
            "validation_error_map": error_dict,
        }

        # print("payload", payload)
        connectors.load("kafka")["res_meta.meta_one.status_updates"].publish(
            payload, use_kgateway=True, coerce=True
        )
    except Exception as e:
        logger.warn("Exception notifying meta.one queue status", error=str(e))
