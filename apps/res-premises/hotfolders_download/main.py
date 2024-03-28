import boto3
from airtable import Airtable
import os
import time
import shutil
import re
from res.utils import secrets
from res.utils.logging.ResLogger import ResLogger
from tenacity import retry, wait_fixed, stop_after_attempt
from functools import lru_cache
from itertools import groupby
from operator import itemgetter
import res.utils.premises.premtools as premtools

DEVICE_NAME = os.getenv("DEVICE_NAME")
RES_ENV = os.getenv("RES_ENV", "production")
RES_NAMESPACE = os.getenv("RES_NAMESPACE", "res_premises")
RES_APP_NAME = os.environ["RES_APP_NAME"] = "hotfolders_download"
logger = ResLogger()

# os.environ.get('BUCKET') # name of the s3 bucket
ASSET_FILE_NAME = "printfile_composite.png"
DOWNLOAD_PATH = os.environ.get("PRINT_ASSET_DOWNLOAD_PATH")
RIP_PATH = os.environ.get("RIP_PATH", "/")
PRINTER_MACHINE_NAME = os.environ.get("RESOURCE_NAME")


def initialize_secrets():
    secrets.secrets_client.get_secret("AIRTABLE_API_KEY", force=True)


@lru_cache(maxsize=1)
def get_s3_session():
    logger.info("Getting s3 session")
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET"],
    )
    logger.info("Connected to S3")
    return session.resource("s3")


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="get_all", **kwargs):
    return getattr(cnx, method)(*args, **kwargs)


@lru_cache()
def s3_bucket(bucket_name):
    s3 = get_s3_session()
    logger.info(f"getting s3 session for bucket {bucket_name}")
    return s3.Bucket(bucket_name)


def get_asset_file(**asset_fields):
    file_name = get_file_name(asset_fields.get("Asset Path"))
    file_bucket = s3_bucket(get_file_bucket(asset_fields.get("Asset Path")))
    return file_bucket.Object(f"{file_name}/{ASSET_FILE_NAME}")


def get_asset_file_size(**asset_fields):
    asset_file = get_asset_file(**asset_fields)
    return asset_file.content_length


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def download_file(asset_file, local_path):
    return asset_file.download_file(local_path)


def start():
    initialize_secrets()

    # Connecting to Airtable
    cnx_print_nested_asset_sets = Airtable(
        "apprcULXTWu33KFsh", "tblAQcPuKUDVfU7Fx", os.environ.get("AIRTABLE_API_KEY")
    )

    logger.info("Connected to Airtable")
    formula = (
        "AND({Print Asset Downloaded?}=FALSE(),{Nests}!='',{Rolls}!='',{Asset Path}!='',"
        "{Asset File Status}!='Downloaded', {Assigned Printer}="
        f"'{PRINTER_MACHINE_NAME}')"
    )

    assets_pending_for_download = call_airtable(
        cnx_print_nested_asset_sets,
        formula=formula,
        sort=[("Rolls", "asc"), ("Rank", "asc")],
    )

    if not assets_pending_for_download:
        logger.info("Nothing to work with")
        return 0

    try:
        assets_pending_by_roll = {}
        for roll_id, assets in groupby(
            assets_pending_for_download, key=lambda rec: rec["fields"]["Rolls"][0]
        ):
            assets_pending_by_roll[roll_id] = [
                {
                    **asset,
                    "asset_file": get_asset_file(**asset["fields"]),
                    "asset_file_size": get_asset_file_size(**asset["fields"]),
                }
                for asset in assets
            ]

    except Exception as e:
        logger.error(f"Issue getting file sizes files...\n{formula!r}\n{e!r}")
        raise e

    total, used, free = shutil.disk_usage(DOWNLOAD_PATH)  # downloads drive
    total_m, used_m, free_m = shutil.disk_usage(RIP_PATH)  # main drive

    roll_asset_sizes = {
        roll_id: sum(map(itemgetter("asset_file_size"), assets))
        for roll_id, assets in assets_pending_by_roll.items()
    }

    queued_rolls = []
    all_rolls_asset_size = sum(roll_asset_sizes.values())
    if all_rolls_asset_size <= free - (
        0.3 * total
    ) and all_rolls_asset_size <= free_m - (0.4 * total_m):
        queued_rolls = list(roll_asset_sizes.keys())
        logger.info(f"All roll assets fit in drive! {all_rolls_asset_size}B")
    else:
        remaining = free - (0.25 * total)
        remaining_m = free - (0.25 * total_m)
        for roll_id, size in sorted(roll_asset_sizes.items(), key=lambda kv: kv[1]):
            if size <= remaining and size <= remaining_m:
                logger.info(
                    f"Roll {roll_id} will fits in drive! Will queue assets. "
                    f"Size: {size}B Remaining: {remaining}B"
                )
                queued_rolls.append(roll_id)
                remaining -= size
                remaining_m -= size

            else:
                logger.info(
                    f"Roll {roll_id} will not fit in drive! "
                    f"Size: {size}B Remaining: {remaining}B"
                )

    queued_assets = [
        asset for roll_id in queued_rolls for asset in assets_pending_by_roll[roll_id]
    ]

    updates = [
        {"id": asset["id"], "fields": {"Asset File Status": "Queued"}}
        for asset in queued_assets
    ]

    call_airtable(
        cnx_print_nested_asset_sets, updates, typecast=True, method="batch_update"
    )

    for asset in queued_assets:
        time.sleep(0.5)
        logger.info("Generating local file name")
        logger.info(DOWNLOAD_PATH)
        logger.info(asset["fields"].get("RIP File Name"))
        local_asset_file = (
            f"{DOWNLOAD_PATH}/{asset['fields'].get('Material Code')[0]}"
            f"/{asset['fields'].get('RIP File Name')}.png"
        )
        logger.info(f"Working with: {local_asset_file}")
        asset_file = asset["asset_file"]

        try:
            logger.info(f"Trying download: {asset_file.key}")
            # Innitiating S3 Session.
            asset_file.download_file(local_asset_file)
        except Exception as e:
            logger.error(
                f"Issues downloading file...\n{asset['id']!r}"
                f"\n{e!r}...\nCleaning download Status"
            )
            cnx_print_nested_asset_sets.update(
                asset["id"],
                fields={"Asset File Status": None},
                typecast=True,
            )
            time.sleep(0.5)
            continue

        call_airtable(
            cnx_print_nested_asset_sets,
            asset["id"],
            fields={
                "Print Asset Downloaded?": True,
                "File Path in Local Machine": local_asset_file,
                "Asset File Status": "Downloaded",
            },
            typecast=True,
            method="update",
        )
        logger.info("Downloaded. Cooling off...")
        time.sleep(0.5)


def get_file_name(raw_file_name):
    logger.info("Working with geting the file")
    logger.info(f"Raw File name is: {raw_file_name}")
    assert raw_file_name.startswith("s3://"), "Asset Path must start with s3://!"
    split_name = raw_file_name.split(re.findall("s3://.+?/", raw_file_name)[0])
    return split_name[1]


def get_file_bucket(raw_file_name):
    s3_bucket = re.findall("s3://(.+?)/", raw_file_name)[0]
    return s3_bucket


if __name__ == "__main__":
    try:
        if premtools.internet_is_up:
            start()
        else:
            exit()

    except Exception as e:
        logger.error(
            f"An unexpected error has ocurred.\n {DEVICE_NAME}-{RES_APP_NAME}\n{e!r}"
        )
        raise e
