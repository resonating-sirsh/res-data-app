from botocore.retries import bucket
import requests
import boto3
import shutil
import filecmp
import time
from datetime import date, datetime
import os

current_picture_path = os.environ["CURRENT_PICTURE_PATH"]
picture_copy_path = os.environ["PICTURE_COPY_PATH"]
backup_picture_path = os.environ["BACKUP_PICTURE_PATH"]
ENVIRONMENT = "dev"
BUCKET = f"dr-factory-resource-images-{ENVIRONMENT}"
RESOURCE = "this is a test"  # os.environ["RESOURCE_ID"]  # "laser-cutter"
RESOURCE_TYPE = "laser-cutters"  # os.environ["RESOURCE_TYPE"]  # "laser-cutters"
time_now = time.time()
datetime_now = datetime.fromtimestamp(time_now)
filename = (
    f"{RESOURCE_TYPE}_{RESOURCE}_{datetime_now.isoformat(timespec='minutes')}.png"
)
file_upload_path = (
    f"{RESOURCE_TYPE}/{RESOURCE}/{datetime_now.date().isoformat()}/{filename}"
)
s3_key = f"s3://{BUCKET}/{file_upload_path}"


def get_s3_session():  # not used
    print("Getting s3 session")
    session = boto3.Session(
        aws_access_key_id=os.environ["AWS_KEY"],
        aws_secret_access_key=os.environ["AWS_SECRET"],
    )
    print("Connected to S3")
    return session.resource("s3")


def picture_is_new():
    if filecmp.cmp(backup_picture_path, picture_copy_path):
        return False
    else:
        return True


def upload_file(file_name, bucket, object_name):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def push_event_to_kakfa(s3_path):
    print("Pushing Event into Kgateway")
    if ENVIRONMENT == "prod":
        url = "https://data.resmagic.io/kgateway/submitevent"
    else:
        url = "https://datadev.resmagic.io/kgateway/submitevent"

    S3_KEY = s3_key

    event = {
        "TIMESTAMP": "Hello Team!!!",  # str(time.time()),  # str(time.time()),
        "SENSOR_ID": RESOURCE,
        "S3_KEY": S3_KEY,
        "METRIC_NAME": "image_location",
    }

    data = {
        "version": "1.0.0",
        "process_name": "LaserImageUploadEvent",
        "topic": "LaserImageUpload",
        "data": event,
    }
    response = requests.post(url, json=data)
    print(response.text)

    if response.text == "success":
        return True
    else:
        return False


def start():

    # 1. create a copy of the current image.
    shutil.copyfile(current_picture_path, picture_copy_path)
    upload_path = "s3://{BUCKET}/{LAYER}}/{datetime_now.year}/{datetime_now.month}/{datetime_now.day}/{FILENAME}}"

    # 2. Compare image with backup image.
    if picture_is_new():

        # 3. Upload picture to s3 if it's a new picture
        if upload_file(current_picture_path, BUCKET, file_upload_path):
            print("Sucessful Upload!")
        else:
            print("Failed to Upload first time... Retrying....")
            if upload_file(current_picture_path, BUCKET, file_upload_path):
                print("Sucessful Upload!")
            else:
                print("Failed to Upload")
                return 0
    else:
        print("No new picture to upload")
        return 0

    # 4. Make backup of current image
    shutil.copyfile(current_picture_path, backup_picture_path)
    # 5. Push event to Kafka.
    push_event_to_kakfa(upload_path)


if __name__ == "__main__":
    start()
