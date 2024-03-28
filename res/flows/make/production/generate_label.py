from blabel import LabelWriter
import boto3
import tempfile
import os
import json
from res.utils import logger, secrets
from functools import lru_cache
from pyairtable import Table
from tenacity import retry, wait_fixed, stop_after_attempt

AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
s3 = boto3.resource("s3")


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    logger.info(f"Airtable request args: {args}, kwargs: {kwargs}")
    response = getattr(cnx, method)(*args, **kwargs)
    logger.info(f"Airtable response: {json.dumps(response)}")
    return response


def handle_new_event(event_data):
    logger.info(f"event -> {event_data}.")
    logger.info("(1/2) Process new transaction")

    template_name = event_data["template_name"]
    style_name = event_data["style_name"]
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            logger.info(tmpdir)
            s3.meta.client.download_file(
                "res-on-premises-logs-prod",
                f"labels_templates/{template_name}.html",
                f"{tmpdir}/{template_name}.html",
            )

            s3.meta.client.download_file(
                "res-on-premises-logs-prod",
                f"labels_templates/{style_name}.css",
                f"{tmpdir}/{style_name}.css",
            )

            logger.info("blable process")
            label_writer = LabelWriter(
                f"{tmpdir}/{template_name}.html",
                default_stylesheets=(f"{tmpdir}/{style_name}.css",),
            )

            label_info = [dict(event_data=event_data)]

            label_writer.write_labels(
                label_info, target=f"{tmpdir}/{event_data['label_name']}.pdf"
            )

            logger.info("airtable process")

            url = upload_to_s3(f"{tmpdir}/{event_data['label_name']}.pdf", event_data)
            call_airtable(
                Table(
                    secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
                    event_data["base_id"],
                    event_data["table_id"],
                ),
                event_data["record_id"],
                {event_data["field_name"]: url},
                typecast=True,
                method="update",
            )
            print(url)

    except Exception as e:
        logger.info(e)


def upload_to_s3(file_path, event_data):
    s3.Bucket("res-on-premises-logs-prod").upload_file(
        file_path, f"{event_data['s3_folder']}/{event_data['label_name']}.pdf"
    )
    url = f"https://res-on-premises-logs-prod.s3.amazonaws.com/{event_data['s3_folder']}/{event_data['label_name']}.pdf"
    return url


if __name__ == "__main__":
    data = {
        "payload": {
            "id": "null",
            "roll_id": 8000,
            "material": "PIQUE",
            "po": "PO-4147",
            "yards": 556.0,
            "legacy": 55,
        },
        "template_name": "rolls_labels_template",
        "style_name": "rolls_labels_style",
        "label_name": "R8000_pique",
        "s3_folder": "rolls-tags",
        "base_id": "appKwfiVbkdJGEqTm",
        "table_id": "tblCWvgoBYJfqwHKJ",
        "field_name": "Roll Tag",
        "record_id": "recOMzn6jasgfqpDH",
    }

    handle_new_event(data)
