import os
from airtable import Airtable
import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration
from res.utils.logging import ResLogger
import boto3
from botocore.client import ClientError
import re
import pprint

logger = ResLogger()
s3 = boto3.client("s3")
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
reports_directory = os.getenv("REPORTS_DIRECTORY")
uploaded_reports_directory = os.getenv("UPLOADED_REPORTS_DIRECTORY")

if os.environ.get("SENTRY_API_KEY"):
    print("Initializing Sentry")

    sentry_sdk.init(
        dsn=f"https://{os.environ.get('SENTRY_API_KEY')}@o571782.ingest.sentry.io/5893438",
        integrations=[AwsLambdaIntegration()],
        traces_sample_rate=1.0,
    )


def main():
    cnx_distortion_data = Airtable(
        "appWxYpvDIs8JGzzr",
        "tblNYNOHuf3MxyLK5",
        AIRTABLE_API_KEY,
    )
    cnx_rolls = Airtable(
        "appWxYpvDIs8JGzzr",
        "tblG5x0hWNr3YQ0ge",
        AIRTABLE_API_KEY,
    )

    for file in os.listdir(reports_directory):
        file_path = f"{reports_directory}{file}"
        if file.endswith(".txt"):
            url = upload_to_s3(file_path, file)
            with open(file_path) as f:
                roll_name = os.path.basename(f.name)
                roll_name = "R" + re.findall("\d+", roll_name)[0]
                try:
                    roll_id = get_roll_id(cnx_rolls, roll_name)
                except Exception as e:
                    logger.warning(f"No available info for {roll_name}")
                    continue
                for count, line in enumerate(f):
                    if count < 5:
                        payload = create_payload(line.split(), roll_id, url)
                        print(payload)
                        cnx_distortion_data.insert(payload)
                    else:
                        break
            os.replace(file_path, f"{uploaded_reports_directory}{file}")
        else:
            continue


def get_roll_id(cnx_rolls, roll_name):
    roll = cnx_rolls.get_all(formula=f"SEARCH('{roll_name}', Name)")
    roll_id = roll[0]["fields"].get("_record_id")
    return roll_id


def create_payload(line, roll_id, url):
    payload = {
        "Roll ID": [roll_id],
        "d0: Length": float(line[0]),
        "d1: Date": f"{line[1]} {line[2]} {line[3]}",
        "d2: Skew Set Point": float(line[4]),
        "d3: Bow Set Point": float(line[5]),
        "d4: Skew <>": float(line[6]) / 100,
        "d5: Bow ()": float(line[7]) / 100,
        "d6: Spare Value": float(line[8]),
        "s3_url": url,
    }
    for key, value in payload.items():
        if value == "nan":
            payload[key] = ""
    return payload


def upload_to_s3(file_path, file):
    print(file_path)
    s3.upload_file(
        file_path, "res-on-premises-logs-dev", f"devices/Straightener/reports/{file}"
    )
    url = f"https://res-on-premises-logs-dev.s3.amazonaws.com/devices/Straightener/reports/{file}"
    return url


if __name__ == "__main__":
    # try:
    main()
    # except Exception as e:
    # logger.error(f"An unknown error has occurred: {e.with_traceback}")
