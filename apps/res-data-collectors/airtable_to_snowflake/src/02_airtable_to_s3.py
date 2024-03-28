import os, sys, boto3, json, datetime
from res.utils import logger, secrets_client
from res.connectors.airtable.AirtableClient import ResAirtableClient

# For a given base, loop thru tables get all records and save to S3 in json format

RES_ENV = os.getenv("RES_ENV", "development")
BUCKET = f"res-data-{RES_ENV}"

if __name__ == "__main__":

    # event = {"base": "appH5S4hIuz99tAjm", "tables": ["tblD4apQytD8mq5wz"], "dates": ["2021_10_19__14_56_40"]}
    event = json.loads(sys.argv[1])
    base_id = event["base"]

    # Set up airtable client. Replace default personal access token with data
    # team specific one. This helps to prevent rate limiting
    DATA_TEAM_AIRTABLE_READER_PAT = secrets_client.get_secret(
        "DATA_TEAM_AIRTABLE_READER_PAT"
    )
    airtable_client = ResAirtableClient()
    airtable_client.api_key = DATA_TEAM_AIRTABLE_READER_PAT
    airtable_client.bearer_header = {
        "Authorization": f"Bearer {DATA_TEAM_AIRTABLE_READER_PAT}"
    }
    boto_client = boto3.client("s3")

    # Get list of bases. This is needed because the base metadata API
    # endpoint no longer is usable and this is the only place base name
    # currently can be retrieved
    all_bases = airtable_client.get_bases()
    bases_info = {
        f"{base['id']}": {
            "id": f"{base['id']}",
            "name": f"{base['name']}",
            "permissionLevel": f"{base['permissionLevel']}",
        }
        for base in all_bases
    }

    # Create a dict to store base schemas and prevent constant querying of the
    # endpoint for tables from the same base
    base_schemas_dict = {f"{base}": None for base in all_bases}

    for table in event["tables"]:
        try:
            # Write full table data
            logger.info(f"Pulling airtable records for table {table}...")
            data = airtable_client.get_records(
                base_id=base_id,
                table_id=table,
                send_errors_to_slack=True,
                slack_channel="alerts-data-quality",
            )
            boto_client.put_object(
                Body=json.dumps(list(data)),
                Bucket=BUCKET,
                Key=f'airtable_archives/{event["dates"][0]}/{base_id}/tables/{table}/records.json',
            )

            # Write base schema and metadata
            base_info = bases_info[base_id]

            # Check to see if base schema has already been pulled
            if base_schemas_dict.get(base_id) is not None:

                base_schema = base_schemas_dict[base_id]

            else:

                logger.info("Pulling base schema...")
                base_schema = airtable_client.get_base_schema(base_id)
                base_schemas_dict[base_id] = base_schema

            base_data = {**base_info, **base_schema}
            boto_client.put_object(
                Body=json.dumps(base_data),
                Bucket=BUCKET,
                Key=f'airtable_archives/{event["dates"][0]}/{base_id}/base.json',
            )
        except Exception as e:
            event = event if "event" in locals() else '["bad event payload"]'
            logger.error(
                "Error pulling Airtable data down to S3!{} {}".format(
                    json.dumps({**event, **{"tables": [table]}}), str(e)
                )
            )

            # Continue so other tables still run
            continue

    else:
        logger.info("Done!")
