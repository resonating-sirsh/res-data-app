# Running local like this!
import sys

# sys.path.append(
#     "/Users/cesarvargas/Documents/Resonance/Data Platform/res-data-platform"
# )

import os
from res import connectors
import time
import boto3
import json
import requests
import traceback
from res.utils import logger, secrets_client
from res.connectors.airtable.AirtableClient import ResAirtableClient


# For a given base, loop thru tables get all records that need to be archive and save the attachment to S3

RES_ENV = os.getenv("RES_ENV", "development")
BUCKET = f"res-data-{RES_ENV}"
RESMETA_BASE_ID = "appc7VJXjoGsSOjmw"
APPLICATION_TABLES_TABLE_ID = "tbl3aVsMc27eT6Q4p"


if __name__ == "__main__":
    # Local testing!
    # event = {
    #     "base": "appH5S4hIuz99tAjm",
    #     "tables": ["tblptyuWfGEUJWwKk"],
    #     "dates": ["2022_05_03__23_10_53"],
    # }

    event = json.loads(sys.argv[1])

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
    if RES_ENV == "development":
        boto_client = boto3.client(
            "s3",
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            aws_access_key_id=os.environ.get("AWS_KEY"),
            aws_secret_access_key=os.environ.get("AWS_SECRET"),
        )
    else:
        boto_client = boto3.client("s3")

    records_to_archive_per_table = {}

    try:
        # Get base schema
        logger.info("Pulling base schema...")
        base_schema = airtable_client.get_base_schema(event["base"])
        logger.info("Done pulling schema!")

        # Get base's tables schema
        logger.info(f"Pulling base {event['base']} tables!")
        tables = base_schema.get("tables")
        logger.info("Done pulling tables!")

        logger.info("Finding if the table has __should_archive fields")
        # Iterate each table and find if it has a field named __should_archive
        for table in tables:
            records_to_archive_per_table[table["name"]] = {
                "id": table["id"],
                "should_archive": False,
                "fields_with_attachments": [],
                "records_to_archive": [],
            }

            for field in table["fields"]:
                # jesus fucking christ.
                if (
                    "options" in field
                    and "result" in field["options"]
                    and field["options"]["result"] is not None
                    and "name" in field["options"]["result"]
                    and field["options"]["result"]["name"] == "multipleAttachments"
                ):
                    records_to_archive_per_table[table["name"]][
                        "fields_with_attachments"
                    ].append(field["name"])

                # Check if it __should_archive field exits
                if field.get("name", "") == "__should_archive":
                    records_to_archive_per_table[table["name"]]["should_archive"] = True

        logger.info("Done creating basic structure!")

        logger.info("Querying all the records that should be archived per table")

        # Now that we have all the tables, let's query all the records that should be archived
        for table_name, table_data in records_to_archive_per_table.items():
            # Check if table should be archived
            if table_data.get("should_archive"):
                logger.info(
                    f"Table {table_data['id']} can be archived, querying it fields..."
                )
                # Initialize Mongo Configuration as false
                mongodb_collection_name = False

                # Get table Mongo Configuration
                filter_by_formula = f"AND({{ID}}='{table_data['id']}',{{Preserve Archived Records in MongoDB}}=TRUE())"
                table_meta_configurations = airtable_client.get_records(
                    RESMETA_BASE_ID,
                    APPLICATION_TABLES_TABLE_ID,
                    filter_by_formula=filter_by_formula,
                )

                # Since this is a generator, obtain the first result

                try:
                    table_meta_configuration = next(table_meta_configurations)
                    logger.info(
                        f"Table {table_data['id']} has a MongoDB Collection configured.. So we need to update MongoDB records"
                    )
                except Exception as e:
                    logger.info(
                        f"Table {table_data['id']} doesn't has a MongoDB Collection configured.."
                    )
                    table_meta_configuration = False

                if table_meta_configuration:
                    # Check if it has a MongoDB Collection Name, if not, mark as False.
                    mongodb_collection_name = table_meta_configuration["fields"].get(
                        "MongoDB Collection Name", False
                    )

                logger.info("Now, query all the fields that should be archive.")
                # Get Records that should be archived.
                records_to_archive = airtable_client.get_records(
                    event["base"],
                    table_data["id"],
                    filter_by_formula="AND({__should_archive}=1)",
                )

                # Get all the attachment of the record, and then upload it to the folder.
                for record in records_to_archive:
                    # Iterate throught the fields that has attachments
                    for field_name in records_to_archive_per_table[table_name][
                        "fields_with_attachments"
                    ]:
                        # Check if it has attachments. If not, continue.
                        if record["fields"].get(field_name):
                            record_attachments = []
                            # Get all the attachments urls
                            for attachment in record["fields"].get(field_name):
                                new_attachment = {
                                    "name": f'{attachment["id"]}-{attachment["filename"].replace(" ", "-")}',
                                    "url": attachment["url"],
                                }
                                # Store all attachments in the list
                                record_attachments.append(new_attachment)

                            logger.info(
                                f"Uploading {len(record_attachments)} attachment(s) in field named '{field_name}' in table '{table_data['id']}' "
                            )
                            # Upload files
                            for attachment in record_attachments:
                                # Path of the attachments...
                                key_prefix = f'airtable_archives/{event["dates"][0]}/{event["base"]}/tables/{table_data["id"]}/attachments/{record["id"]}/{field_name.replace(" ", "-")}'

                                # Dowloading the attachment locally to later upload it
                                attachment_file = requests.get(
                                    attachment["url"], stream=True
                                ).raw
                                req_data = attachment_file.read()
                                try:
                                    # Do the actual upload to s3
                                    boto_client.put_object(
                                        Body=req_data,
                                        Bucket=BUCKET,
                                        Key=f'{key_prefix}/{attachment["name"]}',
                                    )
                                    logger.info(f"Attachment Uploaded...")
                                except Exception as e:
                                    event = (
                                        event
                                        if "event" in locals()
                                        else '["bad event payload"]'
                                    )
                                    logger.error(
                                        "Error uploading the data of field data down to S3!{} {}".format(
                                            json.dumps(event), str(e)
                                        )
                                    )
                                    continue
                            logger.info(
                                f"{len(record_attachments)} correctly upload it!"
                            )

                    # Store the record.
                    records_to_archive_per_table[table_name][
                        "records_to_archive"
                    ].append(record["id"])

                    logger.info(f"Done with {record['id']}")

            # Delete the record if in production.
        if RES_ENV == "production":
            # Iterate per each table / record
            for table_name, table_data in records_to_archive_per_table.items():
                # Check if the table can be archived and confirm if it has records to be archived
                if (
                    table_data.get("should_archive")
                    and len(table_data["records_to_archive"]) > 0
                ):
                    logger.info(
                        f"Archiving from {table_data['id']} in base {event['base']}"
                    )
                    for record_id in table_data["records_to_archive"]:
                        logger.info(f"Deleting the record {record_id}!")
                        airtable_client.delete_record(
                            event["base"],
                            table_data["id"],
                            record_id,
                            True,
                            "alerts-data-quality",
                        )
                        logger.info("Record deleted")
                        # Make a pause / take a break, you have worked hard uploading the attachments
                        time.sleep(0.25)

                        # """
                        # Mark records that need to be preserved in MongoDB with isArchived = true
                        # This will ensure these records remain in MongoDB and maintain their integrity after archiving
                        # """
                        # Check if table has a MongoCollection
                        if mongodb_collection_name:
                            logger.info(
                                f"Updating MongoDB model '{mongodb_collection_name}'"
                            )

                            mongo = connectors.load("mongo")
                            mongodb_collection = mongo["resmagic"][
                                mongodb_collection_name
                            ]

                            print("Marking records as archived in MongoDB...")

                            # mongodb_client = pymongo.MongoClient(MONGODB_CONNECTION_URL)
                            # mongodb_collection = mongodb_client["resmagic"]

                            # record_ids_to_archive = table_data["records_to_archive"]
                            mongodb_collection.update_many(
                                {"_id": {"$in": [record_id]}},
                                {"$set": {"isArchived": True}},
                            )

    except Exception as e:
        event = event if "event" in locals() else '["bad event payload"]'
        logger.error(
            "Error uploading the attachments from Airtable data down to S3!{} {}".format(
                json.dumps(event), traceback.format_exc(e)
            )
        )

    logger.info("Done!")
