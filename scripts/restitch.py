#!/usr/bin/env python3 -m
import res
import json
import time
from res.utils import secrets
from pyairtable import Table

printfiles = Table(
    secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
    "apprcULXTWu33KFsh",
    "tblAQcPuKUDVfU7Fx",
)
printfiles_to_stitch = printfiles.all(
    formula="AND({Print Queue}='TO DO', OR({Stitching Status}='ERROR', AND({Stitching Status}='TO DO', DATETIME_DIFF(NOW(), {Created At}, 's') > 7200)))",
    fields=["Stitching Status", "Stitching Job Payload"],
)

s3 = res.connectors.load("s3")

updated_records = []

for printfile in printfiles_to_stitch:
    event = json.loads(printfile["fields"]["Stitching Job Payload"])
    key = event["task"]["key"]
    output_path = f"s3://res-data-production/flows/v1/make-nest-stitch_printfiles/primary/concat/{key}"
    print(f"For stitching job {key}")
    if s3.exists(f"{output_path}/printfile_composite.png"):
        print("Printfile already exists - updating record")
        updated_records.append(
            {
                "id": printfile["id"],
                "fields": {
                    "Stitching Status": "DONE",
                    "Flag For Review": "false",
                    "Flag for review Reason": "",
                    "Asset Path": output_path,
                },
            }
        )
    else:
        print("Restarting argo job")
        event["args"]["clear_flags"] = True
        event["metadata"]["cpu_override"] = {"handler": "64000m"}
        event["metadata"]["disk_override"] = {"handler": "500G"}
        event["metadata"]["memory_override"] = {"handler": "256G"}
        res.connectors.load("argo").handle_event(
            event, unique_job_name=f"{key}-redo-{int(time.time())}"
        )

printfiles.batch_update(
    updated_records,
    typecast=True,
)
