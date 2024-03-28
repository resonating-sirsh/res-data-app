import json, os, time, re, schedule, arrow

from res.airtable.print import PRINTFILES, NESTS, ROLLS
from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaConsumer import ResKafkaConsumer
from res.connectors.s3.S3Connector import S3Connector
from res.utils import logger, secrets


NESTS_FIELDS = [
    "Asset Path",
    "Assets",
    "Argo Jobkey",
    "Nested Length in Yards",
    "Dataframe Path",
]
NESTS_SORT = ["Rank"]
WOVEN_UTILIZATION_THRESHOLD = (
    0.6  # threshold below which we will automatically flag the printfiles.
)
KNIT_UTILIZATION_THRESHOLD = 0.5  # knit utilization sucks generally
# we could go to the db for this but we also could just hardcode a list.
# we can add to it if we get more materials and they start alerting.
KNIT_MATERIALS = [
    "CFT97",
    "CFTGR",
    "CT170",
    "CTF19",
    "CTJ95",
    "CTNBA",
    "CTNPT",
    "CTNSP",
    "FBTRY",
    "OCTCJ",
    "PIQUE",
    "TNSPJ",
]


def ordered_assigned_nests(msg):
    nests = {
        n["id"]: n
        for n in NESTS.all(
            formula=f"""FIND(RECORD_ID(), '{",".join(msg["nest_record_ids"])}')""",
            fields=NESTS_FIELDS,
            sort=NESTS_SORT,
        )
    }

    if len(nests) != len(msg["nest_record_ids"]):
        logger.error(
            f"Found {len(msg['nest_record_ids'])} nests in message and {len(nests)} nests in airtable!"
        )
        return []

    for id in msg["nest_record_ids"]:
        yield nests[id]


def assigned_print_assets(ordered_nests):
    for n in ordered_nests:
        yield from n["fields"].get("Assets", [])


def create_roll_payload(msg, ordered_nests):
    return {
        "id": msg["roll_record_id"],
        "fields": {
            "Ready for Print": True,
            "Nested Asset Sets": msg["nest_record_ids"],
            "Print Priority": msg["utility"],
            "Print Assets (flow) 3": list(assigned_print_assets(ordered_nests)),
            "Optimus Assigned At": arrow.utcnow().isoformat(),
        },
    }


def create_nests_payload(msg):
    for i, id in enumerate(msg["nest_record_ids"]):
        yield {"id": id, "fields": {"Rank": i + 1}}


def assigned_nests_with_paths(ordered_nests):
    for i, n in enumerate(ordered_nests):
        yield n["id"], i + 1, n["fields"].get("Asset Path", [])


def explode_stitch_info(stitch_info):
    # see remarks in optimus_printer about why this thing is a string to begin with
    return json.loads(stitch_info)


def create_print_files_payload(msg, ordered_nests):
    print_file = {
        "task_key": msg["task_key"],
        "Rolls": [msg["roll_record_id"]],
        "Material": [msg["material_record_id"]],
        "Print Queue": "TO DO",
        "Roll Length Utilization": msg["length_utilization"],
    }

    if msg.get("check_total_utilization", False):
        roll_utilization = msg.get("total_utilization")
        knitted = msg.get("material_code") in KNIT_MATERIALS
        threshold = (
            KNIT_UTILIZATION_THRESHOLD if knitted else WOVEN_UTILIZATION_THRESHOLD
        )
        if roll_utilization is not None and roll_utilization < threshold:
            print_file["Flag For Review"] = "true"
            print_file[
                "Flag for review Reason"
            ] = f"Roll utilization below threshold: {roll_utilization} < {threshold}"

    if msg["stitch_nests"]:
        for i, stitch in enumerate(explode_stitch_info(msg["stitched_nest_info"])):
            yield {
                **print_file,
                "Nests": stitch["nest_ids"],
                "Rank": i + 1,
                "Asset Path": "",  # the path will be updated by stitch_printfiles.
                "Stitching Status": "TO DO",
            }
    else:
        for nest_record_id, rank, asset_path in assigned_nests_with_paths(
            ordered_nests
        ):
            if not asset_path:
                raise NotImplementedError("Unable to handle nests w/o assets!")

            yield {
                **print_file,
                "Nests": [nest_record_id],
                "Rank": rank,
                "Asset Path": asset_path,
                "Stitching Status": "NOT REQUIRED",
            }


def submit_stitching_job(
    roll_name,
    task_key,
    rank,
    print_file_record_id,
    nests,
    headers,
    roll_offset_px,
    header_offset_idx,
    nests_parts,
):
    job_name = f"stitch-{'' if not roll_name else re.sub('[^0-9a-z]', '-', roll_name.lower())}-{rank}-{int(time.time())}"
    logger.info(
        f"Submitting stitching job {job_name} for nests {nests}, with headers={headers}"
    )
    payload = {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {"name": "make.nest.stitch_printfiles", "version": "primary"},
        "assets": [
            {
                "nest_record_id": n["id"],
                "nest_job_key": n["fields"]["Argo Jobkey"],
                "nest_df_path": (
                    n["fields"]["Dataframe Path"]
                    if n["fields"].get("Dataframe Path", "") != ""
                    else f"s3://res-data-production/flows/v1/make-nest-pack_pieces/primary/nest/{n['fields']['Argo Jobkey']}"
                ),
                "nests_parts": nests_parts[i] if len(nests_parts) > i else 1,
            }
            for i, n in enumerate(nests)
        ],
        "args": {
            "print_file_record_id": print_file_record_id,
            "num_headers": headers,
            "roll_name": roll_name,
            "optimus_task_key": task_key,
            "roll_offset_px": roll_offset_px,
            "header_offset_idx": header_offset_idx,
        },
        "task": {"key": job_name},
    }
    logger.info(f"Submitting payload to argo: {payload}")
    ArgoWorkflowsConnector().handle_event(
        payload,
        unique_job_name=job_name,
    )
    return payload


def fix_stuck_printfiles():
    # SAD we have to do this but here we are...
    logger.info("Fixing stuck printfiles")
    printfiles_to_stitch = PRINTFILES.all(
        formula="AND({Print Queue}='TO DO', OR({Stitching Status}='ERROR', AND({Stitching Status}='TO DO', DATETIME_DIFF(NOW(), {Created At}, 's') > 14400)))",
        fields=["Stitching Job Payload"],
    )

    s3 = S3Connector()

    updated_records = []

    for printfile in printfiles_to_stitch:
        event = json.loads(printfile["fields"]["Stitching Job Payload"])
        key = event["task"]["key"]
        output_path = f"s3://res-data-production/flows/v1/make-nest-stitch_printfiles/primary/concat/{key}"
        if s3.exists(f"{output_path}/printfile_composite.png"):
            logger.info(f"Printfile already exists for key {key} - updating record")
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
            job_name = f"{key.split('-redo-')[0]}-redo-{int(time.time())}"
            event["args"]["clear_flags"] = True
            event["metadata"]["cpu_override"] = {"handler": "64000m"}
            event["metadata"]["disk_override"] = {"handler": "500G"}
            event["metadata"]["memory_override"] = {"handler": "256G"}
            event["task"]["key"] = job_name
            updated_records.append(
                {
                    "id": printfile["id"],
                    "fields": {
                        "Stitching Job Payload": json.dumps(event),
                    },
                }
            )
            logger.info(f"Restarting argo job {job_name} for key {key}")
            ArgoWorkflowsConnector().handle_event(event, unique_job_name=job_name)

    PRINTFILES.batch_update(
        updated_records,
        typecast=True,
    )


def handle_message(msg):
    # This if statement can be removed once the new optimus is productionized
    if msg["task_key"].startswith("optimus-nest-roll-assignment"):
        logger.debug("message from non-production process")
        return

    # Create the Airtable data
    ordered_nests = list(ordered_assigned_nests(msg))
    if not ordered_nests:
        return

    print_files = list(create_print_files_payload(msg, ordered_nests))
    nests = list(create_nests_payload(msg))
    roll = create_roll_payload(msg, ordered_nests)

    if not print_files:
        logger.warn(f"No print files to insert! {print_files!r}")
        return
    if not nests:
        logger.warn(f"No nests to assign! {nests!r}")
    logger.info(f"Assigning {len(nests)} nests to roll")
    logger.debug(roll)
    logger.debug(nests)

    logger.info(f"Creating {len(print_files)} Print Files records")
    logger.debug(print_files)

    print_file_response = []

    write_to_airtable = os.environ.get("WRITE_TO_AIRTABLE", "f").lower()[0] == "t"

    if write_to_airtable:

        print_file_response = PRINTFILES.batch_create(
            print_files,
            typecast=True,
        )

        ROLLS.update(
            msg["roll_record_id"],
            roll["fields"],
            typecast=True,
        )

        NESTS.batch_update(nests, typecast=True)

    else:
        logger.info("Skipped writing to Airtable")

    if msg["stitch_nests"]:
        nests_by_id = {n["id"]: n for n in ordered_nests}
        logger.info(f"Create printfile response {print_file_response}")
        printfile_update = []
        for rank, (airtable_record, stitch_info) in enumerate(
            zip(print_file_response, explode_stitch_info(msg["stitched_nest_info"]))
        ):
            submitted_payload = submit_stitching_job(
                msg["roll_name"],
                msg["task_key"],
                rank + 1,
                airtable_record["id"],
                [nests_by_id[id] for id in stitch_info["nest_ids"]],
                stitch_info["headers"],
                stitch_info.get("roll_offset_px", 0),
                stitch_info.get("header_offset_idx", 0),
                stitch_info.get("nests_parts", []),
            )
            printfile_update.append(
                {
                    "id": airtable_record["id"],
                    "fields": {
                        "Stitching Job Payload": submitted_payload,
                        "Number of Headers": stitch_info["headers"],
                    },
                }
            )
        if write_to_airtable:
            PRINTFILES.batch_update(
                printfile_update,
                typecast=True,
            )


def run_kafka():

    # Initialize kafka client and consumer
    kafka_client = ResKafkaClient()
    kafka_consumer = ResKafkaConsumer(kafka_client, os.environ.get("KAFKA_TOPIC"))

    if os.environ["RES_ENV"] == "production":
        # schedule job to unstick printfiles.
        schedule.every(240).minutes.do(fix_stuck_printfiles)

    while True:
        try:
            msg = kafka_consumer.poll_avro()
            if msg is not None:
                logger.debug("New record from kafka: {}".format(msg))
                handle_message(msg)

            # invoke the printfile unstucker if we need to.
            schedule.run_pending()
        except Exception as e:
            logger.error(str(e))
            raise e


if __name__ == "__main__":
    run_kafka()
