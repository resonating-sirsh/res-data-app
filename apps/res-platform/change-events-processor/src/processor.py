"""Process change events from Hasura for dxa.flow_node_run and create.assets.."""

# _nudge... ... ... ...

from jsonschema import validate
from pathlib import Path
import json
import os
import requests
from res.utils.dates import parse
from res.connectors.kafka.ResKafkaClient import ResKafkaClient
from res.connectors.kafka.ResKafkaProducer import ResKafkaProducer
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils import logger, secrets_client
import res.flows.meta.one_marker.apply_color_request as apply_color_request
import res.flows.meta.one_marker.body as body
import res.flows.meta.body.unpack_asset_bundle.unpack_asset_bundle as unpack_asset_bundle
from res.flows.meta.one_marker.apply_color_request import flag_apply_color_request
from res.flows.meta.one_marker.apply_color_request import unflag_apply_color_request
from res.flows.meta.utils import parse_status_details_for_issues
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from schemas.pydantic.meta import BodyMetaOneResponse
from res.flows.api import FlowAPI
import res
from res.flows.meta.ONE.body_node import BodyMetaOneNode

hasura = res.connectors.load("hasura")

SCHEMA_DIR = Path(__file__).resolve().parent / ".." / "schema"

# APP_NAME = os.getenv("RES_APP_NAME")
APP_NAME = "hasura-change-data-processor"
APP_NAMESPACE = "res-platform"

hasura_event_schema = json.load(open(SCHEMA_DIR / "event_payload.json"))

AIRTABLE_INGESTION_TOPIC = "res-infrastructure.kafka_to_airtable.airtable_updates"
AIRTABLE_DXA_ONE_MARKERS_BASE = "appqtN4USHTmyC6Dv"
AIRTABLE_APPLY_COLOR_QUEUE_TABLE = "tblWMyfKohTHxDj3w"

AIRTABLE_STYLES_BASE = "appjmzNPXOuynj6xP"
AIRTABLE_STYLES_TABLE = "tblmszDBvO1MvJrlJ"

AIRTABLE_META_ONE_BASE = "appa7Sw0ML47cA8D1"
AIRTABLE_BODIES_TABLE = "tblXXuR9kBZvbRqoU"

kafka_client = ResKafkaClient()
airtable_kafka_producer = ResKafkaProducer(kafka_client, AIRTABLE_INGESTION_TOPIC)

create_one_gql = ResGraphQLClient()

HASURA_ENDPOINT = os.getenv(
    "HASURA_GRAPHQL_API_URL", "https://hasura-dev.resmagic.io/v1/graphql"
)

HASURA_HEADERS = {
    "content-type": "application/json",
    "x-hasura-admin-secret": secrets_client.get_secret("HASURA_API_SECRET_KEY"),
}

LATEST_ASSET_QUERY = """
query($where: create_asset_bool_exp) {
  create_asset(where: $where, order_by: {created_at: desc}, limit: 1){
    id
    path
  }
}
"""

JOB_COLOR_QUEUE_RECORD_ID_QUERY = """
query ($id: uuid!){
  dxa_flow_node_run_by_pk(id: $id){
    details
    type
  }
}
"""


STYLE_COVER_PHOTO_MUTATION = """
mutation($id: ID!, $url: String) {
  updateStyleCoverImage(id: $id, input: {coverImages: [{url: $url}]}){
    style {
      id
    }
  }
}
"""


ADD_BODY_ASSET_BUNDLE = """
mutation addBodyAssetBundle($id:ID!, $input: UpdateBodyAssetBundleInput!){
    addBodyAssetBundle(id:$id, input:$input){
        body {
            id
        }
    }
}
"""


def metric(asset, verb, status):
    """
    - match: "flows.*.*.*.*.*"
      name: "flows"
      labels:
        flow: "$1"
        node: "$2"
        data_group: "$3"
        verb: "$4"
        status: "$5"
    """

    metric_name = f"flows.dxa.bertha.{asset}.{verb}.{status}"
    res.utils.logger.incr(metric_name, 1)


def _get_latest_dxa_job_asset(job_id):
    where_clause = {
        "where": {
            "_and": [
                {"type": {"_eq": "DXA_ASSET_BUNDLE"}},
                {"job_id": {"_eq": job_id}},
            ]
        }
    }

    request_payload = {
        "query": LATEST_ASSET_QUERY,
        "variables": where_clause,
    }

    assets = (
        requests.post(HASURA_ENDPOINT, json=request_payload, headers=HASURA_HEADERS)
        .json()
        .get("data")
        .get("create_asset")
    )
    if not len(assets) == 1:
        raise Exception("Job should only have one asset")
    bundle_s3_path = assets[0]
    return bundle_s3_path


def update_airtable_via_kafka(base_id, table_id, column_name, record_id, new_value):
    """Update Airtable asynchronously via Kafka."""
    airtable_payload = {
        "submitting_app": APP_NAME,
        "submitting_app_namespace": APP_NAMESPACE,
        "airtable_base": base_id,
        "airtable_table": table_id,
        "airtable_column": column_name,
        "airtable_record": record_id,
        "new_value": new_value,
    }
    logger.info("Updating Airtable asynchronously", record=airtable_payload)
    airtable_kafka_producer.produce(airtable_payload)
    airtable_kafka_producer.flush()


def handle_create_asset_event(e):
    validate(instance=e, schema=hasura_event_schema)

    source_info = e["table"]
    source = source_info["schema"] + "." + source_info["name"]

    event = e["event"]
    old_data = event["data"].get("old")
    new_data = event["data"]["new"]
    op = event["op"]

    # We can make this declarative when we have a few more use cases
    res.utils.logger.info(f"*** CREATE ASSET: {new_data}")
    if source == "create.asset":
        if (
            op in ["CREATE", "UPDATE", "MANUAL"]
            and new_data.get("status") == "UPLOADED"
            and new_data.get("type")
            in ["MEDIA_RENDER", "DXA_ASSET_BUNDLE", "TURNTABLE", "SCAN"]
        ):
            result = hasura.execute_with_kwargs(
                JOB_COLOR_QUEUE_RECORD_ID_QUERY, id=new_data.get("job_id")
            )
            job = result.get("dxa_flow_node_run_by_pk", {})
            logger.info(f"job: {job}")
            job_details = job.get("details")

            if job.get("type") == "BRAND_ONBOARDING":
                if (
                    new_data.get("is_public_facing")
                    and new_data.get("name") == "front.png"
                ):
                    style_id = new_data.get("style_id")
                    new_asset_s3_uri = new_data.get("path")
                    if not style_id or not new_asset_s3_uri:
                        raise Exception(
                            f"Can't update the cover photo for asset {new_data.get('id')}. Require style_id and s3_uri: found {(style_id, new_asset_s3_uri)}"
                        )

                    # Need to translate s3 URI to URL. This is a little flaky, though `front.png` is pretty safe.
                    s3_http_parts = new_asset_s3_uri.replace("s3://", "").split("/")
                    s3_http_parts[0] = f"https://{s3_http_parts[0]}.s3.amazonaws.com"
                    s3_url = "/".join(s3_http_parts).replace(" ", "+")

                    resp = ResGraphQLClient().query(
                        STYLE_COVER_PHOTO_MUTATION,
                        variables={
                            "id": style_id,
                            "url": s3_url,
                        },
                    )
                    if resp.get("errors"):
                        metric(style_id, "BRAND_ONBOARDING", "FAILED")
                        raise Exception(
                            f"Error updating profile photo! {resp.get('errors')}"
                        )
                    else:
                        metric(style_id, "BRAND_ONBOARDING", "OK")
                        logger.info(f"updated profile photo to {s3_url}")
                        return {
                            "success": True,
                            "message": "Successfully updated profile photo",
                        }

            elif job.get("type") == "DXA_EXPORT_ASSET_BUNDLE":
                record_id = job_details.get("at_color_queue_record_id")
                if new_data.get("name") == "3d_model.zip":
                    if record_id:
                        apply_color_request.process_3d_model(
                            record_id, new_data["path"]
                        )
                    else:
                        body.process_3d_model(
                            job.get("details", {}).get("body_code"),
                            job.get("details", {}).get("body_version"),
                            new_data["path"],
                        )
                else:
                    if not record_id:
                        return {
                            "success": True,
                            "message": "No `at_color_queue_record_id` found on job details",
                        }

                    elif new_data.get("type") == "DXA_ASSET_BUNDLE":
                        res.utils.logger.info(f"    Updating AirTable for asset.zip")
                        apply_color_request.process_on_asset_bundle_created(
                            record_id,
                            new_data.get("path"),
                        )
                    elif new_data.get("type") == "TURNTABLE":
                        res.utils.logger.info(
                            f"    Updating AirTable for turntable.zip"
                        )
                        apply_color_request.process_on_turntable_created(
                            record_id,
                            new_data.get("path"),
                        )

                metric(
                    job_details.get("style_id", "ANY"),
                    "DXA_EXPORT_ASSET_BUNDLE",
                    "OK",
                )
                return {"success": True}
            elif job.get("type") == "DXA_EXPORT_BODY_BUNDLE":
                if not job_details.get("body_id"):
                    return {
                        "success": True,
                        "message": "No `body_id` found on job details",
                    }
                elif not job_details.get("body_version"):
                    return {
                        "success": True,
                        "message": "No `body_version` found on job details",
                    }

                if new_data.get("type") == "DXA_ASSET_BUNDLE":
                    updated_request = create_one_gql.query(
                        ADD_BODY_ASSET_BUNDLE,
                        {
                            "id": job_details.get("body_id"),
                            "input": {
                                "uri": new_data.get("path"),
                                "bodyVersion": json.dumps(
                                    job_details.get("body_version")
                                ),
                            },
                        },
                    )
                    if updated_request.get("errors"):
                        metric(
                            job_details.get("body_code", "ANY"),
                            "DXA_EXPORT_BODY_BUNDLE",
                            "FAILED",
                        )
                        raise Exception(
                            f"Error updating the request: {updated_request.get('errors')}"
                        )
                    else:
                        metric(
                            job_details.get("body_code", "ANY"),
                            "DXA_EXPORT_BODY_BUNDLE",
                            "OK",
                        )
                        logger.info(f"Request response: {updated_request} ")
                        return {
                            "success": True,
                            "message": "Asset Bundle Unpack Requested!",
                        }
                if new_data.get("type") == "SCAN":
                    if not job_details.get("body_code"):
                        return {
                            "success": True,
                            "message": "No `body_code` found on job details",
                        }
                    if not job_details.get("body_version"):
                        return {
                            "success": True,
                            "message": "No `body_version` found on job details",
                        }
                    unpack_asset_bundle.process_on_scan_created(
                        job_details.get("body_code"),
                        job_details.get("body_version"),
                        new_data.get("path"),
                    )

                return {"success": True}

    else:
        return {"success": False, "message": f"Unable to handle source: {source}"}


def has_truthy_value(input, field):
    return field in input and bool(input[field])


def log_report(data):
    issues = []
    try:
        type_map = {
            "DXA_EXPORT_BODY_BUNDLE": "body",
            "DXA_EXPORT_ASSET_BUNDLE": "style",
            "BRAND_ONBOARDING": "brand",
        }

        details = data["details"]
        status_details = data.get("status_details", "")

        status = data["status"]

        job_type = type_map.get(data["type"]) or data["type"]
        node = f"bertha_{job_type}"

        body_code = details.get("body_code")
        body_version = details.get("body_version")
        style_id = details.get("style_id") or details.get("body_id")

        asset_key = f"{body_code} v{body_version}" if job_type == "body" else style_id
        process = data["id"]

        logger.metric_node_state_transition_incr(
            node, asset_key, status, process=process
        )

        if status == "COMPLETED_ERRORS":
            issues = parse_status_details_for_issues(status_details)
            logger.warning("Issues found...")
            logger.warning(issues)

            failed_contracts = [issue["issue_type_code"] for issue in issues]
            for contract in failed_contracts:
                logger.metric_contract_failure_incr(
                    node, asset_key, contract, process=process
                )
    except:
        import traceback

        logger.warn(f"Couldn't log stats {traceback.format_exc()}")

    return issues


def handle_status_update(e):
    validate(instance=e, schema=hasura_event_schema)

    source_info = e["table"]
    source = source_info["schema"] + "." + source_info["name"]

    event = e["event"]
    old_data = event["data"].get("old")
    new_data = event["data"]["new"]
    op = event["op"]

    issues = log_report(new_data)

    res.utils.logger.info(f"*** STATUS UPDATE {new_data}")
    if source == "dxa.flow_node_run":
        if op in ["UPDATE", "MANUAL", "INSERT"]:
            if new_data.get("type") == "DXA_EXPORT_ASSET_BUNDLE":
                preset_key = new_data.get("preset_key")
                if preset_key == "3d_model_v0":
                    logger.info(
                        f"3d_model_v0 preset_key found, skipping updating any airtable etc..."
                    )
                    return {"success": True, "message": "No actions needed."}

                details = new_data.get("details")
                apply_color_request_id = details.get("at_color_queue_record_id")

                if (
                    new_data.get("status") in ["COMPLETED_SUCCESS", "COMPLETED_ERRORS"]
                    and has_truthy_value(new_data, "started_at")
                    and has_truthy_value(new_data, "ended_at")
                ):
                    # job has completed, be sure to record the runtime
                    started_at = parse(new_data.get("started_at"))
                    ended_at = parse(new_data.get("ended_at"))
                    runtime = (ended_at - started_at).total_seconds() * 1000
                    status = new_data.get("status")

                    logger.timing("dxa.generate_style_bundle.completed.ms", runtime)
                    logger.timing(
                        "dxa.generate_style_bundle.%s.ms"
                        % (status.lower().replace("_", ".")),
                        runtime,
                    )

                apply_color_request.update(
                    apply_color_request_id,
                    {
                        "exportAssetBundleJobStartedAt": new_data.get(
                            "started_at", None
                        ),
                        "exportAssetBundleJobEndedAt": new_data.get("ended_at", None),
                    },
                )
                if new_data.get("status") == "COMPLETED_ERRORS":
                    updated_request = flag_apply_color_request(
                        apply_color_request_id,
                        [
                            {
                                "context": issue["context"],
                                "issueTypeCode": issue["issue_type_code"],
                            }
                            for issue in issues
                        ],
                    )

                    if updated_request.get("errors"):
                        metric(new_data.get("type"), "UPDATE", "FAILED")
                        raise Exception(
                            f"Error updating the request: {updated_request.get('errors')}"
                        )
                    else:
                        logger.info(f"Request response: {updated_request}")
                        metric(new_data.get("type"), "UPDATE", "FLAGGED")
                        return {
                            "success": True,
                            "message": "Request flagged for review.",
                        }
                else:
                    if apply_color_request_id:
                        unflag_apply_color_request(apply_color_request_id)
                    metric(new_data.get("type"), "UPDATE", "OK")
                    return {"success": True, "message": "No actions needed."}
            elif new_data.get("type") == "DXA_EXPORT_BODY_BUNDLE":
                if (
                    new_data.get("status") in ["COMPLETED_SUCCESS", "COMPLETED_ERRORS"]
                    and has_truthy_value(new_data, "started_at")
                    and has_truthy_value(new_data, "ended_at")
                ):
                    details = new_data.get("details")
                    body_code = details.get("body_code")
                    body_version = details.get("body_version")

                    # need to get the sample_size from GRAPHAPI
                    gql = ResGraphQLClient()
                    sample_size = gql.query_with_kwargs(
                        """
                        query body($code: String){
                            body(number:$code){
                            basePatternSize {
                                code
                            }
                        }
                    }
                    """,
                        code=body_code,
                    )["data"]["body"]["basePatternSize"]["code"]

                    contracts_failed = [issue["issue_type_code"] for issue in issues]
                    contract_failure_context = {
                        issue["issue_type_code"]: issue["context"] for issue in issues
                    }

                    if (
                        new_data.get("status") == "COMPLETED_ERRORS"
                        and not contracts_failed
                    ):
                        contracts_failed = ["UNKNOWN"]

                    if contracts_failed:
                        logger.info(
                            f"Contracts failed: {contracts_failed}, sending response"
                        )

                        """
                        instead of directly writing to the table request the refresh with the contracts
                        """
                        BodyMetaOneNode.refresh(
                            body_code,
                            body_version,
                            # sample_size_only=True, #SA we can probably do this if we are just registering contracts
                            contract_failures=contracts_failed,
                            contract_failure_context=contract_failure_context,
                        )

                        logger.info(f"AirTable update sent...")

                        metric(new_data.get("type"), "UPDATE", "FLAGGED")
                        return {
                            "success": True,
                            "message": "Request flagged for review.",
                        }
                    else:
                        metric(new_data.get("type"), "UPDATE", "OK")
                        return {"success": True, "message": "No actions needed."}

            else:
                metric(new_data.get("type"), "UPDATE", "OK")
                return {"success": True, "message": "No actions needed."}
        else:
            metric(new_data.get("type"), "UPDATE", "FAILED")
            return {"success": False, "message": f"Unable to handle operation {op}."}
    else:
        metric(new_data.get("type"), "UPDATE", "FAILED")
        return {"success": False, "message": f"Unable to handle source: {source}."}
