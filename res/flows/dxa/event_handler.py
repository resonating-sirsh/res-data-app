import res
import res.flows.meta.one_marker.apply_color_request as apply_color_request
import res.flows.meta.one_marker.body as body
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.connectors.graphql.hasura import ChangeDataTypes as hcd
from res.flows.dxa import node_run
from res.utils import logger
from res.flows.meta.one_marker.apply_color_request import flag_apply_color_request
import json
import asyncio
import requests

AIRTABLE_STYLES_QUERY = """
query ($id: ID!) {
  style(id: $id) {
    id
    body {
      code
    }
    artworkFile {
      file {
        s3 {
          key
          bucket
        }
      }
    }
  }
}"""


def process_brand_onboarding_event(event):
    style_id = event.get("requestor_id")
    c1_graph = ResGraphQLClient()
    result = c1_graph.query(AIRTABLE_STYLES_QUERY, {"id": style_id})
    style = result["data"]["style"]

    artwork_s3 = result["data"]["style"]["artworkFile"]["file"]["s3"]
    bucket = artwork_s3["bucket"]
    artwork_file_path = artwork_s3["key"]

    node = node_run.FlowNodes.BRAND_ONBOARDING.value

    details = {
        "style_id": style["id"],
        "body_code": style["body"]["code"],
        "remote_artwork_file_path": f"s3://{bucket}/{artwork_file_path}",
    }

    node_run.submit_synchronously(node, details)


def process_export_asset_bundle_event(event):
    from schemas.pydantic.apply_color_request import (
        AssetsGenerationMode,
    )

    logger.info(event)
    event_type = event.get("event_type")
    requestor_id = event.get("requestor_id")

    if event_type == hcd.CREATE.value:
        job_inputs = apply_color_request.get_export_asset_bundle_job_inputs(
            requestor_id
        )
        apply_color_flow_type = job_inputs.get("apply_color_flow_type")
        assets_generation_mode = job_inputs.get("assets_generation_mode")
        logger.info(f"apply_color_flow_type: {apply_color_flow_type}")
        logger.info(f"assets_generation_mode: {assets_generation_mode}")

        if assets_generation_mode == AssetsGenerationMode.CREATE_NEW_ASSETS.value:
            if (
                apply_color_flow_type
                == apply_color_request.APPLY_COLOR_FLOW_TYPE["VSTITCHER"]
            ):
                logger.info("Submit Job")
                job = node_run.submit_synchronously(
                    run_type=node_run.FlowNodes.EXPORT_STYLE_BUNDLE.value,
                    details=job_inputs.get("details"),
                    priority=job_inputs.get("priority"),
                )

                if job:
                    apply_color_request_payload_update = {
                        "exportAssetBundleJobId": job.get("id")
                    }
                    apply_color_request.update(
                        requestor_id, apply_color_request_payload_update
                    )
                logger.info(f"response: {job}")
            elif (
                apply_color_flow_type
                == apply_color_request.APPLY_COLOR_FLOW_TYPE[
                    "APPLY_DYNAMIC_COLOR_WORKFLOW"
                ]
            ):
                logger.info("Submit apply_dynamic_color_workflow ")
                acr_record_id = job_inputs.get("details").get(
                    "at_color_queue_record_id"
                )
                try:
                    argo = res.connectors.load("argo")
                    argo.handle_event(
                        {
                            "apiVersion": "v0",
                            "kind": "resFlow",
                            "metadata": {
                                "name": "dxa.apply_dynamic_color",
                                "version": "dev",
                            },
                            "args": {
                                "body_version": job_inputs.get("details").get(
                                    "body_version"
                                ),
                                "body_code": job_inputs.get("details").get("body_code"),
                                "sizes": ["all"],
                                "sizeless_sku": job_inputs.get("details").get(
                                    "style_code"
                                ),
                                "apply_color_request_record_id": acr_record_id,
                            },
                        }
                    )
                except Exception as e:
                    import traceback

                    e = traceback.format_exc()
                    logger.error(f"Error submitting apply_dynamic_color_workflow: {e}")
                    issues_input = [
                        {
                            "issueTypeCode": "SUBMIT_DYNAMIC_COLOR",
                            "context": f"Error submitting apply_dynamic_color_workflow: {e}",
                        }
                    ]
                    flag_apply_color_request(acr_record_id, issues_input)

                # logger.info(f"response: {response}")

    elif event_type == hcd.UPDATE.value:
        new_status = {
            node_run.Status.IN_PROGRESS.value: "In Progress",
            node_run.Status.COMPLETED_SUCCESS.value: "Done",
            node_run.Status.COMPLETED_ERRORS.value: "Failed",
        }.get(event.get("status"))

        if new_status:
            apply_color_request.update_apply_color_job_status(requestor_id, new_status)

        apply_color_request_sync_payload = (
            apply_color_request.get_fields_to_sync_to_export_asset_bundle_job(
                requestor_id
            )
        )
        job_id = apply_color_request_sync_payload.get("export_asset_bundle_job_id")
        fields_to_update = apply_color_request_sync_payload.get("fields_to_update")

        if job_id and fields_to_update:
            asyncio.run(node_run.update_by_id(job_id, fields_to_update))


def process_dxa_export_body_bundle_event(event):
    event_type = event.get("event_type")
    requestor_id = event.get("requestor_id")
    body_version = event.get("body_version")
    input_file_uri = event.get("input_file_uri")
    rename_input_file = event.get("rename_input_file")

    if event_type == hcd.CREATE.value:
        node_run.submit_synchronously(
            run_type=node_run.FlowNodes.EXPORT_BODY_BUNDLE.value,
            details=body.get_dxa_export_body_bundle_details(
                requestor_id, body_version, input_file_uri, rename_input_file
            ),
        )


_nodes_to_handlers = {
    node_run.FlowNodes.BRAND_ONBOARDING.value: process_brand_onboarding_event,
    node_run.FlowNodes.EXPORT_STYLE_BUNDLE.value: process_export_asset_bundle_event,
    node_run.FlowNodes.EXPORT_BODY_BUNDLE.value: process_dxa_export_body_bundle_event,
}


def handle(event):
    """Handle node run events"""
    node = event.get("job_type")
    dispatcher = _nodes_to_handlers.get(node)

    if dispatcher is not None:
        dispatcher(event)
    else:
        acceptable_nodes = [n.value for n in node_run.FlowNodes]
        raise Exception(
            f"No node found for job type: {node}. Should be one of {acceptable_nodes}"
        )
