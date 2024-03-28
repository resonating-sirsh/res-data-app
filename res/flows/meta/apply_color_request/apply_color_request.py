from res.utils.logging import logger
from res.flows.meta.one_marker.apply_color_request import (
    APPLY_COLOR_ISSUE_TYPE_CODE,
)  # TODO: migrate to this folder as enum
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import json
import res.flows.meta.apply_color_request.graphql_queries as graphql_queries
from res.flows.meta.one_marker.apply_color_request import flag_apply_color_request
from res.flows.meta.one_marker.apply_color_request import unflag_apply_color_request
from schemas.pydantic.apply_color_request import (
    CreateApplyColorRequestInput,
    ReproduceApplyColorRequestInput,
)
from datetime import datetime
import os
import requests


def get_apply_color_requests(variables):
    gql = ResGraphQLClient()

    response = gql.query(
        graphql_queries.GET_APPLY_COLOR_REQUESTS,
        variables=variables,
    )

    errors = response.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return response["data"]["applyColorRequests"]


def create_apply_color_request(input: CreateApplyColorRequestInput):
    gql = ResGraphQLClient()

    response = gql.query(
        graphql_queries.CREATE_APPLY_COLOR_REQUEST,
        variables={
            "input": {
                "styleId": input.style_id,
                "style_version": input.style_version,
                "assigneeEmail": input.assignee_email,
                "hasOpenOnes": input.has_open_ones,
                "requestsIds": input.requests_ids,
                "requestType": input.request_type,
                "originalRequestPlacedAt": input.original_request_placed_at,
                "priority": input.priority,
                "requesterEmail": input.requester_email,
                "designDirection": input.design_direction,
                "requestReasonsContractVariablesIds": input.request_reasons_contract_variables_ids,
                "targetCompleteDate": input.target_complete_date,
            }
        },
    )

    logger.info(f"create_apply_color_request_response={json.dumps(response,indent=2)}")

    errors = response.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return response["data"]["createApplyColorRequest"]


def reproduce_apply_color_request(id: str, input: ReproduceApplyColorRequestInput):
    gql = ResGraphQLClient()

    logger.info(f"reproduce_apply_color_request id={id} input={input}")
    response = gql.query(
        graphql_queries.REPRODUCE_APPLY_COLOR_REQUEST,
        variables={
            "id": id,
            "input": {
                "requestReasonsContractVariablesIds": input.request_reasons_contract_variables_ids,
                "reproduceReasons": input.reproduce_reasons,
                "reproduceDetails": input.reproduce_details,
                "cancellerEmail": input.canceller_email,
            },
        },
    )

    logger.info(f"reproduce_apply_color_request={json.dumps(response,indent=2)}")

    errors = response.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return response["data"]["reproduceApplyColorRequest"]


def update_apply_color_request(id, input):
    gql = ResGraphQLClient()
    response = gql.query(
        graphql_queries.UPDATE_APPLY_COLOR_REQUEST,
        variables={
            "id": id,
            "input": input,
        },
    )

    errors = response.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return response["data"]["updateApplyColorRequest"]


# def flag_apply_color_request():
#     """"
#     ***NOT YET IMPLEMENTED***
#     Flag Apply Color Request
#     """
#     # TODO: TO BE IMPLEMENTED (currently lives in res/flows/meta/one_marker/apply_color_request - need to merge anything relevant here to make a single file)
#     raise NotImplementedError()

# def unflag_apply_color_request():
#     """
#     ***NOT YET IMPLEMENTED***
#     Unflag Apply Color Request
#     """
#     # TODO: TO BE IMPLEMENTED (currently lives in res/flows/meta/one_marker/apply_color_request - need to merge anything relevant here to make a single file)
#     raise NotImplementedError()


class TryLater(Exception):
    pass


class FinishEarly(Exception):
    pass


def validate_apply_color_request_inputs(apply_color_request_id):
    from schemas.pydantic.apply_color_request import RequestType
    from operator import itemgetter

    os.environ[
        "RES_APP_NAME"
    ] = "meta-one/apply-color-request/validate-apply-color-request-inputs"
    """
    TODO:
    - validate if the request version is outdated/needs reproduce and kick that off automatically
    - refactor things (enums etc)
    """

    logger.info(
        f"validate_apply_color_request_inputs apply_color_request_id={apply_color_request_id}"
    )

    gql = ResGraphQLClient()

    contracts_failed = []
    update_request_payload = {}
    should_retry_later = False

    try:
        acr = gql.query(
            graphql_queries.GET_APPLY_COLOR_REQUEST,
            variables={"id": apply_color_request_id},
        )["data"]["applyColorRequest"]

        style = acr["style"]
        body = acr["style"]["body"]
        current_body_version = acr["style"]["body"]["patternVersionNumber"]
        request_body_version = acr["bodyVersion"]

        if not body["code"]:
            contracts_failed.append(
                {
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                    "context": f"Body Code not found",
                }
            )

        if not current_body_version:
            contracts_failed.append(
                {
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                    "context": f"Body Version not found",
                }
            )
        validate_body_as_first_one = acr["requestType"] in [
            RequestType.SAMPLE.value,  # sample is legacy name for first ones
            RequestType.META_ONE_MODIFICATION.value,  # sometimes modification requests are based in first ONEs
        ]

        # 1. validate body status
        is_body_valid, body_contracts_failed = validate_body_status(
            body["id"],
            body["code"],
            request_body_version,
            current_body_version,
            validate_body_as_first_one,
        )
        contracts_failed.extend(body_contracts_failed)
        if not is_body_valid:
            raise FinishEarly(
                f"Body is not yet ready for the requested version v{request_body_version}. contracts_failed={contracts_failed}"
            )

        # 2. validate if we can address request automatically or manually
        request_reasons_contract_variables_codes = [
            contract["code"]
            for contract in acr["requestReasonsContractVariables"]
            if acr["requestReasonsContractVariables"]
        ]

        (
            color_application_mode,
            body_changes_validated_at,
            are_body_changes_insignificant,
            assets_generation_mode,
            validate_body_changes_status,
            should_retry_later,
        ) = compute_color_application_mode(
            style["id"],
            style["code"],
            request_body_version,
            acr["applyColorFlowType"],
            acr["colorType"],
            request_reasons_contract_variables_codes,
            style["isStyle3dOnboarded"],
        )

        # DISABLE_COPY_ASSETS_MODE = False
        # if DISABLE_COPY_ASSETS_MODE and assets_generation_mode == "Copy Assets from Previous Version":
        #     color_application_mode = "Manually"
        #     should_retry_later = False
        #     assets_generation_mode = "Create New Assets"

        update_request_payload = {
            "colorApplicationMode": color_application_mode,
            "bodyChangesValidatedAt": body_changes_validated_at,
            "areBodyChangesInsignificant": are_body_changes_insignificant,
            "assetsGenerationMode": assets_generation_mode,
            "validateBodyChangesStatus": validate_body_changes_status,
        }

        if should_retry_later:
            raise TryLater(
                "Try Later. we'll try to validate is the body has not change to see if we can re-use the style file."
            )

        if not color_application_mode:
            # should not happen but doublechecking
            raise Exception("An error occurred. color application mode missing.")

        _, contracts_failed = itemgetter("assigneeEmail", "contractsFailed")(
            assign_assignee(apply_color_request_id, color_application_mode)
        )
        contracts_failed.extend(contracts_failed)

    except FinishEarly as e:
        logger.info(e)
    except TryLater as e:
        logger.info(e)
    except Exception as e:
        import traceback

        logger.error(f"error: {e} {traceback.format_exc()}")
        contracts_failed.append(
            {
                "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                "context": f"An error occurred: {e} \n {traceback.format_exc()}",
            }
        )
    logger.info(f"Contracts Failed: {contracts_failed}")
    if len(contracts_failed) > 0:
        flag_apply_color_request(apply_color_request_id, contracts_failed)
    else:
        unflag_apply_color_request(apply_color_request_id)

    update_request_payload.update(
        {
            "validateApplyColorRequestInputsStatus": (
                "Retry"
                if should_retry_later
                else ("Done" if len(contracts_failed) == 0 else "Failed")
            ),
            "validateApplyColorRequestInputsPostedAt": datetime.utcnow().strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }
    )

    logger.info(update_request_payload)

    apply_color_request = gql.query(
        graphql_queries.UPDATE_APPLY_COLOR_REQUEST,
        variables={
            "id": apply_color_request_id,
            "input": update_request_payload,
        },
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    return {
        "isReady": len(contracts_failed) == 0,
        "contractsFailed": contracts_failed,
        "applyColorRequest": apply_color_request,
    }


def validate_body_status(
    body_id,
    body_code,
    requested_body_version,
    current_body_version,
    validate_as_first_one=False,
):
    import res.flows.meta.body_one_ready_request.body_one_ready_request as BodyOneReadyRequest
    from schemas.pydantic.body_one_ready_request import (
        BodyOneReadyRequestType,
        RequestorLayer,
    )
    import res.flows.meta.body_one_ready_request.graphql_queries as body_one_ready_request_graphql_queries

    # NOTE: maybe we want to just receive (args) body_code & version and query the rest

    gql = ResGraphQLClient()
    contracts_failed = []

    current_version_open_borr = BodyOneReadyRequest.get_body_one_ready_requests(
        variables={
            "first": 1,
            "where": {
                "bodyNumber": {"is": body_code},
                "isOpen": {"is": True},
                "bodyVersion": {"is": current_body_version},
            },
            "sort": [{"field": "CREATED_AT", "direction": "DESCENDING"}],
        },
    )["bodyOneReadyRequests"]

    if not validate_as_first_one and len(current_version_open_borr) > 0:
        contracts_failed.append(
            {
                "context": f"Found in progress body one ready request for current version {current_body_version}",
                "issueTypeCode": "FOUND_IN_PROGRESS_BODY_ONE_READY_REQUEST",
            }
        )

    GET_BODY_GATES_STATUS = """
        query body($entityId:String!){
            body(entityId:$entityId){
                id
                isOneReady
                isReadyForSample
            }
        }
    """
    body = gql.query(GET_BODY_GATES_STATUS, variables={"entityId": body_id})["data"][
        "body"
    ]

    if not body["isOneReady"]:
        if validate_as_first_one:
            if not body["isReadyForSample"]:
                contracts_failed.append(
                    {
                        "context": "Body is not Ready for Sample",
                        "issueTypeCode": "BODY_NOT_READY_FOR_FIRST_ONE",
                    }
                )  # NOTE: maybe we might want to require this check of all request types at some point, but since we're reframing the validity/integrity of this gate now, we'll enable it for samples/1st ONEs only for now so we don't block production orders
        else:
            contracts_failed.append(
                {
                    "context": "Body is not ONE Ready",
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["BODY_NOT_ONE_READY"],
                }
            )

    body_meta_one_response_status = None
    body_status_response = requests.get(
        f"https://data.resmagic.io/meta-one/bodies/status",
        params={
            "body_code": body_code,
            "body_version": current_body_version,
        },
        headers={"content-type": "application/json"},
    )

    logger.info(f"body_status_response:{body_status_response}")

    body_status_response.raise_for_status()
    body_meta_one_response_status = body_status_response.json()
    logger.info(f"body_meta_one_response_status: {body_meta_one_response_status}")

    if body_meta_one_response_status:
        body_meta_one_contracts_status = body_meta_one_response_status.get(
            "status", "ERROR"
        )

        body_meta_one_contracts_failed = body_meta_one_response_status.get(
            "contracts_failed", []
        )
        if len(body_meta_one_contracts_failed) > 0:
            contracts_failed.append(
                {
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "BODY_HAS_FAILING_CONTRACTS"
                    ],
                    "context": f"Failing contracts found in Body Meta ONE Response: {','.join(body_meta_one_contracts_failed)}",
                }
            )
            # For cross-queues transparency into the apply color queue, post what are the failing_contracts that will be worked in the body queue while the body is being worked
            contracts_failed.extend(
                [
                    {"issueTypeCode": contract_variable_code, "context": ""}
                    for contract_variable_code in body_meta_one_contracts_failed
                ]
            )

            # Check if there's an open BORR to address failing contracts
            borr_response = gql.query(
                body_one_ready_request_graphql_queries.GET_BODY_ONE_READY_REQUESTS,
                variables={
                    "where": {
                        "bodyNumber": {"is": body_code},
                        "isOpen": {"is": True},
                    },
                    "first": 1,
                    "sort": [{"field": "CREATED_AT", "direction": "DESCENDING"}],
                },
            )

            if borr_response.get("errors"):
                raise Exception(borr_response.get("errors"))

            borr_response = borr_response["data"]["bodyOneReadyRequests"][
                "bodyOneReadyRequests"
            ]
            borr = borr_response[0] if len(borr_response) > 0 else None

            if not borr:
                logger.info("Create BORR")
                from res.flows.meta.ONE.body_node import BodyMetaOneNode
                from schemas.pydantic.body_one_ready_request import (
                    CreateBodyOneReadyRequestInput,
                )

                logger.info(f"body = {body_code}")
                # Create Body ONE Ready Request
                borr = BodyOneReadyRequest.create_body_one_ready_request(
                    CreateBodyOneReadyRequestInput(
                        **{
                            "body_id": body_id,
                            "requested_by_email": "techpirates@resonance.nyc",
                            "body_one_ready_request_type": BodyOneReadyRequestType.CONTRACT_VIOLATION.value,
                            "requestor_layers": [RequestorLayer.PLATFORM.value],
                            "body_design_notes": "**Contract Violations:**\n"
                            + "\n".join(
                                [
                                    f"- {contract}"
                                    for contract in body_meta_one_contracts_failed
                                ]
                            ),
                        }
                    )
                )["bodyOneReadyRequest"]

                BodyMetaOneNode.move_body_request_to_node_for_contracts(
                    borr["id"],
                    body_meta_one_contracts_failed,
                    current_status=borr["status"],
                )
            logger.info(f"BORR: {borr}")

        # Some bodies are in different and sometimes inconsistent states at the moment - commeting the following lines for now to not block them
        # if body_meta_one_response_status["status"] not in [
        #     "Done",
        #     "Order First ONE",
        #     "Make First ONE",
        #     "Sew Development Feedback",
        #     "First ONE Pending Brand Review",
        # ]:
        #     contracts_failed.append(
        #         {
        #             "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE[
        #                 "FOUND_IN_PROGRESS_BODY_ONE_READY_REQUEST"
        #             ],
        #             "context": f"Body ONE Ready Request Status is '{body_meta_one_response_status['status']}'",
        #         }
        #     )

        if body_meta_one_response_status["body_version"] != current_body_version:
            contracts_failed.append(
                {
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "BODY_META_ONE_RESPONSE_VERSION_MISMATCHES_BODY_VERSION"
                    ],
                    "context": "Body Meta ONE Response doesn't match current Body Version",
                }
            )

        if requested_body_version != current_body_version:
            # once BORR for newer version is completed, the version will be registered into platform by reproducing in progress requests
            contracts_failed.append(
                {
                    "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "REQUEST_VERSION_MISMATCHES_BODY_VERSION"
                    ],
                    "context": "Request doesn't match current body version. Body ONE Ready Request for newer version is most likely in progress..",
                }
            )

        # we may want to add this once the data is backfilled etc
        # if body_meta_one_response_status["updated_at"] > body_meta_one_response_status["body_file_uploaded_at"]:
        #     failing_contracts.append({"issueTypeCode":APPLY_COLOR_ISSUE_TYPE_CODE["PENDING_BODY_META_ONE_RESPONSE_UPDATE"],"context":""})

        if len(contracts_failed) == 0:
            if body_meta_one_contracts_status == "ERROR":
                # NOTE: when the bodies/status is ERROR usually we don't have a body file. we may want to improve this check but adding this flag in the meantimr while we learn more of the flow.
                contracts_failed.append(
                    {
                        "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                        "context": f"It seems body is still missing body file for version {requested_body_version}",
                    }
                )
    else:
        contracts_failed.append(
            {
                "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                "context": "Unable to resolve body meta one response status",
            }
        )

    is_body_valid = len(contracts_failed) == 0
    return is_body_valid, contracts_failed


def is_previous_version_style_meta_one_valid(
    style_id,
    style_code,
    body_version,
    request_reasons_contract_variables_codes,
    is_style_3d_onboarded,
):
    from res.utils.secrets import secrets_client

    previous_done_style_acr = get_apply_color_requests(
        variables={
            "first": 1,
            "where": {
                # "bodyVersion": {"isLessThan": body_version}, # do we want to only consider styles from previous versions?
                "applyColorFlowStatus": {"is": "Done"},
                "styleId": {"is": style_id},
                "styleCode": {"is": style_code},
            },
        },
    )

    meta_one_status = None
    body_changes_validated_at = None
    are_body_changes_insignificant = False

    url = "https://data.resmagic.io/meta-one/meta-one/status/"
    meta_one_status_response = requests.get(
        url,
        params={"style_sku": style_code, "body_version": body_version},
        headers={
            "content-type": "application/json",
            "Authorization": "Bearer "
            + secrets_client.get_secret("RES_META_ONE_API_KEY"),
        },
    )

    meta_one_status_response.raise_for_status()

    logger.info(f"/meta-one/status/:{meta_one_status_response}")

    meta_one_status_response.raise_for_status()
    meta_one_status = meta_one_status_response.json()
    logger.info(f"/meta-one/status/: {meta_one_status}")

    if not meta_one_status:
        raise Exception(f"Failed trying to get /meta-one/status from {url}")

    m1_contracts_failing_codes = meta_one_status.get("contracts_failing", []) or []

    body_changes_validated_at = meta_one_status.get(
        "piece_version_delta_test_results", {}
    ).get("test_run_at", None)
    are_body_changes_insignificant = not meta_one_status.get(
        "piece_version_delta_test_results", {}
    ).get("significant_change", True)
    # style_latest_body_version = meta_one_status.get(
    #     "body_version", None
    # )
    # style_body_version = meta_one_status.get(
    #     "piece_version_delta_test_results", {}
    # ).get("style_body_version", None)
    bw_file_body_version = meta_one_status.get(
        "piece_version_delta_test_results", {}
    ).get("bw_file_body_version", None)

    m1_versioned_body_contracts_codes = [
        contract_code
        for body_contracts in meta_one_status.get("versioned_body_contracts", []) or []
        for contract_code in body_contracts.get("Contracts Failed Codes") or []
    ]

    if (
        body_changes_validated_at
        and bw_file_body_version
        and bw_file_body_version != body_version
    ):
        # NOTE: before trying to get the color_application_mode AND meta-one/status, we should've confirmed that the body status is ONE Ready for the desired body_version.
        # Adding an extra check just in case in the future the gate/check was skipped
        Exception(
            f"body meta one has not been generated for version {body_version}. Details: meta-one/status brings bw_file_body_version={bw_file_body_version} while the expected version is body_version={body_version}."
        )

    # TODO: some of the request_reasons_contract_variables_codes are NONE because a contract variable was used but it has no code associated. we need to add the codes and make it a standard requirement for all new contract variables
    all_failing_contracts = (
        request_reasons_contract_variables_codes
        + m1_contracts_failing_codes
        + m1_versioned_body_contracts_codes
    )

    logger.info(f"All failing contracts: {all_failing_contracts}")

    ALLOWED_FAILING_CONTRACTS_REUSING_META_ONES = [
        "BODY_VERSION_CHANGED",
        "BODY_FILE_CHANGED",
        "NO_CLOSE_TOGETHER_POINTS",
        "NO_SHARP_BENDS",
    ]

    is_previous_meta_one_still_valid = (
        len(previous_done_style_acr) > 0
        and body_version > 1
        and is_style_3d_onboarded  # safety-net to ensure we've made this style before. to be incorporated into the meta-one/status
        and body_changes_validated_at
        and are_body_changes_insignificant
        and (
            len(all_failing_contracts) == 0
            or set(all_failing_contracts).issubset(
                ALLOWED_FAILING_CONTRACTS_REUSING_META_ONES
            )
        )
    )

    should_retry_later = (
        len(previous_done_style_acr) > 0
        and body_version > 1
        and is_style_3d_onboarded
        and not body_changes_validated_at
        and (
            len(all_failing_contracts) == 0
            or set(all_failing_contracts).issubset(
                ALLOWED_FAILING_CONTRACTS_REUSING_META_ONES
            )
        )
    )  # in case the body changes have not been yet validated (body_changes_validated_at=None), the meta-one/status endpoint will trigger it and we can re-try later to pickup the results

    return (
        is_previous_meta_one_still_valid,
        all_failing_contracts,
        should_retry_later,
        are_body_changes_insignificant,
        body_changes_validated_at,
    )


def compute_color_application_mode(
    style_id,
    style_code,
    body_version,
    apply_color_flow_type,
    color_type,
    request_reasons_contract_variables_codes,
    is_style_3d_onboarded,
):
    """
    Compute the color application mode that would be used in the apply color queue.
    possible options are:
    Automatically: color will be placed automatically by platform
    Manually: a user from the apply color queue team will apply color
    """

    from schemas.pydantic.apply_color_request import (
        ColorApplicationMode,
        ColorType,
        ApplyColorFlowType,
        AssetsGenerationMode,
    )

    color_application_mode = None
    assets_generation_mode = AssetsGenerationMode.CREATE_NEW_ASSETS.value

    (
        is_previous_meta_one_still_valid,
        failing_contracts,
        should_retry_later,
        are_body_changes_insignificant,
        body_changes_validated_at,
    ) = is_previous_version_style_meta_one_valid(
        style_id,
        style_code,
        body_version,
        request_reasons_contract_variables_codes,
        is_style_3d_onboarded,
    )

    validate_body_changes_status = "Done" if body_changes_validated_at else "Skip"

    if apply_color_flow_type == ApplyColorFlowType.APPLY_DYNAMIC_COLOR_WORKFLOW.value:
        color_application_mode = ColorApplicationMode.AUTOMATICALLY.value
        should_retry_later = False
    elif apply_color_flow_type == ApplyColorFlowType.VSTITCHER.value:
        if color_type == ColorType.DEFAULT.value:
            color_application_mode = ColorApplicationMode.AUTOMATICALLY.value
            should_retry_later = False
        elif color_type == ColorType.CUSTOM.value:
            if not should_retry_later:
                if is_previous_meta_one_still_valid:
                    color_application_mode = ColorApplicationMode.AUTOMATICALLY.value
                    assets_generation_mode = (
                        AssetsGenerationMode.COPY_ASSETS_FROM_PREVIOUS_VERSION.value
                    )
                else:
                    color_application_mode = ColorApplicationMode.MANUALLY.value
            else:
                validate_body_changes_status = None
                assets_generation_mode = None
        else:
            raise Exception(f"Unsupported colorType {color_type}")
    else:
        raise Exception(f"Unsupported applyColorFlowType {apply_color_flow_type}")
    logger.info(
        {
            "is_previous_meta_one_still_valid": is_previous_meta_one_still_valid,
            "failing_contracts": failing_contracts,
            "assets_generation_mode": assets_generation_mode,
            "color_application_mode": color_application_mode,
            "should_retry_later": should_retry_later,
            "body_changes_validated_at": body_changes_validated_at,
            "are_body_changes_insignificant": are_body_changes_insignificant,
        }
    )
    return (
        color_application_mode,
        body_changes_validated_at,
        are_body_changes_insignificant,
        assets_generation_mode,
        validate_body_changes_status,
        should_retry_later,
    )


def assign_assignee(apply_color_request_id, color_application_mode):
    from schemas.pydantic.apply_color_request import ColorApplicationMode

    logger.info(f"assign_assignee apply_color_request_id={apply_color_request_id}")

    gql = ResGraphQLClient()

    contracts_failed = []
    update_request_payload = {}
    new_assignee_email = None

    try:
        acr = gql.query(
            graphql_queries.GET_APPLY_COLOR_REQUEST,
            variables={"id": apply_color_request_id},
        )["data"]["applyColorRequest"]

        request_color_application_mode = (
            color_application_mode or acr["colorApplicationMode"]
        )

        if not request_color_application_mode:
            raise Exception(
                "Request is missing the color application mode. An assignee will be able to be assigned once the color application mode is provided."
            )

        current_assignee_email = (
            acr["assignee"]["email"]
            if acr["assignee"] and acr["assignee"]["email"]
            else None
        )

        if current_assignee_email:
            # NOTE: when a request is created via the reproduceApplyColorRequest mutation, the assignee_email is copied from the old request into the new one on request creation.
            raise FinishEarly(
                f"Assignee has been already assigned to this request: {current_assignee_email}"
            )

        resources = []
        has_more = True
        after = None
        while has_more:
            response = gql.query(
                graphql_queries.GET_ONE_RESOURCES,
                variables={
                    "first": 100,
                    "where": {
                        "queue": {"is": "Apply Color Requests"},
                        "isAutoTaskAssignationEnabled": {"is": True},
                    },
                    "sort": [{"field": "RESOURCE_ID", "direction": "ASCENDING"}],
                    "after": after,
                },
            )["data"]["oneResources"]

            has_more = response["hasMore"]
            after = response["cursor"]

            resources.extend(response["oneResources"])

        if len(resources) == 0:
            raise FinishEarly(
                "No resources available to assign at this time. If required, please assign them in the Resources Table https://airtable.com/appH5S4hIuz99tAjm/tblD4apQytD8mq5wz "
            )

        resource_email_to_resource_id_mapping = {
            r["userId"]: r["resourceId"]
            for r in resources
            if r and r["userId"] and r["resourceId"]
        }

        previous_acr_response = get_apply_color_requests(
            {
                "first": 1,
                "where": {
                    "colorApplicationMode": {"is": request_color_application_mode},
                },
                "sort": [
                    {"field": "ASSIGNEE_EMAIL_ASSIGNED_AT", "direction": "DESCENDING"}
                ],
            }
        )["applyColorRequests"]

        previous_acr = (
            previous_acr_response[0] if len(previous_acr_response) > 0 else None
        )

        previous_assignee_email = (
            previous_acr["assignee"]["email"]
            if previous_acr["assignee"] and previous_acr["assignee"]["email"]
            else None
        )

        previous_assignee_resource_id = (
            resource_email_to_resource_id_mapping.get(previous_assignee_email)
            if previous_assignee_email
            else None
        )

        idx_prev_assignee = (
            list(resource_email_to_resource_id_mapping.values()).index(
                previous_assignee_resource_id
            )
            if previous_assignee_resource_id
            else None
        )

        id_new_assignee_email = (
            idx_prev_assignee + 1
            if idx_prev_assignee is not None
            and idx_prev_assignee > -1
            and len(resource_email_to_resource_id_mapping.keys())
            > idx_prev_assignee + 1
            else 0
        )

        new_assignee_email = list(resource_email_to_resource_id_mapping.keys())[
            id_new_assignee_email
        ]

        logger.info(f"new_assignee_email {new_assignee_email}")

        if new_assignee_email:
            gql.query(
                graphql_queries.UPDATE_APPLY_COLOR_REQUEST,
                variables={
                    "id": apply_color_request_id,
                    "input": {
                        "assigneeEmail": new_assignee_email,
                        "assigneeEmailAssignedAt": datetime.utcnow().strftime(
                            "%Y-%m-%dT%H:%M:%SZ"
                        ),
                    },
                },
            )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    except FinishEarly as e:
        logger.info(e)
    except Exception as e:
        import traceback

        logger.error(f"error: {e} {traceback.format_exc()}")
        contracts_failed.append(
            {
                "issueTypeCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                "context": f"An error occurred: {e} \n {traceback.format_exc()}",
            }
        )

    return {
        "assigneeEmail": update_request_payload,
        "contractsFailed": contracts_failed,
    }


def get_apply_color_request_is_make_one_ready_status():
    """
    Get a full summary of all gates - presummably by nodes. we can use this to migrate the DxA Exit bot logic into the API and also get a state of all nodes
    """
    raise NotImplementedError()
