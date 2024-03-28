from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import os
from res.utils import logger
import json, res, os, traceback
import res.flows.meta.one_marker.graphql_queries as graphql_queries
from res.flows.dxa.styles.helpers import _style_id_from_name
from res.flows.dxa.styles.queries import ADD_STYLE_3D_MODEL_URIS

s3 = res.connectors.load("s3")

APPLY_COLOR_ISSUE_TYPE_CODE = {
    "PIECE_MAPPING_NOT_ONE_READY": "PIECE_MAPPING_NOT_ONE_READY",
    "PIECE_MAPPING_INVALID": "PIECE_MAPPING_INVALID",
    "DEFAULT_ISSUE_TYPE": "ERROR_CREATING_META_ONE",  # default issue type: this is the issue type we were using from the beggining for all automated flags in the 3d flow - we gotta update all sources to not use it (use an specific flag for each known issue) and just use the default for unknown exceptions
    "PIECE_EXCEEDS_CUTTABLE_WIDTH": "PIECE_EXCEEDS_CUTTABLE_WIDTH",
    "PIECE_HAS_MISSING_PIXELS": "PIECE_HAS_MISSING_PIXELS",
    "FAILED_GENERATING_TURNTABLE": "FAILED_GENERATING_TURNTABLE",
    "FAILED_UNPACKING_SUSPICIOUS_PIECE": "FAILED_UNPACKING_SUSPICIOUS_PIECE",
    "BODY_NOT_ONE_READY": "BODY_NOT_ONE_READY",
    "UNKNOWN_ERROR": "UNKNOWN_ERROR",
    "FAILED_VALIDATING_BODY_META_ONE_RESPONSE": "FAILED_VALIDATING_BODY_META_ONE_RESPONSE",
    "BODY_HAS_FAILING_CONTRACTS": "BODY_HAS_FAILING_CONTRACTS",
    "FOUND_IN_PROGRESS_BODY_ONE_READY_REQUEST": "FOUND_IN_PROGRESS_BODY_ONE_READY_REQUEST",
    "BODY_META_ONE_RESPONSE_VERSION_MISMATCHES_BODY_VERSION": "BODY_META_ONE_RESPONSE_VERSION_MISMATCHES_BODY_VERSION",
    "REQUEST_VERSION_MISMATCHES_BODY_VERSION": "REQUEST_VERSION_MISMATCHES_BODY_VERSION",
    "PENDING_BODY_META_ONE_RESPONSE_UPDATE": "PENDING_BODY_META_ONE_RESPONSE_UPDATE",
    "MISSING_3D_BODY_FILE": "MISSING_3D_BODY_FILE",
}

APPLY_COLOR_FLOW_TYPE = {
    "VSTITCHER": "VStitcher",
    "APPLY_DYNAMIC_COLOR_WORKFLOW": "Apply Dynamic Color Workflow",
}

GET_APPLY_COLOR_REQUEST = """
    query getApplyColorRequest($id: ID!){
        applyColorRequest(id:$id){
            id
            key
            priority
            colorType
            bodyVersion
            colorApplicationMode
            artworkBundleDirS3Uri
            exportAssetBundleJobId
            applyColorFlowType
            assetsGenerationMode
            requestBody3dSimulationFile{
                file {
                    uri
                }
                bodyVersion
            }
            coloredPiecesFile {
                uri
            }
            style {
                id
                code
                body {
                    code
                    number
                }
                artworkFile {
                    file {
                        s3 {
                            bucket
                            key
                        }
                    }
                }
                pieceMapping{
                    bodyPiece {
                        code
                    }
                    material{
                        code
                        cuttableWidth
                        offsetSizeInches
                        printFileCompensationWidth
                    }
                }
                material {
                    code
                    cuttableWidth
                    offsetSizeInches
                    printFileCompensationWidth
                }
                color {
                    code
                }
                name
                isOneReadyOverridden
            }
        }
    }
"""

GET_APPLY_COLOR_REQUEST_VALIDATION_DETAILS = """
    query getApplyColorRequest($id: ID!){
        applyColorRequest(id:$id){
            id
            key
            bodyVersion
            requestType
            applyColorFlowType
            bodyVersion
            style {
                id
                body {
                    id
                    isOneReady
                    isReadyForSample
                }
                stylePiecesMappingStatus {
                    isMakeOneReady
                    reasons
                    internalReasons
                }
                isOneReadyOverridden
            }
        }
    }
"""

UPDATE_APPLY_COLOR_REQUEST_MUTATION = """
    mutation updateApplyColorRequest($id: ID!, $input: UpdateApplyColorRequestInput!){
        updateApplyColorRequest(id:$id, input:$input){
            applyColorRequest {
                id
                exportColoredPiecesJobStatus
                turntableRequiresBrandFeedback
                style {
                    id
                }
            }
        }
    }
"""

UPDATE_STYLE_MUTATION = """
    mutation updateStyle($id:ID!, $input: UpdateStyleInput!){
        updateStyle(id:$id, input:$input){
            style {
                id
                simulationStatus
            }
        }
    }
"""


def flag_apply_color_request(apply_color_request_id, issues_input):
    submitting_service_name = os.getenv("RES_APP_NAME")
    submitting_service_namespace = os.getenv("RES_NAMESPACE")

    gql = ResGraphQLClient()
    logger.info(
        f"Flagging apply color request {apply_color_request_id}: {json.dumps(issues_input, indent=2)} from service: {submitting_service_name}"
    )

    if not submitting_service_name:
        logger.warning(
            f"Service name needed & not defined in the environment: {submitting_service_name}. Please ensure it is added."
        )

    flag_apply_color_request_input = {
        "id": apply_color_request_id,
        "input": {
            "flagApplyColorRequest": True,
            "submittingServiceName": submitting_service_name,
            "submittingServiceNamespace": submitting_service_namespace,
            "createIssuesInput": issues_input,
        },
    }

    try:
        flagged_request = gql.query(
            graphql_queries.FLAG_APPLY_COLOR_REQUEST_MUTATION,
            flag_apply_color_request_input,
        )
    except Exception as e:
        logger.error(f"an error ocurred: {e}")

        flag_apply_color_request_input["input"]["createIssuesInput"] = [
            {
                "issueTypeCode": "UNKNOWN",  #
                "context": f"An error ocurred while flagging:\n{traceback.format_exc()}",
            }
        ]

        flagged_request = gql.query(
            graphql_queries.FLAG_APPLY_COLOR_REQUEST_MUTATION,
            flag_apply_color_request_input,
        )

    os.putenv("WERE_ERRORS_FOUND", "True")
    return flagged_request


def unflag_apply_color_request(apply_color_request_id):
    """
    this fn can may be used at the end of each service running around the apply color queue to resolve any flag/issues that the given service might have caused in the past:
    in case the service is one that retries, on the 1st run it might be that the service failed & flagged the request, but after a retry it succeeds - at this point it might be useful
    to resolve the flags that the service might have triggered initially.

    the mutation will specifically resolve the flag for review tags that are associated to the submittingServiceName (will still be flagged in case other services have open flag for review tags still)
    """

    submitting_service_name = os.getenv("RES_APP_NAME")
    unflagged_request = None

    if not submitting_service_name:
        logger.warning(
            f"Service name needed & not defined in the environment: {submitting_service_name}"
        )
    else:
        gql = ResGraphQLClient()
        logger.info(
            f"Unflagging apply color request {apply_color_request_id} from service: {submitting_service_name}"
        )

        unflag_apply_color_request_input = {
            "id": apply_color_request_id,
            "input": {
                "submittingServiceName": submitting_service_name,
            },
        }

        unflagged_request = gql.query(
            graphql_queries.UNFLAG_APPLY_COLOR_REQUEST_MUTATION,
            unflag_apply_color_request_input,
        )

    return unflagged_request


def get_apply_color_request_prerequisites_errors(record_id):
    """
    fn to validate prerequisites/gates that need to be met before (at the level) but might require fix/need to be gated at this stage as we fix data: initially Style Pieces Mapping!
    """

    from schemas.pydantic.apply_color_request import RequestType

    gql = ResGraphQLClient()
    apply_color_request_response = gql.query(
        GET_APPLY_COLOR_REQUEST_VALIDATION_DETAILS, variables={"id": record_id}
    )["data"]

    apply_color_request = apply_color_request_response["applyColorRequest"]
    style = apply_color_request["style"] if apply_color_request["style"] else None
    body = style["body"] if style["body"] else None
    one_ready_overridden = style.get("isOneReadyOverridden", False)

    errors = []

    # Apply Color Request Checks
    if not apply_color_request["applyColorFlowType"]:
        errors.append(
            {
                "context": "Invalid Input: 'Apply Color Flow Type' is not defined.",
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["DEFAULT_ISSUE_TYPE"],
            }
        )
    elif apply_color_request["applyColorFlowType"] not in list(
        APPLY_COLOR_FLOW_TYPE.values()
    ):
        errors.append(
            {
                "context": f"Invalid Input: 'Apply Color Flow Type' provided is not supported. Possible values are: {list(APPLY_COLOR_FLOW_TYPE.values())}",
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["DEFAULT_ISSUE_TYPE"],
            }
        )

    # Body Checks
    if not apply_color_request["requestType"]:
        errors.append(
            {
                {
                    "context": "Request is missing the Request Type. Tech team please investigate.",
                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                }
            }
        )
    if not body["isOneReady"] and not one_ready_overridden:
        if apply_color_request["requestType"] in [
            RequestType.SAMPLE.value,
            RequestType.META_ONE_MODIFICATION.value,
        ]:
            if not body["isReadyForSample"]:
                errors.append(
                    {
                        "context": "Body is not Ready for Sample",
                        "issue_type_code": "BODY_NOT_READY_FOR_FIRST_ONE",
                    }
                )  # NOTE: maybe we might want to require this check of all request types at some point, but since we're reframing the validity/integrity of this gate now, we'll enable it for samples/1st ONEs only for now so we don't block production orders
        else:
            errors.append(
                {
                    "context": "Body is not ONE Ready",
                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "BODY_NOT_ONE_READY"
                    ],
                }
            )

    if not apply_color_request["bodyVersion"]:
        errors.append(
            {
                "context": "Apply Color Request does not have an associated body version",
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["DEFAULT_ISSUE_TYPE"],
            }
        )

    # Style Checks

    style_pieces_mapping_status = apply_color_request["style"][
        "stylePiecesMappingStatus"
    ]

    brand_not_valid_reasons = style_pieces_mapping_status["reasons"]
    dxa_not_valid_reasons = style_pieces_mapping_status["internalReasons"]

    if len(dxa_not_valid_reasons) > 0:
        # some of the internal issues might fix brand issues, let's check these first
        errors.append(
            {
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["PIECE_MAPPING_INVALID"],
                "context": ", ".join(dxa_not_valid_reasons),
            }
        )
        gql.query(
            UPDATE_STYLE_MUTATION,
            {
                "id": apply_color_request["style"]["id"],
                "input": {
                    "fixStylePiecesMappingInternalRank": [
                        "1. Style Flagged in DXA Queue"
                    ]
                },
            },
        )  # temp: Automatically rank this style as top priority for the "Style Pieces Mapping Fix Queue" for dxa

    else:
        # meta.ONE validation requests are SELF-- styles to validate the body in the Body ONE Ready flow...
        # ..these may be in development/not be yet ONE Ready
        if not one_ready_overridden:
            if apply_color_request["requestType"] == "Meta ONE Validation":
                brand_not_valid_reasons = list(
                    filter(
                        lambda r: "body under development" not in r.lower(),
                        brand_not_valid_reasons,
                    )
                )

            if len(brand_not_valid_reasons) > 0:
                errors.append(
                    {
                        "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                            "PIECE_MAPPING_NOT_ONE_READY"
                        ],
                        "context": ", ".join(brand_not_valid_reasons),
                    }
                )
                gql.query(
                    UPDATE_STYLE_MUTATION,
                    {
                        "id": apply_color_request["style"]["id"],
                        "input": {
                            "fixStylePiecesMappingRank": [
                                "1. Style Flagged in DXA Queue"
                            ]
                        },
                    },
                )  # temp: Automatically rank this style as top priority for the "Style Pieces Mapping Fix Queue" for Brand Success

    return errors


def validate_request_settings(
    apply_color_flow_type, color_application_mode, assets_generation_mode, color_type
):
    from schemas.pydantic.apply_color_request import (
        AssetsGenerationMode,
    )

    errors = []
    VALID_COMBINATIONS = {
        1: {
            "apply_color_flow_type": APPLY_COLOR_FLOW_TYPE[
                "APPLY_DYNAMIC_COLOR_WORKFLOW"
            ],
            "color_application_mode": "automatically",
            "assets_generation_mode": AssetsGenerationMode.CREATE_NEW_ASSETS.value,
            "color_type": "custom",
        },
        2: {
            "apply_color_flow_type": APPLY_COLOR_FLOW_TYPE[
                "APPLY_DYNAMIC_COLOR_WORKFLOW"
            ],
            "color_application_mode": "automatically",
            "assets_generation_mode": AssetsGenerationMode.CREATE_NEW_ASSETS.value,
            "color_type": "default",  # inverse piece control, others?
        },
        3: {
            "apply_color_flow_type": APPLY_COLOR_FLOW_TYPE["VSTITCHER"],
            "color_application_mode": "manually",
            "assets_generation_mode": AssetsGenerationMode.CREATE_NEW_ASSETS.value,
            "color_type": "custom",
        },
        4: {
            "apply_color_flow_type": APPLY_COLOR_FLOW_TYPE["VSTITCHER"],
            "color_application_mode": "manually",  # this is possible only if we choose to override and make a color_type=default manually
            "assets_generation_mode": AssetsGenerationMode.CREATE_NEW_ASSETS.value,
            "color_type": "default",
        },
        5: {
            "apply_color_flow_type": APPLY_COLOR_FLOW_TYPE["VSTITCHER"],
            "color_application_mode": "automatically",
            "assets_generation_mode": AssetsGenerationMode.CREATE_NEW_ASSETS.value,
            "color_type": "default",
        },
        6: {
            "apply_color_flow_type": APPLY_COLOR_FLOW_TYPE["VSTITCHER"],
            "color_application_mode": "automatically",
            "assets_generation_mode": AssetsGenerationMode.COPY_ASSETS_FROM_PREVIOUS_VERSION.value,
            "color_type": "custom",
        },
    }
    request_settings = {
        "apply_color_flow_type": apply_color_flow_type,
        "color_application_mode": color_application_mode,
        "assets_generation_mode": assets_generation_mode,
        "color_type": color_type,
    }
    is_valid = True if request_settings in VALID_COMBINATIONS.values() else False

    if not is_valid:
        errors.append(
            {
                "context": "Apply Color Request does not seem to have been properly configured. Tech team please investigate.",
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["DEFAULT_ISSUE_TYPE"],
            }
        )

    return errors


class ValidationError(Exception):
    pass


def get_export_asset_bundle_job_inputs(record_id):
    """
    function to get asset bundle details for the given apply color request<>associated body version
    here the Source of Truth for the body version is the body version that the applyColorRequest got on creation,
    in case the version has upgraded at body level it might not match the version of the applyColorRequest anymore.
    in case the version is upgraded at body level, the apply color request would need to be reproduced so this process
    happens again for the new version.

    apply_color_request["body_version"] --> the version of the request (the version the body had when the request was created)
    apply_color_request["requestBody3dSimulationFile"] --> returns the body .bw file associated to the body version of the apply color request (not necessarily the current body version)

    """
    from schemas.pydantic.apply_color_request import (
        AssetsGenerationMode,
    )
    import res.flows.meta.apply_color_request.apply_color_request as ApplyColorRequest

    gql = ResGraphQLClient()

    apply_color_request = gql.query(
        GET_APPLY_COLOR_REQUEST, variables={"id": record_id}
    )["data"]["applyColorRequest"]
    apply_color_flow_type = apply_color_request.get("applyColorFlowType", None)

    errors = []

    try:
        errors = errors + get_apply_color_request_prerequisites_errors(record_id)
        color_type = (
            apply_color_request["colorType"].lower()
            if apply_color_request["colorType"]
            else None
        )
        style = apply_color_request["style"]
        one_ready_overridden = style.get("isOneReadyOverridden", False)
        request_body_version = apply_color_request["bodyVersion"]
        color_application_mode = (
            apply_color_request["colorApplicationMode"].lower()
            if apply_color_request["colorApplicationMode"]
            else None
        )
        assets_generation_mode = apply_color_request.get("assetsGenerationMode", None)

        job_priority = (
            apply_color_request["priority"] if apply_color_request["priority"] else 0
        )

        job_details = {
            "at_color_queue_record_id": record_id,
            "body_code": style["body"]["code"],
            "style_id": style["id"],
            "body_version": request_body_version,
            "style_code": style["code"],
        }

        job_details["default_seam_buffer_inches"] = (
            (apply_color_request.get("style", {}) or {}).get("material", {}) or {}
        ).get("offsetSizeInches") or 0
        job_details["piece_material_mapping"] = (
            apply_color_request.get("style", {}).get("pieceMapping") or []
        )
        errors = errors + validate_request_settings(
            apply_color_flow_type,
            color_application_mode,
            assets_generation_mode,
            color_type,
        )

        if len(errors) > 0:
            raise ValidationError(errors)
        if apply_color_flow_type.lower() == APPLY_COLOR_FLOW_TYPE["VSTITCHER"].lower():
            if color_application_mode == "manually":
                # use coloredPiecesFile/style file: body file with color applied manually
                # Note: re color_type, this usually might be a default request or a custom that doesn't have an artwork_bundle exported to replace automatically
                if (
                    apply_color_request["coloredPiecesFile"]
                    and apply_color_request["coloredPiecesFile"]["uri"]
                ):
                    job_details["input_file_uri"] = apply_color_request[
                        "coloredPiecesFile"
                    ]["uri"]
                else:
                    errors.append(
                        {
                            "context": "No Colored Pieces File (style file) has been uploaded to the apply color request.",
                            "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                "DEFAULT_ISSUE_TYPE"
                            ],
                        }
                    )
            elif color_application_mode == "automatically":
                if (
                    assets_generation_mode
                    == AssetsGenerationMode.CREATE_NEW_ASSETS.value
                ):
                    if color_type == "custom":
                        # place color automatically based on the artwork bundle, needs artwork bundle directory and body file (without color)
                        request_body_file_s3_uri = (
                            apply_color_request["requestBody3dSimulationFile"]["file"][
                                "uri"
                            ]
                            if apply_color_request["requestBody3dSimulationFile"]
                            and apply_color_request["requestBody3dSimulationFile"][
                                "file"
                            ]
                            and apply_color_request["requestBody3dSimulationFile"][
                                "file"
                            ]["uri"]
                            else None
                        )

                        artwork_bundle_dir_s3_uri = apply_color_request[
                            "artworkBundleDirS3Uri"
                        ]

                        if request_body_file_s3_uri:
                            job_details["input_file_uri"] = request_body_file_s3_uri
                        elif not one_ready_overridden:
                            errors.append(
                                {
                                    "context": "No Body File available for the body version of this apply color request.",
                                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                        "MISSING_3D_BODY_FILE"
                                    ],
                                }
                            )

                        if artwork_bundle_dir_s3_uri:
                            job_details[
                                "remote_artwork_bundle_dir_path"
                            ] = artwork_bundle_dir_s3_uri
                        else:
                            errors.append(
                                {
                                    "context": "No artwork bundle has been exported for this body/color combination.",
                                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                        "DEFAULT_ISSUE_TYPE"
                                    ],
                                }
                            )
                    elif color_type == "default":
                        # place color automatically as a repeat, needs artwork file and body file (without color).
                        artwork_s3_file = (
                            style["artworkFile"]["file"]["s3"]
                            if style["artworkFile"]
                            and style["artworkFile"]["file"]
                            and style["artworkFile"]["file"]["s3"]
                            else None
                        )

                        request_body_file_s3_uri = (
                            apply_color_request["requestBody3dSimulationFile"]["file"][
                                "uri"
                            ]
                            if apply_color_request["requestBody3dSimulationFile"]
                            and apply_color_request["requestBody3dSimulationFile"][
                                "file"
                            ]
                            and apply_color_request["requestBody3dSimulationFile"][
                                "file"
                            ]["uri"]
                            else None
                        )

                        if artwork_s3_file:
                            artwork_file_s3_uri = f"s3://{artwork_s3_file['bucket']}/{artwork_s3_file['key']}"
                            job_details[
                                "remote_artwork_file_path"
                            ] = artwork_file_s3_uri
                        else:
                            errors.append(
                                {
                                    "context": "No artwork found associated to this style.",
                                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                        "DEFAULT_ISSUE_TYPE"
                                    ],
                                }
                            )

                        if request_body_file_s3_uri:
                            job_details["input_file_uri"] = request_body_file_s3_uri
                        elif not one_ready_overridden:
                            errors.append(
                                {
                                    "context": "No Body File available for the body version of this apply color request.",
                                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                        "MISSING_3D_BODY_FILE"
                                    ],
                                }
                            )
                    else:
                        errors.append(
                            {
                                "context": "Not valid color type provided. Possible values are 'default' or 'custom'.",
                                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                    "DEFAULT_ISSUE_TYPE"
                                ],
                            }
                        )
                elif (
                    assets_generation_mode
                    == AssetsGenerationMode.COPY_ASSETS_FROM_PREVIOUS_VERSION.value
                ):
                    # we don't need to manage .bw files since platform we'll copy the assets in generate-meta-one workflow from a previous body version
                    pass
                else:
                    errors.append(
                        {
                            "context": f"Not valid 'Assets Generation Mode' provided. Possible values are {[mode.value for mode in AssetsGenerationMode]}",
                            "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                "DEFAULT_ISSUE_TYPE"
                            ],
                        }
                    )
            else:
                errors.append(
                    {
                        "context": "Not valid color application mode provided. Possible values are 'Automatically' or 'Manually'.",
                        "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                            "DEFAULT_ISSUE_TYPE"
                        ],
                    }
                )
        elif (
            apply_color_flow_type.lower()
            == APPLY_COLOR_FLOW_TYPE["APPLY_DYNAMIC_COLOR_WORKFLOW"].lower()
        ):
            if color_application_mode == "manually":
                errors.append(
                    {
                        "context": f"Invalid Input: 'Color Application Mode'='Manually' not supported for 'Apply Color Flow Type'='{APPLY_COLOR_FLOW_TYPE['APPLY_DYNAMIC_COLOR_WORKFLOW']}'. Color is placed automatically when using when using this flow type.",
                        "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                            "DEFAULT_ISSUE_TYPE"
                        ],
                    }
                )
            elif color_application_mode == "automatically":
                request_body_file_s3_uri = (
                    apply_color_request["requestBody3dSimulationFile"]["file"]["uri"]
                    if apply_color_request["requestBody3dSimulationFile"]
                    and apply_color_request["requestBody3dSimulationFile"]["file"]
                    and apply_color_request["requestBody3dSimulationFile"]["file"][
                        "uri"
                    ]
                    else None
                )

                if not request_body_file_s3_uri and not one_ready_overridden:
                    # NOTE: this check should be moved upstream to the Body ONE Ready Flow - bodies shouldn't be able to move to this step without all requirements provided
                    errors.append(
                        {
                            "context": "No Body File available for the body version of this apply color request.",
                            "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                "MISSING_3D_BODY_FILE"
                            ],
                        }
                    )

                artwork_s3_file = (
                    style["artworkFile"]["file"]["s3"]
                    if style["artworkFile"]
                    and style["artworkFile"]["file"]
                    and style["artworkFile"]["file"]["s3"]
                    else None
                )

                if (
                    not artwork_s3_file
                    or not artwork_s3_file["bucket"]
                    or not artwork_s3_file["key"]
                ):
                    errors.append(
                        {
                            "context": "No artwork found associated to this style.",
                            "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                                "DEFAULT_ISSUE_TYPE"
                            ],
                        }
                    )
            else:
                errors.append(
                    {
                        "context": "Not valid color application mode provided. Possible values are 'Automatically' or 'Manually'.",
                        "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                            "DEFAULT_ISSUE_TYPE"
                        ],
                    }
                )
        else:
            errors.append(
                {
                    "context": f"Invalid Input: 'Apply Color Flow Type' provided is not supported. Possible values are: {list(APPLY_COLOR_FLOW_TYPE.values())}",
                    "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE[
                        "DEFAULT_ISSUE_TYPE"
                    ],
                }
            )

        ApplyColorRequest.update_apply_color_request(
            record_id,
            {
                "validateExportColoredPiecesInputsStatus": (
                    "Done" if len(errors) == 0 else "Failed"
                )
            },
        )

    except ValidationError as e:
        logger.info(f"an error ocurred: {e}")
    except Exception as e:
        logger.info(f"an error ocurred: {e}")
        errors.append(
            {
                "context": f"An error occured:\n{traceback.format_exc()}",
                "issue_type_code": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
            }
        )

    if len(errors) > 0:
        logger.info(f"Errors: {errors}")
        issues_input = [
            {"issueTypeCode": error["issue_type_code"], "context": error["context"]}
            for error in errors
        ]
        flag_apply_color_request(record_id, issues_input)
        raise Exception(json.dumps(errors, indent=2))
    else:
        unflag_apply_color_request(record_id)

    return {
        "details": job_details,
        "priority": job_priority,
        "apply_color_flow_type": apply_color_flow_type,
        "assets_generation_mode": assets_generation_mode,
    }


def get_fields_to_sync_to_export_asset_bundle_job(record_id):
    gql = ResGraphQLClient()

    apply_color_request = gql.query(
        GET_APPLY_COLOR_REQUEST, variables={"id": record_id}
    )["data"]["applyColorRequest"]

    return {
        "export_asset_bundle_job_id": apply_color_request.get("exportAssetBundleJobId"),
        "fields_to_update": {"priority": apply_color_request.get("priority")},
    }


def update_apply_color_job_status(record_id, job_status):
    gql = ResGraphQLClient()
    response = gql.query(
        UPDATE_APPLY_COLOR_REQUEST_MUTATION,
        variables={
            "id": record_id,
            "input": {"exportColoredPiecesJobStatus": job_status},
        },
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    return response


def update(record_id, payload):
    gql = ResGraphQLClient()
    response = gql.query(
        UPDATE_APPLY_COLOR_REQUEST_MUTATION,
        variables={
            "id": record_id,
            "input": payload,
        },
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    return response


def process_on_turntable_created(id, path_to_turntable):
    gql = ResGraphQLClient()
    updated_request = gql.query(
        UPDATE_APPLY_COLOR_REQUEST_MUTATION,
        variables={
            "id": id,
            "input": {"turntableS3Uri": path_to_turntable},
        },
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    return updated_request


def process_on_asset_bundle_created(id, path_to_asset_bundle):
    gql = ResGraphQLClient()
    updated_request = gql.query(
        UPDATE_APPLY_COLOR_REQUEST_MUTATION,
        variables={
            "id": id,
            "input": {"exportedPiecesFileUri": path_to_asset_bundle},
        },
    )["data"]["updateApplyColorRequest"]["applyColorRequest"]

    return updated_request


def process_3d_model(record_id, path):
    hasura = res.connectors.load("hasura")

    gql = ResGraphQLClient()
    request = gql.query(GET_APPLY_COLOR_REQUEST, variables={"id": record_id})
    request = request["data"]["applyColorRequest"]

    body_version = request["bodyVersion"]
    body_code = request["style"]["body"]["code"]
    color_code = request["style"]["color"]["code"]

    root = "s3://meta-one-assets-prod/color_on_shape"
    relative = f"{body_code.replace('-', '_')}/v{body_version}/{color_code}"
    dest = f"{root}/{relative.lower()}"

    logger.info(f"Unzipping {path} to {dest}")
    s3.unzip(path, dest)

    name = request["style"]["name"]
    id = _style_id_from_name(name, body_code)
    logger.info(f"Updating '{id}' generated from '{name}' and '{body_code}'")
    hasura.execute_with_kwargs(
        ADD_STYLE_3D_MODEL_URIS,
        id=id,
        metadata={
            "has_3d_model": True,
            "3d_model": f"{dest}/3d.glb",
            "point_cloud": f"{dest}/point_cloud.json",
            "front_image": f"{dest}/front.png",
        },
        model_3d_uri=f"{dest}/3d.glb",
        point_cloud_uri=f"{dest}/point_cloud.json",
        front_image_uri=f"{dest}/front.png",
    )
