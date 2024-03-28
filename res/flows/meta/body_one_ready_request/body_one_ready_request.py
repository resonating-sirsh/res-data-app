from res.utils.logging import logger
from schemas.pydantic.body_one_ready_request import CreateBodyOneReadyRequestInput
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import json
import http.client
import res.flows.meta.body_one_ready_request.graphql_queries as graphql_queries
import res.flows.meta.body.graphql_queries as body_graphql_queries
from res.flows.meta.style.style import assign_trims_to_first_one_style
from res.utils import secrets_client, post_with_token


def get_body_one_ready_requests(variables):
    gql = ResGraphQLClient()

    response = gql.query(
        graphql_queries.GET_BODY_ONE_READY_REQUESTS,
        variables=variables,
    )

    errors = response.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return response["data"]["bodyOneReadyRequests"]


def create_body_one_ready_request(input: CreateBodyOneReadyRequestInput):
    import res

    s3 = res.connectors.load("s3")

    # TODO: we want to return both BORR + trims info
    # TODO: return custom error messages and details

    # TODO: map trim taxonomies to codes
    DEFAULT_LABEL_TRIM_TAXONOMIES_TO_IDS = {
        "MAIN_WOVEN_LABEL": "recytU0QpAUyXqSSw",
        "SIZE_WOVEN_LABEL": "recjII0WrhWSNXeGu",
        "CARE_AND_CONTENT_WOVEN_LABEL": "recJNeV3ZguRuSQhh",
    }
    starting_from_scratch_trims = None
    from schemas.pydantic.body_one_ready_request import (
        BillOfMaterialSupplier,
        StartingPointType,
        BodyOneReadyFlow,
    )

    gql = ResGraphQLClient()
    logger.info(
        f"create_body_one_ready_request input: {json.dumps(str(dict(input)),indent=2)}"
    )

    create_boms_payload = []

    # NOTE: maybe we'd like to check if we have any trims beforehand and dont create new ones?

    ### Bill of materials ###
    # a. Labels
    if input.starting_point_type:
        if (
            input.label_settings.supplier == BillOfMaterialSupplier.RESONANCE
        ):  # Design label
            if not input.label_settings.label_id:
                raise Exception("Please provide a Label as your Resonance Label.")

            query_label = gql.query(
                graphql_queries.QUERY_LABEL,
                variables={"id": input.label_settings.label_id},
            )["data"]["label"]

            # Add main label
            create_boms_payload.extend(
                [
                    {
                        "bodyId": input.body_id,
                        "supplier": BillOfMaterialSupplier.RESONANCE.name,
                        "trimTaxonomyId": query_label["trimTaxonomyEntityId"],
                    },
                    {
                        "bodyId": input.body_id,
                        "supplier": BillOfMaterialSupplier.RESONANCE.name,
                        "trimTaxonomyId": DEFAULT_LABEL_TRIM_TAXONOMIES_TO_IDS[
                            "CARE_AND_CONTENT_WOVEN_LABEL"
                        ],
                    },
                ]
            )
        elif (
            input.label_settings.supplier == BillOfMaterialSupplier.BRAND
        ):  # Supply label
            if not input.label_settings.brand_intake_files:
                raise Exception(
                    "Please provide a File that represents your brand designed label."
                )

            create_boms_payload.extend(
                [
                    {
                        "bodyId": input.body_id,
                        "supplier": BillOfMaterialSupplier.BRAND.name,
                        "trimTaxonomyId": DEFAULT_LABEL_TRIM_TAXONOMIES_TO_IDS[
                            "MAIN_WOVEN_LABEL"
                        ],
                        "brandIntakeFilesMetadata": [
                            {"uri": file.uri}
                            for file in input.label_settings.brand_intake_files
                        ],
                        "brandIntakeFiles": [
                            {"url": s3.generate_presigned_url(file.uri)}
                            for file in input.label_settings.brand_intake_files
                        ],
                    },
                    {
                        "bodyId": input.body_id,
                        "supplier": BillOfMaterialSupplier.BRAND.name,
                        "trimTaxonomyId": DEFAULT_LABEL_TRIM_TAXONOMIES_TO_IDS[
                            "SIZE_WOVEN_LABEL"
                        ],
                    },
                    {
                        "bodyId": input.body_id,
                        "supplier": BillOfMaterialSupplier.BRAND.name,
                        "trimTaxonomyId": DEFAULT_LABEL_TRIM_TAXONOMIES_TO_IDS[
                            "CARE_AND_CONTENT_WOVEN_LABEL"
                        ],
                    },
                ]
            )
    # b. Trims
    if input.starting_point_type == StartingPointType.STARTING_FROM_EXISTING_BODY:
        if not input.bill_of_materials_settings:
            raise Exception("Missing Bill of Materials.")

        if not input.parent_body_id:
            raise Exception("Missing Existing/Parent Body")

        parent_body_boms = gql.query(
            graphql_queries.QUERY_BODY_BILL_OF_MATERIALS,
            variables={"id": input.parent_body_id},
        )["data"]["body"]["billOfMaterials"]

        # filter out labels since we've mapped labels already
        formatted_parent_body_boms = list(
            filter(
                lambda bom: not bom["trimTaxonomy"]
                or not bom["trimTaxonomy"]["name"]
                or "label" not in bom["trimTaxonomy"]["name"].lower(),
                parent_body_boms,
            )
        )
        logger.info(f"formatted_parent_body_boms={formatted_parent_body_boms}")
        logger.info(f"bill_of_materials_settings={input.bill_of_materials_settings}")

        parent_bom_id_to_supplier_mapping = {
            bom.parent_bill_of_material_id: bom.supplier
            for bom in input.bill_of_materials_settings
        }

        logger.info(
            f"parent_bom_id_to_supplier_mapping: {parent_bom_id_to_supplier_mapping}"
        )

        create_boms_payload.extend(
            [
                {
                    "bodyId": input.body_id,
                    "supplier": (
                        parent_bom_id_to_supplier_mapping[parent_body_bom["id"]].name
                        if parent_bom_id_to_supplier_mapping.get(
                            parent_body_bom.get("id", None), None
                        )
                        else None  # set to rensonance by default?
                    ),
                    "trimTaxonomyId": (
                        parent_body_bom["trimTaxonomy"]["id"]
                        if parent_body_bom["trimTaxonomy"]
                        and parent_body_bom["trimTaxonomy"]["id"]
                        else None
                    ),
                    "trimLength": parent_body_bom["trimLength"],
                    "trimQuantity": parent_body_bom["trimQuantity"],
                    "trimTaxonomyUsage": parent_body_bom["trimTaxonomyUsage"],
                    "fusingId": parent_body_bom["fusingId"],
                    "pieceType": parent_body_bom["pieceType"],
                    "bodyPiecesIds": parent_body_bom["bodyPiecesIds"],
                }
                for parent_body_bom in formatted_parent_body_boms
            ]
        )

    elif input.starting_point_type == StartingPointType.STARTING_FROM_SCRATCH:
        if not input.bill_of_materials_settings:
            raise Exception("Missing Bill of Materials.")

        starting_from_scratch_trims = []
        # TODO: validate that we're using StartingFromScratchBillOfMaterialsSettings
        for bom in input.bill_of_materials_settings:
            starting_from_scratch_trims.append(
                {
                    "category": bom.category,
                    "size": bom.size,
                    "placementLocation": bom.placement_location,
                    "quantity": bom.quantity,
                    "supplier": bom.supplier.name,
                }
            )

        # TODO: product team will confirm logic re how to auto assign threads

    logger.info(f"create_boms_payload: {json.dumps(str(create_boms_payload),indent=2)}")
    # TODO: move BOM creation to its own path/controller
    for new_bom_payload in create_boms_payload:
        create_bom_response = gql.query(
            graphql_queries.CREATE_BILL_OF_MATERIAL,
            variables={"input": new_bom_payload},
        )
        logger.info(f"create_bom_response {create_bom_response}")

    if input.pattern_files or input.techpack_files:
        # add pattern files & techpacks to the body
        # TODO: in the future we might want to add multiple pattern & techpacks for different versions. make sure to add the new ones instead of re-writting them
        gql.query(
            graphql_queries.UPDATE_BODY,
            variables={
                "id": input.body_id,
                "input": {
                    "patternFiles": [
                        {"uri": file.uri, "bodyVersion": 0}
                        for file in input.pattern_files
                    ],
                    "techpackFiles": [
                        {"uri": file.uri, "bodyVersion": 0}
                        for file in input.techpack_files
                    ],
                },
            },
        )

    # Create BORR
    create_body_one_ready_request_response = gql.query(
        graphql_queries.CREATE_BODY_ONE_READY_REQUEST,
        variables={
            "input": {
                "bodyId": input.body_id,
                "parentBodyId": input.parent_body_id,  # TODO: allow only if starting from existing
                "oneSketchFilesIds": input.one_sketch_files_ids,
                "referenceImagesFilesIds": input.reference_images_files_ids,
                "baseSizeId": input.base_size_id,
                "bodyOperationsIds": input.body_operations_ids,
                "requestedByEmail": input.requested_by_email,
                "wasParentBodySelectedByBrand": input.was_parent_body_selected_by_brand,
                "bodyDesignNotes": input.body_design_notes,
                "sizesNotes": input.sizes_notes,
                "trimNotes": input.trim_notes,
                "labelNotes": input.label_notes,
                "prospectiveLaunchDate": input.prospective_launch_date,
                "bodyOneReadyRequestType": input.body_one_ready_request_type.value,
                "changesRequested": input.changes_requested,
                "areasToUpdate": input.areas_to_update,
                "requestorLayers": [
                    requestor_layer.value for requestor_layer in input.requestor_layers
                ],
                "startingPointType": (
                    input.starting_point_type.name
                    if input.starting_point_type
                    else None
                ),
                "rawBrandIntakeTrims": starting_from_scratch_trims,
                "bodyOneReadyFlow": (
                    input.body_one_ready_flow.name
                    if input.body_one_ready_flow
                    else BodyOneReadyFlow.FLOW_3D.name
                ),
                # "patternFiles": [
                #     {"uri": file.uri, "bodyVersion": 0} for file in input.pattern_files
                # ],
                "patternFilesAttachments": [
                    {"url": s3.generate_presigned_url(file.uri)}
                    for file in input.pattern_files
                ],
                # "techpackFiles": [{"uri": file.uri} for file in input.techpack_files],
                "techpackFilesAttachments": [
                    {"url": s3.generate_presigned_url(file.uri)}
                    for file in input.techpack_files
                ],
            }
        },
    )

    logger.info(
        f"create_body_one_ready_request={json.dumps(create_body_one_ready_request_response,indent=2)}"
    )
    if create_body_one_ready_request_response.get("errors"):
        raise Exception(create_body_one_ready_request_response.get("errors"))

    # TODO: define standards for return types from the graph-api while we are still using the graph for this. should we index ["data"] or ["data"]["createBodyOneReadyRequest"] ?
    return create_body_one_ready_request_response["data"]["createBodyOneReadyRequest"]


class DoesntQualifyForOrderingFirstOne(Exception):
    pass


class InvalidInput(Exception):
    pass


def order_first_one_for_body_one_ready_request(body_one_ready_request_id):
    FIRST_ONE_DEFAULTS = {
        "color_id": "recIhPitvd0omQFlL",  # self-- color id
        "email": "techpirates@resonance.nyc",
        "order_channel": "resmagic.io",
        "sales_channel": "First ONE",  # TODO: Create enum in make or fullfillment for sales_channels
        "quantity": 1,
        "shipping_adress": {
            "shipping_name": "Resonance Manufacturing LTD",
            "shipping_address1": "Corporacion Zona Franca Santiago",
            "shipping_address2": "Segunda Etapa, Calle Navarrete No.4",
            "shipping_city": "Santiago",
            "shipping_country": "Dominican Republic",
            "shipping_province": "Santiago",
            "shipping_zip": "51000",
        },
    }

    from res.flows.meta.style.style import map_style_pieces_to_onboarding_materials
    import res.flows.meta.style.graphql_queries as style_graphql_queries
    import schemas.pydantic.style as style_schemas
    from schemas.pydantic.body_one_ready_request import OrderFirstOneMode

    gql = ResGraphQLClient()
    errors = []  # [{"context":"","contractVariableCode":""}]
    order = None
    style = None
    status_code = None
    abort_early = False
    try:
        body_one_ready_request = gql.query(
            graphql_queries.GET_BODY_ONE_READY_REQUEST,
            variables={"id": body_one_ready_request_id},
        )["data"]["bodyOneReadyRequest"]

        body = gql.query(
            body_graphql_queries.GET_BODY,
            variables={"number": body_one_ready_request["bodyNumber"]},
        )["data"]["body"]

        logger.info(
            f"order_first_one_for_body_one_ready_request record_id={body_one_ready_request_id}"
        )

        # logger.info(f"body_one_ready_request={body_one_ready_request}")
        # logger.info(f"body={body}")

        brand_code = body["brand"]["code"]
        onboarding_materials = body["onboardingMaterials"]

        # TODO: Maybe we want to validate Status="Order First ONE"?

        if body_one_ready_request["orderFirstOneStatus"] == "Done":
            status_code = "PREVIOUSLY_CREATED"
            logger.warn(
                "First ONE has been previously ordered (Order First ONE_Status=Done)"
            )
            raise DoesntQualifyForOrderingFirstOne(status_code)
        if body_one_ready_request["firstOneOrder"]:
            order = body_one_ready_request["firstOneOrder"]
            status_code = "PREVIOUSLY_CREATED"
            logger.warn(
                f"First ONE has been previously ordered (First ONE Order ID={order['id']})"
            )
            raise DoesntQualifyForOrderingFirstOne(status_code)

        if body_one_ready_request["orderFirstOneStatus"] == "N/A":
            status_code = "REQUEST_DOESNT_REQUIRE_FIRST_ONE"
            logger.info(
                "This request doesn't require a First ONE to be ordered. (Order First ONE_Status=N/A)."
            )
            raise DoesntQualifyForOrderingFirstOne(status_code)

        order_first_one_mode = body_one_ready_request["orderFirstOneMode"]

        if not order_first_one_mode:
            raise Exception(
                "'Order First ONE Mode' needs to be assigned by the body team prior to requesting the first one."
            )
        elif order_first_one_mode == OrderFirstOneMode.AUTOMATICALLY.value:
            pass
        elif order_first_one_mode == OrderFirstOneMode.MANUALLY.value:
            status_code = "FIRST_ONE_SHOULD_BE_MANUALLY_REQUESTED"
            raise DoesntQualifyForOrderingFirstOne(status_code)
        else:
            Exception(
                f"Invalid 'Order First ONE Mode', valid values are: {','.join([e.value for e in OrderFirstOneMode])}"
            )

        # 1. Validate Body is Ready for Sample
        if not body["isReadyForSample"]:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": f"Body is not Ready For First ONE: {body['readyForSampleStatus']}",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        # doublechecking body onboarding material - this is already part of the readyForSample gate
        first_one_material_code = (
            onboarding_materials[0]["code"] if len(onboarding_materials) > 0 else None
        )
        if not first_one_material_code:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": "Body is missing Onboarding Material",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        if not body["basePatternSize"]:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": "Body is missing Base Size",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        # 2. Pick Style
        style = body_one_ready_request["firstOneStyle"]
        if style:
            logger.info(
                f"Using Style associated to Body ONE Ready Request: {style['code']}"
            )

            if style["body"]["code"] != body_one_ready_request["bodyNumber"]:
                errors.append(
                    {
                        "contractVariableCode": "UNKNOWN",
                        "context": f"Selected First ONE Style ({body_one_ready_request['firstOneStyle']['code']}) should use the body associated with the Body ONE Ready Request ({body_one_ready_request['bodyNumber']}) ",
                    }
                )
                raise InvalidInput(json.dumps(errors, indent=2))

        else:
            styles_response = gql.query(
                style_graphql_queries.GET_STYLES,
                variables={
                    "where": {
                        "bodyCode": {"is": body["code"]},
                        "materialCode": {"is": first_one_material_code},
                        "colorId": {"is": FIRST_ONE_DEFAULTS["color_id"]},
                    }
                },
            )["data"]["styles"]["styles"]

            if len(styles_response) > 0:
                # style exists
                style = styles_response[0]
                logger.info(f"Picking existing style: {style['code']}")
            else:
                # create style
                style = gql.query(
                    style_graphql_queries.CREATE_STYLE,
                    variables={"input": {"brandCode": brand_code}},
                )["data"]["createStyle"]["style"]

                # Assign BMC (body, material color)
                style = gql.query(
                    style_graphql_queries.UPDATE_STYLE,
                    variables={
                        "id": style["id"],
                        "input": {
                            "bodyCode": body["code"],
                            "materialCode": body["onboardingMaterials"][0]["code"],
                            "colorId": FIRST_ONE_DEFAULTS["color_id"],
                        },
                    },
                )["data"]["updateStyle"]["style"]
                logger.info(f"Created new style: {style['code']}")

        style = gql.query(
            style_graphql_queries.GET_STYLE,
            variables={"id": style["id"]},
        )["data"]["style"]

        if not style["body"]:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": "Style is missing Body",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        if not style["material"]:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": "Style is missing Material",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        if not style["color"]:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": "Style is missing Color",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        if len(style["code"].split(" ")) != 3:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": f"Style Code {style['code']} seems incorrect",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        # 3. Assign Print Type
        if not style["printType"]:
            gql.query(
                style_graphql_queries.UPDATE_STYLE,
                variables={
                    "id": style["id"],
                    "input": {"printType": style_schemas.PrintType.DIRECTIONAL.value},
                },
            )["data"]["updateStyle"]["style"]

        # 4. Map Materials to Combo Pieces
        style = map_style_pieces_to_onboarding_materials(style["id"])["style"]

        # 5. Assign Trims
        # TODO: dont reassign if style has trims assigned already..
        assign_trims_to_first_one_style(style["id"])

        # 6. Validate that Style is Ready For Sample.
        style = gql.query(
            style_graphql_queries.GET_STYLE,
            variables={"id": style["id"]},
        )["data"]["style"]

        sku = f"{style['code'].replace('-','',1)} {style['body']['basePatternSize']['code']}"
        if not style["isReadyForSample"]:
            errors.append(
                {
                    "contractVariableCode": "UNKNOWN",
                    "context": f"Style is not Ready For First ONE: {style['styleReadyForSampleStatus']}",
                }
            )
            raise InvalidInput(json.dumps(errors, indent=2))

        if len(errors) == 0:
            # 7. Place Order
            order = order_first_one(
                {
                    "request_name": f"First ONE {style['brand']['name']}",
                    "brand_code": style["brand"]["code"],
                    "email": FIRST_ONE_DEFAULTS.get("email"),
                    "order_channel": FIRST_ONE_DEFAULTS.get("order_channel"),
                    "sales_channel": FIRST_ONE_DEFAULTS.get("sales_channel"),
                    "shipping_name": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_name"
                    ),
                    "shipping_address1": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_address1"
                    ),
                    "shipping_address2": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_address2"
                    ),
                    "shipping_city": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_city"
                    ),
                    "shipping_country": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_country"
                    ),
                    "shipping_province": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_province"
                    ),
                    "shipping_zip": FIRST_ONE_DEFAULTS.get("shipping_adress").get(
                        "shipping_zip"
                    ),
                    "shipping_phone": "",
                    "line_items_info": [
                        {"sku": sku, "quantity": FIRST_ONE_DEFAULTS.get("quantity")}
                    ],
                }
            )["order"]
            logger.info(
                f"order_first_one_for_body_one_ready_request {json.dumps(order,indent=2)}"
            )
            order["id"] = order.get("order_airtable_id") or None
            if not order or not order["id"]:
                errors.append(
                    {
                        "contractVariableCode": "UNKNOWN",
                        "context": "Failed creating order.",
                    }
                )
    except DoesntQualifyForOrderingFirstOne as e:
        abort_early = True
    except InvalidInput as e:
        pass
    except Exception as e:
        import traceback

        errors.append(
            {
                "contractVariableCode": "UNKNOWN",
                "context": f"An error occurred: {traceback.format_exc()}",
            }
        )

    if not status_code:
        if order and order["id"] and len(errors) == 0:
            status_code = "SUCCESS_CREATED"
        else:
            status_code = "ERROR"

    # 8. Update Body ONE Ready Request
    logger.info(f"errors: {json.dumps(errors,indent=2)}")

    if not abort_early:
        body_one_ready_request = gql.query(
            graphql_queries.UPDATE_BODY_ONE_READY_REQUEST,
            variables={
                "id": body_one_ready_request_id,
                "input": {
                    "orderFirstOneStatus": (
                        "Done" if len(errors) == 0 and order else "Failed"
                    ),
                    "orderFirstOneLog": errors,
                    **({"orderFirstOneLog": errors} if errors else {}),
                    **({"firstOneStyleId": style["id"]} if style else {}),
                    **({"firstOneOrderId": order["id"]} if order else {}),
                },
            },
        )["data"]["updateBodyOneReadyRequest"]["bodyOneReadyRequest"]

    return {
        "status_code": status_code,
        "order": order,
        "errors": errors,
        "style": style,
        "bodyOneReadyRequest": body_one_ready_request,
    }


def order_first_one(first_one_info):
    # logger.info(json.dumps(first_one_info, indent=2))
    payload = {
        "request_name": first_one_info["request_name"],
        "brand_code": first_one_info["brand_code"],
        "email": first_one_info["email"],
        "order_channel": "resmagic.io",
        "sales_channel": "First ONE",
        "shipping_name": first_one_info["shipping_name"],
        "shipping_address1": first_one_info["shipping_address1"],
        "shipping_address2": first_one_info["shipping_address2"],
        "shipping_city": first_one_info["shipping_city"],
        "shipping_country": first_one_info["shipping_country"],
        "shipping_province": first_one_info["shipping_province"],
        "shipping_zip": first_one_info["shipping_zip"],
        "shipping_phone": first_one_info["shipping_phone"],
        "line_items_info": first_one_info["line_items_info"],
        "is_sample": True,
    }

    logger.info(
        f"send request to fulfillment-one/orders: {json.dumps(payload,indent=2)}"
    )
    response = post_with_token(
        "https://data.resmagic.io/fulfillment-one/orders",
        secret_name="FULFILLMENT_ONE_API_KEY",
        json=payload,
    )

    print(f"response: {response}")
    response.raise_for_status()
    body = response.json()
    print(f"data: {body}")

    # The GraphQL API can return a 200 OK response code in cases
    # that would typically produce 4xx or 5xx errors in REST
    errors = (
        (body.get("data", {}).get("createMakeOneOrder", {}).get("userErrors"))
        or (body.get("errors", {}))
        or []
    )

    if errors and len(errors) > 0:
        raise Exception(f"GraphQL Errors: {errors}")

    logger.info(f"order_first_one response: {body}")
    return {"order": body}


def is_request_ready_to_register_new_body_version(
    body_one_ready_request_status,
    register_new_body_version_status,
    request_body_version,
    active_body_version,
):
    register_new_body_version = {"is_ready": False, "message": ""}
    if register_new_body_version_status == "Done":
        register_new_body_version = {
            "is_ready": False,
            "message": "Body Version has been already registered for this body/request",
        }
    elif body_one_ready_request_status != "Done":
        register_new_body_version = {
            "is_ready": False,
            "message": "Body ONE Ready Request must be Done prior to being able to Register the version that is being developed",
        }
    # NOTE: Do we need this?
    # elif request_body_version != active_body_version:
    #     register_new_body_version = {
    #         "is_ready": False,
    #         "message": "Request doesn't match the current body version",
    #     }
    else:
        register_new_body_version = {"is_ready": True, "message": ""}

    return register_new_body_version


def register_new_body_version(body_one_ready_request_id):
    """
    On BORR completion, formally register the new version on platform.
    initially this means:
    - reproduce any open Apply Color Request currently in progress for a previous version
    """

    import res.flows.meta.apply_color_request.apply_color_request as ApplyColorRequest
    import res.flows.meta.body.graphql_queries as body_graphql_queries
    from schemas.pydantic.apply_color_request import ReproduceApplyColorRequestInput

    from res.flows.meta.one_marker.apply_color_request import (
        APPLY_COLOR_ISSUE_TYPE_CODE,
    )

    gql = ResGraphQLClient()
    failing_contracts = []

    try:
        borr = gql.query(
            graphql_queries.GET_BODY_ONE_READY_REQUEST,
            variables={"id": body_one_ready_request_id},
        )["data"]["bodyOneReadyRequest"]

        body_code = borr["bodyNumber"]

        body = gql.query(
            body_graphql_queries.GET_BODY,
            variables={"number": body_code},
        )["data"]["body"]

        # Validate if the request currently qualifies/is ready
        register_new_body_version = is_request_ready_to_register_new_body_version(
            borr["status"],
            borr["registerNewBodyVersionStatus"],
            borr["bodyVersion"],
            body["patternVersionNumber"],
        )

        if not register_new_body_version["is_ready"]:
            return {
                "errors": [
                    {
                        "contractVariableCode": APPLY_COLOR_ISSUE_TYPE_CODE[
                            "UNKNOWN_ERROR"
                        ],
                        "context": register_new_body_version["message"],
                    }
                ],
                "status": "Failed",
                "bodyOneReadyRequest": borr,
            }

        logger.info(f"body={body}")
        apply_color_requests = []
        has_more_acr = True
        after = None
        while has_more_acr:
            data = ApplyColorRequest.get_apply_color_requests(
                variables={
                    "first": 100,
                    "where": {
                        "isOpen": {"is": True},
                        "bodyCode": {"is": body["code"]},
                        "or": [
                            {"bodyVersion": {"isNot": body["patternVersionNumber"]}},
                            {"isFlaggedForReview": {"is": True}},
                        ],
                    },
                    "after": after,
                }
            )

            has_more_acr = data["hasMore"]
            after = data["cursor"]

            apply_color_requests.extend(data["applyColorRequests"])

        logger.info(f"ApplyColorRequests to be reproduced: {len(apply_color_requests)}")

        from res.flows.meta.ONE.contract import ContractVariables

        body_version_changed_cv = ContractVariables.load_by_codes(
            ["BODY_VERSION_CHANGED"], "appqtN4USHTmyC6Dv", "tbl4s194nIMut9xoA"
        )
        if body_version_changed_cv and len(body_version_changed_cv) > 0:
            body_version_changed_cv = body_version_changed_cv[0]
        else:
            raise Exception("BODY_VERSION_CHANGED Contract Variable not found")

        for acr in apply_color_requests:
            reproduced_acr = ApplyColorRequest.reproduce_apply_color_request(
                id=acr["id"],
                input=ReproduceApplyColorRequestInput(
                    **{
                        "request_reasons_contract_variables_ids": [
                            body_version_changed_cv["id"]
                        ],
                        "reproduce_reasons": ["Body Version Change"],
                        "canceller_email": "techpirates@resonance.nyc",
                        "reproduce_details": "Body Version Changed. Request reproduced automatically.",
                    }
                ),
            )["applyColorRequest"]
            logger.info(reproduced_acr)
    except Exception as e:
        import traceback

        failing_contracts.append(
            {
                "contractVariableCode": APPLY_COLOR_ISSUE_TYPE_CODE["UNKNOWN_ERROR"],
                "context": f"{e} \n {traceback.format_exc()}",
            }
        )
        logger.error(f"{traceback.format_exc()}")

    # TODO: Ping to slack when this fails (+ succeds?)

    updated_borr = gql.query(
        graphql_queries.UPDATE_BODY_ONE_READY_REQUEST,
        variables={
            "id": body_one_ready_request_id,
            "input": {
                "registerNewBodyVersionLog": (
                    failing_contracts if len(failing_contracts) > 0 else None
                ),
                "registerNewBodyVersionStatus": (
                    "Done" if len(failing_contracts) == 0 else "Failed"
                ),
            },
        },
    )["data"]["updateBodyOneReadyRequest"]["bodyOneReadyRequest"]

    return {
        "errors": failing_contracts,
        "status": "Done" if len(failing_contracts) == 0 else "Failed",
        "bodyOneReadyRequest": updated_borr,
    }
