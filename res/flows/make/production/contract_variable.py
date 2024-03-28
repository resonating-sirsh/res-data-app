from datetime import datetime
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient


def register_contract_variables(
    production_request_id,
):
    """
    once a "Contract Variable" is tagged on a production request, this endpoint will serve to inspect the contract variables and trigger any action that may be needed,
    e.g. invalidate the meta.one, create apply color requets, etc. - registering the contract violations.


    we can and should simplify this fn but expediting for v1
    """
    from res.utils import logger

    logger.info(
        f"make-one/register_contract_variables record_id={production_request_id}"
    )
    error = None
    update_mopr_payload = {}
    try:
        gql = ResGraphQLClient()

        mopr = gql.query(
            """
                query makeOneProductionRequest($id: ID!) {
                makeOneProductionRequest(id: $id) {
                    id
                    createdAt
                    type
                    contractVariables {
                    id
                    code
                    }
                    contractVariablesRegistered {
                    id
                    code
                    }
                    contractVariablesFixed {
                    id
                    code
                    }
                    style {
                        id
                        code
                    }
                }
            }
            """,
            {"id": production_request_id},
        )["data"]["makeOneProductionRequest"]

        enforced_contract = "PLACEMENT_MISMATCH"  # for now it's only one enforced contract.. we'll ignore others

        mopr_cv = [
            c["code"]
            for c in mopr["contractVariables"]
            if c is not None and c["code"] is not None
        ]
        cv_registered = [
            c["code"]
            for c in mopr["contractVariablesRegistered"]
            if c is not None and c["code"] is not None
        ]

        cv_fixed = [
            c["code"]
            for c in mopr["contractVariablesFixed"]
            if c is not None and c["code"] is not None
        ]

        should_register_cv = (enforced_contract in mopr_cv) and (
            enforced_contract not in cv_registered and enforced_contract not in cv_fixed
        )
        logger.info(f"should register contracts: {should_register_cv}")
        if should_register_cv:
            # for now all make-enforced contracts are style-related
            acr = gql.query(
                """
                query applyColorRequests($where: ApplyColorRequestsWhere! $first:Int! $sort:[ApplyColorRequestsSort!]) {
                    applyColorRequests(where: $where, sort:$sort first: $first) {
                        applyColorRequests {
                        id
                        requestReasonsContractVariables{
                            id
                            code
                            }
                        }
                    }
                }
                """,
                {
                    "where": {
                        "styleId": {"is": mopr["style"]["id"]},
                        "isOpen": {"is": True},
                    },
                    "first": 1,
                    "sort": [{"field": "CREATED_AT", "direction": "DESCENDING"}],
                },
            )["data"]["applyColorRequests"]["applyColorRequests"]

            acr = acr[0] if len(acr) > 0 else None

            from res.flows.meta.ONE.contract import ContractVariables

            enforced_contract_dxa_one_markers = ContractVariables.load_by_codes(
                [enforced_contract], "appqtN4USHTmyC6Dv", "tbl4s194nIMut9xoA"
            )
            enforced_contract_dxa_one_markers_id = [
                cv["id"] for cv in enforced_contract_dxa_one_markers if cv
            ]
            logger.info(f"Apply color request exists?: {bool(acr)}")

            create_apply_color_request_payload = {
                "styleId": mopr["style"]["id"],
                "assigneeEmail": "ekim@resonance.nyc",
                "hasOpenOnes": True,
                "requestsIds": [mopr["id"]],
                "requestType": mopr["type"],
                "originalRequestPlacedAt": mopr["createdAt"],
                "requesterEmail": "techpirates@resonance.nyc",
                "targetCompleteDate": datetime.utcnow().strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
                "requestReasonsContractVariablesIds": enforced_contract_dxa_one_markers_id,
            }
            # logger.info(create_apply_color_request_payload)
            if not acr:
                new_apply_color_request = gql.query(
                    """
                    mutation createApplyColorRequest($input: CreateApplyColorRequestInput!){
                        createApplyColorRequest(input:$input){
                            applyColorRequest {
                                id
                            }
                        }
                    }
                    """,
                    {"input": create_apply_color_request_payload},
                )["data"]["createApplyColorRequest"]["applyColorRequest"]
                logger.info(f"createApplyColorRequest: {new_apply_color_request}")
            else:
                acr_request_reasons_contract_variables = (
                    acr["requestReasonsContractVariables"]
                    if acr["requestReasonsContractVariables"]
                    else []
                )
                acr_cv = [
                    cv["code"]
                    for cv in acr_request_reasons_contract_variables
                    if cv is not None and cv["code"] is not None
                ]

                # if ACR is not accountable for the enforced contract variable, reproduce it and add the contract variable to it
                if enforced_contract not in acr_cv:
                    reproduce_apply_color_request_payload = {
                        "requestReasonsContractVariablesIds": enforced_contract_dxa_one_markers_id,
                        "reproduceReasons": ["Placement Mismatch"],
                        "cancellerEmail": "techpirates@resonance.nyc",
                        "reproduceDetails": "Placement Mismatch. Request created automatically.",
                        "assigneeEmail": "ekim@resonance.nyc",
                        "targetCompleteDate": datetime.utcnow().strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),
                    }
                    logger.info(reproduce_apply_color_request_payload)
                    # NOTE: here we should also add the target_complete_date and assignee email.. should update the graph
                    reproduced_acr = gql.query(
                        """
                                mutation reproduceApplyColorRequest($id: ID! $input: ReproduceApplyColorRequestInput!) {
                                reproduceApplyColorRequest(id: $id, input: $input) {
                                    applyColorRequest {
                                    id
                                        }
                                    }
                                }
                            """,
                        {
                            "id": acr["id"],
                            "input": reproduce_apply_color_request_payload,
                        },
                    )["data"]["reproduceApplyColorRequest"]["applyColorRequest"]

                    logger.info(f"reproduceApplyColorRequest: {reproduced_acr}")
                else:
                    # this won't re-create the request - it will just make sure to append the mopr["id"] to the existing ACR
                    new_apply_color_request = gql.query(
                        """
                        mutation createApplyColorRequest($input: CreateApplyColorRequestInput!){
                            createApplyColorRequest(input:$input){
                                applyColorRequest {
                                    id
                                }
                            }
                        }
                    """,
                        {"input": create_apply_color_request_payload},
                    )["data"]["createApplyColorRequest"]["applyColorRequest"]

            # TODO: here we should invalidate the m1 by ingesting the failing contracts. need to test that - in the meantime we'll do this workaround to consider the style as invalid (isStyle3dOnboarded=false)
            gql.query(
                """    
                mutation invalidateStyleMetaOnes($id: ID!) {
                    invalidateStyleMetaOnes(id: $id) {
                        style {
                            id
                            isStyle3dOnboarded
                        }
                    }
                }
                """,
                {"id": mopr["style"]["id"]},
            )["data"]["invalidateStyleMetaOnes"]["style"]
            enforced_contract_mop_base = ContractVariables.load_by_codes(
                [enforced_contract], "appH5S4hIuz99tAjm", "tbllZTfwC1yMLWQat"
            )
            enforced_contract_mop = [
                cv["id"] for cv in enforced_contract_mop_base if cv
            ]
            update_mopr_payload = {
                "contractVariablesLastRegisteredAt": datetime.utcnow().strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "registerContractVariablesStatus": "Done" if not error else "Failed",
                "contractVariablesIdsRegistered": (
                    enforced_contract_mop if not error else []
                ),
                "registerContractVariablesLogs": error,
            }
    except Exception as e:
        import traceback

        error = f"An error occurred: {traceback.format_exc}"
        logger.info(f"{e} {error}")
        update_mopr_payload = {
            "registerContractVariablesStatus": "Done" if not error else "Failed",
            "registerContractVariablesLogs": error,
        }

    if update_mopr_payload:
        gql.query(
            """
            mutation updateProductionRequest($id: ID!, $input: UpdateMakeOneProductionRequestInput!){
                updateProductionRequest(id:$id, input:$input){
                    makeOneProductionRequest {
                        id
                    }
                }
            }
            """,
            {
                "id": production_request_id,
                "input": update_mopr_payload,
            },
        )

    return update_mopr_payload


def add_contract_variables_fixed(
    production_request_ids: list[str],
    contract_variables_codes_fixed: list[str],
):
    from res.utils import logger

    try:
        gql = ResGraphQLClient()

        logger.info(
            f"make-one/unblock_requests_contract_variables_fixed record_id={production_request_ids} contract_variables_fixed_codes={contract_variables_codes_fixed}"
        )

        for r in production_request_ids:
            mopr = gql.query(
                """                
                query makeOneProductionRequest($id: ID!) {
                    makeOneProductionRequest(id: $id) {
                        id
                        currentMakeNode
                        contractVariablesFixed {
                            id
                            code
                        }
                    }
                }
                """,
                {"id": r},
            )["data"]["makeOneProductionRequest"]

            # if the request is sitting in dxa graphics, the MOPR doesn't need to get registered as 'fixed', it is being made from the beginning
            if mopr["currentMakeNode"] != "DxA Graphics":
                # parse
                from res.flows.meta.ONE.contract import ContractVariables

                fixed_contracts_mop_base = ContractVariables.load_by_codes(
                    contract_variables_codes_fixed,
                    "appH5S4hIuz99tAjm",
                    "tbllZTfwC1yMLWQat",
                )
                cv_ids_fixed = [cv["id"] for cv in fixed_contracts_mop_base if cv]

                # merge with previous fixed cvs
                mopr_cv_fixed = (
                    [
                        cv["id"]
                        for cv in mopr["contractVariablesFixed"]
                        if cv is not None
                    ]
                    if mopr["contractVariablesFixed"]
                    else []
                )
                updated_mopr_cv_fixed = list(set(mopr_cv_fixed + cv_ids_fixed))

                gql.query(
                    """
                    mutation updateProductionRequest($id: ID!, $input: UpdateMakeOneProductionRequestInput!){
                        updateProductionRequest(id:$id, input:$input){
                            makeOneProductionRequest {
                                id
                            }
                        }
                    }
                    """,
                    {
                        "id": r,
                        "input": {
                            "contractVariablesIdsFixed": updated_mopr_cv_fixed,
                            "contractVariablesLastFixedAt": datetime.utcnow().strftime(
                                "%Y-%m-%dT%H:%M:%S.%fZ"
                            ),
                        },
                    },
                )
    except Exception as e:
        import traceback

        error = f"An error occurred: {e} {traceback.format_exc()}"
        logger.info(error)
        # TODO: Ping Slack

    return {}
