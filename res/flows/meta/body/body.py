from res.utils.logging import logger
from schemas.pydantic.body import CreateBodyInput
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import json
import res.flows.meta.body.graphql_queries as graphql_queries


def create_body(input: CreateBodyInput):
    gql = ResGraphQLClient()

    logger.info(f"create_body input: {json.dumps(str(dict(input)),indent=2)}")

    create_body_response = gql.query(
        graphql_queries.CREATE_BODY,
        variables={
            "input": {
                "name": input.name,
                "brandCode": input.brand_code,
                "categoryId": input.category_id,
                "coverImagesFilesIds": input.cover_images_files_ids,
                "campaignsIds": input.campaigns_ids,
                "onboardingMaterialsIds": input.onboarding_materials_ids,
                "onboardingComboMaterialIds": input.onboarding_combo_material_ids,
                "onboardingLiningMaterialsIds": input.onboarding_lining_materials_ids,
                "fitAvatarsIds": input.fit_avatars_ids,
                "sizeScalesIds": input.size_scales_ids,
                "basePatternSizeId": input.base_pattern_size_id,
                "createdByEmail": input.created_by_email,
                "onboardingFlowType": (
                    input.onboarding_flow_type.name
                    if input.onboarding_flow_type
                    else None
                ),
                "bodyOnboardingRequestId": input.body_onboarding_request_id,
            }
        },
    )

    logger.info(f"create_body_response={json.dumps(create_body_response,indent=2)}")
    if create_body_response.get("errors"):
        raise Exception(create_body_response.get("errors"))

    return create_body_response["data"]["createBody"]
