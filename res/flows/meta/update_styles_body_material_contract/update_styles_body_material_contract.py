import os
import requests
import arrow
from res.utils.logging.ResLogger import ResLogger

GRAPH_API = os.environ.get('GRAPH_API', 'http://localhost:3000/')
GRAPH_API_KEY = os.environ.get("GRAPH_API_KEY", '')
AWS_LAMBDA_FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", 'test')
GRAPH_API_HEADERS = {
    "x-api-key": GRAPH_API_KEY,
    "apollographql-client-name": "lambda",
    "apollographql-client-version": AWS_LAMBDA_FUNCTION_NAME,
}

GET_ALL_STYLES = """
query getStyles($where: StylesWhere $after: String){
    styles(first:100, where:$where, after:$after){
       styles {
            id
            body {
                code
            }
            material {
                code
            }
        }
        cursor
        hasMore
    }
}
"""

VALIDATE_BM_COMBINATION = """
mutation validateBodyMaterialCombination($styleId:ID!){
    validateBodyMaterialCombination(styleId:$styleId){
        style {
            id
            body {
                code
            }
            material {
                code
            }
            bodyMaterialContractGate
        }
    }
}
"""

GET_BODY = """
query body($number: String){
    body(number: $number){
        id
        code
		allSupportedMaterialTaxonomyGroupsLastChanged
        constructionOperationsSupportedMaterialTaxonomyGroupsLastChanged
    }
}
"""

UPDATE_BODY = """
mutation updateBody($number: String, $input: UpdateBodyInput!){
    updateBody(number:$number, input:$input){
        body {
            id
            number
            stylesBodyMaterialContractLastUpdatedAt
        }
    }
}
"""

logger = ResLogger()

def request(query, variables):
    response = requests.post(
        GRAPH_API,
        json={
            "query": query,
            "variables": variables,
        },
        headers=GRAPH_API_HEADERS,
    )
    response_body = response.json()

    errors = response_body.get("errors")
    if errors:
        error = errors[0]
        raise ValueError(error)

    return response_body['data']


def update_styles_body_material_contract_status(event, context=None):
    body_code = event.get('body_code')
    is_dry_running = event.get('is_dry_running')

    body = request(GET_BODY, {"number": body_code})['body']
    all_supported_material_taxonomy_groups_last_changed = arrow.get(body['allSupportedMaterialTaxonomyGroupsLastChanged'] or 0)
    construction_operations_supported_material_taxonomy_groups_last_changed = arrow.get(body['constructionOperationsSupportedMaterialTaxonomyGroupsLastChanged'] or 0)
    last_taxonomies_changed_at = (
        all_supported_material_taxonomy_groups_last_changed
        if all_supported_material_taxonomy_groups_last_changed > construction_operations_supported_material_taxonomy_groups_last_changed
        else construction_operations_supported_material_taxonomy_groups_last_changed
    )

    has_more = True
    after = None
    all_styles = []

    while has_more:
        query_response = request(GET_ALL_STYLES, {
            'where': {
                'bodyCode': {'is': body_code},
            },
            'after': after
        })

        has_more = query_response['styles']['hasMore']
        after = query_response['styles']['cursor']
        all_styles.extend(query_response['styles']['styles'])

    logger.info(f'Body: {body_code}')
    logger.info(f'Total styles: {len(all_styles)}')
    if not is_dry_running:
        i=1
        for style in all_styles:
            if style['body'] is None or style['material'] is None:
                logger.info(f"Style {style['id']} is missing material or body")
                continue

            r = request(VALIDATE_BM_COMBINATION, {
                'styleId': style['id']
            })
            style = r['validateBodyMaterialCombination']['style']
            logger.info(f'({i}/{len(all_styles)}) Style: {style}')
            i=i+1

        request(UPDATE_BODY, {
            'number': body_code,
            'input': {
                'stylesBodyMaterialContractLastUpdatedAt': str(last_taxonomies_changed_at.utcnow())
            }
        })

    else:
        logger.info(f"{all_styles}")


if __name__ == '__main__':
    # Test
    update_styles_body_material_contract_status({
        'body_code': 'CC-8084',
        'is_dry_running': False
    }, dict)
