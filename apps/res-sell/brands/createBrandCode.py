from flask import Flask, abort, request, make_response, Response
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
import json

app = Flask(__name__)

PROCESS_NAME = "brands"

GET_BRAND = """
    query ($id: ID!) {
        brand(id: $id) {
            id
            code
            name
        }
    }
"""

GET_BRAND_BY_CODE = """
    query ($code: String!) {
        brand(code: $code) {
            id
            code
            name
        }
    }
"""

UPDATE_BRAND_CODE = """
    mutation updateBrand($id: ID!, $input: UpdateBrandInput!) {
        updateBrand(id: $id, input: $input){
            brand{
                id
                code
                name
            }

        }
    }
"""

SYNC_BRAND = """
    mutation syncBrandsWithMeta($id: ID!) {
        syncBrandsWithMeta(id: $id){
            brand{
                id
                code
                name
            }

        }
    }
"""


def updateBrand(graphql_client, brand_id, brand_code):
    graphql_client.query(
        UPDATE_BRAND_CODE,
        {
            "id": brand_id,
            "input": {
                "code": brand_code,
            },
        },
    )

    graphql_client.query(
        SYNC_BRAND,
        {
            "id": brand_id,
        },
    )

    return make_response(json.dumps({"code": brand_code, "error": False}), 200)


@app.route("/brands/brandcode/create", methods=["POST"])
def create_brandcode():
    data = json.loads(request.data)
    brand_id = data["brandId"]

    graphql_client = ResGraphQLClient(PROCESS_NAME)

    response = graphql_client.query(GET_BRAND, {"id": brand_id})
    brand = response["data"]["brand"]

    brand_name = brand["name"]
    # Delete spaces in brandName
    brand_name = brand_name.replace(" ", "")
    alpha_combination_found = False
    for i in range(len(brand_name)):
        leftChar = brand_name[i]
        rightChar = brand_name[len(brand_name) - i - 1]
        code = leftChar + rightChar
        code = code.upper()

        query_brand_code = graphql_client.query(GET_BRAND_BY_CODE, {"code": code})

        if not query_brand_code["data"]["brand"]:
            alpha_combination_found = True
            return updateBrand(graphql_client, brand_id, code)

    if not alpha_combination_found:
        for i in range(len(brand_name)):
            leftChar = brand_name[i]
            code = leftChar.upper()

            for j in range(10):
                code = code + str(j)
                query_brand_code = graphql_client.query(
                    GET_BRAND_BY_CODE, {"code": code}
                )
                if not query_brand_code["data"]["brand"]:
                    return updateBrand(graphql_client, brand_id, code)
                else:
                    code = leftChar.upper()

    return make_response(json.dumps({"error": True}), 200)


if __name__ == "__main__":
    app.run(host="0.0.0.0")
