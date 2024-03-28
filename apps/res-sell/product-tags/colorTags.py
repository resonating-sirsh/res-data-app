from flask import Flask, abort, request, make_response, Response
from res.media.images.colorExtraction import extract_colors
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.utils.logging.ResLogger import ResLogger

app = Flask(__name__)

PROCESS_NAME = 'product-tags'

GET_PRODUCT_TAG = """
    query productTag($where: ProductTagsWhere) {
        productTags(first: 1, where: $where) {
            productTags{
                id
                code
                name
            }
        }
    }
"""

CREATE_PRODUCT_TAG_MUTATION = """
      mutation createProductTag($input: CreateProductTagInput!) {
           createProductTag(input: $input) {
               productTag {
                   id
                   code
               }
           }
      }
"""


UPDATE_STYLE_MUTATION = """
    mutation updateStyle($id: ID!, $input: UpdateStyleInput!){
        updateStyle(id:$id, input:$input){
            style {
                id
            }
        }
    }
"""

GET_STYLE_QUERY = """
query getStyle($id: ID!) {
    style(id: $id){
        id
        ecommerceColorTags
    } 
}
"""

@app.route("/product-tags/colors", methods=["POST"])
def get_color_tags():
    data = request.data
    # print(data)

    graphql_client = ResGraphQLClient(
        PROCESS_NAME
    )

    logger = ResLogger()


    # data = {
    #     "style_id": "rec1DWr17aFEglmab"
    # }
    
    style_id = data["style_id"]

    logger.debug("Processing Style ID: {}".format(style_id))


    response = graphql_client.query(GET_STYLE_QUERY, {
        "id": data["style_id"]
    })

    style = response["data"]["style"]
    
    payload = {
        "s3_bucket": "res-data-platform",
        "s3_key": f'flows/v0/dxa.process_images/dev/thumbnails/dpi_25/{style_id}/thumbnail.png',
        "dominant_colors_count": 1,
        "color_format": "name"
    }

    dominant_colors = extract_colors(payload)

    tags_ids = []
    for color in dominant_colors["palette_colors"]:
        # Get or create Product Tag

        response = graphql_client.query(GET_PRODUCT_TAG, {
            "where": {"type": {"is": "Color"}, "category": {"is": "Primary"}, "name": {"is": color}, "version": {"is": 2.0}}
        })

        if len(response.get("data").get("productTags").get("productTags")) > 0:
            product_tag = response.get("data").get("productTags").get("productTags")[0]
        else:
            response = graphql_client.query(CREATE_PRODUCT_TAG_MUTATION, {
                "input": {
                    "name": color,
                    "type": "Color",
                    "category": "Primary",
                    "isResonanceDefined": True,
                    "isEcommerceTag": True,
                    "ecommerceCategory": "Color",
                    "taxonomy": "Color Taxonomy",
                    "taxonomyLevel": 1,
                    "version": 2.0
                }
            })
            product_tag = response.get("data").get("createProductTag").get("productTag")
        tags_ids.append(str(product_tag.get("name")))

    # Remove keeping current tags and removing duplicated ones
    tags_string = ','.join(tags_ids)
    color_tags = f"{tags_string},{style.get('ecommerceColorTags')}" if style.get('ecommerceColorTags') else tags_string
    
    logger.debug("Color tags: {}".format(color_tags))
    
    graphql_client.query(UPDATE_STYLE_MUTATION, {
        "id": style_id,
        "input": {
            "ecommerceColorTags": ','.join(set(color_tags.split(',')))
        }
    })

    return make_response("Success!", 200)

if __name__ == "__main__":
    # get_color_tags()
    app.run(host="0.0.0.0")