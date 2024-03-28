import os
import requests
import res
from res.utils import logger
from res.media.images.providers import DxfFile

GRAPH_API = os.environ.get("GRAPH_API")
GRAPH_API_KEY = os.environ.get("GRAPH_API_KEY")
GRAPH_API_HEADERS = {
    "x-api-key": GRAPH_API_KEY,
    "apollographql-client-name": "argo",
    "apollographql-client-version": "split_pieces",
}

AWS_KEY = os.environ.get("AWS_KEY")
AWS_SECRET = os.environ.get("AWS_SECRET")

GET_ASSET_REQUEST = """
query getAssetRequest($id: ID!){
    assetRequest(id:$id){
        id
        style {
            body {
                number
                patternVersionNumber
            }
        }
        marker {
            rank
        }
    }
}
"""

UPDATE_ASSET_REQUEST_MUTATION = """
mutation updateAssetRequest($id:ID!, $input: UpdateAssetRequestInput!){
    updateAssetRequest(id:$id, input:$input){
        assetRequest {
            id
            numberOfPiecesInPattern
        }
    }
}
"""


def request(query, variables):
    response = requests.post(
        GRAPH_API,
        json={
            "query": query,
            "variables": variables,
        },
        headers=GRAPH_API_HEADERS,
    )
    body = response.json()
    errors = body.get("errors")

    if errors:
        error = errors[0]
        raise ValueError(error)

    return body["data"]


def compute_number_of_pieces_needed(event):
    s3 = res.connectors.load("s3")
    body_code = event.get("body_code").replace("-", "_").lower()
    version = event.get("body_version_code")
    piece_type = event.get("piece_type")
    uri = f"s3://meta-one-assets-prod/bodies/{body_code}/pattern_files/body_{body_code}_{version}_pattern.dxf"

    if not s3.exists(uri):
        raise FileNotFoundError(
            f"The S3 file {uri} does nto exist - check that the body exists and the version is correct"
        )

    s3_file = DxfFile(uri)

    total_pieces = 0

    if piece_type == "self":
        try:
            total_pieces += s3_file.get_piece_count("self")
        except Exception as e:
            logger.info("Couldn't find a tag 'self' on the available pieces.")
        try:
            total_pieces += s3_file.get_piece_count("block_fuse")
        except Exception as e:
            logger.info("Couldn't find a tag 'block_fuse' on the available pieces.")

    if piece_type == "combo":
        try:
            total_pieces += s3_file.get_piece_count("combo")
        except Exception as e:
            logger.info("Couldn't find a tag 'combo' on the available pieces.")
        try:
            total_pieces += s3_file.get_piece_count("lining")
        except Exception as e:
            logger.info("Couldn't find a tag 'lining' on the available pieces.")

    return total_pieces


def process_new_asset_request(event):
    record_id = event.get("record_id")
    asset_request = request(GET_ASSET_REQUEST, {"id": record_id})["assetRequest"]

    body_code = asset_request["style"]["body"]["number"]
    body_version = f'v{asset_request["style"]["body"]["patternVersionNumber"]}'

    piece_type = "self" if asset_request["marker"]["rank"] == "Primary" else "combo"

    total_pieces_needed = compute_number_of_pieces_needed(
        {
            "body_code": body_code,
            "body_version_code": body_version,
            "piece_type": piece_type,
        }
    )

    # print(total_pieces_needed)

    request(
        UPDATE_ASSET_REQUEST_MUTATION,
        {"id": record_id, "input": {"numberOfPiecesInPattern": total_pieces_needed}},
    )
