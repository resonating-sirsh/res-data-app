import datetime
import icontract

from res.utils import logger
from res.connectors.graphql.hasura import Client
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from src.utils import DigitalProductRequestStatus
from src.connectors.query_definition import (
    GET_COUNT_DIGITAL_PRODUCT_CREATED,
    GET_ASSET_URL,
    GET_IPFS_URL_METADATA,
    GET_DUPLICATE_REQUEST,
    UPDATE_DIGITAL_PRODUCT_REQUEST,
)


def get_paths_from_assets(asset_id: str, hasura_client: Client) -> str:
    logger.debug(asset_id)
    data = hasura_client.execute(GET_ASSET_URL, {"id": asset_id})
    logger.debug(data)
    if data["create_asset_by_pk"] is None:
        logger.warn("Record not found")
        raise Exception("Record not found")
    return data["create_asset_by_pk"]["path"]


def get_metadata_ipfs_url_from_style(
    payload: dict, graphql_client: ResGraphQLClient
) -> str:
    data = graphql_client.query(
        GET_IPFS_URL_METADATA,
        {
            "style_code": payload["style_code"],
            "animation_path": payload["animation_path"],
            "thumbnail_path": payload["thumbnail_path"],
        },
    )
    logger.debug(data)
    if data.get("data") is None:
        logger.error("error getting metdata ipfs url from style")
        logger.warn(data)
        raise Exception("Something fail trying to create asset in ipfs")
    return data["data"]["createIpfsMetadata"]["metadataFile"]


def post_condition_save_data(result, record_id, data):
    return (
        result["id"] == record_id
        and result["ipfs_location"] == data["nft_metadata_url"]
        and result["costing"] is not None
    )


@icontract.ensure(post_condition_save_data)
def save_more_data_request_record(record_id: str, data, hasura_client: Client):
    logger.debug(f"updating record id: {record_id}, {data}")
    response = hasura_client.execute(
        UPDATE_DIGITAL_PRODUCT_REQUEST,
        {
            "id": record_id,
            "transfered_at": datetime.datetime.utcnow().isoformat(),
            "ipfs_location": data["nft_metadata_url"],
            "costing": data["costing"],
            "status": DigitalProductRequestStatus.DONE,
            "token_id": str(data.get("token_id", "")),
            "contract_address": data["contract_id"],
        },
    )
    return response["update_digital_product_digital_product_request_by_pk"]


def validate_not_duplicate_product_request(
    brand_id: str, type: str, animation_id: str, hasura_client: Client
):
    payload = {
        "brand_id": brand_id,
        "type": type,
        "animation_id": animation_id,
        "status": [
            DigitalProductRequestStatus.HOLD,
            DigitalProductRequestStatus.PROCESSING,
            DigitalProductRequestStatus.DONE,
        ],
    }
    logger.debug(payload)

    response = hasura_client.execute(GET_DUPLICATE_REQUEST, payload)

    if "digital_product_digital_product_request" not in response:
        return False

    data = response["digital_product_digital_product_request"]

    logger.info(f"amount of request {len(data)}")

    return True if len(data) <= 1 else False


def get_all_created_digital_product_count(brand_code: str, hasura_client: Client):
    response = hasura_client.execute(
        GET_COUNT_DIGITAL_PRODUCT_CREATED, {"brand_id": brand_code}
    )

    if "digital_product_digital_product_request" not in response:
        raise Exception("Error pulling data from hasura")

    data = response["digital_product_digital_product_request"]

    logger.info(f"len_data: {len(data)}")
    return len(data)
