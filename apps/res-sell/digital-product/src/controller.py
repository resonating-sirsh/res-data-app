import json
import os
import icontract

from res.utils import logger
from src.utils import get_matic_price, calculate_matic_to_usd
from src.connectors.query_definition import CHANGING_STATUS, SAVING_TRANSFER
from src.connectors.bases import ONEContractException
from src.connectors.web3_contracts import ONEContract, Web3Client
from src.connectors.graphql import (
    get_paths_from_assets,
    get_metadata_ipfs_url_from_style,
    save_more_data_request_record,
    get_all_created_digital_product_count,
)

from src.utils import DigitialProductRequestType, DigitalProductRequestStatus

MAX_NFT_PER_BRAND = 5


def pre_create_nft_digital_product(created_record, hasura_client):
    brand_code = created_record["brand_id"]
    count = get_all_created_digital_product_count(brand_code, hasura_client)
    if count > MAX_NFT_PER_BRAND:
        hasura_client.execute(
            CHANGING_STATUS,
            {
                "record_id": created_record["id"],
                "status": DigitalProductRequestStatus.ERROR,
                "reason": "Max NFT limit reach",
            },
        )
        return False
    return created_record["type"] == DigitialProductRequestType.CREATED


@icontract.require(pre_create_nft_digital_product)
def create_nft(created_record, hasura_client, graphql_client):
    request_id = created_record["id"]
    style_code = created_record["style_code"]
    thumbnail_id = created_record["thumbnail_id"]
    brand_id = created_record["brand_id"]
    animation_path = get_paths_from_assets(
        created_record["animation_id"], hasura_client
    )
    thumbnail_path = get_paths_from_assets(thumbnail_id, hasura_client)
    payload = {
        "animation_path": animation_path,
        "thumbnail_path": thumbnail_path,
        "style_code": style_code,
        "request_id": request_id,
        "brand_code": brand_id,
    }

    metadata_ipfs_url = get_metadata_ipfs_url_from_style(payload, graphql_client)
    try:
        client = Web3Client(
            node_url=os.environ.get("MORALIS_NODE_URL"),
            private_key=os.environ.get("WALLET_PRIVATE_KEY"),
        )
        one_contract = ONEContract(
            client=client,
            contract_address=os.getenv("CONTRACT_ADDRESS"),
            abi=json.loads(os.environ.get("CONTRACT_ABI", "")),
        )
        transaction_receipt = one_contract.mint_nft(metadata_ipfs_url, request_id)
        save_more_data_request_record(
            record_id=request_id,
            data={
                "nft_metadata_url": metadata_ipfs_url,
                "token_id": request_id,
                "contract_id": os.getenv("CONTRACT_ADDRESS"),
                "to": os.getenv("WALLET_ADDRESS"),
                "costing": get_costing(transaction_receipt, client),
            },
            hasura_client=hasura_client,
        )
        return {"response": "NFT created"}, 200
    except ONEContractException as err:
        logger.error("Error on ONE contract")
        hasura_client.execute(
            CHANGING_STATUS,
            {
                "record_id": request_id,
                "status": DigitalProductRequestStatus.ERROR,
                "reason": "Error trying to connect or call the smart contract",
            },
        )
        print(err)
        return {"error": "Error connecting with smart contract"}, 422


@icontract.require(
    lambda record_created: record_created["type"] == DigitialProductRequestType.TRANSFER
)
def transfer_nft(record_created, hasura_client):
    request_id = record_created["id"]
    token_id = record_created["token_id"]
    to_address = record_created["to_address"]
    client = Web3Client(
        node_url=os.environ.get("MORALIS_NODE_URL"),
        private_key=os.environ.get("WALLET_PRIVATE_KEY"),
    )
    try:
        one_contract = ONEContract(
            client=client,
            contract_address=os.getenv("CONTRACT_ADDRESS"),
            abi=json.loads(os.environ.get("CONTRACT_ABI", "")),
        )
        receipt = one_contract.transfer_nft(token_id, to_address)

        hasura_client.execute(
            SAVING_TRANSFER,
            {
                "id": request_id,
                "status": DigitalProductRequestStatus.DONE,
                "costing": get_costing(receipt, client),
            },
        )

        return {"success": True}, 200
    except ONEContractException:
        hasura_client.execute(
            CHANGING_STATUS,
            {
                "record_id": request_id,
                "status": DigitalProductRequestStatus.ERROR,
                "reason": "Error trying to connect or call the smart contract",
            },
        )
        return {"error": "Error connecting with smart contract"}, 422


def get_costing(transaction_receipt, client):
    logger.info(client)
    gas_consumed = transaction_receipt.cumulativeGasUsed
    # gas_price = transaction_receipt.effectiveGasPrice
    # matic_fee = client.to_eth(gas_consumed)
    matic_to_usd_rate = get_matic_price()
    # gas_price_in_matic = client.to_eth(gas_price)
    # logger.debug({gas_price, gas_price, matic_fee, matic_to_usd_rate})
    costing = {
        "gas_consumed": gas_consumed,
        "matic_to_usd_rate": get_matic_price(),
        "total_in_usd": calculate_matic_to_usd(
            gas_amount=gas_consumed, matic_to_usd_rate=matic_to_usd_rate
        )
        # "gas_fee": int(gas_consumed),
        # "gas_in_matic": float(matic_fee),
        # "gas_price_in_matic": gas_price_in_matic,
        # "gas_price_in_eth": float(gas_price),
        # "matic_to_used_rate": float(matic_to_usd_rate),
        # "total_value_usd": calculate_matic_to_usd(float(matic_fee), matic_to_usd_rate),
    }
    logger.info(costing)
    return costing
