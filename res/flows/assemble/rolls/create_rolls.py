import os
import res
import json
from res.utils import logger
from schemas.pydantic.make import RollsRequest
from res.flows.api import FlowAPI
from warnings import filterwarnings

os.environ["KAFKA_SCHEMA_REGISTRY_URL"] = "localhost:8001"
os.environ["KAFKA_KGATEWAY_URL"] = "https://datadev.resmagic.io/kgateway/submitevent"
os.environ["RES_ENV"] = "development"
os.environ["HASURA_ENDPOINT"] = "https://hasura-dev.resmagic.io/v1/graphql"

kafka = res.connectors.load("kafka")
filterwarnings("ignore")


def handle_new_event(event):
    logger.info(f"event -> {event}.")
    logger.info("(1/2) Create new roll")
    roll = RollsRequest(**event)
    logger.info(f"roll -> {roll.dict()}")
    api = FlowAPI(RollsRequest, response_type=RollsRequest, base_id="appKwfiVbkdJGEqTm")
    api_response = api.update(roll, plan_response=False)

    logger.info(f"roll created: {roll}")

    return api_response


if __name__ == "__main__":
    data = {
        "roll_type": "Parent",
        "roll_length": 44,
        "roll_key": "R40364__CTW70_IN_44",
        "material_name_match": "CTW70",
        "roll_id": 40364,
        "material": "recufP4asmM7SkMMg",
        "purchasing_record_id": "recG94k8u14CuRiGx",
        "_warehouse_rack_location": "null",
        "location_id": "reczQP4WKOEK1v2rx",
        "po_number": "PO-4840",
        "standard_cost_per_unit": 2.4,
    }

    handle_new_event(data)
