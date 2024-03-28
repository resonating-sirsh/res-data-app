# import os
# os.environ['KAFKA_SCHEMA_REGISTRY_URL'] = "localhost:8001"
# os.environ['KAFKA_KGATEWAY_URL'] = 'https://data.resmagic.io/kgateway/submitevent'

from schemas.pydantic.sell import OrderItem
from res.flows.api import FlowAPI
from res.connectors.airtable.AirtableUpdateQueue import AirtableQueue

# from res.connectors.airtable import AirtableConnector
from res.flows.sell.orders.item_node import OrderItemNode

# # from res.flows.meta.ONE.meta_one import BodyMetaOne
# from warnings import filterwarnings
import res
import json

# from warnings import filterwarnings

# from res.flows.meta.body.unpack_asset_bundle.body_db_sync import sync_body_to_db_by_versioned_body
kafka = res.connectors.load("kafka")
# http://localhost:8000/#/cluster/default/schema/res_meta.dxa.style_pieces_update_requests-value/version/latest
# filterwarnings('ignore')
airtable = res.connectors.load("airtable")


def create_item_from_json(data):
    data = json.load(f)
    return OrderItem(**data)


def set_fulfillment_status(f):
    # d = {
    #     "revenue_share": None,
    #     "brand_priority": None,
    #     "brand_balance_at_order": None,
    #     "current_body_version": None,
    #     "current_meta_one_ref": None,
    # }

    # for k, v in order_row.items():
    #     if "shipping" in k:
    #         d[snakecase(k)] = v
    # return d
    # orderItem = OrderItem(quantity=1, sku="A B C D E", source_order_item_id=124)
    item_API = FlowAPI(OrderItem)
    # API
    # o
    orderItem = create_item_from_json(f)

    # m.update_forward_refs()

    res.utils.logger.info(orderItem.dict())
    try:
        item_API.update(orderItem)
    except Exception as error:
        res.utils.logger.error("Error on flow API update.", error)

    return orderItem
