from res.flows.api.core.node import FlowApiNode
from schemas.pydantic.sell import OrderItem
import res
import traceback
from res.connectors.airtable.AirtableConnector import AIRTABLE_ONE_PLATFORM_BASE

AIRTABLE_QUEUE_BASE_ID = AIRTABLE_ONE_PLATFORM_BASE


REQUEST_TOPIC = "res_sell.orders.item_update_requests"


def generator(event, context={}):
    with res.flows.FlowContext(event, context) as fc:
        return {}


def reducer(event, context={}):
    return {}


@res.flows.flow_node_attributes(
    memory="10Gi",
)
def handler(e, c={}):
    """ """
    from res.utils import ignore_warnings

    ignore_warnings()

    with res.flows.FlowContext(e, c) as fc:
        # 1. these are serialized into schema objects including decompression if needed
        for asset in fc.decompressed_assets:
            res.utils.logger.debug(asset)
            # 2. this handles status transactions - a well behaved node will fail gracefully anyway. Here we also manage some casting to a strong type
            with fc.asset_processing_context(asset) as ac:
                res.utils.logger.debug(f"Processing asset of type {type(ac.asset)}")
                # lesson: notice the response type, we receive data on the request but we are always sending updates using the response object
                node = OrderItemNode(
                    ac.asset, response_type=OrderItem, queue_context=fc
                )
                # res.utils.logger.debug(
                #     f"saving asset @ {node} - will update queue for sample size: {ac.asset.is_sample_size}"
                # )

                # experimental queue update logic: relay to kafka and airtable and as its a relay dont try to save anything to hasura which we will do in the reducer for this node
                _ = node._save(queue_update_on_save=ac.asset.is_sample_size)
    return {}


class OrderItemNode(FlowApiNode):
    # we handle these types
    atype = OrderItem
    # when in doubt, this is our unique node id
    uri = "one.platform.sell.orderItem"

    def __init__(self, asset, response_type=None, queue_context=None):
        super().__init__(
            asset,
            response_type=response_type,
            queue_context=queue_context,
            base_id=AIRTABLE_QUEUE_BASE_ID,
        )
        self._name = OrderItemNode.uri

    def _save(
        self,
        typed_record: OrderItem,
        sku_override=None,
        hasura=None,
        plan=False,
        on_error=None,
    ):
        try:
            return None
        except:
            res.utils.logger.warn(
                f"Failing in the body node posting status - error is {traceback.format_exc()}"
            )

            d = typed_record.dict()
            d["contracts_failed"] = d["contracts_failed"] + ["ORDER_ITEM_ISSUE"]
            if len(d["contracts_failed"]) > 0:
                d["status"] = "Failed"
            return OrderItem(
                **d,
                trace_log=traceback.format_exc(),
            )
        return None
