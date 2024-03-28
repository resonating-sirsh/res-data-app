# payload structure, assets
from . import FlowContext

ONE_STORE = ""
PROD_REQUEST = ""
ALLOCATION_REQUEST = ""
RECEIVED_ONE_ORDER_TOPIC = ""


def _to_stored_allocate(collection):
    """
    Create an allocation record where we take existing ONEs and satisfy an order with them
    Create an entry so the item becomes inavailable
    """
    pass


def _to_production_request(collection):
    """
    Create a production request entry
    """
    pass


def _to_received_order_event(collection):
    """
    Create a production request entry
    """
    pass


def produce_assets(event, context):
    assets = event.get("args", {}).get("assets")
    with FlowContext(event, context) as fc:
        dgraph = fc.connectors["dgraph"]
        kafka = fc.connectors["kafka"]

        # one way
        stored_ones = dgraph[ONE_STORE].get_dataframe(keys=assets["style_key"])
        stored_ones = assets.join(stored_ones)
        stored_ones = _to_stored_allocate(stored_ones)
        dgraph[ALLOCATION_REQUEST].update_dataframe(stored_ones)
        not_stored_ones = assets[assets["key"].notin(stored_ones["key"])]
        not_stored_ones = _to_production_request(not_stored_ones)
        dgraph[ONE_STORE].update_dataframe(not_stored_ones)

        # if the logic that each node handles specific things we must route here ?
        orders = _to_received_order_event(stored_ones)
        kafka[RECEIVED_ONE_ORDER_TOPIC].publish(orders)
        orders = _to_received_order_event(not_stored_ones)
        kafka[RECEIVED_ONE_ORDER_TOPIC].publish(orders)
        # at this point we have saved the state of allocations and prod requests
        # and the event is on the queue for downstream consumers in make

        # strategy two -> adorn with things like is_in_store
        # orders = assign_flow(order)
        # for flow, set in assets.groupby('flow):
        #  fc._relay(set, flow) #each node has a persistence in dgraph and a topic. we stream into the node and it persists and routes to other nodes
        # fc.forward(orders)
