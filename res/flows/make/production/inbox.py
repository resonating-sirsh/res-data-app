"""
We are replacing the bots that make production and print requests from orders with this one

one_order_requests / from res.sell.one_orders 
flows.sell.orders.process is the interface logic for mapping sell to make. 
We send clean orders with finance, customer and brand facing context and turn in into a Make order which has a meta one etc.

this order collector actually subscribes to multiple sources - there are multiple listeners in the apps for these
- shopify listener -> rewrites onto one orders effectively so we listen to that
- res-connect sends onto the one orders
- so then we listen to the processed one orders and process everything there
- finally we listen to the healing - for healing the generator groups assets (or can)


we always explain why we do in the metadata of the asset
- if we are supplying from inventory, we do not DO anything as the inventory manager will but we do communicate that are decision to not produce was based on that assumption
"""

import json
import traceback

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from schemas.pydantic.make import *

import res
from res.flows.api import FlowAPI
from res.flows.api.core.node import NodeStates
from res.flows.dxa.styles import queries
from res.flows.make.production.queries import increment_instance_for_group_at_time
from res.flows.meta.ONE import StyleNotRegisteredContractException

REQUEST_TOPIC_CREATE = "res_make.orders.one_requests"
REQUEST_TOPIC_INSPECTIONS = ""


def _flow_2d_auto(asset):
    pass


def _flow_2d_await(asset):
    pass


def _flow_3d_auto(asset):
    pass


def _flow_3d_await(asset):
    pass


def notify_production_requests(asset, demand_delta=1, fc=None):
    """
    the demand can be multiple of 0 if supplying from inventor

    here we essentially write to other kafka queues
    """

    asset = _notify_print_requests(asset, demand_delta=demand_delta)

    return {}


def _notify_print_requests(asset, demand_delta=1, fc=None):
    """
    the demand can be multiple of 0 if supplying from inventory
    """

    return {}


def send_response(asset, demand_delta, fc=None):
    pass


def determine_demand(asset, fc=None):
    """
    There may be 0 demand for this asset or we may want to make multiple.
    """
    return 1


def generator(event, context=None):
    return {}


# move these to meta one
def get_meta_asset(asset):
    return {}


def make_asset_request(asset):
    return {}


# ########################
@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def reconcile_order_for_sku(
    order_number, sku, one_numbers=None, plan=False, known_cancellations=None
):
    """
    A workflow to fully reconcile one orders with what know are required one-numbers
    The one order is the piece tracking object and we use this to assign ONE numbers to those one orders
    Its slightly complicated by the fact that we cancel things and its hard to track across all systems
    In future we will stop cancelling ONEs making this less of a concern

        if you dont pass the one number it will be looked up

    Example

    reconcile_order_for_sku('KT-30267', 'KT-2011 LTCSL OCTCLW 2ZZSM', one_numbers=None, plan=False)

    """
    from res.flows.make.production.queries import (
        cancel_one_order_by_ids,
        get_existing_one_orders,
        get_order_map_from_bridge_table,
    )
    from res.flows.sell.orders.FulfillmentNode import (
        FulfillmentException,
        FulfillmentNode,
    )
    from res.flows.sell.orders.queries import reimport_order

    if one_numbers is None:
        res.utils.logger.debug(
            f"As the one numbers were not specified we will look them up for {order_number=}, {sku=}"
        )
        one_numbers = get_order_map_from_bridge_table(order_number).get(sku)
        res.utils.logger.debug(f"We found {one_numbers=} for the order for this sku")

    try:
        # we will return existing even if they are cancelled so we can not recover
        # a one number that is cancelled unless we use the hint that we know its cancelled
        # this is an edge case that requires more engineering for example we need to put the cancelled things and their ids somewhere
        existing = get_existing_one_orders(order_number, sku)
        existing_assignments = {
            f["one_number"]: f["id"]
            for f in existing
            if f["one_number"] and f["one_number"] not in (known_cancellations or [])
        }

        res.utils.logger.info(f"We currently have {existing_assignments=}")
        pending_one_numbers = [
            p for p in one_numbers if p not in existing_assignments.keys()
        ]
        not_valid_one_number = [
            f["id"] for f in existing if f["one_number"] not in one_numbers
        ]

        if len(not_valid_one_number):
            res.utils.logger.warn(
                f"Cancellations required! We currently have ids for {not_valid_one_number=}"
            )
            cancel_one_order_by_ids(not_valid_one_number)

        res.utils.logger.info(
            f"The following known one numbers are still pending {pending_one_numbers}. Generating some requests"
        )

        """
        this is used to generate the payload for the ONE
        its assumes at this point we have a style mapped to META.ONE or this can fail
        the sku filter is useful in this context 
        """
        try:
            requests = FulfillmentNode.get_make_order_request_for_order(
                order_number, sku_filter=sku, raise_failed_track=True
            )
        except FulfillmentException as ex:
            # poor mans typing for now
            metric = res.utils.logger.metric_exception_log_incr(
                "sell_order_missing", "fulfillment_node"
            )

            res.utils.logger.warn(
                f"We failed to register - going to try to reimport the sell order nad try again - {metric=}"
            )
            reimport_order(order_number)
            requests = FulfillmentNode.get_make_order_request_for_order(
                order_number, sku_filter=sku, raise_failed_track=True
            )
        except Exception as ex:
            res.utils.logger.metric_exception_log_incr(
                "make_order_failed", "fulfillment_node"
            )
            raise

        """
        this is important part of making sure we dont have collisions
        we are only trying to save ONEs we dont have in our database already 
        and we are using only free requests that are not pinned to open ONEs
        for example the customer may have ordered 3 skus which means we can only make three 
        unique requests linked to known fulfillment items!!
        """
        free_requests = [
            r for r in requests if r["id"] not in existing_assignments.values()
        ]

        num_free_requests = len(free_requests)

        res.utils.logger.info(
            f"<< There are {num_free_requests=} from total-for-sku={len(requests)} and {pending_one_numbers=} - we will make as many assignments as we need to and can >>"
        )

        """
        this assigns one numbers to new or existing requests before upserting later
        we cannot assign  one number that is already in use based on above logic
        """
        requests_to_process = []

        for i in range(len(pending_one_numbers)):
            if len(free_requests) > i:
                R = free_requests[i]
                R["one_number"] = pending_one_numbers.pop(0)
                requests_to_process.append(R)

        """
        if we have any work to do we will do it here
        """
        resp = None
        if len(requests_to_process):
            res.utils.logger.info(f"{requests_to_process}")
            if plan:
                return [respond_to(r) for r in requests_to_process]
            resp = process_requests(requests_to_process)
            res.utils.logger.info(
                f"<<< PROCESSED REQUEST TO SAVE ONE ORDER (PIECE TRACKING)>>>>"
            )
        else:
            res.utils.logger.info(
                f"<<< NO WORK TO DO SAVING ONE ORDERS (PIECE TRACKING) >>>>"
            )

        res.utils.logger.metric_node_state_transition_incr(
            "FulfillmentNode", order_number, NodeStates.EXITED.value
        )
        return resp
    except Exception as ex:
        res.utils.logger.warn(f"Failing to reconcile {order_number=}, {sku=}")
        res.utils.logger.warn(traceback.format_exc())
        res.utils.logger.metric_node_state_transition_incr(
            "FulfillmentNode", order_number, NodeStates.FAILED.value
        )
        res.utils.logger.metric_exception_log_incr(
            "missing_one_assignment", "one_order_save"
        )

        res.utils.logger.send_message_to_slack_channel(
            "sirsh-test",
            f"<>  Failing to save the one_order stuff - {order_number=}, {sku=} - {repr(ex)}",
        )

        raise
    finally:
        res.utils.logger.metric_node_state_transition_incr(
            "FulfillmentNode", order_number, NodeStates.ENTERED.value
        )


@retry(wait=wait_fixed(2), stop=stop_after_attempt(2), reraise=True)
def save_order_for_one_number(order_number, one_number, check_exists=False, plan=False):
    """
    - the one numbers are expected to exist and added to the map - detect when not
    - the make order request is generated for the request and the one number is saved

    """
    from res.flows.sell.orders.FulfillmentNode import FulfillmentNode

    # check exists the order already and do nothing
    # might be an idea to do this for the primary rank or in generators for first encounter

    orders = FulfillmentNode.get_make_order_request_for_order(
        order_number, map_one_numbers=True
    )
    match = None
    for o in orders:
        if o.get("one_number") == one_number:
            match = o
            break
    if match:
        res.utils.logger.info(
            f"We found a match for one number {one_number} in the order {order_number}"
        )
        if not plan:
            return process_requests([match])
        response = respond_to(match)
        # now we can save the pieces for tracking
        return response
    """
    if there is no match then we need to look into why we are not saving the order mapping
    """
    # res.utils.logger.metric_exception_log_incr("missing_one_assignment", "order_save")


def respond_to(request, **kwargs):
    """
    REQUEST_TOPIC gives examples of what the request structure looks like

    todo compute instances

    """

    # TODO - determine the make instance for the fulfillment item

    known_one_number = request.get("one_number", kwargs.get("one_number", 0)) or 0
    sku = request["sku"]
    size_code = sku.split(" ")[-1]
    redis = res.connectors.load("redis")
    cache = redis["CACHE"]["SIZES"]
    if cache is None:
        raise Exception(
            "TODO  need to populate the redis cache when we cannot lookup a size"
        )
    sized_normed = cache[size_code]

    def gen_order_number(p):
        m = p.get("metadata")
        order_name = m.get("order_name")
        brand_code = m.get("brand_code")
        if brand_code + "-" in order_name:
            return order_name
        # an old format not really supported any more
        source_order_id = m.get("source_order_id")
        return f"{m.get('brand_code')}-{source_order_id}"

    resp = queries.get_active_style_size_pieces_by_id(
        request["style_size_id"], flatten_to_df=True
    )

    if len(resp) == 0:
        res.utils.logger.warn(
            f"There was no style for the request {request} - not sure how this is possible"
        )

        # this can happen for example if there is a material swap so that the old sku is discontinued
        # to get out of this
        # 1. the queue needs to show that we are holding this because the bad sku is requested
        # 2. the product in sell needs to be discontinued and transferred to a new sku
        # 3. an auto-repair should try to make this request again - the next time it requests the order request it will point to a different style id
        style_sku = " ".join(sku.split(" ")[:3])
        res.utils.logger.metric_contract_failure_incr(
            "production-inbox", asset_key=style_sku, contract=["STYLE_REGISTERED"]
        )
        res.utils.logger.metric_exception_log_incr(
            "style_registered", "fulfillment_node"
        )
        raise StyleNotRegisteredContractException(
            f"Missing Style Association for {sku} - failing contract STYLE_REGISTERED"
        )

    delegate = dict(resp.iloc[0])
    piece_registry_rank = delegate.get("piece_registry_rank")

    # process the pieces and get the piece metadata out of it
    pieces = dict(resp[["piece_id", "meta_piece_key"]].values)
    # body_piece_keys = dict(resp[["piece_id", "body_piece_key"]].values)
    # this pulls the pieces components for body and version e.g. KT-2011-V10
    unique_garment_key = delegate.get("style_group").replace("-", "")

    # this pulls the rank which comes from the style via the pieces query
    garment_instance = piece_registry_rank or delegate.get("style_rank", 1)

    one_code_prefix = f"{unique_garment_key}-G{garment_instance}-{sized_normed}"
    make_hash = request["id"]
    # pass in a make instance for testing/seeding or generate
    make_instance = increment_instance_for_group_at_time(
        one_code_prefix, make_hash, sku=sku
    )["make_instance"]

    d = {
        "request_id": request["id"],
        "style_size_id": request["style_size_id"],
        "style_group": unique_garment_key,
        "style_rank": garment_instance or 0,
        "size_code": size_code,
        "make_instance": make_instance,
        "metadata": {"request_id": request["id"]},
        "order_number": request.get("order_key", gen_order_number(request)),
        "one_pieces": [],
        "order_item_id": request.get("order_line_item_id"),
        "order_item_rank": request.get("metadata").get("fulfillment_item_ordinal"),
        "sku": sku,
        "one_number": known_one_number,
    }

    one_code = OneOrder(**d).one_code
    res.utils.logger.debug(
        f"attaching pieces to one code {one_code} and request {request['id']}"
    )
    d["one_pieces"] = [
        OnePiece.make_pieces_for_one_request(
            request["id"], pid, piece_key, one_number=known_one_number
        )
        for pid, piece_key in pieces.items()
    ]

    return d


def bump_make_increment(rid, one_number, piece_codes, versioned_body_code):
    """
    we norm the piece codes here to be safe and generate a bundle for anything that we are healing
    we assume one piece code in the set but it works for more

    return the piece code mapped to make increments

    piece_code are in the short format e.g. PNTBKPKTLF-S
    """
    from res.flows.make.production.queries import update_healing_status

    instances = {}
    for pc in piece_codes:
        instances[pc] = update_healing_status(
            one_number=one_number,
            piece_code=pc,
            request_id=rid,
            versioned_body_code=versioned_body_code,
        )
    return instances


def _ppp_move_pieces_to_roll_packing_enter(ppp_response_asset, plan=False):
    try:
        one_number = int(float(ppp_response_asset["one_number"]))
        rid = ppp_response_asset.get("id")
        pieces = ppp_response_asset["make_pieces"]

        rank = ppp_response_asset.get("metadata", {}).get("rank")
        make_increment = None
        try:
            if rank.lower() == "healing":
                if not len(pieces):
                    uri = ppp_response_asset["uri"].split("/")[-1].split(".")[0]
                    piece_code = f"-".join([a for a in uri.split("-")[-2:]])
                    pieces = [{"piece_name": piece_code}]
                else:
                    piece_code = pieces[0]["piece_code"]

                # in practice there should only be one healing piece in the observation
                # we can move this logic to the piece observer process because we dont really do anything here that is not known to the payload
                # we only apply this logic of the rank is Healing
                # if we are saving this make increment to a database we need to make sure the piece observer mutation does not overwrite the healing counts
                instances = bump_make_increment(
                    rid,
                    one_number,
                    [piece_code],
                )
                # PPP only supports one piece here (NOTE WE ARE USING 0 INDEX IN KAFKA)
                make_increment = instances[piece_code]
                res.utils.logger.info(
                    f"Make increment for {piece_code} - {make_increment}"
                )
        except:
            res.utils.logger.warn(
                f"Failing to update the piece healing status {traceback.format_exc()}"
            )

        asset = {
            "id": rid,
            "one_number": one_number,
            "one_code": "",
            "pieces": [
                {
                    "code": p.get("piece_code", p["piece_name"]),
                    # we 0 index in ppp requests - see from res.flows.make.production.queries import get_one_piece_healing_instance_resolver
                    "make_instance": make_increment - 1,
                }
                for p in pieces
            ],
            "node": "Make.Print.RollPacking",
            "status": "Enter",
            "contracts_failed": [],
            "defects": [],
            "observed_at": res.utils.dates.utc_now_iso_string(),
            "metadata": {
                "material_code": ppp_response_asset.get("material_code"),
                "rank": rank,
                "make_increment": make_increment,
            },
        }
        if plan:
            return asset
        return move_pieces_to_roll_packing_enter(asset)
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to relay the piece observation {res.utils.ex_repr(ex)}"
        )


def move_pieces_to_roll_packing_enter(asset):
    """
    observe the pieces entering rollpacking
    """
    # a = OnePieceSetUpdateRequest(**asset)
    return res.connectors.load("kafka")[
        "res_make.piece_tracking.make_piece_observation_request"
    ].publish(asset, use_kgateway=True)


def get_one_number_for_request_from_data(f, df):
    n = f["metadata"]["order_name"]
    active = df[df["is_cancelled"] == False]
    omap = active[active["order_number"] == n][
        ["sku", "key", "one_number", "order_number"]
    ]
    omap = dict(
        omap.groupby("sku")
        .agg({"one_number": lambda x: sorted(list(x))})
        .reset_index()
        .values
    )
    # lookup the sku and get the ith one number
    try:
        return omap[f["sku"]][f["metadata"]["fulfillment_item_ordinal"] - 1]
    except:
        print(traceback.format_exc())
        return 0


def process_requests(requests, one_data=None, on_error=None):
    """
    simple wrapper to take a request and save it
    e.g
      make_requests = FulfillmentNode.get_make_order_request_for_order(order_number)
      process_requests(make_requests)
    """

    for request in requests:
        try:
            (
                handle_event(request)
                if one_data is None
                else handle_event(
                    request,
                    one_number=get_one_number_for_request_from_data(request, one_data),
                    on_error=on_error,
                )
            )

        except Exception as ex:
            res.utils.logger.warn(f"Failed on request in set... {ex}")


def verify(response, total_required=None, swap_rank=True):
    """
    looking ahead
    r = get_by(order_number, sku)
    rank = r['order_item_rank]
    one_number = r['one_number]

    if response['one_number']

    BAD STATES:
    - if im sending in a zero on top of a one number that exists
    - if im sending in a one number to a different rank

    That means we can only save new blank things to fill later OR we can update if our checks all pass
    That means that we will have some holes that need to be filled in later
    - add a rank we dont have
    - pair a one number
    both can be assumed into another function `pair_make_request_to_one_order`
    """
    from res.flows.make.production.queries import get_existing_sku_orders

    one_number = response["one_number"]
    order_number = response["order_number"]
    rank = response["order_item_rank"]
    sku = response["sku"]

    existing_one_match = None
    existing = get_existing_sku_orders(order_number=order_number, sku=sku)
    used_ranks = [o["order_item_rank"] for o in existing]
    # if we know how many we need we can pass it in - otherwise we can use the max rank based on what we have
    max_rank = total_required or int(max(used_ranks)) if used_ranks else 0
    free_ranks = list(set(list(range(1, max_rank + 1))) - set(used_ranks))

    res.utils.logger.info(f"Max rank {max_rank}: rem {free_ranks}")

    warnings = []
    if one_number != 0:
        for o in existing:
            if o["one_number"] == one_number:
                existing_one_match = o

                if existing_one_match["order_item_rank"] != rank:
                    res.utils.logger.warn(
                        f"You are trying to change the rank when saving - the one number {one_number} is already saved"
                    )
                    warnings.append("ILLEGAL_RANK_CHANGE")

                #     else:
                #         raise Exception(
                #             f"You are trying to overwrite a ONE record {existing_one_match} with an item of rank {rank}. "
                #         )
    else:
        for o in existing:
            existing_one_match = o

            # if the other thing has a one number non zero, we cannot use its rank if it has a different one number to us
            if (
                o["one_number"]
                and o["one_number"] != one_number
                and o["order_item_rank"] == rank
            ):
                if free_ranks:
                    res.utils.logger.warn(
                        f"Swapping for a free rank for the empty make order {response['order_item_rank']} -> {free_ranks[0]}. Later you can pair a one number or if there is a free one we could do it here"
                    )
                    response["order_item_rank"] = free_ranks[0]
                else:
                    raise Exception(
                        f"You are trying to overwrite a ranked record that has a one number {existing_one_match} with an item of rank {rank} and one {one_number}"
                    )

    res.utils.logger.info(f"Verification complete against existing order")
    return warnings


def explain_violation(O: OneOrder, typ=None):
    for p in O.one_pieces:
        print(p.code, "id", p.id, "oid", p.oid)


def handle_event(event, **kwargs):
    """
    any type of job can relay a payload to this function to update hasura for now and in future the flow api when we deprecate the older process
    this process will process requests from kafka to generate make orders (without one numbers) - we may update the make instance later too
    ...
    """
    API = FlowAPI(OneOrder, postgres="dont need")
    try:
        # work on the make instance lookups / inventory -> we get a make instance OR an inventory reference
        # create a transaction for the make instance by looking and committing

        res.utils.logger.info(
            f"Processing asset {event.get('sku')} in order {event.get('metadata',{}).get('order_name')}"
        )
        # we generate the make instance in the database
        response = respond_to(event, **kwargs)  # , make_instance=make_instance_for_item

        total_required = int(
            event.get("metadata", {}).get("original_item_ordered_quantity", 0) or 0
        )

        warnings = verify(response, total_required)

        if warnings:
            res.utils.logger.warn(
                f"Skipping save as there are warnings we cannot ignore"
            )
            if kwargs.get("on_warning") == "raise":
                raise

            return

        O = OneOrder(**response)
        res.utils.logger.info(
            f"Saving id:{O.id} <- {O.order_number}. One number {O.one_number}({O.oid}) with item rank {O.order_item_rank} with {len(O.one_pieces)} pieces. The first piece id is {O.one_pieces[0].id } -points back to parent id {O.one_pieces[0]}"
        )
        try:
            R = API.update_hasura(O)
        except Exception as ex:
            # try and explain: one_pieces_oid_key
            res.utils.logger.info(
                f"Constraint violation {ex} - things to check: {O.one_number}({O.oid}) - sample piece {O.one_pieces[0]} "
            )
            if "one_pieces_oid_key" in str(ex):
                explain_violation(O, "one_pieces_oid_key")
            raise
        res.utils.logger.info(
            f"<<< Done with {O.one_code} - One Number At Time is {O.one_number}>>>"
        )
        # API.update(OneOrder(**response))
    except Exception as ex:
        res.utils.logger.warn(f"Failing {traceback.format_exc()}")
        if kwargs.get("on_error") == "raise":
            raise


def snowflake_load_request_by_order_number(
    order_number, since_date=None, as_response=True, one_number=0
):
    """
    Example usage, we can get the snowflake data "as-response" and this is in the form of a one order
    we can save that one order
    NOTE - we do this if there is none and we can simulate a one number with any integer value

        API = FlowAPI(OneOrder)
        #get the order as a response object that we can be sent to hasura
        R = snowflake_load_request_by_order_number('KT-66648')
        #to simulate,we do not have a one number in the beginning
        R['one_number'] = one_number

        #now save
        O = OneOrder(**response)
        res.utils.logger.info(f"Saving {O.id} <- {O.order_number}")
        R = API.update_hasura(O)

    IF there is no order, we need to go back further to the FulfillmentNode and save an order from some place

    If we have the order, and we have the make order - then we can save the association in `pair_make_request_to_one_order`

    Args:
     since_date: used only as a hint to avoid parsing (maybe) e.g. 2023-04-12
     as_response: we convert the snowflake request into a response type that is saved
     order_number to look up
     one_number: this is used to simulate the known/unknown associated one number
    """

    import json

    # and TO_TIMESTAMP_TZ(parse_json(RECORD_CONTENT):created_at) > '2023-04-12'

    res.utils.logger.info("Fetching")
    Q = f"""select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_ORDERS_ONE_REQUESTS"  where parse_json(RECORD_CONTENT):metadata:order_name::string = '{order_number}' """
    if since_date:
        Q += f" AND TO_TIMESTAMP_TZ(parse_json(RECORD_CONTENT):created_at) > {since_date}"
    res.utils.logger.info(Q)
    data = res.connectors.load("snowflake").execute(Q)
    if len(data):
        data = json.loads((data.iloc[-1]["RECORD_CONTENT"]))
        return data if not as_response else respond_to(data)
    res.utils.logger.warn(f"Order {order_number} not found")


def pair_make_request_to_one_order(
    sku, order_number, one_number, channel=None, plan=False
):
    """
    This snippet is roughly used in the legacy process to pair new one numbers with existing make-one orders
    it takes a SKU within an order (e.g. from shopify) and then associates it with the one number
    for example we may have 2x a sku and the one number is paired to the first / second of these fulfillment items
    the mapping below creates an ordered list of one numbers for each sku
    """
    from schemas.pydantic.make import OneOrder, OneOrderResponse

    from res.flows.api import FlowAPI
    from res.flows.make.production.queries import (
        associate_one_number_with_make_order,
        update_order_map_for_association,
    )

    # sku, order_number, one_number = 'KT-2011 LTCSL OCTCLW 2ZZSM','KT-66778','10304305'
    api = FlowAPI(
        OneOrder,
        response_type=OneOrderResponse,
        on_init_fail=None,
    )
    res.utils.logger.info("Updating the mapping for the order/skus/ones")
    mapping = update_order_map_for_association(order_number, sku, one_number)

    # if we did not have one of these requests it is because the order was never saved 0 when the order is saved in the fulfillment node we post to this topic
    res.utils.logger.info(
        f"Looking up a one order - these are created by the production inbox in response to things posted to {REQUEST_TOPIC_CREATE}"
    )
    order = associate_one_number_with_make_order(mapping, sku, one_number)

    if order:
        try:
            order["request_id"] = order["id"]
            order = OneOrder(**order)
            if len(order.one_pieces) == 0:
                raise Exception(
                    "Illegal state - should not update api with empty piece set in one order"
                )
            if plan:
                return order

            return api.update(order, plan_response=False)
        except:
            res.utils.logger.warn(
                f"[Make One Order] <@U01JDKSB196> failing to save order with id {order.id} - sample piece {order.one_pieces[0]}"
            )
            # print(order.db_dict())
            raise
    else:
        res.utils.logger.warn(f"No make order found")

    # if we fail to fetch a make order it was never saved - we could try to save it ..
    return None


def handler(event, context=None):
    """ """

    with res.flows.FlowContext(event, context) as fc:
        for asset in fc.assets:
            handle_event(asset)


def piece_inspection_handler(event, context=None):
    """
    Observing pieces - in particular healing requests
    - update the node that we observe the piece is at
    - generate a new piece if their are not too many healings and the piece inspection fails
    - send some time of warnings if there are too many
    """
    API = FlowAPI(OnePiece)
    with res.flows.FlowContext(event, context) as fc:
        # TODO grouping logic for sets of pieces
        for asset in fc.assets:
            try:
                res.utils.logger.debug("Processing asset")
                response = {}
                API.update(OnePiece(**response))
            except Exception as ex:
                API.to_dead_letters(REQUEST_TOPIC_INSPECTIONS, asset, ex)


def restore_requests_with_order_sku_mapping(sk_map):
    """

    provide a dict from order mapping data like this one sk_map=dict(orders.reset_index()[['order_number', 'sku_map']].values)

    """

    Q = f"""select * from "IAMCURIOUS_DB"."IAMCURIOUS_PRODUCTION"."KAFKA_RES_MAKE_ORDERS_ONE_REQUESTS"  """

    res.utils.logger.info(Q)
    original_one_order_requests = res.connectors.load("snowflake").execute(Q)
    ooor = (
        pd.DataFrame(
            [d for d in original_one_order_requests["RECORD_CONTENT"].map(json.loads)]
        )
        .sort_values("created_at")
        .reset_index()
    )
    ooor["RANK"] = ooor.groupby(["order_number", "sku"])["index"].rank(ascending=True)
    ooor["order_number"] = ooor["metadata"].map(lambda x: x.get("order_name"))
    ooor["brand_code"] = ooor["metadata"].map(lambda x: x.get("brand_code"))

    """
    apply some sort of pruning here - what have we saved already - how do we know
    """

    API = FlowAPI(OneOrder)
    status = []
    from tqdm import tqdm

    for order, records in tqdm(ooor.groupby("order_number")):
        mapping = sk_map.get(order)
        if mapping:
            res.utils.logger.info(f"we have a mapping for {order}")
            stat = {"order_number": order}
            for record in records.to_dict("record"):
                try:
                    r = respond_to(record)
                except:
                    stat["has_failed_style_lookup"] = True
                    continue

                idx = int(record["RANK"] - 1)
                sku = record["sku"]
                try:
                    r["one_number"] = mapping[sku][idx]
                    res.utils.logger.info(f'{r["one_number"]} "<-" {sku}')
                except:
                    stat["has_incomplete_one_mapping"] = True
                    continue

                O = OneOrder(**r)
                res.utils.logger.info(
                    f"Saving {O.id} <- {O.order_number} - the one order id is {O.oid}"
                )
                try:
                    R = API.update_hasura(O)
                    res.utils.logger.info(f"Saved {O.one_number} -> {O.one_code}")
                    stat["oid"] = O.oid
                    stat["id"] = O.id
                    stat["one_number"] = O.one_number
                    stat["one_code"] = O.one_code
                except:
                    stat["has_failed_commit"] = True
        status.append(stat)
    return status


def check_state(event, context=None):
    """
    look at the requests and the responses for both orders and healings
    - if we have not response
      - check if the assets are ready for this request and re-queue the request. for example we could have been waiting for a placement. We can check the garment status at all times
    """

    print(
        "Where we check if there are things we could not process before that we can now"
    )


def shopify_relay(event, context=None):
    """
    read from shopify onto our one orders
    create one will be writing directly to one orders which we subscribe to in the handler
    """
    return {}
