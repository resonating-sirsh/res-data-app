"""
adnan notes: https://coda.io/d/Technology_dZNX5Sf3x2R/Fulfillment-queue-ReFrame_suayx#_lu5wU
"""
import re
from res.flows.api.core.node import FlowApiNode
from schemas.pydantic.sell import OrderItem, OrderQueue
import res
import traceback
from res.connectors.airtable.AirtableConnector import AIRTABLE_ONE_PLATFORM_BASE
from res.flows.sell.orders import queries
from res.flows.api import FlowAPI
from res.flows.sell.orders.queries import get_product_by_sku
from res.flows.sell.orders.process import (
    analyze_sku_status,
    _requests_to_one_assignment,
)
from schemas.pydantic.sell import Order
import pandas as pd
import numpy as np
from res.flows.api.upserts.flatteners import flatten_insert_sell_orders_one
from typing import List, Callable
from res.flows.sell.orders.update_status import (
    set_fulfillment_status,
    create_item_from_json,
)

KAFKA_MAKE_REQUEST_TOPIC = "res_make.orders.one_requests"

USE_KGATEWAY = True


class FulfillmentException(Exception):
    pass


def execute_functions(
    functions: List[Callable[[List[str], OrderItem], List[str]]], data: OrderItem
) -> List[str]:
    failed_contracts = []

    for function in functions:
        r = function(failed_contracts, data)
        if r:
            failed_contracts += r
    return failed_contracts


def is_invalid_address(address):
    po_box_pattern = r"(?i)(p[\s\.]?o.?[\s\.]?box|post\s?office)\s?#?\d+|\bpo\sbox\b"
    military_pattern = r"^(?i)(dpo\sae|apo\sap)"

    if isinstance(address, str):
        result = ""
        if re.match(po_box_pattern, address):
            result = "PO_BOX_ADDRESS"
        if re.match(military_pattern, address):
            result = "MILITARY_ADDRESS"

        return bool(result), result

    if isinstance(address, list):
        for line in address:
            if not re.match(po_box_pattern, line):
                return True, "PO_BOX_ADDRESS"
            if not re.match(military_pattern, line):
                return True, "MILITARY_ADDRESS"

    return False, ""


def is_valid_zip(zip_code):
    pattern = r"^\d{5}$"
    match = re.match(pattern, zip_code)
    return match is not None


def check_po_box_address(fails, data_input):
    if not fails:
        fails = []
    # print(data_input)

    failed_contracts = []
    shipping = vars(data_input["shipping"])
    res.utils.logger.debug(data_input)

    if not shipping:
        return ["NO_SHIPPING_OBJECT"]

    name = shipping["name"]
    address1 = shipping["address1"]
    address2 = shipping["address2"]
    email1 = data_input["email"]
    email2 = shipping["email"]

    # city = shipping.get('city')

    zipcode = shipping["zipcode"]
    # province = data_input.get('shipping').get('province')
    phone = shipping["phone"]
    # country = data_input.get('shipping').get('country')
    not is_valid_zip(zipcode) and failed_contracts.append("IS_VALID_ZIP")

    invalid1 = is_invalid_address(address1)
    invalid2 = is_invalid_address(address2)

    if invalid1[0]:
        failed_contracts.append(invalid1[1])
    if invalid2[0]:
        failed_contracts.append(invalid2[1])
    if not name:
        failed_contracts.append("NAME_REQUIRED")
    if not phone or not email1 or not email2:
        failed_contracts.append("PHONE_OR_EMAIL_REQUIRED")

    result = list(set(fails) | set(failed_contracts))

    return result


def check_for_failed_contracts(order_item_data: OrderItem):
    # Example lambda functions
    # random["contracts_failing"].append(string_to_append)
    # hello = lambda fails, data_input: print(data_input)
    result = []

    try:
        check_for_payment_fields = lambda fails, data_input: (
            ("was_payment_successful" in data_input)
            and ("was_balance_not_enough" in data_input)
            and (not data_input["was_payment_successful"])
            and (data_input["was_balance_not_enough"])
            and fails.append("BRAND_SUFFICIENT_BALANCE")
        )

        # Array of functions
        function_array = [check_for_payment_fields, check_po_box_address]

        # Call the function to execute all functions in the array
        result = execute_functions(function_array, vars(order_item_data))
    except:
        res.utils.logger.warn(f"Failing to test for contracts")
        res.utils.logger.warn(f"{traceback.format_exc()}")
    return result


def _has_res_sku(oi):
    if 4 != len(oi["sku"].split(" ")):
        res.utils.logger.warn(
            f"Skipping order item which does not look a resonance item: {oi}"
        )
        # if it does not look like a resonance sku skip processing
        return False
    return True


class FulfillmentNode(FlowApiNode):
    atype = OrderItem
    uri = "one.platform.sell.fulfillment"

    def __init__(self, asset, response_type=None, queue_context=None, postgres=None):
        super().__init__(
            asset,
            response_type=response_type,
            queue_context=queue_context,
            base_id=AIRTABLE_ONE_PLATFORM_BASE,
            postgres=postgres,
        )
        self._name = FulfillmentNode.uri

    @staticmethod
    def run_request_collector(order):
        request_collector = []
        R = FulfillmentNode(Order, postgres="dont_care").run(
            order,
            relay_queue=False,
            update_queue=False,
            disable_relay_kafka=True,
            request_collector=request_collector,
        )

        return request_collector

    @staticmethod
    def read_by_name(order_name):
        return Order(**queries.get_full_order_by_name(order_name, flatten=True))

    @staticmethod
    def get_make_order_request_for_order(
        order_name, map_one_numbers=False, sku_filter=None, raise_failed_track=False
    ):
        request_collector = []
        O = FulfillmentNode.read_by_name(order_name)
        register_fulfillable_items(
            O,
            disable_relay=True,
            request_collector=request_collector,
            sku_filter=sku_filter,
            raise_failed_track=raise_failed_track,
        )

        if map_one_numbers:
            _requests_to_one_assignment(
                order_number=order_name, requests=request_collector
            )

        return request_collector

    # def execute_functions(functions: List[(List[str]) -> List[str]]) -> None:
    #     for functiona in functions:
    #         functiona()

    # multiline_lambda = lambda stringsx, input_type: if (
    #         "was_payment_successful" in input_type
    #         and "was_balance_not_enough" in input_type
    #         and not input_type["was_payment_successful"]
    #         and input_type["was_balance_not_enough"]
    #     ):stringsx["contracts_failing"].append("BRAND_SUFFICIENT_BALANCE")

    # print(multiline_lambda(5))  # Output: (6, 7, 8)

    def _check_contracts_failed(self, odict={}):
        if (
            "was_payment_successful" in odict
            and "was_balance_not_enough" in odict
            and not odict["was_payment_successful"]
            and odict["was_balance_not_enough"]
        ):
            odict["contracts_failing"].append("BRAND_SUFFICIENT_BALANCE")

    def _save(
        self,
        typed_record: OrderItem,
        hasura=None,
        disable_relay_kafka=False,
        plan=False,
        on_error=None,
        update_queue=False,
        request_collector=None,
        **kwargs,
    ):
        hasura = hasura or res.connectors.load("hasura")

        res.utils.logger.info(f"Running")
        response = {}
        failed_contracts = []

        #######
        ##        1. PROCESS ORDER/ITEMS, SAVE THEM WITH ANY CONTRACTS FAILING IN THE DATABASE
        #######
        count_saved_items = 0
        try:
            failed_contracts = check_for_failed_contracts(typed_record)

            # self.check_for_failed_contracts(typed_record)

            api = FlowAPI(Order, hasura=hasura, postgres="dont_care")
            m = api.ensure_loaded_mutation()

            # parse out what we want - todo modify the mutation to actually just take an order
            odict = typed_record.db_dict()
            #
            # TODO: UPDATE FROM shopify orders

            # check_for_shopify_updates(typed_record.order_items, odict)
            customer = typed_record.shipping.db_dict()
            odict["customer_id"] = customer["id"]
            order_items = [d.db_dict() for d in typed_record.order_items]
            # filter
            order_items = [oi for oi in order_items if _has_res_sku(oi)]
            distinct_skus = set([s["sku"] for s in order_items])
            products = {}
            for sku in distinct_skus:
                try:
                    products[sku] = get_product_by_sku(sku, hasura=hasura)
                except Exception as ex:
                    res.utils.logger.warn(
                        f"Could not get product for {sku}: {res.utils.ex_repr(ex)}"
                    )
            for oi in order_items:
                # dont saving anything to our database, or even attempt it, if the sku is not conforming
                # this is not to validate data but to avoid processing overload from but we could do better to detect this

                try:
                    # se set this to none in case it was passed into the schema as sometimes happens
                    oi["product_id"] = None
                    # parent relation here but we can change this in future with a simpler mutation batch
                    oi["order_id"] = odict["id"]
                    p = products[oi["sku"]]
                    oi["product_id"] = p["id"]
                    oi["name"] = oi.get("name") or p.get("name")
                except Exception as ex:
                    res.utils.logger.warn("Error resolving product for order item")
                    res.utils.logger.warn(ex)
                    # res.utils.logger.info(
                    #     f"Adding a failed contract for the SKU {oi['sku']}: PRODUCT_REGISTERED"
                    # )
                    # This is not a state that is supposed to happen ordinarily but can happen for the following reasons
                    # 1. The style is not registered yet in the env (dev) or (prod) . if the 3d style is sold it should have gone into the queue
                    #    we can, for testing, repair MetaOneNode.repair_locally('KT-3030 LY115 OCEAVP', force_bootstrap_header=True)
                    #    we can also request the style in that env MetaOneNode.refresh('KT-3030 LY115 OCEAVP') using the cluster in the given env (post to correct kafka env)
                    # 2. The style may be in 2d and not destined for 3d at all. We dont support this now
                    # 3. The style may be valid but we have not registered the product link (this is rare but a possible DB glitch)
                    # 4. the sku may have been swapped and we have not registered the old sku linking to the current meta one
                    # in short we need to know if we are actually supporting and have saved the meta one and the product link
                    fail = ["PRODUCT_REGISTERED"]
                    status = analyze_sku_status(oi["sku"])

                    if status.get("style_exists", "") == False:
                        fail.append("STYLE_REGISTERED")
                    elif status.get("in3d", "") == False:
                        fail.append("3D_ENABLED")

                    odict["contracts_failing"] = list(
                        set(odict["contracts_failing"]) | set(fail)
                    )

            odict["contracts_failing"] = list(
                set(odict["contracts_failing"]) | set(failed_contracts)
            )

            if len(order_items) == 0:
                res.utils.logger.warn(
                    f"Because we did not detect resonance skus in the order, we are not going to save it"
                )
                # TODO implement a better void response
                return {}
            # todo can we save a record without an order item list
            response = hasura.execute_with_kwargs(
                m, order=odict, customer=customer, order_line_items=order_items
            )

            res.utils.logger.info(f"saved {odict['id']} {odict['name']}")
            count_saved_items += 1

        except:
            res.utils.logger.warn(f"{traceback.format_exc()}")

        # if we get a response and the order is valid, then we can register the fulfillment items
        #####
        #      2. UPDATE MAKE AND START TRACKING ITEMS THAT WE SHOULD SHIP
        #####

        if count_saved_items == 0:
            res.utils.logger.warn(
                f"We did not save any items for one reason or another"
            )
            return None
        try:
            res.utils.logger.info(f"Registering the fulfillable items .")
            order_response = flatten_insert_sell_orders_one(response)
            # print(order_response)
            res.utils.logger.info(f"Order response: {order_response}")
            register_fulfillable_items(
                Order(**order_response),
                disable_relay=disable_relay_kafka,
                request_collector=request_collector,
            )
        except Exception as ex:
            # explain

            res.utils.logger.warn(
                f"Failed to register fulfillable items {traceback.format_exc()}"
            )

        #####
        #      3. UPDATE ORDER QUEUE TO PROVIDE VIEW OF OPEN ORDERS
        #####
        order_queue = _try_make_queue_response(response)
        # at this point we have saved everything - if we fail to save to airtable we should error or something but not dead letter
        if order_queue is None or order_queue.status.lower() == "fulfilled":
            res.utils.logger.debug("Will not add fulfilled items to the airtable queue")
        elif kwargs.get("relay_queue", True):
            # use the flow api to update the order queue - in future this can be integrated to a single type of response
            api = FlowAPI(OrderQueue)
            api._airtable_queue.refresh_schema_from_type(OrderQueue)
            api.update_airtable_queue(order_queue)
        else:
            res.utils.logger.info(f"Queue update disabled")

        return order_queue


# TODO: check style status and brand status and then relay only what we are ready to relay or relay with contracts to hold (if we have an order monitor better not to send them yet)
def _try_make_queue_response(r):
    try:
        o = r["insert_sell_orders_one"]
        oi = r["insert_sell_order_line_items"]["returning"]
        # create as many skus as we have
        o["skus"] = list(
            np.concatenate([np.repeat(item["sku"], item["quantity"]) for item in oi])
        )
        return OrderQueue(**o)
    except:
        res.utils.logger.warn(
            f"Could not create a queue response for this item - maybe the fulfillment is invalid"
        )
        res.utils.logger.warn(f"{traceback.format_exc()}")
        return None


def determine_fulfillment_action_from_item_status(item, make_item_instance):
    fulfillment_status = item.status
    fulfillable_quantity = item.fulfillable_quantity

    # print(make_item_instance, item.fulfilled_quantity, item.fulfillable_quantity)
    if pd.isnull(fulfillment_status):
        fulfillment_status = "pending"
    fulfillment_action = "Pending"
    if fulfillment_status == "fulfilled":
        fulfillment_action = "Fulfilled"
    elif make_item_instance <= item.fulfilled_quantity:
        fulfillment_action = "Fulfilled"
    elif make_item_instance > fulfillable_quantity:
        # but if we dont, its not always clear. if everything is cancelled fine
        if fulfillment_status.lower() == "cancelled":
            fulfillment_action = "Cancelled"
        # but otherwise we don know how many things have been actually fulfilled yet
        # it could be there only n left to fulfil because stuff has shipped or it could be that the customer edited their order
        else:
            # not sure what to do here
            fulfillment_action = "Pending"
        # we may be able to get a smarter cancellation state on an individual item
    return fulfillment_action


def iterate_make_response(order, sku_filter=None, hasura=None):
    """
    this turns the fulfillment items that we saved into make requests
    we can filter this response based on states.

    """
    hasura = hasura or res.connectors.load("hasura")

    cache_lookups = {}
    for item in order.order_items:
        if not item.product_id:
            # if this happens its not hard to get the meta one from the sku and register the product
            res.utils.logger.warn(
                f"the order item with sku {item.sku} is not associated with a product so will be skipped - this should be gated properly at order saving time"
            )
            continue
        sla_days_at_order = -1

        for make_item_instance in range(item.quantity):
            if sku_filter:
                if item.sku not in sku_filter:
                    continue

            make_item_instance += 1

            # TODO: tenacity on this + track any errors from this and continue on error depending on options
            # TODO: also register expected make costs here too - we will track actual and compare
            try:
                key = f"{item.product_style_id} {item.size_code}"
                if key in cache_lookups:
                    style_info = cache_lookups[key]
                else:
                    res.utils.logger.info(
                        f"Looking up the product style '{item.product_style_id}' '{item.size_code}'"
                    )
                    style_info = queries.get_style_size(
                        style_id=item.product_style_id,
                        size_code=item.size_code,
                        hasura=hasura,
                    )
                    cache_lookups[key] = style_info
            except:
                res.utils.logger.metric_exception_log_incr(
                    "style_lookup", "fulfillment_node"
                )
                res.utils.logger.warn(
                    f"Failing to get the product in the size read from the sell.order-item (which may differ from the make request's sku size) - check that the style exists first and then check if the size is on the style. Sometimes its worth checking if the size event exists in the size chart https://airtable.com/appjmzNPXOuynj6xP/tblvexT7dliEamnaK/viwA3VbIYx8zOjWd9?blocks=hide"
                )
                raise

            make_order = {
                # the item instance is the id - alias for a fulfillment item id
                # This is the session id
                "id": res.utils.uuid_str_from_dict(
                    {"order_line_item_id": item.id, "instance": int(make_item_instance)}
                ),
                # "one_code": one_code,
                # this is our make sku, not necessarily what was ordered
                "sku": style_info["sku"],
                "style_size_id": style_info["id"],
                "product_name": item.name,  # or is it the meta one we load
                "order_date": order.ordered_at,
                # this is our id not the source one
                "order_line_item_id": item.id,
                "email": order.email,
                "order_channel": order.order_channel,
                "sales_channel": order.sales_channel,
                "price": item.price,
                "sla_days_at_order": sla_days_at_order,
                "created_at": res.utils.dates.utc_now_iso_string(),
                # make one or something more
                "quantity": 1,
                "metadata": {
                    "order_name": order.name,
                    "cost": style_info.get("cost"),
                    "brand_code": order.brand_code,
                    "fulfillment_item_ordinal": make_item_instance,
                    "original_item_ordered_quantity": item.quantity,
                    "source_order_id": order.source_order_id,
                    "product_id": item.product_id,
                    "ordered_quantity_of_sku": item.quantity,
                    "fulfillment_action": determine_fulfillment_action_from_item_status(
                        item, make_item_instance
                    ),
                    # the ordered one may differ from the make one with swaps
                    "ordered_sku": item.sku,
                },
            }

            # alias
            # make_order["request_id"] = make_order["id"]

            yield make_order, make_item_instance

    # after we have tested this we can move this back to queries and replace the other one


def _track_fulfillment(
    item_id, item_instance, fulfillment_action, make_order_id, hasura=None
):
    try:
        record = {
            "id": res.utils.uuid_str_from_dict(
                {"order_item_id": item_id, "order_item_ordinal": item_instance}
            ),
            "make_order_id": make_order_id,
            "status": fulfillment_action,
            "started_at": res.utils.dates.utc_now_iso_string(),
            "order_item_id": item_id,
            "order_item_ordinal": item_instance,
        }

        r = hasura.execute_with_kwargs(queries.UPDATE_FULFILLMENT, record=record)

        return r["insert_sell_order_item_fulfillments_one"]
    except Exception as ex:
        res.utils.logger.warn(
            f"Failed to track fulfillment   {record} - ignoring for now:> {ex}"
        )
        raise


def register_fulfillable_items(
    order_response,
    disable_relay=False,
    request_collector=None,
    sku_filter=None,
    hasura=None,
    raise_failed_track=False,
):
    """
    this is refactored from the generic handler making it easier to use
    here we assume that something looks like a response order is saved to the database
    then we can get the response of that OR read the thing from the database and register the fulfillment
    - if read we are just reading from the database like a proble
    - if created we thing we are creating the order for the first time or at least some of the items??
    - if update we are updating the order
    we may want to know if there are NEW changes to the order i.e. if the items are being created as fulfllable or we already know about them

    - we can do some legacy support things to determine a one number here as metadata


    This function is just a workflow function that takes a DB read response which "knows" if it has created any changes to fulfillments and sends messages onwards

    """

    hasura = hasura or res.connectors.load("hasura")
    kafka = res.connectors.load("kafka")

    # we use the database callback to construct the payload to relay - there are multiple items per order header
    for item, instance in iterate_make_response(
        order_response, hasura=hasura, sku_filter=sku_filter
    ):
        # that thing has the option to return none
        if not item:
            continue

        ###################################################################
        # see unit tests as we want to test different scenarios for this
        fulfillment_action = item["metadata"]["fulfillment_action"]
        # print("fulfillment action", fulfillment_action)

        result = None
        for i in range(1):
            try:
                result = _track_fulfillment(
                    item["order_line_item_id"],
                    instance,
                    fulfillment_action,
                    item["id"],
                    hasura=hasura,
                )
            except:
                pass
            # break

        #           this is the key logic of this function
        ###################################################################

        if result is None:
            res.utils.logger.warn(
                f"Fatal error- did not track fulfill items - it is useful to validate the order with [get_order_by_name(order_number, validate=False)]"
            )
            if raise_failed_track:
                raise FulfillmentException("Failed to track fulfillments")
            return order_response
        # check if we already sent to make, then we are waiting on make
        sent_at = result.get("sent_to_assembly_at")
        if sent_at and fulfillment_action != "Cancelled":
            fulfillment_action = "Active"

        if request_collector is not None:
            request_collector.append(item)
        # is the item new and we have not requested it from make, then we request
        if fulfillment_action == "Pending" and not disable_relay:
            # check if we already sent it to assembly
            res.utils.logger.info(
                "    we will log that we have just sent the message to make"
            )
            kafka[KAFKA_MAKE_REQUEST_TOPIC].publish(
                item, use_kgateway=USE_KGATEWAY, coerce=True
            )
            queries.log_fulfillment_item_request(result["id"], hasura=hasura)
        else:
            res.utils.logger.debug(
                f"Skipping kafka update with option for disable relay {disable_relay} and action {fulfillment_action}"
            )
            # query

        # if its active we dont need to take an action
        if fulfillment_action == "Active":
            pass

        # if we determine the order was cancelled we can take an action
        if fulfillment_action == "Cancelled":
            pass  # cancel or at least update the make one status

        res.utils.logger.debug(
            f"Fulfillment action {fulfillment_action} applied on sku {item['sku']} - sent to assembly at: {sent_at}"
        )

        # if its not we would not have called this but we do need to take other actions
    return order_response


def shopify_handler(event, context=None):
    """
    process the input schema and return a response in the output format which is the res_make order request
    expected to handle shopifyOrder kafka
    """
    try:
        res.utils.logger.info(f"Processing in handler...")
        o = Order.from_shopify_payload(event)
        # use the fulfillment node to save and add any business logic
        # call the underlying function not the base one which does some other flow api stuff
        # if we fail to save as a transaction we add a failing contracts
        response = FulfillmentNode(o)._save(o)
        # after we get a response we can flatten it and pack it into an object which we can then consume to update make
        return response

    except Exception as error:
        res.utils.logger.warn("Error on Shopify Handler for relay. Payload follows")
        res.utils.logger.warn(f"{event}")
        raise error


def create_one_handler(event, context=None, skip_make_update=False):
    """
    process the input schema and return a response in the output format which is the res_make order request
    receives event from res-connect but in future could be a create-one kafka message
    """
    try:
        o = Order.from_create_one_payload(event)
        # use the fulfillment node to save and add any business logic
        # call the underlying function not the base one which does some other flow api stuff
        # if we fail to save as a transaction we add a failing contracts
        response = FulfillmentNode(o)._save(o)
        # after we get a response we can flatten it and pack it into an orject object which we can then consume to update make
        return response
    except Exception as error:
        res.utils.logger.warn("Error on Create-ONE Handler for relay. Payload follows")
        res.utils.logger.warn(f"{event}")
        raise error


@res.flows.flow_node_attributes(
    memory="12Gi",
)
def bulk_loader(event, context={}):
    from res.flows.sell.orders.queries import bulk_load_from_fulfillments_warehouse

    with res.flows.FlowContext(event, context) as fc:
        # todo pass in loader mode and dates...
        window = (0, 1000)
        if fc.args.get("win_start") and fc.args.get("win_end"):
            window = (fc.args.get("win_start"), fc.args.get("win_end"))
        res.utils.logger.info(f"Partition {window}")
        partitions = bulk_load_from_fulfillments_warehouse(None, window=window)

        res.utils.logger.info(f"Partitions {partitions}")
