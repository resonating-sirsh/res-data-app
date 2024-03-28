"""
we manage all the queues
we can bootstrap and check status updates. some nodes will publish item changes
- style node, body node, make api, 
- orders??
some will require a polling of statuses i.e. the make processes on the airtable queues
"""

from res.observability.entity import AbstractEntity, typing, root_validator
from datetime import datetime
import res
import traceback


class Queue(AbstractEntity):
    node: str
    # list of contract names failing
    contracts_failing_list: typing.Optional[typing.List[str]] = []
    # when the specific queue was entered
    entered_at: datetime
    # the date exiting e.g. for orders this means fulfillment or for styles this means color queue exit
    exited_at: typing.Optional[datetime]
    # cancelled
    cancelled_at: typing.Optional[datetime]
    # the sub node status
    node_status: typing.Optional[str]
    # a queue owner or issue owner
    owner: typing.Optional[str] = "ONE"
    # any short comments on the status for leaning
    comments: typing.Optional[str] = ""
    # write time - also good for diffs
    last_updated_at: datetime
    # we should look for bottle necks
    owners_queue_size_approx: typing.Optional[int]
    # usually usefully to provide a link to a record
    airtable_link: str
    # scheduled times for slas - defaults to 1, 6, 12 or 24 hours
    scheduled_exit_at: datetime
    # mapping of references e.g. if its an order, the ONE numbers or if its a style. the body color and material - these will of form -has-[X]->
    refs: typing.Optional[dict] | typing.Optional[str] = {}

    # sorting inf
    @root_validator
    def _fields_def(cls, values):
        for ltype in ["contracts_failing_list"]:
            values[ltype] = list(values.get(ltype, []) or [])

        if not values.get("node_status"):
            values["node_status"] = ""  # what is a good default?

        return values

    # last node or last sub node entered?

    @classmethod
    def get_violations(cls):
        """
        List all open things that have contract violations or are late
        base classes can override to get deep on stuff
        """
        pass


class Order(Queue):
    """
    this shows the interplay between the order and one requests
    when we process one requests we also update these one order items at the same time
    the customer order date can be updated on the production request
    """

    # list of linked one numbers
    brand_name: str
    order_channel: str
    sales_channel: str
    skus: typing.Optional[typing.List[str]] = []
    one_numbers: typing.Optional[typing.List[str]] = []
    # all_delay_reasons: typing.List[str]
    fulfilled_at: typing.Optional[datetime]
    number_of_order_items: int
    number_of_fulfilled_order_items: int
    number_of_production_requests_total: int
    number_of_cancelled_order_items: int
    number_of_production_requests_delayed_in_nodes: int
    number_of_production_requests_with_failing_contracts: int

    @root_validator
    def _fields_def(cls, values):
        for ltype in ["contracts_failing_list", "skus", "one_numbers"]:
            values[ltype] = list(values.get(ltype, []) or [])

        return values


class ProductionRequest(Queue):
    """
    the make node can be used to generate these after bootstrapping
    the ppp requests will updated the print aspects -we will not use the roll info here as it would be expensive
    instead we will just manage roll states when looking at the aggregated production requests
    """

    # the date of the customer order
    sku: str
    brand_name: str
    customer_ordered_at: datetime
    customer_order_number: str
    sales_channel: typing.Optional[str]
    # if there is currently a part of this one in print
    is_being_healed: bool = False
    # all the pieces that are healing if known - should flush when they are processed
    healing_piece_list: typing.Optional[typing.List[str]] = []
    # particular interesting when something is held up in DXA
    is_pending_dxa: bool = False
    # when entered DXA interesting for delay analysis
    # sku_entered_dxa_at: typing.Optional[datetime]
    # when if ever we printed this entire ONE
    dxa_assets_ready_at: typing.Optional[datetime]
    exited_print_at: typing.Optional[datetime]
    exited_cut_at: typing.Optional[datetime]
    exited_sew_at: typing.Optional[datetime]
    shipped_at: typing.Optional[datetime]
    order_priority_type: typing.Optional[str]
    # we could store a secondary analysis of every time something on the one was printed
    # print_events_list: typing.List[str] = []
    # materials used by the one - determines if its a combo
    # materials_used: typing.List[str] = []
    # num_materials_used: int = 1
    rolls: typing.Optional[typing.List[str]] = []

    # sorting inf
    @root_validator
    def _fields_def(cls, values):
        if values.get("healing_piece_list"):
            values["is_being_healed"] = True
        else:
            values["healing_piece_list"] = []
        if not values.get("dxa_assets_ready_at"):
            values["is_pending_dxa"] = True

        return values


class Body(Queue):
    """
    after bootstrapping this will be updated in the body node when status events are processes
    """

    body_version: int
    body_name: str
    brand_name: str
    sub_statuses: dict = {}
    sizes: typing.Union[typing.List[str], typing.Any] = []

    # first ones are interesting
    # Body Design Notes Onboarding Form --> onboarding/modification inquiry notes
    first_one_body_design_notes: typing.Optional[str]
    # Rejection Reasons --> when Status=Rejected, it tells the reason
    first_one_rejection_reason: typing.Optional[str]
    # Ready for Sample Status
    first_one_ready_for_sample_status: typing.Optional[str]
    # Order First ONE Mode
    first_one_order_node: typing.Optional[str]
    # first one order id
    first_one_order_number: typing.Optional[str]
    airtable_link_first_one_order: typing.Optional[str]
    make_one_production_number: typing.Optional[str]


class Style(Queue):
    """
    after bootstrapping this will be updated in the body node when status events are processes
    """

    body_code: str
    body_version: int
    material_code: str
    other_materials: typing.Optional[typing.List[str]]
    color_code: str
    sizes: typing.Union[typing.List[str], typing.Any] = []
    is_custom_placement: bool
    is_done: bool
    is_cancelled: bool
    is_flagged: bool
    flag_for_review_tag: typing.Optional[str]
    sub_statuses: dict = {}

    @root_validator
    def _fields_def(cls, values):
        # coerce
        for ltype in ["sizes", "other_materials"]:
            values[ltype] = list(values.get(ltype, []) or [])

        return values


class PPU(Queue):
    """
    tis is a roll allocation
    We would like to understand the scheduled and deviation

    the exit date is when it exits print / or when it gets to cut. lets say cut
    """

    # the ones referenced by this ppu
    one_numbers_list: typing.List[str]
    # roll keys etc.
    components: typing.List[str]
    material_code: str
    # the expected measurements may not match the actual
    expected_length: float
    expected_width: float
    # the measurements @ named sites
    length_measurements: dict
    printed_at: typing.Optional[datetime]
    inspected_at: typing.Optional[datetime]


class MaterialPreparation(Queue):
    """
    understand full consumption of materials and resources
    """

    pass


class Returns(Queue):
    pass


def publish_queue_update(
    work: typing.Union[Queue, typing.List[Queue]], key_field="name", description=None
):
    """
    push the status of the queue item from any code
    """
    from res.observability.io import EntityDataStore, ColumnarDataStore
    from res.observability.io.EntityDataStore import Caches

    if not isinstance(work, list):
        work = [work]

    Model = work[0]

    try:
        _ = res.utils.logger.metric_node_state_transition_incr(
            node="observability",
            asset_key=Model.entity_name,
            status="ENTER",
            inc_value=len(work),
        )

        #################################################
        description = description or f"For entity {Model}"
        ES = EntityDataStore(
            Model,
            description=f"Entity store:  {description}",
            cache_name=Caches.QUEUES,
        )
        CS = ColumnarDataStore(
            Model,
            f"Columnar and statistics store:  {description}",
            create_if_not_found=True,
        )

        # return records, CS

        res.utils.logger.info(f"Adding to columnar store")
        CS.add(work, key_field=key_field)
        res.utils.logger.info(f"Adding to entity store")
        ES.add(work, key_field=key_field)
        res.utils.logger.info(f"done")
        #################################################

        _ = res.utils.logger.metric_node_state_transition_incr(
            node="observability",
            asset_key=Model.entity_name,
            status="EXIT",
            inc_value=len(work),
        )
    except:
        res.utils.logger.warn(f"Failed to publish queue{traceback.format_exc()}")
        _ = res.utils.logger.metric_node_state_transition_incr(
            node="observability", asset_key=Model.entity_name, status="EXCEPTION"
        )

    return work


def dump_order_one_status_to_db(order_name: str, production_requests: typing.List[str]):
    """
    store the state in a proper entity database

    """
    pass


def handler(event, context={}):
    """
    the job that processes and state refreshes that are not pushed from nodes
    also generally check all open queues where things are stuck

    - observe changes in make one production
    """
    pass


# provided a function / summarizing prompt pair for each standard report + use prompt for how they would like to see it
AGENT_PROMPT = [
    "for node x or node all, get all concerning things divided with SLA violation / delta as a dynamic variable ",
    "For customer orders, show orders late in date cohorts SLA + 1,2,3,4,5 Days, 1 WEEK, 2 WEEKS,  MORE",
    "For custom orders, for failing contract type, Standard Stats on days since order",
]

# add extra questions in redis, then for each question get the data and summarize


def try_get_queue_agent():
    """

    An opinionated agent that does not search for anything not provided here as stores
    """
    from res.observability.queues import Body, Style, Order, ProductionRequest
    from res.observability.io import ColumnarDataStore

    from res.learn.agents import InterpreterAgent
    from res.observability.io.EntityDataStore import Caches

    stores = [
        ColumnarDataStore(
            Body,
            description="This is for the body onboarding process in DXA which happens before anything else",
        ),
        ColumnarDataStore(
            Style,
            description="This is the apply color queue which places color on body pieces to make styles",
        ),
        ColumnarDataStore(
            Order,
            description="This is for custom order information before we start to make things",
        ),
        ColumnarDataStore(
            ProductionRequest,
            description="This is for the production request where we make the ONEs i.e. physical garments",
        ),
    ]
    agent = InterpreterAgent(
        # use the queue cache which should become default in future
        cache_name=Caches.QUEUES,
        # dont allow function search - this one will be locked s
        allow_function_search=False,
        initial_functions=[s.as_function_description() for s in stores],
    )
    return agent
