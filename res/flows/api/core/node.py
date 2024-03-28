from res.flows.api import FlowAPI
import res
from schemas.pydantic.flow import Node
from enum import Enum

SHARED_STATE_KEY = "Flow.Api.Shared.State"


class NodeStates(Enum):
    EXITED = "EXITED"
    ENTERED = "ENTERED"
    FAILED = "FAILED"


class NodeException(Exception):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class FlowApiNode:
    """
    Flow API is a factory of types but in this implementation the types need to be registered from module import
    We should maybe create some other way to associate data types with nodes
    """

    subclasses = []

    def __init__(
        self, asset, response_type=None, queue_context=None, base_id=None, postgres=None
    ) -> None:
        """
        a typed node for the queue
        a queue context for things like shared state in distributed bobs
        """
        self._asset = asset
        self._atype = type(asset)
        self._api = FlowAPI(
            self._atype, response_type=response_type, base_id=base_id, postgres=postgres
        )
        self._contracts = None  # Contracts(asset) -> allowing for future outsourcing of validation but for now the nodes wil do it
        self._queue_context = queue_context
        self._name = "A.Flow.Node"

    @property
    def key_cache(self):
        return self._api._airtable_queue._table.key_cache

    def try_get_log_path(cls):
        try:
            return cls._queue_context.log_path
        except:
            return None

    @property
    def name(self):
        return self._name

    def try_update_shared_state(cls, key, value):
        """
        this will send an update to redis to a shared space. For example suppose each size in a style is processed separately
        the queue context will have a datagroup with the queue id and provide that to the queue context
        within that we can then save different things into a shared dictionary e.g
        {
            "size1" : ["FAILED_CONTRACT_NAME"],
            "size2" : [ ],
        }
        """
        if cls._queue_context:
            cache = cls._queue_context.shared_cache
            if cache:
                res.utils.logger.debug("we have a cache and we are updating it")
                return cache.update_named_map(SHARED_STATE_KEY, key, value)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.subclasses.append(cls)

    # experimetnal to create a factory pattern - remove for now to avoid confusion
    # def __new__(cls, asset, **kwargs):
    #     """Create instance of appropriate subclass using path prefix."""
    #     discriminant = None
    #     mapping = {}
    #     T = type(asset)
    #     if discriminant:
    #         T = str(T) + discriminant

    #     if type(cls) != FlowApiNode:
    #         return object.__new__(cls, asset, **kwargs)

    #     for subclass in cls.subclasses:
    #         if (subclass.atype == T and discriminant is None) or (
    #             subclass.atype == T and discriminant == subclass.uri
    #         ):
    #             if T in mapping:
    #                 raise Exception(
    #                     "multiple implementing types - please provide a discriminant"
    #                 )
    #             mapping[T] = subclass

    #     if T in mapping:
    #         # Using "object" base class method avoids recursion here.

    #         return object.__new__(mapping[T])
    #     raise Exception(
    #         f"Could not auto resolve the type inside the generic Flow API Node - provided type {T} from subclasses {cls.subclasses}"
    #     )

    def _save(cls, asset):
        # do we determine costs
        res.utils.logger.debug("Using base class to save which is a void operation")
        return asset

    def on_error(cls, asset, **kwargs):
        """
        save as saving a trace but assume adding some info on the object??
        """

        response = cls._api.update_hasura(asset)
        # interpret the response and get the object.queue_status_dict()
        # get a generic update and post just the status update to the relay
        # we should have a relay mode that restricts to the flow api interface fields like node, status, contracts only

    def observe_asset(cls, asset, **kwargs):
        """
        for now an observation just writes the object to hasura 0 this is assumed to be queue type which is auditable on nodes and states and defects
        """
        cls._api.update_hasura(asset, **kwargs)

    def save(cls, asset=None, **kwargs):
        """
        by default if the child returns a response it means it needs to be relayed and not dropped
        this can be controlled with a flag --relay_response
        """
        update_on_save = kwargs.get("queue_update_on_save", False)
        response = cls.run(asset, update_queue=update_on_save, **kwargs)

        return response

    def save_with_relay(cls, asset=None, **kwargs):
        """
        relay does not modify hasura state - it is assumed that we know the full state and we want to update other system
        update on the other hand writes state and then gets a response back that can be relayed.
        note this is update is better only if the database response is complete as a response which happens in some and maybe most contexts
        however, it may be a more complex compilation of state is required which should be managed by the node and then relay is used.
        - this state may not make sense for now but suffice to say it would be necessary to augment attributes from the database and
         "other attributes" to do this in general as a single update so for now its safer to separate these transactions
        TODO make this more clear
        """
        relay_on_save = kwargs.get("queue_update_on_save", True)
        response = cls.run(
            asset, relay_queue=relay_on_save, update_queue=False, **kwargs
        )

        return response

    def run(cls, asset=None, **kwargs):
        """
        Normally we construct with a type but we can call multiple times for the same asset type
        The base class implements a save which writes a request and generates a response
        The response is validated by contracts, failures attached to the response, and the response is relayed with the API

        Args:
         asset: a typed asset to run a node against
        """
        # normally we run a sort of singleton but just in case we can run over batches with one context
        asset = asset or cls._asset
        assert (
            asset.primary_key is not None
        ), f"the asset {asset} has no primary key value which is illegal - the Pydantic type needs to specify a primary key to be used in the node as a queue item"
        res.utils.logger.metric_node_state_transition_incr(
            cls.name, asset.primary_key, NodeStates.ENTERED.value
        )
        response = cls._save(asset or cls._asset, **kwargs)
        if response is None:
            return None
        if hasattr(response, "primary_key"):
            if (
                hasattr(response, "contracts_failed")
                and len(response.contracts_failed) > 0
            ):
                for contract in response.contracts_failed:
                    res.utils.logger.metric_contract_failure_incr(
                        cls.name, response.primary_key, contract
                    )
                res.utils.logger.metric_node_state_transition_incr(
                    cls.name, response.primary_key, NodeStates.FAILED.value
                )
            else:
                res.utils.logger.metric_node_state_transition_incr(
                    cls.name, response.primary_key, NodeStates.EXITED.value
                )
        if kwargs.get("update_queue", True):
            return cls._api.update(response)
        if kwargs.get("relay_queue", True):
            return cls._api.relay(response)
        return response

    def __call__(self, asset, **kwargs):
        return self.run(asset, **kwargs)

    def make_payload(cls, **kwargs):
        """
        conventions used to create a flow api payload so that we have all the right construction logic using the cls type
        """
        return {}


def ensure_nodes_exist(nodes_typed, hasura=None):
    return add_nodes(nodes_typed, check_exists=True, hasura=hasura)


def add_nodes(nodes_typed, check_exists=False, hasura=None):
    """
    check exists just a trick modification of update so we only insert the blank with name unless doing a full upsert
    """
    hasura = hasura or res.connectors.load("hasura")

    if not isinstance(nodes_typed, list):
        nodes_typed = [nodes_typed]

    nodes = [u.db_dict() for u in nodes_typed]

    update_list = "name" if check_exists else "name, metadata, type"

    UPSERT_MANY = f"""
                mutation upsert_many_nodes($nodes: [flow_nodes_insert_input!] = {{}}) {{
                    insert_flow_nodes(objects: $nodes, on_conflict: {{constraint: nodes_pkey, update_columns: [{update_list}]}}){{
                    returning {{
                        id
                        name
                    }}
                    }}
                }}
        """

    return hasura.execute_with_kwargs(UPSERT_MANY, nodes=nodes)


def _bootstrap_nodes(hasura=None, plan=False):
    """
    seed the database with nodes
    """

    hasura = hasura or res.connectors.load("hasura")

    nodes = [{"name": "ONE"}, {"name": "ONE.Meta.Body"}, {"name": "ONE.Meta"}]

    nodes = [Node(**n) for n in nodes]
    if plan:
        return nodes

    return add_nodes(nodes, hasura=hasura)


def node_id_for_name(name):
    return Node(**{"name": name}).id
