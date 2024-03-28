import wrapt
import pandas as pd
from res.utils import logger, slack_exceptions
import res
import functools
import os
from .api import FlowAPI
import requests

_API = FlowAPI


def try_acquire_lock(entity_key, hold_minutes=15, context_key=None):
    """

    try acquire a lock on a resource
    we do not block and we will return the lock if we own it None if we cannot get  the lock
    the lock is held for hold_minutes.
    the context is a qualifier for the key - the lock key will be called context_key:entity_key
    """
    redis = res.connectors.load("redis")
    if context_key:
        key = f"{context_key}:{entity_key}"
    l = redis.get_lock(key=key, blocking=False, timeout=hold_minutes * 60)
    l.acquire()
    if l.owned():
        res.utils.logger.info(f"Acquired lock for resource {key}")
        return l
    res.utils.logger.info(f"Could not acquire lock for resource {key}")
    return None


def lock(key):
    redis = res.connectors.load("redis")
    """
    returns context manager so you can do 
    with res.flows.lock(key) as lock:
        pass
    """
    return redis.get_lock(key)


def get_recent_errors(
    node,
    since_days_ago=3,
    env="production",
    path="s3://res-data-{}/flows/v1/{}/primary/errors/",
):
    """
    fetch errors that are saved in s3 when fatal and uncaught

    check by count
        df.groupby('exception').count()

    reproduce on dev

        some_asset_from_df = df.iloc[0]['asset']
        TOPIC = "res_meta.dxa.prep_pieces_requests"
        kafka[TOPIC].publish(some_asset_from_df, use_kgateway=True)

    """
    from res.utils.dataframes import expand_column

    path = path.format(env, node)

    res.utils.logger.debug(f"Fetching errors from {path}")

    s3 = res.connectors.load("s3")

    def read_with_date(info):
        d = s3.read(info["path"])
        d["last_modified"] = info["last_modified"]
        return d

    df = pd.DataFrame(
        [
            read_with_date(f)
            for f in s3.ls_info(
                path,
                modified_after=res.utils.dates.relative_to_now(since_days_ago),
            )
        ]
    )
    # df.style.set_properties(subset=['exception'], **{'width-min': '500px'})

    df = expand_column(df, "asset")

    return df.sort_values("last_modified")


def submit_event(event, image_tag=None):
    """
    simplest way to test your node on the flow node template
    better than using res-connect which may not have metadata for your flow
    this way is useful for local development
    - push a docker image
    - call this with your tag

    you can also push a PR/build and sync the flow node to your build in argo cd

    """
    import res

    argo = res.connectors.load("argo")
    argo.handle_event(event, context={}, template="res-flow-node")


def import_from(module, name):
    module = __import__(module, fromlist=[name])
    return getattr(module, name)


def get_res_team_for_node_name(node):
    """
    Traverse up to the flows top level namespace looking for a RES_TEAM attribute and take that value
    """
    if not node:
        return
    # always qualify
    if "flows." not in node:
        node = f"res.flows.{node}"
    if node == "res.flows":
        return None

    name = node.split(".")[-1]
    root = ".".join(node.split(".")[:-1])
    m = import_from(root, name)
    if callable(m):
        node = root
        name = node.split(".")[-1]
        root = ".".join(node.split(".")[:-1])
        m = import_from(root, name)

    if hasattr(m, "RES_TEAM"):
        return m.RES_TEAM

    up_level = ".".join(node.split(".")[:-1])
    return get_res_team_for_node_name(up_level)


def get_pydantic_schema_type(name: str, prefix="schemas.pydantic"):
    """
    The schemas are assumed to be installed on the path
    Here we load the schemas as a module and then relatively resolve the type by the namespace
    Examples:
        get_pydantic_schema_type('meta.MetaOneResponse')
        get_pydantic_schema_type('schemas.pydantic.meta.MetaOneResponse')

    Args:
        name: The semi qualified or fully qualified name of the type


    """
    import schemas
    from schemas import pydantic

    schema_base = schemas, pydantic

    name = name.replace("schemas.pydantic.", "")
    atype = name.split(".")[-1]
    namespace = ".".join(name.split(".")[:-1])
    module = f"{prefix}.{namespace}"

    return import_from(module, atype)


def get_node(name: str, prefix="res.flows", auto_resolve_method=True):
    """
    Load a module or a function on a module representing a node op func(event,context)

    It is assumed that the path represents something that can be imported normally
    e.g. from res.flows.flow_node_example import handler

    when the event payload includes a node name, it can be either my_module.func_name if func_name is callabe
    or my_module must contain a conventional 'handler(event,context)' function

    client should check the result is callable (i.e. a func and not a module) or look for a callable function in the returned module

    Args:
    name: the name of the node in the python path full qualified by prefix
    prefix: default to res.flows where we store all nodes but could be anything assuming importable from python path

    Examples:
        name = "flow_node_example.handler"
        func = get_node(name)

    """

    # we assume all nodes are under the flows namespace but can change that
    # if it is already prefixed with whatever the prefix is then remove it for consistency
    if name[: len(prefix) + 1] == f"{prefix}." and prefix is not None:
        name = name.replace(f"{prefix}.", "")

    name = name if prefix is None else f"{prefix}.{name}"

    module = name.split(".")

    method = module[-1]
    module = ".".join(module[:-1])
    try:
        # the method is the special case. we can also return a non callable set of methods with e.g. .handler, .on_error etc.
        f = import_from(module, method)
        if not callable(f) and auto_resolve_method:
            # print("module not callable - try importing a handler")
            return import_from(f"{module}.{method}", "handler")
        return f
    except Exception as ex:
        raise FlowException(
            f"Unable to load handler for flow {module}. Check that {method} can be imported from {module}. 'node handlers' should either be `callable` or a module with a '.handler(event, context)' function: {repr(ex)}",
            ex=ex,
            flow_context=None,
        )


def is_callable(name: str):
    try:
        return callable(get_node(name, auto_resolve_method=False))
    except:
        return False


def try_add_default_generator_for_mapped_handler(mod):
    """
    This can be used to use a default generator
    Note it is used once at inspection when building the template just to say, yes there is a generator
    Then when we actually involve, the flow event processor will look it up again and invoke the function itself
    """
    from res.flows import generator as default_generator

    try:
        # we only do this if there is not already a generator in the module
        if "generator" in mod:
            return mod
        # now we use the metadata based on the attributes to see if we actually want this generator
        meta = mod["handler"].meta

        if meta["mapped"]:
            mod["generator"] = default_generator
    except Exception as ex:
        # there is technically nothing wrong with not being able to find this but need to switch on exception types
        pass

    return mod


def get_node_module(
    name,
    ops=["generator", "handler", "reducer", "on_success", "on_failure"],
    try_default=True,
):
    """
    Loads the flow node functions by convention into a map
    """

    # if its callable, it is the handler
    if is_callable(name):
        return {"handler": get_node(name, auto_resolve_method=False)}

    def try_get_node(op):
        try:
            # we look for the module without resolving to a function
            return get_node(f"{name}.{op}")
        # depends what the exception is actually
        except Exception as ex:
            if op not in ["on_failure", "on_success"]:
                logger.debug(
                    f"When trying to load the nodes op {op} the following often benign warning: {repr(ex)}"
                )
            return None

    # other modules have sets of functions
    attempted = {op: try_get_node(op) for op in ops}

    module = {k: v for k, v in attempted.items() if v is not None}

    if try_default:
        module = try_add_default_generator_for_mapped_handler(module)

    return module


def generator(event, context):
    """
    this is a generic generator in the flow node
    it consumes from a kafka topic as described by the context and returns work
    the flow-node will typically map over generated work and pass them to reducers
    this means that a f(unit)-unit can be mapped over to do work in parallele
    because the pattern assumes taking work from kafka (or even inline in the payload)
    The worker node f can just focus on processing the unit

    If a generator is not used the worker can also interate directly over the work if its a @flow_node
    """
    from res.flows.FlowNodeManager import NodeManager

    name = event.get("metadata", {}).get("name")
    module = get_node_module(name)
    # assume for now
    context = NodeManager.get_node_context(module.get("handler"))

    with FlowContext(event, context) as fc:
        return fc.assets_dataframe.to_dict("records")


def flow_context(obj=None, send_errors_to_sentry=True):
    if obj is None:
        return functools.partial(
            flow_context, send_errors_to_sentry=send_errors_to_sentry
        )

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        event = kwargs.get("event", args[0])
        context = {}
        try:
            return wrapped(event, context)
        except Exception as ex:
            res.utils.logger.debug(
                f"@flow_context: Raising an error in the wrapped function; send to sentry: {send_errors_to_sentry}"
            )
            FlowContext(event, context).log_errors(
                str(ex), exception=ex, send_errors_to_sentry=send_errors_to_sentry
            )
            raise ex

    return wrapper(obj)


def flow_node(obj=None, validate=True):
    """
    This decorator turns a function f(unit)->unit into something that can be executed on the cluster
    The typing on the f(unit) tells the decorator how to connector kafka and stream data into the f
    Also possible to configure how the node is deployed e.g. in single instance or map-reduce modes
    Integrations with other services like airtable are also transparently managed

    -todo check metrics hooks for grafana
          check sentry tags for queue and node monitor (sid, queue, node, argo log location) -> these are partally in the node context returned
          check s3 glue integration for dumping stuff (does not need to be here per se )
          -> test end end to real time looker dash
    """

    from .FlowNodeManager import NodeManager

    # to allow with or without args we trap the case where there is no obj
    if obj is None:
        return functools.partial(flow_node, validate=validate)

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        # for testing we could pass in a unit but this would be read from the stream
        # if we are testing, this can be injected into the flow context otherwise flow context will pull from the stream
        context = NodeManager.get_node_context(wrapped, **kwargs)
        # the event will be passed in by the framework
        event = kwargs.get("event", {})
        unit = None
        ### can pass in a test unit ###
        if kwargs.get("unit") or len(args):
            # the first arg is the unit of the right type - this is for testing
            if len(args) and NodeManager.is_node(args[0]):
                unit = args[0]
            unit = kwargs.pop("unit", unit)
            if unit:
                # for developer experience we inject this into the payload so the flow context can retrieve it
                event["assets"] = [unit.asdict()]
                # we could also bypass the flow context and just call the function if we dont want to test hooks
        ###############################

        with FlowContext(event, context) as fc:
            # we can stream here too but it depends on how we want to do it
            # for testing using the dataframe which buffers and returns
            # but we can use assets if stream_buffered==False on the context
            # for unit in fc.assets_dataframe.to_dict("records"):
            for unit in fc.assets:
                # generate type
                unit = context["input_unit_type"](**unit)
                out_unit = fc.run_node(wrapped, unit)
                if out_unit:
                    fc.publish(out_unit)
                # for testing we can get our last output as a return result but in runtimes we are publishing the output to a response queue ^
                unit = out_unit

            return unit

    return wrapper(obj)


def relay(source, sinks, persist=False, max_errors=1):
    """
    sources and sinks are passed to whatever context the user passes in.
    user can pass in an event too which can be used to disable the stream in test e.g. by specifying assets to use instead of assets on the stream
    - If the event os populated it is possible to set assets for testing and override the stream construction
    - if a cache is added to the context, this can be used to lookup data
    - the first arg of "wrapped" which is a data transformer must data (dict or dataframe)

    If persist==true, we use the provider to store status - Dgraph is the default provider

    Examples:
        @relay("shopifyOrder", {"resMake.orders": "resMake.orders"})
        def shopify_to_make_orders(df, event=None, context=None):
            #transform df here
            return df

    To use multiple sinks, the transformer method should specify the field `res.flows.sink=topic` on each record
    The relay will then relay the data to that topic. For example, logic can be applied to route data to different work queues

    NOTE: currently experimenting with max errors on the connector so it is wrong to use it here too. Keep max errors as -1

    TODO: add buffering logic to take advantage of dataframes


    """

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        event = kwargs.get("event", {})
        context = kwargs.get("context", {})
        context.update({"res.streams": {"source": source, "sinks": sinks}})

        with FlowContext(event, context) as fc:
            error_count = 0

            # if there is no stream just process the batch
            if not fc._streams_enabled:
                return wrapped(fc.assets_dataframe, **kwargs)

            for records in fc.assets:
                try:
                    # TODO: decide on a policy for continuing on errors or max errors
                    # still trying to decide if we should operate at record or dataframe level
                    if isinstance(records, dict):
                        records = pd.DataFrame([records])

                    fc.persist(records)
                    # the args and kwargs belong to the wrapped method
                    fc.publish(wrapped(records, *args, **kwargs))
                    # TODO: apply flow-context metrics
                except Exception as ex:
                    error_count += 1
                    logger.warn(
                        f"Encountered {error_count} error(s) in processing the stream {source} : {repr(ex)}"
                    )
                    if error_count == max_errors:
                        raise Exception(
                            f"Max errors reached when processing {source}"
                        ) from ex

    return wrapper


def configured_flow_handler(name=None):
    """
    This is a **sketch** of a decorator that can be used to wrap queue handlers
    there is some boiler plate that was illustrated in the `etl.relays.my_entity` function
    and as we try different examples, we can settle on a generic queue processor
    there may be some differences for different handlers and we will (a) try to configure what we can in a datbase
    or (b) bifurcate and create separate types of handlers

    The idea is that we would create a function like below which would be wrapped as something that can be called
    in the argo system and also relay default kafka data and save states in dgrahp and airtable etc. as per the demo
    The idea is that each queue is the same in principle by the logic to handle the data is unique per queue

    ```python
    @configured_flow_handler(name='my_entity')
    def my_func(df, flow_context=None):
        #do some work here
        return df
    ```

    """

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        with FlowContext.load(name) as fc:
            fc.logger.info("doing some work such as getting data from topics")
            # fc.pull_assets() from kafka or just take them from the payload
            # fc.assets
            # fc.resources
            # persist state
            # new_to_airtable
            data = wrapped(flow_context=fc, *args, **kwargs)
            fc.logger.info("doing some work such as publishing data to topics")
            # fc.publish_groups
            return data

    return wrapper


def flow_node_attributes(
    name=None,
    flow_name=None,
    description=None,
    memory=None,
    cpu=None,
    disk=None,
    mapped=True,
    stream_buffered=True,
    airtable_id=None,
    airtable_name=None,
    allow24xlg=False,
    slack_channel=None,
    slack_message="",
):
    """
    Core system properties for flow node functions for runtime envs etc.

    check: hasattr(my_handler,'meta')
    the exectutor can pass this information to the flow-node template e.g. to provision memory for containers per function
    This memory could also be passed as an asset attribute if it depends on the data

    There is a max 63 characters in workflow templates generated from the name if the python method name is really big we truncate and add a hash
    the name can be used to provide a friendlier short name

    The flow name and name(node) can be napped to FlowContext.metadata.name, and FlowContext.metadata.node respecticely (they seem inverted but the perspective is just different)
    TODO: these attributes could be used in default naming of FlowContext but currently the calling module and method are used for this
    """

    def _adorned(func):
        func = (
            func
            if slack_channel is None or os.environ["RES_ENV"] != "production"
            else slack_exceptions(func, slack_channel, slack_message)
        )
        func.meta = {
            "airtable_id": airtable_id,
            "airtable_name": airtable_name,
            "memory": memory,
            "cpu": cpu,
            "disk": disk,
            "name": name,
            "description": description,
            "mapped": mapped,
            "allow24xlg": allow24xlg,
        }
        return func

    return _adorned


from .FlowContext import FlowContext, FlowException
from .FlowEventProcessor import FlowEventProcessor, payload_template, FlowWatermarker

__all__ = [
    "FlowContext",
    "FlowException",
    "FlowEventProcessor",
    "payload_template",
]


watermark = FlowWatermarker()

# Flows are collections of edges (i.e a graph)

# events are relayed outside of the system e.g. kafka stream, S3 webhooks etc. these things are the backbone through which all comms must flow
# there is a flowdb
# the event system must provide a flow id in every event payload and the node state is optional. if no node, we assume we are initialized on all source nodes in the flow
# node handlers can fire node transition events on the flow
# it is possibly that some agents will not know their flow but they may know the node... if they known the node we may need other context to resolve the flow
#    for example: if we are processing a certain [type] e.g. a ONE, we can find out what flow the ONE would be on.
#     we could implement flow_resolve.resolve_flow_id(unit, node) and if we fail to this,
#     we can raise a FlowException->unresolved flow id, please specify the flow id in the event payload or implement a flow resolver (ONE, node[type])
# AIRTABLE syncs can be controlled via an event: we have a datafarme we can operate on and we can push changes... this queues up writes back to airtable on a key value stream that can be monitored
# so an handler can not write back to airtale but rather my_table.sync() push events onto the airtable "bot update" topic which queues updates back onto airtable
# this is for user's benefit so they can see whats-what but we already have a source of truth outside of airtable and do not depend on it

# INSTRUMENTATION: These things usually have lineage and/or process data in a pipeline. You want to make it easy yo trace where packets fell through the gaps
# for example, a session id and counters of work items where you can see that N went in and M came out and thus raise an error
# TODO some viz tools for looking at flows using igraph or something might be worth a small amount of effort

# nodes have dependencies and you can get their data with node.[edge_name][target_qualifier].data e.g. node.parent.data or node.dependency.x.data
# #- there is nothing interesting about this except the flowcontext providers indexers for convenience and we have a convention where every node has "data" which is the output of the function (singular)

# if a depency/node is not qualified with a flow name it is assumed to belong to the flow but flows can depend on nodes in other flows.
# This really means there is a derived flow that branch off something else but does not need to re-compute that something else. subtle.

# - flows combine python functions (not lambda or anything like that)
# - functions are often 1:1 with nodes but "node handler functions" can call other functions
#  - for example if there is a node called `perform_nest` there is a function called `nest`
#  - however `nest` lives in `learn.optimization` which does not map to the FQN of the node
# - these are just food for thought on convention. For now the flow handler is a unique thing
#   and it calls into python functions as required
