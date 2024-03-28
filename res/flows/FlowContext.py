from res.utils import dates
from res.utils.logging import logger
from types import GeneratorType
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from res.connectors import load as load_connector
import json
from res import RES_DATA_BUCKET
import hashlib
from res.utils.dates import utc_now
import res
import inspect
import traceback
from .FlowNodeManager import NodeManager
from res.utils.strings import compress_string, decompress_string
from res.utils.env import ARGO_WORKFLOW_LOGS
from res.flows import get_pydantic_schema_type
import warnings
from res.connectors.redis.RedisConnector import RedisConnectorTable


try:
    from shapely.wkt import loads as shapely_loads
except:
    pass

VERSION = "v1"
# root location where all flow data are written to
FLOW_CACHE_PATH = f"s3://{RES_DATA_BUCKET}/flows/{VERSION}"
USE_KGATEWAY = True


class FlowValidationException(Exception):
    def __init__(self, message, **kwargs):
        super().__init__(message)
        self._kwargs = kwargs

    @property
    def tags(self):
        # do some stuff
        return self._kwargs.get("tags")

    @property
    def tags_string(self):
        tags = self.tags
        if len(tags):
            return ",".join(tags)


class FlowException(Exception):
    def __init__(self, message, ex=None, flow_context=None, experiment=None, node=None):
        """
        Capture context for the flow
        """
        super().__init__(message)
        self._message = message
        self._ex = ex
        self._context = flow_context
        self._node = node
        self._experiment = experiment

    def __str__(self):
        return self._message

    def _repr_(self):
        return f" {self._message} : {self._ex} @ {self._node}, {self._experiment} with context {self._context}"


class AssetFlaggedFlowException(FlowException):
    def __init__(self, message, ex=None, flow_context=None):
        """
        Capture context for the flow
        """
        super().__init__(message, ex, flow_context)
        self._fc = flow_context


class _ServiceContext(object):
    """
    Wrapper around services
    """

    def __init__(self, context=None):
        self._context = context

    def __getitem__(self, key):
        options = {}
        return load_connector(key, **options)


class NamedFlowAsset(object):
    def __init__(self, name, data):
        self._name = name
        self._data = data

    @property
    def named_tuple(self):
        return self._name, self._data

    @property
    def name(self):
        return self._name

    @property
    def data(self):
        return self._data


def flow_node_log_path():
    workflow = os.environ.get("METADATA_ARGO_WORKFLOW_NAME", "[WORKFLOW-NAME]")
    pod = os.environ.get("METADATA_ARGO_POD_NAME", "[POD-NAME]")
    return f"{ARGO_WORKFLOW_LOGS}/{workflow}/{pod}"


class AssetProcessingContextManager:
    """
    A wrapper for processing item by item and handling item level failures and continue
    """

    def __init__(self, asset, **kwargs):
        self._asset = asset
        self._options = kwargs

    @property
    def asset(self):
        # if its typed load the type or else just use untyped dict
        return AssetProcessingContextManager.object_from_payload(self._asset)

    def __enter__(self):
        # log that we are trying to do something with the asset
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type:
            res.utils.logger.warn(
                f"handling errors somehow {exc_traceback}, {exc_value} {traceback.format_exc()}"
            )
        return self

    @staticmethod
    def object_from_payload(payload):
        if "res_entity_type" in payload:
            constructor = get_pydantic_schema_type(payload.get("res_entity_type"))
            return constructor(**payload)
        raise Exception(
            f"Could not load type {payload} - make sure there is a class that can be loaded based on the value of `res_entity_type` in the payload or do not used the asset context and work with dicts"
        )


class FlowContext(object):
    """
    Flow context is a simple wrapper around any activity on the cluster triggered by FlowApi payloads
    Payloads above version "v0" are validated by the schema in the FlowEventProcessor and some examples exist in the test folders

     Example payload:
      res_tests/.sample_test_data/events/sample_function_event.json

    After loading an event into a python dictionary, flows are constructed as context managers,

     with FlowContext(event) as fc:
        ...

    - FlowContext encapsulates connector loading and flow semantics
    - Data are saved on the S3 datalake and in dgraph FlowDB either as payload metadata or individual "nodes" with in a flow
    - Configuration parameters can be loaded from the database
    - Assets are consumed from payloads or kafka streams

    Metrics:
     metrics are logged to flows.flow_name.node_name.data_group.verb.status

    Inferences:
     if the flow name or node name are not added to metadata we infer as the calling module and the calling method.
     For example, if we have something like res.flows.etl.airtable.my_node_handler and inside this function we use flow context,
     The flow_name will be etl.airtable and the node will be my_node_handler.
     It is recommended that you explicitly pass metadata in payloads or be careful with python conventions

    TODO: some work to drilling into assets and inferring certain things like data_groups and asset keys and value conventions
    """

    def __init__(
        self, event, context=None, node=None, data_group="any", parallel_set_id=None
    ):
        self._context = _ServiceContext(context)
        # node can be added in the event
        self._node = node
        # todo infer this so not needed
        self._data_group = data_group
        self._args = self._get_settings()
        self._event = event
        self._metadata = event.get("metadata", {})
        # this is for shared state for some distributed queue processing contexts
        self._parallel_set_id = parallel_set_id or self._metadata.get("parallel_set_id")
        # the node is by default just the handler of the flow but it can be passed by metadata or in the constructor (overrides)
        # we use the calling method as the flow context name which is often what we want
        self._args.update(self._event.get("args", {}))
        self._assets = self._event.get("assets")
        # context is passed to these but we should improve how context works combined with service context
        self._generate_names_and_defaults(context)
        self.setup_streams(event, context)
        self.suppress_failures = False
        self._git_hash = self._metadata.get("git_hash", "NO_HASH")
        self.validations = []

        # try:
        #     # once this is sent, anything in flow context can add new spans
        #     self._otel_tracer = self.get_open_telemetry_tracer()
        #     ctx = None
        #     with self._otel_tracer.start_as_current_span("some_span", ctx) as span:
        #         res.utils.logger.info(
        #             f"testing tracing in flow context {self._task_key}"
        #         )
        #         span.set_attribute("flow_session_key", self._task_key)
        #         span.set_attribute("flow_git_hash", self._git_hash)

        # except:
        #     res.utils.logger.info(traceback.format_exc())
        # for all flow context we will do this - need to think more about it

    def _generate_names_and_defaults(self, context, **kwargs):
        """
        All naming conventions handled here - when settled add unit test as this drives folder paths, metric names etc i.e. its important

        """
        context = context or {}
        # any flow can define where its runbook is so that on error someone knows what to do
        self._help_link = "http://TODO/path/to/coda/docs/for/this/flow"

        self._calling_module = "FlowContext"

        try:
            # two levels up - this does not work for decorated objects
            frm = inspect.stack()[2]
            self._calling_module = inspect.getmodule(frm[0]).__name__.replace(
                "res.flows.", ""
            )

            # frm[3] is the method name of the caller - this is our default node name for FlowContext
            self._node = self._node or self._metadata.get("node", frm[3])
            if "op" in context:
                self._node = context["op"]
        except:
            pass

        self._flow_id = self._resolve_flow_id(self._event)
        self._experiment_id = self._metadata.get("version", "v0")

        self._task_key = self._event.get("task", {}).get("key")

        # this is just back compatibility
        if self._task_key is None:
            self._task_key = self._args.get("jobKey")

        # this is a way to take it from the workflow if we have not specified it in the payload
        # for example on cron job errors this will be set
        if self._task_key is None:
            self._task_key = self._metadata.get("argo.workflow.name")

        self._nodes = {}
        self._cache = {}
        self._started_time = None
        self._result = None
        self._logged_error_count = 0
        self._queue_name = context.get("input_queue_name")
        self._node = self._metadata.get("node")

    def get_open_telemetry_tracer(self):
        """

        As an example the meta data would store things that can populate the env vars

            import res
            e = {'apiVersion': 'resmagic.io/v1',
            'kind': 'resFlow',
            'metadata': {'name': 'make.production.inbox.handler',
            'version': 'primary',
            'node': 'production-inbox'},
            'assets': [],
            'task': {'key': 'handler-res583816da1b'},
            'args': {}}

            #when this is constructed locally or after invoking the flow it adds the tracer
            fc = res.flows.FlowContext(e)


        """
        from res.utils.telemetry import get_app_tracer

        os.environ["RES_NODE"] = self._node or "unknown-node"
        os.environ["RES_APP_NAME"] = self._flow_id
        os.environ["RES_NAMESPACE"] = self._flow_id.split("-")[0]

        # a way to get context from the event TODO

        return get_app_tracer()

    def log_asset_exception(self, asset, ex, datagroup="id"):
        try:
            path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/errors/{self._task_key}/{asset.get('id', 'request')}_{asset.get('unit_key', 'unit')}.json"

            d = {
                "asset": asset,
                "exception": res.utils.ex_repr(ex),
                "trace": traceback.format_exc(),
                "validation_trace": self.validations,
                "session_key": self._task_key,
            }
            res.utils.logger.debug(f"writing error payload to {path}")
            self.connectors["s3"].write(path, d)

            # post asset fail kafka
            # metric increment
            self.metric_incr(
                verb="EXITED", group=asset.get(datagroup, "asset"), status="EXCEPTION"
            )
        except Exception as fex:
            res.utils.logger.warn(f"failed: {repr(fex)}")

    @staticmethod
    def object_from_payload(payload):
        return AssetProcessingContextManager.object_from_payload(payload)

    @staticmethod
    def restore(key, name, flow_version="expv2", api_version="v0"):
        """
        Restore a flow context from name and version with possible persisted payload
        The expv2 is not a general name but for testing this is only used in nesting - this should be renamed to some sort of primary name ...

        Example:
            fc = FlowContext.restore("bcict-0009330405-2021-07-12-18-02-31", name="dxa.printfile")
        """
        path = f"s3://{RES_DATA_BUCKET}/flows/{api_version}"
        path = f"{path}/{name}/{flow_version}/.meta/{key}/payload.json"
        s3 = res.connectors.load("s3")
        if s3.exists(path):
            return FlowContext(s3.read(path))
        else:
            logger.warn(
                f"The path {path} does not exist - check the restore values - returning and empty flow context"
            )
            return FlowContext({})

    @property
    def log_path(self):
        return flow_node_log_path()

    @property
    def log_path_presigned(self):
        """
        the logs are in S3 - this is a good way to provide a url that is downloadable or linkable - makes sense for single log files
        default expiry=3600-seconds
        """
        return res.connectors.load("s3").generate_presigned_url(
            self.log_path,
        )

    @property
    def git_hash(self):
        return self._git_hash

    @property
    def pod_name(self):
        return os.environ.get("METADATA_ARGO_POD_NAME")

    @property
    def argo_logs_location(self):
        """
        Based on convention this is where we expect the logs to be
        This is only true of callable workflows by concentions so we should flesh out other cases TODO:
        """
        # todo in future we can move logs to match the env on s3 for argo dev
        env_part = "dev" if os.environ["RES_ENV"] != "production" else "production"

        return f"s3://argo-{env_part}-artifacts/{self._task_key}"

    @property
    def logs(self):
        """
        Assuming the convention for where logs are, just concat them all into one string list
        """
        s3 = self.connectors["s3"]
        return [
            line
            for f in s3.ls(self.argo_logs_location)
            for line in s3.read_text_lines(f)
        ]

    def _get_settings(self, update_args=False):
        """
        We can add settings for the app which can be used by the flow node and added to args
        settings are stored for RES_NAMESPACE/RES_APP_NAME in key value pairs and can be queried individual or sets for the app
        Here this considers overriding settings in
        """
        # app name will probably need to be passed in from flow context more explicitly e.g. res-flows/flow
        # settings = ResConfigClient().get_config_value("settings-todo")
        # if update_args:
        #  self._args.update(settings)
        return {}

    @property
    def shared_cache(self) -> RedisConnectorTable:
        """
        a shared cache for storing state between jobs e.g. for each size in a style, what contracts failed in each size for the set id
        """
        redis = res.connectors.load("redis")
        if self._parallel_set_id:
            return redis[f"FLOW_CONTEXT_CACHE_{self._flow_id.upper()}"][
                self._parallel_set_id
            ]

    def get_shared_cache_for_set(self, record_id):
        redis = res.connectors.load("redis")
        return redis[f"FLOW_CONTEXT_CACHE_{self._flow_id.upper()}"][record_id]

    def setup_streams(self, event, context):
        """
        Experimental: setup a stream connection from sources to sings. Conventions around topic names
        We can use this to automatically consume assets from a stream rather than a payload and then publish
        Results to one or more sink streams
        """
        self._streams_enabled = False
        self._sinks = {}
        self._consumer = None

        context = context or {}

        kafka = self.connectors["kafka"]
        # dg = self.connectors["dgraph"]

        if "input_queue_name" in context:
            source = context["input_queue_name"]
            sink = context["output_queue_name"]
            context.update(
                {"res.streams": {"source": source, "sinks": {"default": sink}}}
            )

        stream_info = context.get("res.streams", {}) if context else {}

        # if assets are supplied this overrides the use of streams
        # this is to allow for dual use of functions but also to support testing when only streams are used in practice
        # for example, for unit tests we can pass the payload into the asset collection and we do not call kafka
        def make_sink(name):
            """
            factory to create something that sends data to kafka
            """
            res.utils.logger.debug(f"Making sink for topic {name}")
            topic = kafka[name]

            def f(data):
                topic.publish(data, use_kgateway=USE_KGATEWAY, coerce=True)

            return f

        has_assets = False
        if self._assets is not None and len(self._assets) and len(self._assets[0]):
            has_assets = True

        if stream_info and not has_assets:
            self._source = stream_info.get("source")
            logger.debug(
                f"Enabling kafka streams because a stream spec {stream_info} was added to the Flow Context"
            )

            self._consumer = kafka[self._source]
            self._sinks = stream_info.get("sinks", {})
            first_key = list(self._sinks.keys())[0]
            first = self._sinks.get(first_key)
            self._sink = make_sink(self._sinks.get("default", first))
            # the kafka client is iterable and will read of the kafka stream for this app_name's cursor
            self._assets = self._consumer
            self._streams_enabled = True

        self._num_sinks = len(self._sinks)

    def __repr__(self):
        return str(self._event)

    @property
    def key(self):
        return self._task_key

    @staticmethod
    def from_queue_name(
        queue,
        task_key=None,
        data_key="any",
        version="primary",
        api_version="resmagic.io/v1",
    ):
        event = {
            "apiVersion": api_version,
            "kind": "resFlow",
            "metadata": {
                "name": queue,
                "version": version,
            },
            "assets": [],
            "task": {"key": task_key or res.utils.res_hash()},
            "args": {},
        }

        return FlowContext(event, data_group=data_key)

    @staticmethod
    def load(
        name,
        provider="dgraph",
        flow_handler_entity_name="flow_contexts",
        validate=False,
    ):
        """
        Experimental: can we load flows from a database of configurations: do we want to?
        The concept here is that a topology of flow handlers could be configured and loaded in this way
        """
        dg = load_connector(provider)
        rf = dg.get_entity(type=flow_handler_entity_name, name=name)
        # the event payload stores all the default events and metadata to create a flow
        ev = rf["event"]
        # the context object stores inforamation about sys e.g. kafka topics etc.
        ctx = rf["context"]
        if isinstance(ev, str):
            ev = json.loads(ev)
        if isinstance(ctx, str):
            ctx = json.loads(ctx)

        return FlowContext(ev, ctx)

    def _asset_as_payload(
        self, asset, compress=False, parallel_set_id="queue.id", metadata_provider=None
    ):
        """
        the parallel set id is a property if the asset such as the id field
        we can provide other attributes as a function of the asset e.g. sometimes we add more memory when processing certain sample assets
        """

        e = dict(self._event)
        parallel_set_id = asset.get(parallel_set_id)
        m = {k: v for k, v in e["metadata"].items()}

        if parallel_set_id:
            m["parallel_set_id"] = parallel_set_id
        m.update({} if not metadata_provider else metadata_provider(asset))
        # for testing state only
        m["asset.key.attribute"] = asset.get("sku")

        e["metadata"] = m
        if "memory" in m:
            # the new contract to add memory to the event - we should actually just try using item.metadata.memory in the workflow instead
            e["memory"] = m["memory"]

        e["metadata"] = m
        # each asset is its own event
        e["assets"] = [asset if not compress else compress_string(json.dumps(asset))]

        return e

    def _config_as_payload(self, a):
        e = dict(self._event)
        e["args"] = a
        e["assets"] = []
        return e

    def asset_list_to_payload(
        self, assets, compress=False, parallel_set_id=None, metadata_provider=None
    ):
        if not isinstance(assets, list):
            assets = [assets]

        return [
            dict(
                self._asset_as_payload(
                    a,
                    compress=compress,
                    parallel_set_id=parallel_set_id,
                    metadata_provider=metadata_provider,
                )
            )
            for a in assets
        ]

    def asset_list_to_configured_payload(self, assets):
        if not isinstance(assets, list):
            assets = [assets]

        return [self._config_as_payload(a) for a in assets]

    def persist(self, data):
        """
        If persistence is enabled (on streams) we can store whatever assets we are processing to database like dgraph
        """
        print("test")
        # if self._stream_store:
        #     try:
        #         self._stream_store.update(data)
        #     except Exception as ex:
        #         message = f"Failed to write to stream source - {repr(ex)}"
        #         raise Exception(message) from ex

    def publish(self, data, **kwargs):
        """ "
        Publish whatever data we have processed e.g. to a kafka stream
        """
        if self._streams_enabled:
            if isinstance(data, dict) or isinstance(data, pd.DataFrame):
                self._sink(data)
            else:
                # anything with an asdict can be sent e.g. NodeQueue entries
                self._sink(data.asdict())

    def _publish_dataframe(self, data, **kwargs):
        """
        publish the dataframe either grouped by the sink or as a full batch
        the kafka producer topic is labeled in the data rows
        """
        if self._streams_enabled:
            if "res.flows.sink" in data.columns:
                for topic, data in data.groupby("res.flows.sink"):
                    self._sinks[topic].publish(data, **kwargs)
            else:
                for topic, producer in self._sinks.items():
                    producer.publish(data, **kwargs)
        else:
            logger.debug("streams are not enabled - returning data")

        return data

    def _publish_record(self, data, **kwargs):
        """
        publish just one record to the given sink
        """

        def pop_sink(data):
            sink = data.get("res.flows.sink", "*")
            if "res.flows.sink" in data:
                data.pop("res.flows.sink")
            return sink

        if self._streams_enabled:
            sink = pop_sink(data)
            for topic, producer in self._sinks.items():
                if sink in [topic, "*"]:
                    producer.publish(data, **kwargs)
        else:
            logger.debug("streams are not enabled - returning data")

        return data

    @property
    def args(self):
        """
        read write access to the args dictionary in the event
        """
        return self._args

    @property
    def typed_assets(self):
        """
        this is an experimental way to load typed messages from the inputs
        it suffers from needing a good type loader and also failing parsing gracefully in a transaction
        """
        constructor = None
        atype = self._metadata.get("entity_type")
        try:
            constructor = eval(f"schemas.pydantic.{atype}")
        except:
            # try construct from some globals later
            res.utils.logger.warn(
                f"Unable to construct a type {atype} - is the type in the pydantic schema (the only supported location): {traceback.format_exc()}"
            )
        if constructor:
            return [constructor(**a) for a in self.decompressed_assets]
        return self.decompressed_assets

    @property
    def decompressed_assets(self):
        """
        if they were packed on the way e.g. in the generator payload, we can unpack them here
        assumed to be json dictionaries by convention
        - a mode where we just pass the dict if it is one. it should not be but just in case
        """
        return [
            json.loads(decompress_string(asset)) if isinstance(asset, str) else asset
            for asset in self._assets
        ]

    @property
    def decompressed_typed_assets(self):
        """
        if a payload has a res entity type it will be cast to the string typed object
        this does some validation and maybe fail - its really just syntactic sugar
        its better to not do it here in case the iterator blows up trying to coerce so we prefer to


            for asset in fc.decompressed_assets:
                with fc.asset_processing_context(asset) as ac:
                    ....

        note after some testing we can normally assume that the kafka schema will pass non breaking things to this
        in which we can use

            for asset in fc.decompressed_typed_assets:
                ...


        """
        return [
            AssetProcessingContextManager.object_from_payload(p)
            for p in self.decompressed_assets
        ]

    @property
    def assets(self):
        """
        Using a convention of the flow schema - get the assets property
        this is read only so we return it as read from the event and do not set it locally
        if streams are enabled, the assets are read of a kafka stream rather than the event payload
        """
        # back compat
        if self._assets == None:
            return self.args.get("assets", [])

        return self._assets

    @property
    def assets_dataframe(self):
        if self._streams_enabled:
            # this is the batching mode - need to think this out a bit more
            res.utils.logger.info(f"Will return a batch of records as per settings")
            return self._consumer.consume()

        df = pd.DataFrame(self.assets)
        # try unmarshall from json??

        for c in df.columns:
            try:
                if "geometry" in c:
                    df[c] = df[c].map(shapely_loads)
            except:
                pass

        return df

    @property
    def logger(self):
        return logger

    @property
    def args(self):
        return self._event.get("args", {})

    @property
    def nodes(self):
        return self._nodes

    @property
    def connectors(self):
        return self._context

    @staticmethod
    def get_config_for_event(event, environment=None):
        environment = environment or os.environ.get("RES_ENV", "development")
        from res.utils.configuration.ResConfigClient import ResConfigClient

        config = ResConfigClient(
            environment,
            app_name=event.get("metadata", {}).get("name", "res_data"),
            namespace="res_data_flows",
        )

        return config

    def _handle_marshall(self, df):
        import numpy as np

        data = df.copy()
        if isinstance(data, pd.DataFrame):
            # need to think more about this but resSchema will all be supported and IO will work in a specific way for supported types
            # but just handling specific cases as work around
            for c in data.columns:
                if "geometry" in c:  # or we may do this for all object types
                    sample = data.iloc[0][c]
                    # numpy arrays are another way to store geoms but they are not supposede to be strings
                    if not isinstance(sample, np.ndarray):
                        data[c] = data[c].astype(str)
        return data

    def _handle_unmarshall(self, data):
        import numpy as np

        if isinstance(data, pd.DataFrame):
            # need to think more about this but resSchema will all be supported and IO will work in a specific way
            # but just handling specfic cases as work around
            for c in data.columns:
                if "geometry" in c:  # or we may do this for all object types
                    sample = data.iloc[0][c]
                    # numpy arrays are another way to store geoms but they are not supposede to be strings
                    if not isinstance(sample, np.ndarray):
                        data[c] = data[c].map(shapely_loads)
        return data

    def _write_output(self, result, node, key, **kwargs):
        if result is None:
            return

        output_type = (
            "dataframe" if "output_type" not in kwargs else kwargs["output_type"]
        )

        # TODO: HARD CODING
        output_type = "image_collection"
        # logger.debug("infer op contract for omitted args see http://githublink/docs/flows/contracts#ops")

        if output_type == "image_collection" and not isinstance(result, pd.DataFrame):
            # we assume by contract that things are generators for many types but this is a WIP
            # if not isinstance(result, GeneratorType):
            #     result = [result]

            for i, element in enumerate(result):
                # special case if we know this thing
                name = i
                if isinstance(element, NamedFlowAsset):
                    name = element.name
                    element = element.data

                try:
                    path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}/{key}/{name}.png"
                    logger.info(f"Writing element of type {type(element)} to {path}")
                    # TODO: think how generally to relay args correctly to low level connectors
                    image_kwargs = {}
                    if "dpi" in kwargs:
                        image_kwargs["dpi"] = kwargs["dpi"]
                    self.connectors["s3"].write(path, element, **image_kwargs)
                except:  # TODO: this is really cheating but we will add some semantics later
                    path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}/{key}/{name}.feather"
                    logger.info(f"Writing element of type {type(element)} to {path}")
                    element = pd.DataFrame(element)
                    element.columns = [f"column_{c}" for c in element.columns]
                    self.connectors["s3"].write(path, self._handle_marshall(element))

        else:  # default
            # we could use a parquet file or whatever if we wanted to
            output_name = kwargs.get("output_filename", "output.feather")
            path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}/{key}/{output_name}"
            logger.info(f"Writing element of type {type(result)} to {path}")
            result = pd.DataFrame(result)
            if len(result) > 0:
                result.columns = [f"{c}" for c in result.columns]
                self.connectors["s3"].write(path, self._handle_marshall(result))

    def get_node_data_by_index(self, node, keys, transformer=None, index=0, **kwargs):
        # just scan and return the item - wasteful but fewer assumptons
        for i, rec in enumerate(
            self.get_node_data(node=node, keys=keys, transformer=transformer, **kwargs)
        ):
            if i == index:
                return rec

    def get_node_path_for_flow_and_key(self, flow, node, key):
        return f"{FLOW_CACHE_PATH}/{flow}/{self._experiment_id}/{node}/{key}"

    def get_node_path_for_key(self, node, key):
        return self.get_node_path_for_flow_and_key(self._flow_id, node, key)

    def get_node_data(self, node, keys, transformer=None, **kwargs):
        if not isinstance(keys, list):
            keys = [keys]
        # TODO: kwargs would allow some sort of filtering
        # TODO: the job key should be in the context and needed as a param
        s3 = self.connectors["s3"]
        root = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}"
        logger.debug(f"Loading node data for node path {root}")
        for key in keys:
            path = f"{root}/{key}"
            files = list(s3.ls(path))
            # TODO: there is a chance we have some corrupted sets i.e. stale data that needs to be purged - ignoring for now
            logger.info(f"Fetching {len(files)} files from {path}...")
            for file in files:
                # TODO: temp hack to avoid a problem loading this file by mistake in nesting
                if "mask.png" in file:
                    continue
                # logger.debug(f"fetching file {file}")
                data = s3.read(file)
                data = self._handle_unmarshall(data)
                yield data if transformer is None else transformer(data)

    def clear_node_data(self, node, keys, older_than=None, **kwargs):
        """
        specify keys under node data to delete - this is used to purge data that were stored for any node for that particular experiment etc.
        """
        s3 = self.connectors["s3"]
        root = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}"
        for key in keys:
            path = f"{root}/{key}"
            files = list(s3.ls(path, modified_before=older_than))
            logger.debug(
                f"clearing {len(files)} node data files from {path} using the watermark {older_than}"
            )
            s3.delete_files(files)

    @staticmethod
    def _sort_files(files, sorter=None):
        """
        Pass a named sorter or any callable f(filename)->ordered_key
        """
        if sorter == "file_index":
            # sort files by a supposed integer representation of the names
            files.sort(key=lambda f: (int((Path(f).stem))))
        elif callable(sorter):
            files.sort(key=sorter)
        return files

    def get_node_file_names(self, node, keys, transformer=None, **kwargs):
        if not isinstance(keys, list):
            keys = [keys]
        s3 = self.connectors["s3"]
        root = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}"
        counter = 0
        all_files = []
        for key in keys:
            path = f"{root}/{key}"
            files = list(s3.ls(path))
            all_files += files
            counter += len(files)

        all_files = FlowContext._sort_files(all_files, kwargs.get("sorter"))

        return all_files

    def get_node_data_count(self, node, keys, transformer=None, **kwargs):
        s3 = self.connectors["s3"]
        root = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/{node}"
        counter = 0
        for key in keys:
            path = f"{root}/{key}"
            files = list(s3.ls(path))
            counter += len(files)
        logger.debug(f"Counted {counter} items for node data at root {root}")
        return counter

    def run_node(self, node, unit, **kwargs):
        """

        Main purpose of this is to catch errors for sentry and to log metrics for grafana

        Errors can report things like this if configure

         tags={
                "flow_version": self._experiment_id,
                "flow_node_name": self._node,
                "flow_name": self._flow_id,
                "data_group": self._data_group,
                "help_link": self._help_link,
                "task_key": self._task_key,
                "s3_logs": self.argo_logs_location,
                "queue_name": self._queue_name,
                "queue_request_id: kwargs.get('queue_request_id'),
                "unit_key": kwargs.get('unit_key')
            },


        Grafana will log all events
        - For any node/queue how many events have we seen entering in the TODO queue
        - How many of them have existed without validation errors (currently flow context catches validation errors in the unit)
        - When there are exceptions

            - match: "flows.*.*.*.*"
              name: "flows"
                labels:
                    flow: "$1" e.g. 3d
                    node: "$2" e.g. queue or node name
                    data_group: "$3"  unit key
                    verb: "$4"  entered /exited
                    status: "$5" ok/ failure / warnings

        """

        validate = kwargs.get("validate", True)

        # these special types expose queue id and unit keys as part of the abstraction
        queue_request_id = unit.id
        unit_key = unit.unit_key

        try:
            self.metric_incr("ENTERED", group=unit_key, status="OK", with_gauge=False)
            self.metric_gauge("PRESENT", group=unit_key, status="OK", value=1)

            validation_errors = unit.validate() if validate else None
            if not validation_errors:
                ###################
                result = node(unit)
                ###################

                NodeManager.publish_queue_status(unit)
                self.metric_incr(
                    "EXITED", group=unit_key, status="OK", with_gauge=False
                )
                self.metric_gauge("PRESENT", group=unit_key, status="OK", value=0)
                return result
            else:
                self.validations += validation_errors
                # we could log specific validation tags if we wanted to count them
                res.utils.logger.warn(
                    f"The unit {unit} is invalid - will not return a response"
                )
                NodeManager.publish_queue_status(unit, validation_errors)
                self.metric_incr("EXITED", group=unit_key, status="FAILED")
                return

        except Exception as ex:
            self.metric_incr("EXCEPTION", group=unit_key, status="FAILED")
            self.log_errors(
                f"An error ocurred in node <{self._node}> when processing queue <{self._queue_name}>",
                exception=ex,
                queue_request_id=queue_request_id,
                unit_key=unit_key,
                send_errors_to_sentry=True,
            )

    def apply(self, node, func, data, key, **kwargs):
        """
        A way to apply a function @ a node.
        Functions are applied to data such as images,dataframes, dictionaries, collections etc.
        And the args are passed in as parameters. All of this can be logged and we can inject stuff
        Parameters for example can be loaded from a database and we can log what parameters are used
        Functions are considered to be "node handlers" so each function is a node in a DAG/state machine

        Key is used to create a save partition e.g. for a particular image or other unit being processed
        """
        # load args from FlowDB
        configured_args = self.args
        configured_args.update(kwargs)
        logger.info(
            f"Running node step {node} with args {configured_args} for key {key}"
        )

        try:
            result = func(data, **configured_args)

            # extract lists from generators as we want to hold onto the collection
            if isinstance(result, GeneratorType):
                result = list(result)

            self._write_output(result, node, key, **configured_args)

            return result
        except Exception as ex:
            # res.utils.logger.debug(f"failed when executing function: {repr(ex)}")
            fex = self.handle_exception_for_node(node, key, ex, **kwargs)
            raise fex

    def handle_exception_for_node(self, node, key, ex, **kwargs) -> Exception:
        """
        TODO want to work out a flow for error routing
        """
        error = {
            "exception": repr(ex),
            "key": key,
            "node": node,
            "task": self._task_key,
            "flow_key": self._flow_id,
            # get what is effectively the ONE-number from the assets
            # need to abstract this a little better but in flows today the assets are processed by value
            "asset": self._asset_by_value(key),
        }
        status = "any"
        if isinstance(ex, ValueError):
            root = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/value-errors/{node}/{key}"
            path = (
                f"{root}/{hashlib.md5(utc_now().isoformat().encode()).hexdigest()}.json"
            )
            error["root"] = root

            logger.debug(
                f"FlowContext handling ValueError at node {node} for key {key} - saving details to {path}"
            )

            self.connectors["s3"].write(
                path,
                error,
            )

            status = "value_errors"

        exception_callback = kwargs.get("exception_callback")
        if callable(exception_callback):
            exception_callback(ex)

        self.metric_incr(verb="failed", group=self._data_group, status=status)
        self.publish_asset_status(
            self._asset_by_value(key), message=repr(ex), node=node, status="FAILED"
        )

        # increment the counter to say this error is handled - we can make this more sophisticated but this will stop the exit handler from firing
        self.logger.info(
            "incrementing error handle count in on-node-errors so the exit handler does not write errors"
        )
        self._logged_error_count += 1

        return ex

    def publish_asset_status(self, asset, node, message, status, **kwargs):
        """
        For the asset status contract
        https://coda.io/d/Platform-Documentation_dbtYplzht1S/Event-contracts_suH9L#Asset-assignment_turCy/r6
        Try to fill in what values we can
        Key at least is required

        The type can be within the asset or just passed in the kwargs
        """
        try:
            # refactor to put this name somewhere better
            topic = "res_infrastructure.flows.asset_status"
            kafka = self.connectors["kafka"][topic]
            message = {
                "key": asset.get("key", asset.get("unit_key")),
                # for an easy life, if the value is not supplied used the key
                "value": asset.get("value", asset.get("key")),
                "type": asset.get(
                    "unit_type",
                    asset.get("type", kwargs.get("asset_type", "make-asset")),
                ),
                # testing value
                "prev_key": asset.get(
                    "id",
                    asset.get(
                        # todo flesh out the contracts a bit more for this sort of thing - this is just a nesting use case
                        "prev_key",
                        asset.get("intelligent_one_marker_file_id", ""),
                    ),
                ),
                "status": status,
                "message": message or "",
                "node": node or "unknown",
                "task": self._task_key or "",
                "flow": self._flow_id or "unknown",
                "timestamp": dates.utc_now_iso_string(),
            }
            kafka.publish(message, use_kgateway=True, coerce=True)
        except Exception as ex:
            logger.warn(f"failed to publish asset status to kafka - {repr(ex)}")

    def _asset_by_key(self, key):
        for a in self.assets:
            k = a.get("key")
            # fuzzy match to suport stems but really we should be more strict here
            if key in k:
                return a
        return None

    def _asset_by_value(self, key):
        if self.assets:
            for a in self.assets:
                k = a.get("value")
                # fuzzy match to suport stems but really we should be more strict here
                if k and key in k:
                    return a
        return None

    def _nodes(self):
        pass

    def _resolve_flow_id(self, event):
        """
        The flow name/id is resolved either from metadata or from the python context when payload is lacking.
        """

        name = self._metadata.get("name", self._calling_module)
        # todo we could use the metadata in other ways
        # TODO: regex for valid names
        return name.replace(".", "-") if name is not None else name

    def asset_processing_context(self, asset):
        atype = self._metadata.get("entity_type")
        return AssetProcessingContextManager(asset, atype=atype)

    def save_queue_ids(self, map):
        path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/.meta/{self._task_key}/job_keys.json"
        res.utils.logger.info(f"Saving queue records to {path}")
        if len(map):
            res.utils.logger.debug(f"Saving {path}")
            res.connectors.load("s3").write(path, map)
        return path

    def restore_queue_ids(self):
        path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/.meta/{self._task_key}/job_keys.json"
        res.utils.logger.info(f"Restore queue records from {path}")
        try:
            return res.connectors.load("s3").read(path)
        except:
            return {}

    def __enter__(self):
        # todo remove on exit
        warnings.filterwarnings("ignore", category=FutureWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

        self._started_time = datetime.utcnow()
        logger.debug(
            f"Flow metrics to {self._metric_name(verb='<VERB>', group='<DATA_GROUP>', status='<STATUS>')}"
        )

        self.metric_incr(verb="invoked")

        # TODO: write the payload to S3 for posterity - these may become kafka assets - see flow context restore
        path = f"{FLOW_CACHE_PATH}/{self._flow_id}/{self._experiment_id}/.meta/{self._task_key}/payload.json"
        if self._resolve_flow_id and self._event:
            # logger.debug(
            #     f"Entering flow {self._flow_id}.{self._experiment_id} - writing event payload to {path}"
            # )

            try:
                self.connectors["s3"].write(path, self._event)
            except:
                pass

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Exit handler for the context manager
        """
        completed = datetime.utcnow()
        duration = (completed - self._started_time).seconds

        # TODO some sort of conditions on if we should write this or not
        # in this case if the app has already written errors we dont want to spam with more (by default at least)
        if self._logged_error_count == 0 and exc_type:
            self.log_errors(
                f"Unhandled exception of type {str(exc_type)} in flow context for {self._flow_id}.{self._experiment_id} - logging errors...",
                exception=exc_value,
                errors=repr(traceback.format_tb(exc_traceback)),
            )

        if exc_type:
            # traceback.format_tb(exc_traceback))
            logger.warn(
                f"The flow did not finish without unhandled exceptions. Error: {exc_type} - {repr(exc_value)}"
            )

        logger.debug(
            f"Exiting flow {self._flow_id}.{self._experiment_id} - process took {duration} seconds"
        )

        if len(self.validations) > 0:
            # it does not matter how many errors just that the fow exited with some
            self.metric_incr(verb="has_validation_errors")

        # return False progagates and True says we have handled
        return self.suppress_failures

    def _on_fail_node_apply(self, fex, group=None, status=None):
        """
        what to do on failure... this does not log errors and somehing else does
        may want to deprecate
        """

        self.metric_incr(verb="failed", group=group, status=status)

        logger.warn(f"Failed to apply step at node {repr(fex)}")
        # depending on environment may trigger sentry via an error

    @staticmethod
    def invoke_flow(fname, **kwargs):
        """
        Dev tool to run a function on the cluster in test
        Builds the local docker and runs the module on the flow-node temp
        """
        from res.utils.dev import build_docker

        tag = build_docker()

        module = res.flows.get_node_module(fname)
        res.utils.logger.info(f"Found module {module}")
        job_name = f"{fname.split('.')[-1]}-{res.utils.res_hash()}"

        payload = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
            fname
        )

        # flow api contract - reserved type - coudld also handle metadata and task but ignore for now
        if "assets" in kwargs:
            assets = kwargs.pop("assets")
            if not isinstance(assets, list):
                assets = [assets]
            payload["assets"] = assets

        # just to allow simple mode
        payload.update(kwargs)
        payload["apiVersion"] = "v0"

        payload["args"].update(kwargs)
        job_name = payload.get("task", {}).get("key")

        argo = res.connectors.load("argo")
        return (
            argo.handle_event(payload, image_tag=tag, unique_job_name=job_name),
            payload,
        )

    @staticmethod
    def flownode_dataflow_metric_incr(flow, node, asset_type, asset_group, status, inc):
        """
        Dataflow metrics can be written from anywhere but adding the the FlowContext as the primary because it is rooted in "where" the processing happens
        Going to use this in services that process data such as webhooks/etl etc. for row/error counts

        Example is
          data_flow.etl.airtable-webhooks.cdc-cells-changes.<table_id>.ok      42
          data_flow.etl.airtable-webhooks.cdc-cells-changes.<table_id>.errors  2

          In this example the 'etl' flow is just part of generic infra but could be more interesting

        - match: "data_flow.*.*.*.*"
          name: "data_flow"
          labels:
            flow: "$1"
            node: "$2"
            asset_type: "$3"
            asset_group: "$4"
            status: "$5"
        """
        metric_name = f"data_flows.{flow}.{node}.{asset_type}.{asset_group}.{status}"
        res.utils.logger.incr(metric_name, inc)

    @staticmethod
    def flownode_dataflow_metric_gauge(
        flow, node, asset_type, asset_group, status, inc
    ):
        """
        see incr
        """
        metric_name = f"data_flows.{flow}.{node}.{asset_type}.{asset_group}.{status}"
        res.utils.logger.gauge(metric_name, inc)

    def dataflow_metric_incr(self, asset_group, asset_type, inc):
        """
        Wrap the static to send data flow metrics for this flow node
        """

        def clean_var(s):
            s = s or "any"
            # actually we could replace any non alpha_numeric with - probably
            return s.replace(".", "-").replace("_", "-")

        fq_flow_name = clean_var(f"{self._flow_id}-{self._experiment_id}")
        FlowContext.flownode_dataflow_metric_incr(
            flow=fq_flow_name,
            node=clean_var(self._node),
            asset_group=clean_var(asset_group),
            asset_type=clean_var(asset_type),
            inc=inc,
        )

    def metric_incr(self, verb, group=None, status=None, inc=1, with_gauge=False):
        """
        Flow context increments metrics for a group dimension and for the general un-grouped
        The data group can be set on the context e.g. the material we are nesting or the kafka topic we are processing
        """

        group = group or self._data_group
        metric_name = self._metric_name(verb=verb, group=group, status=status)
        # logger.debug(f"Writing metric {metric_name}")
        logger.incr(metric_name, inc)
        if with_gauge:
            logger.gauge(metric_name, value=inc)

    def queue_entered_metric_inc(self):
        self.metric_incr(verb="ENTERED", group=self._data_group, status="OK")

    def queue_exit_success_metric_inc(self):
        self.metric_incr(verb="EXITED", group=self._data_group, status="OK")

    def queue_exit_failed_metric_inc(self):
        self.metric_incr(verb="EXITED", group=self._data_group, status="FAILED")

    def queue_exception_metric_inc(self):
        self.metric_incr(verb="EXCEPTION", group=self._data_group, status="FAILED")

    def metric_gauge(self, verb, group=None, status=None, value=1):
        """
        as for incr; we gauge ...
        """

        group = group or self._data_group
        metric_name = self._metric_name(verb=verb, group=group, status=status)
        # logger.debug(f"Writing metric {metric_name}")
        logger.gauge(metric_name, value)

    def _metric_name(self, verb="observed", group=None, status=None):
        """
        The configuration for statsd uses this for flows

            - match: "flows.*.*.*.*"
              name: "flows"
                labels:
                    flow: "$1"
                    node: "$2"
                    data_group: "$3"
                    verb: "$4"
                    status: "$5"


        The group is an important data attribute for organizing a top level data parallelism or understanding
        For example in nesting different materials behave differently - of course you could have any number of attributes to analyse but the out-of-the-box metrics chooses one top level one

        status are important for understanding errors in particular.
        For example nesting can fail due to OOMs, Value Errors, etc.
        Combined with the data group we can see if any particular data leads to any particular  problem

        """

        def clean_var(s):
            s = s or "any"
            # actually we could replace any non alpha_numeric with - probably
            return s.replace(".", "-").replace("_", "-")

        group = clean_var(group)
        status = clean_var(status)
        node = self._node or "handler"
        fq_flow_name = clean_var(f"{self._flow_id}-{self._experiment_id}")

        return f"flows.{fq_flow_name}.{node}.{group}.{verb}.{status}"

    # get node attribute query (graphql)

    # get node callbacks

    def write_record_check_sum(self, df, uid, node_name):
        """
        This is a utility method for ETL processes if we have a pipeline it is useful to check record counts
        And to ensure nothing is dropped
        """
        path = f"{FLOW_CACHE_PATH}/.chksum/{uid}/{node_name}/data.parquet"

        logger.debug(f"Writing checksum data to {path}")

        df.to_parquet(path)

    @staticmethod
    def on_success(event, context=None, data_group=None, **kwargs):
        fc = FlowContext(event, context, data_group=data_group)

        # auto publish flow can be turned on or off
        if kwargs.get("publish_asset_status"):
            for asset in fc.assets:
                # todo - get asset resolver for type here
                fc.publish_asset_status(
                    asset,
                    fc._metadata.get("node", "unknown"),
                    message="",
                    status="DONE",
                )

    @staticmethod
    def on_failure(event, context=None, data_group=None, **kwargs):
        """
        Generic behaviour for any flow node to trigger an error/sentry message and write metrics
        FlowContext is supposed to parse out the context - this is called e.g. from an Argo service so we have no other python context
        """
        # todo send the event to sentry?

        message = f"Error handler for flow {event.get('metadata', {}).get('name')} - Flow node failed!"

        fc = FlowContext(event, context, data_group=data_group)
        fc.metric_incr(verb="failed")
        fc.log_errors(message, errors=event.get("error_details"))

        if kwargs.get("publish_asset_status"):
            for asset in fc.assets:
                # todo - get asset resolver for type here
                fc.publish_asset_status(
                    asset,
                    fc._metadata.get("node", "unknown"),
                    message="",
                    status="DONE",
                )

    def log_errors(
        self,
        message,
        exception=None,
        errors=None,
        send_errors_to_sentry=False,
        **kwargs,
    ):
        """
        Conventional wrapper for reporting errors that allow troubleshooting WIP
        We care about where logs are, what data group we are processing, the task key which is also the argo key
        The message should be groupable by additional errors can be added for context
        """
        f = self.logger.warning if not send_errors_to_sentry else self.logger.error

        tags = {
            "flow_version": self._experiment_id,
            "flow_node_name": self._node,
            "flow_name": self._flow_id,
            "data_group": self._data_group,
            "help_link": self._help_link,
            "task_key": self._task_key,
            "s3_logs": self.argo_logs_location,
            "queue_name": self._queue_name,
            "queue_request_id": kwargs.get("queue_request_id"),
            "unit_key": kwargs.get("unit_key"),
        }

        # LOG level could be read from somewhere - check with latest logger for sentry
        f(
            message,
            # exception=exception,
            errors=errors,
            tags=tags,
        )

        self._logged_error_count += 1

    def get_logging_function(self, node, key):
        """
        Returns a handler function that will save any data at a path relative to some node
        """
        meta_folder = self.get_node_path_for_key(node, key)

        def f(path, data):
            res.utils.logger.info(f"saving data to {meta_folder}/{path}")
            self.connectors["s3"].write(f"{meta_folder}/{path}", data)

        return f

    def write_dead_letter(
        self, topic, json_data, exception_trace=None, error_after_counter=10
    ):
        """
        Write a kafka payload or any payload really to S3 and log where we put it in hasura
        we can then retrieve things that were dropped and handle them later
        we md5 hasdh the payload to make an id. this is primarily to know how many times we tried to handle it in favour of storing the uri by dates
        the database of course has all the important dates
        we keep a counter every time we re-process; we select only things that have not been processed making the whole thing idempotent on these IO methods
        For example;
        - when failing to process a kafka message, call write_dead_letter queue with the topic name and topic data
        - then at a later time, another process can requeue the data to try again any number of times

        the exception can be supplied which is good for analysis

        TODO implement error after counter e.g. if we retry 10 times (presumably with some time window) then we should pager duty
        """
        try:
            # begin metrics dead letter queue
            res.utils.logger.metric_node_state_transition_incr(
                "Infra.DeadLetters", topic, "ENTER"
            )

            hasura = res.connectors.load("hasura")
            s3 = res.connectors.load("s3")
            M = """
            mutation add_dead_letter($dead_letter: infraestructure_dead_letters_insert_input = {}) {
                insert_infraestructure_dead_letters_one(object: $dead_letter, on_conflict: {constraint: dead_letters_pkey, update_columns: [metadata,reprocessed_at]}) {
                    id
                    uri
                    name
                    counter
                    reprocessed_at
                }
            }
            """
            # parts = res.utils.dates.utc_now_iso_string(None).split("T")
            # uri = f"{FLOW_CACHE_PATH}/dead-letter-queue/{topic}/{parts[0]}/{parts[1]}.json"
            uri = f"{FLOW_CACHE_PATH}/dead-letter-queue/{topic}/{res.utils.hash_dict(json_data)}.json"
            res.utils.logger.debug(f"Logging to {uri}")
            s3.write(uri, json_data)
            # create a request to process the data
            # when we later process it we set a handled date and we no longer need to do anything
            response = hasura.execute_with_kwargs(
                M,
                dead_letter={
                    "id": res.utils.uuid_str_from_dict({"uri": uri}),
                    "uri": uri,
                    "name": topic,
                    # we can retry many times and fail again and then retry...
                    "reprocessed_at": None,
                    "metadata": {"exception": exception_trace},
                },
            )
            res.utils.logger.metric_node_state_transition_incr(
                "Infra.DeadLetters", topic, "EXIT"
            )
            # TODO check counter is not too high
            # logger.error
            return response
        except Exception as ex:
            res.utils.logger.metric_node_state_transition_incr(
                "Infra.DeadLetters", topic, "EXCEPTION"
            )
            res.utils.logger.error(
                f"Failed to write dead letter event {json_data}: {res.utils.ex_repr(ex)}"
            )

    def handle_dead_letters(self, topic):
        """
        Simple workflow to re-process dead letter queue; by sending back to the original topic we are done
        it is up to the handler of the topic to re-populate dead letters
        """
        hasura = res.connectors.load("hasura")
        s3 = res.connectors.load("s3")
        kafka = res.connectors.load("kafka")
        Q = """
        query get_my_dead_letters($name: String =   "" ) {
                infraestructure_dead_letters(where: {name: {_eq: $name}, reprocessed_at: {_is_null: true}}) {
                    id
                    reprocessed_at
                    name
                    uri
                    counter
                }
                }
                """
        M = """mutation MyMutation($id: uuid = "", $reprocessed_at: timestamptz = "") {
        update_infraestructure_dead_letters_by_pk(pk_columns:  {id: $id}, _set: { reprocessed_at: $reprocessed_at}, _inc: {counter: 1}) {
            name
            id
            counter
        }
        }"""
        dead_letters = hasura.execute_with_kwargs(Q, name=topic).get(
            "infraestructure_dead_letters"
        )
        for l in dead_letters:
            # send the payload back to the topic
            kafka[l["name"]].publish(s3.read(l["uri"]), use_kgateway=True)
            # update the transaction processed if re-sent
            hasura.execute_with_kwargs(
                M, id=l["id"], reprocessed_at=res.utils.dates.utc_now_iso_string()
            )


# TODO:
# meta type proxies locations e.g. for bodies that are processed for all vs task runs. abstract this (more generally meta types should have s3 home locations)
# state f(t) in all flows
# how flows use the dgraph interface t- make associations. the ideas that we consume and produce assets
# asset stream from connector query e.g. snowflake and res.meta entity sink so we can run any ingestion job code-free once the connector supports the query and renaming logic

# TESTS:
# tests updating settings
# read and write node data for supported types/function types e.g. generators or notß


# WIP Contract
# Dataframes should be named with default index for marshalling
# Types supported are primitives, numpy or WKY/WKB polygons
# Generators should be used and prefer in V1+ to use metadata, data in enumerators
# ops should always take kwargs although we may relax this - this is because flow context passes kwargs down
