import dataclasses
from dataclasses import dataclass
from datetime import datetime
import typing
from xmlrpc.client import boolean
from dataclasses_avroschema import AvroModel
from dataclasses_jsonschema import JsonSchemaMixin
import dataclasses_avroschema
from enum import Enum, EnumMeta
import json
from dataclasses import field as dfield

import res
from res.connectors.airtable.AirtableWebhookSpec import AirtableWebhookSpec


"""
Alot of these nodes and tpyings will move to a new library
some resonance specific types e.g. decorators that tells if we are metric or something can be added to control serializers 
"""


# may go with To Do, In Progress, Done, Failed instead
class StatusType(Enum):
    TODO = "To Do"
    WORKING = "In Progress"
    DONE = "Done"
    FAILED = "Failed"
    CANCELLED = "Cancelled"


PINOT_DATETIME_FORMATS = {
    # I think this is equivalent to ISOish
    "default": "1:MILLISECONDS:SIMPLE_DATE_FORMAT:yyyy-MM-ddTHH:mm:ss.SSS"
}

"""
IM keeping a couple of provider specific schema tools close to the the Node for now
"""

SYSTEM_FIELDS = ["event_timestamp", "event_elapsed_seconds"]


class PinotTableSchema(object):
    """
    Given a dataclass with possible resonance decorators, generate the schema
    https://docs.pinot.apache.org/basics/components/schema


    """

    def __init__(self, obj):
        self._klass = dataclass(obj)
        # for testing take the avro types but can refactor this
        self._fields = dataclasses.fields(self._klass)

        # controlling for dots for now
        self._name = f"{obj.Meta.namespace}.{obj.__name__}"
        self._dimension_fields = []
        self._metric_fields = []
        self._timestamp_field = "created_at"

        self._schema = {
            "schemaName": self._name,
            "dimensionFieldSpecs": self.fields,
            "dateTimeFieldSpecs": [
                {
                    "name": self._timestamp_field,
                    "dataType": "STRING",
                    # how to parse the kafka time
                    "format": PINOT_DATETIME_FORMATS.get("default"),
                    # our granularity for the table
                    "granularity": "1:SECONDS",
                }
            ],
        }

        key = ["unit_key"]
        if key:
            self._schema["primaryKeyColumns"] = key

    @property
    def fields(self):
        """ """
        fields = []
        type_mapping = {
            str: "STRING",
            float: "FLOAT",
            int: "INT",
            boolean: "BOOLEAN",
            datetime: "TIMESTAMP",
            #             # actually want a list of enums here
            #             typing.List: "multipleSelects",
            #             list: "multipleSelects",
            #             Enum: "singleSelect",
            #             StatusType: "singleSelect",
            dict: "JSON",
        }

        def is_time(f):
            # temp
            return f.name == "created_at"

        def is_metric(f):
            return False

        for f in self._fields:
            if is_time(f):
                continue

            o = typing.get_origin(f.type)
            if f.type in type_mapping or o in type_mapping:
                m = type_mapping.get(f.type, type_mapping.get(o))

                d = {
                    "name": f.name,
                    "dataType": type_mapping.get(f.type),
                    "singleValueField": True,
                }

                if o is list:
                    # not sure what we want to do yet
                    d["singleValueField"] = False
                    pass

                #                 if isinstance(f.type, EnumMeta):
                #                     d['singleValueField'] = False

                fields.append(d)
            else:
                res.utils.logger.warn("We dont have a pinot mapping for type", f.type)

        return fields

    @property
    def schema(self):
        return self._schema

    def __repr__(self):
        return str(self._schema)


DEV_AIRTABLE_BASE_ID = ""
# this can move to the airtable connector maybe
class AirtableSchema:
    """
    We can be smarter about how we implement this
    I was lazy and copied the dataclasses code for the avro without abstracting
    and, i only implement a few types
    """

    # https://airtable.com/api/enterprise#fieldTypes
    def __init__(self, obj):
        self._klass = dataclass(obj)
        # for testing take the avro types but can refactor this
        self._fields = dataclasses.fields(self._klass)
        self._name = json.loads(self._klass.avro_schema())["name"]
        self._namespace = self._klass.Meta.namespace
        self._airtable_name_mapping = (
            {}
            if not hasattr(self._klass.Meta, "airtable_mapping")
            else self._klass.Meta.airtable_mapping
        )

        self._resmeta = {} if not hasattr(self._klass, "meta") else self._klass.meta
        # the base id is a test thing for - we have a single base where we would create experimental queues if not base exists
        self._airtable_id = self._resmeta.get("airtable_id", DEV_AIRTABLE_BASE_ID)
        self._airtable_name = self._resmeta.get("airtable_name")
        # we can override the name we use with the attributes or use the standard one that would be used everywhere
        self._fully_qualified_name = (
            self._airtable_name or f"{self._namespace}.{self._name}".replace("-", "_")
        )

        # this is a lazy way to get our name

    @staticmethod
    def try_get_airtable_base_id(qtype):
        if hasattr(qtype, "meta"):
            aid = qtype.meta.get("airtable_id")
            bid = aid.split("/")[0]
            if bid:
                assert (
                    bid[:3] == "app"
                ), "The airtable id must be of the format baseid/table_id e.g. appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w"
                return bid

    @staticmethod
    def try_get_airtable_config(qtype):
        if hasattr(qtype, "meta"):
            aid = qtype.meta.get("airtable_id")
            if "/" in aid:
                bid, tid = aid.split("/")
                return AirtableWebhookSpec.load_config(bid, tid)

    @staticmethod
    def try_get_airtable_res_schema_mapping(qtype):
        k = dataclass(qtype)
        return k.Meta.airtable_mapping if hasattr(k.Meta, "airtable_mapping") else None

    @property
    def fields(self):
        fields = []

        type_mapping = {
            str: "singleLineText",
            float: "number",
            int: "number",
            # actually want a list of enums here
            typing.List: "multipleSelects",
            list: "multipleSelects",
            EnumMeta: "singleSelect",
            StatusType: "singleSelect",
            dict: "multilineText",
            datetime: "dateTime",
        }

        for f in self._fields:
            # if there is a mapping, map it or else leave as is
            name = self._airtable_name_mapping.get(f.name, f.name)

            # apply a trial convention to always map unmapped ids to record id from AT perspective
            if "id" not in self._airtable_name_mapping and name == "id":
                name = "record_id"

            if name in SYSTEM_FIELDS:
                res.utils.logger.debug(
                    f"Skipping system fields on type not used in airtable: {name}"
                )
                continue

            if name == "record_id":
                # this is reserved
                res.utils.logger.warn(
                    f"We will skip record id when creating an airtable schema - reserved keyword"
                )
                continue

            o = typing.get_origin(f.type)
            if isinstance(f.type, EnumMeta):
                o = EnumMeta

            res.utils.logger.debug(
                f"{name}, {o}, {f.type}, {isinstance(f.type, EnumMeta)}"
            )

            if f.type in type_mapping or o in type_mapping:
                m = type_mapping.get(f.type, type_mapping.get(o))
                # print(f'we have a type for {f.name} which will map {f.type} -> {m}')
                d = {"name": name, "type": m}

                if o is list:
                    gen_arg = f.type.__args__[0]
                    if isinstance(gen_arg, EnumMeta):
                        d["options"] = {
                            "choices": [{"name": item.value} for item in list(gen_arg)]
                        }
                    # else:
                    #     raise Exception(
                    #         f"We need to handle the case for airtable where there are a list of something e.g. {gen_arg}- for now using Enums"
                    #     )

                # TODO how to generally handle enums / also multi select
                if isinstance(f.type, EnumMeta):
                    # print('look for enum - probably a better way to handle these types but moving on')
                    d["options"] = {
                        "choices": [{"name": item.value} for item in list(f.type)]
                    }

                if m == "dateTime":
                    d["options"] = {
                        "dateFormat": "iso",
                        "timeFormat": "24hour",
                        "timeZone": "utc",
                    }

                fields.append(d)
            else:
                res.utils.logger.warn(
                    f"We don't have airtable mapping for type {f.type}, {o}"
                )
        return {
            # for testing we are going to make the name match what we do for the kafka topic
            "name": self._fully_qualified_name,
            "fields": fields,
        }


class LookerViewBuilder(object):
    """
    Given a dataclass with possible resonance decorators, generate the schema
    https://docs.pinot.apache.org/basics/components/schema


    """

    def __init__(self, obj):

        self._klass = dataclass(obj)

        # for testing take the avro types but can refactor this
        self._fields = dataclasses.fields(self._klass)

        self._name = obj.__name__
        # controlling for dots for now
        self._full_name = f"{obj.Meta.namespace}.{self._name}"
        self._dimension_fields = []
        self._metric_fields = []
        self._timestamp_field = "created_at"
        key = ["unit_key"]
        # hard coding for now
        self._namespace = "res.Meta"

    @property
    def fields(self):
        """ """
        import lookml

        fields = []
        type_mapping = {
            str: "string",
            float: "number",
            int: "number",
            datetime: "date",
            list: "string",
            dict: "string",
            EnumMeta: "string",
            type: "string",
        }

        def is_metric(f):
            return False

        for f in self._fields:
            o = typing.get_origin(f.type)
            # poor mans complex type mapping for enums
            if isinstance(f.type, EnumMeta):
                o = EnumMeta
            if f.type in type_mapping or o in type_mapping:
                t = type_mapping.get(f.type, type_mapping.get(o))
                name = lookml.core.lookCase(f.name)
                fields.append(
                    f"""
                     dimension: {name} {{
                         label: "{name}"
                         type: {t}
                         sql: ${{TABLE}}.{name} ;;
                     }}
                """
                )

            else:
                res.utils.logger.warn("We dont have a looker mapping for type", f.type)

        return fields

    @property
    def view_name(self):
        return f"{self._full_name.lower()}"

    @property
    def view(self):
        vo = ""

        for field in self.fields:
            vo += field

        # primary key define

        vo += f'sql_table_name: kafka.default."{self.view_name}" ;;'

        return vo

    def __repr__(self):
        return str(self.view)


@dataclass
class NodeQueue(AvroModel, JsonSchemaMixin):
    # see https://stackoverflow.com/questions/51575931/class-inheritance-in-python-3-7-dataclasses

    unit_key: str
    # todo - how do we want to handle dates - strings seem fine -> maybe some logical types can be used here for airtable
    id: str
    tags: typing.List[str] = dfield(default_factory=lambda: [])
    created_at: str = dfield(default_factory=res.utils.dates.utc_now_iso_string)
    # experimental - these make sense as control fields but we dont necessarily want to bind to user queues
    event_timestamp: str = dfield(default_factory=res.utils.dates.utc_now_iso_string)
    event_elapsed_seconds: int = None

    @classmethod
    def airtable_create_request(cls, dumps=True, validate=False):
        f = AirtableSchema(cls).fields
        return f if not dumps else json.dumps(f)

    @classmethod
    def pinot_table_schema(cls, dumps=True, validate=False):
        schema = PinotTableSchema(cls).schema
        return schema if not dumps else json.dumps(schema)

    @classmethod
    def looker_view(cls, validate=False):

        view = LookerViewBuilder(cls).view
        return view

    @classmethod
    def res_type_name(cls):
        name_of = lambda o: str(o).split(".")[-1]
        return f"{cls.Meta.namespace}.{name_of(cls.__name__)}"

    def publish(self):
        topic = self.res_type_name()
        kafka = res.connectors.load("kafka")
        kafka[topic].publish(self.asdict(), use_kgateway=True)


class NodeManager:
    def __init__(self):
        pass

    def try_get_airtable_base_id(qtype):
        if hasattr(qtype, "meta"):
            aid = qtype.meta.get("airtable_id")
            bid = aid.split("/")[0]
            if bid:
                assert (
                    bid[:3] == "app"
                ), "The airtable id must be of the format baseid/table_id e.g. appqtN4USHTmyC6Dv/tblWMyfKohTHxDj3w"
                return bid

    def create(self, qtype, **kwargs):
        """
        create the node connectors; kafka, airtable etc.
        The same schema is used to generate all of these things

        options:

        -airtable_webhook_only: False :> if this is set, the airtable connector does not try to create or modify the table but will create webhook
        """
        base_id = "appaaqq2FgooqpAnQ"
        res_type_name = qtype.res_type_name()
        airtable = res.connectors.load("airtable")
        kafka = res.connectors.load("kafka")
        pinot = res.connectors.load("pinot")
        looker = res.connectors.load("looker")
        kafka.update_queue(qtype.avro_schema(), **kwargs)
        # if the type has an airtable base id attribute we can use it
        base_id = AirtableSchema.try_get_airtable_base_id(qtype)
        ator_mapping = AirtableSchema.try_get_airtable_res_schema_mapping(qtype)
        # think about this - its not as clean as we would like if we have to know what is on the interface - this is because there is a mapping from theirs to ours we want to store somewhere
        airtable.update_queue(
            qtype.airtable_create_request(),
            base_id=base_id,
            res_schema_mapping=ator_mapping,
            res_type_name=res_type_name,
            **kwargs,
        )
        pinot.update_queue(qtype.pinot_table_schema())
        looker.update_queue(qtype)
        res.utils.logger.info("Queues have been upserted in all registered systems.")

    def deploy(self, f, **kwargs):
        """
        build a docker image and deploy the node
        the node runs on the flow node by default
        we will consume from a queue and either inline process units or map
        1. for map mode, the flow node provides a default generator over the kafka stream and then pass works in parallel
           the handler function processes each unit as normal in the flow node
           each handler will be given a single unit in its payload
        3. for non map mode, the payload is blank in the handler and the flow context will pull directly from the
           queue and iterate and map inline by invoking the handler
        """
        context = NodeManager.get_node_context(f)
        fname = context["name"]
        queue_name = context["input_queue_name"].split(".")[-1].lower()
        res.utils.logger.info(f"deploying op {f} with {fname}")
        res.utils.logger.info(
            "Building docker - this will take a moment if requirements have changed..."
        )
        tag = res.utils.dev.build_docker()

        job_name = f"{queue_name}-{res.utils.res_hash()}".lower()
        payload = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
            fname
        )
        job_name = payload.get("task", {}).get("key")

        argo = res.connectors.load("argo")
        return argo.handle_event(payload, image_tag=tag, unique_job_name=job_name)

    def schedule(self, f, **kwargs):
        """
        TODO
        """
        tag = res.utils.dev.build_docker()
        fname = self._get_node_name(f)
        payload = res.flows.FlowEventProcessor().make_sample_flow_payload_for_function(
            fname
        )
        return res.flows.FlowEventProcessor().schedule(payload, tag, internal=5)

    def cleanup_queues(self, **kwargs):
        """
        On some schedule,
        For all given or discovered queues,
        STALE DONE states should be burged
        STALE TODO states should be requeued
        """
        return None

    @staticmethod
    def publish_queue_status(unit, validation_tags=None, **kwargs):
        """
        This is a demo version; in reality we would do a lot more inspection for
        where to write to and what write
        """

        try:
            # we should not do this every time, just do it once in the flow
            context = NodeManager.get_unit_context(unit)
            input_queue = context["queue_name"]
            airtable = res.connectors.load("airtable")
            # a test base is added
            base = context.get("airtable_base_id", "appaaqq2FgooqpAnQ")
            rec_id = unit.id

            res.utils.logger.debug(
                f"publishing to the airtable queue {base}.{input_queue} with status for record_id {rec_id}"
            )

            d = {"record_id": rec_id, "status": "DONE"}
            if validation_tags:
                d["status"] = "FAILED"
                d["tags"] = validation_tags
            else:
                d["tags"] = []

            airtable[base][input_queue].update(d)
        except Exception as ex:
            # currently we fail silently if there is no valid record id
            res.utils.logger.warn(f"Failed to update airtable {repr(ex)}")

    @staticmethod
    def is_node(obj):
        return isinstance(obj, NodeQueue)

    @staticmethod
    def get_unit_context(obj, **kwargs):
        if not isinstance(obj, type):
            obj = type(obj)

        def name_of(o):
            "TODO: TBD"
            return str(o).split(".")[-1]

        return {
            "module": obj.__module__,
            "unit": obj.__name__,
            "name": f"{obj.__module__}.{obj.__name__}",
            "unit_type": obj,
            "queue_name": f"{obj.Meta.namespace}.{name_of(obj.__name__)}",
        }

    @staticmethod
    def get_node_context(f, **kwargs):
        """
        Everything you need to know about a node and the queue inputs and outputs
        """
        import inspect

        if f is None:
            res.utils.logger.info(
                f"Note: Attempting to load context for a null function."
            )
            return {}

        sig = inspect.signature(f)
        p = sig.parameters["unit"]
        f_input = p.annotation
        f_output = sig.return_annotation

        def name_of(o):
            "TODO: TBD"
            return str(o).split(".")[-1]

        d = {
            "module": f.__module__,
            "op": f.__name__,
            "name": f"{f.__module__}.{f.__name__}",
            "input_unit_type": f_input,
            "output_unit_type": f_output,
            "input_queue_name": f"{f_input.Meta.namespace}.{name_of(f_input.__name__)}",
            "output_queue_name": f"{f_output.Meta.namespace}.{name_of(f_output.__name__)}",
        }

        if hasattr(f, "meta"):
            d["flows.metadata"] = getattr(f, "meta")

        return d
