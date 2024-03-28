import json
import uuid
import res
from res.flows.FlowContext import FlowContext
import yaml
import os
from res.utils.env import RES_DATA_BUCKET
from dateparser import parse as date_parse

TOPIC = "flowTasks"
SCHEMA_VERSIONS = ["v0", "resmagic.io/v1"]
# this is the current spec for events
SCHEAM_V1 = """
    kind:
      type: string
      required: true
      allowed:
      - resFlow
    apiVersion:
      type: string
      required: true
      allowed:
      - v0
      - resmagic.io/v1
    metadata:
      required: true
      type: dict
      allow_unknown: true
      schema:
        name:
          required: true
          type: string
        version:
          required: true
          type: string
          nullable: true
    task:
      required: true
      type: dict
      schema:
        key:
          required: true
          type: string
          regex: "[a-z0-9]([-a-z0-9]*[a-z0-9])?"
    assets:
      required: true
      type: list
    memory:
        type: string
    args:
      required: true
      type: dict
      allow_unknown: true
      """
# map all versions to a dict
SCHEMA_YAML = {"resmagic.io/v1": SCHEAM_V1}
# this is the event structure..


def payload_template(name, version="resmagic.io/v1", **kwargs):
    """
    Merely a dev tool / helper to generate an empty template to save typing
    """
    return {
        "apiVersion": version,
        "kind": "resFlow",
        "metadata": {
            "name": name,
            "version": kwargs.get("node_version", "v0"),
        },
        "assets": [],
        "task": {"key": str(uuid.uuid1())},
        "args": {},
    }


class FlowWatermarker:
    def __init__(self):
        self.r = res.connectors.load("redis")
        self.s = res.connectors.load("s3")
        self.cache = self.r["FLOWS"]["WATERMARK"]
        self.cold_store_uri = f"s3://{RES_DATA_BUCKET}/cache/watermarks"

    def get(self, key, use_cold_storage=False):
        data = self.cache[key]
        if use_cold_storage and data is None:
            uri = f"{self.cold_store_uri}/watermark.json"
            if self.s.exists(uri):
                # do i need to
                return date_parse(self.s.read(uri).get("watermark"))
        return data

    def set(self, key, use_cold_storage=False):
        watermark = res.utils.dates.utc_now()
        self.cache[key] = watermark
        if use_cold_storage:
            uri = f"{self.cold_store_uri}/watermark.json"
            self.s.write(uri, {"watermark": watermark, "key": key})
        return watermark


class FlowEventValidationException(Exception):
    def __init__(self, message, error=None):
        super().__init__(message)


class FlowEventProcessor(object):
    """
    Instead of posting argo requests via REST we will process them on a kafka queue
    We will call this thing a flow manager as there will be an event convention
    All nodes can send requests to the flow manager and this will trigger the correct workflow templates
    using the argo connector
    """

    def __init__(self, flow_context=None):
        self._flow_context = flow_context or FlowContext({}, {})

    def _cerberus_validate(self, event):
        from cerberus.validator import Validator

        schema = event.get("apiVersion")
        if not schema:
            raise FlowEventValidationException(
                "There is no apiVersion property at the root of the event payload",
                errors=["missing apiVersion"],
            )

        # we allow any payload for v0
        if schema == "v0":
            return []

        if schema not in SCHEMA_VERSIONS:
            raise FlowEventValidationException(
                f"The apiVersion must be one of {SCHEMA_VERSIONS}",
                errors=["schema version not in allowed list"],
            )

        schema_str = SCHEMA_YAML[schema]
        v = Validator(yaml.safe_load(schema_str))
        if not v.validate(event):
            return v.errors
        return []

    def validate(self, event, try_notify_sender=True):
        """
        Validate returns any errors that it finds. If no errors the event is deemed valid
        """
        errors = self._cerberus_validate(event)
        return errors

    def is_flow_event(self, data):
        return data.get("kind", "") in ["resFlow"] if data else False

    def event_to_task(self, event, client_id=None, calling_uid=None):
        node = event.get("metadata").get("name")
        uid = event.get("args").get("job_key")

        return {
            "id": uid,
            "node": node,
            "payload_json": json.dumps(event),
            "client_id": client_id or "",
            "calling_task_uid": calling_uid or "",
        }

    def make_sample_flow_payload_for_function(
        self,
        unique_function_name,
        key=None,
        version="primary",
        api_version="resmagic.io/v1",
    ):
        """
        convenience method to create a valid payload structure for a given function
        to skip validation but still use some of the structure set the `api_version="v0"` which is not validated beyond apiVersion field
        even if not using the rest of the payload structure its good practice to use a consistent payload structure

        The name of the function should be unique from the flows/ module for example if you have
        Example:

            res/
              flows/
                /res_section
                    my_module.py

            then `unique_function_name="res_section.my_module"`
        """
        function = unique_function_name.split(".")[-1]
        # here is a function key but you can make your own...
        function_key = f"{function}-{res.utils.res_hash()}".replace("_", "-").lower()

        event = {
            "apiVersion": api_version,
            "kind": "resFlow",
            "metadata": {
                "name": unique_function_name,
                "version": version,
            },
            "assets": [],
            "task": {"key": key or function_key},
            "args": {},
        }

        if len(self.validate(event)) > 0:
            raise Exception(
                "There is a problem with the code as we did not generate a payload that is valid against the schema!"
            )

        return event

    def flow_task_to_event(self, flow_task):
        payload = flow_task.get("payload_json")
        payload = json.loads(payload)
        return payload

    # def wrap_event_in_task

    def process_event(self, event, context, op="handler"):
        """
        Using the name in the event, resolve to a runnable function
        The event can be either a json string or a python dictionary

        """
        if isinstance(event, str):
            event = json.loads(event)
        if isinstance(context, str):
            context = json.loads(context)

        errors = self.validate(event)
        if len(errors) == 0:
            node_name = event.get("metadata").get("name")

            res.utils.logger.debug(
                f"Flow Event Processor is loading module {node_name} for the team {os.environ.get('RES_TEAM')}"
            )
            module = res.flows.get_node_module(node_name)
            res.utils.logger.debug(f"the loaded module looks like {module}")
            available_ops = list(module.keys())
            if op not in available_ops:
                raise Exception(
                    f"<FEP>: The op {op} is not part of the module {module}"
                )

            f = res.flows.get_node(node_name, auto_resolve_method=False)
            if callable(f):
                return f(event, context)
            else:
                node_name = f"{node_name}.{op}"
                f = res.flows.get_node(node_name)
                return f(event, context)
        else:
            # TODO: create a proper proper FlowEventValidation exception type for this - do the validator first
            raise Exception(f"Failed to validate the event: {errors}")

    def run(self, offset, limit, **kwargs):
        kc = self._flow_context.connectors["kafka"]
        argo = self._flow_context.connectors["argo"]
        while True:
            # todo something with offsets and limits
            batch = kc[TOPIC].consume()
            for task in batch:
                # a task is a wrapper with the payload
                # we wrap it in the submission client for filtering and indexing the topics
                event = self.flow_task_to_event(task)
                context = {}
                if self.validate(event):
                    argo.handle_event(event, context)
