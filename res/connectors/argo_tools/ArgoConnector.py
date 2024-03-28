import re
import json
import uuid
import os

import pandas as pd
from argo.workflows.client import (
    ApiClient,
    WorkflowServiceApi,
    Configuration,
    V1alpha1WorkflowCreateRequest,
)

import res
from res.flows.event_compression import compress_event
from res.utils import safe_http, logger
from res.flows.FlowEventProcessor import FlowEventValidationException


ARGO_SERVER = os.environ.get("ARGO_SERVER", "http://localhost:2746")


class ArgoWorkflowsConnector(object):
    def __init__(self, *args, **kwargs):
        self._config = Configuration(host=ARGO_SERVER)
        self._client = ApiClient(configuration=self._config)
        self._service = WorkflowServiceApi(api_client=self._client)

    @staticmethod
    def get_workflow_template(
        name, namespace, remove_fields=["resourceVersion", "managedFields"]
    ):
        remove_fields = remove_fields or []
        logger.debug(f"removing fields {remove_fields} from template")

        url = f"{ARGO_SERVER}/api/v1/workflow-templates/{namespace}/{name}"
        res = safe_http.request_get(url)
        if res.status_code == 200:
            template = res.json()
            # i am not sure about the specification i.e. what is the difference between a template
            # and additional fields store on the cluster version of the template but some of these fields should not be added
            metadata = template.get("metadata", {})
            for field in remove_fields:
                if field in metadata:
                    metadata.pop(field)
            return template

        raise Exception(
            f"Error: got a status {res.status_code} trying to load workflow {namespace}/{name}"
        )

    def get_workflows(self, namespace="argo"):
        url = f"{ARGO_SERVER}/api/v1/workflows/{namespace}"
        res = safe_http.request_get(url)
        recs = []
        if res.status_code == 200:
            for item in res.json()["items"]:
                md = item["metadata"]
                status = item["status"]

                recs.append(
                    {
                        "name": md["name"],
                        "created_at": md["creationTimestamp"],
                        "generation": md["generation"],
                        "phase": status.get("phase", ""),
                        "started_at": status["startedAt"],
                        "finished_at": status["finishedAt"],
                    }
                )
            df = pd.DataFrame(recs)

            return df

        return []

    def terminate_workflow(self, name, namespace="argo"):
        url = f"{ARGO_SERVER}/api/v1/workflows/{namespace}/{name}/terminate"

    def delete_workflow(self, name, namespace="argo"):
        url = f"{ARGO_SERVER}/api/v1/workflows/{namespace}/{name}/delete"
        return self._service.delete_workflow(namespace, name)

    @staticmethod
    def resolve_argo_template_from_event(event, context):
        return "res-flow-node", "argo"

    @staticmethod
    def try_resolve_job_name_from_payload(data):
        """
        return either the flow convention or support legacy name in the nest job which predates the flow spec
        these can be passed to argo handle_event as 'unique_job_name' to override
        the name generation from the template. This can be important for tracking jobs in logs
        The argo client will validate the name supplied against K8s conventions
        """
        return data.get("task", {}).get("key") or data.get("args", {}).get("jobKey")

    @staticmethod
    def handle_event(
        event,
        context={},
        template="res-flow-node",
        namespace="argo",
        image_tag=None,
        unique_job_name=None,
        plan=False,
        assumed_ops=None,
        compressed=False,
    ):
        """
        This will need to be a flexible router for ginding templates or functions in res
        If templates they may be named with underscores but im not going to support that for now
        We assume its a function

        Mapping a template from an arbitrary event is a big question but for the moment the
        convention is that it is always a flow-node and the handler function is in res

        The template is typically on the cluster but could be anywhere
        """

        # todo in flow db we can use other templates for nodes
        # maybe not this - get parameterized template can do all the magic - defaults or database

        # construct

        if unique_job_name is not None:
            valid_job_key = r"[a-z0-9]([-a-z0-9]*[a-z0-9])?"
            if not re.compile(valid_job_key).fullmatch(unique_job_name):
                raise FlowEventValidationException(
                    f"The name supplied for the job must match the K8s name rules {valid_job_key} but the job name {unique_job_name} was supplied "
                )

        if isinstance(event, str):
            logger.debug(f"The event was passed as a string - parsing it to json")
            event = json.loads(event)

        argo_client = ArgoWorkflowsConnector()

        return argo_client.submit_template_by_name(
            event,
            context,
            name=template,
            namespace=namespace,
            image_tag=image_tag,
            unique_job_name=unique_job_name,
            plan=plan,
            assumed_ops=assumed_ops,
            compressed=compressed,
        )

    @staticmethod
    def flow_node_set_step_param(
        template, target_step_name, param_name, param_value, dag_template="dag"
    ):
        """
        this is a fragile function - we need some robust dictionary path code
        and we also need some checks on the schema

        """

        def by_name(t, n):
            for d in t:
                if d["name"] == n:
                    return d
            return None

        step_name = ""
        try:

            all_steps = by_name(template["spec"]["templates"], dag_template)["steps"]
            for step in all_steps:
                step_name = step[0]["name"]
                params = step[0]["arguments"]["parameters"]
                param = by_name(params, param_name)
                if target_step_name == step_name and param is not None:
                    param["value"] = param_value
        except Exception as ex:
            logger.warn(
                f"Failed in an attempt to alter a step memory in the template for step {step_name}: {repr(ex)}"
            )
        return template

    @staticmethod
    def set_flow_node_module_params(template, event):
        """
        The event should define a module in flow-node
        This module has multiple methods and each can have their own memory requirements
        This reflects the decorated python function for the memory requirements and updates the template
        Conventional ops names are used for the flow node

        """
        step_map = {
            "generator": ["generate"],
            "reducer": ["reduce"],
            "handler": ["simple", "map"],
        }
        try:
            MEM_DEFAULT = "512Mi"
            # back compat add a memory to the event - now we are going to not replace the memory in the template because it will be in the event
            # its trick to know if we want to use this for some processes
            # in future the events used in mappers should add memory somehow into the payloads
            # but old code will now have done that
            # for now adding a memory attribute e.g. into the event, shows the code is aware of this and we will try yo use the {{item.memory}}} instead of a har coded vaue
            USE_DATA_MAPPED_MEMORY = event.get("metadata", {}).get(
                "data_bind_memory", False
            )
            # but we are going to override it to be whatever
            event["memory"] = event.get("memory", MEM_DEFAULT)
            metadata = event.get("metadata", {})
            logger.info(
                f"Mapping module memory for argo template steps using metadata {metadata}"
            )
            module_name = metadata.get("name")
            overrides = {
                "memory": metadata.get("memory_override", {}),
                "disk": metadata.get("disk_override", {}),
                "cpu": metadata.get("cpu_override", {}),
            }
            if module_name:
                logger.debug(f"Loading module {module_name}")
                module = res.flows.get_node_module(module_name)

                template = ArgoWorkflowsConnector.flow_node_set_step_param(
                    template,
                    target_step_name="map",
                    param_name="memory",
                    param_value="1Gi",
                )

                for op_name, v in module.items():
                    for param_name in ["memory", "cpu", "disk", "allow24xlg"]:
                        if hasattr(v, "meta"):
                            # choose what is either passed into the decorator or what is top level on the event by default
                            # we may deprecate top level later and using just for a simple argo test on the mapper
                            if USE_DATA_MAPPED_MEMORY:
                                # if we are just defaulting it, we should check the handler first which is the legacy way
                                param_value = event.get(
                                    param_name, v.meta.get(param_name, MEM_DEFAULT)
                                )
                            else:
                                param_value = v.meta.get(param_name, MEM_DEFAULT)
                            # now check the metadata in the data
                            param_value = metadata.get(param_name, param_value)
                            # then check if its overrides per op
                            param_value = overrides.get(param_name, {}).get(
                                op_name, param_value
                            )

                            res.utils.logger.info(
                                f"{op_name}.{param_name}={param_value}"
                            )

                            if param_value is not None:
                                # SA: moving the memory setting to the event from wherever it was set before.
                                # we just dont replace the memory param we map it from the event
                                if param_name == "memory":
                                    event["memory"] = param_value

                                # just get the op name if there is no map and it is callable
                                for op in step_map.get(op_name, ["simple", "map"]):
                                    logger.info(
                                        f"Setting flow node module {module_name}: {param_name} to {param_value} for step {op}"
                                    )
                                    if (
                                        op == "map"
                                        and USE_DATA_MAPPED_MEMORY
                                        and param_name == "memory"
                                    ):
                                        # this is the default actually nut we want safe defaults too
                                        param_value = "{{item.memory}}"
                                        res.utils.logger.info(
                                            "Data binding memory in the map"
                                        )
                                    else:
                                        res.utils.logger.info(
                                            "Leaving yaml memory map as is"
                                        )
                                    template = (
                                        ArgoWorkflowsConnector.flow_node_set_step_param(
                                            template,
                                            target_step_name=op,
                                            param_name=param_name,
                                            param_value=param_value,
                                        )
                                    )
                    else:
                        logger.debug(
                            f"Module part {op_name} does not have a meta attribute - add a flow_node_attribute decorator for {module_name}, {op_name}"
                        )
        except Exception as ex:
            logger.warn(
                f"Failed when trying to set memory for the flow node module. Defaults will be used: {repr(ex)}"
            )
        logger.debug(f"Updated template")
        return template

    @staticmethod
    def get_module_from_event(event):
        metadata = event.get("metadata", {})
        module_name = metadata.get("name")
        # inspect the module, if it has a generator and a reducer use it
        module = res.flows.get_node_module(module_name)
        return module

    @staticmethod
    def get_module_res_team_from_event(event):
        """
        res team is a bit specific but dont want to over think it for now
        """
        try:
            metadata = event.get("metadata", {})
            module_name = metadata.get("name")
            # inspect the module, if it has a generator and a reducer use it
            res_team = res.flows.get_res_team_for_node_name(module_name)
            return res_team
        except Exception as ex:
            res.utils.logger.warn(
                f"Failed to set the RES_TEAM for the module {module_name}: {repr(ex)}"
            )

    @staticmethod
    def _process_params_for_flow_node(template, param_map, name, event=None, **kwargs):
        """
        this can with some certainty but run for anything that supports these
        options but flow-node is the only case we expect. but for example, other
        callable templates could always have an event and context but we dont know that yet


        Example: by calling the parent function passing kwargs this our WIP approach to enabling
        The generators and reducers for the node. These could also be inferred from the node's supported methods

            kwargs = {}
            ### this block shows to enable flow-node features/these are params mapped into the argo params
            ### experimenting with reflecting this from the module at the moment but it is not clear if we should always do that
            ### probably yhe compiled module can say if tt should have reducers and maps and if anything they could be enabled or not

            kwargs['ops'] ={
                #handler is the default so no need to set it
                "generator": "generator",
                "reducer" : "reducer"
            }

            #####
             given a template loaded using `get_workflow_template` from argo context `ac` ...
             T = ac.get_parameterized_workflow_template(template, event, context, **kwargs)
        """

        logger.debug(f"Processing the template {name} with kwargs {kwargs}")

        params = template["spec"]["arguments"]["parameters"]
        param_names = [v["name"] for v in params]

        # update ops to enable
        if name in ["flow-node", "res-flow-node"]:
            module = ArgoWorkflowsConnector.get_module_from_event(event=event)

            assumed_ops = kwargs.get("assumed_ops")
            logger.debug(f"adding ops to kwargs - assuming {assumed_ops}")
            if not assumed_ops:
                assumed_ops = []
            if "ops" not in kwargs:
                kwargs["ops"] = {}
            for op in ["reducer", "generator"]:
                kwargs["ops"][op] = op if module.get(op) or op in assumed_ops else False

        # we need something wiser than argo to create the ops mapping for the args
        if "ops" in kwargs:
            param_map.update(kwargs["ops"])

        if name in ["flow-node", "res-flow-node"]:
            template = ArgoWorkflowsConnector.set_flow_node_module_params(
                template, event=event
            )

            # TODO: validate if all ops are callable

        if not param_map.get("generator", False) and name in [
            "flow-node",
            "res-flow-node",
        ]:
            try:
                # this is because there seems a flaw in argo where we can conditional remove a step with a withParam
                # or it will complain about the variables we know are not there
                map_step = 2
                template["spec"]["templates"][0]["steps"][map_step][0].pop("withParam")
                logger.debug(
                    "removed the withParam which is only used with a generator"
                )
            except Exception as ex:
                logger.debug(
                    f"Error removing the mapping step from flow node - {repr(ex)}"
                )

        # ordered replacement for the known flow-node template from this list
        for i, key in enumerate(param_names):
            if key in param_map:
                # replace name value params in order (we could add but these are already in the template)...
                params[i] = {"name": key, "value": param_map[key]}

        # the event payload - we use to add events inside {event: } but not any more
        template["spec"]["arguments"]["parameters"] = params

        if name in name in ["flow-node", "res-flow-node"]:
            # now get the modules attributes like res team
            team = ArgoWorkflowsConnector.get_module_res_team_from_event(event)

            for t in template["spec"]["templates"]:
                # only for res-data
                if t["name"] in ["res-data", "res-nest"]:
                    if kwargs.get("image_tag") is not None:
                        logger.debug(
                            f"setting image tag from args to {kwargs['image_tag']} with pull policy `Always`"
                        )

                        # get the latest tag has whatever tag is on the build
                        latest = (
                            t["script"]["image"].split(":")[-1]
                            if ":" in t["script"]["image"]
                            else None
                        )

                        # if there is a tag, which there should be, replace it with the incoming
                        if latest:
                            t["script"]["image"] = t["script"]["image"].replace(
                                latest, kwargs["image_tag"]
                            )
                        else:  # if there is not, add the incoming
                            t["script"]["image"] = (
                                t["script"]["image"] + f":{kwargs['image_tag']}"
                            )
                        t["script"]["imagePullPolicy"] = "Always"

                    if team:
                        # add the environment variable for all the res-data template(s) in the workflow
                        # going to assume there is already a default env called RES_TEAM on the flow node template
                        res.utils.logger.info(f"Setting RES_TEAM={team} for workflow")
                        # rewrite the list of envs with the team
                        t["script"]["env"] = [
                            d
                            if d["name"] != "RES_TEAM"
                            else {"name": "RES_TEAM", "value": team}
                            for d in t["script"]["env"]
                        ]

    @staticmethod
    def make_name(event, template, add_qualifier_to_name=None):
        """
        A convention for generating unique names based on templates, flow methods and job keys
        """

        logger.debug(f"Making template name using event {event}")

        assert isinstance(
            event, dict
        ), "The event should be parsed json to dict but is not"

        add_qualifier_to_name = (
            add_qualifier_to_name if add_qualifier_to_name else str(uuid.uuid4())[:8]
        )

        # we use the [template|flow-node]/job_key/random string to make the job id unique
        template_name = template["metadata"]["name"]
        # k8s format of the fully qualified python method
        node_name = (
            event.get("metadata", {})
            .get("name", "")
            .replace("_", "-")
            .replace(".", "-")
        )
        # if there is a job key we can use it
        job_key = event.get("task", {}).get("key", {})
        if job_key:
            add_qualifier_to_name = f"{job_key}-{add_qualifier_to_name}"

        name = f"{node_name or template_name}-{add_qualifier_to_name}"

        logger.debug(f"generated name - {name}")

        return name

    @staticmethod
    def get_parameterized_workflow_template(
        template,
        event,
        context,
        name,
        add_qualifier_to_name=None,
        unique_job_name=None,
        compressed=False,
        **kwargs,
    ):
        """
        res "alpha" will call this - load a function-set and call...
        the main idea is just to run op(event, context) but we can do a more complex thing
        where we can have a generator and reducer for a rap-reduce application

        the workflow may be the default flow-node or anything else
        for flow-node we find a function either the function exists or we will try path.handler

        """
        # by default argo just submits the template with the flow name as the function
        # it is assumed that when the template is actually run, something knows how to resolve the python function
        # if we know something about the node before calling argo we can update the context
        # for example, we cam add other ops for the experimental flow-node or lookup options in a database
        event_str = compress_event(event) if compressed else json.dumps(event)
        param_map = {
            "event": event_str,
            "context": json.dumps(context),
            "compressed": str(compressed),
        }

        # extra checks for special parameterizations of the workflow
        ArgoWorkflowsConnector._process_params_for_flow_node(
            template, param_map, name=name, event=event, **kwargs
        )

        # either specify a unique name or we will generate one
        template["metadata"][
            "name"
        ] = unique_job_name or ArgoWorkflowsConnector.make_name(
            event, template, add_qualifier_to_name
        )

        return template

    @staticmethod
    def try_add_tag_to_event_metadata_from_template(event, template):
        """
        this wont always work - this assumes the template uses known images e.g. res_data and then looks for the tag
        providers should implement their own template processing
        """
        try:
            git_hash = "???"
            for t in template["spec"]["templates"]:
                # only for res-data
                if t["name"] == "res-data":
                    git_hash = t["script"]["image"].split(":")[-1]
            event["metadata"]["git_hash"] = git_hash
        except:
            pass
        return event

    def submit_template_by_name(
        self,
        event,
        context,
        name="res-flow-node",
        namespace="argo",
        unique_job_name=None,
        plan=False,
        compressed=False,
        **kwargs,
    ):
        """
        Submit any template by name and namespace
        By default we use the generic res flow node and just pass event and context

        TODO: add labels for searching the UI or looking up logs or whatever
        """
        template = ArgoWorkflowsConnector.get_workflow_template(name, namespace)

        try:

            event = ArgoWorkflowsConnector.try_add_tag_to_event_metadata_from_template(
                event, template
            )

            template = ArgoWorkflowsConnector.get_parameterized_workflow_template(
                template,
                event,
                context,
                name,
                unique_job_name=unique_job_name,
                compressed=compressed,
                **kwargs,
            )
        except Exception as ex:
            logger.warn(
                f"Failed to modify the template: {repr(ex)} - this is an error but allowing percolation and attempting to run the template"
            )
            raise ex
        try:
            if plan:
                return template

            logger.info(f"submitting {unique_job_name}...")
            res = self._service.create_workflow(
                namespace, V1alpha1WorkflowCreateRequest(workflow=template)
            )
            # return some kind of callback to monitor the workflow?
        except Exception as ex:
            raise Exception(
                f"Failed to submit workflow {namespace}/{name}  due to {repr(ex)}. The payload: {event}"
            )

        logger.debug(
            f"The job {template.get('metadata', {}).get('name')} was submitted - check the UI or (TODO) callback"
        )

        return True


# TODO: needed unit tests

# 1. module reflection: adds generator and reducer at the right time / also takes ops from kwargs
# 2. updates memory from module attributes
# 3. exception when a request template does not exist / test this too
# 4. add test for functions that are callable as opposed to the handler and reducer types e.g. memory setting
# 5. Adds template name if supplied in jobKey or task.key for all templates
# 6. Does not validate flow node api version 0 but does otherwise
# 7. Does not validate payloads for general templates - only res-data ones (flow nodes)
# 8. Res-connect tests
# 9. Test that the generator omitted and res-flow-node always removed withParam
# 10. Test generic payload
