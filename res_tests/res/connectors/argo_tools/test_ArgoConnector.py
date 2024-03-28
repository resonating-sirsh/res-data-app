import pytest
import json


@pytest.mark.service
def test_submit_event_plan():
    """

    def handle_event(
        event,
        context={},
        template="res-flow-node",
        namespace="argo",
        image_tag=None,
        unique_job_name=None,
        plan=False,
    ):
    """
    from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector

    payload = {"id": "abcd"}

    # create one?
    template_name = "swap-style-material"
    namespace = "argo"
    kwargs = {}
    unique_job_name = None  # ""

    template = ArgoWorkflowsConnector.handle_event(
        payload,
        {},
        template_name,
        unique_job_name=unique_job_name,
        plan=True,
        **kwargs,
    )


@pytest.mark.service
def test_loads_generic_template():
    from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector

    payload = {"id": "abcd"}

    # create one?
    template_name = "swap-style-material"
    namespace = "argo"
    kwargs = {}
    unique_job_name = None  # ""
    template = ArgoWorkflowsConnector.get_workflow_template(template_name, namespace)

    template = ArgoWorkflowsConnector.get_parameterized_workflow_template(
        template,
        payload,
        {},
        template_name,
        unique_job_name=unique_job_name,
        **kwargs,
    )


def _flow_template(name=None):
    with open(
        "res_tests/.sample_test_data/events/sample_function_event.json", "r"
    ) as f:
        e = json.load(f)
        if name:
            e["metadata"]["name"] = name
        return e


@pytest.mark.service
def test_flow_node_loader():
    from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector

    def exists_with_param(t):
        return "withParam" in list(t["spec"]["templates"][0]["steps"][2][0].keys())

    for sample in [
        # the default module
        None,
        # a simple handler that does not use module structure
        "flow_node_example.simpler_handler",
        # something that is not in res-data - currently we dont test for this but we could in future
        "flow_node_example.non_existing_handler",
    ]:
        payload = _flow_template(sample)
        template_name = "res-flow-node"
        namespace = "argo"
        kwargs = {}
        unique_job_name = None
        template = ArgoWorkflowsConnector.get_workflow_template(
            template_name, namespace
        )

        template = ArgoWorkflowsConnector.get_parameterized_workflow_template(
            template,
            payload,
            {},
            template_name,
            unique_job_name=unique_job_name,
            **kwargs,
        )

        if sample is None:
            assert exists_with_param(
                template
            ), "The withParam should be included for this template"

        if sample == "flow_node_example.simpler_handler":
            assert not exists_with_param(
                template
            ), "The withParam should not be included for this template"

        # check the name is correct


@pytest.mark.service
def test_unique_job_name():
    from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector

    payload = _flow_template()
    template_name = "res-flow-node"
    namespace = "argo"
    kwargs = {}
    unique_job_name = "test-123"
    template = ArgoWorkflowsConnector.get_workflow_template(template_name, namespace)

    template = ArgoWorkflowsConnector.get_parameterized_workflow_template(
        template,
        payload,
        {},
        template_name,
        unique_job_name=unique_job_name,
        **kwargs,
    )

    assert (
        template["metadata"]["name"] == unique_job_name
    ), "the unique job name was not set"


# TODO test configures memory


@pytest.mark.service
def mod_memory_1():
    """
    data bound memory takes the "data_bind_memory": True option and can take memory settings from a number of places

    """
    from res.connectors.argo_tools.ArgoConnector import ArgoWorkflowsConnector

    payload = {
        "apiVersion": "resmagic.io/v1",
        "args": {},
        "assets": [],
        "kind": "resFlow",
        ####### 1 #######
        "memory": "21Gi",
        "metadata": {
            "name": "meta.ONE.body_node",
            "version": "primary",
            "data_bind_memory": True,
            "git_hash": "1599d6c",
            ####### 3 #######
            "memory": "13Gi",
            "argo.workflow.name": "body-node-res2d3f68b2f2",
            "argo.pod.name": "body-node-res2d3f68b2f2-2874870528",
            "parallel_set_id": "res1234",
            ####### 3 #######
            # 'memory_override': {'handler': '50G'}
        },
        "task": {"key": "body-node-res2d3f68b2f2"},
    }
    template_name = "res-flow-node"
    namespace = "argo"
    kwargs = {}
    unique_job_name = "test-123"
    template = ArgoWorkflowsConnector.get_workflow_template(template_name, namespace)

    template = ArgoWorkflowsConnector.get_parameterized_workflow_template(
        template,
        payload,
        {},
        template_name,
        unique_job_name=unique_job_name,
        **kwargs,
    )

    step = template["spec"]["templates"][0]["steps"][2]
    #     [{'name': 'map',
    #   'template': 'res-data',
    #   'arguments': {'parameters': [{'name': 'event', 'value': '{{item}}'},
    #     {'name': 'context', 'value': '{{workflow.parameters.context}}'},
    #     {'name': 'op_name', 'value': '{{workflow.parameters.op}}'},
    #     {'name': 'compressed', 'value': '{{workflow.parameters.compressed}}'},
    #     {'name': 'memory', 'value': '{{item.memory}}'},
    #     {'name': 'cpu', 'value': '1000m'},
    #     {'name': 'disk', 'value': '2G'},
    #     {'name': 'allow24xlg', 'value': False}]},
    #   'withParam': '{{steps.generate.outputs.parameters.out}}',
    #   'when': '{{workflow.parameters.generator}} != false'}]
    step_args = step[0]["arguments"]["parameters"]

    # the memory should be data bound in this case
    assert [arg for arg in step_args if arg["name"] == "memory"][0][
        "value"
    ] == "{{item.memory}}"

    # and the payloads memory should be 13Gi taking it from the meta data
    assert payload["memory"] == "13Gi"
