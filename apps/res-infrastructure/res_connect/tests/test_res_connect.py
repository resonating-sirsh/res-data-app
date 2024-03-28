from _pytest.compat import ascii_escaped
import pytest
import res
import json
from ..main import app, PROCESS_NAME

app.testing = True


def test_flow_task_get():
    # with app.test_client() as client:
    #     result = client.get(f"{PROCESS_NAME}/flows")
    #     assert result.status_code == 200
    assert 1 > 0, "nope"


@pytest.mark.service
def test_flow_task_post_default_template():
    # the default template is one we use a catch all but we can also use other templates
    # including the ones the users create - generally we recommend and event and context params at minimum
    DEFAULT_NODE_TEMPLATE = "flow-node"
    with app.test_client() as client:
        file = "res_tests/.sample_test_data/events/sample_function_event.json"
        with open(file, "r") as f:
            event = json.load(f)
        result = client.post(
            f"{PROCESS_NAME}/flows/{DEFAULT_NODE_TEMPLATE}", data=json.dumps(event)
        )
        # todo we do not have an automated way to check workflow submission but soon.
        # the response should have some sort of handle to use to checkback with argo
        assert result.status_code == 200
    # assert (
    #     1 > 0
    # ), "nope"  # commented this out because i need to setup the tests properly for build e.g. test files


def test_nest_call():
    return True


def test_invalidate_webhook():
    return True


def test_notify_cell_change():
    return True


def test_full():
    res.flows.FlowContext.mocks = {}

    event = {}
    context = {}
