from res.flows.FlowContext import FlowContext
import res
import json, yaml
import pytest


@pytest.mark.service
def test_flows_relay_setup_streams():

    sinks = {"test_sink": "test_sink"}
    stream_info = {"res.streams": {"source": "test_source", "sinks": sinks}}

    event = {}
    context = stream_info

    fc = res.flows.FlowContext(event, context)

    assert (
        fc._streams_enabled
    ), "streams were not enabled even though the correct context was set"

    assert set(fc._sinks.keys()) == set(sinks.keys())


def test_flow_no_context():
    event = {}
    context = None
    done = False
    with FlowContext(event, context) as fc:
        done = True

    assert done, "Failed to do"


def test_flows_relay_overides_assets():

    sinks = {"test_sink": "test_sink"}
    stream_info = {"res.streams": {"source": "test_source", "sinks": sinks}}

    event = {"assets": [{"test": "key"}, {"test": "key"}]}
    context = stream_info

    fc = res.flows.FlowContext(event, context)

    assert isinstance(
        fc.assets, list
    ), "The assets passed should be a list to override streams"

    assert len(fc.assets) == 2, f"Expected two test assets but found {len(fc.assets)}"


@pytest.mark.service
def test_flows_relay_streams_adds_kafka_connector_iterator():

    sinks = {"test_sink": "test_sink"}
    stream_info = {"res.streams": {"source": "test_source", "sinks": sinks}}

    event = {}
    context = stream_info

    fc = res.flows.FlowContext(event, context)

    object_type = type(res.connectors.load("kafka")["dummy_topic"])

    assert isinstance(
        fc.assets, object_type
    ), "Expected the stream to be a kafka connector"


def test_FlowContext_constructor():

    event = {}
    context = {}

    # check empties work
    fc = FlowContext(event, context)

    event = {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {"name": "flow_node_example", "version": "exp1"},
        "assets": [{}, {}, {}],
        "task": {"key": "test123"},
        "args": {"spoof_fail": True},
    }

    fc = FlowContext(event, context)

    assert fc.key == "test123", "the task key was not set properly from the event"

    assert len(fc.assets) == 3, "the assets were not loaded properly from the event"

    assert fc.args.get(
        "spoof_fail"
    ), "an argument could not be retrieved from the args - args not setup properly"

    assert fc._flow_id == "flow_node_example", "name not set properly from metadata"

    assert fc._experiment_id == "exp1", "version not set properly from metadata"
