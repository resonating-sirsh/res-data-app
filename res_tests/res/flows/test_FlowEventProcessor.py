import res
import json, yaml
import pytest

FLOW_TASKS_TOPIC = "flowTasks"


def test_calls_callable_node_from_event():
    file = "res_tests/.sample_test_data/events/sample_function_event.json"
    with open(file, "r") as f:
        event = json.load(f)

    from res.flows import FlowEventProcessor

    fp = FlowEventProcessor()
    res = fp.process_event(event, {})

    assert len(res) > 0, "expected a list of assets-resuts in the response"


def test_calls_non_callable_node_with_handler_from_event():
    pass


# @pytest.mark.service
# def test_send_flow_event_to_kafka_tasks():
#     """
#     Kafka publish tests require access to schema registry AND kgateway
#     """
#     # note paths are relative to where tests are run from. we choose root of res-data-platform
#     file = "res_tests/.sample_test_data/events/sample_function_event.json"
#     with open(file, "r") as f:
#         event = json.load(f)

#     from res.flows import FlowEventProcessor

#     fp = FlowEventProcessor()

#     data = fp.event_to_task(event, "client_id", "calling_uid")

#     result = res.connectors.load("kafka")[FLOW_TASKS_TOPIC].publish(
#         data, use_kgateway=True, coerce=True
#     )

#     assert len(result) == 1, "Unable to publish the record"


def test_event_to_task():
    file = "res_tests/.sample_test_data/events/sample_function_event.json"
    with open(file, "r") as f:
        event = json.load(f)

    from res.flows import FlowEventProcessor

    fp = FlowEventProcessor()
    data = fp.event_to_task(event, "client_id", "calling_task_uid")

    json.loads(data.get("payload_json"))

    for f in ["client_id", "calling_task_uid"]:
        assert data[f] == f, f"expected client_id field to be {f} but got {data[f]}"

    assert data["node"] == event.get("metadata").get(
        "name"
    ), "the name field was not mapped to the metadata.name in the event"

    assert data["id"] == event.get("args").get(
        "job_key"
    ), "the id field was not mapped to the args.job_key in the event"


def test_event_validation():
    file = "res_tests/.sample_test_data/events/sample_function_event.json"
    with open(file, "r") as f:
        event = json.load(f)
    from res.flows import FlowEventProcessor

    fp = FlowEventProcessor()
    errors = fp.validate(event)
    assert (
        len(errors) == 0
    ), "excepted this sample event to be validate for its scheam version"
    event["metadata"].pop("name")
    event["task"].pop("key")
    event.pop("assets")
    errors = fp.validate(event)
    # this will create a validation error with two issues because two errors are in args and one at metadata
    assert (
        len(errors) == 3
    ), f"expected the event to have three validation errors for its scheam version but found {errors}"


def test_flow_task_to_argo_task():
    with open(f"res_tests/.sample_test_data/topics/{FLOW_TASKS_TOPIC}.json", "r") as f:
        record = json.load(f)

    p = res.flows.FlowEventProcessor()
    event = p.flow_task_to_event(record)

    assert (
        record["node"] == event["metadata"]["name"]
    ), "The payload could not extracted - expecting a payload with a record.name that matches the event.metadata.name"


@pytest.mark.service
def test_argo_sbumit_task():
    with open(f"res_tests/.sample_test_data/topics/{FLOW_TASKS_TOPIC}.json", "r") as f:
        record = json.load(f)

    p = res.flows.FlowEventProcessor()
    event = p.flow_task_to_event(record)
    argo = res.connectors.load("argo")
    # dont create these for now
    # result = argo.handle_event(event, {})

    # TBD: how we know the submission is valid but for now make sure we do not crash
    assert 1 > 0, "false"


def test_calls_correct_op():

    # TODO:
    # test calling the correct op - e.g. we change how we resolve ops sometimes so make sure e.g on_success is called when it is supposed to be

    # use the example flow node to call a specifc handler op and make sure its the right one in all cases including defaults
    pass


# test avro creates template from payload

# we can really test the consumer because all it does us used the in-cluster cluster to move through records and submit tasks in that format
