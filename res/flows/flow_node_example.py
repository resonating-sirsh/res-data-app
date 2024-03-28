"""
This is a sample flow node module illustrating running code/modules on argo

A payload is posted to res connect or the ufnction can be run or invoked from the res cli

The payload uses the Api spec post to resconnect 
https://datadev.resmagic.io/res-connect/flows/res-flow-node

event = {
    "apiVersion": "resmagic.io/v1",
    "kind": "resFlow",
    "metadata": {
        "name": "flow_node_example",
        "version": "experimental"
    },
    "assets": [
        {}
    ],
    "task": {
        "key": "test123"
    },
    "args": {
        "something": True #true for javascript type
    }
}

Another way to think about the event is how FlowContext uses args to get metadata e.g. to choose s3 past for persiting state

Example:
    from res.flows import FlowContext

    def some_function(df, **kwargs):
        return df

    df = pd.DataFrame(['123'],columns=['a'])

    with FlowContext(event) as fc:
        #this will be saved to s3 at a location determined by the flow context event args 
        data = fc.apply("my_f", some_function, df, key=fc.key)

        
"""


from res.flows import flow_context, flow_node_attributes, FlowContext
from res.utils import logger


def generator(event, context):
    def event_with_args(a):
        e = dict(event)
        if "args" not in e:
            e["args"] = {}
        e["args"].update(a)
        return e

    assets = [event_with_args({"day": i}) for i in range(90)]

    assets = [event_with_args({"test_arg": A}) for A in ["A", "B", "C"]]
    logger.incr("flows_sample_processed_assets", len(assets))
    return assets


@flow_node_attributes(
    "sample flow node reducer step",
    description="this is a reference flow node module",
    memory="410Mi",
)
def reducer(event, context):
    message = """{  "message" : "All done!" }"""
    print(message)
    return message


@flow_node_attributes(
    "sample flow node data handler step",
    description="this is a reference flow node module",
    memory="510Mi",
)
def handler(event, context):
    logger.incr("flows_sample_invoked", 1)

    spoof_fail = event.get("args", {}).get("spoof_fail")
    if spoof_fail:
        raise Exception("spoof fail workflow")

    messages = """[{  "message" : "Hello world" }]"""
    print(messages)
    # optionally annotate event or context which is relayed
    return messages


def on_failure(event, context):
    logger.incr("flows_sample_failed", 1)
    print("got an onExit->error and we can callback or something")


def on_success(event, context):
    logger.incr("flows_sample_succeeded", 1)
    print("got a onExit->success and we can callback or something")


def simple_handler(event, context):
    print("this is a simple handler that ignores the module structure")


def flow_context_error(event, context):
    with FlowContext(event, context) as fc:

        raise Exception("test")


def kafka_consumer(event, context):
    with FlowContext(event, context) as fc:
        kc = fc.connectors["kafka"]
        topic = fc.args["topic"]
        timeout = fc.args.get("timeout", 10)
        logger.info(f"consuming messages from topic {topic}")
        for message in kc[topic].consume(timeout_poll_avro=timeout):
            logger.info(message)


@flow_node_attributes(
    memory="120Gi",
)
def handle_bms(event, context={}):
    """
    sample data processing function
    we created an experimental onboard body node to be separate from the meta one for a style
    in reality we want to do almost everything except for apply color i.e
    we want to check how the body behaves on materials and get costs etc.
    if we load styles we can use the method get_request_for_white_style_with_meta_body in the marker
    to generate requests that are the correct shape for meta one but with some different options
    there is an argument that we should keep the separate but this example illustrates calling the meta one
    with payloads saved for styles airtable data distinct on  body-materials

    we wish to save data in and this can be used to evaluate nestings etc.

    from res.connectors.snowflake import CachedWarehouse
    CachedWarehouse("body_material_yard_usage", key="key").read()

    the major short coming of this approach is it lacks understanding of cmombos
    we could easily use style combo data to eval bodies better but the style data seems unreliable

    """
    import pandas as pd
    import res
    from res.flows.meta.marker import export_meta_one

    s3 = res.connectors.load("s3")

    data = s3.read(
        "s3://res-data-platform/samples/inputs/distinct_bms.feather"
    ).to_dict("records")
    problem_rows = []

    with FlowContext(event, context) as fc:
        watermark = fc.args.get("watermark", 0)
        res.utils.logger.info(f"Watermarker is {watermark}")
        for i, a in enumerate(data):
            if i < watermark:
                continue
            try:
                # the unfold geometries shows that for legacy 2d flows this would be important
                a = export_meta_one(
                    a, record_id_is_style=True, should_unfold_geometries=True
                )
            except Exception as ex:
                a["error"] = repr(ex)
                problem_rows.append(a)

        print("write errors")
        errors = pd.DataFrame(problem_rows)
        s3.write(
            f"s3://res-data-platform/samples/inputs/distinct_bms_problems_{watermark}.feather",
            errors,
        )


def image_joiner(event, context):
    from res.media.images import compositor

    with FlowContext(event, context) as fc:
        files = [p["key"] for p in fc.assets]
        compositor.join_images(files, fc.args.get("output_path"))

    return {}
