"""
This is a sample relay
"""

from res.utils import logger
from res.flows import FlowContext


def do_work(fc, data):
    # may load data as dgraph queries over all our data to prepare for work
    return data


def handler(event, context):
    """
    This is a simple queue processor that pulls work off kafka and writes out state to dgraph and airtable
    In reality it would do some work that would be sent to another queue i.e. it manages the state of its own queue and forwards work to others
    The forwarded work is anything that enters a DONE  stauts (and possibly other conditions)

    Most of the below code is boiler plate and we could configure it in a database - the main thing to do here is the `do_work` function
    This do_work could easily just be decorated in a handler and we can hide the other stuff in the decorator

        @queue_handler(name='my_entity)
        def do_work(fc, data):
            # ...
            return data

    A] Local testing:
    The consumer usually only works on the same cluster as Kafka but we can mock this by setting an env var which results in reading the data out of S3 as a mock

    ```bash
    EXPORT STUB_KAFKA_CONSUMER=true
    ```

    To exec the code using res you can run any function under res.flows

    ```bash
    python -m res flow run etl.relays.my_entity
    ```

    B] To run this on argo without argo-cd you can push a docker image with your own tag `./push_docker.sh my-tag`
    and then call the function with your image tag. It will call res-connect in dev (or prod depending on your env)

    ```bash
    ./push_docker.sh my-tag
    python -m res flow invoke etl.relays.my_entity -t=my-tag
    ```

    and then browse the Argo UI to watch progress. This runs for ever so be sure to turn it off

    C] To push this to dev follow the normal build process and TODO: configure a workflow to run this function probably as part of a set

    """
    with FlowContext(event, context) as fc:
        kc = fc.connectors["kafka"]
        dg = fc.connectors["dgraph"]
        # we could have different bases for dev and prod but need a way to manage names generally
        ac = fc.connectors["airtable"]["res-data-dev"]

        logger.info(f"Running consumer process in an infinite while loop...")
        while True:
            # for testing use a small batch size - in reality we should combine time outs and batch sizes for good latency
            # we can reduce to
            data = kc["my_entity"].consume(give_up_after_records=1)

            if len(data) == 0:
                continue

            logger.info(
                f"batch of {len(data)} records - updating state in dgraph (brutally for now)"
            )
            dg["my_entity"].update(data)

            logger.info(
                f"batch of {len(data)} records to send to airtable that are new records (TODO fix filters)"
            )
            ac["my_entity"].update(data)

            # we would clear anything out of the work queue that is done or cancelled
            # we can do special handling of issue states
            data = do_work(fc, data)

            # kc['SOME_OTHER_TOPIC'].publish(data)
            # also logic to split things onto different flows e.g. different materials get sent to different queues
            # general we can add keys to `data` and then assign keys to topics which will be published on exit


def on_failure(event, context):
    logger.incr("flows_sample_failed", 1)
    logger.info("got an onExit->error and we can callback or something")


def on_success(event, context):
    logger.incr("flows_sample_succeeded", 1)
    logger.info("got a onExit->success and we can callback or something")
