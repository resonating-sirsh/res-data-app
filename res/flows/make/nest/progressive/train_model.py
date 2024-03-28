import res
import time
from res.flows import flow_node_attributes, FlowContext
from res.learn.optimization.packing.annealed.nn import train_model
from .utils import ping_rob


@flow_node_attributes("train_model.handler", memory="32G", cpu="16000m", mapped=False)
def handler(event, context):
    with FlowContext(event, context) as fc:
        train_model(**fc.args)


def _last_iter(path):
    max(
        (
            int(p.split("_")[-1].replace(".dat", ""))
            for p in res.connectors.load("s3").ls(path)
        ),
        None,
    )


def on_failure(event, context):
    with FlowContext(event, context) as fc:
        last_iter = _last_iter(fc.args.get("model_dir"))
        ping_rob(f"nesting job failed on {last_iter}")
        job = f"train-model-{int(time.time())}"
        res.connectors.load("argo").handle_event(
            {
                "apiVersion": "resmagic.io/v1",
                "args": {
                    **fc.args,
                    "first_iter": last_iter,
                },
                "assets": [],
                "kind": "resFlow",
                "metadata": {
                    "name": "make.nest.progressive.train_model",
                    "version": "primary",
                },
                "task": {
                    "key": job,
                },
            },
            context={},
            template="res-flow-node",
            image_tag="rhall-nn",
            unique_job_name=job,
        )


def on_success(event, context):
    with FlowContext(event, context) as fc:
        ping_rob(f"nesting job finished")
