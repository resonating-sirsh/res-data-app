from res.flows import flow_node_attributes, FlowContext
from res.utils import logger
from res.connectors import load
from res.media.images.providers import DxfFile


def generator(event, context=None):
    def event_with_assets(a):
        e = {}
        for k in ["metadata", "apiVersion", "kind", "args", "task"]:
            e[k] = event[k]

        if not isinstance(a, list):
            a = [a]
        e["assets"] = a

        return e

    s3 = load("s3")

    assets = [
        event_with_assets(row)
        for row in s3.read(
            "s3://res-data-platform/samples/data/sized_bodies.feather"
        ).to_dict("records")
    ]
    return assets


@flow_node_attributes(
    memory="2Gi",
)
def handler(event, context=None):

    with FlowContext(event, context) as fc:
        for rec in fc.assets:
            body = rec["Body"]
            size = rec["n_size"]

            data = fc.apply(
                "nest_body",
                DxfFile.nest_body,
                data=body,
                res_normed_size=size,
                key=f"{body}-{size}",
            )

            logger.info(f"nested {len(data)} pieces")
