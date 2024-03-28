import res
import pandas as pd
from ast import literal_eval
from res.utils.dates import relative_to_now
from res.utils import logger
from res.flows import FlowContext, flow_node_attributes


def generator(event, context):
    def event_with_args(arg):
        args = dict(event.get("args", {}))
        args.update(arg)

        return {
            "task": event["task"],
            "metadata": event["metadata"],
            "args": args,
            "apiVersion": "v0",
        }

    assets = [event_with_args({"day": i}) for i in range(180)]
    return assets


def get_transform(lines):
    l = list(lines)[0]
    try:
        l = literal_eval(l)
        if "new_material_code" in l:
            return l
    except:
        return None


@flow_node_attributes(memory="2Gi")
def handler(event, context=None):
    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]
        day = fc.args["day"]

        data = []
        for info in s3.ls_info(
            "s3://argo-dev-artifacts/",
            modified_after=relative_to_now(day + 1),
            modified_before=relative_to_now(day),
        ):
            f = info["path"]
            if "swap-style" in f:
                transform = get_transform(s3.read_text_lines(f))
                if transform:
                    transform["timestamp"] = info["last_modified"].isoformat()
                    data.append(transform)

                    break

    if len(data) > 0:
        all_data = pd.DataFrame([d for d in data])
        out = f"s3://res-data-platform/samples/data/material-swaps/{day}/data.csv"
        logger.info(f"writing to {out}")
        all_data.to_csv(out)


def reducer(event, context):
    pass
