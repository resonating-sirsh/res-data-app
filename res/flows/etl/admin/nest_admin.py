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

    assets = [event_with_args({"day": i}) for i in range(90)]
    logger.incr("flows_sample_processed_assets", len(assets))
    return assets


@flow_node_attributes(memory="2Gi")
def handler(event, context=None):
    with FlowContext(event, context) as fc:
        s3 = fc.connectors["s3"]
        day = fc.args["day"]
        for environment in ["production", "development", "platform"]:
            for version in ["v0", "v1"]:
                all_data = []
                inp = f"s3://res-data-{environment}/flows/{version}/dxa-printfile/expv2/.meta/"
                logger.info(f"scrape logs for {inp}, day={day} ago")
                for f in s3.ls_info(
                    inp,
                    modified_after=relative_to_now(day + 1),
                    modified_before=relative_to_now(day),
                ):
                    key = f["path"].split("/")[-2]
                    # print('loading', key)
                    aenv = environment if environment == "production" else "dev"
                    lines = [
                        line
                        for f in s3.ls(f"s3://argo-{aenv}-artifacts/{key}")
                        for line in s3.read_text_lines(f)
                    ]
                    for l in lines:
                        if "Notify nesting solution" in l:
                            result = []
                            timestamp = l[: l.index(" [")]
                            start = l.index("{")
                            try:
                                data = literal_eval(l[start:].replace("\n", ""))

                                for a in data.get("nested_assets"):
                                    a["status"] = True
                                    a["job"] = key
                                    a["timestamp"] = timestamp
                                    result.append(a)
                                for a in data.get("omitted_assets"):
                                    a["status"] = False
                                    a["job"] = key
                                    a["timestamp"] = timestamp
                                    result.append(a)
                                break
                            except:
                                # parse error skipping
                                pass

                    all_data.append(pd.DataFrame(result))
                if len(all_data) > 0:
                    all_data = pd.concat(all_data)
                    out = f"s3://res-data-platform/samples/data/nest-logs/{environment}/{version}/{day}/data.csv"
                    logger.info(f"writing to {out}")
                    all_data.to_csv(out)


def reducer(event, context):
    pass
