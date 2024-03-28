#! /opt/anaconda3/bin/python

#####            notes on cli execution                      ###
#  this file is added for an entry point for installed modules #
#  to run it directly the nest way is to run `python -m res`   #
#  from the directory that contains the res folder ..            #
# ##############################################################


import typer
from ast import literal_eval
from typing import Optional
import res
import yaml
import os
from pathlib import Path
import json

app = typer.Typer()

airtable_app = typer.Typer()
app.add_typer(airtable_app, name="airtable")

flow_app = typer.Typer()
app.add_typer(flow_app, name="flow")

dxf_app = typer.Typer()
app.add_typer(dxf_app, name="dxf")


def resolve_event_arg(s, name=None, env=None):
    if s is None:
        return None
    if os.path.isfile(s):
        with open(s, "r") as f:
            res.utils.logger.info(f"Loading the arg dict from {s}")
            if Path(s).suffix in [".yaml", ".yml"]:
                s = yaml.safe_load(f)
            else:
                s = json.load(f)

    elif isinstance(s, str):
        try:
            s = json.loads(s)
        except:
            raise ValueError(
                "The argument could not be parsed as JSON or a path to a json or yaml arg file!"
            )

    # use the non validated schema...
    if "apiVersion" not in s:
        s["apiVersion"] = "v0"

    # add the function as a flow node convention or for invoking apps via flow run
    if "metadata" not in s:
        s["metadata"] = {}
    s["metadata"]["name"] = name
    s["metadata"]["version"] = env

    return s


def try_parse_list(s):
    s = s.split(",") if s is not None and s != "" else None
    l = [s] if s and not isinstance(s, list) else s
    return [s for s in l if s != "" and s is not None]


@dxf_app.command("validate")
def dxf_app_validate(
    name: Optional[str] = typer.Option(None, "--name", "-n"),
):
    """
    install latest version of typer

    BAD example (at time of writing)
        python -m res dxf validate -n s3://meta-one-assets-prod/bodies/kt_4004/pattern_files/body_kt_4004_v4_pattern.dxf
    GOOD example
        python -m res dxf validate -n s3://meta-one-assets-prod/bodies/dj_2004/pattern_files/body_dj_2004_v1_pattern.dxf
    """
    from res.media.images.providers.dxf import DxfFile

    if name:
        errors = DxfFile(name).validate()

        if errors:
            res.utils.logger.warn(f"File has Validation Errors {errors}")
        else:
            res.utils.logger.info("There are no validation errors in the file")
    else:
        print("Pass a dxf file name to validate from local or s3 path")


@flow_app.command("run")
def flow_app_run_by_name(
    name: Optional[str] = typer.Argument(None),
    # these are optional args - we dont need them if we can load the default event from the name (not implemented)
    event: Optional[str] = typer.Argument(None),
    context: Optional[str] = typer.Argument(None),
):
    """
    Run a function locally - can be used in docker images to invoke functions
    The name should be something under flows that either is a specific function or a module with a handler function

    The event and context can be passed in. If event is_file, we parse as json or yaml otherwise we try to parse it as json
    Context is not really used here
    The event structure should have a metadata section for use in the FlowAPI - this is used to determine what method to invoke
    and also used when creating logging and other outputs related to the flow-node
    If no event is passed in, the event structure is constructed by convention and the name argument is used to determine what function to run in res-data
    """
    env = os.environ.get("RES_ENV", "development")

    event = resolve_event_arg(event, name, env)

    event = event or {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {
            "name": name,
            "version": env,
        },
        "assets": [],
        "task": {"key": "submitted-from-res"},
        "args": {},
    }

    context = context or {}

    fp = res.flows.FlowEventProcessor()
    fp.process_event(event, context)


@flow_app.command("invoke")
def flow_app_run_by_name(
    name: Optional[str] = typer.Argument(None),
    tag: Optional[str] = typer.Option(
        None,
        "--image-tag",
        "-t",
        help="(Optional) add a res-data docker image tag",
    ),
    # these are optional args - we dont need them if we can load the default event from the name (not implemented)
    event: Optional[str] = typer.Argument(None),
    context: Optional[str] = typer.Argument(None),
):
    """
    This is a developer tool to test functions remotely on the cluster.
    In this mode the tag field is used to run a version on a docker image built and pushed

    Example

    ./push_docker.sh your-tag
    python -m res flow invoke your.flow.function

    The function is expect to live under res.flows - flows are submitted async so you can look in the relevant argo UI for progress.
    If submission fails, there could/should be error logs in the relevant res-connect logs

    Add a tag e.g.
        python -m res flow invoke etl.relays.my_entity -t res-meta-sa
    """
    env = os.environ.get("RES_ENV", "development")

    event = resolve_event_arg(event, name, env)

    event = event or {
        "apiVersion": "resmagic.io/v1",
        "kind": "resFlow",
        "metadata": {
            "name": name,
            "version": env,
        },
        "assets": [],
        "task": {"key": "submitted-from-res"},
        "args": {},
    }

    template_name = "res-flow-node"

    # these could be env vars but leaving it for now
    url = f"https://datadev.resmagic.io/res-connect/flows/{template_name}"
    if env == "production":
        url = f"https://data.resmagic.io/res-connect/flows/{template_name}"
    if tag:
        url += f"?image_tag={tag}"

    res.utils.logger.info(f"submitting invocation request to {url}")
    r = res.utils.safe_http.request_post(url, json=event)

    if r.status_code == 200:
        res.utils.logger.info("job submitted")
    else:
        res.utils.logger.warn(
            f"There was a problem submitting the request - {r.json()}"
        )

    # TODO: it might be good to figure out what was the new submitted tasks and --await here
    # so that when we stop the program we kill the task
    # Todo this we just need argo to tell us what the job id is


@flow_app.command("event")
def flow_app_event(
    event: Optional[str] = typer.Argument(None),
    context: Optional[str] = typer.Argument(None),
):
    """
    we use positional args and not named as the docker context can run by passing them simply
    in generic fashion. e.g. argo templates can pass events in order to the docker run context
    "Event" mode specifically assumes there is enough information in the payload to find a handler
    We could also have a submit option where that could be chosen
    """
    # Just for comparison, if argo handled the event we would do this to send the template
    # argo = res.connectors.load("argo")
    # result = argo.handle_event(event, context)
    # but we call the simple function that is handled within a simple argo template which is in the event
    fp = res.flows.FlowEventProcessor()
    fp.process_event(event, context)


@airtable_app.command("snapshot")
def airtable_app_snapshot(
    base_id: Optional[str] = typer.Option(
        ...,
        "--base",
        "-b",
        help="the base to snapshot",
    ),
    table_id: Optional[str] = typer.Option(
        None, "--tables", "-t", help="(Optional) list tables to snapshot"
    ),
    merge: Optional[bool] = typer.Option(
        None,
        "--merge",
        "-m",
        help="(Optional) merge snapshots with latest table data",
    ),
):
    """
    a test base and test table: https://airtable.com/tblt9jo5pmdo0htu3/viwxAmdwJom7Q21Xk?blocks=hide
    the cli example is like so (tables are a comma seperated list in general)
    python -m res airtable snapshot --base=appaaqq2FgooqpAnQ --tables=tblt9jo5pmdo0htu3 -m
    """
    from .connectors.airtable import AirtableConnector

    tables = try_parse_list(table_id)
    service = AirtableConnector()
    base = service[base_id.rstrip(",")]
    tables = base.table_ids if tables is None else tables
    for table in tables:
        base[table].take_snapshot(merge_latest=merge)


if __name__ == "__main__":
    app()

# profile with
# fil-profile run -m res flow run -n dxa.printfile -m extract_image_elements -f ~/code/res/res-data-platform/tests/event_mocks/print_flow_event.yaml
