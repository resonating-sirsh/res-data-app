"""
Res connect is a service that extends the res library so that that functions can be triggered via REST
_nudge .... ...
"""

from ctypes import ArgumentError
from res.flows.etl.production_status import update_status_table
import res
from res.connectors import kafka
from res.connectors.airtable import (
    AirtableConnector,
    AirtableMetadata,
)
from res.connectors.dynamo.DynamoConnector import AirtableDynamoTable
from res.utils.logging import logger
from res.utils.flask import helpers as flask_helpers
import json
import pandas as pd
import flask
from flask import Flask, request, Response, render_template
import requests
import re
import os
from res.flows import FlowEventProcessor
from res.flows.FlowEventProcessor import FlowEventValidationException
from healing import (
    get_nest_info,
    cache_timestamp,
    get_roll_info,
    update_roll_inspection_progress,
    sync_roll_to_healing_app,
    pre_cache_nest_info,
    inspectable_roll_info,
    get_manual_healing_data,
    renest_pieces,
    healable_printfiles,
    composite_printfile,
    retry_healing_sync,
)
from threading import Timer, Thread
from plugin_validation import PluginValidationHelper
import traceback
from res.flows.meta.ONE.meta_one import MetaOne
from flask_cors import cross_origin
from tenacity import retry, wait_fixed, stop_after_attempt
from res.utils.error_codes import ErrorCodes

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True
flask_helpers.attach_metrics_exporter(app)

PROCESS_NAME = "res-connect"
CELL_CHANGES_TOPIC = "airtableCellChanges"
ISSUES_TOPIC = "resIssues"
STATE_TRANSITION_TOPIC = "resStateTransitions"
ASSEMBLY_MASTER_TABLE_ID = "tblptyuWfGEUJWwKk"
RELAY_TOPICS = {}
DEFAULT_FLOW_TEMPLATE = "res-flow-node"
import decimal
from res.utils.env import get_res_connect_token

# bump


def must_be_post_response():
    return Response(
        "{'error':'Requests must be POST'}!",
        status=405,
        mimetype="application/json",
    )


def exception_response(ex):
    return Response(
        response=json.dumps({"error": f"Problem handling request: {repr(ex)}"}),
        status=500,
    )


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    logger.info("healthcheck")
    return Response("ok", status=200)


@app.route(
    f"/{PROCESS_NAME}/meta/analyze_artwork",
    methods=["POST"],
)
@cross_origin()
def analyze_artwork():
    """
    retrieve the last meta one made for that style [BMCS]
    additional filters can be added: m1hash, body_version, valid_only (true by default)

    good:
        s3://meta-one-assets-prod/artworks/jl/7a9e5c8a2cf5014fea0d7550fc473d64_300.png
        s3://resmagic/uploads/3dm1_tests/RUSTSTRIPE.tif
    bad:
        s3://resmagic/uploads/Channel_digital_image_CMYK_color.jpg
    weird (no dpi but pyvips defaults 300 on mac and 72 on linux):
        s3://resmagic/uploads/924150c3-842c-4ed5-95b2-581c0e02da54.png
    nudge..............
    ####
    """

    logger.info("analyze_artwork")
    request = flask.request.get_json()

    try:
        from res.media.images.outlines import analyze_artwork

        artwork_uri = request.pop("artwork_uri")
        logger.info(f"  uri: {artwork_uri}")

        result = analyze_artwork(artwork_uri, **request)
        if not "errors" in result:
            return Response(
                response=json.dumps(result),
                status=200,
            )
        else:
            return Response(
                response=json.dumps(result),
                status=404 if ErrorCodes.IMAGE_NOT_FOUND in result["errors"] else 400,
            )
    except Exception as ex:
        error_message = f"error accessing meta_one_details with request {request} --> {res.utils.ex_repr(ex)}"
        logger.warn(error_message)
        return Response(
            response=json.dumps({"error": f"Problem handling request: {request}"}),
            status=500,
        )


@app.route(
    f"/{PROCESS_NAME}/meta/product-meta-ones/<sku>",
    methods=["POST"],
)
def meta_one_details(sku):
    """
    retrieve the last meta one made for that style [BMCS]
    additional filters can be added: m1hash, body_version, valid_only (true by default)
    nudge.....

    """
    request = flask.request.get_json()
    m1hash = request.get("m1hash")
    bv = request.get("body_version")
    valid_only = request.get("valid_only", True)

    if str(bv) == "null":
        res.utils.logger.warn(f"A non integer body version was passed {bv}")
        bv = None
    try:
        if bv:
            int(float(str(bv).replace("v", "")))
    except:
        res.utils.logger.warn(f"A non integer body version was passed {bv}")
        bv = None

    from res.flows.meta.marker import MetaMarker
    from res.flows.meta.ONE.queries import _get_costs_match_legacy

    try:
        try:
            # this is a bit flawed right now because we only determine costs for the latest and we dont check if its valid
            # in future we should compute the effective costs based on the body and material for the sku and compute and color stuff from that
            # we will have the placed color files by version if required otherwise its all based on the body
            # also any trims etc should be versioned too

            meta_one_details = _get_costs_match_legacy(sku, body_version=bv)
            # ensure values or fallback -> piece_statistics is what we map to in the reader for legacy
            meta_one_details["piece_statistics"][0]["area_nest_yds_physical"]
        except Exception as ex:
            res.utils.logger.warn(f"Failing to load the new meta one {repr(ex)}")
            # legacy
            meta_one = MetaMarker.from_sku(
                sku,
                m1hash,
                body_version=bv,
                valid_only=valid_only,
                return_metadata_only=True,
            )
            meta_one_details = meta_one if meta_one else None
            if meta_one_details is not None and "nesting_stats" in meta_one_details:
                meta_one_details["piece_statistics"] = meta_one_details.pop(
                    "nesting_stats"
                )
                # for now - this is an int64 that should not be there. need to refactor further down
                if "index" in meta_one_details:
                    meta_one_details.pop("index")

        return Response(
            response=json.dumps({"data": meta_one_details}, cls=DecimalEncoder),
            status=200,
        )
    except Exception as ex:
        error_message = f"error accessing meta_one_details with params {sku} {request}: error --> {res.utils.ex_repr(ex)}"
        logger.debug(error_message)
        return exception_response()


@app.route(
    f"/{PROCESS_NAME}/meta/style-costs",
    methods=["POST"],
)
@cross_origin()
def get_style_costs():
    """
    compile some one costs
    """
    from res.flows.meta.ONE.meta_one import MetaOne

    token = None
    # try:
    #     headers = flask.request.headers
    #     bearer = headers.get("Authorization")
    #     token = bearer.split()[1].strip()
    #     # res.utils.logger.info(f"Got token {token} from {bearer}")
    # except Exception as ex:
    #     res.utils.logger.warn(f"No token passed or not valid {ex}")
    # if token != get_res_connect_token():
    #     return Response("Invalid token", status=401)

    request = flask.request.get_json()

    water = request.get("less_water_pct", 75.4)
    ggas = request.get("less_greenhouse_gas_pct", 69.4)
    dye_waste = request.get("less_dye_waste_pct", 97.0)
    biodegradeble_pct = request.get("biodegradeble_pct", 99.0)

    sku = request.get("sku")
    use_base_size = request.get("use_base_size", False)

    try:
        if len(sku.split()) == 3 and use_base_size == True:
            m1 = MetaOne.for_sample_size(sku)
        else:
            m1 = MetaOne.safe_load(sku)

        costs = m1.read_costs()
        for p in m1:
            break

        data = {}
        data.update(costs)
        data["name"] = m1.name
        data["meta_one_material_saving"] = 0
        #
        material_fabric_type_base_saving = 50 if not p.is_stable_material else 60
        data["material_saving_pct_estimate"] = material_fabric_type_base_saving

        data["3d_file"] = res.connectors.load("s3").generate_presigned_url(
            f"{m1.input_objects_s3}/3d.glb"
        )

        data["less_water_pct"] = water
        data["less_greenhouse_gas_pct"] = ggas
        data["less_dye_waste_pct"] = dye_waste
        data["biodegradeble_pct"] = biodegradeble_pct

        return Response(
            response=json.dumps({"data": data}),
            status=200,
        )
    except Exception as ex:
        error_message = f"error accessing meta_one_details with params   {request}: error --> {res.utils.ex_repr(ex)}"
        logger.debug(error_message)
        return exception_response()


@app.route(
    f"/{PROCESS_NAME}/meta/one-costs",
    methods=["POST"],
)
def get_meta_one_costs():
    """
    compile some one costs
    """

    token = None
    try:
        headers = flask.request.headers
        bearer = headers.get("Authorization")
        token = bearer.split()[1].strip()
        # res.utils.logger.info(f"Got token {token} from {bearer}")
    except Exception as ex:
        res.utils.logger.warn(f"No token passed or not valid {ex}")
    if token != get_res_connect_token():
        return Response("Invalid token", status=401)

    request = flask.request.get_json()

    one_number = request.get("one_number")

    from res.flows.meta.ONE.meta_one import MetaOne

    try:
        one_number = int(str(one_number))

        return Response(
            response=json.dumps(
                {"data": MetaOne.read_costs_for_one(one_number=one_number)}
            ),
            status=200,
        )
    except Exception as ex:
        error_message = f"error accessing meta_one_details with params   {request}: error --> {res.utils.ex_repr(ex)}"
        logger.debug(error_message)
        return exception_response()


@app.route(
    f"/{PROCESS_NAME}/vs_plugin_validation",
    methods=["POST"],
)
def vs_plugin_validation():
    if request.method != "POST":
        return must_be_post_response()

    try:
        data = json.loads(request.data, strict=False)

        PluginValidationHelper().submit_validation_report(data)

        return Response(
            response=json.dumps({}),
            status=200,
        )
    except Exception as ex:
        return exception_response(ex)


@app.route(
    f"/{PROCESS_NAME}/get_validation_data",
    methods=["POST"],
)
def get_validation_data():
    if request.method != "POST":
        return must_be_post_response()

    try:
        data = json.loads(request.data, strict=False)

        result = PluginValidationHelper().validate(data)

        return Response(
            response=json.dumps(result),
            status=200,
        )
    except Exception as ex:
        return exception_response(ex)


# for backwards compatibility, it has moved on from just validating piece names ^^
@app.route(
    f"/{PROCESS_NAME}/validate_piece_names",
    methods=["POST"],
)
def validate_piece_names():
    return get_validation_data()


@app.route(
    f"/{PROCESS_NAME}/vs_ignore_validation_error/<username>/<piece>/<error>/<ignored>",
    methods=["GET"],
)
@cross_origin()
def vs_ignore_validation_error(piece, error, username, ignored):
    try:
        ignored = ignored.lower() == "true"
        logger.info(f"ignoring error {error} for {username}")
        PluginValidationHelper().ignore_validation_error(
            piece, error, username, ignored
        )

        return Response(
            response=f"""
            <div style="font-family: monospace">
                Successfully {"added" if ignored else "removed"} rule to ignore "{error}" for {username} 
                <br/>
                <br/>
                <br/>
                <button onclick="goBack()">Go Back</button>
            </div>

                <script>
                    function goBack() {{
                        window.history.back();
                }}
                </script>
            """,
            status=200,
        )
    except Exception as ex:
        return exception_response(ex)


@app.route(
    f"/{PROCESS_NAME}/one_code/<code>",
    methods=["GET"],
)
def handle_one_code(code):
    return "GOT IT, WILL HANDLE THIS REQUEST (SOMEHOW)"


@app.route(
    f"/{PROCESS_NAME}/uploads/<otype>/<name>",
    methods=["GET"],
)
def upload_path(name, otype="dxa_body_bundle"):
    """
    a way to upload files of different types by requesting a provider uri location
    hard code one type for now

    name here is BRAND-BODY-VERSION-ETC
    if name has .zip at end which it should not we remove it

    s3://meta-one-assets-prod/bodies/3d_body_files/tk_6121/v3/uploaded/


    uploads go to uploads instead of extracted for testing
    """

    # TODO check bearer token and stuff
    res.utils.logger.info(f"Request type: {otype}")

    parts = name.lower()
    parts = parts.split("-")
    body_code = "-".join(parts[0:2]).lower().replace("-", "_")
    body_version = parts[2].lower().split(".")[0]

    s3 = res.connectors.load("s3")
    uri = f"BAD REQUEST {otype} - choose a correct upload profile - todo warp this end point properly"
    if otype == "dxa_body_bundle":
        # e.g.  s3://meta-one-assets-prod/bodies/3d_body_files/tk_6121/v3/uploaded/
        path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code}/{body_version}/uploaded/{name}"
        uri = s3.generate_presigned_url(path, for_upload=True)

    return uri


@app.route(
    f"/{PROCESS_NAME}/meta/meta-one/approve/",
    methods=["GET", "POST"],
)
def approve_metaones():
    """
    post the same kwargs we send to the function... ...

     {
        'body'
        'color'
        'body_version
     }

     {
         'body'
         'path'
     }

    """
    from res.flows.meta.marker import MetaMarker

    if request.method == "GET":
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Post the search parameters as a dictionary"}
            ),
        }
    elif request.method != "POST":
        return must_be_post_response()

    try:
        data = json.loads(request.data, strict=False)

        if "path" not in data and "color" not in data:
            raise ArgumentError(
                "you must specify either a meta one path or a style color to invalidate a meta-one set"
            )

        res.utils.logger.info(f"Approving meta-ones with query {data}")
        result = MetaMarker.find_meta_one(**data, _approve_result=True)

        data = {
            "message": f"{len(result)} records updated"
        }  # result.to_dict("records")

        return Response(
            response=json.dumps({"data": data}),
            status=200,
        )
    except Exception as ex:
        return exception_response(ex)


@app.route(
    f"/{PROCESS_NAME}/meta/meta-one/invalidate/",
    methods=["GET", "POST"],
)
def invalidate_metaones():
    """
    post the same kwargs we send to the function

     {
        'body'
        'color'
        'body_version'
     }

     {
         'body'
         'path'
     }

    """

    if request.method == "GET":
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Post the search parameters as a dictionary"}
            ),
        }
    elif request.method != "POST":
        return must_be_post_response()

    try:
        data = json.loads(request.data, strict=False)

        if "path" not in data and "color" not in data:
            raise ArgumentError(
                "you must specify either a meta one path or a style color to invalidate a meta-one set"
            )

        res.utils.logger.info(f"Invalidating meta-ones with query {data}")
        # result = MetaMarker.find_meta_one(**data, _invalidate_result=True)
        # probably wont use this path - but add contracts directly
        MetaOne.invalidate_meta_one(data["sku"], body_version=data.get("body_version"))
        data = {"message": "done"}  # result.to_dict("records")

        return Response(
            response=json.dumps({"data": data}),
            status=200,
        )
    except Exception as ex:
        return exception_response(ex)


@app.route(
    f"/{PROCESS_NAME}/meta/meta-one/search/",
    methods=["GET", "POST"],
)
def search_metaones():
    """
    post the same kwargs we send to the function
    """
    from res.flows.meta.marker import MetaMarker

    if request.method == "GET":
        return {
            "statusCode": 200,
            "body": json.dumps(
                {"message": "Post the search parameters as a dictionary"}
            ),
        }
    elif request.method != "POST":
        return must_be_post_response()

    try:
        data = json.loads(request.data, strict=False)

        result = MetaMarker.find_meta_one(**data)

        data = result

        return Response(
            response=json.dumps({"data": data}),
            status=200,
        )
    except Exception as ex:
        return exception_response(ex)


@app.route(
    f"/{PROCESS_NAME}/meta-one/body",
    methods=["GET", "POST"],
)
def refresh_meta_body():
    """
    <
      body_code
      flow
      default_color ?
      default_size_only=False

      unpack_path ?
    >

    This thing kicks of the meta one on the new body onboarding flow... we need to make sure when we post we take this into account
        MetaMarker.make_body_metaone('KT-2011', sync_sew=True)

    furthermore, we can add an extract=True to make sure we unpack assets loaded from BZW

    also we can take an option that says if we want meta ones for all sizes or just one and when we do one we can add the sew to just the sample size

    se could also add default materials and colors here too -> this is all in the payload structure

      path: path
      body_version
    >

    example zip path: s3://meta-one-assets-prod/bodies/3d_body_files/tk_6121/v3/uploaded/TK-6121-V3-3D_BODY.zip
    the body unpack location is probably without the last two parts but we can control it as f(body, flow)


    also we can take an option that says if we want meta ones for all sizes or just one and when we do one we can add the sew to just the sample size
    we could also add default materials and colors here too -> this is all in the payload structure

    LAMBDA S3 WEBHOOK:
     see https://us-east-1.console.aws.amazon.com/lambda/home?region=us-east-1#/functions/DigitalAssetsCreated?tab=code
     which posts the path etc. of detected files - that webhook triggers this function for multiple pathways

    {
        "body_code" : "jr_3114",
        "flow" : "3d",
        "body_version" : "v2",
        "path" : "s3://meta-one-assets-prod/bodies/3d_body_files/uploads/dxa_body_bundle/jr-3114/v2/JR-3114-V2-3D_BODY.zip
    }
    """

    from res.flows.meta.body.request import get_sample_payload

    return Response(response=json.dumps({"data": None}), status=200)

    argo = res.connectors.load("argo")
    try:
        data = json.loads(request.data, strict=False)
        res.utils.logger.debug(data)
        e = get_sample_payload()
        e["args"] = data
        supplied_job_name = f"body-request-{data.get('body_code').lower().replace('_','-')}-{res.utils.res_hash().lower()}"
        _ = argo.handle_event(
            e, template=DEFAULT_FLOW_TEMPLATE, unique_job_name=supplied_job_name
        )

        return Response(
            response=json.dumps({"data": e}),
            status=200,
        )
    except Exception as ex:
        return Response(
            response=json.dumps(
                {"error": f"Problem handling request: {res.utils.ex_repr(ex)}"}
            ),
            status=500,
        )


@app.route(
    f"/{PROCESS_NAME}/make/orders",
    methods=["GET", "POST"],
)
def make_orders():
    """
    Refactor this as a generic endpoint to ingest from other systems
    Rather than orders use <endpoint> and then load the transformer dynamically to relay to kafka
    ... ... ...
    """
    if request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}!",
            status=405,
            mimetype="application/json",
        )

    data = {"id": "test"}
    try:
        # from res.flows.etl.orders import create_one_to_make_orders
        from res.flows.sell.orders.process import create_one_handler

        data = json.loads(request.data, strict=False)

        # temp
        import uuid

        h = data.get("id", uuid.uuid1())
        data = pd.DataFrame([data])

        from uuid import uuid1

        channel = request.args.get("orderChannel", "resmagic.io")

        if channel == "resmagic.io":
            logger.debug(f"Received payload {data} on order channel {channel}")
            topic_name = "resMake.orders"

            data.to_csv(
                f"s3://res-data-platform/samples/make-orders/create-one-verified/{uuid1()}.csv"
            )

            # data = create_one_to_make_orders(data)
            # ....
            # # do some renaming and mapping here and post to kafka
            # res.connectors.load("kafka")[topic_name].publish(
            #     data,
            #     use_kgateway=True,
            #     coerce=True,
            # )
            data = create_one_handler(data)

    except Exception as ex:
        logger.warn(f"Error processing payload {data} : {repr(ex)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {
                    "message": "There was an error processing the make/orders payload and sending to kafka - see the res-connect logs"
                }
            ),
        }

    return {
        "statusCode": 200,
    }


@app.route(
    f"/{PROCESS_NAME}/flows/<template>",
    methods=["GET", "POST"],
)
@cross_origin()
def flow_task(template=DEFAULT_FLOW_TEMPLATE):
    TASK_TOPIC = "flowTasks"

    if request.method == "GET":
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "In future we promise to describe the flow and its parameters!!!"
                }
            ),
        }
    elif request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}!",
            status=405,
            mimetype="application/json",
        )

    # get the client id from the header...
    client_id = None
    calling_uid = None

    try:
        data = json.loads(request.data, strict=False)
    except:
        return Response("{'error':'Invalid body JSON'}", status=400)

    fp = FlowEventProcessor()

    if template == DEFAULT_FLOW_TEMPLATE:
        validation_errors = []
        try:
            validation_errors = fp.validate(data)
        except FlowEventValidationException as fex:
            validation_errors = [repr(fex)]

        if len(validation_errors) > 0:
            return {
                "statusCode": 500,
                "body": json.dumps(
                    {
                        "message": "The task payload is invalid",
                        "errors": validation_errors,
                    }
                ),
            }

    argo = res.connectors.load("argo")

    image_tag = request.args.get("image_tag")
    assumed_ops = request.args.get("assumed_ops")
    ##assumed ops

    # if this is None, argo connector will auto generate a name from the template ... ...
    supplied_job_name = argo.try_resolve_job_name_from_payload(data)

    logger.info(
        f"submitting event to argo with template {template}, job name {supplied_job_name}, image tag {image_tag} aand payload {data}"
    )

    try:
        result = argo.handle_event(
            data,
            context={"image_tag": image_tag},
            template=template,
            image_tag=image_tag,
            unique_job_name=supplied_job_name,
            assumed_ops=assumed_ops,
        )
    except Exception as ex:
        logger.info(f"Error in submitting payload: {traceback.format_exc()}")
        return {
            "message": f"Failed to run template on argo - check if the template exists. Check the res-connet logs",
            "statusCode": 500,
        }

    return {
        "statusCode": 200,
    }


# this is a duplicate of the nest added to phase out the old approach but allow for backwards compat
@app.route(
    f"/{PROCESS_NAME}/res-nest",
    methods=["GET", "POST"],
)
def res_nest():
    """
    perform nesting with payload. payload structure is

    {
       ... metadata ...
        "args":{}
    }
    """

    if request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}", status=405, mimetype="application/json"
        )

    try:
        payload = json.loads(request.data, strict=False)

        logger.debug(f"Received nest payload {payload}")

        # the legacy handler used event to post to printfile but we dont keep the event outer property in the argo submission in future
        event = payload.get("event", None)
        if event == None:
            raise Exception(
                f"event was not included in the payload keys. Keys submitted are {list(payload.keys())}"
            )

        args = event.get("args", None)
        if args == None:
            raise Exception(
                f"args were not submitted in event. Keys submitted were {list(event.keys())}"
            )

        for k in ["jobKey", "material"]:
            v = args.get(k)
            if v == None:
                raise Exception(
                    f"{k} was not submitted in the event/args. Keys submitted were {list(args.keys())}"
                )

        logger.info("nest: CHECKS FOR BAD JOB KEYS V0")
        jobKey = args.get("jobKey", "")
        if "_" in jobKey:
            jobKey = jobKey.replace("_", "-")
            payload["event"]["args"]["jobKey"] = jobKey
            logger.debug("replacing underscores in invalid key for nest")

        valid_job_key = r"[a-z0-9]([-a-z0-9]*[a-z0-9])?"
        if not re.compile(valid_job_key).fullmatch(jobKey):
            raise Exception(
                f"the job key is not valid - must match regex - {valid_job_key}. We recommend lower case alphanumeric characters possibly with hyphens/dashes or underscores"
            )

        payload["event"]["args"]["addCutline"] = False

        logger.info(f"submitting payload {payload}")

        argo = res.connectors.load("argo")
        image_tag = None
        result = argo.handle_event(
            payload["event"],
            context={"image_tag": image_tag},
            template="res-nest",
            image_tag=image_tag,
            unique_job_name=jobKey,
        )

        return {"statusCode": 200, "body": {"message": "success"}}

    except Exception as ex:
        logger.error(f"Failed to process nesting request: {repr(ex)}", exception=ex)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": f"Problem handling request with payload - {repr(ex)}"}
            ),
        }


@app.route(
    f"/{PROCESS_NAME}/nest",
    methods=["GET", "POST"],
)
def nest():
    """
    perform nesting with payload. payload structure is

    {
       ... metadata ...
        "args":{}
    }
    """

    if request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}", status=405, mimetype="application/json"
        )

    try:
        payload = json.loads(request.data, strict=False)

        logger.debug(f"Received nest payload {payload}")

        # the legacy handler used event to post to printfile but we dont keep the event outer property in the argo submission in future
        event = payload.get("event", None)
        if event == None:
            raise Exception(
                f"event was not included in the payload keys. Keys submitted are {list(payload.keys())}"
            )

        args = event.get("args", None)
        if args == None:
            raise Exception(
                f"args were not submitted in event. Keys submitted were {list(event.keys())}"
            )

        for k in ["jobKey", "material"]:
            v = args.get(k)
            if v == None:
                raise Exception(
                    f"{k} was not submitted in the event/args. Keys submitted were {list(args.keys())}"
                )

        logger.info("nest: CHECKS FOR BAD JOB KEYS V0")
        jobKey = args.get("jobKey", "")
        if "_" in jobKey:
            jobKey = jobKey.replace("_", "-")
            payload["event"]["args"]["jobKey"] = jobKey
            logger.debug("replacing underscores in invalid key for nest")

        valid_job_key = r"[a-z0-9]([-a-z0-9]*[a-z0-9])?"
        if not re.compile(valid_job_key).fullmatch(jobKey):
            raise Exception(
                f"the job key is not valid - must match regex - {valid_job_key}. We recommend lower case alphanumeric characters possibly with hyphens/dashes or underscores"
            )

        payload["event"]["args"]["addCutline"] = False

        logger.info(f"submitting payload {payload}")

        response = requests.post(
            "http://argo.resmagic.io/printfile",
            data=json.dumps(payload),
            headers={"content-type": "application/json"},
        )

        # argo = res.connectors.load("argo")
        # image_tag = None
        # result = argo.handle_event(
        #     payload["event"],
        #     context={"image_tag": image_tag},
        #     template="res-nest",
        #     image_tag=image_tag,
        #     unique_job_name = jobKey
        # )

        return {"statusCode": 200, "body": {"message": "success"}}

    except Exception as ex:
        logger.error(f"Failed to process nesting request: {repr(ex)}", exception=ex)
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"error": f"Problem handling request with payload - {repr(ex)}"}
            ),
        }


@app.route(
    f"/{PROCESS_NAME}/notify_airtable_cell_change/<profile>/<base>/<table>",
    methods=["GET", "POST"],
)
def airtable_notify_cell_change(profile, base, table):
    """
    Will be deployed to resmagic e.g.
    https://datadev.resmagic.io/res-connect/notify_airtable_cell_change/%3Cprofile%3E/%3Cbase%3E/%3Ctable%3E
    The airtable Fields table must use this pattern to register webhooks. Airtable will call this endpoint when
    there are changes on the webhook.
    This function saves changes in dynamo and checks for true changes then writes to kafka
    A consumer of the kafka topic can do further processing of kanban state changes or issues etc.
    """
    if request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}", status=405, mimetype="application/json"
        )
    try:
        logger.incr(f"{table}.webhook_invalidated", 1)
        body = json.loads(
            request.data, strict=False
        )  # strict = False allow for escaped char
    except ValueError:
        return Response(
            "{'error':'Invalid JSON'}", status=400, mimetype="application/json"
        )

    # this is what we use in serverless so being consistent even though will probably focus on flask
    body["pathParameters"] = {"profile": profile, "base": base, "table": table}

    return webhook_notification_handler(body, {})


@app.route(
    f"/{PROCESS_NAME}/airtable_webhook_invalidation_handler",
    methods=["GET", "POST"],
)
def airtable_webhook_invalidation_handler():
    """
    invlaidate the wenbook causing it to be rebuilt from metadata
    airtable will call back to whatever webhook call back we specify when creating the webhook
    We try to delete any webhooks in the same profile when creating new ones for the same table
    """
    if request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}", status=405, mimetype="application/json"
        )
    try:
        # body = json.loads(event['body'])
        body = json.loads(
            request.data, strict=False
        )  # strict = False allow for escaped char
    except ValueError:
        return Response(
            "{'error':'Invalid JSON'}", status=400, mimetype="application/json"
        )

    # this is what we use in serverless so being consistent even though will probably focus on flask
    # body["pathParameters"] = {"profile": profile, "base": base, "table": table}

    return webhook_invalidation_handler(body, {})


# TODO: when we start building up some prod event logic we can move this code to somewhere better
def track_production_status(changes):
    """
    This is a temporary thing to keep track of production requests wrt orders
    In future we should manage this in kafka in the order processor
    Adding this here as part of the webhook handler to keep context in a lookup
    """
    from res.connectors.dynamo import ProductionStatusDynamoTable
    from res.utils.dataframes import rename_and_whitelist

    # only for this table
    prod_events = changes[(changes["table_id"] == ASSEMBLY_MASTER_TABLE_ID)]

    if len(prod_events) == 0:
        return

    prod_events = prod_events.sort_values("timestamp").drop_duplicates(
        subset=["record_id", "field_id"], keep="last"
    )

    # get timestamps
    times = (
        prod_events[["record_id", "timestamp"]].drop_duplicates().set_index("record_id")
    )
    prod_events = (
        prod_events.pivot("record_id", "column_name", "cell_value")
        .join(times)
        .reset_index()
    )

    # MAKE ONE PRODUCTION see master table
    prod_events = rename_and_whitelist(
        prod_events,
        columns={
            "Factory Request Name": "key",
            "SKU": "sku",
            "Order Number v3": "one_number",
            "Current Assembly Node": "current_status",
            "record_id": "record_id",
            "timestamp": "created_at",
            "body_version": "body_version",
            "SIZE": "size_code",
        },
    )

    prod_events = prod_events[prod_events["one_number"].notnull()].reset_index(
        drop=True
    )

    def treat_list(x):
        if isinstance(x, list):
            return ",".join(x)
        return None

    prod_events["current_status"] = prod_events["current_status"].map(treat_list)

    logger.info(
        f"tracking production status fields of length {len(prod_events)}. Updating dynamo..."
    )

    ps = ProductionStatusDynamoTable()

    ps.update(prod_events)

    return prod_events


def relay_changes_to_kafka_topics(changes, metadata, table_id):
    if table_id in RELAY_TOPICS:
        try:
            info = RELAY_TOPICS[table_id]
            topic_name = info["topic"]
            type_name = info["type"]
            schema = res.utils.schema.types.get_type(type_name)
            mapping = dict(
                pd.DataFrame(schema["fields"])[["airtable_field_name", "name"]]
                .dropna()
                .values
            )
            # i still have to write this and its because i want to check the direct publish version
            res.connectors.load("kafka")[topic_name].publish(
                changes.rename(columns=mapping), use_kgateway=True
            )
        except Exception as ex:
            logger.warn(f"failed to publish to kafka because {repr(ex)}")


def handle_specific_tables(table_id, changes):
    """
    We can apply custom logic for specific table to support use cases
    this is something we need to do while migrating backend processing from airtable.
    """

    if len(changes) > 0:
        table_name = None
        try:
            if table_id == "tblUcI0VyLs7070yI":
                table_name = "fulfillment order items"

                from res.flows.etl import production_status

                data = production_status.from_changes(changes)
                update_status_table(None, data)

            if table_id == "tblwDQtDckvHKXO4w":
                table_name = "assets"
                from res.flows.etl import production_status

                data = production_status.update_ones_on_rolls(changes)

        except Exception as ex:
            logger.warn(
                f"Failed to apply table specific changes for {table_id} ({table_name}): {repr(ex)}"
            )


def handle_specific_tables_after_dynamo_filter(table_id, confirmed_changes, changes):
    if len(changes) > 0:
        table_name = None
        try:
            if table_id == "tblDdqL0Dt0ypJwj4":
                """
                Here we are tracking resource status changes with all the attributes for the resource
                We can add an interval event logic by hour segments using the dataframe helper
                """

                def is_not_in_use(s):
                    if pd.isnull(s):
                        return False
                    if s.lower() in ["production", "off"]:
                        return False
                    return True

                table_name = "make resource states"
                ch = AirtableConnector.cell_status_changes_with_attributes(
                    confirmed_changes,
                    changes,
                    track_columns="Resource Status",
                    attributes=["Name", "Resource Type"],
                )
                hourly = res.utils.dataframes.make_hour_segment_durations(ch)
                logger.info(
                    f"publishing {len(hourly)} records to makeNodeResourceEvents topic for changes on {table_name}"
                )

                # This is important: the old cell value captures the time segment - the new cell value is counting from now
                # in reporting we can use the latest time stamp to get time until now values
                hourly["is_not_in_use"] = (
                    hourly["old_cell_value"].map(is_not_in_use).fillna(False)
                )
                kafka.KafkaConnector()["makeNodeResourceEvents"].publish(
                    hourly,
                    show_failed=True,
                    coerce=True,
                    use_kgateway=True,
                    snake_case_names=True,
                )
                logger.info(f"Successfully published changes to {table_name}")

        except Exception as ex:
            logger.warn(
                f"Failed to apply table specific changes for {table_id} ({table_name}): {repr(ex)}"
            )


def webhook_notification_handler(event, context):
    """
    Receive a ping callback from Airtable based on the webhooks API contract
    We provide a callback notification url api.resmagic.io/events/airtable/webhook/{profile}/{base}
    And airtable posts

    {
        "base": {"id": "appXXX"},
        "webhook": {"id": "achYYY"},
        "timestamp": "2020-03-11T21:25:05.663Z"
    }

    We use our AirtableConnector to stream changes from the webhook
    There is some logic to "lock" the connection to avoid concurrency issues but this may need some revision
    When we call hook.run() it gets all changes from the webhook and writes them to our store

    """

    def get_state_transitions_fn(metadata, stype, suffix=""):
        def _get_state_transitions(changes):
            if changes is None or len(changes) == 0:
                return changes
            df = AirtableMetadata.filter_type(stype, metadata)(changes).reset_index()
            df["state_type"] = stype + suffix
            return df.drop_duplicates(subset=["record_id"])

        return _get_state_transitions

    def get_cell_time_delta(row):
        try:
            return (row["timestamp"] - row["old_timestamp"]).minutes
        except:
            pass

    status = 200
    message = "airtable notification handled successfully"
    needed_invalidation = False
    change_count = 0
    try:
        req = event["pathParameters"]
        logger.debug(f"received a ping {event}")
        wid = event["webhook"]["id"]
        logger.debug(f"received request with path {req} - body {event}")

        profile = req["profile"]

        connector = AirtableConnector(profile=profile)
        table_id = AirtableMetadata._table_id_from_webhook_id(wid)
        table = connector[req["base"]][table_id]

        ############ CUSTOM FLOW - PIGGY BACK ON EXISTING FOR NOW #########  #########
        ## we will focus on this approach from now on and deprecate the other use case
        if profile in ["development", "production"]:
            try:
                logger.info(f"handling non legacy flow")
                data = table.changes(require_lock=True)

                data = data.sort_values("timestamp").drop_duplicates(
                    subset=["record_id", "column_name"], keep="last"
                )
                ts = data[["record_id", "timestamp"]].groupby("record_id").max()
                data = data.pivot("record_id", "column_name", "cell_value").join(ts)
                # get entity info somehow - there is some way to map schema on airtable to kafka topics

                topic = table.table_name

                logger.info(
                    f"sending airtable data for table {table.table_name} to topic {topic}"
                )
                kafka.KafkaConnector()[topic].publish(
                    data, show_failed=True, coerce=True, use_kgateway=True
                )

            except Exception as ex:
                logger.warn(
                    f"error handling new flow for table {table_id} - {repr(ex)}"
                )
            return {
                "statusCode": status,
                "body": json.dumps(
                    {
                        "message": "handled",
                    }
                ),
            }
        ##############################################################################
        logger.info(
            f"loading metadata for airtable table {table_id} ({table.table_name})"
        )

        metadata = table.get_extended_field_metadata()

        dynamo_table = AirtableDynamoTable()
        changes = table.changes(require_lock=True)

        # flow 1: for assembly, when we track state we track ALL of the row so we dont introduce null values and update status (PATTERN X)
        # below is for tracking genuine cell changes PATTER Y
        if table_id == ASSEMBLY_MASTER_TABLE_ID:
            if len(changes) > 0:
                track_production_status(changes)

        handle_specific_tables(table_id, changes)

        relay_changes_to_kafka_topics(changes, metadata, table_id)

        # TODO the changes are fetch in a transaction - we could roll back the cursor if we fail to write to kafka ??
        change_count = len(changes)
        # increment all events from the webhook (not real changes) and specific dimension
        logger.incr(f"webhook_messages", change_count)
        logger.incr(f"{table_id}.webhook_messages", change_count)
        if change_count > 0:
            # below should be mockable
            # we can add special row metadata that is not available on the cell changes for this batch
            changes = AirtableMetadata.annotate_changes(changes, metadata=metadata)
            logger.info(
                f"based on the streamed cells of size {change_count} we are checking for changes in the key value store. Annotated and checking {len(changes)} records"
            )
            confirmed_changes = dynamo_table.insert_new_batch(changes)

            message = f"airtable notification handled successfully. processed perceived {change_count} cell changes."

            handle_specific_tables_after_dynamo_filter(
                table_id, confirmed_changes, changes
            )

            if len(confirmed_changes) > 0:
                # increment actual field changes total and on the dimension
                logger.incr(f"webhook_cell_changes", confirmed_changes)
                logger.incr(f"{table_id}.webhook_cell_changes", confirmed_changes)

                # this is a nullable int so better to treat it as a string
                confirmed_changes["old_cursor"] = confirmed_changes["old_cursor"].map(
                    str
                )

                # publish to kafka
                # going ti disable sending cell changes because we should just process what we want
                # when adding this back, there is an error on node which is boolean ??
                # logger.info(f"publishing {len(changes)} cell value changes to kafka ->")
                # kafka.KafkaConnector()[CELL_CHANGES_TOPIC].publish(
                #     confirmed_changes, show_failed=True, use_kgateway=True
                # )

                # try:
                #     track_production_status(confirmed_changes)
                # except Exception as ex:
                #     logger.warn(f"Failed to track production confirmed_changes {repr(ex)}")

                logger.info(f"publishing any state transitions to kafka topic")
                # experimental - kanbans in different place mean different things
                # in the assembly master table this is a redundant overview but it can be useful to track it
                suffix = "" if table_id != ASSEMBLY_MASTER_TABLE_ID else " - Assembly"
                kanban_filter = get_state_transitions_fn(
                    metadata, "Kanban - Summary", suffix
                )

                confirmed_changes["time_delta_minutes"] = confirmed_changes.apply(
                    get_cell_time_delta, axis=1
                )

                kafka.KafkaConnector()[STATE_TRANSITION_TOPIC].publish(
                    kanban_filter(confirmed_changes),
                    show_failed=True,
                    coerce=True,
                    use_kgateway=True,
                )
                kanban_filter = get_state_transitions_fn(
                    metadata, "Kanban - Leaf", suffix
                )
                kafka.KafkaConnector()[STATE_TRANSITION_TOPIC].publish(
                    kanban_filter(confirmed_changes),
                    show_failed=True,
                    coerce=True,
                    use_kgateway=True,
                )
            else:
                logger.info(
                    "There are no changes to relay on because they all appear to already be in the key value store."
                )
        else:
            logger.info("No changes on the webhooks cursor at this time.")

        invalid_webhooks = table.refresh_invalid_webhook()
        needed_invalidation = len(invalid_webhooks) > 0

    except Exception as ex:
        logger.incr(f"webhook_handler_errors", 1)
        logger.incr(f"{table_id}.webhook_handler_errors", 1)
        # setting the status to 200 because we can just keep the health of the system up from airtable's perspective
        # because we still need monitoring anyway internally. there is no advantage to failing the response
        status = 200
        message = repr(ex)
        logger.warn(
            f"Something went wrong when running the webhook stream processing for event {event} - error info:  {repr(ex)}"
        )

    return {
        "statusCode": status,
        "body": json.dumps(
            {
                "message": message,
                "event": event,
                # useful to return if we needed to refresh the webhook after getting changes
                "is_webhook_refreshed": needed_invalidation,
            }
        ),
    }


def webhook_invalidation_handler(event, context):
    """
    This is used simply to denote that the metadata in airtable has changed
    and we need to update the webhook with new columns to watch
    We use this workflow so that we do not forcibly replace the webhook
    when there are changes but instead wait for clients to finish streaming
    from existing webhooks.

    The change notification suppies profile, table and base in the post payload

    """
    status = 200
    message = "webhook invalidated successfully."

    try:
        logger.debug(f"received invalidation request {event}")
        connector = AirtableConnector(profile=event["profile"])
        table_id = event["table_id"]
        table = connector[event["base_id"]][table_id]
        logger.incr(f"webhook_invalidations", 1)
        logger.incr(f"{table_id}.webhook_invalidations", 1)
        table.webhook.invalidate()
    except Exception as ex:
        status = 500
        message = repr(ex)
        logger.warn(
            f"The webhook could not be invalidated using payload {event} due to {message}"
        )

    return {
        "statusCode": status,
        "body": json.dumps({"message": message, "event": event}),
    }


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/<inspector_id>/rolls",
    methods=["GET"],
)
def render_rolls(inspector_id):
    try:
        return render_template(
            "rolls.html",
            rolls_info=inspectable_roll_info(inspector_id, cache_timestamp()),
        )
    except Exception as e:
        return repr(e), 400


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/<inspector_id>/roll/<roll_key>",
    methods=["GET"],
)
def render_roll(roll_key, inspector_id):
    try:
        roll_data = get_roll_info(roll_key, inspector_id)
    except Exception as e:
        return repr(e), 400
    return render_template("roll.html", **roll_data)


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/<inspector_id>/nest/<nest_key>",
    methods=["GET"],
)
def render_nest(nest_key, inspector_id):
    try:
        nest_data = get_nest_info(nest_key, inspector_id, cache_timestamp())
    except Exception as e:
        return str(e), 400
    return render_template("nest.html", **nest_data)


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/inspect_piece",
    methods=["POST"],
)
def inspect_piece():
    data = json.loads(request.data)
    fields = [
        "piece_id",
        "nest_key",
        "inspector",
        "defect_idx",
        "airtable_defect_id",
        "defect_name",
        "image_idx",
        "fail",
        "challenge",
    ]
    payload = {f: data[f] for f in fields}
    logger.info(f"Inspecting piece: {payload}")
    res.connectors.load("hasura").execute(
        """
        mutation (
            $challenge: Boolean,
            $airtable_defect_id: String,
            $defect_idx: Int,
            $defect_name: String,
            $fail: Boolean,
            $inspector: Int,
            $nest_key: String,
            $piece_id: String,
            $image_idx: Int
        ) {
            insert_make_roll_inspection_one(
                object: {
                    challenge: $challenge,
                    airtable_defect_id: $airtable_defect_id,
                    defect_idx: $defect_idx,
                    defect_name: $defect_name,
                    fail: $fail,
                    inspector: $inspector,
                    nest_key: $nest_key,
                    piece_id: $piece_id,
                    image_idx: $image_idx
                },
                on_conflict: {
                    constraint: roll_inspection_pkey,
                    update_columns: [fail, challenge, defect_id, defect_name, image_idx]
                }
            ) {
                updated_at
            }
        }
        """,
        payload,
    )
    return "ok"


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/piece_state",
    methods=["POST"],
)
def piece_state():
    data = json.loads(request.data)
    fields = [
        "piece_id",
        "nest_key",
        "inspector",
    ]
    payload = {f: data[f] for f in fields}
    logger.info(f"Getting piece state: {payload}")
    response = res.connectors.load("hasura").execute(
        """
        query ($nest_key: String = "", $piece_id: String = "", $inspector: Int = 10) {
            make_roll_inspection(where: {inspector: {_eq: $inspector}, nest_key: {_eq: $nest_key}, piece_id: {_eq: $piece_id}}) {
                challenge
                defect_name
                defect_idx
                image_idx
                defect_id
                airtable_defect_id
            }
        }
        """,
        payload,
    )
    return json.dumps(response["make_roll_inspection"])


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/delete_piece_state",
    methods=["POST"],
)
def delete_piece_state():
    data = json.loads(request.data)
    fields = ["piece_id", "nest_key", "inspector", "defect_idx"]
    payload = {f: data[f] for f in fields}
    logger.info(f"Deleting piece inspection: {payload}")
    response = res.connectors.load("hasura").execute(
        """
        mutation ($defect_idx: Int = 10, $inspector: Int, $nest_key: String, $piece_id: String) {
            delete_make_roll_inspection(where: {defect_idx: {_eq: $defect_idx}, inspector: {_eq: $inspector}, nest_key: {_eq: $nest_key}, piece_id: {_eq: $piece_id}}) {
                affected_rows
            }
        }
        """,
        payload,
    )
    logger.info(f"Response: {response}")
    return "ok"


@app.route(
    f"/{PROCESS_NAME}/roll_inspection_to_airtable",
    methods=["POST"],
)
def roll_inspection_to_airtable():
    try:
        data = json.loads(request.data)
        Thread(
            target=sync_roll_to_healing_app,
            kwargs={
                "roll_key": data["roll_key"],
                "inspector_id": data["inspector"],
            },
        ).start()
        return "ok"
    except Exception as e:
        logger.error(f"Failed to sync healing to airtable: {repr(e)}")
        from res.flows.make.nest.progressive.utils import ping_rob

        ping_rob(f"Failed to write healing info to airtable for {data}: {repr(e)}")
        return repr(e), 500


@app.route(
    f"/{PROCESS_NAME}/roll_inspection/update_progress",
    methods=["POST"],
)
def roll_inspection_progress():
    try:
        data = json.loads(request.data)
        inspector = data["inspector"]
        roll_id = data["roll_id"]
        roll_key = data["roll_key"]
        state = data["state"]
        logger.info(
            f"Updating roll inspection for inspector {inspector} and roll {roll_id} to state {state}"
        )
        update_roll_inspection_progress(roll_key, roll_id, inspector, state)
        if state == "Finish":
            Thread(
                target=sync_roll_to_healing_app,
                kwargs={
                    "roll_key": roll_key,
                    "inspector_id": inspector,
                },
            ).start()
        return "ok"
    except Exception as e:
        logger.error(f"Failed to start roll in airtable: {repr(e)}")
        from res.flows.make.nest.progressive.utils import ping_rob

        ping_rob(f"Failed to update healing app progress {data}: {repr(e)}")
        return repr(e), 500


@app.route(
    f"/{PROCESS_NAME}/manual_healing",
    methods=["GET"],
)
def render_printfiles():
    try:
        return render_template(
            "printfiles.html", pf_info=healable_printfiles(cache_timestamp())
        )
    except Exception as e:
        return repr(e), 400


@app.route(
    f"/{PROCESS_NAME}/manual_healing/printfile/<printfile_key>",
    methods=["GET"],
)
def render_printfile(printfile_key):
    try:
        pf_data = get_manual_healing_data(printfile_key)
    except Exception as e:
        import traceback

        return traceback.format_exc(), 400  # repr(e), 400
    return render_template("manual_healing.html", **pf_data)


@app.route(
    f"/{PROCESS_NAME}/manual_healing/renest",
    methods=["POST"],
)
def renest_pieces_post():
    try:
        data = json.loads(request.data)
        return json.dumps(
            renest_pieces(data["printfile_key"], ",".join(sorted(data["piece_ids"])))
        )
    except Exception as e:
        logger.error(f"Failed to renest pieces {repr(e)}")
        return repr(e), 400


@app.route(
    f"/{PROCESS_NAME}/manual_healing/composite",
    methods=["POST"],
)
def composite_printfile_post():
    try:
        data = json.loads(request.data)
        return json.dumps(composite_printfile(data["printfile_key"], data["nest_key"]))
    except Exception as e:
        logger.error(f"Failed to renest pieces {repr(e)}")
        return repr(e), 400


@app.route(
    f"/{PROCESS_NAME}/upload_3d_model/<sku>",
    methods=["POST"],
)
@cross_origin()
def upload_3d_model(sku):
    try:
        logger.info(f"Uploading 3d model for {sku}")
        s3 = res.connectors.load("s3")
        body, _, color = sku.lower().split(" ")[:3]
        body = body.replace("-", "_")

        body_root_path = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body}"
        logger.info(f"Looking for latest version in {body_root_path}")

        # use s3 to find the latest body version
        body_paths = list(s3.ls(body_root_path))
        body_paths.sort(
            key=lambda p: int(p.split(body_root_path)[-1].split("/")[1][1:])
        )

        # get the latest version string, folder after body_root_path
        version_str = body_paths[-1].split(body_root_path)[-1].split("/")[1]
        logger.info(f"Latest version is {version_str}")
        color_root_path = (
            f"s3://meta-one-assets-prod/color_on_shape/{body}/{version_str}/{color}"
        )
        model_3d_path = f"{color_root_path}/3d.glb"

        logger.info(f"Uploading to {model_3d_path}")

        file = request.files["file"]

        s3.write(model_3d_path, file)
        logger.info("Done.")

        # need to copy the point cloud from the body too
        point_cloud_path = f"{body_root_path}/{version_str}/extracted/point_cloud.json"
        logger.info(f"Copying point cloud from {point_cloud_path}")
        s3.copy(point_cloud_path, f"{color_root_path}/point_cloud.json")

        return "success"
    except Exception as e:
        logger.error(f"Failed to upload 3d model {repr(e)}")
        return repr(e), 400


@app.route(
    f"/{PROCESS_NAME}/upload_mask_data/<bodyCodeAndVersion>",
    methods=["POST"],
)
@cross_origin()
def upload_mask_data(bodyCodeAndVersion):
    try:
        import base64
        import json
        from PIL import Image
        import io
        import urllib.parse

        logger.info(f"Uploading mask data for {bodyCodeAndVersion}")
        s3 = res.connectors.load("s3")
        parts = bodyCodeAndVersion.lower().split("-")
        if len(parts) != 3:
            raise ValueError("Invalid body code and version")

        body_code = f"{parts[0]}_{parts[1]}"
        version = parts[2]

        body_base = f"s3://meta-one-assets-prod/bodies/3d_body_files"
        body_root_path = f"{body_base}/{body_code}/{version}/extracted/mask_data"

        logger.info(f"Uploading to {body_root_path}")

        def upload_file(file_name, text_data):
            file_path = f"{body_root_path}/{file_name}"
            logger.info(f"    Uploading {file_name} to {file_path}")

            if file_name.endswith(".json"):
                raw_json = urllib.parse.unquote(text_data.split(",")[1])
                s3.write(file_path, json.loads(raw_json))
            elif file_name.endswith(".png"):
                b64 = base64.b64decode(text_data.split(",")[1])
                img = Image.open(io.BytesIO(b64))
                s3.write(file_path, img)
            else:
                logger.warn(f"Unknown file type {file_name}")

            logger.info(f"    Uploaded {file_name}")

        for file_name in request.form.keys():
            logger.info(f"Processing {file_name}")
            text_data = request.form[file_name]

            upload_file(file_name, text_data)

        logger.info("Done.")

        return "success"
    except Exception as e:
        logger.error(f"Failed to upload mask files {repr(e)}")
        return repr(e), 400


@app.route(
    f"/{PROCESS_NAME}/artwork/<search>",
    methods=["GET"],
)
@cross_origin()
def artwork(search):
    hasura = res.connectors.load("hasura")
    s3 = res.connectors.load("s3")

    QUERY_ARTWORK = """
        query latestArtwork($search: String!) {
            meta_artworks(where: {name: {_ilike: $search}}, order_by: {created_at: desc}, limit: 5) {
                id
                dpi_72_uri
                dpi_36_uri
                name
            }
        }
    """

    res.utils.logger.info(search)
    result = hasura.execute_with_kwargs(QUERY_ARTWORK, search=f"%{search}%")

    artworks = []
    for a in result["meta_artworks"]:
        artworks.append(
            {
                "id": a["id"],
                "dpi_36_uri": s3.generate_presigned_url(a["dpi_36_uri"]),
                "dpi_72_uri": s3.generate_presigned_url(a["dpi_72_uri"]),
                "name": a["name"],
            }
        )

    return Response(
        response=json.dumps(artworks),
        status=200,
    )


@app.route(
    f"/{PROCESS_NAME}/3d_models/<model_type>/<body_code>/<body_version>",
    methods=["GET"],
    defaults={"color": ""},
)
@app.route(
    f"/{PROCESS_NAME}/3d_models/<model_type>/<body_code>/<body_version>/<color>",
    methods=["GET"],
)
@cross_origin()
def get_3d_model_uris(model_type, body_code, body_version, color):
    """
    get a presigned url for a 3d model, body or style
    e.g.
        s3://meta-one-assets-prod/bodies/3d_body_files/cc_6001/v11/extracted/3d.glb
    or
        s3://meta-one-assets-prod/color_on_shape/cc_6001/v12/sprimz/3d.glb
    """

    # TODO check bearer token and stuff

    body_code = body_code.lower().replace("-", "_")
    body_version = (
        f"v{body_version}" if not body_version.startswith("v") else body_version
    )
    color = color.lower()

    s3 = res.connectors.load("s3")
    if model_type == "body":
        base = f"s3://meta-one-assets-prod/bodies/3d_body_files/{body_code}/{body_version}/extracted"
    elif model_type == "style":
        base = f"s3://meta-one-assets-prod/color_on_shape/{body_code}/{body_version}/{color}"
    else:
        # bad request
        return Response(
            json.dumps({"error": f"<type> must be 'body' or 'style' not {model_type}"}),
            status=400,
        )

    res.utils.logger.info(f"Base to check for 3d model: {base}")

    model = f"{base}/3d.glb"
    point_cloud = f"{base}/point_cloud.json"
    front_image = f"{base}/front.png"

    # check these exist
    model_exists = s3.exists(model)
    point_cloud_exists = s3.exists(point_cloud)
    front_image_exists = s3.exists(front_image)
    if not model_exists or not point_cloud_exists or not front_image_exists:
        res.utils.logger.info(model)
        res.utils.logger.info(point_cloud)
        res.utils.logger.info(front_image)
        return Response(
            json.dumps(
                {"error": f"3D model/image/point cloud does not exist: {model}"}
            ),
            status=404,
        )

    model_uri = s3.generate_presigned_url(model, for_upload=False)
    point_cloud_uri = s3.generate_presigned_url(point_cloud, for_upload=False)
    front_image_uri = s3.generate_presigned_url(front_image, for_upload=False)

    return Response(
        response=json.dumps(
            {
                "model": model_uri,
                "point_cloud": point_cloud_uri,
                "front_image": front_image_uri,
            }
        ),
        status=200,
    )


@app.route(
    f"/{PROCESS_NAME}/unassign_roll",
    methods=["POST"],
)
def unassign_roll():
    from res.flows.make.nest.clear_nest_roll_assignments import (
        clear_nest_roll_assignments,
    )

    try:
        data = json.loads(request.data)
        roll_name = data["roll_name"]
        logger.info(f"Unassignin roll: {roll_name}")
        total_assets, total_nests, total_pfs = clear_nest_roll_assignments(
            ["all"],
            [roll_name],
            os.environ["RES_ENV"] != "production",
            notes=data.get("notes"),
        )
        return json.dumps(
            {
                "assets": total_assets,
                "nests": total_nests,
                "printfiles": total_pfs,
            }
        )
    except Exception as e:
        logger.error(f"Failed to unassign roll {repr(e)}")
        return repr(e), 400


class RepeatTimer(Timer):
    def run(self):
        self.function(*self.args, **self.kwargs)
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def create_timer(interval, func):
    timer = RepeatTimer(interval, func)
    timer.daemon = True
    timer.start()
    return timer


if __name__ == "__main__":
    res.utils.secrets_client.get_secret("AIRTABLE_API_KEY")
    timers = [
        create_timer(60 * 60, pre_cache_nest_info),
        create_timer(60 * 60, retry_healing_sync),
    ]
    app.run(host="0.0.0.0", port=5000)
    for t in timers:
        t.cancel()
