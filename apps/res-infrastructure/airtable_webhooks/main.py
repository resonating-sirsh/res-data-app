from res.flows.FlowContext import FlowContext
import res
import json
import pandas as pd
from flask import Flask, request, Response
from res.connectors.airtable import (
    AirtableWebhookSession,
    AirtableWebhookSpec,
    AirtableConnector,
)

from res.utils.flask import helpers as flask_helpers
from res.utils.logging import logger
from functools import partial
from flask import jsonify, make_response
from res.utils import ex_repr

app = Flask(__name__)
flask_helpers.attach_metrics_exporter(app)

PROCESS_NAME = "airtable-webhooks"
SINK_TOPIC_NAME = "res_infrastructure.airtable_to_kafka.airtable_changes"
CDC_TOPIC_NAME = "res_infrastructure.airtable_to_kafka.airtable_changes_cdc"
TOPIC_FIELD_MAP = {
    "table_id": "table_id",
    "field_id": "field_id",
    "cell_value": "value",
    "record_id": "record_id",
    "base_id": "base_id",
    "timestamp": "timestamp",
    "key": "table_key",
    "column_name": "field_name",
}

USE_KGATEWAY = True

# for now just track what we receive as need to think where other dataflows should be logged
# this is good just for showing activity on the webhook for now
metric_writer = partial(
    FlowContext.flownode_dataflow_metric_incr, "etl", PROCESS_NAME, "cell_updates"
)


@app.route("/healthcheck", methods=["GET"])
def healthcheck():
    return Response("ok", status=200)


def filter_cdc(data, res_schema_map, status_filter=["todo"]):
    """
    Look at cell value change
    if there are rows that match status field and the value has changed to any of the status filters then release
    if the rows have changed any of the subnode fields then release
    if the tags have changed we also update the status
    return the primarykey, record_id, event timestamp and delta
    """
    # trying to trigger build from app

    status_field = res_schema_map.get("status", "status")
    subnode_field = res_schema_map.get("subnode", "subnode")
    tags_field = res_schema_map.get("tags", "tags")

    if len(data) == 0:
        return data

    ret_cols = ["record_id", "primary_key", "event_timestamp", "event_elapsed_seconds"]

    def safe_match(x, v):
        if pd.isnull(x):
            return False
        return x.lower().replace(" ", "") == v

    # we could replace this with a cleaned value isin(set)
    c1 = (data["field_name"] == status_field) & (
        data["value"].map(lambda x: safe_match(x, "todo"))
    )
    c2 = data["field_name"] == subnode_field
    c3 = data["field_name"] == tags_field
    data["event_elapsed_seconds"] = res.utils.dataframes.diff_seconds(data)
    data["event_timestamp"] = data["timestamp"]

    filtered = data[c1 | c2 | c3].drop_duplicates(subset=["record_id"])

    res.utils.logger.debug(
        f"Filtering cdc rows of length {len(data)} for changes on status field {status_field} or subnode field {subnode_field}"
    )

    res.utils.logger.debug(f"have cdc filter of length {len(filtered)}")

    return filtered[ret_cols]


def publish_changes_via_res_schema(base_id, table_id, changes):
    """
    We relay actual cell changes to two places;
    1. We update status rows based on our schema. This can be anything; rolls, styles, orders
    2. We create a special CDC change records that is sent to a second kafka topic with tracked changes on airtable cells

    (Note) something will process CDC events for hour activity and we should do two things
    1. Unpack hour segments into their own rows instead of as a ist in this CDC schema
    2. Always update current partition for all record id in the same schema
      - druid can load this incremetally to compute all changes to date and activity to now()
      - this is nice to have if we are prepared to have segments as of the ast clock hour
    """
    s3 = res.connectors.load("s3")
    spec = AirtableWebhookSpec.load_config(base_id, table_id)
    # i there is a table name it is "typed"
    res_meta_schema_key = spec.get("res_type_name")

    # this app has its own config where we can store those things as part of registration and the provider(dgraph) can load by key e.g. test.rolls
    # res_schema = AirtableWebhookSpec.try_get_res_schema_for_airtable(base_id, table_id)

    if res_meta_schema_key:
        res.utils.logger.info(f"RES SCHEMA KEY: {res_meta_schema_key}")
        # the schema is just a list of fields with some conventional attributes
        # but we only need a subset of it here e.g. field names, key,
        res_schema = AirtableWebhookSpec.config_as_schema(spec)
        kafka = res.connectors.load("kafka")
        # res_meta = res.connectors.load("dgraph")  # dgraph is the res-meta provider
        logger.debug(
            f"Found res-schema {res_meta_schema_key} for airtable {table_id}. Determining cell changes..."
        )

        status_rows, cdc = AirtableConnector.cdc(changes, res_schema)
        if cdc is not None:
            logger.debug(
                f"Writing {len(status_rows)} status rows and {len(cdc)} cdc records to res-meta for {table_id} ({res_meta_schema_key})"
            )

            if table_id == "tblWMyfKohTHxDj3w":
                cdc.to_csv(
                    f"s3://res-data-platform/samples/cdc/{table_id}.csv", index=None
                )
                status_rows.to_csv(
                    f"s3://res-data-platform/samples/status_rows/{table_id}.csv",
                    index=None,
                )

            logger.incr_data_flow(
                "airtable_changed_rows",
                res_meta_schema_key,
                "received",
                len(status_rows),
            )

            ###### some conventions about our schema mapping and how we relay to kafka
            # changes to any of these things causes a relay
            field_mapping = {"record_id": "id"}
            cdc_filtered = filter_cdc(
                cdc, res_schema_map=spec.get("res_schema_mapping", {})
            )
            #######

            if len(cdc_filtered):
                """
                We are adding a pattern for pushing TODOs to the kafka topic
                the names in the airtable here matches the kafka topic for testing - but we could resolve anyhow
                also any node changes are mapped
                """
                # somehow check the records in cdc where status has changed to dodo

                primary_key_field = [
                    f["key"] for f in res_schema["fields"] if f.get("is_key")
                ][0]

                # status rows can be filtered if they trigger a relevant cdc
                # cdcs also have event timestamp data and ids will take the record id if the schema says so
                # this will be sent to a coerced kafka topic
                # we name the record id which is always on the airtable data to be our conventional id if we are writing to kafka
                events = pd.merge(
                    status_rows,
                    cdc_filtered,
                    left_on=primary_key_field,
                    right_on="primary_key",
                    suffixes=["", "_cdc"],
                ).rename(columns=field_mapping)

                if len(events):
                    res.utils.logger.info(
                        f"publishing events like: {dict(events.iloc[0])}"
                    )
                    kafka[res_meta_schema_key].publish(
                        events, use_kgateway=USE_KGATEWAY, coerce=True
                    )

            # TODO written and errors metric_writer(status='ok', len(write))
            kafka[CDC_TOPIC_NAME].publish(
                cdc, use_kgateway=USE_KGATEWAY, coerce=True, publish_to_replica=False
            )

            logger.debug(
                f"happily sent items to make db for table {table_id} - {res_meta_schema_key}"
            )


@app.route(
    f"/{PROCESS_NAME}/<profile>/<base>/<table>",
    methods=["GET", "POST"],
)
def airtable_notify_cell_change(profile, base, table):
    """
    Receive changes from airtable

    {
        "base": {"id": "appXXX"},
        "webhook": {"id": "achYYY"},
        "timestamp": "2020-03-11T21:25:05.663Z"
    }

    - Profile will be 1:1 with env e.g. development, production, staging
    - when we receive a ping for a base/table, we use the connector to resolve the webhook id

    """
    if request.method != "POST":
        return Response(
            "{'error':'Requests must be POST'}", status=405, mimetype="application/json"
        )
    try:
        logger.incr(f"{table}.webhook_invalidated", 1)
        body = json.loads(request.data, strict=False)
    except ValueError:
        return Response(
            "{'error':'Invalid JSON'}", status=400, mimetype="application/json"
        )

    req = {"profile": profile, "base_id": base, "table_id": table}

    # flask metrics here, session

    # res.utils.safe_http.relay_to_res_replica(request, async_request=True)
    # thought about this ^ which would be elegant but it means a second call back to airtable for changes so now
    # what the above would do is just make the production flask app request the changes on the same webhook

    logger.debug(f"received request with path {req} and body {body}")
    cell_count = 0
    try:
        wid = body["webhook"]["id"]
        kafka = res.connectors.load("kafka")
        # get the connector for this table and get changes on the webhook id (for this profile/env)

        if base == "apprcULXTWu33KFsh":
            return make_response(
                jsonify(
                    {
                        "message": f"ok - skipping base",
                        "event": body,
                    }
                ),
                200,
            )

        with AirtableWebhookSession(
            base_id=base, table_id=table, webhook_id=wid, annotate_key=True
        ) as session:

            # we have the option to stream to kafka record at a time if we want to using the iterator on session
            # changes is a dataframe the reads the entire change set from the objects iterator
            changes = session.changes
            cell_count = len(changes)
            if cell_count > 0:
                # metric_writer(table, "received", len(changes))
                logger.debug(
                    f"Sending {len(changes)} records to kafka topic {SINK_TOPIC_NAME}"
                )

                # flask metrics here, all changes

                kafka[SINK_TOPIC_NAME].publish(
                    changes.rename(columns=TOPIC_FIELD_MAP),
                    use_kgateway=USE_KGATEWAY,
                    coerce=True,
                    publish_to_replica=False,
                )
                logger.info(
                    "Published airtable cell change records to Kafka - see Kafka metrics for details"
                )

                # we also publish change data where relevant and update status tables
                publish_changes_via_res_schema(
                    base_id=base, table_id=table, changes=changes
                )

    except Exception as ex:
        # metrics TODO:
        logger.warn(
            f"Something went wrong when running the webhook stream processing for event {body} - error info:  {ex_repr(ex)}"
        )

        # flask metrics here - failed session

        # for testing pass a flag
        if request.args.get("suppress_errors", True) == "false":
            return make_response(
                jsonify(
                    {
                        "exception": repr(ex),
                        "trace": None,
                        "event": body,
                    }
                ),
                500,
            )

    # always return a 200 (in 20 seconds) or airtable kills our webhook
    # there is a cron job that "renews" webhooks - it can be update to sync their schema too
    return make_response(
        jsonify(
            {
                "message": f"ok - pulled {cell_count} cell changes",
                "suppressing": request.args.get("suppress_errors", True),
                "event": body,
            }
        ),
        200,
    )


# reminder to come back to this and the dataflow metrics
# @app.after_request
# def log_response(response):
#     options = {}
#     use: metric_set_total_requests() inside logger but resolve app and namespace which is known to the logger, request.endpoint should be passed / pass all the request
#     logger.on_flask_response(response, request, **options)

#     return response


if __name__ == "__main__":
    app.run(host="0.0.0.0")
