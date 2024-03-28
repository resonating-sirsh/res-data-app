import res
import json
import pandas as pd


def _resubmit_requests(data):
    assets = list(data["payloads"])

    # os.environ['KAFKA_KGATEWAY_URL'] = "https://data.resmagic.io/kgateway/submitevent"
    TOPIC = "res_meta.meta_one.requests"
    kafka = res.connectors.load("kafka")
    for a in assets:
        kafka[TOPIC].publish(a, use_kgateway=True, coerce=True)


def requests_missing_responses(since_date_str):
    """
    since date string e.g. ß"2022-10-10"
    get any requests missing responses and needing replay
    """
    snowflake = res.connectors.load("snowflake")
    req = snowflake.execute(
        f""" SELECT * FROM "IAMCURIOUS_PRODUCTION".META_ONE_REQUESTS_QUEUE  """
    )
    resp = snowflake.execute(
        f""" SELECT * FROM "IAMCURIOUS_PRODUCTION".META_ONE_RESPONSES_QUEUE  """
    )
    stuff = pd.merge(req, resp, on="id", how="left", suffixes=["_req", "_res"])
    chk = stuff[stuff["RECORD_CONTENT_res"].isnull()].sort_values("created_at_req")
    chk = chk[chk["created_at_req"] > since_date_str]
    chk = chk[chk["size_code_req"].map(lambda m: "P" not in m)]
    chk["payloads"] = chk["RECORD_CONTENT_req"].map(json.loads)
    chk = chk.drop_duplicates(subset=["id", "size_code_req"])

    return chk


def get_metaones_color_queue_id(
    apply_color_request_key, schema="IAMCURIOUS_DEVELOPMENT"
):
    """
    query the resonse queue data in the kafka topic that is in the snowflake sink for the given environment
    this is useful to confirm that we did get a response for the request

    """
    snowflake = res.connectors.load("snowflake")

    data = snowflake.execute(
        f"""SELECT * FROM {schema}.META_ONE_RESPONSES_QUEUE WHERE "id" = '{apply_color_request_key}';"""
    )
    data["RECORD_CONTENT"] = data["RECORD_CONTENT"].map(json.loads)
    return data


def get_metaones_color_queue_requests_by_id(
    apply_color_request_key, resubmit=False, schema="IAMCURIOUS_DEVELOPMENT"
):
    """
    query the request queue data in the kafka topic that is in the snowflake sink for the given environment
    this is useful confirm a request was made and re-submit it if necessary

    """
    snowflake = res.connectors.load("snowflake")

    data = snowflake.execute(
        f"""SELECT * FROM {schema}.META_ONE_REQUESTS_QUEUE WHERE "id" = '{apply_color_request_key}';"""
    )
    data["RECORD_CONTENT"] = data["RECORD_CONTENT"].map(json.loads)

    if resubmit:
        kafka = res.connectors.load("kafka")
        kafka = kafka["res_meta.meta_one.requests"].publish(
            list(data["RECORD_CONTENT"]), use_kgateway=True
        )

    return data


def get_last_request_response_by_color_queue_id(
    apply_color_request_key, size=None, schema="IAMCURIOUS_DEVELOPMENT"
):
    """
    Get a sample json response object for the request and the response together - it would be useful to select a specific size
    """
    snowflake = res.connectors.load("snowflake")
    data = snowflake.execute(
        f"""SELECT * FROM {schema}.META_ONE_REQUESTS_QUEUE WHERE "id" = '{apply_color_request_key}';"""
    )
    req = json.loads(data.iloc[0]["RECORD_CONTENT"])
    data = snowflake.execute(
        f"""SELECT * FROM {schema}.META_ONE_RESPONSES_QUEUE WHERE "id" = '{apply_color_request_key}';"""
    )
    res = json.loads(data.iloc[0]["RECORD_CONTENT"]) if len(data) else None
    return req, res
