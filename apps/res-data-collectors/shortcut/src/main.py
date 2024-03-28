from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils import secrets_client, logger
from typing import List, Dict
from datetime import datetime
import time, json, os, arrow
import requests
import pandas as pd
from res.utils import secrets_client
from tenacity import retry, wait_fixed, stop_after_attempt


SHORTCUT_HOST = os.environ.get("SHORTCUT_HOST", "https://api.app.shortcut.com")
SHORTCUT_API_TOKEN = secrets_client.get_secret("SHORTCUT_API_TOKEN")
DRY_RUN = bool(int(os.environ.get("DRY_RUN", 0)))
BACKFILL = bool(int(os.environ.get("BACKFILL", 0)))

SECRET_NAME = "SHORTCUT"
RUN_TIME = int(arrow.utcnow().timestamp())
DAILY_START = arrow.utcnow().shift(days=-2)
START_TIME = "*" if BACKFILL else DAILY_START.format("YYYY-MM-DD")


BASE_QUERY = {"updated": "%s..*" % START_TIME}


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def _fetch(url):
    logger.debug("fetching %s" % url)
    response = requests.get(url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}).json()

    return pd.DataFrame(response)


def _sfetch(url, params={}):
    logger.debug("fetching %s with params: %s" % (url, params))
    response = requests.get(
        url, headers={"Shortcut-Token": SHORTCUT_API_TOKEN}, params=params
    ).json()
    df = pd.DataFrame(response["data"])

    if "next" in response and response["next"]:
        next_url = SHORTCUT_HOST + response["next"]
        logger.debug("recursing, getting %s" % next_url)
        return pd.concat([df, _sfetch(next_url)])
    else:
        return df


def _pker(df):
    """
    adds the required primary key field
    """
    return df.assign(
        # is this right? adding the run time to the primary key?
        timestamp_primary_key=lambda x: x.id.apply(lambda id: "%s_%s" % (id, RUN_TIME)),
        primary_key=lambda x: x.id,
        run_time=RUN_TIME,
    )


def _default_fetch(source):
    """
    this works for many shortcut data types
    stories and a few others are contextual and require a different approach
    """
    logger.info("fetching %s data" % source)
    return peek(_pker(_fetch(SHORTCUT_HOST + "/api/v3/%s" % source)))


def _make_query_string(query):
    """
    maybe there'll be a need to get clever here in the future
    now it's just a conjunction or even a single term
    """
    return "AND".join(["%s:%s" % (k, v) for k, v in query.items()])


def _make_params(query):
    qs = _make_query_string(query)
    return {"query": qs, "page_size": 25}


def search_fetch(source):
    """
    it seems that stories requrire the search api
    """
    logger.info("searching for %s data" % source)

    params = _make_params(BASE_QUERY)
    results = _sfetch(SHORTCUT_HOST + "/api/v3/search/%s" % source, params=params)
    return peek(_pker(results))


def peek(df):
    logger.info("\t".join(df.columns.values))
    return df


def insert_to_snowflake(df, source, sf, dry_run, pk="primary_key"):
    logger.info("Loading to snowflake...")
    if not dry_run:
        logger.info("sending %d records to snowflake")
        sf.load_json_data(df.to_dict("records"), "shortcut_%s" % source, "primary_key")
        logger.incr("rows_sent_to_snowflake", len(df))
        logger.info("All Rows (%d) of %s Sent to Snowflake" % (len(df), source))
    else:
        logger.info("[dry run mode, not loading (%d records)]" % len(df))
    return df


def default_etl(source, sf, dry_run, fetcher=_default_fetch):
    df = fetcher(source)
    return insert_to_snowflake(df, source, sf, dry_run)


if __name__ == "__main__":
    logger.info("Starting to gather shortcut data job...")
    start_time = time.time()

    logger.info("proceeding in %s mode" % "dry run" if DRY_RUN else "production")

    snow_client = ResSnowflakeClient()

    sources = [
        "milestones",
        "epics",
        "projects",
        "categories",
        "groups",
        "iterations",
        "labels",
        "members",
    ]

    logger.info("starting with basic data sources")

    type_data = {
        source: default_etl(source, snow_client, DRY_RUN) for source in sources
    }

    logger.info("fetching stories")
    stories = insert_to_snowflake(
        search_fetch("stories"), "stories", snow_client, DRY_RUN
    )
    logger.info("done, got %d" % len(stories))

    logger.timing("total_time_elapsed_ms", int((time.time() - start_time) * 1000))
