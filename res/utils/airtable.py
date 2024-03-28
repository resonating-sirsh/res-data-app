import os
import time
import pandas as pd
from tenacity import retry, wait_fixed, stop_after_attempt, wait_random, wait_chain
from functools import lru_cache as cache
from pyairtable import Table as PyAirtableTable
from tenacity import retry, wait_fixed, stop_after_attempt
from res.utils import logger

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", 7))
SLEEP_SECS = 0.5


def retry_callback(retry_state):
    logger.warn("reached last retry %s" % retry_state)
    return True


@retry(wait=wait_fixed(3) + wait_random(1, 3), stop=stop_after_attempt(2 * MAX_RETRIES))
def update_airtable_record(record_id, payload, connection):
    connection.update(record_id, fields=payload, typecast=True)
    time.sleep(SLEEP_SECS)


@cache
@retry(
    wait=wait_fixed(3) + wait_random(1, 3),
    stop=stop_after_attempt(2 * MAX_RETRIES),
    retry_error_callback=retry_callback,
)
def delete_airtable_record(record_id, connection, record_type=None):
    """
    we use caching because the code may request to delete a single nest
    repeatedly; airtable will error otherwise.
    """
    connection.delete(record_id)
    logger.info(f"deleted {record_type or 'airtable record'}: {record_id}")
    time.sleep(SLEEP_SECS)


def format_filter_conditions(conditions):
    return f"AND({','.join(filter(lambda x: x is not None, conditions))})"


def multi_option_filter(
    column_name, options, is_list_column=False, is_exclude_filter=False
):
    if "all" in map(lambda x: x.lower(), options):
        return []
    elif is_exclude_filter:
        if len(options) > 0:
            filter_format = (
                "FIND('{1}', {{{0}}})=0" if is_list_column else "{{{0}}}!='{1}'"
            )
            return [
                "AND(%s)"
                % ",".join(map(lambda x: filter_format.format(column_name, x), options))
            ]
        else:
            return []
    elif len(options) > 0:
        filter_format = "FIND('{1}', {{{0}}})>0" if is_list_column else "{{{0}}}='{1}'"
        return [
            "OR(%s)"
            % ",".join(map(lambda x: filter_format.format(column_name, x), options))
        ]
    else:
        return ["{%s}='some-nonexistent-value-to-filter-out-all-records'" % column_name]


def get_table(spec):
    app, table = spec.split("/")
    return get_table(app, table)


def get_table(app, table, secret_name="AIRTABLE_PAT_TOKEN"):
    from res.utils.secrets import secrets_client

    return RetryingTable(
        lambda: secrets_client.get_secret(secret_name, force=False),
        app,
        table,
    )


class RetryingTable:
    def __init__(self, key_provider, app_id, table_id):
        self.key_provider = key_provider
        self.app_id = app_id
        self.table_id = table_id
        self.table = None

    def __getattr__(self, name):
        if self.table is None:
            self.table = PyAirtableTable(
                self.key_provider(), self.app_id, self.table_id
            )
            # something is fucked up with python keep-alive connection handling over long periods of time.
            self.table.session.headers.update({"Connection": "close"})
        member = self.table.__getattribute__(name)
        if hasattr(member, "__call__"):
            return retry(
                wait=wait_chain(
                    *[wait_fixed(3) + wait_random(0, 2) for i in range(1)]
                    + [wait_fixed(7) + wait_random(0, 2) for i in range(1)]
                    + [wait_fixed(9) + wait_random(0, 2)]
                ),
                stop=stop_after_attempt(3),
            )(member)
        return member

    def df(self, formula=None, field_map={}, include_id=True):
        return pd.DataFrame(
            [
                {
                    **{v: r["fields"].get(k) for k, v in field_map.items()},
                    **({"id": r["id"]} if include_id else {}),
                }
                for r in self.all(fields=field_map.keys(), formula=formula)
            ]
        )


def spam_event_trigger(event_trigger_id):
    """
    USE AT YOUR OWN RISK
    """
    import boto3
    import json

    trigger = get_table("appc7VJXjoGsSOjmw", "tblVuZdshZhW0790A").get(event_trigger_id)

    triggers_table = get_table(
        trigger["fields"]["Base ID"], trigger["fields"]["Airtable Table ID"]
    )

    triggers_queue = triggers_table.all(
        formula=trigger["fields"]["Airtable Formula"],
    )

    logger.info(
        f"Queue for trigger {trigger['fields']['KEY']} is size {len(triggers_queue)}"
    )
    client = boto3.client("lambda", region_name="us-east-1")

    for record in triggers_queue:
        client.invoke(
            FunctionName=trigger["fields"]["Function Name"],
            InvocationType="Event",  # trigger["fields"]["Lambda Invocation Type"],
            Payload=json.dumps({"record_id": record["id"], "dry_run": False}),
        )
