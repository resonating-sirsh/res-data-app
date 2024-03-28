import os
from .. import DatabaseConnector, DatabaseConnectorTable, DatabaseConnectorSchema
import pandas as pd
from . import PinotPayloadTemplates as templates
from res.utils.safe_http import request_get, request_post, request_put
from res.utils import logger
import res
import json


PINOT_API = os.environ.get("PINOT_URL", "http://localhost:9000")
PINOT_HEADERS = {
    "Content-type": "application/json",
    "Accept": "application/json",
}


def generic_validator(res):
    if res.status_code != 200:
        raise Exception("General API exception TODO handle cases")
    return True


def validate_schema_update_response(res):
    return generic_validator(res)


def validate_realtime_table__update_response(res):
    return generic_validator(res)


def validate_schema_get_response(res):
    return generic_validator(res)


class PinotConnector(DatabaseConnector):
    def __init__(self):
        pass

    def __getitem__(self, catalog):
        return PinotConnectorSchema(catalog)

    def update_queue(self, payload, **kwargs):
        """
        Adding the node queue for flow api means creating types that take data consistent with types in other systems
        """

        if isinstance(payload, str):
            payload = json.loads(payload)

        name = payload.get("schemaName")

        res.utils.logger.info(f"Creating queue for {name}")

        p = PinotConnectorTable("meta", name)

        p.update_schema(payload)
        p.update_table()

        return True


class PinotConnectorSchema(DatabaseConnectorSchema):
    def __init__(self, schema):
        self._schema = schema

    def __getitem__(self, table):
        return PinotConnectorTable(self._schema, table)


class PinotConnectorTable(DatabaseConnectorTable):
    def __init__(self, schema, table):
        """
        Constructor sets up the session for a particular account and endpoint
        """
        self._table = table
        self._schema = schema

    def get_client(self):
        """
        Get the quickbooks client object
        """
        pass

    def update_schema(self, payload):
        if isinstance(payload, str):
            payload = json.loads(payload)

        queue_name = payload["schemaName"]

        response = request_get(f"{PINOT_API}/schemas/", headers=PINOT_HEADERS)
        existing = response.content.decode() if response.status_code == 200 else []

        if queue_name not in existing:
            logger.info(f"The schema {queue_name} does not exist - adding it")
            response = request_post(
                f"{PINOT_API}/schemas", headers=PINOT_HEADERS, json=payload
            )
        else:
            logger.info(f"The schema {queue_name} exists - updating it")
            response = request_put(
                f"{PINOT_API}/schemas/{queue_name}", headers=PINOT_HEADERS, json=payload
            )

        # do some validation

        return response

    def update_table(self, **kwargs):
        """
        There are some options we can configure but creating table should really
        just point to a schema that has all the typing
        """
        payload = templates.realtime_table_create(self._table, **kwargs)
        response = request_post(
            f"{PINOT_API}/tables", headers=PINOT_HEADERS, json=payload
        )
        # do some validation
        return response

    def execute(self, query):
        conn = self.get_client()
        cur = conn.cursor()
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=[t[0] for t in cur.description])
