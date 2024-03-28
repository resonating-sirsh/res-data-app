from datetime import datetime
from importlib import import_module
import yaml

from .. import _utils, logging


def python_type_from_avro_type(f, datetime_fields=["created_at", "updated_at"]):
    """
    A somewhat opinionated way to map to python types from types or conventions
    daetfields are normally iso strings in our convention
    json is usually added as a string
    """
    ftype = f["type"]
    if f["name"] in datetime_fields:
        return datetime

    if ftype == "array":
        return list

    # all maps are taken as strings if primitives
    if ftype == "map":
        return str

    if not isinstance(ftype, list):
        ftype = [ftype]

    if "string" in ftype:
        return str
    elif "float" in ftype:
        return float
    elif "int" in ftype:
        return int
    elif "boolean" in ftype:
        return bool


def transfer_res_schema_to_s3(path):
    """

    supply the local path under res schemas - this needs to be updated - makes some
    assumptions - use for dev only

    Read the res-schema/res data and move to s3://res-data-platform/schema/ replacing
    any / . etc. with -
    this is a temp utility until we move to a proper schema management system
    this allows server code to read schema to map airtable tables to our interface in a
    low-tech way

    example path "make/nest_assets"


    """
    s3 = import_module("res.connectors").load("s3")
    s = _utils.read(f"{_utils.get_res_root()}/res-schemas/res/{path}.yaml")

    return s3.write(
        f"s3://res-data-platform/schema/{path.replace('/','-').replace('.','.')}.yaml",
        yaml.safe_dump(s),
    )


def register_meta_type_from_schema_file(file, update_webhook=False):
    """
    For now this is just a utility to capture the workflow
    - upsert the dgraph meta type which then allows auto-mutations for writing these
    types
    - create a webhook where relevant
    This workflow could be added to a git action when mature

    Reminder: updating a tye in dgraph does two things
    1. it tells dgraph the schema for the dgraph functionality like expand types etc.
    2. it tells res-meta the schema for our conventions e.g. auto mutations... it also
       happens to store that res-meta schema in dgraph too
    """
    DgraphConnector = import_module(
        "res.connectors.dgraph.DgraphConnector"
    ).DgraphConnector
    AirtableWebhookSpec = import_module("res.connectors.airtable").AirtableWebhookSpec

    logging.logger.info(f"_utils.reading {file}")
    res_schema = _utils.read(file)
    key = res_schema.get("key")

    logging.logger.info(f"registering {key} to dgraph")
    DgraphConnector.update_type_from_metatype(key, res_schema)

    if update_webhook and res_schema.get("airtable_table_id"):
        logging.logger.info(f"registering webhook for type{key}")
        spec = AirtableWebhookSpec.spec_from_res_schema(res_schema)
        spec.save_config()
        spec.sync()

    logging.logger.info(f"done - dgraph type and webhook update for {key}")
