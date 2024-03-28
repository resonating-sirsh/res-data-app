import os

if "HASURA_ENDPOINT" in os.environ:
    os.environ.pop("HASURA_ENDPOINT")
os.environ["RES_ENV"] = "production"
os.environ["HASURA_ENDPOINT"] = "https://hasura.resmagic.io"

import res
import json
import arrow
from res.utils import logger, secrets
from functools import lru_cache
from pyairtable import Table
from tenacity import retry, wait_fixed, stop_after_attempt
from res.flows.meta.ONE.meta_one import MetaOne


@lru_cache(maxsize=1)
def cnx_table(base_id, table_id):
    return Table(
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY"),
        base_id,
        table_id,
    )


@retry(wait=wait_fixed(3), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    logger.info(f"Airtable request args: {args}, kwargs: {kwargs}")
    if method == "find":
        # Use the "get" method instead of "find" to fetch a single record by ID
        response = cnx.get(*args, **kwargs)
    else:
        response = getattr(cnx, method)(*args, **kwargs)
    logger.info(f"Airtable response: {json.dumps(response)}")
    return response


def handle_new_event(event):
    logger.info(f"event -> {event}.")
    logger.info("(1/2) Creating cut request")

    make_one_production = call_airtable(
        cnx_table("appH5S4hIuz99tAjm", "tblptyuWfGEUJWwKk"),
        event["make_id"],
        method="get",
    )

    m1 = MetaOne(event["sku"])
    one_created = m1.create_cut_requests(
        one_number=event["one_number"],
        brand_name=make_one_production["fields"]["__brandcode"],
        factory_request_name=make_one_production["fields"]["Request Name"],
    )

    if one_created:
        call_airtable(
            cnx_table("appH5S4hIuz99tAjm", "tblptyuWfGEUJWwKk"),
            event["make_id"],
            {"__cut_app_request_id": one_created},
            typecast=True,
            method="update",
        )

    logger.info(f"request processed: {event}")
    return "Done"


if __name__ == "__main__":
    data = {
        "id": None,
        "sku": "KT2055 CTW70 SUNRYQ 4ZZLG",
        "one_number": "10214157",
        "make_id": "recaOx1z6Sjjpiiqh",
    }

    handle_new_event(data)
