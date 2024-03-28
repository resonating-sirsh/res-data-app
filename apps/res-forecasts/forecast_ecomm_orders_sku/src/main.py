import json, os, datetime
from functools import lru_cache
from hashlib import blake2b

from pyairtable import Table
from tenacity import retry, wait_fixed, stop_after_attempt

from res.connectors.snowflake.ResSnowflakeClient import ResSnowflakeClient
from res.utils import logger, secrets


def initialize_secrets():
    if "AIRTABLE_API_KEY" not in os.environ:
        secrets.secrets_client.get_secret("AIRTABLE_API_KEY")


@lru_cache(maxsize=1)
def cnx_recommended_orders():
    return Table(
        os.environ.get("AIRTABLE_API_KEY"),
        os.environ.get("PRINT_BASE_ID"),
        os.environ.get("RECOMMENDED_ORDERS_TABLE"),
    )


@retry(wait=wait_fixed(35), stop=stop_after_attempt(3))
def call_airtable(cnx, *args, method="all", **kwargs):
    try:
        response = getattr(cnx, method)(*args, **kwargs)
    except Exception as e:
        logger.debug(f"Request to {cnx!r} failed with {e!r}, will be tried 3 times")
        logger.debug(json.dumps({"args": args, "method": method, **kwargs}))
        raise e
    else:
        logger.debug(f"Request to {cnx!r} succeeded!")
        logger.debug(json.dumps({"args": args, "method": method, **kwargs}))
        logger.debug(f"Response from {cnx!r}:")
        logger.debug(json.dumps(response))
        return response


def query_recommended_orders():
    try:
        snowflake_connector = ResSnowflakeClient()
        with open("src/query.sql") as query_file:
            results = snowflake_connector.execute(query_file.read(), return_type="list")
            return results
    except Exception as e:
        logger.error("Error running query for recommended orders: {}".format(str(e)))


def create_recommended_order_payload(row, metadata):
    return {
        "Argo Job Key": metadata["argo_job_key"],
        "Material Code": row[0],
        "Brand Code": row[1],
        "Brand Name": row[2],
        "SKU": row[3],
        "Nested Length (Yards)": row[4],
        "Category": "Forecasted",
        "Quantity": row[5],
    }


def generate_job_metadata():
    forecast_created_at = datetime.datetime.utcnow().strftime(
        "%Y-%m-%dT%H:%M:%S.%f+00:00"
    )
    job_name = "forecast-ecomm-orders-sku"
    job_hash = blake2b(
        (job_name + forecast_created_at).encode("utf-8"), digest_size=10
    ).hexdigest()
    return {
        "argo_job_key": f"forecast-ecomm-orders-sku-{job_hash}",
        "forecast_created_at": forecast_created_at,
    }


def run_recommender():
    initialize_secrets()

    job_metadata = generate_job_metadata()
    recommended_orders_payload = list(
        map(
            lambda x: create_recommended_order_payload(x, job_metadata),
            query_recommended_orders(),
        )
    )

    logger.debug(
        f"Number of SKUs with recommended orders: {len(recommended_orders_payload)}"
    )
    # logger.debug(json.dumps(recommended_orders_payload))

    if os.environ.get("WRITE_TO_AIRTABLE", "f").lower()[0] == "t":
        call_airtable(
            cnx_recommended_orders(),
            recommended_orders_payload,
            typecast=True,
            method="batch_create",
        )
    else:
        logger.info("Skipped writing to Airtable")


if __name__ == "__main__":
    run_recommender()
