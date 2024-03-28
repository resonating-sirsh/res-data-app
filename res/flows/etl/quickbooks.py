from botocore.config import Config
from res.connectors.quickbooks import QuickbooksConnector, ENDPOINTS, ACC_IDS, REPORTS
from res.flows import FlowContext
import os
from res.utils import logger
import pandas as pd

CONFIG_PATH = "s3://res-data-platform/flows/etl/quickbooks/.meta/config.yaml"


def get_config(fc):
    s3 = fc.connectors["s3"]
    return {} if not s3.exists(CONFIG_PATH) else s3.read(CONFIG_PATH)


def ingest_endpoints(event, context):
    """
    Use the quickbooks connector to load data from all endpoints (not reports)
    and ingest to targets

    Useage

    from res.flows.etl import quickbooks
    #optionally specify a subset or leave blank to ingest all endpoints
    #note REPORTS are not ingested in this flow and should be done separately
    quickbooks.ingest_endpoints({"args":{"endpoints_to_ingest": ['Bill']}},{})

    """
    args = event.get("args", {})
    endpoints_to_ingest = args.get("endpoints_to_ingest", ENDPOINTS)

    with FlowContext(event, context) as fc:
        qb = QuickbooksConnector()
        s3 = fc.connectors["s3"]
        config = get_config(fc)
        for account_name, _ in ACC_IDS.items():
            logger.infp(f"Processing endpoints for account {account_name}")
            for endpoint in endpoints_to_ingest:
                # look for possible configuration per endpoint
                endpoint_config = config.get("endpoints", {}).get(endpoint, {})
                pos = endpoint_config.get("watermark", 0)
                key = endpoint_config.get("key")
                timestamp_column = endpoint_config.get(
                    "timestamp", "__created_timestamp"
                )
                alias = endpoint_config.get("alias", endpoint)
                logger.info(
                    f"Ingesting endpoint {endpoint} alias {alias} from pos={pos} and partitioning on timestamp column={timestamp_column}"
                )

                # load the endpoint data
                account_connector = qb[account_name]
                data = account_connector[endpoint].get_records(start_position=pos)

                logger.info(
                    f"queried {len(data)} records from endpoint. Writing s3 daily partitions..."
                )

                # quickbooks has line items in the data which we can extract out
                if "Line" in data.columns:
                    logger.info(f"Exploding line items for entity {endpoint}")
                    line_items = QuickbooksConnector.explode_line_items(
                        data, entity=alias
                    )
                    logger.info(
                        f"Writing child Line items of {endpoint} to s3 partitions"
                    )
                    # annotate line items with timestamp and the record key
                    s3.write_date_partition(
                        line_items,
                        group_partition_key=None,
                        date_partition_key=timestamp_column,
                        entity=f"{alias}Line",
                    )

                logger.info(f"Writing data to entity {endpoint} s3 partitions")
                s3.write_date_partition(
                    data,
                    group_partition_key=None,
                    date_partition_key=timestamp_column,
                    entity=alias,
                )

                if args.get("ingest_to_kafka"):
                    kafka = fc.connectors["kafka"]
                    logger.info(f"publishing data to kafka topic {alias}")
                    kafka[alias].publish(data, dedup_on_key=key)

                if args.get("ingest_to_snowflake"):
                    snowflake = fc.connectors["snowflake"]
                    # the connector could load this but i want to be explicit about it
                    schema = os.getenv("SNOWFLAKE_SCHEMA", "IAMCURIOUS_DEVELOPMENT")
                    # create the stage from the entity in s3
                    logger.info(f"ingesting data into snowflake schema {schema}")
                    snowflake.ingest_entity_stage(entity=alias, schema=schema)

                # fc.on_processed_records(data, key=endpoint)


# we could also move data to other places
# if args.get("ingest_to_druid"):
#     druid = fc.connectors["snowflake"]
#     # create the datasource from the entity in s3

# if args.get("ingest_to_athena"):
#     athena = fc.connectors["athena"]
#     # add the index from the entity in s3
