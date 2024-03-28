import datetime
from res.utils.dataframes import coerce_to_avro_schema
from types import SimpleNamespace
from pandas.core.indexes import base
from . import FlowContext
from res.utils import logger, dates
from res.connectors.dynamo import AirtableDynamoTable
import pandas as pd
from res.connectors.airtable import readers
import os
from res.connectors.airtable import AIRTABLE_LATEST_S3_BUCKET
import time
from res.flows import flow_node_attributes

AIRTABLE_LATEST_BUCKET = "airtable-latest"
AIRTABLE_CELL_CHANGE_TOPIC = "airtable_cell_changes"


# deprecate
def get_handlers(fc, row):
    return None, None


def test_flow(event, context):
    return FlowContext(event, context, data_group="table_id")


def relay_cell_changes(event, context):
    with FlowContext(event, context) as fc:
        dy = AirtableDynamoTable()
        # TODO: FlowContext should pass some process name to determine the consumer group
        cell_changes = fc.connectors["kafka"][AIRTABLE_CELL_CHANGE_TOPIC].consume()
        # this is possibly inefficient. often time just the cell change and a key will be enough info
        row_changes = dy.batch_recover_rows(cell_changes)
        for row in row_changes:
            for topic, handler in get_handlers(fc, row):
                # the generator approach can be used to yield nothing or to flatten
                for row in handler(row):
                    fc.connectors["kafka"][topic].publish(
                        row, coerce_to_avro_schema=True
                    )


def get_tables_to_sync(event, context):
    with FlowContext(event, context) as fc:
        ac = fc.connectors["airtable"]

        from res.utils.configuration.ResConfigClient import ResConfigClient

        logger.info(
            f"Loading tables config for app {os.environ.get('RES_NAMESPACE')}.{os.environ.get('RES_APP_NAME')}"
        )

        logger.info(
            f"The event args passed were {event} and these will override what is read from config if they exist in assets"
        )

        samples = ResConfigClient().get_config_value("tables")

        # TODO: we will store this list somewhere but for dev we are just trying to decide what we need
        # samples = [
        #     {
        #         "table_id": "tblm1UVsXY1Npg1Jm",
        #         "base_id": "app4TnQg13PocmVAr",
        #         "name": "Whys_Table",
        #     },
        #     {
        #         "table_id": "tblt9jo5pmdo0htu3",
        #         "base_id": "appaaqq2FgooqpAnQ",
        #         "name": "test_hooks",
        #     },  # test hooks(
        #     {
        #         "table_id": "tblSYU8a71UgbQve4",
        #         "base_id": "apprcULXTWu33KFsh",
        #         "name": "rolls",
        #         "watermark": 30,
        #     },
        #     {
        #         "table_id": "tbl7n4mKIXpjMnJ3i",
        #         "base_id": "apprcULXTWu33KFsh",
        #         "name": "nested_asset_sets",
        #         "watermark": 30,
        #     },
        #     {
        #         "table_id": "tblwDQtDckvHKXO4w",
        #         "base_id": "apprcULXTWu33KFsh",
        #         "name": "print_assets",
        #         "watermark": 30,
        #     },
        #     {
        #         "table_id": "tblIH1BBcRFsnYPEr",
        #         "base_id": "apprcULXTWu33KFsh",
        #         "name": "pretreatments",
        #     },
        #     {
        #         "table_id": "tblZ6JDrAtMhxqFnT",
        #         "base_id": "appfaTObyfrmPHvHc",
        #         "name": "fulfillments",
        #     },
        #     {
        #         "table_id": "tblUcI0VyLs7070yI",
        #         "base_id": "appfaTObyfrmPHvHc",
        #         "name": "OrderLineItems",
        #         "base": "Fulfillments",
        #         "watermark": 60,
        #     },
        #     # {
        #     #     "table_id": "tblhtedTR2AFCpd8A",
        #     #     "base_id": "appfaTObyfrmPHvHc",
        #     #     "name": "Orders",
        #     #     "base": "Fulfillments",
        #     #     "watermark": 60,
        #     # },
        #     {
        #         "table_id": "tblc4lat36qr4UowZ",
        #         "base_id": "appoaCebaYWsdqB30",
        #         "name": "Requests",
        #         "base": "Purchasing",
        #     },
        #     {
        #         "table_id": "tblJOLE1yur3YhF0K",
        #         "base_id": "appoaCebaYWsdqB30",
        #         "name": "Materials",
        #         "base": "Purchasing",
        #     },
        #     {
        #         "table_id": "tblHbpG18hefs4nGJ",
        #         "base_id": "appoaCebaYWsdqB30",
        #         "name": "Shipments",
        #         "base": "Purchasing",
        #     },
        #     {
        #         "table_id": "tblpfjL2la1lKwyku",
        #         "base_id": "appoaCebaYWsdqB30",
        #         "name": "Items",
        #         "base": "Purchasing",
        #     },
        #     {
        #         "table_id": "tblwz5bE6llsShHzJ",
        #         "base_id": "appoaCebaYWsdqB30",
        #         "name": "Orders",
        #         "base": "Purchasing",
        #     },
        #     {
        #         "table_id": "tblAn5BLTFomuQFYf",
        #         "base_id": "app1FBXxTRoicCW8k",
        #         # "name": "Colors",
        #         "base": "Material Development",
        #     },
        #     {
        #         "table_id": "tblXXuR9kBZvbRqoU",
        #         "base_id": "appa7Sw0ML47cA8D1",
        #         # "name": "Bodies",
        #         "base": "Res Meta One",
        #     },
        #     {
        #         "table_id": "tblmszDBvO1MvJrlJ",
        #         "base_id": "appjmzNPXOuynj6xP",
        #         # "name": "Styles",
        #         "base": "Res Magic",
        #     },
        #     {
        #         "table_id": "tblrq1XfkbuElPh0l",
        #         "base_id": "appa7Sw0ML47cA8D1",
        #         # "name": "Body one ready requests",
        #         "base": "Res Meta ONE",
        #     },
        #     {
        #         "table_id": "tbl4g5Me2UigK9z3l",
        #         "base_id": "appjmzNPXOuynj6xP",
        #         # "name": "Markers",
        #         "base": "Res Magic",
        #     },
        #     {
        #         "table_id": "tbl8L3lrXxG6lnBnJ",
        #         "base_id": "appH5S4hIuz99tAjm",
        #         # "name": "Issues",
        #         "base": "Production",
        #     },
        #     {
        #         "table_id": "tblJxRYHI5OLFwKBg",
        #         "base_id": "appH5S4hIuz99tAjm",
        #         # "name": "Make ONE production",
        #     },

        #   {
        #          "table_id": "tblkj2cYxAtNaLWRS",
        #          "base_id": "app35ALADpo8gEqTy",
        #          "name": "healing"
        # }

        # ]

        def mem_for_table(t):
            """
            For now manage here but this could be config too
            """
            if t["table_id"] == "tblUcI0VyLs7070yI":
                return "64Gi"
            if t["table_id"] == "tblSYU8a71UgbQve4":
                return "32Gi"
            if t["table_id"] == "tblmszDBvO1MvJrlJ":
                return "32Gi"
            if t["table_id"] == "tblwDQtDckvHKXO4w":
                return "32Gi"

            # if its configured or default
            return t.get("memory", "4Gi")

        # allow for passing in specific ones ot default to this set
        # the payload should be a res flow type and include assets : []
        samples = event.get("assets", samples)
        # return an event sub structure
        return [{"args": t, "memory": mem_for_table(t)} for t in samples]


def airtable_to_dgraph(event, context):
    assets = event.get("args", {})  # .get("assets")
    timestamp_watermark = None
    # for now
    assets = [assets]
    with FlowContext(event, context) as fc:
        at = fc.connectors["airtable"]
        for asset in assets:
            if asset.get("to_dgraph", False):
                logger.info(f"Loading dgraph schema for table {asset['name']}")
                dg = fc.connectors["dgraph"][asset]
                schema = dg.get_schema()

                logger.info("loading ")
                table = at[schema["airtable_base_id"]][schema["airtable_table_id"]]
                fields = dict(
                    pd.DataFrame(schema["fields"])[["airtable_field_name", "name"]]
                    .dropna()
                    .values
                )

                logger.info(
                    f"Loading airtable records from watermark: {timestamp_watermark}"
                )

                # the default modified data will be __modified_timestamp but the schema will refer to non defaults such as Last Updated At
                # but wll tables that will be synced will have a modified field
                data = table.to_dataframe(
                    fields=list(fields.keys()), modified=timestamp_watermark
                ).rename(columns=fields)

                logger.info(
                    f"Updating dgraph with {len(data)} records from airtable..."
                )
                dg.update_dataframe(data)
            else:
                logger.info(
                    f"Skipping {asset['name']} which is not configured for dgraph"
                )


def bootstrap_tables(event, context):
    logger.info("reading locations")
    # locs = readers._get_legacy_archive_locations()
    # save to snapshot location if its slow
    locs = pd.read_csv("s3://res-data-platform/.cache/airtable_archive_locations.csv")

    # pretreatemts, orderlinteitems, requests, materials, shipments, items/additions, POs
    for table_id in [
        # "tblIH1BBcRFsnYPEr",
        # "tblUcI0VyLs7070yI",
        # "tblc4lat36qr4UowZ",
        # "tblJOLE1yur3YhF0K",
        # "tblHbpG18hefs4nGJ",
        # "tblpfjL2la1lKwyku",
        # "tblwz5bE6llsShHzJ",
        # "tblJxRYHI5OLFwKBg"
        "tblmszDBvO1MvJrlJ"
    ]:
        logger.info(f"Compiling data for {table_id}")
        readers.bootstrap_table_history(table_id, locs)


def distinct_arch_rec(event, context):
    locs = pd.read_csv("s3://res-data-platform/.cache/airtable_archive_locations.csv")
    table_id = "tblUcI0VyLs7070yI"
    data = readers.count_distinct_archive_records(locs, table_id=table_id)
    logger.info(f"saving {len(data)} records...")
    try:
        data.to_parquet(
            f"s3://res-data-platform/.cache/distinct_records/{table_id}.parquet"
        )
    except:
        data.to_csv(f"s3://res-data-platform/.cache/distinct_records/{table_id}.csv")


@flow_node_attributes(memory="32Gi")
def take_snapshot(event, context):
    table_id = event.get("args", {}).get("table_id")
    base_id = event.get("args", {}).get("base_id")

    if table_id is not None and base_id is not None:
        with FlowContext(event, context, data_group=table_id) as fc:
            ac = fc.connectors["airtable"]
            table = ac[base_id][table_id]
            latest = table.take_snapshot(merge_latest=True)
            pivot = table.make_typed_table(latest=latest)
            return table.update_record_partitions(pivot)
    else:
        logger.warn(
            f"No action taken as valid table and base not supplied in event {event}"
        )


@flow_node_attributes(memory="32Gi")
def build_stages(event, context):
    """
    Build monthly partitions of archived updated moved to cell formats
    This is done before taking all snapshots from stages
    """
    table_id = event.get("args", {}).get("table_id", "any")
    with FlowContext(event, context, data_group=table_id) as fc:
        metadata = pd.read_csv(
            "s3://res-data-platform/.cache/airtable_archive_locations.csv"
        )
        metadata["stage"] = metadata["ts"].map(lambda x: str(x)[:7])
        for stage in list(metadata["stage"].unique()):
            readers.bootstrap_table_history(
                table_ids=[fc.args.get("table_id")],
                metadata=metadata,
                show_progress=True,
                stage=stage,
            )


@flow_node_attributes(memory="32Gi")
def take_all_snapshots(event, context):
    """
    We can archive in monthly stages - these can each be merged into their respective partition file
    We take the so called legacy archives and merge them into a common format

    #post to res-connect or invoke to run on argo
    {
        "apiVersion": "v0",
        "kind": "resFlow",
        "metadata": {
            "name": "etl.airtable.take_all_snapshots",
            "version": "exp1"
        },
        "assets": [
            {}
        ],
        "task": {
            "key": "test123"
        },
        "args": {
            "table_id" : "tblUcI0VyLs7070yI",
            "base_id" : "appfaTObyfrmPHvHc"
        }
    }
    #push docker tag if necessary
    """
    # table_id = event.get("args", {}).get("table_id", "tblwDQtDckvHKXO4w")
    # base_id = event.get("args", {}).get("base_id", "apprcULXTWu33KFsh")
    table_id = event.get("args", {}).get("table_id", "tblUcI0VyLs7070yI")
    base_id = event.get("args", {}).get("base_id", "appfaTObyfrmPHvHc")

    logger.info(f"processing table {table_id}")

    examples = [
        "CC-8002 CTNOX SELF",
        "TK-6077 CTW70 COBAOI",
        "TK-2035 SGT19 LGACQS",
        "KT-6041 VIC55 ORANHB",
        "TK-3001 CDC16 BLACML",
        "KT-6041 VIC55 MOUNOB",
        "TK-3025 SGT19 IVORQH",
        "TK-3075 SGT19 BLACML",
        "TK-6093 SGT19 LEOPAR",
    ]

    if table_id is not None and base_id is not None:
        with FlowContext(event, context, data_group=table_id) as fc:
            ac = fc.connectors["airtable"]
            s3 = fc.connectors["s3"]
            table = ac[base_id][table_id]
            # read alls staged files we have
            for latest_stage in s3.ls(
                f"s3://{AIRTABLE_LATEST_S3_BUCKET}/{base_id}/{table_id}/stages/"
            ):
                try:
                    logger.info(f"Merging stage {latest_stage} to partitions")
                    latest = pd.read_parquet(latest_stage)

                    found = list(
                        latest[latest["cell_value"].isin(examples)][
                            "cell_value"
                        ].unique()
                    )

                    logger.info(
                        f"FOUND THESE - expect to see them in partitions {found}"
                    )

                    # tracker - special case to track material swaps on styles (assuming resonance code is the key through time which is semi safe but otherwise its a bit messy(er))
                    if table_id == "tblmszDBvO1MvJrlJ":
                        logger.info(
                            "looking up style history as special case for styles table - will write a new entity called style_key_history to s3"
                        )
                        lookup = latest[
                            latest["column_name"] == "Resonance_code"
                        ].reset_index()
                        lookup["key"] = lookup.apply(
                            lambda x: f"{x['record_id']}-{x['cell_value']}", axis=1
                        )
                        s3.write_date_partition(
                            lookup,
                            date_partition_key="timestamp",
                            entity="style_key_history",
                            dedup_key="key",
                        )
                        #

                    pivot = table.make_typed_table(latest=latest)
                    table.update_record_partitions(pivot)
                except:
                    logger.warn(
                        f"FAILED ON THIS ONE - load the file and see if we can update the partition"
                    )
    else:
        logger.warn(
            f"No action taken as valid table and base not supplied in event {event}"
        )


def update_snowflake(event, context={}):
    table_id = event.get("args", {}).get("table_id")
    base_id = event.get("args", {}).get("base_id")
    watermark = event.get("args", {}).get("watermark")
    sleep = event.get("args", {}).get("sleep", 120)
    watermark_date = dates.relative_to_now(watermark)

    upsert_prod = event.get("args", {}).get("upsert_staging_to_prod", False)
    if table_id is not None and base_id is not None:
        with FlowContext(event, context, data_group=table_id) as fc:
            table = fc.connectors["airtable"][base_id][table_id]
            stage_root = (
                f"s3://{AIRTABLE_LATEST_BUCKET}/{base_id}/{table_id}/partitions/"
            )
            result = fc.connectors["snowflake"].ingest_partitions(
                table_name=table.table_name,
                namespace="airtable",
                table_metadata=table.fields,
                partitions_modified_since=watermark_date,
                stage_root=stage_root,
                trigger_upsert_transaction=upsert_prod,
                table_discriminator=table.base_name,
            )

            if sleep:
                time.sleep(sleep)

            logger.info(
                f"Slept for {sleep or 0} seconds and checking progress to confirm there were no errors at least"
            )
            # TODO handle errors properly - its async so need to look into this
            logger.info(f"{result.get_history()}")
            return result
    else:
        logger.warn(f"No action taken as valid table and base not supplied")


def update_looker(event, context):
    """
    For the given table, lookup schema metadata/pass metadata
    Create views in the environment env/views/ and make make sure the extendable model has all the views

    views/test
    """

    table_id = event.get("args", {}).get("table_id")
    base_id = event.get("args", {}).get("base_id")
    # other props

    with FlowContext(event, context, data_group=table_id) as fc:
        pass


def update_entities(event, context):
    """
    We move our data from the airtable snapshots to our cleaned 'entity' views for analytics
    """
    args = event.get("args", {})
    table_id = args.get("table_id")
    base_id = args.get("base_id")
    ingest_druid = args.get(
        "druid_ingestion",
        True,
    )
    ingest_snowflake = args.get(
        "snowflake_ingestion",
        False,
    )

    # use the table name as the map top our entities in this case
    table_name = event.get("args", {}).get("table_name")
    partitions_modified_since = None

    if table_id is not None and base_id is not None:
        with FlowContext(event, context, data_group=table_id) as fc:
            # now actually setting snowflake schema yet
            snow_schema = os.getenv("SNOWFLAKE_SCHEMA", "IAMCURIOUS_DEVELOPMENT")
            if os.getenv("RES_ENV", "development") == "production":
                snow_schema = "IAMCURIOUS_PRODUCTION"

            table = fc.connectors["airtable"][base_id][table_id]
            # this is not the one we want to partition by necessarily
            timestamp_column = "__timestamp"
            # read airtable pivot and map to our schema if the entity mapping exists
            # we only need as much data s we want to update partitions for - entity name means we map the schema
            data = table.read_all(
                entity_name=table_name,
                partitions_modified_since=partitions_modified_since,
            )

            partitions = fc.connectors["s3"].write_date_partition(
                data, entity=table_name, timestamp_column=timestamp_column
            )

            if ingest_druid:
                # in processes 'like' this we can create an augmentation for an entity e.g. entity_with_material_attributes and ingest that
                result = fc.connectors["druid"].ingest_partitions(
                    entity=table_name, partitions=partitions
                )

            logger(f"Using snow account {os.environ.get('SNOWFLAKE_ACCOUNT')}")

            if ingest_snowflake:
                stage_root = f"s3://res-data-platform/entities/{table_name}"
                result = fc.connectors["snowflake"].ingest_partitions(
                    table_name=table_name,
                    stage_root=stage_root,
                    snow_schema=snow_schema,
                    partitions_modified_since=partitions_modified_since,
                )

    else:
        logger.warn(f"No action taken as valid table and base not supplied")


def webhook_health_check(event, context):
    from res.connectors.airtable import AirtableWebhooks

    AirtableWebhooks.health()


# implement the flow node interface
def generator(event, context):
    pass


def handler(event, context):
    pass
