from datetime import datetime, timedelta
import res


DEFAULT_TABLES = [
    "res_pixel_bot_logs",
    "flower_events",
    "resMagicFulfillment_Order_Line_Items",
    "make_one_production_request" "Rolls",
    "items_rolls",
    "DxA_Nodes",
    "resPrintActivities",
    "Print_Assets_flow",
]


def update_table_usage_for_tables(event, context):
    watermark = event.get("args", {}).get("watermark", 2)
    tables = event.get("args", {}).get("tables", DEFAULT_TABLES)

    with res.flows.FlowContext(event, context) as fc:
        snow = fc.connectors["snowflake"]
        s3 = fc.connectors["s3"]
        ac = fc.connectors["athena"]
        entity = "infra/snow/selected_table_usage"

        res.utils.logger.info(
            f"Processing the last {watermark} days of query data in snowflake for select tables {tables}"
        )

        for days in range(watermark):
            # add + 1 to always do current partition it "ENDS" on offset
            offset = datetime.now().date() - timedelta(days)
            snow.get_daily_table_usage(
                tables=tables,
                tday=offset,
                write_partitions=True,
                entity_name=entity,
            )

        res.utils.logger.info(f"Indexing entity {entity} in athena")

        path = s3.get_partition_path_for_entity("infra/snow/selected_table_usage")
        # ac.index_entity(entity, path)
