import res
import typing

from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient

from . import queries

gql = ResGraphQLClient()


from res.airtable.misc import ORDER_LINE_ITEMS, ORDERS_TABLE


def get_line_item(line_item_id: str):
    line_item_record = ORDER_LINE_ITEMS.get(line_item_id)
    return line_item_record["fields"]


# anywhere that and whatever_id is really an airtable_recordid - rename it to whatever_recordid instead of just _id
def get_order(order_id: str):
    order_record = ORDERS_TABLE.get(order_id)
    return order_record["fields"]


def get_orders_by_brand(brand_code: str, start_date, end_date, postgres=None):
    res.utils.logger.info(f"Processing function payment_api_utils get_orders_by_brand")
    query = queries.GET_ORDERS_BY_BRAND_AND_DATE

    postgres = postgres or res.connectors.load("postgres")

    df = postgres.run_query(query, (brand_code, start_date, end_date))

    df = df.sort_values(by="updated_at", ascending=False)

    order_columns = [
        "id",
        "created_at",
        "updated_at",
        "customer_id",
        "ecommerce_source",
        "source_order_id",
        "ordered_at",
        "brand_code",
        "source_metadata",
        "sales_channel",
        "order_channel",
        "email",
        "tags",
        "item_quantity_hash",
        "name",
        "status",
        "contracts_failing",
        "revenue_share_code",
        "was_payment_successful",
        "was_balance_not_enough",
    ]

    order_line_items_columns = [
        "line_item_id",
        "line_item_created_at",
        "line_item_updated_at",
        "order_id",
        "source_order_line_item_id",
        "sku",
        "quantity",
        "price",
        "line_item_source_metadata",
        "line_item_status",
        "customization",
        "line_item_metadata",
        "line_item_name",
        "product_id",
        "fulfillable_quantity",
        "fulfilled_quantity",
        "refunded_quantity",
    ]

    # Group by order-specific columns and create the desired structure
    orders_json = []

    for _, group in df.groupby("name"):
        order_info = group.iloc[0][
            order_columns
        ].to_dict()  # Extract order-specific info from the first row
        order_info["order_line_items"] = group[order_line_items_columns].to_dict(
            orient="records"
        )
        orders_json.append(order_info)

    return orders_json


def get_production_requests_for_order(
    order_number: str, include_cancelled: bool = False
) -> typing.List[dict]:
    """
    Get the raw data joining all ONEs ever created against the order
    Used to allow brands to track orders as they progress through the production flow
    """

    res.utils.logger.info(
        f"Processing function fulfillments.get_production_requests_for_order"
    )
    query = queries.GET_ORDER_PRODUCTION_REQUESTS

    postgres = res.connectors.load("postgres")

    df = postgres.run_query(query, (order_number,))

    # TODO apply some logic here to format results as the client requires and take into account the include-cancelled flag
    # at a minimum we should do the checksum to show when an order item does not have a production request at all
    # - this is a notification worthy event depending on the delay
    # we should also see what the legacy create one interface does and fill in the pieces
    # note this data depends on a process that updates one status on a 5 minute cron job in the res/flows/make/production/queue_update

    return df.to_dict("records")
