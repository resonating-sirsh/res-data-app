"""
Workflow which refresh or sync a ShopByColorInstance data with its products in Shopify.
"""
from res.connectors import load
from res.flows import flow_context
from res.utils.logging import logger
from res.flows.sell.product.group.src.graphql import (
    get_instance_id_from_event,
    get_shop_by_color_instance,
    update_shop_by_color_record,
    validate_color_options,
    update_product_shop_by_color,
)

graphql = load("graphql")
mongo = load("mongo")
file_collection = mongo.get_client().get_database("resmagic").get_collection("files")


@flow_context(send_errors_to_sentry=True)
def handler(event, context):
    logger.debug(context)
    instance_id = get_instance_id_from_event(event)
    shop_by_color_instance = get_shop_by_color_instance(id=instance_id, graphql=graphql)
    if not shop_by_color_instance:
        logger.error(f"shop by color instance with id {instance_id} not found")
        return
    logger.info("uptaing shopify data")
    product_ids = validate_color_options(
        shop_by_color_instance, file_collection, graphql
    )
    for id in product_ids:
        logger.info(f"Refreshing product {id}")
        update_product_shop_by_color(id, graphql)


def on_success(event, context):
    logger.debug(context)
    id = get_instance_id_from_event(event)
    update_shop_by_color_record(
        id=id,
        input={
            "status": "success",
        },
        graphql=graphql,
    )


def on_failure(event, context):
    logger.debug(context)
    id = get_instance_id_from_event(event)
    update_shop_by_color_record(
        id=id,
        input={
            "status": "failure",
        },
        graphql=graphql,
    )


if __name__ == "__main__":
    event = {"args": {"id": "61f859b8b5aa8f0009bea232"}}
    context = {}
    try:
        handler(event, None)
        on_success(event, context)
    except Exception as e:
        on_failure(event, context)
