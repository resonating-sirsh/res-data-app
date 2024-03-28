"""
Workflow create or update a ShopByColorInstance in shopify.

To invoke this workflow, you need to send post request to res-connect with
the following body:

TODO: Make this body more descriptive

{
    "args": {
        "id": '<shop-by-color-instance-id>',
    }
    metadata: {
    }
}
"""

from res.connectors import load
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.flows import flow_context
from res.flows.sell.product.group.src.graphql import (
    create_stamped_group,
    get_instance_id_from_event,
    get_shop_by_color_instance,
    release_shop_by_color,
    update_shop_by_color_record,
    validate_color_options,
    validate_if_other_sbc_live,
)
from res.utils import logger

# dependencies
mongo = load("mongo")
file_collection = mongo.get_client().get_database("resmagic").get_collection("files")
graphql: ResGraphQLClient = load("graphql")


@flow_context(send_errors_to_sentry=True)
def handler(event, context):
    instance_id = get_instance_id_from_event(event)
    shop_by_color_instance = get_shop_by_color_instance(id=instance_id, graphql=graphql)
    if not shop_by_color_instance:
        raise Exception(f"shop by color instance with id {instance_id} not found")
    logger.info("uptaing shopify data")
    product_ids = validate_color_options(
        shop_by_color_instance, file_collection, graphql
    )

    store_code = shop_by_color_instance["storeBrand"]["code"]

    try:
        rule_stamped = create_stamped_group(product_ids)

        payload = {
            "reviewProductGroupItemList": rule_stamped,
            "groupName": shop_by_color_instance["title"],
        }

        graphql.query(
            "mutation createStampedProductGroup( $store_code: String! $body: StampedGroupBody) { syncStampedGroup(storeCode: $store_code, body: $body) }",
            {"store_code": store_code, "body": payload},
        )
    except:
        logger.warning("Not created group, error happents")

    validate_if_other_sbc_live(shop_by_color=shop_by_color_instance, graphql=graphql)

    logger.info("shop by color instance release")
    for id in product_ids:
        release_shop_by_color(id=id, instance_id=instance_id, graphql=graphql)

    update_shop_by_color_record(
        instance_id, {"status": "success", "live": True}, graphql
    )


def on_success(event, context):
    logger.debug(context)
    instance_id = get_instance_id_from_event(event)
    logger.info(f"shop by color instance with id {instance_id} updated")


def on_failure(event, context):
    logger.debug(context)
    instance_id = get_instance_id_from_event(event)
    logger.error(f"shop by color instance with id {instance_id} not updated")
    update_shop_by_color_record(
        instance_id, {"status": "failure", "live": False}, graphql
    )


if __name__ == "__main__":
    # tested record
    id = "63bdd0919a55515f496a5146"
    event = {"args": {"id": id}}

    try:
        handler(event, {})
        on_success(event, {})
    except Exception as e:
        logger.error(str(e))
        on_failure(event, {})
