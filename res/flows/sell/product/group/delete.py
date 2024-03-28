"""
Worflow delete a ShopByColorInstance based on the campaign from Shopify.
"""
import res

from res.flows.sell.product.group.src.graphql import (
    disable_shop_by_color_from_product,
    get_all_products,
    update_shop_by_color_record,
)

from res.utils import logger

graphql_client = res.connectors.load("graphql")


@res.flows.flow_context(send_errors_to_sentry=True)
def handler(event, context):
    instance_id = get_instance_id_from_event(event)
    products = get_all_products(
        {
            "shopByColorId": {
                "is": instance_id,
            },
        },
        graphql_client,
    )
    for product in products:
        disable_shop_by_color_from_product(product.get("id"), graphql_client)


def on_success(event, context):
    instance_id = get_instance_id_from_event(event)
    logger.info(f"shop by color instance with id {instance_id} succeeded")
    update_shop_by_color_record(
        instance_id, {"live": False, "status": "success"}, graphql_client
    )


def on_failure(event, context):
    instance_id = get_instance_id_from_event(event)
    logger.info(f"shop by color instance with id {instance_id} failed")
    update_shop_by_color_record(instance_id, {"status": "failure"}, graphql_client)


# handlers


def get_instance_id_from_event(event):
    return event.get("args").get("id")


DELETE_SHOP_BY_COLOR = """
mutation delete($id: ID!) {
    deleteShopByColorInstance(id: $id) {
        shopByColorInstance {
            id
        }
    }   
}
"""

if __name__ == "__main__":
    event = {"args": {"id": "61fade96f1c141000981db7b"}}
    try:
        handler(event, None)
        on_success(event, None)
    except Exception as e:
        logger.error(e)
        on_failure(event, None)
