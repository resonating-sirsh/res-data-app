import uuid


def get_create_one_order_object(original_order):
    return {
        "uuid": str(uuid.uuid4()),
        "source": "create_one",
        "source_order_id": original_order["id"],
        "brand_code": original_order["brandCode"][0],
        "created_at": original_order["createdAt"],
        "updated_at": original_order["createdAt"],
        "sales_channel": original_order["salesChannel"],
        "total_line_items_price": 0.0,
        "total_discounts": 0.0,
        "subtotal_price": 0.0,
        "total_tax": 0.0,
        "total_price": 0.0,
        "fulfillment_status": original_order["orderStatus"],
    }


def get_create_one_items_objects(original_order):
    return_items = []
    for item in original_order["lineItemsInfo"]:
        return_items.append(
            {
                "uuid": str(uuid.uuid4()),
                "source_order_id": original_order["id"],
                "source_line_item_id": item["id"],
                "quantity": item["quantity"],
                "sku": item["sku"],
                "brand_code": original_order["brandCode"][0],
                "sales_channel": original_order["salesChannel"],
                "created_at": original_order["createdAt"],
                "updated_at": original_order["createdAt"],
                "fulfillment_status": original_order["orderStatus"],
            }
        )
    return return_items
