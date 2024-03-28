import uuid


def get_shopify_order_object(original_order):
    return_data = {
        "uuid": str(uuid.uuid4()),
        "source": "shopify",
        "source_order_id": str(original_order["id"]),
        "source_order_secondary_id": original_order["name"],
        "brand_code": original_order["brand_code"],
        "created_at": original_order["created_at"],
        "updated_at": original_order["updated_at"],
        "sales_channel": "ECOM",
        "total_line_items_price": float(original_order["total_line_items_price"]),
        "total_discounts": float(original_order["total_discounts"]),
        "subtotal_price": float(original_order["subtotal_price"]),
        "total_tax": float(original_order["total_tax"]),
        "total_price": float(original_order["total_price"]),
        "fulfillment_status": original_order["fulfillment_status"],
        "cancel_reason": original_order["cancel_reason"],
        "cancelled_at": original_order["cancelled_at"],
    }
    if "customer" in original_order:
        return_data["source_customer_id"] = str(original_order["customer"]["id"])
    return return_data


def get_shopify_items_objects(original_order):
    return_items = []
    for item in original_order["line_items"]:
        return_items.append(
            {
                "uuid": str(uuid.uuid4()),
                "source_order_id": str(original_order["id"]),
                "source_line_item_id": str(item["id"]),
                "source_variant_id": str(item["variant_id"]),
                "quantity": item["quantity"],
                "sku": item["sku"],
                "price": float(item["price"]),
                "brand_code": original_order["brand_code"],
                "sales_channel": "ECOM",
                "created_at": original_order["created_at"],
                "updated_at": original_order["updated_at"],
                "fulfillment_status": original_order["fulfillment_status"],
            }
        )
    return return_items
