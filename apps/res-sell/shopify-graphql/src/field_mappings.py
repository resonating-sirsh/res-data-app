shopify_order_map = {
    "id": "channelOrderId",
    "email": "email",
    "tags": "tags",
    "source_name": "sourceName",
    "discount_codes": "discountCode",
    "fulfillment_status": "fulfillmentStatus",
    "closed_at": "platformClosedAt",
    "financial_status": "financialStatus",
    "total_discounts": "discountAmount",
    "created_at": "dateCreatedAt",
    "shipping_title": "shippingMethod",
}

shopify_order_line_item_map = {
    "id": "channelOrderLineItemId",
    "name": "productName",
    "variant_title": "productVariantTitle",
    "sku": "sku",
    "price": "lineItemPrice",
    "fulfillment_status": "shopifyFulfillmentStatus",
    "fulfillable_quantity": "shopifyFulfillableQuantity",
    "variant_id": "variantId",
}

shopify_order_line_item_customization_map = {
    "id": "styleCustomizationId",
    "value": "value",
}

shopify_customer_map = {
    "customer_id": "customerIdChannel",
    "customer_first_name": "requesterFirstName",
    "customer_orders_count": "customerOrderCount",
    "customer_total_spent": "customerTotalSpent",
    "customer_tags": "customerTags",
    "customer_created_at": "customerFirstPurchaseDate",
}
