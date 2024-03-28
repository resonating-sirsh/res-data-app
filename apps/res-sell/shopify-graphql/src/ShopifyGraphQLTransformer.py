import os, json, requests, logging, arrow
from . import field_mappings, graphql_queries
from res.utils import logger

# This is a transformer to take a raw Shopify order and convert it to the type GraphQL expects


class ShopifyGraphQLTransformer:
    def __init__(self, graphql_client):
        self.graphql_client = graphql_client
        self.new_order_payload = None
        self.sales_channel = "ECOM"
        self.should_send = True
        self.brand = None
        self.fulfillment_order_id = None
        self.order_mutation_type = "create"  # or "update"

    def _get_body_code(self, sku):
        # Resonance sku's are always 25 characters, with 5 parts
        if len(sku) != 25:
            return None
        sku_parts = sku.split()
        if len(sku_parts) < 4:
            return None
        # First 2 characters are the brand code and remainder is the body code
        body_code = sku_parts[0][:2] + "-" + sku_parts[0][2:]
        return body_code

    def _get_external_items(self, line_items):
        # Determines "external" items, meaning not a Resonance item
        external_items = []
        for line_item in line_items:
            if not line_item.get("sku"):
                # If there is no sku, count as external
                external_items.append(line_item["id"])
            else:
                body_code = self._get_body_code(line_item["sku"])
                if not body_code:
                    # Body code doesn't match Resonance format
                    external_items.append(line_item["id"])
                elif not self.graphql_client.query(
                    graphql_queries.FIND_BODY_QUERY, {"number": body_code}
                ):
                    # No record of this item in GraphQl, count as external
                    external_items.append(line_item["id"])
        return external_items

    def _count_existing_orders(self):
        orders = self.graphql_client.query(
            graphql_queries.GET_ORDERS_QUERY,
            {
                "where": {
                    "orderChannel": {"is": "Shopify"},
                    "name": {"is": self.new_order_payload["name"]},
                    "brandCode": {"is": self.brand["code"]},
                }
            },
        )["data"]["orders"]["orders"]
        if len(orders) > 0:
            # Fulfillment record id to update order
            self.fulfillment_order_id = orders[0]["id"]
        return len(orders)

    def _submit_issue(self):
        self.graphql_client.submit_issue(
            context="This order is duplicated",
            subject=f"{self.new_order_payload['name']} - Duplicated Order",
            dxa_node_id="7",
            issue_type_ids=["recFtBwBiqtxpyWEC"],
            source_record_id=self.new_order_payload["name"],
        )

    def _normalize_fields(self, raw_order):
        new_order_payload = {}

        # Normalizing Shopify order fields
        new_order_payload["name"] = f"#{str(raw_order['order_number'])}"
        for field_name in field_mappings.shopify_order_map:
            normalized_field_name = field_mappings.shopify_order_map[field_name]
            if field_name == "id":
                # ID is stored as a string
                new_order_payload[normalized_field_name] = str(raw_order[field_name])
            else:
                # No special mapping needed for the rest of fields
                new_order_payload[normalized_field_name] = raw_order.get(
                    field_name, "unknown"
                )

        # Normalizing Shopify customer fields
        for field_name in field_mappings.shopify_customer_map:
            normalized_field_name = field_mappings.shopify_customer_map[field_name]
            new_order_payload[normalized_field_name] = str(
                raw_order.get(field_name, "unknown")
            )

        return new_order_payload

    def _get_brand(self, brand_code):
        self.brand = self.graphql_client.query(
            graphql_queries.GET_BRAND_QUERY,
            {"where": {"code": {"is": brand_code}, "isActive": {"is": True}}},
        )["data"]["brands"]["brands"][0]

    def _insert_internal_fields(self, has_external_items):
        self.new_order_payload["lastUpdatedAt"] = arrow.utcnow().isoformat()
        self.new_order_payload["brandId"] = self.brand["fulfillmentId"]
        self.new_order_payload["orderChannel"] = "Shopify"
        self.new_order_payload["salesChannel"] = self.sales_channel
        self.new_order_payload["containsExternalItem"] = has_external_items
        self.new_order_payload["releaseStatus"] = "HOLD"
        self.new_order_payload["lineItemsInfo"] = []
        if self.order_mutation_type != "create":
            self.new_order_payload["id"] = self.fulfillment_order_id

    def _get_shipping_address(self, raw_order):
        address = {}
        for field in field_mappings.shopify_order_shipping_address_map:
            normalized_field_name = field_mappings.shopify_order_shipping_address_map[
                field
            ]
            address[normalized_field_name] = raw_order[field]
        self.new_order_payload["shippingAddress"] = address

    def _get_order_line_items(self, raw_order, external_items):
        should_submit_an_issue = False
        for line_item in raw_order["line_items"]:
            new_line_items_payload = {}
            # Ignore line items that are external
            if line_item["id"] not in external_items and self.should_send:
                for field in field_mappings.shopify_order_line_item_map:
                    normalized_field_name = field_mappings.shopify_order_line_item_map[
                        field
                    ]
                    # ID needs to be an string
                    if field == "id":
                        new_line_items_payload[normalized_field_name] = str(
                            line_item[field]
                        )
                    else:
                        new_line_items_payload[normalized_field_name] = line_item[field]

                # Change fulfillment status to unfulfilled instead of null
                if new_line_items_payload["shopifyFulfillmentStatus"] is None:
                    new_line_items_payload["shopifyFulfillmentStatus"] = "unfulfilled"

                new_line_items_payload["orderNumber"] = self.new_order_payload["name"]

                # Because we only do ONEs
                new_line_items_payload["quantity"] = 1

                # Get Line Items customization values
                if line_item.get("customizations"):
                    customizations = []
                    for customization in line_item.get("customizations"):
                        new_customization = {}
                        for (
                            field
                        ) in field_mappings.shopify_order_line_item_customization_map:
                            normalized_field_name = field_mappings.shopify_order_line_item_customization_map[
                                field
                            ]
                            new_customization[normalized_field_name] = customization[
                                field
                            ]
                        customizations.append(new_customization)

                    new_line_items_payload["customizations"] = customizations
                now = arrow.utcnow()
                tdelta = now - arrow.get(self.new_order_payload["dateCreatedAt"])
                if self.order_mutation_type == "update" and (tdelta.seconds > 300):
                    item_in_platform_response = self.graphql_client.query(
                        graphql_queries.ORDER_LINES_QUERY,
                        {
                            "where": {
                                "brandCode": {"is": self.brand["code"]},
                                "orderNumber": {"is": self.new_order_payload["name"]},
                                "sku": {"is": line_item["sku"]},
                            }
                        },
                    )
                    items_in_platform = item_in_platform_response["data"][
                        "orderLineItems"
                    ]["orderLineItems"]

                    if (
                        len(items_in_platform) != line_item["quantity"]
                        and tdelta.days >= 1
                    ):
                        should_submit_an_issue = True
                        err = "Missing line items in platform for order: {}".format(
                            self.new_order_payload["name"]
                        )
                        logger.critical(err)
                    else:
                        for x in range(line_item["quantity"]):
                            new_line_items_payload["id"] = items_in_platform[x]["id"]
                            self.new_order_payload["lineItemsInfo"].append(
                                new_line_items_payload
                            )

                else:
                    for x in range(line_item["quantity"]):
                        self.new_order_payload["lineItemsInfo"].append(
                            new_line_items_payload
                        )

        if should_submit_an_issue:
            self.graphql_client.submit_issue(
                context="Fulfillment order was missing line items from this order",
                subject=f"{self.new_order_payload['name']} - Missing Line Item",
                dxa_node_id="7",
                issue_type_ids=["recP8JtxebQmO0MHi"],
                source_record_id=self.new_order_payload["name"],
            )

    def load_order(self, raw_order):

        self.order_mutation_type = raw_order["type"]
        # Count number of external items
        external_items = self._get_external_items(raw_order["line_items"])

        has_external_items = False

        if len(external_items) == len(raw_order["line_items"]):
            # All items in order are external, don't send to graphQL
            self.should_send = False
            logger.warn("Should not submit order: all line items are external")
            return 0

        if len(external_items) > 0:
            has_external_items = True

        # Check tags to determine whether to send / type of sales channel
        tags = raw_order["tags"].lower().split(", ")
        if "invoice" in tags:
            self.should_send = False
            logger.warn("Should not submit order: this is an invoice")
            return 0
        if "wholesale" in tags:
            self.sales_channel = "Wholesale"

        # Convert field names to match what GraphQL expects
        self.new_order_payload = self._normalize_fields(raw_order)

        # Get Brand
        self._get_brand(raw_order["brand"])

        # Determine if this is an update or create, or an issue if >1 orders existing
        existing_order_count = self._count_existing_orders()
        if existing_order_count > 1:
            # More than 1 copy in GraphQL, don't send, flag as issue
            self._submit_issue()
            logger.critical("Should not submit order: more than 1 order found")
            self.should_send = False
            return 0
        if existing_order_count == 1 and self.order_mutation_type == "create":
            self._submit_issue()
            logger.warn("Order already exists")
            self.should_send = False
            return 0
        if existing_order_count == 0 and self.order_mutation_type == "update":
            self._submit_issue()
            logger.warn("Order does not yet exist")
            self.should_send = False
            return 0

        # Insert Resonance special fields
        self._insert_internal_fields(has_external_items)

        self._get_order_line_items(raw_order, external_items)
        # self._get_shipping_address(raw_order)

        return 0

    def send_order(self, env):
        logger.debug("Environment: {}".format(env))
        logger.info("Payload: {}".format(json.dumps(self.new_order_payload, indent=4)))
        if self.should_send and env == "development":
            # References Testinng Brand to prevent order to get to production
            self.new_order_payload["brandId"] = "recfgXXaCZpsiHYss"
        response = {}
        if self.should_send:
            if self.order_mutation_type == "create":
                # Send using create query
                logger.info("Event: {}".format("Creating"))
                response = self.graphql_client.query(
                    graphql_queries.CREATE_ORDER_MUTATION,
                    {"input": self.new_order_payload},
                )
                # Check response for 200 TODO
            elif self.order_mutation_type == "update":
                # Send using update query
                logger.info("Event: {}".format("Updating"))
                response = self.graphql_client.query(
                    graphql_queries.UPDATE_ORDER_MUTATION,
                    {"input": self.new_order_payload},
                )
            elif self.order_mutation_type == "cancel":
                logger.info("Event: {}".format("Cancelling"))
                response = self.graphql_client.query(
                    graphql_queries.UPDATE_ORDER_MUTATION,
                    {"input": {"fulfillmentStatus": "CANCELED"}},
                )
            logger.info("Graphql Response: {}".format(json.dumps(response, indent=4)))
        else:
            logger.info("Payload not sent.")
            return None, False
        # if "errors" in response.json():
        #     return response.json()['errors'][0]['message']
        # # False - No errors found
        return None, True
