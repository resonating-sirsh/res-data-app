import os
from res.utils import logger
from res.connectors.graphql.ResGraphQLClient import ResGraphQLClient
from res.flows.make.payment.queries import ORDER_UPDATE_MUTATION


class PaymentOrderPaid:
    """This process updates airtable orders for wasPaymentSuccessful field."""

    def __init__(self):
        self.res_graphql_client = ResGraphQLClient()

    def update(self, transaction_source, order_id, is_failed, customer_id=None):
        order_update_data = {}
        ##Payment saved only on production
        if (
            transaction_source == "order"
            and order_id
            and (
                os.getenv("RES_ENV") == "production"
                or customer_id == os.getenv("TECH_PIRATES_TEST_CUSTOMER_ID")
            )
            and not is_failed
        ):
            order_fields = {"wasPaymentSuccessful": True}
            order_update_data = self.res_graphql_client.query(
                ORDER_UPDATE_MUTATION, {"id": order_id, "input": order_fields}
            )
            logger.info(f"update_order_was_paid: {order_update_data}")
        else:
            logger.info(
                "No proper input for update or running on development environment or failed transaction."
            )
        return order_update_data
