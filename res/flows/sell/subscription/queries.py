INSERT_TRANSACTION = """
mutation insertTransaction(
  $input: sell_transactions_insert_input!
) {
  insert_sell_transactions_one(object: $input) {
    id
    amount
    reference_id
    source
    stripe_charge_id
    subscription
    type
    currency
    description
    is_failed_transaction
  }
}
"""

UPDATE_BALANCE_SUBSCRIPTION = """
mutation updateTransactionBalance($subscription_id: Int!, $amount: Int!) {
  update_sell_subscriptions(where: {id: {_eq: $subscription_id}}, _inc: {balance: $amount}) {
    returning{
      id
      price_amount
      subscription_id
      currency
      brand_id
      stripe_customer_id
    }
  }
}
"""

GET_SUBSCRIPTION = """
query MyQuery($stripe_customer_id: String!) {
  sell_subscriptions(where: {stripe_customer_id: {_eq: $stripe_customer_id}}) {
    id
    balance
    brand_id
    collection_method
  }
}
"""

ORDER_UPDATE_MUTATION = """
mutation ChangeOrderPaymentState($id:ID!, $input:UpdateOrderInput!) {
  updateOrder(id:$id, input:$input){
    order{
      id
      wasPaymentSuccessful
    }
  }
}
"""

INSERT_TRANSACTION_DETAILS = """
mutation($details:[sell_transaction_details_insert_input!]!) {
  insert_sell_transaction_details(objects: $details) {
    affected_rows
    returning {
      id
      sku
      make_cost
      type
      price
      transaction_id
      order_number
      order_date
      order_type
      revenue_share
      total_amount
    }
  }
}
"""
