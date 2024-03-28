SAVING_BRAND = """
mutation saveBrand(
  $input: [sell_brands_insert_input!]!
) {
  insert_sell_brands(objects: $input) {
    returning {
      id
      airtable_brand_code
      airtable_brand_id
    }
  }
}
"""


SAVING_STRIPE_SUBSCRIPTION = """
mutation saveStripeSubscription(
  $input: [sell_subscriptions_insert_input!]!
) {
  insert_sell_subscriptions(objects: $input) {
    returning {
      balance
      collection_method
      created_at
      deleted_status
      end_date
      id
      name
      start_date
      subscription_id
      updated_at
      stripe_customer_id
      brand_id
      payment_method
      current_period_start
      current_period_end
      is_direct_payment_default
    }
  }
}
"""


GET_SUBSCRIPTION = """
query getSubscription(
  $brand_id: Int!
) {
  sell_subscriptions(where: {brand_id: {_eq: $brand_id}}) {
    id
    balance
    brand_id
    collection_method
    created_at
    deleted_status
    end_date
    current_period_start
    current_period_end
    name
    payment_method
    start_date
    stripe_customer_id
    subscription_id
    updated_at
  }
}
"""


GET_BRAND = """
query getBrand(
  $brand_id: String!
) {
  sell_brands(where: {airtable_brand_id: {_eq: $brand_id}}) {
    id
  }
}
"""

GET_BRAND_DIRECT_PAYMENT_DEFAULT = """
query getBrandDirectPaymentDefault($id:ID!) {
	brand(id:$id){
    id
    isDirectPaymentDefault
  }
}
"""

GET_TRANSACTION = """
query GetTransactions($transaction_id: Int!) {
  sell_transactions(where: {id: {_eq: $transaction_id}}) {
    amount
    created_at
    currency
    description
    direct_stripe_payment
    id
    is_failed_transaction
    reference_id
    source
    stripe_charge_id
    subscription
    type
    updated_at
    transaction_type {
      value
    }
    transaction_source {
      value
    }
    subscriptionBySubscription {
      balance
      brand_id
      collection_method
      created_at
      currency
      current_period_end
      current_period_start
      deleted_status
      id
      end_date
      name
      is_direct_payment_default
      payment_method
      price
      price_amount
      start_date
      stripe_customer_id
      subscription_id
      updated_at
      brand {
        subscription {
          name
        }
        airtable_brand_code
      }
    }
    transaction_details {
      id
      make_cost
      order_date
      order_number
      order_type
      price
      revenue_share
      sku
      total_amount
      transaction_id
      type
    }
  }
}

"""

ORDER_UPDATE_MUTATION = """
mutation ChangeOrderPaymentState($id:ID!, $input:UpdateOrderInput!) {
  updateOrder(id:$id, input:$input){
    order{
      id
      wasBalanceNotEnough
    }
  }
}
"""
