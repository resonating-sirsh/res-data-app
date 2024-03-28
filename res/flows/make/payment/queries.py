GET_CURRENT_INVOICE = """
query GetInvoicing($order_id: ID!) {
  order(id: $order_id) {
    approvedForFulfillment
    code
    styleCodes
    orderStatus
    orderStatusV3
  	isCancelable
    orderChannel
    lineItemsCreated
    fulfillmentStatus
    wasPaymentSuccessful
    salesChannel
    number
    dateCreatedAt
    createdAt
    brand{
      id
      code
      name
      revenueShareEcommerce
      revenueShareWholesale
    }
    lineItemsInfo {
      id
      sku
      orderId
      lineItemPrice
    }
  }
}
"""

GET_STYLES = """
query GetStylesInvoice($after:String, $resonance_code_array: [WhereValue!]) {
  styles(first:100, after:$after, where:{code: {isAnyOf: $resonance_code_array}} ){
    count
    cursor
    styles{
      id
      code
      allProducts{
        wholesalePrice
        price
        allSku
      }
      onePrices{
        cost
        size {
          code
        }
      }
    }
  }
}
"""

GET_SUBSCRIPTION = """
query MyQuery($brand_id: Int!) {
  sell_subscriptions(where: {brand_id: {_eq: $brand_id}}) {
    id
    balance
    brand_id
    collection_method
    stripe_customer_id
    end_date
    is_direct_payment_default
  }
}
"""

GET_BRAND = """
query MyQuery($brand_id_string: String!) {
  sell_brands(where: {airtable_brand_id: {_eq: $brand_id_string}}) {
    airtable_brand_code
    airtable_brand_id
    id
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
