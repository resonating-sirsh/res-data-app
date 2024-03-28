GET_ALL_BRANDS = """
query getBrands($limit: Int, $offset: Int) {
  sell_brands(limit:$limit, offset:$offset) {
    id
    airtable_brand_id
    airtable_brand_code
  }
}
"""


GET_DELAYED_ORDERS_FOR_BRANDS = """
query GetOrders($after:String, $brand_code:WhereValue!) {
  orders(first: 100, after:$after, where:{and:[{brandCode:{is:$brand_code}}, {wasPaymentSuccessful: {is: false}}, {wasBalanceNotEnough: {is: true}}]}){
    count
    cursor
    orders{
      id
      name
      code
      sourceName
      wasBalanceNotEnough
      wasPaymentSuccessful
      brand{
        id
        code
        name
      }
    }
  }
}

"""

GET_SUBSCRIPTIONS = """
query MyQuery($brand_id: String!) {
  sell_subscriptions(where: {brand: {airtable_brand_id: {_eq: $brand_id}}}) {
    id
    balance
    brand_id
    collection_method
    stripe_customer_id
    end_date
    payment_method
    brand {
      id
      airtable_brand_id
    }
  }
}
"""
