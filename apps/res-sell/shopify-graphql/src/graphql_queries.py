FIND_BODY_QUERY = """
       query Body($number: String!) {
        body(number: $number) {
            name  
        }
    }
"""

UPDATE_ORDER_MUTATION = """
    mutation updateEcommerceOrder($input: UpdateOrderInput!) {
        updateEcommerceOrder(input: $input){
            order{
                id
                brand{
                    name
                    code
                }
                email
                fulfillmentStatus
                financialStatus
                discountCode
                sourceName
                isFlaggedForReview
                flagForReviewNotes
                dateCreatedAt
                closedAt
                name
            }
             
        }
    }
"""

CREATE_ORDER_MUTATION = """
    mutation createEcommerceOrder($input: CreateEcommerceOrderInput!) {
    createEcommerceOrder(input: $input) {
        order{
            id
            brand{
                name
                code
            }
            email
            fulfillmentStatus
            financialStatus
            discountCode
            sourceName
            isFlaggedForReview
            flagForReviewNotes
            dateCreatedAt
            closedAt
            name
        }
    }
}
"""

GET_ORDERS_QUERY = """
    query Orders ($where: OrdersWhere) {
    orders(first: 100, where: $where) {
        orders{
            id
        }
    }
}   
"""

ORDER_LINES_QUERY = """
    query OrderLineItems($where: OrderLineItemsWhere) {
    orderLineItems(first: 100, where: $where) {
        orderLineItems{
            id
            orderNumber
            shopifyFulfillmentStatus
            channelOrderLineItemId
            productName
            productVariantTitle
            sku
            quantity
            lineItemPrice
            shopifyFulfillmentStatus
            shopifyFulfillableQuantity
        }
    }
}
"""

GET_BRAND_QUERY = """
    query Brand($where: BrandsWhere) {
        brands(first: 1, where: $where){
            brands{
                id
                code
                fulfillmentId
            }
        }
    }
"""
