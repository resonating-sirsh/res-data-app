GET_LINE_ITEM = """
query getLineItemByOneNumber($where: OrderLineItemsWhere!) {
  orderLineItems(first: 1, where: $where) {
    orderLineItems {
      id
      name
      order {
        id
        key
      }
      brand {
        id
        code
      }
      style {
        id
      }
      salesChannel
      requestType
      isUnitReadyInWarehouse
      fulfillmentStatus
      factoryInvoicePrice
      flagForReviewTags
      fulfillment {
        id
        lineItemsCount
      }
      makeOneProductionRequest {
        id
      }
    }
  }
}
"""

GET_INVENTORY_DATA = """
query getInventoryByOneNumber($where: UnitsWhere!) {
  units(first: 100, where: $where) {
    units {
      id
      checkinType
      barcodeUnit
      warehouseCheckinLocation
      resMagicOrderNumber
      flagForReviewNotes
      binStatus
      warehouseLocation(first: 100){
        warehouseLocations{
          id
          name
        }
      }
    }
  }
}
"""

GET_MAKE_DATA = """
query getMakeOneProductionByOneNumber($where: MakeOneProductionRequestsWhere!) {
  makeOneProductionRequests(first: 100, where: $where, sort: [{ field: DAYS_SINCE_REQUEST_SUBMITTED, direction: DESCENDING }]) {
    makeOneProductionRequests {
      id
      sku
      currentMakeNode
      daysSinceOriginalRequest
      daysSinceRequestSubmitted
      daysInMakeNode
      allocationStatus
      brand {
        id
        code
      }
      type
      salesChannel
      orderLineItem {
        id
        name
        order {
          id
          key
        }
        brand {
          id
          code
        }
        style {
          id
        }
        salesChannel
        requestType
        isUnitReadyInWarehouse
        fulfillmentStatus
        factoryInvoicePrice
        flagForReviewTags
        fulfillment {
          id
          lineItemsCount
        }
        makeOneProductionRequest {
          id
        }
      }
    }
  }
}
"""

UPDATE_INVENTORY = """
mutation updateUnit($id: ID!, $input: UpdateUnitInput!) {
    updateUnit(id:$id, input:$input) {
        unit {
            id
        }
    }
}
"""

UPDATE_LINE_ITEM = """
    mutation updateOrderLineItem($id: ID!, $input: UpdateOrderLineItemInput){
        updateOrderLineItem(id:$id, input:$input){
            orderLineItem {
                id
            }
        }
    }
"""

UPDATE_MAKE_ONE_PRODUCTION = """
    mutation updateProductionRequest($id: ID!, $input: UpdateMakeOneProductionRequestInput!) {
        updateProductionRequest(id: $id, input: $input) {
            makeOneProductionRequest{
                id
            }
        }
    }
"""
